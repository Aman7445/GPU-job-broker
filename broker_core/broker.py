"""Schedule jobs, apply lifecycle changes, and stop idle nodes."""

import logging
from pathlib import Path
from time import time

from . import costs, fairness, store
from .provider import CapacityUnavailable, FakeProvider, NODE_LIMIT, TYPES

LOG = logging.getLogger(__name__)
TERMINAL = ("succeeded", "failed", "cancelled")


class RequestError(ValueError):
    def __init__(self, message, status=400):
        super().__init__(message)
        self.status = status


class Broker:
    def __init__(self, path, start=None, seed=42, idle_seconds=300, provider=None, *, policy="fair"):
        self.path = Path(path)
        self.idle_seconds = idle_seconds
        self.provider = provider if provider is not None else FakeProvider()
        if policy not in ("fair", "fifo"):
            raise ValueError("Scheduling policy must be fair or fifo")
        self.policy = policy
        if idle_seconds < 0:
            raise ValueError("Idle seconds must be nonnegative")
        store.initialize(self.path, int(time()) if start is None else start, seed)

    @staticmethod
    def _state(conn):
        return conn.execute("SELECT * FROM broker_state WHERE id=1").fetchone()

    def submit(self, request):
        return self.submit_batch([(request, None, "succeeded")])[0]

    def submit_batch(self, requests):
        """Queue simultaneous arrivals before making placement decisions."""
        with store.transaction(self.path, write=True) as db:
            now = self._state(db)["sim_time"]
            accepted = []
            for request, actual_seconds, outcome in requests:
                team, key, gpus, estimate, priority = request
                saved = db.execute(
                    "SELECT * FROM jobs WHERE team=? AND client_key=?", (team, key)
                ).fetchone()
                if saved is not None:
                    if (saved["gpus"], saved["estimated_seconds"], saved["priority"]) != (
                        gpus, estimate, priority
                    ):
                        raise RequestError("Idempotency key already has a different submission", 409)
                    accepted.append((saved["id"], False))
                    continue

                job_id = db.execute(
                    """INSERT INTO jobs
                       (team,client_key,gpus,estimated_seconds,priority,status,submitted_at)
                       VALUES (?,?,?,?,?,'queued',?)""",
                    (team, key, gpus, estimate, priority, now),
                ).lastrowid
                db.execute(
                    "INSERT INTO sim_events(job_id,outcome,runtime_seconds) VALUES (?,?,?)",
                    (job_id, outcome, estimate if actual_seconds is None else actual_seconds),
                )
                accepted.append((job_id, True))
            if any(created for _, created in accepted):
                self._settle(db, now)
            return [(self._get_job(db, job_id), created) for job_id, created in accepted]

    @staticmethod
    def _get_job(conn, job_id):
        row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            raise RequestError("Job not found", 404)
        return dict(row)

    def job(self, job_id):
        with store.transaction(self.path) as conn:
            return self._get_job(conn, job_id)

    def queue(self, after, limit):
        with store.transaction(self.path) as conn:
            return [dict(row) for row in conn.execute(
                "SELECT * FROM jobs WHERE status='queued' AND id>? ORDER BY id LIMIT ?",
                (after, limit),
            )]

    def nodes(self, after, limit, include_stopped):
        with store.transaction(self.path) as conn:
            sql = """SELECT n.*, COALESCE(SUM(CASE WHEN j.status='running' THEN j.gpus END),0)
                     AS running_gpus,
                     COALESCE(SUM(CASE WHEN j.status='queued' THEN j.gpus END),0) AS reserved_gpus
                     FROM nodes n LEFT JOIN jobs j ON n.id=j.node_id WHERE n.id>?"""
            if not include_stopped:
                sql += " AND n.status IN ('booting','ready')"
            return [dict(row) for row in conn.execute(
                sql + " GROUP BY n.id ORDER BY n.id LIMIT ?", (after, limit),
            )]

    def finish(self, job_id, outcome):
        with store.transaction(self.path, write=True) as conn:
            job = self._get_job(conn, job_id)
            if job["status"] in TERMINAL:
                return job
            if job["status"] == "queued" and outcome != "cancelled":
                raise RequestError("The job is still queued", 409)
            now = self._state(conn)["sim_time"]
            self._finish(conn, job, outcome, now)
            self._settle(conn, now)
            return self._get_job(conn, job_id)

    def _finish(self, conn, job, outcome, now):
        node_id = job["node_id"] if job["status"] == "running" else None
        conn.execute(
            "UPDATE jobs SET status=?,finished_at=?,node_id=? WHERE id=?",
            (outcome, now, node_id, job["id"]),
        )
        conn.execute("DELETE FROM sim_events WHERE job_id=?", (job["id"],))
        if node_id is not None:
            conn.execute(
                """UPDATE nodes SET idle_since=? WHERE id=?
                   AND NOT EXISTS (SELECT 1 FROM jobs WHERE node_id=? AND status='running')""",
                (now, node_id, node_id),
            )

    def _start(self, conn, job, node_id, now):
        conn.execute(
            "UPDATE jobs SET status='running',node_id=?,started_at=? WHERE id=?",
            (node_id, now, job["id"]),
        )
        conn.execute("UPDATE nodes SET idle_since=NULL WHERE id=?", (node_id,))
        conn.execute(
            "UPDATE sim_events SET due_at=?+runtime_seconds WHERE job_id=?", (now, job["id"])
        )

    def _process_events(self, conn, now):
        events = conn.execute(
            "SELECT * FROM sim_events WHERE due_at<=? ORDER BY due_at,id", (now,)
        ).fetchall()
        for event in events:
            node_id = event["node_id"]
            if node_id is None:
                self._finish(conn, self._get_job(conn, event["job_id"]), event["outcome"], now)
            elif event["outcome"] == "ready":
                conn.execute(
                    "UPDATE nodes SET status='ready',ready_at=?,idle_since=? WHERE id=?",
                    (now, now, node_id),
                )
                for job in conn.execute(
                    "SELECT * FROM jobs WHERE node_id=? AND status='queued' ORDER BY id", (node_id,)
                ).fetchall():
                    self._start(conn, job, node_id, now)
            else:
                self.provider.terminate_node(conn, node_id, now, "failed")
                conn.execute("UPDATE jobs SET node_id=NULL WHERE node_id=? AND status='queued'", (node_id,))
                LOG.warning("event=boot_failed node_id=%s sim_time=%s", node_id, now)
            conn.execute("DELETE FROM sim_events WHERE id=?", (event["id"],))

    @staticmethod
    def _live_nodes(conn):
        return [dict(row) for row in conn.execute(
            """SELECT n.*, COALESCE(SUM(j.gpus),0) AS used
               FROM nodes n LEFT JOIN jobs j ON n.id=j.node_id AND j.status IN ('queued','running')
               WHERE n.status IN ('booting','ready') GROUP BY n.id"""
        )]

    def _schedule(self, conn, now):
        state = self._state(conn)
        launch_allowed = state["launch_retry_at"] is None or state["launch_retry_at"] <= now
        while True:
            nodes = self._live_nodes(conn)
            queued = conn.execute("SELECT * FROM jobs WHERE status='queued'").fetchall()
            if not queued:
                return
            if self.policy == "fair":
                protected, held_node = fairness.protected_placement(queued, nodes, now, NODE_LIMIT)
                team_gpus = dict(conn.execute(
                    """SELECT team, SUM(gpus) FROM jobs
                       WHERE node_id IS NOT NULL AND status IN ('queued','running') GROUP BY team"""
                ))
                ordered = sorted(queued, key=lambda row: fairness.rank_key(row, now, team_gpus, protected))
            else:
                protected, held_node = None, None
                ordered = sorted(queued, key=fairness.fifo_key)

            for job in ordered:
                candidates = [
                    node for node in nodes if node["gpus"] - node["used"] >= job["gpus"]
                    and (node["id"] != held_node or job["id"] == protected)
                    and (job["node_id"] is None or node["status"] == "ready")
                ]
                if candidates:
                    node = min(candidates, key=lambda n: (
                        n["status"] != "ready", n["gpus"] - n["used"], n["id"]
                    ))
                    if node["status"] == "ready":
                        self._start(conn, job, node["id"], now)
                    else:
                        conn.execute("UPDATE jobs SET node_id=? WHERE id=?", (node["id"], job["id"]))
                    break
                if job["node_id"] is not None or not launch_allowed or len(nodes) >= NODE_LIMIT:
                    continue
                chosen = next(node_type for node_type in TYPES if node_type[0] >= job["gpus"])
                try:
                    node_id = self.provider.launch_node(conn, chosen, job["id"], now, state["seed"])
                except CapacityUnavailable:
                    attempt = self._state(conn)["backoff_attempt"]
                    retry_at = now + min(300, 30 * 2 ** min(attempt, 4))
                    conn.execute(
                        """UPDATE broker_state SET launch_retry_at=?,backoff_attempt=backoff_attempt+1,
                           capacity_errors=capacity_errors+1 WHERE id=1""", (retry_at,)
                    )
                    launch_allowed = False
                    LOG.warning("event=capacity_unavailable retry_at=%s sim_time=%s", retry_at, now)
                    continue
                conn.execute("UPDATE broker_state SET launch_retry_at=NULL,backoff_attempt=0 WHERE id=1")
                conn.execute("UPDATE jobs SET node_id=? WHERE id=?", (node_id, job["id"]))
                LOG.info("event=node_launched node_id=%s type=%s sim_time=%s", node_id, chosen[1], now)
                break
            else:
                return

    def _reap_idle_nodes(self, db, now):
        expired = db.execute(
            """SELECT id FROM nodes WHERE status='ready' AND idle_since IS NOT NULL
               AND idle_since + ? < ?""", (self.idle_seconds, now)
        ).fetchall()
        for node in expired:
            self.provider.terminate_node(db, node["id"], now)
            LOG.info("event=node_terminated node_id=%s sim_time=%s", node["id"], now)
        return bool(expired)

    def _settle(self, db, now):
        while True:
            self._process_events(db, now)
            self._schedule(db, now)
            if not self._reap_idle_nodes(db, now) and not db.execute(
                "SELECT 1 FROM sim_events WHERE due_at<=? LIMIT 1", (now,)
            ).fetchone():
                return

    def _next_due(self, db, now):
        deadlines = [
            db.execute("SELECT MIN(due_at) FROM sim_events").fetchone()[0],
            db.execute(
                "SELECT MIN(idle_since)+?+1 FROM nodes WHERE status='ready'", (self.idle_seconds,)
            ).fetchone()[0],
            self._state(db)["launch_retry_at"],
        ]
        if self.policy == "fair":
            for age in (fairness.PROMOTION_SECONDS, fairness.PROTECTION_SECONDS):
                deadlines.append(db.execute(
                    """SELECT MIN(submitted_at) + ? FROM jobs
                       WHERE status='queued' AND node_id IS NULL AND submitted_at + ? > ?""",
                    (age, age, now),
                ).fetchone()[0])
        return min((deadline for deadline in deadlines if deadline is not None and deadline > now), default=None)

    def advance(self, *, seconds):
        if seconds < 0:
            raise RequestError("Simulated time cannot move backward")
        with store.transaction(self.path, write=True) as db:
            now = self._state(db)["sim_time"]
            target = now + seconds
            self._settle(db, now)
            while now < target:
                due = self._next_due(db, now)
                now = min(target, due) if due is not None else target
                db.execute("UPDATE broker_state SET sim_time=? WHERE id=1", (now,))
                self._settle(db, now)
            return now

    def next_deadline(self):
        with store.transaction(self.path) as db:
            return self._next_due(db, self._state(db)["sim_time"])

    def cost_report(self, start, end):
        try:
            start, end = costs.parse_timestamp(start), costs.parse_timestamp(end)
        except ValueError as exc:
            raise RequestError(str(exc)) from None
        if end <= start:
            raise RequestError("to must be later than from")
        with store.transaction(self.path) as db:
            return costs.report(db, start, end, self._state(db)["sim_time"])
