from hashlib import sha256

TYPES = ((1, "gpu-s", 100), (4, "gpu-m", 450), (8, "gpu-l", 850))
NODE_LIMIT = 10


class CapacityUnavailable(Exception):
    pass


def draw(seed, *parts):
    data = ":".join(map(str, (seed, *parts))).encode()
    return int.from_bytes(sha256(data).digest()[:8], "big")


class FakeProvider:
    """Use database records as provider state so launch and assignment commit together."""

    def launch_node(self, conn, node_type, launch_job_id, now, seed):
        bucket, offset = divmod(now, 1800)
        if draw(seed, "outage", bucket) % 100 < 12:
            if offset < 480 + draw(seed, "length", bucket) % 421:
                raise CapacityUnavailable()

        gpus, name, cents = node_type
        sequence = conn.execute("SELECT COUNT(*) FROM nodes WHERE type = ?", (name,)).fetchone()[0]
        failed = draw(seed, "boot-failure", name, sequence) % 100 < 3
        boot_seconds = 900 if failed else 240 + draw(seed, "boot-time", name, sequence) % 241
        node_id = conn.execute(
            """INSERT INTO nodes(type,gpus,hourly_cents,launch_job_id,status,launched_at)
               VALUES (?,?,?,?,'booting',?)""",
            (name, gpus, cents, launch_job_id, now),
        ).lastrowid
        conn.execute(
            "INSERT INTO sim_events(due_at,node_id,outcome) VALUES (?,?,?)",
            (now + boot_seconds, node_id, "failed" if failed else "ready"),
        )
        return node_id

    def terminate_node(self, conn, node_id, now, status="terminated"):
        conn.execute(
            "UPDATE nodes SET status=?, stopped_at=?, idle_since=NULL WHERE id=?",
            (status, now, node_id),
        )
