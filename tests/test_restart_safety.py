import asyncio
import json
import os
import re
import signal
import sqlite3
import subprocess
import sys
import unittest
from contextlib import closing, contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from time import monotonic, sleep
from urllib.request import ProxyHandler, Request, build_opener

from broker_core.broker import Broker
from broker_core.store import service_lock
from job_broker import create_app


PROJECT = Path(__file__).resolve().parents[1]
ADDRESS = re.compile(r"Uvicorn running on (http://127\.0\.0\.1:\d+)")
LOCAL_HTTP = build_opener(ProxyHandler({}))


@contextmanager
def running_service(database, kill_on_exit=False):
    env = dict(os.environ, BROKER_DB=str(database), BROKER_CLOCK_SPEED="0", BROKER_IDLE_SECONDS="300")
    log_path = database.with_suffix(".log")
    with log_path.open("wb") as log:
        process = subprocess.Popen(
            [sys.executable, "-B", "-m", "uvicorn", "job_broker:app",
             "--host", "127.0.0.1", "--port", "0", "--no-access-log"],
            cwd=PROJECT, env=env, stdout=log, stderr=subprocess.STDOUT,
        )
        try:
            deadline = monotonic() + 10
            while monotonic() < deadline and process.poll() is None:
                log_text = log_path.read_text(encoding="utf-8", errors="replace")
                match = ADDRESS.search(log_text)
                if match:
                    break
                sleep(0.025)
            else:
                raise AssertionError(f"Broker failed to start\n{log_path.read_text()}")

            def send(method, route, body=None, key=None):
                headers = {}
                data = None
                if body is not None:
                    headers["Content-Type"] = "application/json"
                    data = json.dumps(body).encode()
                if key is not None:
                    headers["Idempotency-Key"] = key
                request = Request(match[1] + route, data=data, headers=headers, method=method)
                with LOCAL_HTTP.open(request, timeout=5) as response:
                    return response.status, json.load(response)

            yield send
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGKILL if kill_on_exit else signal.SIGTERM)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
                raise AssertionError("Broker did not shut down")


def read_state(database):
    tables = ("broker_state", "jobs", "nodes", "sim_events")
    with closing(sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True)) as db:
        db.row_factory = sqlite3.Row
        db.execute("BEGIN")
        return {
            table: [dict(row) for row in db.execute(f"SELECT * FROM {table} ORDER BY id")]
            for table in tables
        }


def job_payload(gpus, minutes):
    return {"team": "vision", "gpus": gpus, "estimated_minutes": minutes}


class RestartSafetyTests(unittest.TestCase):
    def setUp(self):
        scratch = TemporaryDirectory(prefix="job-broker-restart-")
        self.addCleanup(scratch.cleanup)
        self.database = Path(scratch.name) / "broker.sqlite"
        # Start outside an outage window so the test exercises the same node states each run.
        Broker(self.database, start=19500)

    def test_graceful_restart(self):
        self.check_restart(kill_on_exit=False)

    def test_process_kill_then_restart(self):
        self.check_restart(kill_on_exit=True)

    def test_second_instance_cannot_open_same_database_through_symlink(self):
        alias = self.database.with_name("alias.sqlite")
        alias.symlink_to(self.database)
        app = create_app(alias, clock_speed=0)

        async def start():
            async with app.router.lifespan_context(app):
                pass

        with service_lock(self.database):
            with self.assertRaisesRegex(ValueError, "holds the database lock"):
                asyncio.run(start())

    def check_restart(self, kill_on_exit):
        running_payload = job_payload(2, 120)
        with running_service(self.database, kill_on_exit) as send:
            _, finished_job = send("POST", "/jobs", job_payload(1, 1), "history")
            send("POST", "/sim/advance", {"seconds": 1200})
            _, running_job = send("POST", "/jobs", running_payload, "running")
            send("POST", "/sim/advance", {"seconds": 480})
            _, boot_job = send("POST", "/jobs", job_payload(8, 120), "booting")

            saved = read_state(self.database)
            self.assertEqual(
                [(job["id"], job["status"]) for job in saved["jobs"]],
                [(finished_job["id"], "succeeded"), (running_job["id"], "running"), (boot_job["id"], "queued")],
            )
            self.assertEqual([node["status"] for node in saved["nodes"]], ["terminated", "ready", "booting"])
            stopped_node = saved["nodes"][0]
            self.assertGreater(stopped_node["stopped_at"], stopped_node["launched_at"])
            self.assertEqual(stopped_node["hourly_cents"], 100)
            self.assertTrue(any(event["due_at"] is not None for event in saved["sim_events"]))

        with running_service(self.database) as send:
            # This compares identifiers, reservations, deadlines, the clock, and every billing input.
            self.assertEqual(read_state(self.database), saved)
            code, repeated = send("POST", "/jobs", running_payload, "running")
            self.assertEqual((code, repeated["id"], repeated["status"]), (200, running_job["id"], "running"))

            _, clock = send("POST", "/sim/advance", {"seconds": 480})
            self.assertEqual(clock["sim_time"], saved["broker_state"][0]["sim_time"] + 480)
            _, booted = send("GET", f"/jobs/{boot_job['id']}")
            self.assertEqual((booted["status"], booted["node_id"]), ("running", saved["nodes"][2]["id"]))
            resumed = read_state(self.database)
            self.assertEqual(resumed["nodes"][0], stopped_node)
            self.assertEqual(
                [(node["id"], node["launched_at"], node["hourly_cents"]) for node in resumed["nodes"]],
                [(node["id"], node["launched_at"], node["hourly_cents"]) for node in saved["nodes"]],
            )
            send("POST", "/sim/advance", {"seconds": 7300})
            for job_id in (running_job["id"], boot_job["id"]):
                _, job = send("GET", f"/jobs/{job_id}")
                self.assertEqual(job["status"], "succeeded")
