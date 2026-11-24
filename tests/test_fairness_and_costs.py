import logging
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from urllib.error import HTTPError

from broker_core import store
from broker_core.broker import Broker, RequestError
from broker_core.provider import CapacityUnavailable, FakeProvider
from test_restart_safety import running_service


class ControlledProvider(FakeProvider):
    available = True

    def launch_node(self, conn, node_type, launch_job_id, now, seed):
        if not self.available:
            raise CapacityUnavailable()
        return super().launch_node(conn, node_type, launch_job_id, now, seed)


class FairnessTests(unittest.TestCase):
    def setUp(self):
        scratch = TemporaryDirectory(prefix="broker-fairness-")
        self.addCleanup(scratch.cleanup)
        self.provider = ControlledProvider()
        self.broker = Broker(Path(scratch.name) / "broker.sqlite", start=1200, provider=self.provider)
        warnings = patch.object(logging.getLogger("broker_core.broker"), "warning")
        warnings.start()
        self.addCleanup(warnings.stop)

    def submit(self, team, key, gpus=2, priority="normal"):
        return self.broker.submit((team, key, gpus, 10_000, priority))[0]["id"]

    def status(self, job_id):
        return self.broker.job(job_id)["status"]

    def test_team_using_fewer_gpus_gets_next_slot_at_same_priority(self):
        first = self.submit("vision", "a")
        self.submit("vision", "b")
        self.broker.advance(seconds=480)
        self.provider.available = False
        older = self.submit("vision", "c")
        other_team = self.submit("speech", "a")

        self.broker.finish(first, "succeeded")
        self.assertEqual(self.status(older), "queued")
        self.assertEqual(self.status(other_team), "running")

    def test_fresh_high_priority_beats_team_gpu_tie_breaker(self):
        first = self.submit("vision", "a")
        self.submit("vision", "b")
        self.broker.advance(seconds=480)
        self.provider.available = False
        low = self.submit("speech", "a", priority="low")
        high = self.submit("vision", "c", priority="high")

        self.broker.finish(first, "succeeded")
        self.assertEqual(self.status(low), "queued")
        self.assertEqual(self.status(high), "running")

    def test_waiting_job_is_promoted_after_thirty_minutes(self):
        first = self.submit("vision", "a")
        self.submit("vision", "b")
        self.broker.advance(seconds=480)
        self.provider.available = False
        low = self.submit("speech", "a", priority="low")
        self.broker.advance(seconds=1800)
        normal = self.submit("vision", "c")

        self.broker.finish(first, "succeeded")
        self.assertEqual(self.status(low), "running")
        self.assertEqual(self.status(normal), "queued")

    def test_oldest_large_job_can_claim_a_whole_node(self):
        starter = self.submit("vision", "all-gpus", gpus=8)
        self.broker.advance(seconds=480)
        node_id = self.broker.job(starter)["node_id"]
        self.broker.finish(starter, "succeeded")
        running = [self.submit("vision", f"fill-{i}") for i in range(4)]
        self.provider.available = False
        waiting_large = self.submit("forecast", "large", gpus=8, priority="low")
        waiting_small = self.submit("speech", "small", priority="high")

        self.broker.advance(seconds=3600)
        self.broker.finish(running[0], "succeeded")
        self.assertEqual(self.status(waiting_small), "queued")
        for job_id in running[1:]:
            self.broker.finish(job_id, "succeeded")
        self.assertEqual(self.status(waiting_large), "running")
        self.assertEqual(self.broker.job(waiting_large)["node_id"], node_id)


    def test_large_job_can_free_a_launch_slot_in_a_full_small_node_fleet(self):
        running = [self.submit("vision", f"fill-{i}", gpus=1) for i in range(10)]
        self.broker.advance(seconds=480)
        self.assertEqual(len(self.broker.nodes(0, 100, False)), 10)
        waiting_large = self.submit("forecast", "large", gpus=8, priority="low")
        waiting_small = self.submit("speech", "small", gpus=1, priority="high")

        self.broker.advance(seconds=3600)
        smallest_node_id = self.broker.job(running[0])["node_id"]
        self.broker.finish(running[0], "succeeded")
        self.assertEqual(self.status(waiting_small), "queued")
        self.broker.advance(seconds=2100)

        nodes = {node["id"]: node for node in self.broker.nodes(0, 100, True)}
        self.assertEqual(nodes[smallest_node_id]["status"], "terminated")
        selected = self.broker.job(waiting_large)
        self.assertEqual(selected["status"], "running")
        self.assertEqual(nodes[selected["node_id"]]["type"], "gpu-l")


class CostTests(unittest.TestCase):
    def setUp(self):
        scratch = TemporaryDirectory(prefix="broker-cost-")
        self.addCleanup(scratch.cleanup)
        self.path = Path(scratch.name) / "broker.sqlite"
        self.broker = Broker(self.path, start=500)
        # A 4-GPU node boots at second 240 and shuts down at second 480.
        with store.transaction(self.path, write=True) as db:
            db.execute(
                """INSERT INTO jobs(id, team, client_key, gpus, estimated_seconds, priority,
                   status, submitted_at, started_at, finished_at)
                   VALUES (1, 'vision', 'v1', 2, 120, 'normal', 'failed', 0, 240, 360),
                          (2, 'speech', 's1', 1, 120, 'normal', 'cancelled', 60, 300, 420)"""
            )
            db.execute(
                """INSERT INTO nodes(id, type, gpus, hourly_cents, launch_job_id,
                   status, launched_at, ready_at, stopped_at)
                   VALUES (1, 'gpu-m', 4, 450, 1, 'terminated', 0, 240, 480)"""
            )
            db.execute("UPDATE jobs SET node_id=1")

    def test_running_gpu_hours_and_bill_split_including_minimum(self):
        report = self.broker.cost_report(0, 500)
        self.assertEqual(report["total_usd"], 0.75)
        self.assertEqual(report["teams"], [
            {"team": "speech", "gpu_hours": 0.03333333, "cost_usd": 0.15625},
            {"team": "vision", "gpu_hours": 0.06666667, "cost_usd": 0.59375},
        ])
        self.assertEqual(Broker(self.path).cost_report(0, 500), report)

    def test_shared_runtime_idle_and_minimum_fall_in_the_right_windows(self):
        shared = self.broker.cost_report(300, 360)
        self.assertEqual(shared["teams"], [
            {"team": "speech", "gpu_hours": 0.01666667, "cost_usd": 0.025},
            {"team": "vision", "gpu_hours": 0.03333333, "cost_usd": 0.05},
        ])
        idle = self.broker.cost_report(420, 480)
        self.assertEqual(idle["teams"], [
            {"team": "speech", "gpu_hours": 0.0, "cost_usd": 0.025},
            {"team": "vision", "gpu_hours": 0.0, "cost_usd": 0.05},
        ])
        self.assertEqual(self.broker.cost_report(480, 481)["total_usd"], 0.15)
        self.assertEqual(self.broker.cost_report(600, 700)["teams"], [])

    def test_live_charges_stop_at_current_simulated_time(self):
        with store.transaction(self.path, write=True) as db:
            db.execute("UPDATE nodes SET status='ready', stopped_at=NULL, idle_since=420")
        report = self.broker.cost_report(480, 800)
        self.assertEqual((report["as_of"], report["total_usd"]), (500, 0.025))

    def test_cost_route_accepts_iso_and_unix_bounds(self):
        with running_service(self.path) as send:
            code, report = send("GET", "/costs?from=1970-01-01T00:00:00Z&to=500")
            self.assertEqual(code, 200)
            self.assertEqual(report, self.broker.cost_report(0, 500))
            with self.assertRaises(HTTPError) as error:
                send("GET", "/costs?from=500&to=500")
            self.assertEqual(error.exception.code, 400)
        with self.assertRaisesRegex(RequestError, "ISO 8601"):
            self.broker.cost_report("yesterday", 500)
