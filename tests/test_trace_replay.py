import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from broker_core.broker import Broker
from replay import TRACE, compare


class ReplayTests(unittest.TestCase):
    def test_recorded_runtime_and_outcome_apply_after_placement(self):
        with TemporaryDirectory() as scratch:
            broker = Broker(Path(scratch) / "test.sqlite", start=1200)
            jobs = broker.submit_batch([
                (("vision", "first", 2, 3600, "normal"), 60, "failed"),
                (("speech", "second", 2, 3600, "normal"), 120, "cancelled"),
            ])
            broker.advance(seconds=800)
            for (job, _), outcome, runtime in zip(jobs, ("failed", "cancelled"), (60, 120)):
                result = broker.job(job["id"])
                self.assertEqual(result["estimated_seconds"], 3600)
                self.assertEqual(result["status"], outcome)
                self.assertEqual(result["finished_at"] - result["started_at"], runtime)

    def test_northwind_trace_replays_both_policies(self):
        with self.assertLogs("broker_core.broker", level="WARNING"):
            result = compare(TRACE)
        self.assertEqual(result["input"]["rows"], 494)
        self.assertEqual(result["input"]["accepted"], 479)
        self.assertEqual(len(result["input"]["duplicate_lines"]), 13)
        self.assertEqual([row["line"] for row in result["input"]["rejected"]], [88, 393])
        self.assertEqual(set(result["policies"]), {"fair", "fifo"})
        for policy, report in result["policies"].items():
            with self.subTest(policy=policy):
                self.assertEqual(report["jobs"], {"succeeded": 432, "failed": 30, "cancelled": 17})
                self.assertEqual(sum(team["started"] for team in report["wait_by_team"].values()), 479)
                self.assertTrue(all(team["never_started"] == 0 for team in report["wait_by_team"].values()))
                self.assertTrue(all(state in ("failed", "terminated") for state in report["nodes"]))
                self.assertAlmostEqual(sum(team["gpu_hours"] for team in report["teams"]), 2093.1)
                self.assertAlmostEqual(sum(team["cost_usd"] for team in report["teams"]), report["total_usd"])
