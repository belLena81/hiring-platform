"""The backlog drill deletes rows only when the scope looks like exactly one seeded job.
Run: python3 -I -m unittest discover -s scripts/tests -p 'test_alert_release*.py'"""

import importlib.util
import json
import subprocess
import sys
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / "alert_release_scope.py"
spec = importlib.util.spec_from_file_location("alert_release_scope", SCRIPT)
scope = importlib.util.module_from_spec(spec)
spec.loader.exec_module(scope)


class ReleaseDecision(unittest.TestCase):
    def test_one_job_and_one_outbox_row_is_allowed_with_no_embedding_rows(self):
        self.assertTrue(scope.decide(1, ["e1"], []).allowed)

    def test_one_job_with_its_outbox_and_embedding_rows_is_allowed(self):
        self.assertTrue(scope.decide(1, ["e1", "e2"], ["Job:j1"]).allowed)

    def test_zero_matching_outbox_rows_is_refused(self):
        decision = scope.decide(1, [], ["Job:j1"])
        self.assertFalse(decision.allowed)
        self.assertIn("no pending outbox row", decision.reason)

    def test_more_than_one_seeded_job_is_refused(self):
        self.assertFalse(scope.decide(2, ["e1"], []).allowed)

    def test_no_seeded_job_is_refused(self):
        self.assertFalse(scope.decide(0, ["e1"], []).allowed)

    def test_outbox_rows_over_the_bound_are_refused(self):
        self.assertTrue(scope.decide(1, ["e%d" % n for n in range(4)], []).allowed)
        self.assertFalse(scope.decide(1, ["e%d" % n for n in range(5)], []).allowed)

    def test_embedding_rows_over_the_bound_are_refused(self):
        self.assertTrue(scope.decide(1, ["e1"], ["a", "b"]).allowed)
        self.assertFalse(scope.decide(1, ["e1"], ["a", "b", "c"]).allowed)

    def test_command_line_prints_allow_and_refuse_with_the_matching_exit_code(self):
        def run(payload):
            return subprocess.run([sys.executable, "-I", str(SCRIPT)], input=payload, capture_output=True, text=True)
        allowed = run(json.dumps({"jobsCreated": 1, "outboxIds": ["e1"], "embeddingIds": []}))
        self.assertEqual((0, "ALLOW"), (allowed.returncode, allowed.stdout.strip()))
        refused = run(json.dumps({"jobsCreated": 3, "outboxIds": ["e1"], "embeddingIds": []}))
        self.assertEqual(3, refused.returncode)
        self.assertTrue(refused.stdout.startswith("REFUSE "))
        garbage = run("not json")
        self.assertEqual(3, garbage.returncode)


if __name__ == "__main__":
    unittest.main()