"""The seed's --one-fresh-job mode creates exactly one job with random idempotency keys (used by the alert drills).
Run: python3 -I -m unittest discover -s scripts/tests -p 'test_seed_fresh*.py'  (no network: the HTTP layer is replaced)"""

import contextlib
import importlib.util
import io
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / "seed-local-jobs.py"
spec = importlib.util.spec_from_file_location("seed_local_jobs", SCRIPT)
seed = importlib.util.module_from_spec(spec)
spec.loader.exec_module(seed)


class FreshJobMode(unittest.TestCase):
    def setUp(self):
        self.calls = []
        self.originals = (seed.post, seed.auth, seed.write_private)

        def fake_post(base, query, variables, token=None, rate_limit_reason="rate-limited"):
            self.calls.append((query, variables))
            if query is seed.CREATE_JOB:
                return {"createJob": {"__typename": "Job", "id": "job-1"}}
            if query is seed.PUBLISH_JOB:
                return {"publishJob": {"__typename": "Job"}}
            raise AssertionError("unexpected request in fresh-job mode")

        seed.post = fake_post
        seed.auth = lambda *args, **kwargs: ("header.payload.signature", "2099-01-01T00:00:00Z")
        seed.write_private = lambda *args, **kwargs: None

    def tearDown(self):
        seed.post, seed.auth, seed.write_private = self.originals

    def run_fresh(self):
        self.calls.clear()
        with tempfile.TemporaryDirectory() as repo, contextlib.redirect_stdout(io.StringIO()), \
                contextlib.redirect_stderr(io.StringIO()):
            return seed.run("http://127.0.0.1:8080", 50, 1, repo, True)

    def test_creates_and_publishes_exactly_one_job_whatever_the_jobs_option_says(self):
        detail = self.run_fresh()
        queries = [query for query, _ in self.calls]
        self.assertEqual([seed.CREATE_JOB, seed.PUBLISH_JOB], queries)
        self.assertIn("jobs_created=1", detail)

    def test_never_counts_the_recruiters_open_jobs(self):
        self.run_fresh()
        self.assertNotIn(seed.MY_OPEN_JOBS, [query for query, _ in self.calls])

    def test_uses_random_idempotency_keys_so_no_earlier_receipt_can_replay_it(self):
        self.run_fresh()
        first = [variables["input"]["idempotencyKey"] for _, variables in self.calls]
        self.run_fresh()
        second = [variables["input"]["idempotencyKey"] for _, variables in self.calls]
        fixed = {str(seed.uuid.uuid5(seed.NAMESPACE, "create-%d" % i)) for i in range(60)}
        fixed |= {str(seed.uuid.uuid5(seed.NAMESPACE, "publish-%d" % i)) for i in range(60)}
        self.assertEqual(2, len(set(first)))
        self.assertTrue(set(first).isdisjoint(second))
        self.assertTrue(set(first + second).isdisjoint(fixed))


if __name__ == "__main__":
    unittest.main()