"""Behaviour of the review-run log and its comparison with spec verdicts.
Run: python3 -I -m unittest discover -s scripts/tests -p 'test_*.py'"""

import datetime
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent.parent


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


recorder = load("record_review_run", "record-review-run.py")
gate = load("check_spec_verdicts_log", "check-spec-verdicts.py")
NOW = datetime.datetime(2026, 11, 4, 12, 0, tzinfo=datetime.timezone.utc)
ALL_PASS = ("- Code Reviewer verdict (x): PASS\n"
            "- Security Engineer verdict: PASS\n"
            "- Final independent QA verdict: PASS\n")
DONE_SPEC = "# Spec\n\n- Status: done\n\n" + ALL_PASS


def run(role, verdict, epoch=100):
    return {"role": role, "verdict": verdict, "epoch": epoch}


class RecordingARun(unittest.TestCase):
    def test_ARE_07_1_a_reviewer_verdict_is_recorded_without_review_content(self):
        event = {"agent_type": "qa-engineer", "last_assistant_message": "secret finding\nVERDICT: PASS\nScope: x"}
        entry = recorder.entry_for(event, NOW, "abc")
        self.assertEqual({"role": "qa-engineer", "verdict": "PASS", "at": "2026-11-04T12:00:00Z",
                          "epoch": int(NOW.timestamp()), "commit": "abc"}, entry)
        self.assertNotIn("secret", json.dumps(entry))

    def test_ARE_07_1_the_verdict_is_read_from_a_handback_report(self):
        event = {"hook_event_name": "PostToolUse", "agent_type": "code-reviewer", "tool_name": "SubagentHandback",
                 "tool_input": {"message": "findings\n```text\nVERDICT: FAIL\nScope: x\n```"}}
        self.assertEqual("FAIL", recorder.entry_for(event, NOW, "abc")["verdict"])

    def test_ARE_07_1_the_report_template_never_counts_as_a_verdict(self):
        for text in ("VERDICT: PASS | FAIL | BLOCKED", "the line VERDICT: PASS is required", "VERDICT: PASSED"):
            event = {"agent_type": "qa-engineer", "last_assistant_message": text}
            self.assertIsNone(recorder.entry_for(event, NOW, "a"), text)

    def test_ARE_07_1_the_worst_verdict_line_decides_so_a_quoted_pass_cannot_override_a_fail(self):
        for text in ("VERDICT: FAIL\nquoted\nVERDICT: PASS", "VERDICT: PASS\nVERDICT: FAIL"):
            event = {"agent_type": "qa-engineer", "last_assistant_message": text}
            self.assertEqual("FAIL", recorder.entry_for(event, NOW, "a")["verdict"], text)

    def test_ARE_07_1_other_agents_and_missing_verdicts_record_nothing(self):
        self.assertIsNone(recorder.entry_for({"agent_type": "Explore", "last_assistant_message": "VERDICT: PASS"}, NOW, "a"))
        self.assertIsNone(recorder.entry_for({"agent_type": "", "last_assistant_message": "VERDICT: PASS"}, NOW, "a"))
        self.assertIsNone(recorder.entry_for({"agent_type": "qa-engineer", "last_assistant_message": "no verdict"}, NOW, "a"))

    def test_ARE_07_1_the_hook_never_blocks_even_on_bad_input(self):
        result = subprocess.run([sys.executable, "-I", str(SCRIPTS / "record-review-run.py")], input="not json", text=True,
                                capture_output=True)
        self.assertEqual(0, result.returncode)


class ComparingVerdictsWithTheLog(unittest.TestCase):
    def test_ARE_07_2_a_claimed_pass_without_a_logged_run_is_reported(self):
        self.assertEqual(3, len(gate.review_log_problems(ALL_PASS, [], 50)))

    def test_ARE_07_2_a_run_older_than_the_last_commit_does_not_count(self):
        entries = [run("code-reviewer", "PASS", 10), run("security-engineer", "PASS"), run("qa-engineer", "PASS")]
        problems = gate.review_log_problems(ALL_PASS, entries, 50)
        self.assertEqual(1, len(problems))
        self.assertIn("code-reviewer", problems[0])

    def test_ARE_07_2_the_latest_run_decides(self):
        entries = [run("code-reviewer", "FAIL"), run("code-reviewer", "PASS"), run("security-engineer", "PASS"),
                   run("qa-engineer", "PASS"), run("qa-engineer", "FAIL")]
        problems = gate.review_log_problems(ALL_PASS, entries, 50)
        self.assertEqual(1, len(problems))
        self.assertIn("qa-engineer", problems[0])

    def test_ARE_07_2_not_applicable_lines_are_not_compared(self):
        text = ALL_PASS.replace("Security Engineer verdict: PASS", "Security Engineer verdict: not applicable - tests only, no data")
        entries = [run("code-reviewer", "PASS"), run("qa-engineer", "PASS")]
        self.assertEqual([], gate.review_log_problems(text, entries, 50))

    def test_damaged_and_non_object_log_lines_are_skipped_and_a_missing_log_is_empty(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "log.jsonl"
            self.assertEqual([], gate.read_review_log(path))
            path.write_text('{"role": "qa-engineer"}\nbroken\n5\n[1]\n')
            self.assertEqual([{"role": "qa-engineer"}], gate.read_review_log(path))


class TheScriptsAgainstARealRepository(unittest.TestCase):
    """Runs copies of both scripts, so PROJECT is a throwaway git repository."""

    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.repo = Path(self.folder.name)
        (self.repo / "scripts").mkdir()
        (self.repo / "docs/specs").mkdir(parents=True)
        for name in ("record-review-run.py", "check-spec-verdicts.py"):
            shutil.copy(SCRIPTS / name, self.repo / "scripts" / name)
        self.git("init", "-q")
        self.git("add", "-A")
        self.commit("base")
        self.log = self.repo / ".local/logs/review-runs.jsonl"

    def git(self, *args):
        subprocess.run(["git", *args], cwd=self.repo, check=True, capture_output=True)

    def commit(self, message):
        self.git("-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-qm", message, "--allow-empty",
                 "--date=2000-01-01T00:00:00Z")
        env = {**os.environ, "GIT_COMMITTER_DATE": "2000-01-01T00:00:00Z"}
        subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "--amend", "-q",
                        "--no-edit", "--allow-empty"], cwd=self.repo, check=True, capture_output=True, env=env)

    def record(self, role, verdict):
        event = {"agent_type": role, "last_assistant_message": f"VERDICT: {verdict}"}
        return subprocess.run([sys.executable, "-I", str(self.repo / "scripts/record-review-run.py")], text=True,
                              input=json.dumps(event), capture_output=True)

    def verdicts(self, *args):
        return subprocess.run([sys.executable, "-I", str(self.repo / "scripts/check-spec-verdicts.py"), *args], text=True,
                              capture_output=True, cwd=self.repo)

    def write_spec(self, name="x.md"):
        (self.repo / "docs/specs" / name).write_text(DONE_SPEC)

    def test_ARE_07_1_the_hook_appends_one_entry_per_report(self):
        self.assertEqual(0, self.record("qa-engineer", "PASS").returncode)
        self.assertEqual(0, self.record("qa-engineer", "FAIL").returncode)
        lines = [json.loads(line) for line in self.log.read_text().splitlines()]
        self.assertEqual(["PASS", "FAIL"], [line["verdict"] for line in lines])

    def test_ARE_07_1_the_hook_does_not_follow_a_link_at_the_log_path(self):
        target = self.repo / "target.txt"
        target.write_text("original")
        self.log.parent.mkdir(parents=True)
        self.log.symlink_to(target)
        self.assertEqual(0, self.record("qa-engineer", "PASS").returncode)
        self.assertEqual("original", target.read_text())

    def test_ARE_07_2_an_uncommitted_done_spec_without_logged_runs_fails(self):
        self.write_spec()
        result = self.verdicts()
        self.assertEqual(1, result.returncode)
        self.assertIn("no code-reviewer run is logged", result.stderr)

    def test_ARE_07_2_a_spec_with_a_non_ascii_name_is_still_compared(self):
        self.write_spec("spéc.md")
        self.assertEqual(1, self.verdicts().returncode)

    def test_ARE_07_2_an_uncommitted_done_spec_with_logged_passes_succeeds(self):
        self.write_spec()
        for role in ("code-reviewer", "security-engineer", "qa-engineer"):
            self.record(role, "PASS")
        self.assertEqual(0, self.verdicts().returncode)

    def test_ARE_07_2_a_committed_done_spec_is_not_compared(self):
        self.write_spec()
        self.git("add", "-A")
        self.commit("spec")
        self.assertEqual(0, self.verdicts().returncode)

    def test_ARE_07_3_lines_only_keeps_verdict_lines_and_skips_the_log(self):
        self.write_spec()
        self.assertEqual(0, self.verdicts("--lines-only").returncode)
        (self.repo / "docs/specs/x.md").write_text("- Status: done\n")
        self.assertEqual(1, self.verdicts("--lines-only").returncode)


if __name__ == "__main__":
    unittest.main()
