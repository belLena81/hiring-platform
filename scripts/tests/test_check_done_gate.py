"""Behaviour of the Stop gate, run against a throwaway copy of the scripts it calls.
Run: python3 -I -m unittest discover -s scripts/tests -p 'test_*.py'"""

import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent


class StopGate(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.repo = Path(self.folder.name) / "repo"
        (self.repo / "scripts").mkdir(parents=True)
        (self.repo / "src").mkdir()
        (self.repo / "analytics/src").mkdir(parents=True)
        (self.repo / ".gitignore").write_text("target/\n")
        shutil.copy(ROOT / "scripts/check-done.sh", self.repo / "scripts/check-done.sh")
        for name, code in (("check-skills.py", "SKILLS_FAIL"), ("check-spec-verdicts.py", "VERDICTS_FAIL"),
                           ("check-docs-current.py", "DOCS_FAIL")):
            (self.repo / "scripts" / name).write_text(
                f"import os,sys\nprint('{name} ran')\nsys.exit(1 if os.environ.get('{code}') else 0)\n")
        self.git("init", "-q")
        self.git("add", "-A")
        self.git("-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-qm", "base")

    def git(self, *args):
        subprocess.run(["git", *args], cwd=self.repo, check=True, capture_output=True)

    def stop(self, *args, repeated=False, **env):
        return subprocess.run(["bash", "scripts/check-done.sh", *args], cwd=self.repo, text=True, capture_output=True,
                              input=json.dumps({"stop_hook_active": repeated}), env={**os.environ, **env})

    def scala_at(self, relative, epoch):
        path = self.repo / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("class A")
        os.utime(path, (epoch, epoch))

    def analysis_at(self, build, epoch):
        path = self.repo / build / "target/scala-3.9.0/zinc/inc_compile_3.zip"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("analysis")
        os.utime(path, (epoch, epoch))

    def test_ARE_05_1_a_verdict_failure_blocks_the_first_and_the_repeated_stop(self):
        self.assertEqual(2, self.stop(VERDICTS_FAIL="1").returncode)
        self.assertEqual(2, self.stop(repeated=True, VERDICTS_FAIL="1").returncode)

    def test_ARE_05_1_a_skills_failure_blocks_the_repeated_stop(self):
        self.assertEqual(2, self.stop(repeated=True, SKILLS_FAIL="1").returncode)

    def test_ARE_05_2_a_documentation_failure_blocks_only_once(self):
        self.assertEqual(2, self.stop(DOCS_FAIL="1").returncode)
        self.assertEqual(0, self.stop(repeated=True, DOCS_FAIL="1").returncode)

    def test_ARE_05_3_a_repeated_subagent_stop_runs_no_checks(self):
        self.assertEqual(2, self.stop("--subagent", VERDICTS_FAIL="1").returncode)
        self.assertEqual(0, self.stop("--subagent", repeated=True, VERDICTS_FAIL="1").returncode)

    def test_a_subagent_stop_does_not_check_documentation_or_compile_freshness(self):
        self.scala_at("src/A.scala", 2000)
        self.assertEqual(0, self.stop("--subagent", DOCS_FAIL="1").returncode)

    def test_success_prints_only_a_system_message_on_stdout(self):
        result = self.stop()
        self.assertEqual(0, result.returncode)
        self.assertIn("systemMessage", json.loads(result.stdout))

    def test_ARE_06_1_a_tree_without_scala_changes_passes(self):
        (self.repo / "notes.md").write_text("text")
        self.assertEqual(0, self.stop().returncode)

    def test_ARE_06_1_a_scala_change_newer_than_the_analysis_blocks_once_and_names_the_command(self):
        self.analysis_at(".", 1000)
        self.scala_at("src/deep/new/A.scala", 2000)
        first = self.stop()
        self.assertEqual(2, first.returncode)
        self.assertIn("sbt Test/compile", first.stderr)
        self.assertEqual(0, self.stop(repeated=True).returncode)

    def test_ARE_06_1_a_scala_change_older_than_the_analysis_passes(self):
        self.scala_at("src/A.scala", 1000)
        self.analysis_at(".", 2000)
        self.assertEqual(0, self.stop().returncode)

    def test_ARE_06_1_a_scala_change_with_no_analysis_at_all_blocks(self):
        self.scala_at("src/A.scala", 1000)
        self.assertEqual(2, self.stop().returncode)

    def test_ARE_06_1_each_build_is_judged_by_its_own_analysis(self):
        self.analysis_at(".", 3000)
        self.analysis_at("analytics", 1000)
        self.scala_at("analytics/src/A.scala", 2000)
        result = self.stop()
        self.assertEqual(2, result.returncode)
        self.assertIn("analytics build", result.stderr)

    def test_ARE_06_1_a_deleted_file_beside_a_stale_one_does_not_hide_the_stale_one(self):
        self.scala_at("src/Gone.scala", 1000)
        self.git("add", "-A")
        self.git("-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-qm", "scala")
        (self.repo / "src/Gone.scala").unlink()
        self.analysis_at(".", 1500)
        self.scala_at("src/New.scala", 2000)
        self.assertEqual(2, self.stop().returncode)

    def test_ARE_06_1_a_staged_and_a_non_ascii_change_are_seen(self):
        self.analysis_at(".", 1500)
        self.scala_at("src/Спек.scala", 2000)
        self.assertEqual(2, self.stop().returncode)
        self.git("add", "-A")
        self.assertEqual(2, self.stop().returncode)

    def test_ARE_06_1_a_test_support_change_is_judged_against_both_builds(self):
        self.analysis_at(".", 3000)
        self.analysis_at("analytics", 1000)
        self.scala_at("test-support/A.scala", 2000)
        result = self.stop()
        self.assertEqual(2, result.returncode)
        self.assertIn("analytics build", result.stderr)

    def test_ARE_06_1_an_integration_source_is_not_judged_because_test_compile_cannot_clear_it(self):
        self.analysis_at(".", 1000)
        self.analysis_at("analytics", 1000)
        self.scala_at("src/it/A.scala", 2000)
        self.scala_at("analytics/src/it/A.scala", 2000)
        self.assertEqual(0, self.stop().returncode)

    def test_ARE_06_1_an_sbt_edit_is_not_treated_as_a_scala_change(self):
        self.scala_at("build.sbt", 2000)
        self.assertEqual(0, self.stop().returncode)

    def test_ARE_06_1_the_gate_never_runs_sbt(self):
        stub = Path(self.folder.name) / "bin"
        stub.mkdir()
        marker = Path(self.folder.name) / "sbt-ran"
        (stub / "sbt").write_text(f"#!/usr/bin/env bash\ntouch {marker}\n")
        (stub / "sbt").chmod(0o755)
        self.scala_at("src/A.scala", 2000)
        self.stop(PATH=f"{stub}:{os.environ['PATH']}")
        self.assertFalse(marker.exists())

    def test_ARE_05_2_a_repeated_stop_asks_the_verdict_gate_for_lines_only(self):
        (self.repo / "scripts/check-spec-verdicts.py").write_text(
            "import sys\nsys.exit(0 if sys.argv[1:] == ['--lines-only'] else 1)\n")
        self.assertEqual(2, self.stop().returncode)
        self.assertEqual(0, self.stop(repeated=True).returncode)

    def test_ARE_06_1_a_deleted_scala_file_is_not_treated_as_a_change(self):
        self.scala_at("src/A.scala", 1000)
        self.git("add", "-A")
        self.git("-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-qm", "scala")
        (self.repo / "src/A.scala").unlink()
        self.assertEqual(0, self.stop().returncode)


if __name__ == "__main__":
    unittest.main()