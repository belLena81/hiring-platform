"""The workflow-file list is shared: every workflow file must be protected by the guard and carry a decision for the docs
gate. Run: python3 -I -m unittest discover -s scripts/tests -p 'test_*.py'"""

import importlib.util
import json
import sys
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCRIPTS))
import harness_paths  # noqa: E402


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


guard = load("guard_harness", "guard-harness.py")
docs_gate = load("check_docs_current", "check-docs-current.py")

COMPOSE_SAMPLES = ("compose.yaml", "compose.test.yaml", "compose.observability.yaml",
                   "compose.analytics-key-rotation.yaml", "compose.future-name.yml")
# Every entry of the shared list, plus one file under each shared prefix and the compose samples.
ALL_WORKFLOW_PATHS = (tuple(sorted(harness_paths.WORKFLOW_FILES))
                      + tuple(prefix + "example.txt" for prefix in harness_paths.WORKFLOW_PREFIXES)
                      + COMPOSE_SAMPLES)


def harness_paths_root():
    return SCRIPTS.parent


class WorkflowFilesAreCoveredEverywhere(unittest.TestCase):
    def test_the_shared_list_is_not_empty_and_includes_the_known_files(self):
        for path in ("AGENTS.md", "build.sbt", "analytics/Dockerfile", ".mcp.json", ".scalafmt.conf", ".sbtopts", ".jvmopts"):
            self.assertIn(path, harness_paths.WORKFLOW_FILES)

    def test_every_shared_path_is_a_workflow_file(self):
        for path in ALL_WORKFLOW_PATHS:
            self.assertTrue(harness_paths.is_workflow_file(path), path)

    def test_the_guard_asks_before_editing_every_shared_path(self):
        for path in ALL_WORKFLOW_PATHS:
            self.assertTrue(guard.is_protected(path), path)

    def test_the_guard_asks_before_moving_or_replacing_a_workflow_directory(self):
        for path in ("scripts", ".claude", ".githooks", "project"):
            self.assertTrue(guard.is_protected(path), path)

    def test_ARE_07_2_the_guard_asks_before_an_agent_edits_the_review_log(self):
        self.assertTrue(guard.is_protected(".local/logs/review-runs.jsonl"))
        self.assertFalse(guard.is_protected(".local/logs/other.log"))

    def test_ARE_09_7_commands_outside_the_sandbox_start_only_from_the_project_root(self):
        root = harness_paths_root()
        for command in ("scripts/run-local-tests.sh status", "scripts/check-local.sh", "scripts/check-agent-hooks-live.sh",
                        "git commit -m x", "git add README.md", "git push"):
            outcome = guard.check_bash(command, root / "analytics")
            self.assertEqual("deny", outcome and outcome[0], command)
            self.assertIn("project root", outcome[1], command)
            outcome = guard.check_bash(command, root)
            self.assertNotIn("project root", (outcome or ("", ""))[1], command)

    def test_ARE_09_7_read_only_git_stays_allowed_from_a_subdirectory(self):
        self.assertIsNone(guard.check_bash("git status", harness_paths_root() / "analytics"))

    def test_ARE_09_7_the_guard_knows_every_exclusion_in_the_settings(self):
        settings = json.loads((harness_paths_root() / ".claude/settings.json").read_text())
        for pattern in settings["sandbox"]["excludedCommands"]:
            words = pattern.removesuffix(" *").split()
            self.assertTrue(guard.runs_outside_sandbox(words[0], words[1:]), pattern)

    def test_the_docs_gate_requires_documentation_for_every_shared_path(self):
        for path in ALL_WORKFLOW_PATHS:
            self.assertTrue(docs_gate.is_decision(path), path)

    def test_ordinary_files_are_not_workflow_files(self):
        for path in ("src/main/scala/A.scala", "docs/api.md", "README.md", "src/test/scala/A.scala"):
            self.assertFalse(harness_paths.is_workflow_file(path), path)


if __name__ == "__main__":
    unittest.main()