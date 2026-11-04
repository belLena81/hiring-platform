"""Behaviour of the check-skills PostToolUse hook mode. Run: python3 -I -m unittest discover -s scripts/tests -p 'test_*.py'"""

import importlib.util
import json
import os
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / "check-skills.py"
spec = importlib.util.spec_from_file_location("check_skills", SCRIPT)
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)
ROOT = SCRIPT.parent.parent


def call(path):
    return json.dumps({"tool_input": {"file_path": str(path)}})


class WhichEditsTriggerTheCheck(unittest.TestCase):
    def test_role_definitions_trigger_it(self):
        for path in (ROOT / "AGENTS.md", ROOT / ".claude/skills/qa-engineer/SKILL.md", ROOT / ".claude/agents/x.md"):
            self.assertTrue(checker.touches_role_definitions(call(path)), path)

    def test_relative_paths_are_taken_from_the_project_root(self):
        for path in ("AGENTS.md", ".claude/agents/x.md", "docs/../.claude/agents/x.md"):
            self.assertTrue(checker.touches_role_definitions(call(path)), path)
        self.assertFalse(checker.touches_role_definitions(call("docs/api.md")))

    def test_other_project_files_and_outside_paths_do_not(self):
        for path in (ROOT / "src/main/scala/A.scala", ROOT / "docs/api.md", Path("/tmp/elsewhere.md")):
            self.assertFalse(checker.touches_role_definitions(call(path)), path)

    def test_a_link_outside_the_project_that_points_into_claude_triggers_it(self):
        # The link lives in a temporary directory, so the checkout is never written to.
        with tempfile.TemporaryDirectory() as outside:
            link = Path(outside) / "innocent.md"
            try:
                os.symlink(ROOT / ".claude" / "agents" / "qa-engineer.md", link)
            except OSError:
                self.skipTest("cannot create a symlink here")
            self.assertTrue(checker.touches_role_definitions(call(link)))

    def test_a_link_outside_the_project_that_points_elsewhere_does_not(self):
        with tempfile.TemporaryDirectory() as outside:
            link = Path(outside) / "innocent.md"
            try:
                os.symlink(ROOT / "docs" / "api.md", link)
            except OSError:
                self.skipTest("cannot create a symlink here")
            self.assertFalse(checker.touches_role_definitions(call(link)))

    def test_unknown_or_malformed_input_fails_closed(self):
        for raw in ("", "not json", "{}", json.dumps({"tool_input": {}}), json.dumps({"tool_input": {"file_path": 3}})):
            self.assertTrue(checker.touches_role_definitions(raw), raw)


if __name__ == "__main__":
    unittest.main()
