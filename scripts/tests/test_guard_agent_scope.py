"""Behaviour of the subagent role scope hook. Run: python3 -I -m unittest discover -s scripts/tests -p 'test_*.py'"""

import importlib.util
import json
import os
import subprocess
import sys
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / "guard-agent-scope.py"
spec = importlib.util.spec_from_file_location("guard_agent_scope", SCRIPT)
scope = importlib.util.module_from_spec(spec)
spec.loader.exec_module(scope)
PROJECT = str(scope.PROJECT)


class ReviewerEvidenceCommands(unittest.TestCase):
    def test_ARE_01_1_commands_that_collect_evidence_are_allowed(self):
        for command in ("git diff HEAD -- src",
                        "git log --format='%h %s' -5",
                        "grep -n 'a > b' src/A.scala",
                        "grep -n \"a > b\" src/A.scala",
                        "ls docs | head -5",
                        "sed -n 10,20p src/A.scala",
                        "sed -n '10,20p' src/A.scala",
                        "find src -name '*.scala' | wc -l",
                        "sbt test",
                        "sbt 'testOnly com.example.Spec'",
                        "sbt 'Test / testOnly *Spec'",
                        "cd analytics && sbt compile 2>&1",
                        "grep -c x y 2>/dev/null",
                        "grep -c x y >/dev/null 2>&1; echo done",
                        "python3 -I scripts/check-skills.py",
                        "python3 -I -m unittest discover -s scripts/tests -p 'test_*.py'",
                        "scripts/run-local-tests.sh test",
                        "scripts/check-local.sh"):
            self.assertIsNone(scope.read_only_problem(command), command)

    def test_ARE_01_2_commands_that_change_or_hide_are_refused(self):
        for command in ("sed -i s/a/b/ src/A.scala",
                        "touch x",
                        "echo x > y",
                        "echo x >> y",
                        "echo x >&1foo",
                        "cat a | tee b",
                        "git checkout .",
                        "git -c core.pager=evil log",
                        "git diff --output=x",
                        "git diff --ext-diff",
                        "find . -delete",
                        "find . -exec rm {} ;",
                        "python3 evil.py",
                        "python3 -c 'print(1)'",
                        "echo $(touch x)",
                        "echo `touch x`",
                        "echo \"$(touch x)\"",
                        "cat <<EOF",
                        "FOO=bar sbt test",
                        "sbt scalafmtAll",
                        "sbt scalafmtCheckAll",
                        "sbt run",
                        "bash -c 'touch x'",
                        "bash scripts/check-local.sh",
                        "cat a; touch b",
                        "cat a && mv a b",
                        "ls & touch b",
                        "rm x",
                        "rg --pre evil x"):
            self.assertIsNotNone(scope.read_only_problem(command), command)

    def test_ARE_01_2_reported_bypasses_are_refused(self):
        for command in (r"echo \'; touch x; echo \'",
                        r'echo \"; touch x; echo \"',
                        '''echo "'" $(touch x) "'"''',
                        '''echo "it's" $(touch x) "it's"''',
                        "echo a #'\ntouch x\n#'",
                        "ls () ( touch x ); ls",
                        "sed -n 1p -i src/A.scala",
                        "sed -n 1p -e '1e touch x' f",
                        "sort -uo a b",
                        "sort --compress-program=x a",
                        "sort a",
                        "echo x | uniq - target",
                        "git grep -Orm foo",
                        "git grep --open='touch x #' TODO",
                        "sbt 'testOnly x; eval 1'",
                        "sbt 'testOnly x; scalafmtAll'",
                        "python3 -I -m unittest discover -s /tmp/x",
                        "python3 -I -m unittest discover -s scripts/tests -p 'a;b'",
                        "file -C -m x"):
            self.assertIsNotNone(scope.read_only_problem(command), command)

    def test_ARE_01_2_project_scripts_run_only_from_the_project_root(self):
        self.assertIsNotNone(scope.read_only_problem("cd docs && python3 -I scripts/check-skills.py"))
        self.assertIsNotNone(scope.read_only_problem("cd analytics && python3 -I scripts/check-skills.py"))
        self.assertIsNotNone(scope.read_only_problem("cd analytics && scripts/check-local.sh"))
        self.assertIsNotNone(scope.read_only_problem("python3 -I scripts/check-skills.py", os.path.join(PROJECT, "docs")))
        self.assertIsNotNone(scope.read_only_problem("sbt test", os.path.join(PROJECT, "docs")))
        self.assertIsNotNone(scope.read_only_problem("cd /tmp"))
        self.assertIsNone(scope.read_only_problem("sbt test", os.path.join(PROJECT, "analytics")))

    def test_ARE_01_2_an_unquoted_glob_is_refused_because_it_can_expand_to_an_option(self):
        for command in ("sed -n 1,5p *.scala", "git diff *", "find . -de*", "sed -n 1p ?i"):
            self.assertIsNotNone(scope.read_only_problem(command), command)
        self.assertIsNone(scope.read_only_problem("find src -name '*.scala'"))

    def test_ARE_01_2_only_a_leading_cd_that_always_takes_effect_is_allowed(self):
        analytics = os.path.join(PROJECT, "analytics")
        for command in ("cd analytics; cd .. | true; scripts/check-local.sh",
                        "cd .. | true; scripts/check-local.sh",
                        "false && cd ..",
                        "cd .. || true"):
            self.assertIsNotNone(scope.read_only_problem(command, analytics), command)
        self.assertIsNone(scope.read_only_problem("cd .. && scripts/check-done.sh", analytics))

    def test_ARE_01_3_destructive_wrapper_verb_and_unparsable_input_are_refused(self):
        self.assertIsNotNone(scope.read_only_problem("scripts/run-local-tests.sh stop"))
        self.assertIsNotNone(scope.read_only_problem("scripts/run-local-tests.sh"))
        self.assertIsNotNone(scope.read_only_problem("cat 'unterminated"))
        self.assertIsNotNone(scope.read_only_problem('cat "unterminated'))

    def test_other_tools_are_not_judged_by_the_read_only_scope(self):
        self.assertIsNone(scope.decide("read-only", {"tool_name": "Read", "tool_input": {"file_path": "x"}}))


class DocumentAuthorWritePaths(unittest.TestCase):
    def edit(self, role, path):
        return scope.decide(role, {"tool_name": "Edit", "cwd": PROJECT, "tool_input": {"file_path": path}})

    def test_ARE_02_1_the_writer_stays_in_documentation(self):
        for path in ("docs/api.md", "README.md", "ARCHITECTURE.md", f"{PROJECT}/docs/wiki/README.md"):
            self.assertIsNone(self.edit("documentation", path), path)
        for path in ("src/main/A.scala", "docs/../build.sbt", ".claude/settings.json", "/etc/passwd", ""):
            self.assertIsNotNone(self.edit("documentation", path), path)

    def test_ARE_02_2_the_architect_stays_in_design_documents(self):
        for path in ("docs/specs/x.md", "ARCHITECTURE.md"):
            self.assertIsNone(self.edit("design", path), path)
        for path in ("src/main/A.scala", "README.md", "AGENTS.md"):
            self.assertIsNotNone(self.edit("design", path), path)


class HookProtocol(unittest.TestCase):
    def run_hook(self, *args, stdin):
        return subprocess.run([sys.executable, "-I", str(SCRIPT), *args], input=stdin, text=True, capture_output=True)

    def test_a_refusal_exits_2_with_the_reason_on_stderr(self):
        result = self.run_hook("read-only", stdin=json.dumps({"tool_name": "Bash", "tool_input": {"command": "touch x"}}))
        self.assertEqual(2, result.returncode)
        self.assertIn("touch", result.stderr)

    def test_an_allowed_command_exits_0(self):
        result = self.run_hook("read-only", stdin=json.dumps({"tool_name": "Bash", "tool_input": {"command": "git status"}}))
        self.assertEqual(0, result.returncode)

    def test_ARE_01_3_malformed_input_and_unknown_scope_fail_closed(self):
        self.assertEqual(2, self.run_hook("read-only", stdin="not json").returncode)
        self.assertEqual(2, self.run_hook("read-only", stdin=json.dumps({"tool_name": "Bash"})).returncode)
        self.assertEqual(2, self.run_hook("anything", stdin="{}").returncode)
        self.assertEqual(2, self.run_hook(stdin="{}").returncode)


if __name__ == "__main__":
    unittest.main()
