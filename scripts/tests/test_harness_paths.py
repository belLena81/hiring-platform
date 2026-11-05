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

    def test_ARE_09_7_a_redirect_does_not_hide_a_command_from_the_root_check(self):
        for command in ("2>&1 scripts/run-local-tests.sh status",
                        "scripts/run-local-tests.sh>/dev/null status",
                        "2>/dev/null scripts/check-local.sh",
                        "> /dev/null scripts/check-local.sh",
                        "2>&1 git commit -m x",
                        "git 2>&1 commit -m x",
                        "git >/dev/null commit -m x",
                        ">|/dev/null git commit -m x",
                        "<> /dev/null git commit -m x",
                        "{fd}>/dev/null git commit -m x",
                        "\"git\" commit -m x",
                        "g\\it commit -m x"):
            outcome = guard.check_bash(command, harness_paths_root() / "analytics")
            self.assertEqual("deny", outcome and outcome[0], command)
            self.assertIn("project root", outcome[1], command)

    def test_ARE_09_7_a_directory_change_anywhere_in_the_call_is_denied_at_the_root(self):
        for command in ("2>&1 cd analytics && git commit -m x",
                        "FOO=1 cd analytics && git commit -m x",
                        "builtin cd analytics && git commit -m x",
                        "cd analytics; scripts/check-local.sh",
                        "true; cd docs; scripts/run-local-tests.sh status",
                        "pushd analytics; git push",
                        "popd; git push",
                        "pushd;git push",
                        "cd>/dev/null analytics && git commit -m x",
                        "cd</dev/null analytics && git commit -m x",
                        "pushd>/dev/null analytics; git commit -m x",
                        "\"cd\" analytics && git commit -m x",
                        "  \"cd\" analytics && git commit -m x",
                        "true\n\"cd\" analytics\ngit commit -m x",
                        "{ \"cd\" analytics; git commit -m x; }",
                        "A=1 \"cd\" analytics && git commit -m x",
                        " \\cd analytics && git commit -m x",
                        "if \"cd\" analytics; then git commit -m x; fi",
                        "echo \\' ; cd>/dev/null analytics ; git commit -m x ; echo \\'",
                        "scripts/run-local-tests.sh status # it's\ncd sub\nscripts/run-local-tests.sh status # it's",
                        "git commit -m a # it's\ncd sub\ngit commit -m b # it's",
                        "true # it's\ncd analytics # it's\ngit commit -m x",
                        "! case x in x)cd analytics;;y)esac; git commit -m m",
                        "shopt -s expand_aliases\nalias x=cd\nx analytics\ngit commit -m m",
                        "alias x=cd; git commit -m m",
                        "c[d] analytics; git commit -m m",
                        "cd analytics && git commit -m x <(true)",
                        "cd analytics && git commit -m x # >(",
                        "git commit -m x # >(",
                        "$(true)cd analytics; git commit -m x",
                        "${x}cd analytics; git commit -m x",
                        "{cd,analytics}; git commit -m x",
                        "$'\\143d' analytics; git commit -m x",
                        "cd$IFS\"analytics\"; git commit -m x",
                        "2>&1 scripts/run-local-tests.sh stop",
                        "scripts/run-local-tests.sh>/dev/null status",
                        "git 2>&1 commit -m x"):
            outcome = guard.check_bash(command, harness_paths_root())
            self.assertEqual("deny", outcome and outcome[0], command)
            self.assertIn("project root", outcome[1], command)

    def test_ARE_09_7_the_reported_line_continuation_forms_do_not_hide_a_command(self):
        root = harness_paths_root()
        for command, cwd in (("# x\\\ngit commit -m m", root / "analytics"),
                             ("# a\\\ncd analytics # b\\\ngit commit -m m", root),
                             ("git \\\ncommit -m x", root),
                             ("cd analytics #a\\\ngit \\\ncommit -m m", root),
                             ("#a\\\ngit \\\ncommit -m m", root / "analytics")):
            outcome = guard.check_bash(command, cwd)
            self.assertEqual("deny", outcome and outcome[0], command)

    def test_ARE_09_7_a_comment_after_an_escaped_backslash_hides_nothing_from_the_other_rules(self):
        root = harness_paths_root()
        for command in ("echo \\\\ # it's\ncp a /outside/dir\n# it's",
                        "echo \\\\ # it's\ncp a /outside/dir # it's",
                        "echo \\\\ # it's\nsudo id\n# it's",
                        "echo \\\\;# it's\nsudo id # it's",
                        "echo \\\\\\\\ # it's\nsudo id\n# it's"):
            outcome = guard.check_bash(command, root)
            self.assertEqual("deny", outcome and outcome[0], command)

    def test_ARE_09_7_a_comment_at_the_start_of_a_call_hides_nothing_from_the_other_rules(self):
        for command in ("# it's\nsudo id\n# it's", "# its\nsudo id"):
            outcome = guard.check_bash(command, harness_paths_root())
            self.assertEqual("deny", outcome and outcome[0], command)

    def test_ARE_09_7_a_continuation_ending_a_comment_hides_nothing_from_the_other_rules(self):
        for command in ("echo \\\\ #x;\\\nsudo id", "echo a #x;\\\nsudo id", "echo \\\\ #x;\\\ncp a /outside/dir", "# a\\\nsudo id"):
            outcome = guard.check_bash(command, harness_paths_root())
            self.assertEqual("deny", outcome and outcome[0], command)

    def test_ARE_09_7_a_legitimate_continuation_keeps_its_answer(self):
        root = harness_paths_root()
        self.assertIsNone(guard.check_bash("git log \\\n  --oneline -5", root))
        self.assertIsNone(guard.check_bash("grep -rn foo \\\n  src/main", root))
        self.assertIsNone(guard.check_bash("rg foo \\\n  src/main", root))
        self.assertIsNone(guard.check_bash("cd .. && ls \\\n  -l", root / "analytics"))

    def test_ARE_09_7_a_quoted_hash_before_a_continuation_over_asks_as_a_recorded_limit(self):
        outcome = guard.check_bash("grep -rn '#x' \\\n  src/main", harness_paths_root())
        self.assertEqual("ask", outcome and outcome[0])

    def test_ARE_09_7_the_second_reading_starts_from_the_call_s_own_directory(self):
        outcome = guard.check_bash("cd docs #x;\\\ncp a ../../outside", harness_paths_root())
        self.assertEqual("deny", outcome and outcome[0])

    def test_ARE_09_7_recorded_limits_stay_an_ask(self):
        for command in ("echo `echo \\\\ #x; sudo id`", "echo $'\\'\\\\ #''; sudo id", "(( 1 \\\\ # )); sudo id", "(( 1 # )); sudo id"):
            outcome = guard.check_bash(command, harness_paths_root())
            self.assertEqual("ask", outcome and outcome[0], command)

    def test_ARE_09_3_a_script_path_as_the_exclusion_writes_it_asks(self):
        root = harness_paths_root()
        for command in ("scripts/run-local-tests.sh status", "scripts/run-local-tests.sh start", "scripts/run-local-tests.sh --help",
                        "scripts/run-local-tests.sh -h", "scripts/run-local-tests.sh  retention", "scripts/run-local-tests.sh 'test'",
                        "scripts/run-local-tests.sh  'analytics'", "scripts/check-local.sh", "scripts/check-local.sh  ",
                        "scripts/check-agent-hooks-live.sh"):
            outcome = guard.check_bash(command, root)
            self.assertEqual("ask", outcome and outcome[0], command)

    def test_ARE_09_3_a_command_prefix_the_exclusion_strips_asks(self):
        root = harness_paths_root()
        for prefix in ("gnohup", "gtime", "gtimeout 5", "gnice", "gstdbuf -oL", "nohup", "time", "timeout 5", "nice", "stdbuf -oL"):
            for command in ("scripts/check-local.sh", "scripts/run-local-tests.sh status", "git commit -m x", "git push origin main"):
                outcome = guard.check_bash(f"{prefix} {command}", root)
                self.assertEqual("ask", outcome and outcome[0], f"{prefix} {command}")

    def test_ARE_09_3_every_excluded_script_has_a_prefix_ask_rule(self):
        settings = json.loads((harness_paths_root() / ".claude/settings.json").read_text())
        ask = settings["permissions"]["ask"]
        for script in guard.SANDBOX_EXCLUDED_SCRIPTS:
            self.assertIn(f"Bash({script}:*)", ask)
            self.assertEqual([f"Bash({script}:*)"], [rule for rule in ask if script in rule])

    def test_ARE_09_10_monitor_is_denied_because_the_guard_does_not_see_it(self):
        settings = json.loads((harness_paths_root() / ".claude/settings.json").read_text())
        self.assertIn("Monitor", settings["permissions"]["deny"])

    def test_ARE_09_8_only_a_role_skill_at_the_project_root_passes_without_a_prompt(self):
        for skill in guard.os.listdir(harness_paths_root() / ".claude/skills"):
            self.assertIsNone(guard.skill_problem(skill), skill)
        self.assertIsNone(guard.skill_problem(" code-reviewer "))
        for skill in ("probe", " probe", "x:probe", "a:b", "x/y:code-reviewer", "/code-reviewer", "two words", "aws-core:aws-iam",
                      "simplify", "../code-reviewer", ""):
            outcome = guard.skill_problem(skill)
            self.assertEqual("ask", outcome and outcome[0], skill)

    def test_ARE_09_8_the_guard_hook_sees_the_skill_tool(self):
        settings = json.loads((harness_paths_root() / ".claude/settings.json").read_text())
        matchers = [group["matcher"] for group in settings["hooks"]["PreToolUse"]
                    if any("guard-harness.py" in handler["command"] for handler in group["hooks"])]
        self.assertTrue(any("Skill" in matcher.split("|") for matcher in matchers))

    def test_split_segments_tells_an_escaped_hash_position_from_a_real_comment(self):
        self.assertEqual(["", "sudo id"], guard.split_segments("#a\nsudo id"))
        self.assertEqual(["echo \\\\\\ #a", "sudo id"], guard.split_segments("echo \\\\\\ #a\nsudo id"))
        self.assertEqual(["echo \\ #a", "sudo id"], guard.split_segments("echo \\ #a\nsudo id"))
        self.assertEqual(["echo \\\\ ", "sudo id"], guard.split_segments("echo \\\\ #a\nsudo id"))
        self.assertEqual(["echo \\;#a", "sudo id"], guard.split_segments("echo \\;#a\nsudo id"))

    def test_ARE_09_7_a_hash_inside_a_word_hides_nothing_from_the_other_rules(self):
        root = harness_paths_root()
        for command in ("echo \\)#; sudo ls", "echo \\)#x; sudo id", "echo \\)#; cp a /outside/dir", "echo $(true)#; sudo ls",
                        "echo $(true)#x; sudo id", "echo \\;#; sudo ls"):
            outcome = guard.check_bash(command, root)
            self.assertEqual("deny", outcome and outcome[0], command)
        outcome = guard.check_bash("echo \\)#x; 2>/dev/null git status", root)
        self.assertEqual("ask", outcome and outcome[0])

    def test_ARE_09_7_this_rule_does_not_examine_words_in_a_quoted_message(self):
        for command in ("git commit -m \"fix cd handling\"", "git commit -m \"fix; cd handling\"",
                        "git commit -m \"fix (cd handling)\"", "git commit -m 'a > b; cd c'",
                        "git commit -m \"subject\" -m \"body\nCo-Authored-By: A <a@example.invalid>\""):
            outcome = guard.check_bash(command, harness_paths_root())
            self.assertNotIn("project root", (outcome or ("", ""))[1], command)

    def test_ARE_09_7_expansion_in_a_double_quoted_message_is_not_one_simple_command(self):
        for command in ("git commit -m \"$(touch x)\"", "git commit -m \"`touch x`\"", "git commit -m \"a\\\"; touch x; \\\"\""):
            outcome = guard.check_bash(command, harness_paths_root())
            self.assertEqual("deny", outcome and outcome[0], command)

    def test_ARE_09_7_one_simple_command_is_an_allowlist(self):
        for command in ("git commit -m x", "scripts/run-local-tests.sh status", "git add a/b.md c.txt", "git commit -m 'a; b' -m \"c\""):
            self.assertTrue(guard.is_one_simple_command(command), command)
        for command in ("a; b", "a && b", "a | b", "a & b", "a > b", "a < b", "(a)", "{ a; }", "a $b", "a `b`", "a *", "a ?", "a [b]",
                        "a # b", "a\\ b", "a\nb", "!a", "a 'b", "a \"b", "a \"$b\""):
            self.assertFalse(guard.is_one_simple_command(command), command)

    def test_ARE_09_7_an_escaped_or_commented_quote_does_not_hide_a_redirect(self):
        for command in ("echo \\' ; 2>/dev/null git status ; echo \\'",
                        "true # it's\n2>/dev/null git status # it's",
                        "(true)# it's\n2>/dev/null git status # it's",
                        "(true)#'\n2>/dev/null git status\n#'",
                        "A=\\' >/dev/null true \\'"):
            outcome = guard.check_bash(command, harness_paths_root())
            self.assertEqual("ask", outcome and outcome[0], command)

    def test_ARE_09_7_shell_quoting_that_survives_the_text_scan_needs_approval(self):
        for command in ("git com$\"m\"it -m x", "git com$'m'it -m x", "g$\"i\"t commit", "git com$1mit -m x", "git com$@mit -m x"):
            for cwd in (harness_paths_root(), harness_paths_root() / "analytics"):
                outcome = guard.check_bash(command, cwd)
                self.assertIn(outcome and outcome[0], ("deny", "ask"), command)

    def test_ARE_09_7_a_long_redirect_chain_does_not_stall_the_guard(self):
        for link in (">a", ">&a", ">11"):
            for command in ("git status " + link * 2000 + " x", "git status " + (link + " ") * 2000 + "x"):
                outcome = guard.check_bash(command, harness_paths_root())
                self.assertEqual("ask", outcome and outcome[0], link)
        # Here & separates commands, so each part is a redirect alone, not one before the end of a command. It must only finish.
        guard.check_bash("git status " + ">a&" * 2000 + " x", harness_paths_root())

    def test_ARE_09_7_a_redirect_before_the_end_of_a_command_needs_approval(self):
        for command in ("2>/dev/null git status",
                        ">|/dev/null git status",
                        "<> /dev/null git status",
                        "{fd}>/dev/null git status"):
            outcome = guard.check_bash(command, harness_paths_root())
            self.assertEqual("ask", outcome and outcome[0], command)

    def test_ARE_09_7_a_redirect_at_the_end_and_quoted_text_stay_as_they_were(self):
        root = harness_paths_root()
        for command in ("git status 2>&1",
                        "git status >/dev/null 2>&1",
                        "git log --grep='>' -5",
                        "git log --grep=\"a > b\" -5",
                        "git log --format='%h' -5",
                        "git status 2>/dev/null | head -5",
                        "grep -n '=>' src/A.scala",
                        "sbt test 2>&1",
                        "cat a b > /dev/null"):
            self.assertIsNone(guard.check_bash(command, root), command)

    def test_ARE_09_7_a_quoted_redirect_character_does_not_hide_an_argument_from_any_rule(self):
        root = harness_paths_root()
        for command, kind in (("git log --grep='>' --output=notes.txt", "deny"),
                              ("git log --grep='<' --output=notes.txt", "deny"),
                              ("git diff '>' --ext-diff", "ask"),
                              ("find . ! -name '>' -exec true ;", "ask"),
                              ("cp a '>' /outside/dir", "deny")):
            outcome = guard.check_bash(command, root)
            self.assertEqual(kind, outcome and outcome[0], command)

    def test_ARE_09_7_a_cd_earlier_in_the_call_counts(self):
        outcome = guard.check_bash("cd analytics && git commit -m x", harness_paths_root())
        self.assertEqual("deny", outcome and outcome[0])
        self.assertIn("project root", outcome[1])

    def test_ARE_09_7_a_command_wrapper_needs_approval(self):
        for command in ("stdbuf -oL scripts/run-local-tests.sh status", "timeout 5 scripts/run-local-tests.sh status"):
            outcome = guard.check_bash(command, harness_paths_root())
            self.assertEqual("ask", outcome and outcome[0], command)

    def test_ARE_09_7_read_only_git_stays_allowed_from_a_subdirectory(self):
        self.assertIsNone(guard.check_bash("git status", harness_paths_root() / "analytics"))

    def test_ARE_09_7_the_guard_and_the_settings_name_the_same_exclusions(self):
        settings = json.loads((harness_paths_root() / ".claude/settings.json").read_text())
        patterns = [pattern.removesuffix(" *").split() for pattern in settings["sandbox"]["excludedCommands"]]
        self.assertEqual({words[0] for words in patterns if words[0] != "git"}, guard.SANDBOX_EXCLUDED_SCRIPTS)
        self.assertEqual({words[1] for words in patterns if words[0] == "git"}, guard.SANDBOX_EXCLUDED_GIT)

    def test_the_docs_gate_requires_documentation_for_every_shared_path(self):
        for path in ALL_WORKFLOW_PATHS:
            self.assertTrue(docs_gate.is_decision(path), path)

    def test_ordinary_files_are_not_workflow_files(self):
        for path in ("src/main/scala/A.scala", "docs/api.md", "README.md", "src/test/scala/A.scala"):
            self.assertFalse(harness_paths.is_workflow_file(path), path)


if __name__ == "__main__":
    unittest.main()