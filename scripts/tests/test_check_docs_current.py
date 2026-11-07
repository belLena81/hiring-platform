"""Behaviour of the documentation currency gate. Run: python3 -I -m unittest discover -s scripts/tests -p 'test_*.py'"""

import importlib.util
import io
import json
import unittest
from pathlib import Path
from unittest import mock

SCRIPT = Path(__file__).resolve().parent.parent / "check-docs-current.py"
spec = importlib.util.spec_from_file_location("check_docs_current", SCRIPT)
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)


class DecisionAndDocumentationPaths(unittest.TestCase):
    def test_production_configuration_provisioning_build_and_workflow_paths_carry_decisions(self):
        for path in ("src/main/scala/A.scala", "src/main/resources/application.conf",
                     "analytics/src/main/resources/application.conf", "docker/kafka/provision-access.sh",
                     "compose.yaml", "compose.analytics-key-rotation.yaml", "build.sbt", "project/plugins.sbt",
                     "AGENTS.md", "CLAUDE.md", ".claude/skills/qa-engineer/SKILL.md", "scripts/check-local.sh"):
            self.assertTrue(gate.is_decision(path), path)

    def test_tests_script_tests_and_personal_settings_carry_no_decision(self):
        for path in ("src/test/scala/A.scala", "src/it/scala/A.scala", "scripts/tests/test_x.py",
                     ".claude/settings.local.json", "posts/x.txt", "docs/wiki/README.md"):
            self.assertFalse(gate.is_decision(path), path)

    def test_canonical_documentation_includes_wiki_but_not_specs_or_templates(self):
        self.assertTrue(gate.is_canonical_doc("README.md"))
        self.assertTrue(gate.is_canonical_doc("ARCHITECTURE.md"))
        self.assertTrue(gate.is_canonical_doc("docs/mongodb-design.md"))
        self.assertTrue(gate.is_canonical_doc("docs/wiki/security-and-operations.md"))
        self.assertFalse(gate.is_canonical_doc("docs/specs/duplicate-delivery-containment.md"))
        self.assertFalse(gate.is_canonical_doc("docs/templates/feature-spec.md"))


class Verdict(unittest.TestCase):
    code = {"src/main/scala/A.scala"}

    def test_a_decision_change_without_documentation_fails(self):
        self.assertIsNotNone(gate.verdict(self.code))

    def test_a_decision_change_with_canonical_documentation_passes(self):
        self.assertIsNone(gate.verdict(self.code | {"docs/wiki/workflows-and-patterns.md"}))

    def test_a_spec_alone_does_not_count_as_documentation(self):
        self.assertIsNotNone(gate.verdict(self.code | {"docs/specs/x.md"}))

    def test_a_trailer_with_a_reason_declares_the_impact(self):
        self.assertIsNone(gate.verdict(self.code, "Fix\n\nDocs-Impact: none - internal rename, no behavior change"))
        self.assertIsNone(gate.verdict(self.code, "Fix\n\nDocs-Impact: spec-only - recorded in docs/specs/x.md"))

    def test_a_trailer_without_a_real_reason_is_rejected(self):
        self.assertIsNotNone(gate.verdict(self.code, "Fix\n\nDocs-Impact: none - n/a"))
        self.assertIsNotNone(gate.verdict(self.code, "Fix\n\nDocs-Impact: later - will do it tomorrow"))

    def test_changes_without_decisions_pass(self):
        self.assertIsNone(gate.verdict({"src/test/scala/A.scala", "docs/specs/x.md"}))
        self.assertIsNone(gate.verdict(set()))


class CommitScope(unittest.TestCase):
    def test_a_plain_commit_takes_only_the_index(self):
        self.assertFalse(gate.commit_includes_unstaged("git commit -q -F - <<'EOF'"))
        self.assertFalse(gate.commit_includes_unstaged('git commit -m "Message with words"'))
        self.assertFalse(gate.commit_includes_unstaged("git commit --amend --no-edit"))

    def test_all_include_and_pathspec_commits_take_unstaged_changes(self):
        self.assertTrue(gate.commit_includes_unstaged("git commit -a -m x"))
        self.assertTrue(gate.commit_includes_unstaged("git commit -am x"))
        self.assertTrue(gate.commit_includes_unstaged("git commit -m x src/main/A.scala"))
        self.assertTrue(gate.commit_includes_unstaged("git commit -m x -- src/main/A.scala"))


class Hook(unittest.TestCase):
    def run_hook(self, command, staged, unstaged=frozenset(), new=frozenset(), commit_msg_hook=False):
        payload = json.dumps({"tool_name": "Bash", "tool_input": {"command": command}})

        def names(*args):
            return set(staged) if args == ("--cached",) else set(unstaged)

        out = io.StringIO()
        with mock.patch.object(gate, "names", side_effect=names), \
                mock.patch.object(gate, "untracked", return_value=set(new)), \
                mock.patch.object(gate, "merging", return_value=False), \
                mock.patch.object(gate, "commit_msg_hook_enabled", return_value=commit_msg_hook), \
                mock.patch("sys.stdin", io.StringIO(payload)), mock.patch("sys.stdout", out):
            self.assertEqual(gate.hook(), 0)
        return json.loads(out.getvalue()) if out.getvalue() else None

    def test_an_undocumented_decision_commit_is_denied_with_the_next_action(self):
        decision = self.run_hook("git commit -m 'Change lease'", {"src/main/A.scala"})
        output = decision["hookSpecificOutput"]
        self.assertEqual(output["permissionDecision"], "deny")
        self.assertIn("Docs-Impact", output["permissionDecisionReason"])

    def test_a_documented_commit_falls_through_to_the_normal_approval(self):
        self.assertIsNone(self.run_hook("git commit -m x", {"src/main/A.scala", "ARCHITECTURE.md"}))

    def test_a_heredoc_trailer_is_honoured(self):
        command = "git commit -q -F - <<'EOF'\nRename\n\nDocs-Impact: none - internal rename only\nEOF"
        self.assertIsNone(self.run_hook(command, {"src/main/A.scala"}))

    def test_unstaged_changes_count_only_when_the_commit_takes_them(self):
        self.assertIsNone(self.run_hook("git commit -m x", {"docs/specs/x.md"}, {"src/main/A.scala"}))
        self.assertIsNotNone(self.run_hook("git commit -am x", {"docs/specs/x.md"}, {"src/main/A.scala"}))

    def test_an_add_chained_before_the_commit_counts_what_it_may_stage(self):
        self.assertIsNotNone(self.run_hook("git add -A && git commit -m x", set(), {"src/main/A.scala"}))
        self.assertIsNotNone(self.run_hook("git add src && git commit -m x", set(), set(), {"src/main/B.scala"}))
        self.assertIsNone(self.run_hook("git add -A && git commit -m x", set(), {"src/main/A.scala", "README.md"}))

    def test_git_global_options_paths_and_quoted_shells_are_recognized(self):
        for command in ("git --no-pager commit -m x", "/usr/bin/git commit -m x", 'sh -c "git commit -m x"',
                        "git -C . commit -m x"):
            self.assertIsNotNone(self.run_hook(command, {"src/main/A.scala"}), command)

    def test_a_message_the_hook_cannot_see_is_left_to_the_enabled_commit_msg_hook(self):
        code = {"src/main/A.scala"}
        self.assertIsNone(self.run_hook("git commit --amend --no-edit", code, commit_msg_hook=True))
        self.assertIsNone(self.run_hook("git commit", code, commit_msg_hook=True))
        self.assertIsNotNone(self.run_hook("git commit --amend --no-edit", code, commit_msg_hook=False))
        self.assertIsNotNone(self.run_hook("git commit -am x", code, commit_msg_hook=True))
        # --no-verify skips the commit-msg hook, so nothing may be deferred to it.
        self.assertIsNotNone(self.run_hook("git commit --no-verify --amend --no-edit", code, commit_msg_hook=True))
        self.assertIsNotNone(self.run_hook("git commit -n", code, commit_msg_hook=True))

    def test_skipping_the_git_hooks_is_denied_even_for_a_documented_commit(self):
        for command in ("git commit --no-verify -m x", "git commit -n -m x", "git commit -anm x",
                        "git -c core.hooksPath=/dev/null commit -m x", "git commit -m x --no-verify"):
            decision = self.run_hook(command, {"README.md"})
            self.assertEqual("deny", decision["hookSpecificOutput"]["permissionDecision"], command)
            self.assertIn("skip the git hooks", decision["hookSpecificOutput"]["permissionDecisionReason"], command)

    def test_message_text_that_mentions_a_flag_is_not_a_hook_skip(self):
        for command in ("git commit -m 'use --no-verify never'", "git commit -m 'run -n later'", "git commit -mnone",
                        "git commit -F notes.msg"):
            self.assertIsNone(self.run_hook(command, {"README.md"}), command)

    def test_a_multi_line_message_or_heredoc_that_names_the_flags_is_not_a_hook_skip(self):
        for command in ('git commit -m "Title\n\nagents may not use --no-verify, -n or core.hooksPath"',
                        "git commit -q -F - <<'EOF'\nTitle\n\nDon't use --no-verify or -n\nEOF"):
            self.assertIsNone(self.run_hook(command, {"README.md"}), command)

    def test_a_message_file_outside_the_project_or_not_a_regular_file_is_ignored(self):
        self.assertEqual(gate.bounded_message_file("/etc/hostname"), "")
        self.assertEqual(gate.bounded_message_file("../outside.msg"), "")
        self.assertEqual(gate.bounded_message_file("docs"), "")

    def test_a_message_file_is_read_for_the_trailer(self):
        import tempfile
        # target/ is ignored and writable inside the Bash sandbox, where scripts/ is read-only.
        (gate.PROJECT / "target").mkdir(exist_ok=True)
        with tempfile.NamedTemporaryFile("w", dir=gate.PROJECT / "target", suffix=".msg", delete=False) as handle:
            handle.write("Rename\n\nDocs-Impact: none - internal rename only\n")
        relative = Path(handle.name).relative_to(gate.PROJECT)
        try:
            self.assertIsNone(self.run_hook(f"git commit -F {relative}", {"src/main/A.scala"}))
        finally:
            Path(handle.name).unlink()

    def test_other_commands_and_tools_are_ignored(self):
        self.assertIsNone(self.run_hook("git status", {"src/main/A.scala"}))
        self.assertIsNone(self.run_hook("echo git commit is mentioned", {"src/main/A.scala"}))


class UnpushedGatedCommits(unittest.TestCase):
    """Real git in a temporary repository: only commits made after the gate was introduced are judged."""

    def setUp(self):
        import subprocess
        import tempfile
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)

        def run(*args):
            subprocess.run(["git", *args], cwd=self.root, check=True, capture_output=True)

        def commit(path, message):
            target = self.root / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(message)
            run("add", path)
            run("-c", "core.hooksPath=/dev/null", "commit", "-q", "-m", message)

        run("init", "-q", "-b", "main")
        run("config", "user.email", "t@example.invalid")
        run("config", "user.name", "t")
        commit("README.md", "init")
        run("checkout", "-q", "-b", "work")
        commit("src/main/Old.scala", "undocumented change made before the gate existed")
        commit("scripts/check-docs-current.py", "introduce gate\n\nDocs-Impact: none - gate bootstrap commit")
        self.commit = commit
        self.patch = mock.patch.object(gate, "PROJECT", self.root)
        self.patch.start()

    def tearDown(self):
        self.patch.stop()
        self.directory.cleanup()

    def test_commits_before_the_gate_are_not_judged(self):
        self.assertEqual(gate.check(gate.unpushed_gated_commits()), [])

    def test_an_undocumented_commit_after_the_gate_is_found_even_without_hooks(self):
        self.commit("src/main/New.scala", "undocumented change after the gate")
        problems = gate.check(gate.unpushed_gated_commits())
        self.assertEqual(len(problems), 1)
        self.assertIn("src/main/New.scala", problems[0])

    def test_a_documented_or_declared_commit_after_the_gate_passes(self):
        self.commit("src/main/New.scala", "change\n\nDocs-Impact: spec-only - recorded in its spec only")
        self.assertEqual(gate.check(gate.unpushed_gated_commits()), [])


class CommitMessageHook(unittest.TestCase):
    def run_commit_msg(self, message, staged):
        import tempfile
        with tempfile.NamedTemporaryFile("w", suffix=".msg", delete=False) as handle:
            handle.write(message)
        err = io.StringIO()
        with mock.patch.object(gate, "names", return_value=set(staged)), \
                mock.patch.object(gate, "merging", return_value=False), \
                mock.patch("sys.stdout", io.StringIO()), mock.patch("sys.stderr", err):
            code = gate.commit_msg(Path(handle.name))
        Path(handle.name).unlink()
        return code, err.getvalue()

    def test_an_undocumented_decision_commit_is_rejected_with_the_next_action(self):
        code, err = self.run_commit_msg("Change lease\n", {"src/main/A.scala"})
        self.assertEqual(code, 1)
        self.assertIn("Docs-Impact", err)

    def test_a_trailer_in_the_message_allows_the_commit(self):
        code, _ = self.run_commit_msg("Rename\n\nDocs-Impact: none - internal rename only\n", {"src/main/A.scala"})
        self.assertEqual(code, 0)

    def test_a_trailer_only_in_git_comment_lines_does_not_count(self):
        code, _ = self.run_commit_msg("Rename\n# Docs-Impact: none - internal rename only\n", {"src/main/A.scala"})
        self.assertEqual(code, 1)

    def test_a_documented_commit_passes(self):
        code, _ = self.run_commit_msg("Change\n", {"src/main/A.scala", "docs/wiki/README.md"})
        self.assertEqual(code, 0)

    def test_a_merge_commit_is_not_checked(self):
        with mock.patch.object(gate, "merging", return_value=True), \
                mock.patch.object(gate, "names", side_effect=AssertionError("index must not be read")):
            self.assertEqual(gate.commit_msg(Path("/nonexistent")), 0)


class SandboxMounts(unittest.TestCase):
    def test_D7c_a_character_device_is_a_sandbox_mount_and_a_real_file_is_not(self):
        self.assertTrue(gate.is_sandbox_mount(Path("/dev/null")))
        self.assertFalse(gate.is_sandbox_mount(Path(__file__)))
        self.assertFalse(gate.is_sandbox_mount(Path("/nonexistent/quoted-name")))

    def test_D7c_an_empty_read_only_placeholder_is_a_sandbox_mount_and_a_writable_or_filled_file_is_not(self):
        import tempfile
        with tempfile.TemporaryDirectory() as folder:
            placeholder = Path(folder) / "placeholder"
            placeholder.touch()
            self.assertFalse(gate.is_sandbox_mount(placeholder))
            placeholder.chmod(0o444)
            self.assertTrue(gate.is_sandbox_mount(placeholder))
            filled = Path(folder) / "filled"
            filled.write_text("x")
            filled.chmod(0o444)
            self.assertFalse(gate.is_sandbox_mount(filled))


if __name__ == "__main__":
    unittest.main()
