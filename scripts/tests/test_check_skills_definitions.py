"""A role's definition must carry the enforcement its description promises.
Run: python3 -I -m unittest discover -s scripts/tests -p 'test_*.py'"""

import importlib.util
import json
import re
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
spec = importlib.util.spec_from_file_location("check_skills_definitions", ROOT / "scripts/check-skills.py")
skills = importlib.util.module_from_spec(spec)
spec.loader.exec_module(skills)

REVIEWER_AGENT = "---\nname: qa-engineer\nhooks:\n  PreToolUse:\n    - hooks:\n        - command: python3 -I scripts/guard-agent-scope.py read-only\n---\n"
AUTHOR_AGENT = "---\nname: scala-developer\nskills:\n  - scala-developer\n---\n"


class RoleDefinitions(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.skill = Path(self.folder.name) / "SKILL.md"

    def write_skill(self, *lines):
        self.skill.write_text("---\n" + "\n".join(("name: x", "description: y", *lines)) + "\n---\nBody\n")

    def test_ARE_03_1_a_reviewer_skill_must_fork_into_its_own_agent(self):
        self.write_skill("context: fork", "agent: qa-engineer")
        self.assertEqual([], skills.enforcement_problems("qa-engineer", REVIEWER_AGENT, self.skill))
        self.write_skill()
        self.assertEqual(1, len(skills.enforcement_problems("qa-engineer", REVIEWER_AGENT, self.skill)))
        self.write_skill("context: fork", "agent: general-purpose")
        self.assertEqual(1, len(skills.enforcement_problems("qa-engineer", REVIEWER_AGENT, self.skill)))

    def test_ARE_04_1_a_reviewer_must_declare_the_read_only_hook(self):
        self.write_skill("context: fork", "agent: qa-engineer")
        self.assertEqual(1, len(skills.enforcement_problems("qa-engineer", "---\nname: qa-engineer\n---\n", self.skill)))

    def test_ARE_04_1_an_author_must_preload_its_own_skill(self):
        self.write_skill()
        self.assertEqual([], skills.enforcement_problems("scala-developer", AUTHOR_AGENT, self.skill))
        self.assertEqual(1, len(skills.enforcement_problems("scala-developer", "---\nname: scala-developer\n---\n", self.skill)))
        other = AUTHOR_AGENT.replace("  - scala-developer", "  - data-engineer")
        self.assertEqual(1, len(skills.enforcement_problems("scala-developer", other, self.skill)))

    def test_ARE_04_1_the_writer_and_architect_must_declare_their_path_scope(self):
        self.write_skill()
        agent = "---\nname: technical-writer\nskills:\n  - technical-writer\n---\n"
        self.assertEqual(1, len(skills.enforcement_problems("technical-writer", agent, self.skill)))
        architect = "---\nname: software-architect\nskills:\n  - software-architect\n---\n"
        self.assertEqual(1, len(skills.enforcement_problems("software-architect", architect, self.skill)))

    def test_ARE_04_1_a_sentence_in_the_body_does_not_satisfy_a_check(self):
        self.write_skill("context: fork", "agent: qa-engineer")
        body = "---\nname: qa-engineer\n---\nRun scripts/guard-agent-scope.py read-only yourself.\n"
        self.assertEqual(1, len(skills.enforcement_problems("qa-engineer", body, self.skill)))


class AgentsSharingARoleSkill(unittest.TestCase):
    GOOD = "---\nname: scala-refactorer\ntools: Read, Edit\nmodel: opus\nskills:\n  - scala-developer\n---\n"

    def test_ARE_04_1_an_agent_sharing_a_skill_needs_tools_model_and_a_project_skill(self):
        roles = {"scala-developer"}
        name = "scala-refactorer"
        self.assertEqual([], skills.extra_agent_problems(name, self.GOOD, roles))
        self.assertEqual(1, len(skills.extra_agent_problems(name, self.GOOD.replace("tools: Read, Edit\n", ""), roles)))
        self.assertEqual(1, len(skills.extra_agent_problems(name, self.GOOD.replace("model: opus\n", ""), roles)))
        self.assertEqual(1, len(skills.extra_agent_problems(name, self.GOOD.replace("  - scala-developer", "  - unknown"), roles)))
        self.assertEqual(1, len(skills.extra_agent_problems(name, self.GOOD.replace("skills:\n  - scala-developer\n", ""), roles)))

    def test_ARE_04_1_another_file_cannot_declare_the_name_of_a_reviewer(self):
        impostor = self.GOOD.replace("name: scala-refactorer", "name: code-reviewer")
        self.assertEqual(1, len(skills.extra_agent_problems("scala-refactorer", impostor, {"scala-developer"})))


class ProjectSettings(unittest.TestCase):
    def setUp(self):
        self.settings = json.loads((ROOT / ".claude/settings.json").read_text())

    def test_ARE_08_1_every_pre_tool_use_hook_blocks_when_it_fails(self):
        handlers = [h for group in self.settings["hooks"]["PreToolUse"] for h in group["hooks"]]
        self.assertGreaterEqual(len(handlers), 2)
        for handler in handlers:
            self.assertEqual("block", handler.get("onFailure"), handler["command"])
            self.assertIn("|| exit 2", handler["command"])

    def test_ARE_08_1_every_role_scope_hook_fails_closed_on_any_exit(self):
        for agent in (ROOT / ".claude/agents").glob("*.md"):
            for line in agent.read_text().splitlines():
                if "guard-agent-scope.py" in line and "command:" in line:
                    self.assertIn("|| exit 2", line, agent.name)

    def test_the_sandbox_adds_a_boundary_without_removing_an_approval(self):
        sandbox = self.settings["sandbox"]
        self.assertTrue(sandbox["enabled"])
        self.assertTrue(sandbox["failIfUnavailable"])
        self.assertFalse(sandbox["autoAllowBashIfSandboxed"])
        self.assertFalse(sandbox["allowUnsandboxedCommands"])

    def test_sandbox_exclusions_are_named_entry_points_without_a_broad_wildcard(self):
        for command in self.settings["sandbox"]["excludedCommands"]:
            self.assertNotIn(command.split()[0], {"bash", "sh", "python3", "sbt", "docker", "*"}, command)

    def test_ARE_09_2_code_that_runs_outside_the_sandbox_cannot_be_written_from_inside_it(self):
        denied = self.settings["sandbox"]["filesystem"]["denyWrite"]
        for path in ("./scripts", "./.githooks", "./build.sbt", "./analytics/build.sbt", "./project/plugins.sbt",
                     "./analytics/project/plugins.sbt", "./.claude/rules", "./.sbtopts", "./compose.test.yaml",
                     "./.git", "./analytics/.sbtopts", "./analytics/.jvmopts",
                     "./.local/logs", "./.local/config", "./.local/data/test-builds"):
            self.assertIn(path, denied)

    def test_ARE_09_4_sessions_keep_sbt_state_in_the_project_and_cannot_write_the_shared_home(self):
        options = self.settings["env"]["SBT_OPTS"]
        for option in ("sbt.global.base", "sbt.boot.directory", "sbt.ivy.home", "sbt.coursier.home"):
            self.assertIn(f"-D{option}=.sbt-home/", options)
        filesystem = self.settings["sandbox"]["filesystem"]
        for path in filesystem["allowWrite"]:
            self.assertFalse(path.startswith("~"), path)
        for path in ("~/.sbt", "~/.ivy2", "~/.cache/coursier"):
            self.assertIn(path, filesystem["denyRead"])
        self.assertIn(".sbt-home/", (ROOT / ".gitignore").read_text())
        self.assertIn(".sbt-home", (ROOT / ".dockerignore").read_text())

    def test_ARE_09_2_every_compose_file_is_denied_because_the_wrapper_runs_compose_outside_the_sandbox(self):
        denied = self.settings["sandbox"]["filesystem"]["denyWrite"]
        for compose in ROOT.glob("compose*.yaml"):
            self.assertIn(f"./{compose.name}", denied)

    def test_ARE_09_11_common_env_file_names_are_denied_by_name_because_the_sandbox_ignores_globs_on_linux(self):
        denied = self.settings["sandbox"]["filesystem"]["denyRead"]
        for name in ("local", "development", "test", "production"):
            self.assertIn(f"./.env.{name}", denied)
        self.assertNotIn("./.env.example", denied)

    def test_ARE_07_1_the_review_log_hook_is_wired_for_every_reviewer_and_never_blocks(self):
        hooks = self.settings["hooks"]
        recorder = "scripts/record-review-run.py"

        def groups(event):
            return [g for g in hooks[event] if any(recorder in h["command"] for h in g["hooks"])]

        stops, handbacks = groups("SubagentStop"), groups("PostToolUse")
        self.assertEqual(1, len(stops))
        self.assertEqual({"code-reviewer", "security-engineer", "qa-engineer"}, set(stops[0]["matcher"].split("|")))
        self.assertEqual(1, len(handbacks))
        self.assertEqual("SubagentHandback", handbacks[0]["matcher"])
        for group in stops + handbacks:
            for handler in group["hooks"]:
                self.assertNotIn("exit 2", handler["command"])
                self.assertNotEqual("block", handler.get("onFailure"))

    def test_ARE_09_5_no_entry_point_outside_the_sandbox_starts_python_without_isolation(self):
        # Without -I, python3 reads a stdin or -c program with the working directory first on its path, so a module planted
        # in the writable repository root would run as the user, outside the sandbox.
        for script in ("scripts/run-local-tests.sh", "scripts/check-local.sh", "scripts/check-done.sh", "scripts/format-staged.sh",
                       "scripts/operator-lib.sh", "scripts/check-agent-hooks-live.sh", ".githooks/commit-msg", ".githooks/pre-commit"):
            for number, line in enumerate((ROOT / script).read_text().splitlines(), 1):
                # Every real python3 call has -I as its first argument. The tool lists name it among other words.
                if re.search(r"\bpython3?\b", line) and not re.match(r"\s*(#|for tool in)", line):
                    self.assertRegex(line, r"\bpython3? -I\b", f"{script}:{number}")

    def test_ARE_09_5_hook_commands_in_settings_and_agents_start_python_with_isolation(self):
        commands = [h["command"] for groups in self.settings["hooks"].values() for g in groups for h in g["hooks"]]
        for agent in (ROOT / ".claude/agents").glob("*.md"):
            commands += [line for line in agent.read_text().splitlines() if "command:" in line]
        for command in commands:
            if "python3" in command:
                self.assertIn("python3 -I", command)

    def test_ARE_09_6_the_16_listed_git_commands_run_outside_the_sandbox(self):
        excluded = self.settings["sandbox"]["excludedCommands"]
        for command in ("add", "commit", "reset", "checkout", "restore", "switch", "stash", "merge", "rebase", "rm", "mv",
                        "tag", "branch", "push", "fetch", "pull"):
            self.assertIn(f"git {command} *", excluded)
        self.assertEqual(16, sum(1 for pattern in excluded if pattern.startswith("git ")))

    def test_ARE_09_9_no_command_outside_the_sandbox_runs_without_a_prompt_and_skills_run_no_shell(self):
        allow = " ".join(self.settings["permissions"]["allow"])
        for pattern in self.settings["sandbox"]["excludedCommands"]:
            self.assertNotIn(pattern.removesuffix(" *"), allow, pattern)
        self.assertIs(True, self.settings["disableSkillShellExecution"])
        for skill in (ROOT / ".claude/skills").glob("*/SKILL.md"):
            self.assertNotRegex(skill.read_text(), r"(^|\s)!`|^```!", skill.parent.name)

    def test_ARE_09_6_every_excluded_command_has_a_prefix_rule_that_needs_approval(self):
        ask = self.settings["permissions"]["ask"]
        allow = self.settings["permissions"]["allow"]
        for pattern in self.settings["sandbox"]["excludedCommands"]:
            rule = f"Bash({pattern.removesuffix(' *')}:*)"
            self.assertIn(rule, ask, pattern)
            self.assertNotIn(rule, allow, pattern)


if __name__ == "__main__":
    unittest.main()
