"""Subagent-scoped PreToolUse hook: keeps a role inside what its definition promises.

Usage (from the hooks block of an agent's frontmatter), with the hook JSON on stdin:
  python3 -I scripts/guard-agent-scope.py read-only       Bash may only collect evidence
  python3 -I scripts/guard-agent-scope.py documentation   Edit/Write only README.md, ARCHITECTURE.md and docs/
  python3 -I scripts/guard-agent-scope.py design          Edit/Write only ARCHITECTURE.md and docs/

The read-only scope is a guardrail against a reviewer changing what it reviews, not a security boundary: it refuses anything
it cannot read with certainty, and it runs in addition to guard-harness.py and the Bash sandbox. Every command is an allowlisted
program with an exact argument shape, because a list of forbidden flags can never be complete. Running tests writes build output
and executes project code; this hook limits what a reviewer asks for, not what the tests do.
Exit 0 allows, exit 2 refuses with the reason on stderr, and any failure refuses (fail closed).
"""

import json
import os
import re
import sys
from pathlib import Path

PROJECT = Path(__file__).resolve().parent.parent

READ_COMMANDS = {"ls", "cat", "head", "tail", "wc", "grep", "egrep", "fgrep", "cut", "cmp", "stat", "pwd", "which",
                 "basename", "dirname", "realpath", "echo", "true", "false", "test"}
LEADING_CD = re.compile(r"\s*cd\s+[\w./-]+\s*(&&|;)")
GIT_READ = {"status", "diff", "log", "show", "blame", "rev-parse", "ls-files", "check-ignore", "merge-base"}
GIT_FORBIDDEN_PREFIXES = ("--out", "--ext", "--text", "--no-index", "--open", "-O")
FIND_FORBIDDEN_PREFIXES = ("-delete", "-exec", "-ok", "-fprint", "-fls")
# No format tasks: scalafmt has to download itself, which the sandbox prevents, so format checks run outside it.
SBT_TASK = re.compile(r"^(test|compile|Test ?/ ?compile|(Test ?/ ?)?testOnly [\w.*\- ]+)$")
SED_RANGE = re.compile(r"^\d+(,\d+)?p$")
CHECK_SCRIPT = re.compile(r"^scripts/check-(skills|docs-current|spec-verdicts)\.py$")
UNITTEST_PATTERN = re.compile(r"^[\w*.]+$")
PROJECT_ENTRY_POINTS = {"scripts/check-local.sh", "scripts/check-done.sh"}
WRAPPER = "scripts/run-local-tests.sh"
WRAPPER_VERBS = {"start", "status", "test", "analytics", "interview", "retention", "recovery"}
HARMLESS_REDIRECT = re.compile(r"(\d?>\s*/dev/null|\d>&\d)(?=$|[\s;|&])")
ROLE_WRITE_PATHS = {
    "documentation": (("README.md", "ARCHITECTURE.md"), ("docs/",)),
    "design": (("ARCHITECTURE.md",), ("docs/",)),
}


class Unreadable(Exception):
    """The command uses shell syntax this scope refuses to interpret."""


def simple_commands(command):
    """Pure: the words of each simple command in one left-to-right pass. Anything that could hide what runs raises Unreadable.

    Quotes are honored only as plain grouping. Backslashes, expansion, redirection, subshells, function definitions, comments
    and newlines are all refused, so the shell cannot read this command differently from the way this scanner reads it."""
    commands, words, word, in_word, index = [], [], [], False, 0

    def end_word():
        nonlocal word, in_word
        if in_word:
            words.append("".join(word))
        word, in_word = [], False

    def end_command():
        nonlocal words
        end_word()
        if words:
            commands.append(words)
        words = []

    while index < len(command):
        char = command[index]
        if char == "'":
            closing = command.find("'", index + 1)
            if closing < 0:
                raise Unreadable("an unterminated quote")
            word.append(command[index + 1:closing])
            in_word, index = True, closing + 1
        elif char == '"':
            closing = command.find('"', index + 1)
            if closing < 0:
                raise Unreadable("an unterminated quote")
            inner = command[index + 1:closing]
            if re.search(r"[\\$`]", inner):
                raise Unreadable("expansion or a backslash inside double quotes")
            word.append(inner)
            in_word, index = True, closing + 1
        elif char in " \t":
            end_word()
            index += 1
        elif not in_word and (redirect := HARMLESS_REDIRECT.match(command, index)):
            index = redirect.end()
        elif char in ";|&":
            end_command()
            index += 1
        elif char in "\\$`<>(){}\n*?[]" or (char == "#" and not in_word):
            # An unquoted glob can expand to a planted file named like an option, such as -i or --output=x.
            raise Unreadable(f"the shell syntax {char!r}")
        else:
            word.append(char)
            in_word, index = True, index + 1
    end_command()
    return commands


def has_prefix(args, prefixes):
    return any(arg.startswith(prefixes) for arg in args)


def command_problem(words):
    """Pure: why one simple command is outside the evidence allowlist, or None."""
    name, args = words[0], words[1:]
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*=.*", name):
        return "inline environment assignments are not allowed"
    if name in READ_COMMANDS:
        return None
    if name == "find":
        return "find can run or write here" if has_prefix(args, FIND_FORBIDDEN_PREFIXES) else None
    if name == "sed":
        reading = (len(args) >= 3 and args[0] == "-n" and SED_RANGE.match(args[1])
                   and not any(arg.startswith("-") for arg in args[2:]))
        return None if reading else "only sed -n '<from>,<to>p' <files> reads without writing"
    if name == "git":
        if not args or args[0] not in GIT_READ:
            return "only read-only git subcommands, with no options before the subcommand"
        return "this git option can write or run a command" if has_prefix(args, GIT_FORBIDDEN_PREFIXES) else None
    if name == "sbt":
        return None if args and all(SBT_TASK.match(arg) for arg in args) else "only sbt test and compile tasks"
    if name in {"python3", "python"}:
        if len(args) == 2 and args[0] == "-I" and CHECK_SCRIPT.match(args[1]):
            return None
        if (len(args) == 8 and args[:6] == ["-I", "-m", "unittest", "discover", "-s", "scripts/tests"] and args[6] == "-p"
                and UNITTEST_PATTERN.match(args[7])):
            return None
        return "only the project check scripts and the script tests"
    if name in PROJECT_ENTRY_POINTS and not args:
        return None
    if name == WRAPPER:
        return None if len(args) >= 1 and args[0] in WRAPPER_VERBS else "only the non-destructive test wrapper verbs"
    return f"'{name}' with these arguments is not on the evidence allowlist"


RUNS_PROJECT_FILE = re.compile(r"^(python3?|scripts/.*)$")


def read_only_problem(command, cwd=None):
    """Pure: why this Bash command is outside what a reviewer may run, or None.

    A relative project script is trusted only from the project root, because another directory could hold a planted copy.
    sbt may also run in the analytics build, the only other build in this repository."""
    root = os.path.realpath(cwd or PROJECT)
    try:
        commands = simple_commands(command)
    except Unreadable as refused:
        return f"it uses {refused}"
    project = os.path.realpath(PROJECT)
    for position, words in enumerate(commands):
        if words[0] == "cd":
            # Only a leading cd that always takes effect: after a pipe or a condition it would not move this shell.
            if position != 0 or not LEADING_CD.match(command):
                return "cd must come first and be followed by && or ;"
            target = os.path.realpath(os.path.join(root, words[1])) if len(words) == 2 else ""
            if target not in {project, os.path.join(project, "analytics")}:
                return "cd may only move between the project root and analytics"
            root = target
            continue
        problem = command_problem(words)
        if problem:
            return problem
        if RUNS_PROJECT_FILE.match(words[0]) and root != project:
            return "project scripts run only from the project root"
        if words[0] == "sbt" and root not in {project, os.path.join(project, "analytics")}:
            return "sbt runs only in the project's two builds"
    return None


def write_path_problem(scope, target, cwd):
    """Pure apart from resolving symlinks: why this file is outside the role's write paths, or None."""
    files, prefixes = ROLE_WRITE_PATHS[scope]
    if not target:
        return "the tool call names no file"
    resolved = os.path.realpath(os.path.join(cwd, target))
    relative = os.path.relpath(resolved, os.path.realpath(PROJECT))
    if relative in files or relative.startswith(prefixes):
        return None
    return f"{target} is outside this role's write paths ({', '.join(files + prefixes)})"


def decide(scope, event):
    """The refusal reason for one hook event, or None to allow."""
    tool, tool_input = event["tool_name"], event.get("tool_input") or {}
    if scope == "read-only":
        return read_only_problem(tool_input["command"], event.get("cwd")) if tool == "Bash" else None
    if tool in {"Edit", "Write", "NotebookEdit", "MultiEdit"}:
        target = tool_input.get("file_path") or tool_input.get("notebook_path") or ""
        return write_path_problem(scope, target, event.get("cwd") or str(PROJECT))
    return None


def main():
    scope = sys.argv[1] if len(sys.argv) == 2 else ""
    if scope != "read-only" and scope not in ROLE_WRITE_PATHS:
        print(f"guard-agent-scope: unknown scope '{scope}'; refusing", file=sys.stderr)
        return 2
    problem = decide(scope, json.load(sys.stdin))
    if problem:
        print(f"Refused by the {scope} role scope: {problem}.", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:  # fail closed: a refusal is safe, an unchecked command is not
        print(f"guard-agent-scope failed closed: {error.__class__.__name__}: {error}", file=sys.stderr)
        sys.exit(2)
