"""PreToolUse guard: keep agent actions on the isolated test stack and inside the project.

Policy (a guardrail on top of permission rules, not an OS sandbox):
  * deny  - anything that can reach a non-test database/broker, read secrets, or write outside the project;
  * ask   - every cleanup/destructive action and every change to harness/build files, so a human approves it;
  * allow - fall through to the permission rules in .claude/settings.json.
Reads JSON from stdin (Claude Code hook protocol); prints a decision object when it has one.
Any internal error exits 2, which blocks the tool call (fail closed).
"""

import glob
import json
import os
import re
import shlex
import sys
from pathlib import Path

PROJECT = Path(__file__).resolve().parent.parent
MANIFEST = PROJECT / ".local" / "config" / "test-services" / "manifest.json"
WRAPPER = "scripts/run-local-tests.sh"
WRAPPER_VERBS = {"start", "test", "analytics", "interview", "retention", "recovery", "status", "stop"}

WRITE_COMMANDS = {"rm", "rmdir", "unlink", "shred", "mv", "cp", "tee", "touch", "mkdir", "chmod", "chown", "ln", "dd",
                  "install", "truncate", "rsync", "curl", "wget", "tar", "unzip", "patch", "split", "csplit"}
CLEANUP_COMMANDS = {"rm", "rmdir", "unlink", "shred", "truncate"}
DOCKER_CLEANUP = {"rm", "rmi", "kill", "stop", "down", "prune", "volume", "system", "network", "container", "image",
                  "builder", "restart", "pause"}
DIRECT_DB_TOOLS = {"mongosh", "mongo", "mongodump", "mongorestore", "mongoexport", "mongoimport", "mongod", "redis-cli",
                   "psql", "kafka-topics.sh", "kafka-acls.sh", "kafka-consumer-groups.sh", "kafka-configs.sh",
                   "nc", "ncat", "netcat", "socat", "telnet", "kcat", "kafkacat", "busybox", "podman", "nerdctl"}
SHELL_WRAPPERS = {"bash", "sh", "zsh", "dash", "eval", "env", "command", "xargs", "exec", "nohup", "time", "timeout",
                  "nice", "setsid", "script", "watch", "python", "python3", "perl", "ruby", "node", "npx", "npm",
                  "awk", "gawk", "source", "."}
SHELL_KEYWORDS = {"if", "then", "else", "elif", "fi", "do", "done", "while", "until", "for", "!", "{", "}", "(", ")"}
SEGMENT_SPLIT = re.compile(r"\|\||&&|;|\||\n|&(?!>)")
DB_URI = re.compile(r"mongodb(?:\+srv)?://[^\s'\"]+", re.IGNORECASE)

PROTECTED_PREFIXES = (".claude/", ".git/", "scripts/", "project/", "analytics/project/", ".local/config/", ".codex/")
PROTECTED_FILES = {"build.sbt", "analytics/build.sbt", "compose.yaml", "compose.test.yaml", "compose.cloud.yaml",
                   "compose.atlas-local.yaml", "compose.override.yaml", "CLAUDE.md", "AGENTS.md", ".sbtopts", ".jvmopts",
                   "analytics/.sbtopts", "analytics/.jvmopts", ".mcp.json", ".scalafmt.conf", "analytics/.scalafmt.conf"}
SECRET_TEXT = re.compile(
    r"(^|[\s/='\"<>@:])\.env(?!\.(example|sample|template)(\s|$|'|\"))(\.[\w.-]+)?($|[\s/'\")};`])|\.local/config|(^|[\s/='\"<>@])\.(aws|ssh|codex|gnupg)(/|$|\s)|"
    r"\.docker/config|\.config/gh|/proc/[^\s]*/environ|(^|[\s;&|])(printenv|compgen)\b|test-services|compose\.env|\.netrc|\.kube|\.npmrc|\.git-credentials|\.sbt/credentials|\.m2/settings|gcloud",
    re.IGNORECASE)
SECRET_PATH = re.compile(
    r"(^|/)(\.env(?!\.(example|sample|template)$)(\..*)?|\.aws|\.ssh|\.codex|\.gnupg|\.kube|\.netrc|\.npmrc|\.pgpass|\.my\.cnf|\.git-credentials|"
    r"gcloud|manifest\.json|compose\.env)(/|$)|\.local/config|\.sbt/credentials|\.m2/settings|\.docker/config|"
    r"\.config/gh|/proc/[^/]*/environ|(^|/)\.?credentials|"
    r"(^|/)(\.claude\.json|\.bash_history|\.zsh_history|\.gitconfig|\.pki|\.erlang\.cookie|\.pypirc|\.azure|\.oci|"
    r"\.yarnrc|\.mongorc\.js|\.mongodb|\.cert|rclone\.conf|pip\.conf)(/|$)|^/(home|Users)/[^/]+/\.claude/")
SECRET_SAFE_COMMANDS = {"ls", "stat", "test", "["}
UNSAFE_ENV = re.compile(
    r"^(DOCKER_|COMPOSE_|HIRING_TEST_|JAVA_HOME|MONGODB|KAFKA|SBT_|JAVA_TOOL|JDK_JAVA|JAVA_OPTS|LD_|GIT_|HOME=|PATH=|BASH_ENV|"
    r"ENV=|PYTHON|NODE_|PROMPT_COMMAND|IFS=|INTERVIEW_|ANALYTICS_|HIRING_|CLASSPATH|_JAVA|JAVA|ATLAS_|SPARK_|TESTCONTAINERS)")


def scratch_roots():
    roots = [PROJECT]
    scratch = os.environ.get("CLAUDE_CODE_TMPDIR") or ""
    if scratch:
        roots.append(Path(scratch).resolve())
    roots.append(Path("/tmp") / f"claude-{os.getuid()}")
    return roots


def clean(path_text):
    """Strip shell punctuation that is not part of the path itself."""
    return path_text.strip().lstrip("&<>|").strip("'\"").rstrip(")}")


def resolve(path_text, cwd):
    path = Path(os.path.expanduser(path_text))
    if not path.is_absolute():
        path = cwd / path
    return Path(os.path.realpath(path))


def inside_allowed(path_text, cwd):
    resolved = resolve(path_text, cwd)
    return any(resolved == root or root in resolved.parents for root in scratch_roots())


def is_protected(rel):
    base = os.path.basename(rel)
    return (rel in PROTECTED_FILES or rel in {"scripts", ".claude", ".git", "project", ".codex"} or rel.startswith(PROTECTED_PREFIXES) or base.endswith(".sbt")
            or base.startswith(".env") or base in {".sbtopts", ".jvmopts"})


def classify_write(path_text, cwd):
    """deny if the write escapes the project, ask if it hits a protected file, else None."""
    path_text = clean(path_text)
    if not path_text or path_text == "/dev/null":
        return None
    if re.search(r"[$`]", path_text):
        return "deny", f"Non-literal write target cannot be verified: {path_text}"
    if not inside_allowed(path_text, cwd):
        return "deny", f"Write target is outside the project folder: {path_text}"
    rel = os.path.relpath(resolve(path_text, cwd), PROJECT)
    if not rel.startswith("..") and is_protected(rel):
        return "ask", f"{rel} controls the test harness, build or guard: human approval required."
    return None


def manifest_mongo_port():
    try:
        uri = json.loads(MANIFEST.read_text())["mongoUri"]
        return re.match(r"mongodb://127\.0\.0\.1:(\d+)/", uri).group(1)
    except (OSError, KeyError, ValueError, AttributeError):
        return None


def decide(kind, reason):
    return {"hookSpecificOutput": {"hookEventName": "PreToolUse", "permissionDecision": kind,
                                   "permissionDecisionReason": reason}}


def worst(*outcomes):
    outcomes = [o for o in outcomes if o]
    for kind in ("deny", "ask"):
        for outcome in outcomes:
            if outcome[0] == kind:
                return outcome
    return None


def candidate_paths(arg):
    """Yield every substring of an argument that may be a filesystem path (attached options, key=value)."""
    values = [arg]
    if "=" in arg:
        values.append(arg.split("=", 1)[1])
    if re.match(r"^-[A-Za-z]", arg) and len(arg) > 2:
        values.append(arg[2:])
    return values


def check_segment(tokens, cwd):
    """Return (kind, reason) for the first violated rule in one simple command, else None."""
    if tokens and (tokens[0] in {"case", "coproc", "function", "select", "esac"} or tokens[0].endswith("()")
                   or re.fullmatch(r"\(?[^\s()$`]+\)", tokens[0]) or "()" in tokens[:2]):
        return "ask", "case/function/coproc constructs hide the command name from the guard: human approval required."
    while tokens and (tokens[0] in SHELL_KEYWORDS or re.fullmatch(r"[({!]+", tokens[0])):
        tokens = tokens[1:]
    if tokens:
        tokens[0] = tokens[0].lstrip("({!")
    idx = 0
    while idx < len(tokens) and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*=.*", tokens[idx]):
        if UNSAFE_ENV.match(tokens[idx]):
            return "deny", f"Overriding {tokens[idx].split('=')[0]} could redirect tests, tools or the shell."
        idx += 1
    assigned = idx > 0
    tokens = tokens[idx:]
    if not tokens:
        return None
    if assigned and (os.path.basename(tokens[0]) in {"sbt", "git", "docker", "docker-compose", "java"} or tokens[0] == WRAPPER):
        return "deny", "Inline environment assignments before test, build, git or docker commands are not allowed."
    if re.search(r"[$`]", tokens[0]):
        return "ask", "Command name is built dynamically: human approval required."
    command = os.path.basename(tokens[0])
    args = [a.rstrip(")}") if a.endswith((")", "}")) and not a.startswith(("'", '"')) else a for a in tokens[1:]]

    if command in {"sudo", "su", "doas"}:
        return "deny", "Privilege escalation is not allowed in this project."
    if command in {"export", "unset", "declare", "set", "alias", "readonly", "typeset"}:
        if any(UNSAFE_ENV.match(a) for a in args):
            return "deny", "Changing engine/database/shell environment variables is not allowed."
        return None
    if command in {"pushd", "popd"}:
        return "ask", f"{command} changes the working directory in a way the guard cannot track."
    if command == "cd":
        names = [a for a in args if not a.startswith("-") or a == "-"]
        target = names[-1] if names else ""
        if target in {"", "-"}:
            return "deny", "cd without a literal project directory is not allowed."
        if re.search(r"[$`]", target) or not inside_allowed(target, cwd):
            return "deny", f"cd outside the project folder (or non-literal): {target}"
        return None

    secret = secret_access(command, args, cwd)
    if secret:
        return secret
    if command in {"python3", "python"} and args == ["-I", "scripts/check-skills.py"]:
        return None
    if tokens[0] == WRAPPER:
        if re.search(r"[`$]", " ".join(args)):
            return "deny", "Shell substitution is not allowed in wrapper arguments."
        verb = args[0] if args else ""
        if verb not in WRAPPER_VERBS:
            return "deny", f"Unknown test wrapper verb '{verb}'."
        if verb == "stop":
            return "ask", "Stopping the isolated test stack removes its containers and volumes: human approval required."
        return None
    if command in DIRECT_DB_TOOLS:
        return "deny", f"Direct '{command}' access is blocked; use {WRAPPER}, which verifies the isolated test identity."
    if command in SHELL_WRAPPERS:
        return "ask", f"'{command}' can hide nested commands from the guard: human approval required."
    if command == "docker" or command == "docker-compose":
        return check_docker(command, args, cwd)
    if command == "git":
        return check_git(args, cwd)
    if command == "sbt" and any(a in {"clean", "cleanFiles"} for a in args):
        return "ask", "sbt clean deletes build output: human approval required."
    if command == "sbt" and any(re.match(r"^(run|runMain|.*/run)$", a) for a in args):
        return "ask", "Running the application is not a test action: human approval required."
    if command == "find":
        if any(a in {"-delete", "-exec", "-ok", "-execdir", "-okdir"} for a in args):
            return "ask", "find -delete/-exec is a cleanup action: human approval required."
        if any(a.startswith(("-fprint", "-fls")) for a in args):
            return "deny", "find -fprint/-fls writes arbitrary files."
    if command in {"grep", "egrep", "fgrep", "rg"}:
        if command == "rg" and any(a.startswith(("--pre", "--hostname-bin")) for a in args):
            return "deny", "rg --pre/--hostname-bin runs arbitrary commands."
        recursive = (any(re.match(r"^-[a-zA-Z]*[rR]|^--recursive|^--dereference-recursive|^--directories=recurse", a)
                         for a in args) or "recurse" in args) if command != "rg" else True
        widens = command == "rg" and any(
            a in {"-u", "-uu", "-uuu", "--hidden", "-."} or a.startswith("--no-ignore") or re.match(r"^-[a-zA-Z]*u", a)
            for a in args)
        paths = [a for a in args if not a.startswith("-") and a != "recurse"][1:]
        covers_root = not paths or any(
            resolve(clean(p), cwd) == PROJECT or resolve(clean(p), cwd) in PROJECT.parents
            or clean(p) in {"~", "/"} or str(resolve(clean(p), cwd)) == str(PROJECT / ".local") for p in paths)
        if recursive and (covers_root or (widens and command == "rg")):
            return "ask", "Recursive search over the repository root can read ignored secret files: human approval required."
    if command == "curl" and any(a.startswith("--unix-socket") for a in args):
        return "deny", "curl --unix-socket can reach the docker engine."
    if command in CLEANUP_COMMANDS:
        outcome = write_targets(command, args, cwd)
        return outcome if outcome and outcome[0] == "deny" else ("ask", f"'{command}' is a cleanup action: human approval required.")
    if command in WRITE_COMMANDS:
        return write_targets(command, args, cwd)
    if command == "sed":
        in_place = any(re.match(r"^-[a-zA-Z]*i|^--in-place", a) for a in args)
        script_writes = [a for a in args if re.search(r"(^|[;{}\s/])[wW]\s*\S", a) and re.search(r"[/~.]", a)]
        if script_writes and any(re.search(r"[wW]\s*(/|~|\.\.|\$)", a) for a in script_writes):
            return "deny", "sed 'w' command writes to a file outside the project."
        if in_place:
            return write_targets(command, args, cwd)
    return None


def write_targets(command, args, cwd):
    result = None
    for arg in args:
        if arg.startswith("-") and "=" not in arg and len(arg) <= 2:
            continue
        for value in candidate_paths(arg):
            if not value or value.startswith("-") or not re.search(r"[A-Za-z0-9_./~$`]", value):
                continue
            # bare words like "644" or "node" are not paths; require a separator, dot, tilde, or a write-only verb
            if command in {"chmod", "chown"} and not ("/" in value or value.startswith((".", "~"))):
                continue
            if command in {"curl", "wget", "tar", "unzip"} and not (value.startswith(("/", "~", ".")) or "/" in value):
                continue
            if re.match(r"^[a-z]+://", value) or (command == "rsync" and ":" in value and not value.startswith("/")):
                if command == "rsync":
                    return "deny", "rsync to a remote location is not allowed."
                continue
            result = worst(result, classify_write(value, cwd))
    return result


def secret_access(command, args, cwd):
    if command in SECRET_SAFE_COMMANDS or (command == "git" and args[:1] == ["check-ignore"]):
        return None
    for arg in args:
        for value in candidate_paths(arg) + [arg.lstrip("@<")]:
            value = clean(value)
            normalized = os.path.relpath(resolve(value, cwd), PROJECT) if value and not value.startswith("-") else value
            if SECRET_PATH.search(value) or SECRET_PATH.search(normalized) or SECRET_PATH.search(str(resolve(value, cwd))):
                return "deny", "Secret/credential files must not be accessed through the shell."
            if any(ch in value for ch in "*?[") and not value.startswith("-"):
                for hit in glob.glob(str(cwd / os.path.expanduser(value)), recursive=True)[:200]:
                    if SECRET_PATH.search(os.path.relpath(hit, PROJECT)):
                        return "deny", "A glob expands to secret/credential files."
    return None


def check_docker(command, args, cwd):
    if any(a in {"-H", "--host", "--context", "-c", "--config", "--env-file", "--project-directory", "--tls", "--tlsverify"}
           or a.startswith(("--host=", "--context=", "--config=", "--env-file=", "--project-directory=", "-H", "-c", "-l"))
           for a in args):
        return "deny", "docker engine/context/config selection flags are not allowed."
    words = [a for a in args if not a.startswith("-")]
    compose_files = [args[i + 1] for i, a in enumerate(args[:-1]) if a in {"-f", "--file"}]
    compose_files += [a.split("=", 1)[1] for a in args if a.startswith("--file=")]
    compose_files += [a[2:] for a in args if a.startswith("-f") and len(a) > 2]
    if any(str(resolve(f, cwd)) != str(PROJECT / "compose.test.yaml") for f in compose_files):
        return "deny", "Only compose.test.yaml may be driven directly; other compose files target non-test services."
    if command == "docker-compose" or (words and words[0] == "compose"):
        if not compose_files:
            return "deny", "Compose commands must pass -f compose.test.yaml; use the test wrapper instead."
    if set(words) & {"exec", "run", "cp", "attach"}:
        return "deny", "docker exec/run/cp/attach is blocked; it can reach non-test containers."
    if set(words[:3]) & DOCKER_CLEANUP:
        return "ask", "Docker cleanup/lifecycle action: human approval required."
    return "ask", "Direct docker use: prefer the test wrapper; human approval required."


def check_git(args, cwd):
    sub = next((a for a in args if not a.startswith("-")), "")
    if sub in {"diff", "log", "show", "grep", "blame", "cat-file"}:
        for a in args[args.index(sub) + 1:]:
            if a.startswith("-") or a == "/dev/null" or not (a.startswith(("/", "~", ".")) or "/" in a):
                continue
            if not inside_allowed(a, cwd):
                return "deny", f"git {sub} on a path outside the project (implicit --no-index): {a}"
    if sub in {"archive", "format-patch", "bundle", "worktree", "clone", "init", "rebase", "bisect", "difftool",
               "submodule", "filter-branch", "remote", "fetch", "pull", "push"}:
        return "ask", f"git {sub} writes outside the working tree or contacts remotes: human approval required."
    if sub == "grep" and any(a in {"--untracked", "--no-exclude-standard"} or a.startswith(("-O", "--open-files-in-pager"))
                             for a in args):
        return "deny", "git grep options that read ignored files or run commands are not allowed."
    if "--no-index" in args:
        return "deny", "git --no-index reads arbitrary files."
    if any(a == "--output" or a.startswith("--output") for a in args):
        return "deny", "git --output writes arbitrary files."
    if "-c" in args or any(a.startswith("-c") and len(a) > 2 and "=" in a for a in args[:2]) or any(a.startswith("--config-env") for a in args):
        return "deny", "git -c can execute configured commands."
    if "clean" in args:
        return "deny", "git clean is denied."
    if args[:1] == ["config"] or "--ext-diff" in args or "--textconv" in args:
        return "ask", "git config / external diff can run configured commands: human approval required."
    if args[:1] in (["apply"], ["checkout"], ["restore"], ["stash"], ["mv"], ["rm"], ["am"]):
        return "ask", "git working-tree mutation: human approval required."
    return None


def check_bash(command, cwd):
    command = re.sub(r"(?<!\\)((?:\\\\)*)\\\n", r"\1", command)
    for uri in DB_URI.findall(command):
        authority = re.sub(r"^mongodb(?:\+srv)?://", "", uri, flags=re.IGNORECASE).split("/", 1)[0].split("?", 1)[0]
        port = manifest_mongo_port()
        if port is None or not re.fullmatch(r"(?:[^@,/]*@)?127\.0\.0\.1:" + port, authority) or "mongodb+srv" in uri.lower():
            return "deny", "A database URI that is not the isolated test stack's own endpoint appears in the command."
    first = shlex_safe(command)
    stripped = command.strip()
    if SECRET_TEXT.search(command) and not (stripped.startswith(("git check-ignore", "ls ")) or first in SECRET_SAFE_COMMANDS):
        return "deny", "Secret/credential locations must not be referenced in shell commands."
    if "<<" in command:
        result_heredoc = ("ask", "Heredocs hide following lines from the guard: human approval required.")
    else:
        result_heredoc = None
    if ">(" in command or "<(" in command:
        return "ask", "Process substitution can hide writes: human approval required."
    result = result_heredoc
    if re.search(r"\$[A-Za-z_]", command):
        result = ("ask", "Shell variables can build paths the guard cannot see: human approval required.")
    if re.search(r"\$\(|\$\{|`|\$'|\{[^{}\s]*,[^{}\s]*\}", command):
        result = ("ask", "Command substitution or brace expansion hides what will run: human approval required.")
    for match in re.finditer(r"<\s*([\"']?[^\s&|;<>\"']+)", command):
        target = clean(match.group(1))
        if SECRET_PATH.search(target):
            return "deny", "Secret/credential files must not be read through input redirection."
    current = cwd
    for segment in split_segments(command):
        try:
            tokens = shlex.split(segment)
        except ValueError:
            result = worst(result, ("ask", "Command could not be parsed safely: human approval required."))
            continue
        for match in re.finditer(r"\d*>[>|&]?\s*([\"']?[^\s&|;<>\"']+)", segment):
            result = worst(result, classify_write(match.group(1), current))
        result = worst(result, check_segment(list(tokens), current))
        current = next_cwd(tokens, current)
        if result and result[0] == "deny":
            return result
    return result


def next_cwd(tokens, current):
    """Working directory after a segment, so later relative paths resolve where bash will run them."""
    while tokens and (tokens[0] in SHELL_KEYWORDS or re.fullmatch(r"[({!]+", tokens[0])):
        tokens = tokens[1:]
    if tokens and tokens[0].lstrip("({!") == "cd":
        names = [a for a in tokens[1:] if not a.startswith("-") or a == "-"]
        if names and names[-1] not in {"", "-"} and not re.search(r"[$`]", names[-1]):
            return resolve(clean(names[-1]), current)
    return current


def split_segments(command):
    """Split on ; | & && || and newlines, but not inside quotes or in redirections like 2>&1 / &>."""
    segments, current, quote, i = [], [], None, 0
    while i < len(command):
        ch = command[i]
        if quote:
            current.append(ch)
            if ch == "\\" and quote == '"' and i + 1 < len(command):
                current.append(command[i + 1])
                i += 1
            elif ch == quote:
                quote = None
        elif ch == "\\" and i + 1 < len(command):
            current.append(ch + command[i + 1])
            i += 1
        elif ch == "#" and (i == 0 or command[i - 1] in " \t\n;|&("):
            while i < len(command) and command[i] != "\n":
                i += 1
            continue
        elif ch in "'\"":
            quote = ch
            current.append(ch)
        elif ch in ";|\n" or (ch == "&" and (i == 0 or command[i - 1] != ">") and command[i + 1:i + 2] != ">"):
            segments.append("".join(current))
            current = []
        else:
            current.append(ch)
        i += 1
    segments.append("".join(current))
    return segments


def shlex_safe(command):
    try:
        tokens = shlex.split(command)
    except ValueError:
        return ""
    return os.path.basename(tokens[0]) if tokens else ""


def check_file(tool_input, cwd):
    target = tool_input.get("file_path") or tool_input.get("notebook_path") or ""
    if not target:
        return None
    if not inside_allowed(target, cwd):
        return "deny", f"Writes outside the project folder are not allowed: {target}"
    rel = os.path.relpath(resolve(target, cwd), PROJECT)
    if not rel.startswith("..") and is_protected(rel):
        return "ask", f"{rel} controls the test harness, build or guard: human approval required."
    return None


def main():
    event = json.load(sys.stdin)
    cwd = Path(event.get("cwd") or PROJECT)
    tool, tool_input = event.get("tool_name", ""), event.get("tool_input") or {}
    if tool == "Bash":
        outcome = check_bash(tool_input.get("command") or "", cwd)
    elif tool in {"Edit", "Write", "NotebookEdit", "MultiEdit"}:
        outcome = check_file(tool_input, cwd)
    else:
        outcome = None
    if outcome:
        json.dump(decide(*outcome), sys.stdout)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:  # fail closed: exit code 2 blocks the tool call
        print(f"guard-harness failed closed: {error}", file=sys.stderr)
        sys.exit(2)