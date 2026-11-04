#!/usr/bin/env bash
# Self-test of scripts/format-staged.sh (the .githooks/pre-commit formatter) in a throw-away git repository with a fake
# `sbt` on PATH that records its arguments and rewrites one file. It starts no real sbt and writes only to a temporary
# directory under .local/data/ that it removes.
# Contract: see "Operator scripts" in docs/engineering-quality.md.
set -Eeuo pipefail
umask 077
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd -P)"
cd "$repo_root"
result_done=0
checks=0
failures=0
work=""

if [[ "${1:-}" == -h || "${1:-}" == --help ]]; then
  cat <<'HELP'
Usage: scripts/tests/format-staged.sh [--help]

Self-test of scripts/format-staged.sh on a temporary repository with a fake sbt: nothing staged (no sbt start),
partial staging refused, Scala and build files split between the root and analytics builds, a reformatted file
re-staged while an untouched one is left alone, an sbt failure, an unsupported path blocked, and usage errors.
Expected duration: under 10 s. Needs no Docker and no sbt; writes only under .local/data/ and removes it.

Exit codes: 0 PASS, 1 FAIL, 2 usage error, 130 interrupted. The last line is "RESULT: PASS|FAIL ...".
HELP
  exit 0
fi
[[ $# -eq 0 ]] || { printf 'format-staged-test: unknown argument: %s (try --help)\n' "$1" >&2; printf 'RESULT: BLOCKED reason=usage\n' >&2; exit 2; }
printf 'format-staged-test: checking scripts/format-staged.sh with a fake sbt in a temporary repository; expected under 10 s\n'

cleanup() { [[ -z "$work" ]] || rm -rf -- "$work"; }
on_exit() {
  local code=$?
  cleanup
  if (( result_done == 0 )); then
    if (( code == 130 )); then printf 'format-staged-test: interrupted after %s checks\n' "$checks" >&2
    else printf 'format-staged-test: unexpected failure (exit %s)\nRESULT: FAIL checks=%s failures=%s\n' "$code" "$checks" "$failures" >&2; fi
  fi
}
trap on_exit EXIT
trap 'exit 130' INT TERM HUP

mkdir -p "$repo_root/.local/data"
work="$(mktemp -d "$repo_root/.local/data/format-staged-test.XXXXXX")"
repo="$work/repo"
bin="$work/bin"
mkdir -p "$repo" "$bin"
cat >"$bin/sbt" <<'FAKE'
#!/usr/bin/env bash
# Fake sbt: records the working directory and arguments; rewrites any file named Unformatted.scala it is asked to format.
printf '%s|%s\n' "$PWD" "$*" >>"$FAKE_SBT_LOG"
for arg in "$@"; do
  for f in $arg; do
    case "$f" in
      *Unformatted.scala) printf 'object Formatted\n' >"$f" ;;
    esac
  done
done
exit "${FAKE_SBT_EXIT:-0}"
FAKE
chmod +x "$bin/sbt"
export FAKE_SBT_LOG="$work/sbt.log"
export PATH="$bin:$PATH"
# The temporary repository must not inherit the user's global git configuration (hooks path, signing, templates).
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_SYSTEM=/dev/null

git -C "$repo" init -q
git -C "$repo" config user.email test@example.invalid
git -C "$repo" config user.name test
git -C "$repo" config commit.gpgsign false
printf '.local/\n' >"$repo/.gitignore"
mkdir -p "$repo/src/main/scala" "$repo/analytics/src/main/scala" "$repo/project"
printf 'object Clean\n' >"$repo/src/main/scala/Clean.scala"
git -C "$repo" add -A
git -C "$repo" commit -q -m init

run() { # name expected-exit args... ; captures combined output in $out and exit code in $rc
  rc=0
  # JAVA_HOME is unset so the recorded sbt arguments are the same on every machine.
  out="$(cd "$repo" && env -u JAVA_HOME bash "$repo_root/scripts/format-staged.sh" "${@:3}" 2>&1)" || rc=$?
  checks=$((checks + 1))
  if [[ "$rc" != "$2" ]]; then
    failures=$((failures + 1))
    printf 'FAIL %s: exit %s, expected %s\n%s\n' "$1" "$rc" "$2" "$out" >&2
  else
    printf 'ok   %s (exit %s)\n' "$1" "$rc"
  fi
}
expect() { # name pattern text
  checks=$((checks + 1))
  if grep -q -- "$2" <<<"$3"; then printf 'ok   %s\n' "$1"; else failures=$((failures + 1)); printf 'FAIL %s: missing "%s" in:\n%s\n' "$1" "$2" "$3" >&2; fi
}
refute() { # name pattern text
  checks=$((checks + 1))
  if grep -q -- "$2" <<<"$3"; then failures=$((failures + 1)); printf 'FAIL %s: unexpected "%s" in:\n%s\n' "$1" "$2" "$3" >&2; else printf 'ok   %s\n' "$1"; fi
}

# 1. Nothing staged: PASS without starting sbt.
: >"$FAKE_SBT_LOG"
run "nothing staged" 0
expect "nothing staged result" 'RESULT: PASS files=0 reformatted=0' "$out"
refute "nothing staged starts no sbt" '.' "$(cat "$FAKE_SBT_LOG")"

# 2. A staged non-Scala file only: PASS without sbt.
printf 'notes\n' >"$repo/README.md"; git -C "$repo" add README.md
run "only a markdown file staged" 0
refute "markdown starts no sbt" '.' "$(cat "$FAKE_SBT_LOG")"
git -C "$repo" commit -q -m readme

# 3. Partial staging is refused before sbt starts.
printf 'object Clean\nobject Staged\n' >"$repo/src/main/scala/Clean.scala"; git -C "$repo" add src/main/scala/Clean.scala
printf 'object Clean\nobject Staged\nobject Unstaged\n' >"$repo/src/main/scala/Clean.scala"
run "partially staged file refused" 1
expect "partial staging names the file" 'src/main/scala/Clean.scala' "$out"
expect "partial staging result" 'RESULT: FAIL files=1 reason=partially-staged' "$out"
refute "partial staging starts no sbt" '.' "$(cat "$FAKE_SBT_LOG")"
git -C "$repo" reset -q; git -C "$repo" checkout -q -- src/main/scala/Clean.scala

# 4. Root Scala + root build file + analytics Scala + analytics build file: two sbt runs, right directories and tasks,
#    the reformatted file re-staged, the untouched file left as staged.
printf 'object   Unformatted\n' >"$repo/src/main/scala/Unformatted.scala"
printf 'object Tidy\n' >"$repo/src/main/scala/Tidy.scala"
printf 'name := "root"\n' >"$repo/build.sbt"
printf 'object Plugins\n' >"$repo/project/Deps.scala"
printf 'object   Unformatted\n' >"$repo/analytics/src/main/scala/Unformatted.scala"
printf 'name := "analytics"\n' >"$repo/analytics/build.sbt"
git -C "$repo" add -A
: >"$FAKE_SBT_LOG"
run "both builds formatted" 0
expect "root sbt run" "^$repo|-batch scalafmtOnly src/main/scala/Tidy.scala src/main/scala/Unformatted.scala scalafmtSbt$" "$(cat "$FAKE_SBT_LOG")"
expect "analytics sbt run" "^$repo/analytics|-batch scalafmtOnly src/main/scala/Unformatted.scala scalafmtSbt$" "$(cat "$FAKE_SBT_LOG")"
expect "two sbt starts" '^2$' "$(wc -l <"$FAKE_SBT_LOG")"
expect "reformatted root file listed" '  src/main/scala/Unformatted.scala' "$out"
expect "reformatted analytics file listed" '  analytics/src/main/scala/Unformatted.scala' "$out"
expect "result counts" 'RESULT: PASS files=6 reformatted=2' "$out"
expect "reformatted content staged" 'object Formatted' "$(git -C "$repo" show :src/main/scala/Unformatted.scala)"
expect "untouched file still staged as is" 'object Tidy' "$(git -C "$repo" show :src/main/scala/Tidy.scala)"
refute "no unstaged change left" '.' "$(git -C "$repo" diff --name-only)"
git -C "$repo" commit -q -m formatted

# 5. sbt failure fails the run and names the build.
printf 'object   Unformatted2\n' >"$repo/src/main/scala/Again.scala"; git -C "$repo" add -A
FAKE_SBT_EXIT=3 run "sbt failure" 1
expect "sbt failure result" 'RESULT: FAIL files=1 reason=sbt-failed build=root' "$out"
git -C "$repo" reset -q; rm -f "$repo/src/main/scala/Again.scala"

# 6. Unsupported path (whitespace) is blocked before sbt.
printf 'object Space\n' >"$repo/src/main/scala/With Space.scala"; git -C "$repo" add -A
: >"$FAKE_SBT_LOG"
run "path with whitespace blocked" 2
expect "unsupported path result" 'reason=unsupported-path' "$out"
refute "unsupported path starts no sbt" '.' "$(cat "$FAKE_SBT_LOG")"
git -C "$repo" reset -q; rm -f "$repo/src/main/scala/With Space.scala"

# 6b. A path with an sbt command separator is blocked before sbt (sbt splits a command string on ';').
printf 'object Semi\n' >"$repo/src/main/scala/a;clean;b.scala"; git -C "$repo" add -A
: >"$FAKE_SBT_LOG"
run "path with semicolon blocked" 2
expect "semicolon path result" 'reason=unsupported-path' "$out"
refute "semicolon path starts no sbt" '.' "$(cat "$FAKE_SBT_LOG")"
git -C "$repo" reset -q; rm -f "$repo/src/main/scala/a;clean;b.scala"

# 6c. A real `git commit <path>` through the hook: the file is formatted in the working tree, the commit is refused
#     (rerun the same pathspec commit), and the real index is left untouched (git locks it during a pathspec commit).
hooks="$work/hooks"; mkdir -p "$hooks"
printf '#!/usr/bin/env bash\nexec bash "%s/scripts/format-staged.sh"\n' "$repo_root" >"$hooks/pre-commit"; chmod +x "$hooks/pre-commit"
git -C "$repo" config core.hooksPath "$hooks"
printf 'object   Unformatted\n' >"$repo/src/main/scala/PathUnformatted.scala"
printf 'object Other\n' >"$repo/src/main/scala/Other.scala"
git -C "$repo" add src/main/scala/PathUnformatted.scala src/main/scala/Other.scala
rc=0; out="$(cd "$repo" && env -u JAVA_HOME git commit -q -m pathspec -- src/main/scala/PathUnformatted.scala 2>&1)" || rc=$?
checks=$((checks + 1)); if [[ "$rc" == 1 ]]; then printf 'ok   pathspec commit refused (exit 1)\n'; else failures=$((failures + 1)); printf 'FAIL pathspec commit: exit %s, expected 1\n%s\n' "$rc" "$out" >&2; fi
expect "pathspec commit result" 'reason=pathspec-commit' "$out"
expect "pathspec commit advice" 'run the same git commit <path> again' "$out"
expect "pathspec commit formatted the working tree" 'object Formatted' "$(cat "$repo/src/main/scala/PathUnformatted.scala")"
expect "pathspec commit left the index as staged" 'object   Unformatted' "$(git -C "$repo" show :src/main/scala/PathUnformatted.scala)"
refute "pathspec commit recorded nothing" 'pathspec' "$(git -C "$repo" log --format=%s)"
# Rerunning the same pathspec commit passes, records only that path and leaves the other staged file uncommitted.
rc=0; out="$(cd "$repo" && env -u JAVA_HOME git commit -q -m pathspec -- src/main/scala/PathUnformatted.scala 2>&1)" || rc=$?
checks=$((checks + 1)); if [[ "$rc" == 0 ]]; then printf 'ok   pathspec commit rerun (exit 0)\n'; else failures=$((failures + 1)); printf 'FAIL pathspec commit rerun: exit %s\n%s\n' "$rc" "$out" >&2; fi
expect "pathspec rerun holds formatted content" 'object Formatted' "$(git -C "$repo" show HEAD:src/main/scala/PathUnformatted.scala)"
expect "pathspec rerun recorded only that path" '^src/main/scala/PathUnformatted.scala$' "$(git -C "$repo" show --name-only --format= HEAD)"
refute "pathspec rerun left the other file out" 'Other' "$(git -C "$repo" show --name-only --format= HEAD)"
expect "other file still staged" '^A  src/main/scala/Other.scala$' "$(git -C "$repo" status --porcelain)"
git -C "$repo" config --unset core.hooksPath
git -C "$repo" commit -q -m other

# 7. Usage errors and help.
run "unknown argument" 2 --bogus
FORMAT_STAGED_TIMEOUT_SECONDS=5 run "timeout out of range" 2
FORMAT_STAGED_TIMEOUT_SECONDS=0900 run "zero-padded timeout read as decimal" 0
FORMAT_STAGED_TIMEOUT_SECONDS=08 run "zero-padded out-of-range timeout rejected" 2
FORMAT_STAGED_TIMEOUT_SECONDS=abc run "non-numeric timeout rejected" 2
run "help" 0 --help
expect "help text" 'Usage: scripts/format-staged.sh' "$out"

result_done=1
if (( failures == 0 )); then printf 'RESULT: PASS checks=%s failures=0\n' "$checks"; exit 0; fi
printf 'RESULT: FAIL checks=%s failures=%s\n' "$checks" "$failures" >&2
exit 1