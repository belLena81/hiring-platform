#!/usr/bin/env bash
# Formats the staged Scala and sbt files with each build's pinned scalafmt (through sbt) and re-stages them, so an
# unformatted file never reaches a commit. Run by .githooks/pre-commit; may also be run by hand before a commit.
# Contract: see "Operator scripts" in docs/engineering-quality.md. Run with --help for usage and exit codes.
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/operator-lib.sh
source "$script_dir/operator-lib.sh"

limit="${FORMAT_STAGED_TIMEOUT_SECONDS:-900}"
result_done=0
current_build=""

help_text() {
  cat <<'HELP'
Usage: scripts/format-staged.sh
       scripts/format-staged.sh --help

Formats the staged *.scala and *.sbt files (and project/*.scala) of the application and analytics builds with the
scalafmt version pinned in each build (sbt scalafmtOnly / scalafmtSbt), then re-stages the files it changed. It is the
.githooks/pre-commit hook (enable once per clone: git config core.hooksPath .githooks). Files that are not staged are
never formatted or added, with one exception: scalafmtSbt formats every build file of a build, so an unstaged build
file may change in the working tree (it is reported, not staged).

A staged file that also has unstaged changes is refused (exit 1): formatting it would stage the unstaged part.
Stage the whole file or stash the rest, then commit again. During `git commit <path>` git runs the hook on a
temporary index, so a file that needed formatting is formatted in the working tree but the commit is refused (exit 1);
run the same `git commit <path>` again and it passes, recording only that path. `git commit -a` and a plain
`git commit` re-stage normally. `git commit --amend` formats
only the files staged for the amendment, not files already in the commit. A path may contain only letters, digits
and . _ / + @ - (exit 2 otherwise), because the paths form one sbt command string. An edit made to a staged file
while sbt runs is staged with the formatting.

Expected duration: under 1 s when no Scala or sbt file is staged (sbt is not started); otherwise one sbt start per
affected build, about 20-60 s each, bounded by FORMAT_STAGED_TIMEOUT_SECONDS (60-7200, default 900) per build.
Logs: .local/logs/format-staged-<build>.log. JAVA_HOME, when set, is passed to sbt (-java-home).

Exit codes: 0 PASS (files formatted and staged, or nothing to do), 1 FAIL (partial staging, sbt failure, time limit),
2 usage error or precondition not met (BLOCKED), 130 interrupted.
The last line is "RESULT: PASS|FAIL|BLOCKED ..." with file counts and duration.

Examples:
  bash scripts/format-staged.sh
  FORMAT_STAGED_TIMEOUT_SECONDS=300 bash scripts/format-staged.sh
HELP
}

finish() { # RESULT code detail
  local line="RESULT: $1 $3 duration=$(op_elapsed)"
  result_done=1
  if [[ "$1" == PASS ]]; then printf '%s\n' "$line"; else printf '%s\n' "$line" >&2; fi
  exit "$2"
}

on_interrupt() {
  trap '' INT TERM HUP
  printf '\n' >&2
  op_say "interrupted${current_build:+ while formatting the $current_build build}; formatted files are left unstaged, nothing was added to the index; stopping what this script started" >&2
  op_stop_child 20
  result_done=1
  exit 130
}
on_exit() {
  local code=$?
  if (( code != 0 && result_done == 0 )); then
    printf 'format-staged: unexpected failure (exit %s)\n' "$code" >&2
    printf 'RESULT: FAIL reason=unexpected duration=%s\n' "$(op_elapsed)" >&2
  fi
}
trap on_interrupt INT TERM HUP
trap on_exit EXIT

case "${1:-}" in
  "") ;;
  -h | --help) help_text; exit 0 ;;
  *) printf 'format-staged: unexpected argument: %s (run scripts/format-staged.sh --help)\n' "$1" >&2; finish BLOCKED 2 "reason=usage" ;;
esac
if [[ ! "$limit" =~ ^[0-9]{1,5}$ ]] || (( 10#$limit < 60 || 10#$limit > 7200 )); then
  printf 'format-staged: FORMAT_STAGED_TIMEOUT_SECONDS must be an integer between 60 and 7200 (got %s)\n' "$limit" >&2
  finish BLOCKED 2 "reason=usage"
fi
limit=$((10#$limit))

project_root="$(git rev-parse --show-toplevel 2>/dev/null)" || { printf 'format-staged: not inside a git repository\n' >&2; finish BLOCKED 2 "reason=not-a-repository"; }
cd "$project_root"

# Staged, non-deleted Scala and sbt files; NUL-separated so unusual names cannot split.
staged=()
while IFS= read -r -d '' path; do
  case "$path" in
    *.scala | *.sbt) staged+=("$path") ;;
  esac
done < <(git diff --cached --name-only --diff-filter=ACMR -z)
if (( ${#staged[@]} == 0 )); then
  printf 'format-staged: no staged Scala or sbt file; nothing to format (sbt not started)\n'
  finish PASS 0 "files=0 reformatted=0 note=no-staged-scala-or-sbt-files"
fi
printf 'format-staged: %s staged Scala/sbt file(s) to format with the pinned scalafmt (limit %s per build)\n' "${#staged[@]}" "$(op_duration "$limit")"
# `git commit <path>` runs the hook against a temporary index (GIT_INDEX_FILE=.git/next-index-*.lock) while the real
# index is locked: a `git add` here would land only in the temporary one, leaving the unformatted copy staged for the
# next commit. In that mode the files are still formatted in the working tree, but a change is refused, not staged.
pathspec_commit=0
case "$(basename -- "${GIT_INDEX_FILE:-index}")" in next-index-*) pathspec_commit=1 ;; esac

# Refuse a staged file that also carries unstaged changes: formatting the working tree would stage those changes.
partial=()
while IFS= read -r -d '' path; do
  for candidate in "${staged[@]}"; do
    [[ "$candidate" == "$path" ]] && partial+=("$path")
  done
done < <(git diff --name-only -z)
if (( ${#partial[@]} > 0 )); then
  printf 'format-staged: these staged files also have unstaged changes; stage the whole file (git add) or stash the rest, then commit again:\n' >&2
  printf '  %s\n' "${partial[@]}" >&2
  finish FAIL 1 "files=${#staged[@]} reason=partially-staged"
fi
# The paths become one sbt command string, where ';' separates commands and quotes, braces and whitespace have
# meaning, so only an allow-listed character set is passed on; anything else is refused before sbt starts.
for path in "${staged[@]}"; do
  if [[ ! "$path" =~ ^[A-Za-z0-9._/+@-]+$ ]]; then
    printf 'format-staged: cannot pass this path to sbt (only letters, digits, . _ / + @ - are accepted): %s; rename it or format it by hand with sbt scalafmtAll\n' "$path" >&2
    finish BLOCKED 2 "files=${#staged[@]} reason=unsupported-path"
  fi
done

# sbt is resolved once, to an absolute path, so the later `cd` into a build directory cannot change which sbt runs.
sbt_bin="$(command -v sbt 2>/dev/null || true)"
if [[ -z "$sbt_bin" || "$sbt_bin" != /* ]]; then
  printf 'format-staged: sbt is not on PATH as an absolute path; install sbt or format by hand (sbt scalafmtAll scalafmtSbt)\n' >&2
  finish BLOCKED 2 "reason=sbt-missing"
fi
git check-ignore -q "$project_root/.local/logs/format-staged-root.log" || {
  printf 'format-staged: .local/logs is not git-ignored; add it to .gitignore first\n' >&2
  finish BLOCKED 2 "reason=logs-not-ignored"
}
mkdir -p "$project_root/.local/logs"
sbt_args=(-batch)
[[ -z "${JAVA_HOME:-}" ]] || sbt_args+=(-java-home "$JAVA_HOME")
printf 'format-staged: each affected build starts sbt once (about 20-60 s); this loads the working-tree build definition like a manual sbt run\n'

# Split the files by build; the analytics build is a separate sbt project under analytics/.
root_scala=() root_build=() analytics_scala=() analytics_build=()
for path in "${staged[@]}"; do
  case "$path" in
    analytics/*.sbt | analytics/project/*) analytics_build+=("${path#analytics/}") ;;
    analytics/*) analytics_scala+=("${path#analytics/}") ;;
    *.sbt | project/*) root_build+=("$path") ;;
    *) root_scala+=("$path") ;;
  esac
done

hash_of() { git hash-object -- "$1"; }
before=()
for path in "${staged[@]}"; do before+=("$(hash_of "$path")"); done
# Unstaged build files as they are now, so only the ones scalafmtSbt changes are reported later.
declare -A unstaged_build_before=()
while IFS= read -r -d '' path; do
  case "$path" in *.sbt | project/* | analytics/project/*) [[ -f "$path" ]] && unstaged_build_before["$path"]="$(hash_of "$path")" ;; esac
done < <(git diff --name-only -z)

format_build() { # build-name build-dir scala-files... -- build-files...
  local build="$1" dir="$2" rc=0 log commands=() scala=() builds=()
  shift 2
  while (( $# > 0 )) && [[ "$1" != -- ]]; do scala+=("$1"); shift; done
  [[ "${1:-}" != -- ]] || shift
  builds=("$@")
  (( ${#scala[@]} == 0 )) || commands+=("scalafmtOnly ${scala[*]}")
  (( ${#builds[@]} == 0 )) || commands+=("scalafmtSbt")
  (( ${#commands[@]} > 0 )) || return 0
  current_build="$build"
  log="$project_root/.local/logs/format-staged-$build.log"
  op_say "$build build: sbt ${commands[*]} (log: ${log#"$project_root"/})"
  op_run_logged "$build build formatting" "$limit" "$log" yes bash -c 'cd "$1" && shift && exec "$@"' format-staged "$dir" "$sbt_bin" "${sbt_args[@]}" "${commands[@]}" || rc=$?
  if (( rc == 124 )); then
    printf 'format-staged: the %s build did not finish formatting within %s; raise FORMAT_STAGED_TIMEOUT_SECONDS or read %s\n' "$build" "$(op_duration "$limit")" "${log#"$project_root"/}" >&2
    finish FAIL 1 "files=${#staged[@]} reason=time-limit build=$build"
  fi
  if (( rc != 0 )); then
    op_show_failure_tail "$log" 25
    printf 'format-staged: sbt failed for the %s build (exit %s); fix the error above or read %s\n' "$build" "$rc" "${log#"$project_root"/}" >&2
    finish FAIL 1 "files=${#staged[@]} reason=sbt-failed build=$build"
  fi
  current_build=""
}

format_build root "$project_root" "${root_scala[@]}" -- "${root_build[@]}"
format_build analytics "$project_root/analytics" "${analytics_scala[@]}" -- "${analytics_build[@]}"

reformatted=()
for i in "${!staged[@]}"; do
  [[ "$(hash_of "${staged[$i]}")" == "${before[$i]}" ]] || reformatted+=("${staged[$i]}")
done
if (( ${#reformatted[@]} > 0 )); then
  if (( pathspec_commit )); then
    printf 'format-staged: these files were formatted in the working tree but cannot be staged during `git commit <path>` (git locks the real index and runs the hook on a temporary one):\n' >&2
    printf '  %s\n' "${reformatted[@]}" >&2
    printf 'format-staged: next: run the same git commit <path> again; the working tree is now formatted, so it passes and records only that path\n' >&2
    finish FAIL 1 "files=${#staged[@]} reformatted=${#reformatted[@]} reason=pathspec-commit"
  fi
  git add -- "${reformatted[@]}"
  printf 'format-staged: reformatted and re-staged:\n'
  printf '  %s\n' "${reformatted[@]}"
fi
# scalafmtSbt may have touched unstaged build files; name the ones whose unstaged content changed, without staging them.
changed_builds=()
while IFS= read -r -d '' path; do
  case "$path" in
    *.sbt | project/* | analytics/project/*)
      [[ ! -f "$path" || "$(hash_of "$path")" == "${unstaged_build_before[$path]:-}" ]] || changed_builds+=("$path") ;;
  esac
done < <(git diff --name-only -z)
if (( ${#changed_builds[@]} > 0 )); then
  printf 'format-staged: scalafmtSbt changed unstaged build files in the working tree; they were not staged:\n'
  printf '  %s\n' "${changed_builds[@]}"
fi
finish PASS 0 "files=${#staged[@]} reformatted=${#reformatted[@]}"
