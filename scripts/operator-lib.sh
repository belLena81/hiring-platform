#!/usr/bin/env bash
# Shared helpers for the operator scripts (see "Operator scripts" in docs/engineering-quality.md). Sourced, never executed.
# Provides: elapsed formatting, timestamped status lines, a bounded background runner with a 10-second heartbeat,
# an in-process heartbeat ticker, and sbt/MUnit summary counting. No secrets are read or printed here.

OP_HEARTBEAT_SECONDS="${OP_HEARTBEAT_SECONDS:-10}"
OP_START_SECONDS="$SECONDS"
OP_CHILD_PID=""
OP_TICKER_PID=""

op_duration() { # seconds -> 1h02m03s / 4m05s / 7s
  local total="$1"
  if (( total >= 3600 )); then printf '%dh%02dm%02ds' $((total / 3600)) $((total % 3600 / 60)) $((total % 60))
  elif (( total >= 60 )); then printf '%dm%02ds' $((total / 60)) $((total % 60))
  else printf '%ds' "$total"; fi
}

op_elapsed() { op_duration $((SECONDS - OP_START_SECONDS)); }

op_say() { printf '[%s] %s\n' "$(op_elapsed)" "$*"; }

# Strips terminal colour codes and carriage returns from a log line and trims it for the heartbeat.
op_clean_line() { tr -d '\r' | sed -e 's/\x1b\[[0-9;?]*[A-Za-z]//g' | cut -c1-110; }

op_last_line() { # logfile
  [[ -s "$1" ]] || { printf '(no output yet)'; return 0; }
  tail -n 1 "$1" | op_clean_line
}

# Prints a heartbeat every OP_HEARTBEAT_SECONDS while the calling shell is busy in the foreground.
# Usage: op_ticker_start "label"; ...work...; op_ticker_stop
op_ticker_start() {
  local label="$1" started="$SECONDS" parent="$$"
  (
    sleeper=""
    trap '[[ -z "$sleeper" ]] || kill "$sleeper" 2>/dev/null; exit 0' TERM HUP
    while true; do
      sleep "$OP_HEARTBEAT_SECONDS" &
      sleeper=$!
      wait "$sleeper" || true
      # Never outlive the script that started this ticker (killed parent or closed terminal).
      kill -0 "$parent" 2>/dev/null || exit 0
      op_say "$label: still working ($(op_duration $((SECONDS - started))) in this step)"
    done
  ) &
  OP_TICKER_PID=$!
}

op_ticker_stop() {
  if [[ -n "$OP_TICKER_PID" ]]; then
    kill "$OP_TICKER_PID" 2>/dev/null || true
    wait "$OP_TICKER_PID" 2>/dev/null || true
    OP_TICKER_PID=""
  fi
}

# Stops the runner's child process group: TERM first so Resource finalizers run, KILL after the grace period.
op_stop_child() { # grace-seconds
  local grace="${1:-20}" waited=0
  [[ -n "$OP_CHILD_PID" ]] || return 0
  kill -TERM -- "-$OP_CHILD_PID" 2>/dev/null || kill -TERM "$OP_CHILD_PID" 2>/dev/null || true
  while kill -0 "$OP_CHILD_PID" 2>/dev/null && (( waited < grace )); do
    sleep 1
    waited=$((waited + 1))
  done
  if kill -0 "$OP_CHILD_PID" 2>/dev/null; then
    kill -KILL -- "-$OP_CHILD_PID" 2>/dev/null || kill -KILL "$OP_CHILD_PID" 2>/dev/null || true
  fi
  wait "$OP_CHILD_PID" 2>/dev/null || true
  OP_CHILD_PID=""
}

# Runs a command in its own session with output in a log file, printing a heartbeat and enforcing a time limit.
# Usage: op_run_logged "label" limit-seconds logfile show-tail(yes|no) command...
# Returns the command status, or 124 when the limit was reached (the command is stopped first).
# The optional function OP_PHASE_DESCRIBE, when defined, supplies the phase text shown in each heartbeat.
op_run_logged() {
  local label="$1" limit="$2" logfile="$3" show_tail="$4" started="$SECONDS" next_beat rc=0
  shift 4
  : >"$logfile"
  setsid "$@" >"$logfile" 2>&1 </dev/null &
  OP_CHILD_PID=$!
  # Watchdog: if this script dies without running its traps (SIGKILL), stop the child's session so it cannot hold locks forever.
  (
    parent="$$" child="$OP_CHILD_PID"
    trap 'exit 0' TERM HUP
    while kill -0 "$child" 2>/dev/null; do
      kill -0 "$parent" 2>/dev/null || { kill -TERM -- "-$child" 2>/dev/null || true; exit 0; }
      sleep 2
    done
  ) &
  local watchdog=$!
  next_beat=$((SECONDS + OP_HEARTBEAT_SECONDS))
  while kill -0 "$OP_CHILD_PID" 2>/dev/null; do
    if (( SECONDS - started >= limit )); then
      op_say "$label: time limit of $(op_duration "$limit") reached; stopping it"
      op_stop_child 20
      kill "$watchdog" 2>/dev/null || true
      return 124
    fi
    if (( SECONDS >= next_beat )); then
      local detail="$label"
      if declare -F OP_PHASE_DESCRIBE >/dev/null; then detail="$(OP_PHASE_DESCRIBE "$logfile")"; fi
      if [[ "$show_tail" == yes ]]; then
        op_say "$detail running $(op_duration $((SECONDS - started))) of at most $(op_duration "$limit"); last: $(op_last_line "$logfile")"
      else
        op_say "$detail running $(op_duration $((SECONDS - started))) of at most $(op_duration "$limit")"
      fi
      next_beat=$((SECONDS + OP_HEARTBEAT_SECONDS))
    fi
    sleep 1
  done
  wait "$OP_CHILD_PID" || rc=$?
  OP_CHILD_PID=""
  kill "$watchdog" 2>/dev/null || true
  return "$rc"
}

# Sums MUnit/sbt summary lines ("Passed: Total 22, Failed 0, Errors 0, Passed 13, Skipped 9") from a log.
# Prints: "<total> <passed> <failed> <skipped>" (failed includes errors). Prints zeros when no summary was found.
op_count_tests() { # logfile
  sed -e 's/\x1b\[[0-9;?]*[A-Za-z]//g' "$1" | awk '
    /(Passed|Failed): Total [0-9]+/ {
      line = $0
      if (match(line, /Total [0-9]+/)) total += substr(line, RSTART + 6, RLENGTH - 6)
      if (match(line, /Failed [0-9]+/)) failed += substr(line, RSTART + 7, RLENGTH - 7)
      if (match(line, /Errors [0-9]+/)) failed += substr(line, RSTART + 7, RLENGTH - 7)
      if (match(line, /, Passed [0-9]+/)) passed += substr(line, RSTART + 9, RLENGTH - 9)
      if (match(line, /Skipped [0-9]+/)) skipped += substr(line, RSTART + 8, RLENGTH - 8)
      if (match(line, /Ignored [0-9]+/)) skipped += substr(line, RSTART + 8, RLENGTH - 8)
    }
    END { printf "%d %d %d %d\n", total, passed, failed, skipped }'
}

op_show_failure_tail() { # logfile lines
  [[ -s "$1" ]] || return 0
  printf -- '--- last %s lines of %s ---\n' "$2" "${1#"$PWD"/}" >&2
  tail -n "$2" "$1" | tr -d '\r' | sed -e 's/\x1b\[[0-9;?]*[A-Za-z]//g' >&2
  printf -- '--- end of log excerpt ---\n' >&2
}
