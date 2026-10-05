#!/usr/bin/env bash
# Existing independently activated nonce fixture only; never provisions grants or changes source/checkpoint identity.
set -euo pipefail
umask 077
export JAVA_HOME="${HIRING_ANALYTICS_JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"
export PATH="$JAVA_HOME/bin:$PATH"
unset JAVA_TOOL_OPTIONS _JAVA_OPTIONS JDK_JAVA_OPTIONS
[[ -x "$JAVA_HOME/bin/java" ]] || { printf 'Java 17 runtime unavailable.\n' >&2; exit 2; }
nonce="${1:?isolated fixture nonce required}"
boundary="${2:?durable boundary required}"
[[ "$nonce" =~ ^[a-f0-9]{16}$ ]] || exit 2
case "$boundary" in Prepared|BronzeCommitted|SilverCommitted|LateFactsCommitted|IngestionCommitted|PublicationCommitted|TerminalCommitted) ;; *) exit 2;; esac
repository="$(pwd -P)"
config_dir="$repository/.local/config/hiring-streaming-proof-$nonce"
log_dir="$repository/.local/logs/hiring-streaming-proof-$nonce"
output="$log_dir/process-recovery/$boundary"
[[ -d "$config_dir" && ! -L "$config_dir" && -d "$log_dir" && ! -L "$log_dir" ]] || exit 2
for private_file in "$config_dir/state.json" "$config_dir/analytics-runtime.conf" "$config_dir/analytics.classpath"; do
  [[ -f "$private_file" && ! -L "$private_file" && "$(stat -c '%u:%a' "$private_file")" == "$(id -u):600" ]] || exit 2
done
python3 - "$repository" "$config_dir" "$log_dir" "$output" <<'PY_CHECK'
import os, pathlib, sys
root,config,logs,output=map(pathlib.Path,sys.argv[1:])
for target in (root,config,logs,output):
    assert target.is_absolute()
    for path in (target,*target.parents):
        assert not path.is_symlink(), 'symlink ancestor rejected'
for path in (config,logs):
    stat=path.stat()
    assert stat.st_uid==os.getuid() and stat.st_mode & 0o777==0o700
parent=output.parent
if parent.exists():
    stat=parent.stat()
    assert parent.is_dir() and stat.st_uid==os.getuid() and stat.st_mode & 0o777==0o700
PY_CHECK
[[ ! -e "$output" ]] || { printf 'Recovery output exists; preserve it and stage a fresh nonce.\n' >&2; exit 2; }
mkdir -p "$output"
chmod 700 "$output"
analytics_cp="$(cat "$config_dir/analytics.classpath")"
main=com.example.hiring.analytics.cli.StreamingProcessRecoveryProofMain
run_main() {
  java -Dspark.sql.shuffle.partitions=2 -Dspark.databricks.delta.snapshotPartitions=2 \
    --add-opens=java.base/sun.security.action=ALL-UNNAMED \
    -Dconfig.file="$config_dir/analytics-runtime.conf" -cp "$analytics_cp" "$main" "$1" "$boundary" "$output"
}
# Establish a recent durable watermark on the same nonce before appending the closed-day fixture.
# This also proves terminal-before-checkpoint process loss on its own exact twelve source coordinates.
if [[ "$boundary" == LateFactsCommitted && ! -f "$log_dir/process-recovery/TerminalCommitted/second-restart.json" ]]; then
  "$repository/scripts/run-streaming-recovery-proof.sh" "$nonce" TerminalCommitted
fi
# Seed exactly twelve committed source records before query bootstrap.
run_main seed >"$output/seed.log" 2>&1
java -Dspark.sql.shuffle.partitions=2 -Dspark.databricks.delta.snapshotPartitions=2 \
  --add-opens=java.base/sun.security.action=ALL-UNNAMED \
  -Dconfig.file="$config_dir/analytics-runtime.conf" -cp "$analytics_cp" "$main" pause "$boundary" "$output" \
  >"$output/original.log" 2>&1 &
original_pid=$!
cleanup() {
  if kill -0 "$original_pid" 2>/dev/null; then kill -TERM "$original_pid"; wait "$original_pid" || true; fi
}
trap cleanup EXIT
for ((attempt=0; attempt<1800; attempt++)); do
  [[ -f "$output/ready.json" ]] && break
  kill -0 "$original_pid" 2>/dev/null || { wait "$original_pid"; exit 1; }
  sleep 0.1
done
[[ -f "$output/ready.json" ]] || { printf 'Actual durable crash barrier was not reached.\n' >&2; exit 1; }
python3 - "$output/ready.json" "$nonce" "$boundary" "$original_pid" <<'PY'
import json, pathlib, sys
path=pathlib.Path(sys.argv[1])
assert not path.is_symlink() and path.stat().st_size <= 65536
value=json.loads(path.read_text())
assert value['nonce']==sys.argv[2] and value['boundary']==sys.argv[3] and value['pid']==int(sys.argv[4])
assert value['checkpointAcknowledged'] is False and value['records']>0
PY
# The exact child PID is still running at its durable barrier. No process group, name or discovered PID is killed.
kill -KILL "$original_pid"
set +e
wait "$original_pid"
original_exit=$?
set -e
[[ "$original_exit" == 137 ]] || exit 1
! kill -0 "$original_pid" 2>/dev/null || exit 1
trap - EXIT
printf 'originalExit=%s originalPid=%s\n' "$original_exit" "$original_pid" >"$output/process-exit.txt"
run_main release-owner >"$output/owner-recovery.log" 2>&1
run_main recover >"$output/restart.log" 2>&1
run_main recover >"$output/second-restart.log" 2>&1
[[ -f "$output/second-restart.json" ]] || exit 1
printf 'STREAMING_PROCESS_RECOVERY_COMPONENT_PASSED boundary=%s nonce=%s\n' "$boundary" "$nonce"
