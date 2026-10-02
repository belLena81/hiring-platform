#!/usr/bin/env bash
set -euo pipefail
umask 077

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
mode="${1:-stage}"
nonce="${2:-$(openssl rand -hex 8)}"
[[ "$nonce" =~ ^[a-f0-9]{16}$ && ( "$mode" == stage || "$mode" == run || "$mode" == stop ) ]] || {
  printf 'Usage: %s stage [nonce] | run nonce acceptance.json review-directory | stop nonce\n' "$0" >&2
  exit 2
}
project="hiring-streaming-proof-$nonce"
config_dir="$repo_root/.local/config/$project"
log_dir="$repo_root/.local/logs/$project"

if [[ "$mode" == stage ]]; then
  "$repo_root/scripts/run-streaming-analytics-proof.sh" stage "$nonce"
  python3 - "$config_dir/expiry-proof.json" "$nonce" <<'PY'
import json, pathlib, sys
p=pathlib.Path(sys.argv[1]); assert not p.exists() and not p.is_symlink()
p.write_text(json.dumps({'nonce':sys.argv[2], 'purpose':'real-clock-immutable-short-grant-expiry', 'grantSeconds':120}))
p.chmod(0o600)
PY
  printf 'Fresh short-grant proof staged: %s. Rebind current independent HAL evidence and reviews to its identity before run.\n' "$nonce"
  exit 0
fi

for directory in "$config_dir" "$log_dir"; do
  [[ -d "$directory" && ! -L "$directory" && "$(stat -c '%u:%a' "$directory")" == "$(id -u):700" ]] || {
    printf 'Fresh proof directories must be private and owned by this user.\n' >&2
    exit 1
  }
done
for name in state.json expiry-proof.json analytics-operator.conf analytics-runtime.conf analytics.classpath; do
  file="$config_dir/$name"
  [[ -f "$file" && ! -L "$file" && "$(stat -c '%u:%a' "$file")" == "$(id -u):600" ]] || {
    printf 'Short-grant proof configuration is absent or unsafe.\n' >&2
    exit 1
  }
done
python3 - "$config_dir" "$nonce" <<'PY'
import json, pathlib, sys
p=pathlib.Path(sys.argv[1]); marker=json.loads((p/'expiry-proof.json').read_text()); state=json.loads((p/'state.json').read_text())
assert marker=={'nonce':sys.argv[2], 'purpose':'real-clock-immutable-short-grant-expiry', 'grantSeconds':120}
assert state['nonce']==sys.argv[2]
PY

if [[ "$mode" == stop ]]; then
  "$repo_root/scripts/run-streaming-analytics-proof.sh" stop "$nonce"
  exit 0
fi
[[ $# -eq 4 ]] || { printf 'Run requires current identity-bound acceptance and independent review directory.\n' >&2; exit 2; }
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"
export SPARK_LOCAL_IP=127.0.0.1
unset JAVA_TOOL_OPTIONS
analytics_cp="$(cat "$config_dir/analytics.classpath")"
java_run() {
  local conf="$1" main="$2"; shift 2
  (cd analytics && java --add-opens=java.base/sun.security.action=ALL-UNNAMED \
    -Dcats.effect.trackFiberContext=true -Dspark.sql.shuffle.partitions=2 -Dspark.databricks.delta.snapshotPartitions=2 \
    -Dconfig.file="$conf" -cp "$analytics_cp" "$main" "$@")
}
printf 'ISOLATED_STREAMING_EXPIRY_PROFILE master=local[2] configuredShufflePartitions=2 configuredDeltaSnapshotPartitions=2 grantSeconds=120 clock=real guards=unchanged\n' \
  >"$log_dir/expiry-profile.log"
# The production operator provisioner checks all HAL evidence, per-role independent union,
# current source and actual Kafka/runtime identity, and unused storage before immutable insert.
java_run "$config_dir/analytics-operator.conf" com.example.hiring.analytics.cli.StreamingActivationProofMain \
  provision "$3" "$4" proof_operator 120 >"$log_dir/expiry-grant.log" 2>&1
trap '"$repo_root/scripts/run-streaming-analytics-proof.sh" stop "$nonce" >"$log_dir/expiry-stop.log" 2>&1 || true' EXIT
java_run "$config_dir/analytics-runtime.conf" com.example.hiring.analytics.cli.StreamingGrantExpiryProofMain \
  >"$log_dir/expiry-runtime.log" 2>&1
rg -q '^STREAMING_EXPIRY_PRE_EXPIRY_PUBLISHED ' "$log_dir/expiry-runtime.log"
rg -q '^STREAMING_EXPIRY_VERIFIED ' "$log_dir/expiry-runtime.log"
printf 'ISOLATED_STREAMING_EXPIRY_PASS nonce=%s grantSeconds=120 clock=real existingGrantsUnchanged=true logs=%s\n' "$nonce" "$log_dir"
