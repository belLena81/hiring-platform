#!/usr/bin/env bash
# Opt-in supply-chain check for locally built hiring images: vulnerability and secret scan plus a CycloneDX SBOM.
# Requires a locally installed trivy (https://trivy.dev); it downloads its vulnerability database on first use.
# Fails when an image has a fixable HIGH or CRITICAL vulnerability or an embedded secret.
# Contract: see "Operator scripts" in docs/engineering-quality.md. Run with --help for usage and exit codes.
set -Eeuo pipefail

project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/operator-lib.sh
source "$project_root/scripts/operator-lib.sh"

readonly report_directory="$project_root/.local/data/image-scans"
readonly report_display=".local/data/image-scans"
step_limit="${SCAN_TIMEOUT_SECONDS:-900}"
readonly default_images=(hiring-api:local hiring-analytics:local)
result_done=0
scanned=0
total_images=0
current_step="starting"

help_text() {
  cat <<'HELP'
Usage: scripts/scan-container-images.sh [image ...]
       scripts/scan-container-images.sh --help

For each image (default: hiring-api:local hiring-analytics:local) writes a CycloneDX SBOM and scans for fixable HIGH or
CRITICAL vulnerabilities and embedded secrets with trivy. Needs trivy and a local Docker image; no image is built or pulled.
The first run downloads trivy's vulnerability database. Typical duration: 1-5 min per image. Each trivy call is limited to
SCAN_TIMEOUT_SECONDS (default 900); a status line is printed every 10 s. Reports go to .local/data/image-scans/.

Exit codes: 0 PASS, 1 FAIL (findings, scanner error or time limit), 2 usage error or precondition not met (BLOCKED:
trivy, Docker or an image is missing), 130 interrupted (the running trivy process is stopped; partial reports stay).
The last line is "RESULT: PASS|FAIL|BLOCKED images=<scanned>/<total> ..." with duration.

Examples:
  scripts/scan-container-images.sh
  SCAN_TIMEOUT_SECONDS=1800 scripts/scan-container-images.sh hiring-api:local
HELP
}

finish() { # RESULT code detail
  local line="RESULT: $1 images=$scanned/$total_images $3 duration=$(op_elapsed) reports=$report_display"
  result_done=1
  if [[ "$1" == PASS ]]; then printf '%s\n' "$line"; else printf '%s\n' "$line" >&2; fi
  exit "$2"
}
blocked() {
  printf 'scan-container-images: %s\n' "$1" >&2
  printf 'scan-container-images: next: %s\n' "$2" >&2
  finish BLOCKED 2 "reason=precondition"
}
on_interrupt() {
  trap '' INT TERM HUP
  printf '\n' >&2
  op_say "interrupted during $current_step; $scanned of $total_images images fully scanned; stopping the trivy process this script started" >&2
  op_stop_child 10
  result_done=1
  exit 130
}
on_exit() {
  local code=$?
  if (( code != 0 && result_done == 0 )); then
    printf 'scan-container-images: unexpected failure during %s (exit %s)\n' "$current_step" "$code" >&2
    printf 'scan-container-images: next: read the message above, then run again\n' >&2
    printf 'RESULT: FAIL images=%s/%s duration=%s\n' "$scanned" "$total_images" "$(op_elapsed)" >&2
  fi
}
trap on_interrupt INT TERM HUP
trap on_exit EXIT

case "${1:-}" in
  -h|--help) help_text; result_done=1; exit 0 ;;
esac

images=("$@")
if [[ ${#images[@]} -eq 0 ]]; then
  images=("${default_images[@]}")
fi
total_images=${#images[@]}
[[ "$step_limit" =~ ^[0-9]{1,5}$ ]] && (( 10#$step_limit >= 30 && 10#$step_limit <= 86400 )) ||
  { printf 'scan-container-images: SCAN_TIMEOUT_SECONDS must be an integer from 30 to 86400, got: %s\n' "$step_limit" >&2; finish BLOCKED 2 "reason=usage"; }
step_limit=$((10#$step_limit))
for image in "${images[@]}"; do
  [[ "$image" =~ ^[A-Za-z0-9][A-Za-z0-9._/:@-]*$ ]] ||
    { printf 'scan-container-images: invalid image reference (letters, digits and . _ / : @ - only, no leading dash): %s\n' "$image" >&2
      printf 'scan-container-images: run scripts/scan-container-images.sh --help for usage\n' >&2
      finish BLOCKED 2 "reason=usage"; }
done

printf 'scan-container-images: SBOM + vulnerability/secret scan of %s image(s) [%s]; each trivy call limited to %s; expected 1-5 min per image; reports in %s\n' \
  "$total_images" "${images[*]}" "$(op_duration "$step_limit")" "$report_display"

command -v trivy >/dev/null 2>&1 ||
  blocked "trivy is not installed" "install trivy (https://trivy.dev/latest/getting-started/installation/), then run again"
command -v docker >/dev/null 2>&1 && command -v setsid >/dev/null 2>&1 ||
  blocked "docker and setsid are required" "install them and put them on PATH"
git -C "$project_root" check-ignore -q "$report_directory/x" ||
  blocked "$report_display is not git-ignored" "add .local/ to .gitignore, then run again"
mkdir -p "$report_directory"

# scan_step LABEL LOGFILE COMMAND...: bounded trivy call with a heartbeat; ends the run on failure.
scan_step() {
  local label="$1" log="$2" rc=0
  shift 2
  current_step="$label"
  op_say "$label"
  op_run_logged "$label" "$step_limit" "$log" yes "$@" || rc=$?
  if (( rc == 0 )); then return 0; fi
  if (( rc == 124 )); then
    printf 'scan-container-images: "%s" exceeded %s and was stopped\n' "$label" "$(op_duration "$step_limit")" >&2
    printf 'scan-container-images: next: check network access for the trivy database download, or raise SCAN_TIMEOUT_SECONDS\n' >&2
  else
    op_show_failure_tail "$log" 60
    printf 'scan-container-images: "%s" failed (trivy exit %s)\n' "$label" "$rc" >&2
    printf 'scan-container-images: next: fix or rebuild the image for the findings above, or read %s/%s\n' "$report_display" "$(basename "$log")" >&2
  fi
  finish FAIL 1 "failed=\"$label\""
}

for image in "${images[@]}"; do
  current_step="checking image $image"
  timeout --foreground 20 docker image inspect "$image" >/dev/null 2>&1 ||
    blocked "image not found locally (or Docker is not reachable): $image" "build it first (docker compose build) or pass an existing image"
  report_name="${image//[\/:@]/_}"
  scan_step "SBOM for $image" "$report_directory/$report_name.sbom.log" \
    trivy image --quiet --timeout "${step_limit}s" --format cyclonedx --output "$report_directory/$report_name.sbom.json" "$image"
  scan_step "vulnerability and secret scan of $image" "$report_directory/$report_name.scan.log" \
    trivy image --quiet --timeout "${step_limit}s" --scanners vuln,secret --severity HIGH,CRITICAL --ignore-unfixed --exit-code 1 "$image"
  scanned=$((scanned + 1))
  op_say "image ok: $image"
done
finish PASS 0 "findings=none"
