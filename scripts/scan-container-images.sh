#!/usr/bin/env bash
# Opt-in supply-chain check for locally built hiring images: vulnerability and secret scan plus a CycloneDX SBOM.
# Requires a locally installed trivy (https://trivy.dev); it downloads its vulnerability database on first use.
# Usage: scripts/scan-container-images.sh [image ...]   (default: hiring-api:local hiring-analytics:local)
# Fails when an image has a fixable HIGH or CRITICAL vulnerability or an embedded secret.
set -euo pipefail

project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
report_directory="$project_root/.local/data/image-scans"

if ! command -v trivy >/dev/null 2>&1; then
  echo "scan-container-images: trivy is not installed; see https://trivy.dev/latest/getting-started/installation/" >&2
  exit 2
fi

images=("$@")
if [[ ${#images[@]} -eq 0 ]]; then
  images=(hiring-api:local hiring-analytics:local)
fi

mkdir -p "$report_directory"
for image in "${images[@]}"; do
  report_name="${image//[\/:]/_}"
  trivy image --quiet --format cyclonedx --output "$report_directory/$report_name.sbom.json" "$image"
  trivy image --quiet --scanners vuln,secret --severity HIGH,CRITICAL --ignore-unfixed --exit-code 1 "$image"
done
echo "Image scans passed; SBOMs are in .local/data/image-scans/."
