#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"

python3 scripts/check-skills.py
sbt test
printf '%s\n' 'Unit checks passed. Run any additional integration, migration, compatibility, or performance checks required by this task; see docs/engineering-quality.md.'
