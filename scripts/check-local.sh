#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"

python3 scripts/check-skills.py

java_command="${JAVA_HOME:+$JAVA_HOME/bin/}java"
java_version="$("$java_command" -XshowSettings:properties -version 2>&1 | awk '/java.specification.version =/ { print $3 }')"
java_major="${java_version#1.}"
if [[ ! "$java_major" =~ ^[0-9]+$ ]] || (( java_major < 17 )); then
  printf '%s\n' 'Java 17 or newer is required. Set JAVA_HOME to your JDK installation before running local checks.' >&2
  exit 1
fi

sbt test
printf '%s\n' 'Unit checks passed. Run any additional integration, migration, compatibility, or performance checks required by this task; see docs/engineering-quality.md.'
