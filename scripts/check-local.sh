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

# The sbt launcher uses the PATH java unless told otherwise; both builds target Java 17.
sbt_args=()
if [[ -n "${JAVA_HOME:-}" ]]; then
  sbt_args=(-java-home "$JAVA_HOME")
fi

# Both builds: formatting gates first, then the Docker-independent unit suites under JaCoCo.
# `jacoco` runs the unit tests once and fails when line coverage is below the build's threshold (60%).
sbt "${sbt_args[@]}" scalafmtCheckAll scalafmtSbtCheck jacoco
(cd analytics && sbt "${sbt_args[@]}" scalafmtCheckAll scalafmtSbtCheck jacoco)

printf '%s\n' 'Formatting, unit and coverage checks passed for the application and analytics builds. Run any additional integration, migration, compatibility, or performance checks required by this task; see docs/engineering-quality.md.'
