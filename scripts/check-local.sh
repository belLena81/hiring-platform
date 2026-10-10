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

# Both builds: formatting gates first, then unit and integration suites under JaCoCo.
# The integration suites need Docker; the runner owns this workspace's test services.
# A full run fails when merged unit + integration line coverage is below 85%.
sbt "${sbt_args[@]}" scalafmtCheckAll scalafmtSbtCheck
(cd analytics && sbt "${sbt_args[@]}" scalafmtCheckAll scalafmtSbtCheck)
export HIRING_TEST_JAVA_HOME="${HIRING_TEST_JAVA_HOME:-${JAVA_HOME:-}}"
scripts/run-local-tests.sh test
scripts/run-local-tests.sh analytics

printf '%s\n' 'Formatting, unit, integration and merged coverage checks passed for the application and analytics builds. Run any additional migration, compatibility, or performance checks required by this task; see docs/engineering-quality.md.'
