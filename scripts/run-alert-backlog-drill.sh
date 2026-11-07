#!/usr/bin/env bash
# OAV-03 and OAV-04 drills: a stalled outbox backlog and a MongoDB outage, followed through Prometheus and Alertmanager
# to the local sink (spec: docs/specs/operational-alert-verification.md). Needs Docker, curl and the running local stack,
# so a human runs it outside the agent sandbox. Contract: "Operator scripts" in docs/engineering-quality.md.
#
# Usage: scripts/run-alert-backlog-drill.sh [--scenario stalled-backlog|mongo-outage|mongo-outage-during-stall|all]
#   stalled-backlog (default, OAV-03.1 and 03.2, about 9 minutes)
#   mongo-outage (OAV-04.1 on a healthy backlog, about 3.5 minutes)
#   mongo-outage-during-stall (OAV-04.1 while the age alert is already firing, roughly 20 minutes, never run)
#   all = stalled-backlog, then mongo-outage (the second is skipped if the first recorded a failure)
#   Each can take longer when waits time out.
#
# Stall method (Q2): the containerised API runs with KAFKA_ENABLED=false, so no relay claims outbox rows. Any committed
# write that records an operational event leaves a claimable event_outbox row, with no production code change. The drill
# creates one job by construction: scripts/seed-local-jobs.sh --one-fresh-job creates and publishes exactly one job with
# random idempotency keys (no replay, no dependence on earlier seeds or their 7-day receipts). If the check afterwards
# finds any other number of perf-recruiter jobs since the start (another writer), the drill ends BLOCKED, nothing deleted.
# The alert threshold is NOT shortened: the age rule needs the oldest row older than 300 s for 2 more minutes, so
# firing comes at least 420 s after the row.
#
# Release scope. The database clock is read just before seeding (db.hello().localTime). The release selects rows by
# identity and time, collects their _id values once, lets scripts/alert_release_scope.py decide, and deletes by those
# _id values only, so rows written between the check and the delete are never deleted:
#   jobs           perf-recruiter's, createdAt >= start: exactly one must exist (another writer's job makes it refuse)
#   event_outbox   aggregateType Job, aggregateId = that job, state Retryable, occurredAt >= start (at most 4 rows)
#   embedding_work _id "Job:<that job>", state Ready or Retry, createdAt >= start (at most 2 rows; zero is allowed)
# It refuses (BLOCKED, exit 2, nothing deleted, inspection command printed) when the scope does not hold. This narrows
# the deletion to the seeded job; it does NOT guarantee nothing else wrote to that job during the run (the assumption is
# no other writer touches the job the seed just created). The identity filter was run once by the user on 2026-11-06
# (jobs=1, outbox=1, embedding_work=0, ALLOW, deleted by id 1 and 0); a mismatch makes it refuse, never delete more.
#
# Side effects that stay: the seeded job, the synthetic recruiter and candidate accounts, and a refresh of the tokens and
# passwords in .local/data/perf-seed.env (mode 0600) written by the seed. The release may delete the embedding_work row
# of a job that stays, so that job is left without queued embedding work. The stack returns to an empty backlog, not
# exactly to its starting state. MongoDB is restarted if the drill stopped it.
#
# Exit codes: 0 PASS, 1 FAIL, 2 BLOCKED (precondition, usage or refused release), 130 interrupted. Last line: "RESULT: ...".
# Verified by the user on 2026-11-07 (one workstation, local runs): stalled-backlog PASS (3 criteria, 8m39s, seed
# --one-fresh-job, release scope jobs=1 outbox=1 embedding_work=0 ALLOW, real-alert label check) and mongo-outage PASS
# (2 criteria, 3m17s). Earlier versions were run on 2026-11-06 (8m33s and 3m32s).
# Never exercised by a run: mongo-outage-during-stall, the failed-delete path, the release refusal path beyond
# scripts/tests/test_alert_release_scope.py, verdicts printed before a BLOCKED exit, the --scenario all skip path,
# and exact-name matching against more than the sink logs of those runs.
set -Eeuo pipefail
umask 077
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$repo_root"
# shellcheck source=scripts/operator-lib.sh
source "$repo_root/scripts/operator-lib.sh"
# shellcheck source=scripts/alert-delivery-lib.sh
source "$repo_root/scripts/alert-delivery-lib.sh"

readonly age_alert="HiringBacklogOldestPendingTooOld"
readonly unobserved_alert="HiringBacklogUnobserved"
readonly outbox_capability="operational_event_outbox"
readonly seed_recruiter_name="perf-recruiter" # RECRUITER in scripts/seed-local-jobs.py
# The API's database is mongo.database = "hiring" in src/main/resources/application.conf, with no environment override
# in the container. compose.yaml's MONGODB_DATABASE default "hiring" (analytics services only) matches it. One variable.
readonly mongo_database="hiring"
readonly min_firing_seconds=410 # 300 s threshold + 120 s for, minus 10 s clock tolerance
readonly iso_time='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+Z$'
scenario="stalled-backlog"
while (( $# > 0 )); do
  case "$1" in
    -h|--help) sed -n '2,42p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    --scenario)
      (( $# >= 2 )) || { echo "alert-backlog-drill: --scenario needs a value" >&2; echo "RESULT: BLOCKED reason=usage" >&2; exit 2; }
      scenario="$2"; shift 2 ;;
    *) printf 'alert-backlog-drill: unexpected argument: %s (try --help)\nRESULT: BLOCKED reason=usage\n' "$1" >&2; exit 2 ;;
  esac
done
case "$scenario" in stalled-backlog|mongo-outage|mongo-outage-during-stall|all) ;; *)
  printf 'alert-backlog-drill: unknown scenario: %s\nRESULT: BLOCKED reason=usage\n' "$scenario" >&2; exit 2 ;;
esac

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
evidence_dir_rel=".local/logs/alert-drills/$stamp-backlog"
mkdir -p -m 700 "$repo_root/$evidence_dir_rel"
AD_EVIDENCE="$repo_root/$evidence_dir_rel/evidence.md"
printf '# Backlog alert drills (OAV-03, OAV-04), scenario %s, started %s\n\n## Timeline\n\n' "$scenario" "$stamp" >"$AD_EVIDENCE"

failures=0
declare -a verdicts=()
verdict() { # ID PASS|FAIL detail
  verdicts+=("$1 $2 $3")
  [[ "$2" == PASS ]] || failures=$((failures + 1))
  ad_mark "verdict $1" "$2 $3"
}
finished=0
stall_active=0
stall_db_start=""
stall_row_time=""
stall_row_epoch=""
mongo_stopped=0

mongo_js() { ad_compose exec -T mongodb mongosh --quiet --eval "$1"; }
mongo_ready() { mongo_js 'quit(db.hello().isWritablePrimary ? 0 : 1)'; }
pending_outbox_rows() { mongo_js "print(db.getSiblingDB('$mongo_database').event_outbox.countDocuments({state: 'Retryable'}))" | tr -d '\r'; }
# The database server's clock (hello.localTime), not the workstation's.
database_time() { mongo_js 'print(db.hello().localTime.toISOString())' | tr -d '\r'; }
# The oldest pending outbox row of the seeded job (identity and time scoped as in the release).
oldest_outbox_time() {
  mongo_js "$(job_scope_js) const r = d.event_outbox.find({state: 'Retryable', occurredAt: {\$gte: t}, aggregateType: 'Job', aggregateId: {\$in: jobs}}).sort({occurredAt: 1}).limit(1).toArray()[0]; print(r ? new Date(r.occurredAt).toISOString() : '')" | tr -d '\r'
}
# The job scope shared by every query below: jobs of the seed recruiter (perf-recruiter) created at or after the database
# clock start. Defines d, t, and jobs (the string ids). Another recruiter's jobs never count.
job_scope_js() {
  printf "%s" "const d = db.getSiblingDB('$mongo_database'); const t = new Date('$stall_db_start'); const u = d.users.findOne({name: '$seed_recruiter_name'}); const rid = u ? String(u._id) : null; const jobs = rid ? d.jobs.find({recruiterId: rid, createdAt: {\$gte: t}}, {_id: 1}).toArray().map(x => String(x._id)) : [];"
}
# Fails (non-zero, no output) for anything that is not a plain ISO time, never relying on how date treats an empty string.
epoch_of() { [[ "$1" =~ $iso_time ]] && date -u -d "$1" +%s; }

# backlog_note : read-only facts for a BLOCKED message: how many pending outbox rows exist and the oldest occurredAt.
backlog_note() {
  local count oldest
  count="$(pending_outbox_rows 2>/dev/null || echo '?')"
  oldest="$(mongo_js "const d = db.getSiblingDB('$mongo_database').event_outbox.find({state: 'Retryable'}).sort({occurredAt: 1}).limit(1).toArray()[0]; print(d ? new Date(d.occurredAt).toISOString() : 'none')" 2>/dev/null | tr -d '\r' || echo '?')"
  printf 'pending event_outbox rows (state Retryable): %s; oldest occurredAt: %s. Clearing old local rows is your decision; this script never deletes rows outside the seeded job it created.' "$count" "$oldest"
}

# release_scope_json : one mongosh evaluation that collects the ids of the drill's scope (see the header).
release_scope_json() {
  mongo_js "$(job_scope_js) const o = d.event_outbox.find({state: 'Retryable', occurredAt: {\$gte: t}, aggregateType: 'Job', aggregateId: {\$in: jobs}}, {_id: 1}).toArray().map(x => String(x._id)); const e = d.embedding_work.find({state: {\$in: ['Ready', 'Retry']}, createdAt: {\$gte: t}, _id: {\$in: jobs.map(j => 'Job:' + j)}}, {_id: 1}).toArray().map(x => String(x._id)); print(JSON.stringify({jobsCreated: jobs.length, outboxIds: o, embeddingIds: e}))" | tr -d '\r'
}

# print_manual_cleanup : how to inspect the drill's rows by hand (stderr).
print_manual_cleanup() {
  printf 'alert-backlog-drill: inspect the rows, then remove them yourself if they are the drill'"'"'s:\n' >&2
  printf "  docker compose -f compose.yaml -f compose.observability.yaml --profile api exec -T mongodb mongosh --quiet --eval \"const d = db.getSiblingDB('%s'); const t = new Date('%s'); printjson(d.jobs.find({createdAt: {\\\$gte: t}}, {title: 1, recruiterId: 1}).toArray()); printjson(d.event_outbox.find({state: 'Retryable', occurredAt: {\\\$gte: t}}, {aggregateId: 1, eventType: 1}).toArray()); printjson(d.embedding_work.find({createdAt: {\\\$gte: t}}).toArray())\"\n" "$mongo_database" "$stall_db_start" >&2
}

# release_stall : deletes exactly the ids the decision allowed. Returns 3 and deletes nothing when it refuses; returns 4
# and keeps stall_active set (the exit trap tries once more) when the delete call itself failed.
release_stall() {
  local scope decision counts
  scope="$(release_scope_json)" || scope=""
  decision="$(printf '%s' "$scope" | python3 -I "$repo_root/scripts/alert_release_scope.py")" || true
  ad_mark "release scope" "$(python3 -I -c 'import json,sys; s=json.loads(sys.argv[1]); print("jobs=%d outbox=%d embedding_work=%d" % (s["jobsCreated"], len(s["outboxIds"]), len(s["embeddingIds"])))' "${scope:-{\}}" 2>/dev/null || echo unreadable) since database time $stall_db_start -> ${decision:-no decision}"
  if [[ "$decision" != ALLOW ]]; then
    stall_active=0
    printf 'alert-backlog-drill: refusing to delete: %s\nalert-backlog-drill: nothing was deleted.\n' "${decision:-no decision}" >&2
    print_manual_cleanup
    return 3
  fi
  counts="$(mongo_js "const d = db.getSiblingDB('$mongo_database'); const t = new Date('$stall_db_start'); const s = $scope; print(d.event_outbox.deleteMany({_id: {\$in: s.outboxIds}, state: 'Retryable', occurredAt: {\$gte: t}}).deletedCount + ' ' + d.embedding_work.deleteMany({_id: {\$in: s.embeddingIds}, state: {\$in: ['Ready', 'Retry']}, createdAt: {\$gte: t}}).deletedCount)" | tr -d '\r')" || counts=""
  if ! [[ "$counts" =~ ^[0-9]+\ [0-9]+$ ]]; then
    ad_mark "delete failed" "the delete call returned no usable count ('${counts:0:40}'); the rows may still be there"
    printf 'alert-backlog-drill: delete failed: the rows of the seeded job may still be pending.\n' >&2
    print_manual_cleanup
    return 4
  fi
  stall_active=0
  ad_mark "stall released" "deleted rows by id: outbox/embedding_work = $counts"
}

# release_or_block : release_stall, ending the drill as BLOCKED (exit 2) if it refuses, or FAIL (exit 1) if the delete failed.
release_or_block() {
  local rc=0
  release_stall || rc=$?
  case "$rc" in
    0) ;;
    4) finished=1; print_verdicts
       printf 'RESULT: FAIL reason=release-delete-failed evidence=%s/evidence.md\n' "$evidence_dir_rel" >&2
       exit 1 ;;
    *) finished=1; print_verdicts
       printf 'RESULT: BLOCKED reason=release-refused evidence=%s/evidence.md\n' "$evidence_dir_rel" >&2
       exit 2 ;;
  esac
}

# print_verdicts : the verdicts recorded so far, to the console and the evidence file (also before a BLOCKED/FAIL exit,
# so earlier passes stay visible).
print_verdicts() {
  (( ${#verdicts[@]} > 0 )) || return 0
  printf '\n## Verdicts\n\n' >>"$AD_EVIDENCE"
  local v
  for v in "${verdicts[@]}"; do printf -- '- %s\n' "$v" | tee -a "$AD_EVIDENCE"; done
}

cleanup() {
  local code=$?
  trap '' INT TERM HUP # a second Ctrl-C must not leave MongoDB stopped or the release half done
  trap - EXIT
  op_ticker_stop 2>/dev/null || true
  if (( mongo_stopped )); then
    ad_compose up -d --wait mongodb >/dev/null 2>&1 || ad_compose start mongodb >/dev/null 2>&1 || true
    mongo_stopped=0
  fi
  if (( stall_active )); then
    ad_wait 120 3 mongo-ready mongo_ready || true
    release_stall || echo "alert-backlog-drill: the drill's pending rows were not removed (see above)" >&2
  fi
  ad_compose ps --status running --services 2>/dev/null | grep -qx mongodb \
    || echo "alert-backlog-drill: WARNING: MongoDB is not running; start it with: docker compose -f compose.yaml --profile api up -d mongodb" >&2
  if (( finished == 0 )); then
    printf 'RESULT: FAIL phase=interrupted-or-error exit=%s evidence=%s/evidence.md\n' "$code" "$evidence_dir_rel" >&2
    exit "${code/#0/1}"
  fi
}
trap cleanup EXIT
trap 'exit 130' INT TERM HUP

blocked() {
  printf 'alert-backlog-drill: %s\n' "$1" >&2
  finished=1
  print_verdicts # a second scenario's BLOCKED must not hide the first scenario's recorded passes
  printf 'RESULT: BLOCKED reason=precondition evidence=%s/evidence.md\n' "$evidence_dir_rel" >&2
  exit 2
}

# delivered_for SINCE RECEIVER ALERT ALERT_STATUS : the sink saw that alert with that status for the outbox capability.
delivered_for() { ad_delivered "$1" "$2" "$4" "$3" "$outbox_capability"; }
delivered_any() { ad_delivered "$1" "" "$3" "$2"; } # SINCE ALERT ALERT_STATUS : any receiver
age_firing() { [[ "$(ad_alert_firing "$age_alert" "$outbox_capability")" -ge 1 ]]; }
age_quiet() { [[ "$(ad_alert_firing "$age_alert" "$outbox_capability")" == 0 ]]; }
unobserved_firing() { [[ "$(ad_alert_firing "$unobserved_alert")" -ge 1 ]]; }
unobserved_quiet() { [[ "$(ad_alert_firing "$unobserved_alert")" == 0 ]]; }
alert_held() { [[ "$(ad_active_alert_state "$1")" == active ]]; }

# inspect_real_alert NAME : once a REAL alert is held by Alertmanager, run the D7 label allowlist on it and record how
# many alerts of that name were inspected, so OAV-02.5 never rests on a stale alert that happened to be firing.
inspect_real_alert() {
  local name="$1" held="" found count all_violations
  ad_wait 60 3 "$name to be held by Alertmanager" alert_held "$name" && held=1
  if [[ -z "$held" ]]; then
    verdict "OAV-02.5-real($name)" FAIL "Alertmanager did not hold the real alert, so its labels could not be inspected"
    return 0
  fi
  # Fail closed: a failed Alertmanager query is not "no violations".
  if ! count="$(ad_alert_count "$name")" || ! all_violations="$(ad_label_violations)"; then
    verdict "OAV-02.5-real($name)" FAIL "the Alertmanager query for the label check failed, so the labels were not inspected"
    return 0
  fi
  found="$(grep -F " on $name" <<<"$all_violations" || true)"
  if [[ -z "$found" && "$count" -ge 1 ]]; then
    verdict "OAV-02.5-real($name)" PASS "inspected $count real alert(s) named $name: label names in the D7 allowlist, closed values valid"
  else
    verdict "OAV-02.5-real($name)" FAIL "inspected=$count violations: ${found:-none}"
  fi
}

preconditions() {
  local reason
  reason="$(ad_require_stack)" || blocked "$reason"
  ad_curl http://127.0.0.1:8080/health >/dev/null 2>&1 || blocked "the API does not answer on 127.0.0.1:8080 (start it with --profile api)"
  ad_compose ps --status running --services 2>/dev/null | grep -qx mongodb || blocked "the mongodb service is not running"
  [[ "$(ad_alert_firing "$age_alert")" == 0 && "$(ad_alert_firing "$unobserved_alert")" == 0 ]] \
    || blocked "a backlog alert is already firing; resolve it first so the timeline is unambiguous. $(backlog_note)"
  [[ "$(ad_prom_series 'hiring_backlog_oldest_pending_age_seconds')" == 0 ]] \
    || blocked "a backlog already has an oldest pending item; drain it first (the drill must own the stalled rows). $(backlog_note)"
  [[ "$(ad_prom_series 'hiring_backlog_sample_age_seconds')" -ge 1 ]] \
    || blocked "no backlog sample is exported yet; wait for the API to start sampling"
  [[ "$(pending_outbox_rows)" == 0 ]] || blocked "event_outbox already has pending (Retryable) rows. $(backlog_note)"
}

# stall_begin : creates exactly one job through the public API, so one job's rows are pending (see the header).
stall_begin() {
  local out created_json created
  # The database clock, read immediately before seeding, scopes the release to this drill's job.
  stall_db_start="$(database_time)"
  [[ "$stall_db_start" =~ $iso_time ]] || blocked "could not read the database clock"
  ad_mark "database clock at stall start" "$stall_db_start"
  stall_active=1
  out="$repo_root/$evidence_dir_rel/seed-local-jobs.log"
  # --one-fresh-job creates and publishes exactly one new job with random idempotency keys. It does not depend on the
  # seed's earlier jobs or on their idempotency receipts (which expire after 7 days), so it can never replay or create many.
  ad_mark "stall seeding started" "scripts/seed-local-jobs.sh --one-fresh-job (log: $evidence_dir_rel/seed-local-jobs.log)"
  local seeded=0
  "$repo_root/scripts/seed-local-jobs.sh" --one-fresh-job --candidates 1 >"$out" 2>&1 || seeded=1
  created_json="$(release_scope_json)" || created_json=""
  created="$(python3 -I -c 'import json,sys; print(int(json.loads(sys.argv[1])["jobsCreated"]))' "${created_json:-{\}}" 2>/dev/null || echo -1)"
  (( seeded == 0 )) || blocked "scripts/seed-local-jobs.sh failed (see $evidence_dir_rel/seed-local-jobs.log); nothing is deleted unless the scope check allows it on exit"
  (( created == 1 )) || blocked "the stall must be exactly one job but $created job(s) were created since the database-clock start; nothing is deleted (see the refusal message on exit)"
  [[ "$(pending_outbox_rows)" -ge 1 ]] || blocked "seeding wrote no pending outbox row, so no stall exists (the API may not record events); stop and report"
  stall_row_time="$(oldest_outbox_time)"
  [[ "$stall_row_time" =~ $iso_time ]] || blocked "the seeded job has no pending outbox row with a readable time (got '${stall_row_time:0:40}'); stop and report"
  stall_row_epoch="$(epoch_of "$stall_row_time")" || blocked "could not convert the stall row time to epoch seconds"
  stall_since="$(ad_now)"
  ad_mark "stall start (oldest pending row of the seeded job)" "occurredAt=$stall_row_time"
}

# wait_age_alert_firing : waits for the age alert in Prometheus, then for the ticket delivery.
wait_age_alert_firing() { # sets fired_epoch
  local limit=780 started="$SECONDS" pending_marked=0 seen
  while (( SECONDS - started < limit )); do
    seen="$(ad_prom_series "ALERTS{alertname=\"$age_alert\",hiring_capability=\"$outbox_capability\"}")" || seen=0
    if (( seen >= 1 && pending_marked == 0 )); then ad_mark "age alert pending in Prometheus" ""; pending_marked=1; fi
    if age_firing; then fired_epoch="$(date -u +%s)"; ad_mark "age alert firing in Prometheus" "$((fired_epoch - stall_row_epoch)) s after the row occurred"; return 0; fi
    sleep 5
  done
  return 1
}

run_stalled_backlog() {
  local fired_epoch=0 elapsed=-1 delivered=1
  ad_mark "scenario" "stalled-backlog (OAV-03)"
  preconditions
  stall_begin
  if ! wait_age_alert_firing; then
    verdict OAV-03.1 FAIL "the age alert did not fire within 13 minutes of the stall start"
    return 0
  fi
  # Fail closed: an unusable row epoch leaves elapsed at -1, which can never reach the minimum.
  [[ "$stall_row_epoch" =~ ^[0-9]+$ ]] && elapsed=$((fired_epoch - stall_row_epoch))
  ad_wait 60 3 "firing notification at local-ticket" delivered_for "$stall_since" local-ticket "$age_alert" firing || delivered=0
  if (( delivered )); then ad_mark "firing notification arrived at sink" "local-ticket"; fi
  local wrong=0
  delivered_any "$stall_since" "$age_alert" firing && ! delivered_for "$stall_since" local-ticket "$age_alert" firing && wrong=1
  if (( delivered && elapsed >= min_firing_seconds && wrong == 0 )); then
    verdict OAV-03.1 PASS "fired ${elapsed}s after the oldest row (>= ${min_firing_seconds}s expected: 300s threshold + 2m for), delivered to local-ticket"
  else
    verdict OAV-03.1 FAIL "delivered=$delivered elapsed=${elapsed}s (needs >= ${min_firing_seconds}s) wrong_receiver=$wrong"
  fi
  inspect_real_alert "$age_alert"
  local embed
  embed="$(ad_alert_firing "$age_alert" embedding_work)"
  ad_mark "note: embedding_work age alert firing series" "$embed (embedding rows are created by the same writes; informational)"

  release_or_block
  local release_since
  release_since="$(ad_now)"
  local resolved=1 notified=1
  ad_wait 240 5 "age alert to resolve in Prometheus" age_quiet || resolved=0
  if (( resolved )); then ad_mark "age alert resolved in Prometheus" ""; fi
  ad_wait 90 3 "resolved notification at local-ticket" delivered_for "$release_since" local-ticket "$age_alert" resolved || notified=0
  if (( notified )); then ad_mark "resolved notification arrived at sink" "local-ticket"; fi
  if (( resolved && notified )); then verdict OAV-03.2 PASS "alert resolved after release and a resolved notification reached local-ticket"
  else verdict OAV-03.2 FAIL "resolved_in_prometheus=$resolved resolved_notification=$notified"; fi
}

run_mongo_outage() { # during_stall: 0 or 1
  local during_stall="$1" age_before=0 outage_since outage_epoch ok=1 notified=1 age_after resolved_note
  ad_mark "scenario" "mongo-outage (OAV-04), age alert already firing: $([[ $during_stall == 1 ]] && echo required || echo no)"
  preconditions
  if (( during_stall )); then
    stall_begin
    wait_age_alert_firing || { verdict OAV-04.1 FAIL "the stall never fired the age alert, so 'already firing' could not be tested"; return 0; }
  fi
  age_firing && age_before=1
  ad_mark "before outage" "age alert firing=$age_before"
  outage_since="$(ad_now)"
  outage_epoch="$(date -u +%s)"
  mongo_stopped=1
  ad_compose stop mongodb >/dev/null
  ad_mark "MongoDB stopped" ""
  ad_wait 420 5 "HiringBacklogUnobserved to fire in Prometheus" unobserved_firing || ok=0
  if (( ok )); then
    ad_mark "Unobserved firing in Prometheus" "$(( $(date -u +%s) - outage_epoch )) s after the stop"
    inspect_real_alert "$unobserved_alert"
  fi
  ad_wait 60 3 "Unobserved notification at the sink" delivered_any "$outage_since" "$unobserved_alert" firing || notified=0
  if (( notified )); then ad_mark "Unobserved notification arrived at sink" ""; fi
  local unobserved_to_ticket=0
  ad_delivered "$outage_since" local-ticket firing "$unobserved_alert" && unobserved_to_ticket=1
  age_after="$(ad_alert_firing "$age_alert" "$outbox_capability")"
  if (( age_before )); then
    # The age series vanishes with the sample, so the firing age alert resolves; record, do not require, which happened.
    ad_wait 60 3 "age alert to resolve during the outage" age_quiet && resolved_note="resolved while MongoDB was down" || resolved_note="still firing while MongoDB was down"
    ad_mark "age alert during outage (was firing)" "$resolved_note"
  else
    resolved_note="was not firing"
    if delivered_any "$outage_since" "$age_alert" firing || (( age_after >= 1 )); then
      ad_mark "age alert during outage (was quiet)" "UNEXPECTED: fired or delivered"
      ok=0
    else
      ad_mark "age alert during outage (was quiet)" "not fired, not delivered"
    fi
  fi
  ad_compose up -d --wait mongodb >/dev/null
  mongo_stopped=0
  ad_mark "MongoDB started" ""
  local recovery_since recovered=1 resolved_delivery=1
  recovery_since="$(ad_now)"
  ad_wait 420 5 "Unobserved to resolve in Prometheus" unobserved_quiet || recovered=0
  if (( recovered )); then ad_mark "Unobserved resolved in Prometheus" ""; fi
  ad_wait 90 3 "Unobserved resolved notification at the sink" delivered_any "$recovery_since" "$unobserved_alert" resolved || resolved_delivery=0
  if (( resolved_delivery )); then ad_mark "Unobserved resolved notification arrived at sink" ""; fi
  if (( ok && notified && unobserved_to_ticket && recovered && resolved_delivery )); then
    verdict OAV-04.1 PASS "Unobserved delivered to local-ticket and resolved after MongoDB returned; age alert $resolved_note"
  else
    verdict OAV-04.1 FAIL "unobserved_fired_and_age_ok=$ok delivered=$notified to_ticket=$unobserved_to_ticket recovered=$recovered resolved_delivery=$resolved_delivery; age alert $resolved_note"
  fi
  if (( during_stall )); then
    # The seeded job's rows are still pending, so the age alert fires again once the series returns; record, then release.
    local again=1
    ad_wait 420 5 "age alert to fire again" age_firing || again=0
    ad_mark "age alert after recovery (rows still pending)" "firing=$again"
    release_or_block
    ad_wait 240 5 "age alert to resolve after release" age_quiet || ad_mark "WARNING" "age alert did not resolve after release"
  fi
}

ad_mark "drill start" "scenario=$scenario"
op_ticker_start "alert backlog drill"
case "$scenario" in
  stalled-backlog) run_stalled_backlog ;;
  mongo-outage) run_mongo_outage 0 ;;
  mongo-outage-during-stall) run_mongo_outage 1 ;;
  all)
    run_stalled_backlog
    # A recorded failure ends the run: the second scenario's preconditions would only report BLOCKED on its leftovers.
    if (( failures == 0 )); then run_mongo_outage 0
    else ad_mark "second scenario skipped" "stalled-backlog recorded $failures failure(s)"; fi
    ;;
esac
op_ticker_stop
finished=1
print_verdicts
if (( failures == 0 && ${#verdicts[@]} > 0 )); then
  printf 'RESULT: PASS criteria=%s duration=%s evidence=%s/evidence.md\n' "${#verdicts[@]}" "$(op_elapsed)" "$evidence_dir_rel"
  exit 0
fi
printf 'RESULT: FAIL failed=%s of %s duration=%s evidence=%s/evidence.md\n' "$failures" "${#verdicts[@]}" "$(op_elapsed)" "$evidence_dir_rel" >&2
exit 1