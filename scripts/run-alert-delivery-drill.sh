#!/usr/bin/env bash
# OAV-02 drill: Alertmanager routing, resolved messages, a dead receiver and local-only exposure, on the local
# observability stack (spec: docs/specs/operational-alert-verification.md). About 30 s to 1 minute when all
# criteria pass, up to about 8 minutes when waits time out. Needs Docker and
# curl, so a human runs it outside the agent sandbox. Contract: "Operator scripts" in docs/engineering-quality.md.
#
# It posts SYNTHETIC alerts straight to Alertmanager (labels alertname, severity, hiring_capability; no hiring
# data) so routing is proved without waiting for a real rule. The real age rule through delivery is OAV-03
# (scripts/run-alert-backlog-drill.sh). The page route is proved with a synthetic severity=page alert, not with the
# real HiringApiHighErrorRate rule: that is a recorded deviation from the wording of OAV-02.2.
# It briefly stops and restarts only the alert-sink service. It never touches application data and never commits.
# Exit codes: 0 PASS, 1 FAIL, 2 BLOCKED (precondition or usage), 130 interrupted. Last line: "RESULT: ...".
# Verified by the user on 2026-11-06 and again on 2026-11-07 (one workstation): PASS 5 of 5 in 35 s each time, including
# the exact alert-name matching and the stricter port check. Not verified: other timings, Alertmanager versions or sink
# images, the failure paths of the cleanup (an interrupted run), and exact-name matching against more than those logs.
set -Eeuo pipefail
umask 077
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$repo_root"
# shellcheck source=scripts/operator-lib.sh
source "$repo_root/scripts/operator-lib.sh"
# shellcheck source=scripts/alert-delivery-lib.sh
source "$repo_root/scripts/alert-delivery-lib.sh"

if [[ $# -gt 0 ]]; then
  case "$1" in
    -h|--help) sed -n '2,15p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) printf 'alert-delivery-drill: unexpected argument: %s (try --help)\nRESULT: BLOCKED reason=usage\n' "$1" >&2; exit 2 ;;
  esac
fi

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
evidence_dir_rel=".local/logs/alert-drills/$stamp-delivery"
mkdir -p -m 700 "$repo_root/$evidence_dir_rel"
AD_EVIDENCE="$repo_root/$evidence_dir_rel/evidence.md"
printf '# Alert delivery drill (OAV-02), started %s\n\n' "$stamp" >"$AD_EVIDENCE"

failures=0
declare -a verdicts=()
verdict() { # ID PASS|FAIL detail
  verdicts+=("$1 $2 $3")
  [[ "$2" == PASS ]] || failures=$((failures + 1))
  ad_mark "verdict $1" "$2 $3"
}
finished=0
stack_touched=0

cleanup() {
  local code=$?
  trap '' INT TERM HUP # a second Ctrl-C must not leave the sink stopped
  trap - EXIT
  # Change the stack back only if this run changed it: a precondition failure touches nothing.
  if (( stack_touched )); then
    ad_compose start alert-sink >/dev/null 2>&1 || true
    # Resolve with the SAME labels each alert was posted with (a different severity is a different alert).
    ad_post_alert OperationalAlertDrillTicket ticket drill 0 >/dev/null 2>&1 || true
    ad_post_alert OperationalAlertDrillPage page drill 0 >/dev/null 2>&1 || true
    ad_post_alert OperationalAlertDrillDeadReceiver ticket drill 0 >/dev/null 2>&1 || true
    ad_compose ps --status running --services 2>/dev/null | grep -qx alert-sink \
      || echo "alert-delivery-drill: WARNING: the alert-sink service is not running; start it with: docker compose -f compose.yaml -f compose.observability.yaml --profile api up -d alert-sink" >&2
  fi
  if (( finished == 0 )); then
    printf 'RESULT: FAIL phase=interrupted-or-error exit=%s evidence=%s/evidence.md\n' "$code" "$evidence_dir_rel" >&2
    exit "${code/#0/1}"
  fi
}
trap cleanup EXIT
trap 'exit 130' INT TERM HUP

blocked() {
  printf 'alert-delivery-drill: %s\n' "$1" >&2
  finished=1
  printf 'RESULT: BLOCKED reason=precondition evidence=%s/evidence.md\n' "$evidence_dir_rel" >&2
  exit 2
}

reason="$(ad_require_stack)" || blocked "$reason"

start_time="$(ad_now)"
ad_mark "drill start" "stack answers; sink log read from $start_time"
# Real alerts may already be firing (for example a stale outbox row). OAV-02.1 to 02.4 are judged only by this drill's
# own synthetic alert names, so a real alert can neither make them pass nor fail; OAV-02.5 also inspects real alerts.
real_alerts="$(ad_firing_alert_names 2>/dev/null || true)"
if [[ -n "$real_alerts" ]]; then
  ad_mark "real alerts already in Alertmanager (not judged by OAV-02.1 to 02.4)" "$(tr '\n' ';' <<<"$real_alerts")"
else
  ad_mark "real alerts already in Alertmanager" "none"
fi

# OAV-02.1 and OAV-02.2: a ticket alert reaches local-ticket only, a page alert reaches local-page.
stack_touched=1
ad_post_alert OperationalAlertDrillTicket ticket drill 600
ad_post_alert OperationalAlertDrillPage page drill 600
ad_mark "synthetic alerts posted" "ticket and page, 10 minute expiry"
ticket_ok=1; page_ok=1
ad_wait 90 3 ticket ad_delivered "$start_time" local-ticket firing OperationalAlertDrillTicket || ticket_ok=0
ad_wait 90 3 page ad_delivered "$start_time" local-page firing OperationalAlertDrillPage || page_ok=0
(( ticket_ok )) && ad_mark "ticket delivered to local-ticket" "" || true
(( page_ok )) && ad_mark "page delivered to local-page" "" || true
wrong_route=0
ad_delivered "$start_time" local-page firing OperationalAlertDrillTicket && wrong_route=1
ad_delivered "$start_time" local-ticket firing OperationalAlertDrillPage && wrong_route=1
if (( ticket_ok && ! wrong_route )) && [[ "$(ad_count_delivered "$start_time" local-ticket firing OperationalAlertDrillTicket)" -ge 1 ]]; then
  verdict OAV-02.1 PASS "ticket alert delivered to local-ticket; local-page received no ticket alert"
else
  verdict OAV-02.1 FAIL "ticket delivered=$ticket_ok wrong_route=$wrong_route (check the sink log format: docker compose logs alert-sink)"
fi
if (( page_ok )) && ! ad_delivered "$start_time" local-ticket firing OperationalAlertDrillPage; then
  verdict OAV-02.2 PASS "synthetic page alert delivered to local-page only (synthetic, not the real error-rate rule)"
else
  verdict OAV-02.2 FAIL "page delivered=$page_ok"
fi

# OAV-02.5 (labels, decision D7): every label name is in the allowlist of ten and closed-set values are checked.
violations="$(ad_label_violations || echo 'query-failed')"
ports="$(ad_compose ps --format json alertmanager alert-sink 2>/dev/null | python3 -I -c '
import json, sys
bad = []
raw = sys.stdin.read().strip()
# Older Compose prints one JSON array, newer versions one object per line.
containers = json.loads(raw) if raw.startswith("[") else [json.loads(l) for l in raw.splitlines() if l.strip()]
seen, published = set(), {"alertmanager": 0, "alert-sink": 0}
for c in containers:
    service = c.get("Service")
    seen.add(service)
    for p in c.get("Publishers") or []:
        if p.get("PublishedPort", 0):
            published[service] = published.get(service, 0) + 1
            if p.get("URL") != "127.0.0.1":
                bad.append("%s %s:%s" % (service, p.get("URL"), p.get("PublishedPort")))
# An empty listing must not pass: both services have to be found, Alertmanager published, the sink not.
for service in ("alertmanager", "alert-sink"):
    if service not in seen: bad.append("%s not found in compose ps" % service)
if "alertmanager" in seen and published["alertmanager"] == 0: bad.append("alertmanager publishes no port")
if "alert-sink" in seen and published["alert-sink"] > 0: bad.append("alert-sink publishes %d port(s)" % published["alert-sink"])
print("; ".join(bad))' || echo 'ps-failed')"
if [[ -z "$violations" && -z "$ports" ]]; then
  verdict OAV-02.5 PASS "label names within the D7 allowlist and closed values valid; Alertmanager published on 127.0.0.1 only; sink publishes no port"
else
  verdict OAV-02.5 FAIL "label violations: ${violations:-none}; port issues: ${ports:-none}"
fi

# OAV-02.3: resolved notification.
resolve_time="$(ad_now)"
ad_post_alert OperationalAlertDrillTicket ticket drill 0
ad_post_alert OperationalAlertDrillPage page drill 0
ad_mark "synthetic alerts resolved" "posted with endsAt now"
ok=1
ad_wait 90 3 resolved-ticket ad_delivered "$resolve_time" local-ticket resolved OperationalAlertDrillTicket || ok=0
ad_wait 90 3 resolved-page ad_delivered "$resolve_time" local-page resolved OperationalAlertDrillPage || ok=0
if (( ok )); then verdict OAV-02.3 PASS "resolved notifications received by local-ticket and local-page"
else verdict OAV-02.3 FAIL "no resolved notification within 90 s"; fi

# OAV-02.4: a dead sink is visible; the alert stays active; delivery resumes.
failed_before="$(ad_failed_notifications)"
ad_compose stop alert-sink >/dev/null
ad_mark "sink stopped" "failed_total before=$failed_before"
dead_time="$(ad_now)"
ad_post_alert OperationalAlertDrillDeadReceiver ticket drill 600
failed_up=1
failed_rose() { [[ "$(ad_failed_notifications)" -gt "$failed_before" ]]; }
# The failed counter is global: a real alert also retrying against the stopped sink would raise it too.
ad_wait 90 3 "Alertmanager failed-notification counter to rise" failed_rose && failed_up=0
failed_after="$(ad_failed_notifications)"
state_dead="$(ad_active_alert_state OperationalAlertDrillDeadReceiver)"
ad_mark "failure observed" "failed_total after=$failed_after alert_state=$state_dead"
ad_compose start alert-sink >/dev/null
ad_mark "sink restarted" ""
resumed=1
ad_wait 120 3 "delivery of OperationalAlertDrillDeadReceiver after the sink returned" \
  ad_delivered "$dead_time" local-ticket firing OperationalAlertDrillDeadReceiver && resumed=0
if (( failed_up == 0 && resumed == 0 )) && [[ "$state_dead" == active ]]; then
  verdict OAV-02.4 PASS "failed counter $failed_before -> $failed_after, alert stayed active, delivered after the sink returned"
else
  verdict OAV-02.4 FAIL "counter_rose=$((1 - failed_up)) alert_state=$state_dead delivered_after_return=$((1 - resumed))"
fi

ad_post_alert OperationalAlertDrillDeadReceiver ticket drill 0 || true
finished=1
printf '\nVerdicts:\n' | tee -a "$AD_EVIDENCE"
for v in "${verdicts[@]}"; do printf '  %s\n' "$v" | tee -a "$AD_EVIDENCE"; done
if (( failures == 0 )); then
  printf 'RESULT: PASS criteria=%s duration=%s evidence=%s/evidence.md\n' "${#verdicts[@]}" "$(op_elapsed)" "$evidence_dir_rel"
  exit 0
fi
printf 'RESULT: FAIL failed=%s of %s duration=%s evidence=%s/evidence.md\n' "$failures" "${#verdicts[@]}" "$(op_elapsed)" "$evidence_dir_rel" >&2
exit 1
