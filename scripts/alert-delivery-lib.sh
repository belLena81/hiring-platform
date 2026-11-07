#!/usr/bin/env bash
# Shared helpers for the alert delivery drills (spec: docs/specs/operational-alert-verification.md, OAV-02 to OAV-04).
# Sourced, never executed. Sourced after scripts/operator-lib.sh. Local loopback endpoints only; no secrets are read.
# Verified by user-run drills on 2026-11-06 and 2026-11-07 (one workstation; the delivery drill and the stalled-backlog
# and mongo-outage scenarios, all PASS, including the exact alert-name matching, ad_alert_count and the label allowlist);
# the sink-log parser is also unit-tested (scripts/tests/test_alert_sink_notifications.py). Still not verified: the
# optional mongo-outage-during-stall scenario, and exact-name matching against more than the sink logs of those runs.

AD_PROMETHEUS="http://127.0.0.1:9090"
AD_ALERTMANAGER="http://127.0.0.1:9093"
AD_EVIDENCE=""

ad_now() { date -u +%Y-%m-%dT%H:%M:%SZ; }

# ad_compose ARGS... : the observability stack on top of the base stack.
ad_compose() { docker compose -f compose.yaml -f compose.observability.yaml --profile api "$@"; }

# ad_mark NAME [DETAIL] : records a timestamped event on the console and in the evidence file.
ad_mark() {
  local line
  line="$(printf '%s  %-34s %s' "$(ad_now)" "$1" "${2:-}")"
  op_say "$line"
  [[ -z "$AD_EVIDENCE" ]] || printf -- '- %s\n' "$line" >>"$AD_EVIDENCE"
}

ad_curl() { curl -fsS --noproxy '*' --max-time 5 "$@"; }

# ad_prom_series EXPR : prints the number of series the instant query returns (0 for an empty result).
ad_prom_series() {
  ad_curl -G "$AD_PROMETHEUS/api/v1/query" --data-urlencode "query=$1" \
    | python3 -I -c 'import json,sys; d=json.load(sys.stdin); print(len(d["data"]["result"]) if d.get("status")=="success" else -1)'
}

ad_alert_firing() { # ALERTNAME [CAPABILITY] -> number of firing series in Prometheus
  local sel="alertname=\"$1\",alertstate=\"firing\""
  [[ -z "${2:-}" ]] || sel+=",hiring_capability=\"$2\""
  ad_prom_series "ALERTS{$sel}"
}

# ad_diagnostics : what to look at when a wait times out (stderr).
ad_diagnostics() {
  printf -- '--- last 20 lines of the alert-sink log:\n'
  { ad_compose logs --no-log-prefix --tail 20 alert-sink 2>&1 || printf '(sink log unavailable)\n'; } | tr -d '\000-\010\013-\037\177'
  printf -- '--- alerts currently in Alertmanager:\n'
  ad_curl "$AD_ALERTMANAGER/api/v2/alerts" 2>/dev/null | python3 -I -c '
import json, sys
clean = lambda s: "".join(c if c >= " " and c != "\x7f" else "?" for c in str(s))
alerts = json.load(sys.stdin)
for a in alerts:
    print("  %s (%s, %s)" % (clean(a["labels"].get("alertname", "?")), clean(a["labels"].get("hiring_capability", "-")), clean(a["status"]["state"])))
if not alerts: print("  (none)")' || printf '  (Alertmanager unavailable)\n'
}

# ad_wait SECONDS INTERVAL DESCRIPTION CMD... : polls CMD until it succeeds; returns 1 at the limit. Prints one progress
# line every 15 s, and at the limit the diagnostics above, so a slow wait never looks like a hang.
ad_wait() {
  local limit="$1" every="$2" what="$3" started="$SECONDS" next=15 waited
  shift 3
  while ! "$@" >/dev/null 2>&1; do
    waited=$((SECONDS - started))
    if (( waited >= limit )); then
      op_say "timed out after ${waited}s waiting for: $what" >&2
      ad_diagnostics >&2
      return 1
    fi
    if (( waited >= next )); then
      op_say "still waiting for: $what (${waited}s of ${limit}s)"
      next=$((next + 15))
    fi
    sleep "$every"
  done
}

# Stack preconditions shared by both drills. Prints the first unmet one and returns 1.
ad_require_stack() {
  command -v docker >/dev/null 2>&1 || { echo "docker not found on PATH"; return 1; }
  command -v curl >/dev/null 2>&1 || { echo "curl not found on PATH"; return 1; }
  command -v python3 >/dev/null 2>&1 || { echo "python3 not found on PATH"; return 1; }
  ad_curl "$AD_ALERTMANAGER/-/healthy" >/dev/null 2>&1 \
    || { echo "Alertmanager does not answer on $AD_ALERTMANAGER (start the stack: docker compose -f compose.yaml -f compose.observability.yaml --profile api up -d --build)"; return 1; }
  ad_curl "$AD_PROMETHEUS/-/ready" >/dev/null 2>&1 || { echo "Prometheus does not answer on $AD_PROMETHEUS"; return 1; }
  ad_curl "$AD_PROMETHEUS/api/v1/alertmanagers" | grep -q 'alertmanager:9093' \
    || { echo "Prometheus does not list alertmanager:9093 (restart it to load the alerting section: docker compose -f compose.yaml -f compose.observability.yaml --profile api restart prometheus)"; return 1; }
  ad_compose ps --status running --services 2>/dev/null | grep -qx 'alert-sink' \
    || { echo "the alert-sink service is not running"; return 1; }
}

# ad_post_alert ALERTNAME SEVERITY CAPABILITY ENDS_IN_SECONDS : posts one synthetic alert straight to Alertmanager.
# Labels are the closed set (alertname, severity, hiring_capability); the only annotation is summary. ENDS_IN 0 resolves it.
ad_post_alert() {
  python3 -I - "$1" "$2" "$3" "$4" <<'PY' | ad_curl -X POST -H 'Content-Type: application/json' --data-binary @- "$AD_ALERTMANAGER/api/v2/alerts" >/dev/null
import datetime, json, sys
name, severity, capability, ends_in = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
now = datetime.datetime.now(datetime.timezone.utc)
fmt = lambda t: t.strftime("%Y-%m-%dT%H:%M:%S.000Z")
print(json.dumps([{
    "labels": {"alertname": name, "severity": severity, "hiring_capability": capability},
    "annotations": {"summary": "Synthetic drill alert; no hiring data."},
    "startsAt": fmt(now - datetime.timedelta(seconds=1)),
    "endsAt": fmt(now + datetime.timedelta(seconds=ends_in)),
}]))
PY
}

# ad_notifications SINCE : one line per webhook the sink logged since SINCE (RFC 3339):
#   receiver<TAB>group status<TAB>alertname|status|hiring_capability[,...]
# Parsing lives in scripts/alert_sink_notifications.py (unit-tested against a real sink record).
ad_notifications() {
  ad_compose logs --no-log-prefix --since "$1" alert-sink 2>&1 | python3 -I "$repo_root/scripts/alert_sink_notifications.py"
}

# ad_count_delivered SINCE RECEIVER STATUS ALERTNAME [CAPABILITY] -> number of webhooks that carried that alert with that
# alert-level status. Names are compared exactly (the alert list is split, never substring-matched). RECEIVER "" means
# any receiver; CAPABILITY is compared only when given.
ad_count_delivered() {
  ad_notifications "$1" | awk -F'\t' -v r="$2" -v s="$3" -v a="$4" -v c="${5:-}" '
    r == "" || $1 == r {
      n_items = split($3, items, ",")
      for (i = 1; i <= n_items; i++) {
        split(items[i], p, "|")
        if (p[1] == a && p[2] == s && (c == "" || p[3] == c)) { n++; break }
      }
    }
    END { print n + 0 }'
}

# ad_delivered SINCE RECEIVER STATUS ALERTNAME [CAPABILITY] : succeeds when the sink saw that alert (see above).
ad_delivered() { [[ "$(ad_count_delivered "$@")" -ge 1 ]]; }

# ad_alert_count ALERTNAME : how many alerts of that exact name Alertmanager holds now.
ad_alert_count() {
  ad_curl "$AD_ALERTMANAGER/api/v2/alerts" | python3 -I -c '
import json, sys
print(sum(1 for a in json.load(sys.stdin) if a["labels"].get("alertname") == sys.argv[1]))' "$1"
}

# ad_failed_notifications : sum of Alertmanager's failed-notification counters (0 when none exist yet).
ad_failed_notifications() {
  ad_curl "$AD_ALERTMANAGER/metrics" | awk '/^alertmanager_notifications_failed_total/ {n+=$NF} END{printf "%d\n", n+0}'
}

# ad_active_alert_state ALERTNAME : prints the Alertmanager state (active / suppressed / unprocessed) or "absent".
ad_active_alert_state() {
  ad_curl "$AD_ALERTMANAGER/api/v2/alerts?active=true&silenced=false&inhibited=false" | python3 -I -c '
import json, sys
name = sys.argv[1]
states = [a["status"]["state"] for a in json.load(sys.stdin) if a["labels"].get("alertname") == name]
print(states[0] if states else "absent")' "$1"
}

# ad_label_violations : decision D7. Prints every label name outside the allowlist of ten, every value outside a closed
# value set, and every annotation other than summary, on alerts Alertmanager holds (real and synthetic). The extra names
# job, instance, exported_job and service_name/telemetry_sdk_* come from our own Prometheus scrape and OpenTelemetry
# resource, not from hiring data. Any other label name is a violation.
ad_label_violations() {
  ad_curl "$AD_ALERTMANAGER/api/v2/alerts" | python3 -I -c '
import json, sys
names = {"alertname", "severity", "hiring_capability", "job", "instance", "exported_job", "service_name",
         "telemetry_sdk_language", "telemetry_sdk_name", "telemetry_sdk_version"}
values = {"hiring_capability": {"operational_event_outbox", "embedding_work", "drill"},
          "service_name": {"hiring-api"}, "severity": {"page", "ticket"}}
clean = lambda s: "".join(c if c >= " " and c != "\x7f" else "?" for c in str(s))
for a in json.load(sys.stdin):
    who = clean(a["labels"].get("alertname", "?"))
    for k, v in a["labels"].items():
        if k not in names: print("label name", clean(k), "on", who)
        elif k in values and v not in values[k]: print("label value", k, "=", clean(v), "on", who)
    for k in a.get("annotations", {}):
        if k != "summary": print("annotation", clean(k), "on", who)'
}

# ad_firing_alert_names : the alerts Alertmanager currently holds, one "name (capability)" per line.
ad_firing_alert_names() {
  ad_curl "$AD_ALERTMANAGER/api/v2/alerts?active=true&silenced=false&inhibited=false" | python3 -I -c '
import json, sys
clean = lambda s: "".join(c if c >= " " and c != "\x7f" else "?" for c in str(s))
for a in json.load(sys.stdin):
    print("%s (%s)" % (clean(a["labels"].get("alertname", "?")), clean(a["labels"].get("hiring_capability", "-"))))'
}