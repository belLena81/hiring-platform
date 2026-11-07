"""Reading Alertmanager webhooks back out of the alert sink's log.
Run: python3 -I -m unittest discover -s scripts/tests -p 'test_alert_sink*.py'"""

import importlib.util
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / "alert_sink_notifications.py"
spec = importlib.util.spec_from_file_location("alert_sink_notifications", SCRIPT)
parser = importlib.util.module_from_spec(spec)
spec.loader.exec_module(parser)

# The record the first user-run of the delivery drill produced: the debug exporter writes the unescaped JSON body on
# one line and a closing parenthesis on the next. The groupKey holds JSON-escaped quotes.
FIRING_PAGE = (
    'Body: Str({"receiver":"local-page","status":"firing","alerts":[{"status":"firing","labels":{"alertname":'
    '"OperationalAlertDrillPage","hiring_capability":"drill","severity":"page"},"annotations":{"summary":'
    '"Synthetic drill alert; no hiring data."},"startsAt":"2026-11-06T18:39:14Z","endsAt":"0001-01-01T00:00:00Z",'
    '"generatorURL":"","fingerprint":"3afc55979d3d6a0d"}],"notification_reason":"first notification","groupLabels":'
    '{"alertname":"OperationalAlertDrillPage","hiring_capability":"drill"},"commonLabels":{"alertname":'
    '"OperationalAlertDrillPage","hiring_capability":"drill","severity":"page"},"commonAnnotations":{"summary":'
    '"Synthetic drill alert; no hiring data."},"routeLabels":{},"externalURL":"http://df2737e2dd84:9093","version":"4",'
    '"groupKey":"{}/{severity=\\"page\\"}:{alertname=\\"OperationalAlertDrillPage\\", hiring_capability=\\"drill\\"}",'
    '"truncatedAlerts":0}\n)\n'
)

RESOLVED_TICKET = FIRING_PAGE.replace("local-page", "local-ticket").replace("OperationalAlertDrillPage", "OperationalAlertDrillTicket") \
    .replace('"status":"firing"', '"status":"resolved"').replace('"severity":"page"', '"severity":"ticket"') \
    .replace('severity=\\"page\\"', 'severity=\\"ticket\\"')

TWO_ALERTS = (
    'Body: Str({"receiver":"local-ticket","status":"firing","alerts":[{"status":"firing","labels":{"alertname":'
    '"HiringBacklogOldestPendingTooOld","hiring_capability":"operational_event_outbox","severity":"ticket"}},'
    '{"status":"resolved","labels":{"alertname":"HiringBacklogUnobserved","severity":"ticket"}}],"groupLabels":{},'
    '"groupKey":"{}:{}","version":"4","truncatedAlerts":0}\n)\n'
)


class SinkLogParsing(unittest.TestCase):
    def test_reads_the_real_unescaped_firing_record_with_its_escaped_group_key(self):
        self.assertEqual(
            [("local-page", "firing", "OperationalAlertDrillPage|firing|drill")],
            [parser.summary(b) for b in parser.notifications("noise before\n" + FIRING_PAGE + "noise after\n")],
        )

    def test_reads_a_resolved_record_built_the_same_way(self):
        self.assertEqual(
            [("local-ticket", "resolved", "OperationalAlertDrillTicket|resolved|drill")],
            [parser.summary(b) for b in parser.notifications(RESOLVED_TICKET)],
        )

    def test_reports_every_alert_of_a_record_with_two_alerts(self):
        self.assertEqual(
            [("local-ticket", "firing",
              "HiringBacklogOldestPendingTooOld|firing|operational_event_outbox,HiringBacklogUnobserved|resolved|")],
            [parser.summary(b) for b in parser.notifications(TWO_ALERTS)],
        )

    def test_keeps_log_order_across_records(self):
        text = FIRING_PAGE + RESOLVED_TICKET + TWO_ALERTS
        self.assertEqual(["local-page", "local-ticket", "local-ticket"],
                         [parser.summary(b)[0] for b in parser.notifications(text)])

    def test_falls_back_to_unescaping_when_a_log_driver_escapes_the_body(self):
        # Every quote is escaped once more, as a driver that quotes the whole line would do.
        escaped = FIRING_PAGE.replace('"', '\\"')
        self.assertEqual(["local-page"], [parser.summary(b)[0] for b in parser.notifications(escaped)])

    def test_finds_nothing_in_unrelated_log_lines(self):
        self.assertEqual([], parser.notifications('2026-11-06T18:39:14Z info Everything is ready\n{"receiver"\n'))


if __name__ == "__main__":
    unittest.main()
