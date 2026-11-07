"""Reads the alert sink's log (stdin) and prints one tab-separated line per Alertmanager webhook it recorded:
receiver, group status, then alertname|status|hiring_capability for each alert, comma-separated.

The sink is the collector's debug exporter. A record's body line reads `Body: Str({"receiver":"local-page",...})`
with the JSON NOT escaped; the JSON itself holds `\\"` inside the groupKey string, which is valid JSON and must
be left alone. The raw text is decoded first. Only if that finds nothing is a text with `\\"` turned into `"` tried
(for a log driver that escapes the body). Run with python3 -I (spec: docs/specs/operational-alert-verification.md).
"""

import json
import re
import sys

_ANCHOR = re.compile(r'\{"receiver"')
_DECODER = json.JSONDecoder()


def _bodies(text):
    found = []
    for match in _ANCHOR.finditer(text):
        try:
            body, _ = _DECODER.raw_decode(text[match.start():])
        except ValueError:
            continue
        if isinstance(body, dict) and "alerts" in body:
            found.append(body)
    return found


def notifications(text):
    """Webhook bodies found in the sink log text, in log order."""
    bodies = _bodies(text)
    if not bodies:
        bodies = _bodies(text.replace('\\"', '"'))
    return bodies


def summary(body):
    alerts = ",".join(
        "%s|%s|%s" % (
            a.get("labels", {}).get("alertname", "?"),
            a.get("status", "?"),
            a.get("labels", {}).get("hiring_capability", ""),
        )
        for a in body.get("alerts", [])
    )
    return (body.get("receiver", "?"), body.get("status", "?"), alerts)


def main():
    for body in notifications(sys.stdin.read()):
        print("%s\t%s\t%s" % summary(body))


if __name__ == "__main__":
    main()