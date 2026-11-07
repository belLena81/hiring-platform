"""Pure decision for the backlog drill's release step (spec: docs/specs/operational-alert-verification.md, OAV-03).

Given what the drill found after seeding (jobs created since the database-clock start, and the ids of the pending
outbox and embedding work rows that belong to those jobs), decide whether deleting exactly those rows is allowed.
The stall must be exactly one job. The bounds are generous guesses per job: createJob and publishJob write a few events
(at most 4 outbox rows) and one coalesced embedding work row (at most 2). Zero outbox rows is refused; zero embedding
work rows is allowed because embedding can be disabled.

CLI: reads {"jobsCreated": n, "outboxIds": [...], "embeddingIds": [...]} from stdin, prints "ALLOW" or "REFUSE <reason>",
exit 0 or 3. Run with python3 -I.
"""

import json
import sys
from typing import NamedTuple

OUTBOX_ROWS_PER_JOB = 4
EMBEDDING_ROWS_PER_JOB = 2


class Decision(NamedTuple):
    allowed: bool
    reason: str


def decide(jobs_created, outbox_ids, embedding_ids,
           outbox_per_job=OUTBOX_ROWS_PER_JOB, embedding_per_job=EMBEDDING_ROWS_PER_JOB):
    if jobs_created != 1:
        return Decision(False, "the drill must have created exactly one job since the database-clock start, found %s" % jobs_created)
    outbox, embedding = len(set(outbox_ids)), len(set(embedding_ids))
    if outbox == 0:
        return Decision(False, "no pending outbox row belongs to the seeded job")
    if outbox > outbox_per_job:
        return Decision(False, "%d outbox rows exceed the bound of %d for one job" % (outbox, outbox_per_job))
    if embedding > embedding_per_job:
        return Decision(False, "%d embedding work rows exceed the bound of %d for one job" % (embedding, embedding_per_job))
    return Decision(True, "one job, %d outbox rows, %d embedding work rows" % (outbox, embedding))


def main():
    try:
        found = json.load(sys.stdin)
        decision = decide(int(found["jobsCreated"]), list(found["outboxIds"]), list(found["embeddingIds"]))
    except (ValueError, KeyError, TypeError) as error:
        decision = Decision(False, "unreadable scope: %s" % type(error).__name__)
    print("ALLOW" if decision.allowed else "REFUSE %s" % decision.reason)
    return 0 if decision.allowed else 3


if __name__ == "__main__":
    sys.exit(main())
