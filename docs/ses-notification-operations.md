# Interview email through Amazon SES: operations note

Status: **prepared, not activated.** This build contains the typed configuration, the SES adapter behind the interview notification port, its pre-send fence (F1, committed in `429a5c6` per the git log; Security Engineer PASS and Code Reviewer PASS (final narrow re-review) for its scope; QA PASS at unit scope; its Mongo fence integration test passed 6 of 6 in an owner-run local replica-set Docker run on 2026-11-11, owner-reported, after the fence test fixture fix (F), committed in `5d0f3bd`) (the `UserFields.erasedOnDeletion` refactor of (F) came after that run; also passed after the refactor (owner run, 2026-11-12), tests_total=2147, 0 failed) and stub-client tests. It does not contain an AWS SDK dependency, a real SES client, the recipient or workflow lookups, or any AWS resource. With `kafka.interview.enabled = true`, selecting SES fails application startup in this build, because the interview runtime is composed into the application runtime (`MongoHiringRuntime`) and the SES client does not exist. The production gate is separate and is not established by this note. The status of each gate is in the [durable hiring workflows specification](specs/durable-hiring-workflows.md#ses-adapter-preparation).

Sources: [InterviewNotificationConfigReader](../src/main/scala/com/example/graphQL/cats/config/InterviewNotificationConfigReader.scala), [KafkaConfigValidation](../src/main/scala/com/example/graphQL/cats/config/KafkaConfigValidation.scala), [InterviewNotificationProviderSelection](../src/main/scala/com/example/graphQL/cats/runtime/InterviewNotificationProviderSelection.scala), [SesInterviewNotificationProvider](../src/main/scala/com/example/graphQL/cats/infrastructure/ses/SesInterviewNotificationProvider.scala), [SesEmailClient](../src/main/scala/com/example/graphQL/cats/infrastructure/ses/SesEmailClient.scala), [InterviewNotificationSendPolicy](../src/main/scala/com/example/graphQL/cats/domain/workflow/InterviewCalendarPolicy.scala), [InterviewNotificationSendFence](../src/main/scala/com/example/graphQL/cats/service/port/InterviewNotificationSendFence.scala), [MongoInterviewNotificationSendFence](../src/main/scala/com/example/graphQL/cats/repository/mongo/MongoInterviewNotificationSendFence.scala).

## Configuration reference

Settings live under `kafka.interview.notifications` in [application.conf](../src/main/resources/application.conf). The packaged file sets `provider = "ledger"` and `ses.enabled = false`, and reads each SES value from an environment variable only when it is set.

| Setting | Environment variable | Default | Accepted value | Startup error key on failure |
|---|---|---|---|---|
| `provider` | `INTERVIEW_NOTIFICATION_PROVIDER` | `ledger` | `ledger` or `ses` | `INTERVIEW_NOTIFICATION_PROVIDER` |
| `ses.enabled` | `INTERVIEW_SES_ENABLED` | `false` | boolean | `INTERVIEW_SES_ENABLED` |
| `ses.region` | `INTERVIEW_SES_REGION` | none | AWS region name, pattern `[a-z]{2}(-[a-z]+){1,2}-[0-9]{1,2}` (for example `eu-west-1`) | `INTERVIEW_SES_REGION` |
| `ses.from-address` | `INTERVIEW_SES_FROM_ADDRESS` | none | one `local@domain` mailbox with no Unicode separator, control or format character (including NEL, U+2028/2029, U+00A0, bidi and zero-width characters) and none of `@ < > , ; "` beyond the single `@` | `INTERVIEW_SES_FROM_ADDRESS` |
| `ses.configuration-set` | `INTERVIEW_SES_CONFIGURATION_SET` | none (optional) | `[A-Za-z0-9_-]{1,64}` | `INTERVIEW_SES_CONFIGURATION_SET` |

Other startup error keys for this scope:

- `INTERVIEW_SES_SELECTION`: `provider` and `ses.enabled` disagree. `provider = "ses"` with `enabled = false`, and `provider = "ledger"` with `enabled = true`, are both refused. The configuration never falls back to the other provider.
- `INTERVIEW_SES_FORBIDDEN_SETTING`: any of `ses.access-key-id`, `ses.secret-access-key`, `ses.session-token`, `ses.profile`, `ses.credentials` or `ses.endpoint` is present. This is refused even when SES is not selected.
- `CONFIG_FILE`: any other unknown key under `kafka.interview.notifications`, for example `ses.retries` or a top-level `endpoint`.
- Invalid values of `provider`, `ses.region`, `ses.from-address` and `ses.configuration-set` are reported together. The pair check and the missing region and sender checks run after those values decode, and report together when they do.

Rules that apply to every setting:

- **Double opt-in.** SES is selected only when `provider = "ses"` and `ses.enabled = true`. SES values present while the provider is `ledger` are validated and ignored.
- **No credential or endpoint setting.** Credentials come only from the runtime credential chain, and the region is always set explicitly. Nothing resolves a default region.
- **Only the interview runtime acts on the choice.** The notification provider is built inside the enabled interview runtime (`kafka.interview.enabled = true`). With interview disabled, the SES settings are validated but no client is built.
- **Redacted rendering.** The notification settings print as `[REDACTED]`; the selection and the config object that holds them redact their `toString`. The SES settings object itself prints its region (not sensitive); the sender address and configuration set are not secrets either, and no credential field exists to print.

Placeholder example, not a working setup. With `kafka.interview.enabled = true` it stops startup in this build, because SES is selected while the SES client does not exist:

```hocon
kafka.interview.notifications {
  provider = "ses"
  ses {
    enabled = true
    region = "<aws-region>"
    from-address = "<verified-sender-address>"
  }
}
```

## What selecting SES does in this build

- With `kafka.interview.enabled = true` and SES selected, the runtime acquires the notification provider, and acquisition raises `SesClientNotAvailableError` ("SES client not available in this build"). Startup stops. The runtime never falls back to the ledger, because that would silently stop email while the configuration says email is on.
- The recipient lookup and the workflow-context lookup are placeholders that also raise. Even with a client, no message could be built from real data yet.
- The pre-send fence is a required constructor parameter. Its placeholder (`SendFences.refuseEverything`) answers `CheckUnavailable` for every send, so with the placeholder every send is refused as `SES_FENCE_UNAVAILABLE` before the client call. Selection stops before that point in this build.
- Tests use `StubSesEmailClient` and never call AWS.

## Pre-send fence (F1)

Before the one client call, `SesInterviewNotificationProvider.notify` runs the order: resolve the recipient address, read the workflow context, render the message, then `InterviewNotificationSendFence.verify(workflowId, kind, execution)`, then the client call. A refused or unavailable check makes zero client calls.

- **Checks, in order** (`InterviewNotificationSendPolicy.decide`, the same four predicates the receipt ledger applies in `MongoInterviewNotificationLedger`): the claim is held (`InterviewClaimStanding.Held`), the candidate, the recruiter and the initiating actor each have `AccountStatus = Active` (the workflow's `participantsOf`), the kind is allowed in the workflow phase (`InterviewNotificationFence.permits`), and hiring is committed for `Scheduled` only (`InterviewNotificationFence.requiresHiringCommit`). The policy type is declared in `InterviewCalendarPolicy.scala`, not in a file named after it (open follow-up, see the durable workflows specification).
- **Absent claim is refused.** The ledger accepts a call that supplies no claim; the SES fence refuses it, because the runtime always supplies one and a missing claim is a wiring error.
- **Read-only.** `MongoInterviewNotificationSendFence` reads the command claim, the workflow, the account state of the candidate, the recruiter and the initiating actor, and the hiring commit. These are separate queries without a shared snapshot; any skew between them lies inside the check-to-call window in the residual windows below. It writes no receipt and no counter, so it neither records a delivery nor contends with the workflow's own transactions.
- **Refusals and their codes** are in the failure table below (`SES_FENCE_*`, classified `Conflict`). A store failure, or a fence that throws, is `CheckUnavailable`, reported as `SES_FENCE_UNAVAILABLE` and classified `Unavailable`. The interpreter treats an `Unavailable` outcome as unknown and checks again on the next attempt; because nothing was sent, a send after a passing check is the first send.
- **Residual windows** that no external send can close:
  1. A cancellation, deletion or lost claim committed between the check and the client call can still produce one message.
  2. The send can succeed and the process can die before the receipt or acknowledgement is recorded. The interpreter then sends again after recovery: a duplicate is possible, a silent loss is not.
- **Hiring-commit check** has pure-policy tests only. No real-database test covers the hiring-commit predicate yet.

Why: the ledger refuses a stale or unauthorized delivery before recording it, and an SES adapter without the same checks could send a stale "scheduled" notice after a replay, a cancellation or a lost claim, send before the hiring commit, or email a candidate about a workflow whose other participant was deleted. Why a read-only check: the fence records nothing, so a refused or failed check leaves no evidence that SES would have to reconcile.

## Required before any live send (external, not verified here)

These items belong to the AWS account and the owner. The repository cannot establish them, and this note does not record that any of them is done.

1. A verified sender identity for `from-address`.
2. An account that has left the SES sandbox, so mail can reach recipients that are not verified.
3. Security Gate 0 evidence, produced outside the sandbox before `build.sbt` changes: one pinned `awsSdkVersion` for `software.amazon.awssdk:sesv2` with `url-connection-client` only, the classpath before and after (`Runtime/dependencyClasspathAsJars` and `evicted`), licence and a dated advisory lookup for each new artifact, SDK retries disabled and SDK timeouts below `provider-timeout-seconds`.
4. A `software.amazon.awssdk` logger set to OFF in both `src/main/resources/logback.xml` and `logback-container.xml`, and `.aws/` added to `.gitignore`. Neither change exists yet: the Logback files do not mention the SDK logger and `.gitignore` has no `.aws/` entry.
5. The real SES client, built only when SES is selected: explicit region, no endpoint override, credential chain only, retries disabled, and the lookups wired to the user store and workflow projections (email and deleted/tombstone state only; the interval and at most the job title).
6. A fenced pre-send check (Security finding F1). Implemented on 2026-11-11 (committed in `429a5c6` per the git log; unit-tested); Security Engineer PASS for the F1 scope (2026-11-11, uncommitted tree on 71d9ada; stub-only, not activatable; the Mongo fence integration test was not yet run when that verdict was written). Code Reviewer PASS (re-review) and QA PASS at unit scope are recorded in the [durable workflows specification](specs/durable-hiring-workflows.md) (Verdict records). The owner's Mongo fence run of 2026-11-11 passed 6 of 6 (owner-reported) (the `UserFields.erasedOnDeletion` refactor of (F) came after that run; also passed after the refactor (owner run, 2026-11-12), tests_total=2147, 0 failed); a full owner `scripts/check-local.sh` on `41329b7` with fix (F) only (late 2026-11-11) failed at step 5 on a suite outside this note (`MongoBinaryEmbeddingVectorIntegrationSpec`; diagnosed 2026-11-12 as allocation-dependent `storageSize`, see [MongoDB design](mongodb-design.md#job-canonical-filters-and-vector-storage)), and fixes (F) and (S) have Code Reviewer PASS and QA PASS (static; integration not run by the reviewers); (S), the storage assertion fix, was made after that run; the owner's three storage-spec runs of 2026-11-12 passed, and a second full run on 2026-11-12 failed at step 5 on `SubjectFenceRelayContentionIntegrationSpec` (DDR-01, decided 2026-11-12 as a bounded retryable claim in `b5c7e3f`), not on a suite named in this note. The owner's full `check-local` on `481f320` (2026-11-12) passed 6 of 6. The check is described in [Pre-send fence (F1)](#pre-send-fence-f1). For the live path it still needs the real lookups of item 5, which must read the same account status the fence reads. Two residual windows remain open (listed in the F1 section) and are not closed by this build.
7. Independent Security Engineer PASS, Code Reviewer PASS and QA PASS on the final state, and an owner full-suite run.

## IAM: least privilege (requirement, not provisioned)

The sending workload uses a role. Long-lived access keys are not used. The policy must allow one action on one identity, with conditions:

```json
{
  "Effect": "Allow",
  "Action": "ses:SendEmail",
  "Resource": "arn:aws:ses:<region>:<account-id>:identity/<verified-sender-identity>",
  "Condition": {
    "StringEquals": {
      "ses:FromAddress": "<from-address>",
      "aws:RequestedRegion": "<region>"
    }
  }
}
```

Do not grant `ses:*`, and do not use `Resource: "*"`. If a configuration set is used, its resource scope must be confirmed against AWS documentation before the policy is written. The condition keys and the configuration-set scope were not checked against AWS documentation in this pass.

## Delivery guarantee: at least once

Owner decision, 2026-11-10. The SES adapter has no idempotency key and no lookup, and it keeps no receipt. An unknown outcome (a timeout, a lost response, or a crash after SES accepted the message) leaves no evidence, so the interpreter may send again.

- A duplicate notification is possible. An operator who sees two identical emails for one event should expect this behavior, not a fault.
- A silent loss is not allowed by design. An exhausted notification becomes visible command-level repair for Admin, with the workflow phase unchanged ([API reference](api.md#notification-guarantee)).
- The ledger provider is the exception: it keeps one receipt per key and answers repeats with the first outcome.

Failure classes after one send attempt (from `SesNotificationFailure`):

| Code | Classification | Meaning |
|---|---|---|
| `SES_NO_RECIPIENT` | Conflict (no send) | No address, or a deleted or tombstoned subject |
| `SES_INVALID_RECIPIENT` | Conflict (no send) | The stored address fails the sender pattern |
| `SES_NO_CONTEXT` | Conflict (no send) | The workflow has no readable interval |
| `SES_FENCE_CLAIM_NOT_HELD` | Conflict (no send) | The pre-send fence: the claim is lost, replaced or absent |
| `SES_FENCE_PARTICIPANT_INACTIVE` | Conflict (no send) | The pre-send fence: a participant is not `Active` |
| `SES_FENCE_KIND_NOT_ALLOWED` | Conflict (no send) | The pre-send fence: the kind is not allowed in the current phase, for example a stale `Scheduled` after a cancellation |
| `SES_FENCE_HIRING_NOT_COMMITTED` | Conflict (no send) | The pre-send fence: `Scheduled` before the hiring commit |
| `SES_FENCE_WORKFLOW_NOT_FOUND` | Conflict (no send) | The pre-send fence: no workflow with that id |
| `SES_FENCE_UNAVAILABLE` | Unavailable (no send) | The pre-send check could not run, or it threw |
| `SES_RECIPIENT_REJECTED` | Conflict | SES refused the message or the recipient |
| `SES_INVALID_RECEIPT` | InvalidReceipt | Accepted without a message id, or with a blank one |
| `SES_NOT_SENT`, `SES_THROTTLED`, `SES_SERVER_ERROR`, `SES_TIMED_OUT`, `SES_ACCESS_DENIED`, `SES_SENDING_PAUSED`, `SES_CLIENT_FAILED`, `SES_LOOKUP_FAILED` | Unavailable | Transient or unknown. A timeout may have sent the message |

The adapter itself never retries. The interpreter owns the retry budget. An accepted send without a message id is classified `SES_INVALID_RECEIPT` (InvalidReceipt). Verified in source: `InterviewProviderExecutor` maps every provider error other than `Conflict` to `OutcomeUnknown` (`InterviewProviderExecutor.scala`, lines 20-26), so InvalidReceipt is treated as an unknown outcome. The workflow then asks the notification lookup, which for SES answers absent (no receipt), and the workflow issues `Notify` again (`InterviewLifecycle.scala` lines 378-381; `InterviewSchedulingSaga.scala` lines 148-151). A send whose message SES accepted without an id is therefore repeated, which is the at-least-once duplicate described above.

## Recipient addresses and personal data

- The recipient address is read from the user store at send time, used for one client call and then dropped. It is not stored in a workflow, outbox, receipt, log field, header, tag or error.
- The message is plain text: a fixed subject per kind, a headline per kind, `Starts (UTC)` and `Ends (UTC)`, and the job title when it is not blank. The title has control and format characters removed and is cut to 120 characters. The message has no resume, no names and no workflow or message identifiers.
- **New data processor.** SES keeps an account-level suppression list, and bounce and complaint events can contain recipient addresses. That data sits outside this platform. Deleting an account in this platform does not erase it. The owner must record this in the privacy and retention decision before any live send. The AWS retention behavior was not verified against AWS documentation in this pass.
- Bounce and complaint processing is a non-goal. The platform does not read those events and does not act on them.

## Logging

The adapter emits one event, `INTERVIEW_NOTIFICATION_FAILED` (WARN, category `INTERVIEW`), with `errorCode` set to one of the codes above. For an unexpected exception it also adds `errorType` and `errorLocation`, which carry only the class name and file and line. Messages, addresses and bodies never appear. Details are in [logging](logging.md#interview-notification-failures).

The AWS SDK must not write its own logs. See Gate 0 item 4 above.

## Non-goals

- Activating SES in any environment (a separate gate).
- Provisioning a sender identity, an IAM role, a configuration set or any other AWS resource.
- Bounce and complaint processing, and a suppression-list sync.
- Real calendar provider behavior. The calendar provider is still the ledger.
- Any live AWS call in the test suite.

## Verification status (2026-11-11)

- Unit tests: `InterviewNotificationConfigSpec`, `InterviewNotificationProviderSelectionSpec` (the ledger default answers `lookup` from its own receipt store, and the SES client builds zero times on that path), `SesInterviewNotificationProviderSpec` (stub client; one refusal case per `SES_FENCE_*` code with zero client calls and no address in logs; the fence runs before the client call), `InterviewNotificationSendPolicySpec` (the four checks in order, agreement with `InterviewNotificationFence.permits` for every kind and phase, the hiring-commit rule), `InterviewNotificationAtLeastOnceContractSelfCheckSpec` (proves the at-least-once contract fails a key-deduplicating provider: it accepts only the `AssertionError` that carries the contract clue `the retry must call the provider again`) and the shared `InterviewNotificationProviderContract` (exactly-once ledger contract and the at-least-once contract).
- Test run (2026-11-11, uncommitted tree on 71d9ada): `sbt test` 2141 passed, 0 failed (author run, after the fifth GDF-01 test was added; the 2140 count recorded earlier by the author and by QA predates that test; a run is not a verdict by itself). `MongoInterviewNotificationSendFenceIntegrationSpec` (including the expired-lease case, `ClaimNotHeld` for a claim whose `claimUntil` is an hour in the past) passed 6 of 6 in the owner's local replica-set Docker run of 2026-11-11 (tests_total=2147, passed=2147, failed=0, skipped=0; owner-reported terminal output) (the `UserFields.erasedOnDeletion` refactor of (F) came after that run; also passed after the refactor (owner run, 2026-11-12), tests_total=2147, 0 failed). The first owner run failed 1 of 6 because of the test fixture, not the fence: the deleted-participant test set `accountStatus = "Deleted"` on a live user without the erased fields, which the users validator rejects (diagnosed from the validator source; the Mongo error text was not captured); the test now sets `deletedAt` and unsets the erased fields (fix (F), a test-only change, committed in `5d0f3bd`, Code Reviewer PASS and QA PASS (static; integration not run by the reviewers)). The hiring-commit predicate still has pure-policy tests only.
- Verdicts: on committed HEAD `48f271a`, Code Reviewer PASS (after one FAIL and its fixes), Security Engineer PASS for the preparation code only (no dependency, no live call), and final QA PASS, with the owner's `scripts/check-local.sh` 6/6 (2718 tests, 2668 passed, 0 failed, 51 skipped; the skipped tests are not passes). Those verdicts do not cover the F1 fence or the test changes made after `48f271a`; their review status is recorded in the durable workflows specification's Verdict records.
- Not activatable. Live SES activation, the AWS SDK dependency, the real client and lookups, and the Gate 0 items (dependency provenance, logger and `.aws/` ignore rules, least-privilege IAM) remain open. F1 is committed in `429a5c6` with Security Engineer and Code Reviewer PASS for its scope and QA PASS at unit scope; its Mongo fence integration test passed 6 of 6 in the owner's 2026-11-11 replica-set run (owner-reported) (the `UserFields.erasedOnDeletion` refactor of (F) came after that run; also passed after the refactor (owner run, 2026-11-12), tests_total=2147, 0 failed).
