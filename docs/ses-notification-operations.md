# Interview email through Amazon SES: operations note

Status: **prepared, not activated.** This build contains the typed configuration, the SES adapter behind the interview notification port and stub-client tests. It does not contain an AWS SDK dependency, a real SES client, the recipient or workflow lookups, or any AWS resource. With `kafka.interview.enabled = true`, selecting SES fails application startup in this build, because the interview runtime is composed into the application runtime (`MongoHiringRuntime`) and the SES client does not exist. The production gate is separate and is not established by this note. The status of each gate is in the [durable hiring workflows specification](specs/durable-hiring-workflows.md#ses-adapter-preparation).

Sources: [InterviewNotificationConfigReader](../src/main/scala/com/example/graphQL/cats/config/InterviewNotificationConfigReader.scala), [KafkaConfigValidation](../src/main/scala/com/example/graphQL/cats/config/KafkaConfigValidation.scala), [InterviewNotificationProviderSelection](../src/main/scala/com/example/graphQL/cats/runtime/InterviewNotificationProviderSelection.scala), [SesInterviewNotificationProvider](../src/main/scala/com/example/graphQL/cats/infrastructure/ses/SesInterviewNotificationProvider.scala), [SesEmailClient](../src/main/scala/com/example/graphQL/cats/infrastructure/ses/SesEmailClient.scala).

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
- Tests use `StubSesEmailClient` and never call AWS.

## Required before any live send (external, not verified here)

These items belong to the AWS account and the owner. The repository cannot establish them, and this note does not record that any of them is done.

1. A verified sender identity for `from-address`.
2. An account that has left the SES sandbox, so mail can reach recipients that are not verified.
3. Security Gate 0 evidence, produced outside the sandbox before `build.sbt` changes: one pinned `awsSdkVersion` for `software.amazon.awssdk:sesv2` with `url-connection-client` only, the classpath before and after (`Runtime/dependencyClasspathAsJars` and `evicted`), licence and a dated advisory lookup for each new artifact, SDK retries disabled and SDK timeouts below `provider-timeout-seconds`.
4. A `software.amazon.awssdk` logger set to OFF in both `src/main/resources/logback.xml` and `logback-container.xml`, and `.aws/` added to `.gitignore`. Neither change exists yet: the Logback files do not mention the SDK logger and `.gitignore` has no `.aws/` entry.
5. The real SES client, built only when SES is selected: explicit region, no endpoint override, credential chain only, retries disabled, and the lookups wired to the user store and workflow projections (email and deleted/tombstone state only; the interval and at most the job title).
6. A fenced pre-send check (Security finding F1, 2026-11-11). `SesInterviewNotificationProvider.notify` takes the claim (`execution`) but does not use it, and its lookups cannot receive the claim or the notification kind. The ledger provider applies four checks before it records a delivery (`MongoInterviewNotificationLedger`): claim ownership, both participants active, the kind allowed for the workflow phase, and hiring committed before `Scheduled`. Without them a wired SES adapter could, after a replay, a lost claim or a cancellation, send a stale "scheduled" notice, send before the hiring commit, email a candidate about a workflow whose recruiter was deleted, or send from a worker that lost its claim. When the lookups are wired, add a check that takes the claim and the kind and applies the same claim, participant, phase and hiring-commit predicates against the same tombstone source, immediately before the client call, with tests that assert zero client calls after a cancellation, a deleted other participant, a lost claim and `Scheduled` before the commit. One residual window cannot be closed for an external send: a deletion committed between that check and the client call can still produce one email.
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

- Unit tests are written: `InterviewNotificationConfigSpec`, `InterviewNotificationProviderSelectionSpec`, `SesInterviewNotificationProviderSpec` (stub client, including the at-least-once contract), `InterviewNotificationAtLeastOnceContractSelfCheckSpec` (proves the at-least-once contract fails a key-deduplicating provider; it accepts any `AssertionError`) and the shared `InterviewNotificationProviderContract` (exactly-once ledger contract and the at-least-once contract).
- Verdicts (2026-11-11): Code Reviewer PASS (after one FAIL and its fixes), Security Engineer PASS for the preparation code only (no dependency, no live call), and final QA PASS on committed HEAD `48f271a`. The owner's `scripts/check-local.sh` passed 6/6 on the committed tree on 2026-11-11 with the root step run (2718 tests, 2668 passed, 0 failed, 51 skipped; the skipped tests are not passes).
- Not activatable. Live SES activation, the AWS SDK dependency, F1 (the fenced pre-send check) and the Gate 0 items (dependency provenance, logger and `.aws/` ignore rules, least-privilege IAM) remain open.
