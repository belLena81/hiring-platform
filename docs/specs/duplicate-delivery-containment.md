# Duplicate delivery containment

Status: in review (DDC-01 to DDC-05 with local evidence; deployed brokers not verified; the reader starting against a re-provisioned Compose broker not verified, see the 2026-11-08 checkpoint; the 2026-11-03 provisioning change shipped a spelling the CLI refuses, fixed 2026-11-08, and its verdicts do not cover the fix). Context: Operational events (Kafka publication and consumption) and AI search
(embedding work), plus the interview provider ports of Interview scheduling.

## Problem

A lease gives probably one worker, not exactly one, and at-least-once delivery hands every consumer duplicates. Data
correctness already holds on every path (receipt unique `_id`, interview inbox in the state transaction, Delta MERGE
by event id, lease-token-fenced completion). Four gaps remain where a duplicate still costs money or can reopen a
correctness hole after a later change:

1. A successful, paid embedding call is repeated when the entity write that follows it fails transiently or loses its
   revision to an unrelated edit of the same entity.
2. "A duplicate key means already handled" holds only while a receipt outlives every message Kafka can still deliver.
   Nothing checks that, and the current defaults already break it: a segment is deleted only once its newest record is
   older than `retention.ms`, so a record can stay readable for `retention.ms + segment.ms` (7 + 7 = 14 days) while receipts expire after 8 days.
3. The rule "a consumer effect is keyed by event id and written atomically with the receipt" is only a comment.
4. Real notification and calendar providers are planned. "Look up, then send" is safe only because today's provider
   is a local ledger keyed by the idempotency key.

Invariant touched: an operational event or provider request is applied at most once per consumer or idempotency key,
and paid provider work is not repeated for an outcome the platform already holds.

## Source facts (verified 2026-11-03)

- `EmbeddingPipeline.embedPrepared` calls `embeddings.embed` and then `persist`. A repository failure other than
  `Conflict` returns `Retry`, so `retryingOnFailures` reruns `process`, which calls the provider again for the same
  text. A `Conflict` goes to `conflictOutcome`, which answers `Retry` (durable, a later claim) whenever the re-read
  still needs an embedding, even when its source hash equals the one just paid for.
- The embedding lease is `retryAttempts × (timeoutMs + retryDelayMs)` (`MongoHiringRuntime`), renewed every third of
  the lease by `LeaseHeartbeat.guard`. A healthy worker does not lose its lease during a provider call.
- `VoyageEmbeddingService` is the real provider.
- The operational consumer (`OperationalEventKafkaRuntime.consumerResource`) uses `receipt-ttl-days = 8`. Topic
  `retention.ms` and `segment.ms` come from `docker/kafka/provision-access.sh` (`KAFKA_TOPIC_RETENTION_MS`,
  `KAFKA_TOPIC_SEGMENT_MS`, both 7 days). Analytics already declares the same pair as `kafka-retention` and
  `kafka-segment-roll`. The reader principal has READ and DESCRIBE on the topic, not DESCRIBE_CONFIGS.
- `ConsumerReceiptRepository.record` is the receipt and the whole effect. The build compiles with `-Werror`, so a
  non-exhaustive match fails compilation. `MongoConsumerReceiptRepository` takes only a `MongoDatabase` (no client or
  transaction runner), and `record` has three test fakes, so an effect parameter would change four implementations
  without enforcing anything.
- No Admin client is used in `src/main`; `KafkaClientSettings.clientProperties` already builds bounded-timeout
  properties for blocking clients. The application consumer authenticates as `analytics_reader` (shared with
  analytics). `KafkaTestNamespace` creates the events topic with `retention.ms` only (no `segment.ms`) and grants
  `analytics_reader` READ and DESCRIBE only.
- `InterviewNotificationProvider` and `InterviewCalendarProvider` already carry an `idempotencyKey` on every call.
  The only implementations are the ledger providers, keyed by `_id = idempotencyKey`.

## Decisions

- **DDC-01** Embedding work reuses a vector it already paid for within the same claim, keyed by model and source
  hash. It covers in-process retries and a revision conflict whose re-read has the same source hash. Nothing is
  cached across claims or entities: a durable cache would store derived candidate data a second time and need its own
  erasure. Residual: a worker that crashes, or loses its lease, after the provider answered pays once more. Voyage
  offers no idempotency key (assumption, to verify against provider documentation).
- **DDC-02** The deduplication window is enforced, not assumed. Before consuming, the operational consumer reads the
  topic's `retention.ms` and `segment.ms` from the broker and does not start unless
  `receipt TTL >= retention.ms + segment.ms + margin`, with a named margin of 1 hour (covers Kafka's retention check
  interval and clock skew; Mongo's TTL monitor lag only delays receipt deletion, so it needs no margin).
  Infinite retention (`-1`) never passes. The reader principal gets DESCRIBE_CONFIGS on this one topic only.
  Defaults change so the rule holds and Kafka keeps less data: topic `segment.ms` and analytics `kafka-segment-roll`
  become 1 day (maximum readable age 8 days instead of 14) and `receipt-ttl-days` becomes 9. Architecture review
  (2026-11-03) chose a broker read over a config-only declaration, which would stay a convention: the broker is the
  runtime truth, and the topic is altered by an operator outside the application config. Design:
  - Pure rule, no I/O: `ConsumerReceiptWindow.check(receiptTtl: FiniteDuration, retentionMs: Long, segmentMs: Long):
    Either[ConsumerReceiptWindowRefusal, Unit]` in `domain/policy` (next to `CircuitBreakerPolicy`). The refusal is a
    closed enum (`RetentionUnbounded`, `WindowExceedsReceiptTtl`); values are numbers only, never credentials.
  - Adapter: `KafkaTopicRetentionReader` (infrastructure/kafka) built from `KafkaConfig`, using fs2-kafka's
    `KafkaAdminClient.resource(AdminClientSettings)` (fs2-kafka 3.9.1 and kafka-clients 4.3.1 are already
    dependencies; no new dependency) and `describeConfigs` on one `ConfigResource(TOPIC, config.topic)`. Add
    `KafkaClientSettings.admin(...)` beside `consumer(...)` so SASL and the bounded `request.timeout.ms` /
    `default.api.timeout.ms` come from the same builder. Parse `retention.ms` and `segment.ms` as `Long`; a missing or
    unparseable entry is the typed failure `TopicConfigUnreadable`. `describeConfigs` returns the effective value
    (broker default included), so an unset `segment.ms` is read as the broker's roll, not as absent.
  - Placement: a startup gate inside `OperationalEventKafkaRuntime.consumerResource`, evaluated inside the
    `resilientStream` body before `KafkaConsumer.stream(...).subscribeTo`, as `Stream.eval(gate) >>
    <processing stream>.concurrently(beat)`. Every restart re-reads, so an operator fix is picked up without a process
    restart, and the existing `restartPolicy` (jitter capped by `restart-max-delay-seconds`) paces the retries. No
    consumer exists before the gate passes, so no offset can be committed. The gate is a `KafkaConfig => IO[TopicRetention]`
    parameter of `consumerResource` defaulting to the Kafka reader, so unit tests inject a fake.
  - Readiness: a refusal raises a typed error and gets no beat. `ReadinessComponent.KafkaConsumer` is registered at
    start (grace period) and then reports degraded once `workerStaleness` passes; degraded components are reported
    and never decide the readiness status code (`DegradedComponents`). The gate does not beat on failure. Before
    raising, emit one diagnostics event with the numbers (retention, segment, receipt TTL in milliseconds, so `-1` stays visible) because
    `LogFields.failure` records only the exception class and source location.
  - Provisioning: `provision-access.sh` adds `--operation DescribeConfigs --topic "$topic"` (the `kafka-acls.sh` spelling; the tool refuses `DESCRIBE_CONFIGS`, which stopped the first local `kafka-acl-init` start the operator observed, on 2026-11-08 per the operator's terminal output; see [Kafka flow hardening](kafka-flow-hardening.md)) for
    `User:analytics_reader` (the principal the application consumer uses through `KAFKA_READER_USERNAME`; analytics
    shares it, which is acceptable because the grant is on this one topic) and sets the segment default to
    `86400000`. The spelling dates from commit `7ac0bf1` (2026-11-03), so earlier local starts since then probably
    failed the same way silently (an inference: only the analytics workers wait on `kafka-acl-init`). `KafkaTestNamespace` grants DESCRIBE_CONFIGS on the events topic and sets `segment.ms` there (it sets
    only `retention.ms`, so the broker's 7 day roll would make every integration consumer refuse to start).
- **DDC-03** No `ConsumedEventEffect` type now. Architecture review (2026-11-03) found it speculative and not an
  enforcement: with one case it forces nothing, and a developer adding an effect would add a repository method or a
  second write rather than a case, so the compiler would not object. Atomicity also cannot be expressed through
  `record` today, because `MongoConsumerReceiptRepository` holds no session or transaction runner. Instead the rule
  moves from the inline comment in `recordDurably` to the owner of the contract, the `ConsumerReceiptRepository`
  scaladoc ("an effect keyed by the event id is written in the same transaction as the receipt insert; recording
  first loses the event on a crash, applying first applies it twice"), and `recordDurably` points to it. Revisit
  trigger: the first consumer effect that is not the receipt. That change must then introduce, with the effect, the
  transaction (`MongoTransactionRunner.sessions`) and a crash/duplicate test of the atomic write; if a closed type is
  wanted then, it is `enum ConsumedEventEffect { case ReceiptOnly }`, passed as `record(consumerGroup, event, effect,
  now, expiresAt)` and matched without a wildcard so `-Werror` rejects an unhandled case.
- **DDC-04** Provider adapters follow one written contract and prove it with shared contract suites. An adapter uses
  the provider's own idempotency mechanism (an idempotency header or a client-assigned id), or, when the provider has
  none, records a durable send intent per idempotency key before the call and resolves an unknown outcome through
  `lookup`, never by resending blindly. The suites run against the ledger providers now and against every future
  adapter.

- **DDC-02 (recheck, 2026-11-03 follow-up)** The gate also runs on a running consumer every 5 minutes
  (`ReceiptWindowRecheck`), concurrently with the heartbeat. A refusal or an unreadable configuration fails the
  processing stream, so the consumer closes; the restart re-runs the start gate, which keeps refusing, and no beat is
  recorded. Reason: an operator can raise `retention.ms` on a live topic, and receipts written after that would be too
  short until the next process restart. One admin describe per 5 minutes, not per 20 s heartbeat; a raise is detected
  long before any receipt written meanwhile can expire.
- **DDC-02 (outbox retention)** Published outbox rows keep their own named horizon `PublishedOutboxRetention` (7 days,
  TTL index `event_outbox_published_retention`). It is a cleanup horizon, not a replay source: nothing re-sends a
  Published row, so it need not equal the topic's `retention.ms`. Re-sends of `InFlight` rows are covered by DDC-02.13.
- **DDC-05** An embedding claim that takes over an expired `Processing` lease is reported once as
  `EMBEDDING_LEASE_RECLAIMED` (WARN, attempt count only). Reason: the previous holder may have paid the provider for a
  vector it never stored (DDC-01 residual). Persisting the held vector would close it but stores derived candidate data
  a second time with its own erasure, which this spec rejects; instead the residual is made measurable. Revisit
  trigger: reclaim count times average embedding cost becomes material in `docs/infrastructure-cost-and-sizing.md`.

Non-goals: no real email or calendar adapter; no cross-claim embedding cache; no query-embedding cache (a separate
cost question); no Kafka-side fencing tied to the lease; no change to analytics deduplication; no migration (no
stored shape changes).

Unknowns: whether Voyage bills a request that timed out on the client (assumed yes, so a timed-out retry can cost
twice; not avoidable without a provider idempotency key).

Residuals and contract impact (architecture review 2026-11-03):

- `retention.ms + segment.ms` bounds a record's age on a quiet topic too (DDC-02.9). Time retention compares a
  segment's newest record timestamp with `retention.ms`, and when every segment, including the active one, has expired,
  the broker rolls an empty segment and deletes the old one. No later append is needed. A record is therefore deleted
  no later than `retention.ms` after the newest record of its segment, which is at most `segment.ms` after the
  segment's first record. Verified against the test broker (`apache/kafka:3.9.2`): one-day roll, 1.5 s retention, two
  records, no further appends, earliest offset moved past both. The earlier "quiet topic keeps its last records" caveat
  was wrong and is removed. The bound uses producer `CreateTime`; a producer clock ahead of the broker delays deletion
  by that skew, which the margin absorbs. Group offset expiry (`offsets.retention.minutes`) or any offset reset only
  causes re-reads from the earliest retained offset (the consumer resets to `earliest`); every re-read is deduplicated
  by the same inequality, so the window does not depend on offset retention and the gate does not read that broker
  setting (it would need Cluster DESCRIBE_CONFIGS for no gain) (DDC-02.12).
- A re-sent outbox event is a new Kafka record with an old event id: an `InFlight` row whose lease expired after a
  successful send is claimed and sent again, possibly long after the first copy (for example after a publisher
  outage). Its receipt was written at the first consumption, so measured from first delivery it could expire while the
  later copy is still readable. A duplicate delivery therefore extends the receipt's `expiresAt` to `now + receipt TTL`
  (never shortens it), so the receipt outlives every copy (DDC-02.13).
- Weakest link (accepted, DDC-03): the receipt-effect rule is documentation only. The first developer who adds a
  consumer effect gets no compiler help; the revisit trigger above is the only guard.
- Receipts expire `receipt TTL` after consumption, and a record is consumed no earlier than it was produced, so the
  receipt outlives the record by at least the margin. Existing receipts keep their 8 day expiry and records
  produced before the topic is altered can stay readable up to 14 days; this transition ends 14 days after the topic
  change and the gate refuses the old topic configuration until it is altered, so rollout order is topic first.
- Analytics is safe with a 1 day roll: `ErasureCompletionRule.worstCaseCompletion` is the maximum of the Kafka worst
  case (now 8 days), `1 day + delta-log-retention` (15 days) and `delta-vacuum-safety` (7 days), so the default stays 15
  days and the startup budget check (`AnalyticsConfigReaders`, budget 21 days, cap 30) is unchanged. The assertion
  `worstCaseErasureCompletion == 15.days` in `AnalyticsRuntimeConfigSpec` still holds. Fixtures that spell
  `kafka-segment-roll = 7 days` (`AnalyticsRuntimeConfigSpec` lines 80-83, `AnalyticsTestOperationalConfig`,
  `AnalyticsPrivacyAndErasurePropertySpec`) copy the shipped file and move to 1 day with it; nothing else depends on 7
  days (`compose*.yaml` set only `KAFKA_LOG_RETENTION_HOURS=168`, a broker default that the topic override replaces).
- Files that carry `receipt-ttl-days = 8` or `receiptTtlDays = 8` and move to 9: `src/main/resources/application.conf`,
  `AppConfigSpec` (lines 86, 158, 273, 517), `OperationalEventKafkaRuntimeSpec` (48),
  `OperationalEventComposeIntegrationSpec` (53), and the `KafkaConsumerConfig(true, 8, 7, ...)` in
  `InterviewKafkaRestartIntegrationSpec` (328).
- Contracts: no stored shape changes (the receipt document and its `expiresAt` are unchanged; only the value written
  differs), no GraphQL change, no event envelope change, no migration. This is a pre-MVP single active shape change in
  configuration and broker provisioning only; `docs/schema-evolution.md` needs no expand/backfill plan.

## Acceptance criteria and evidence

| ID | Criterion (examples) | Evidence |
|---|---|---|
| DDC-01.1 | Provider succeeds, entity write fails `Unavailable` once, the retry persists the same vector: one provider call, work completed | EmbeddingPipelineSpec `DDC-01.1 ...`: 1 provider call, work completed (2026-11-03, run-local-tests PASS 1831/1831) |
| DDC-01.2 | A job's status changes during embedding (revision bump, same text): the conflict persists the held vector against the new revision with no second provider call, work completed | EmbeddingPipelineSpec `DDC-01.2 ...`: 1 provider call, persisted at the bumped revision (PASS) |
| DDC-01.3 | The text changes during embedding (new hash): the held vector is not used, a new provider call is made (correct cost) | EmbeddingPipelineSpec `DDC-01.3 ...`: 2 calls, stored vector is the second call's (PASS) |
| DDC-01.4 | Provider unavailable then available: two calls, as before (failure path unchanged) | EmbeddingPipelineSpec `DDC-01.4 ...`: 2 calls (PASS) |
| DDC-01.5 | A held vector never serves a different claim or entity | EmbeddingPipelineSpec `DDC-01.5 ...`: identical-text twin job pays its own call (PASS) |
| DDC-02.1 | Retention 7 days, segment 1 day, receipt TTL 9 days: the consumer starts | ConsumerReceiptWindowSpec, OperationalEventConsumerReceiptWindowSpec (unit, passing) |
| DDC-02.2 | Boundary: receipt TTL 8 days with 7 + 1: the consumer does not start, logs one diagnostics event with the three durations per restart attempt (attempts paced by the existing restart backoff), no beat is recorded so `kafka-consumer` becomes degraded after the staleness limit | ConsumerReceiptWindowSpec, OperationalEventConsumerReceiptWindowSpec: no beat, one diagnostics event with retentionMs/segmentMs/receiptTtlMs and windowRefusal; ConsumerReceiptWindowLoggingSpec: the values survive the real `SafeDiagnostics` sink and anything else renders `[FILTERED]` (unit, passing) |
| DDC-02.3 | Retention `-1`: the consumer does not start | ConsumerReceiptWindowSpec, OperationalEventConsumerReceiptWindowSpec (unit, passing) |
| DDC-02.4 | The topic config cannot be read (missing ACL, broker unreachable): the consumer does not start and retries with the existing restart backoff; no offset is committed | OperationalEventConsumerReceiptWindowSpec: TopicConfigUnreadable, no beat, read again on restart (unit, passing; no consumer exists so no offset commits) |
| DDC-02.5 | Shipped defaults (application.conf, provisioning script, analytics config) satisfy the rule: a test parses the shipped files and applies `ConsumerReceiptWindow.check` | ShippedConsumerReceiptWindowSpec (unit, passing) |
| DDC-02.6 | Against real Kafka with the provisioned ACLs (script and `KafkaTestNamespace`), the reader reads its topic's config and starts; describing another topic is denied | KafkaTopicRetentionReaderIntegrationSpec against isolated Kafka (3 tests, passing in run-local-tests.sh); provision-access.sh is covered by DDC-02.5 static parse and by two starts of the local Compose broker's `kafka-acl-init` on 2026-11-08, both reported by the operator: the first stopped at the `DESCRIBE_CONFIGS` spelling (`kafka-acls.sh` wants `DescribeConfigs`); after the correction the rerun exited 0. The rerun's grant list comes from the operator's terminal output (a git-ignored transcript, not an independent capture). "Every grant applied" is an inference from exit 0 under `set -euo pipefail` plus the printed output (one workstation; see [Kafka flow hardening](kafka-flow-hardening.md)) |
| DDC-02.7 | Margin: receipt TTL 8 days with retention 7 days and segment 23 hours 30 minutes (margin 30 minutes): the consumer does not start; with segment 23 hours it starts | ConsumerReceiptWindowSpec, OperationalEventConsumerReceiptWindowSpec (unit, passing) |
| DDC-02.8 | A refusal followed by a corrected topic config (fake reader changes answer): the consumer starts on a later restart without a process restart | OperationalEventConsumerReceiptWindowSpec (unit, passing) |
| DDC-02.9 | Quiet topic: retention 1.5 s, segment roll 1 day, two records and no further append: the earliest offset passes both records | KafkaQuietTopicRetentionIntegrationSpec against the isolated `apache/kafka:3.9.2` broker (PASS, run-local-tests 1823/1823, 2026-11-03) |
| DDC-02.10 | A running consumer whose topic retention is raised past the receipt TTL stops at the next recheck and records no further beat | OperationalEventConsumerReceiptWindowSpec `DDC-02.10 ...` with the real `KafkaConsumer.stream`: beats before the raise, none after (unit, PASS) |
| DDC-02.11 | A recheck that cannot read the topic configuration stops the consumer; a corrected answer starts it again on restart | OperationalEventConsumerReceiptWindowSpec `DDC-02.11 ...`: `RuntimeFailed` with `TopicConfigUnreadable`, then beats resume after the corrected answer (unit, PASS) |
| DDC-02.12 | The committed consumer resets to `earliest`, reads committed records and never auto-commits; a group whose offsets are deleted rereads and stores no second receipt | KafkaClientSettingsSpec `DDC-02.12 ...` (existing assertion renamed; unit, PASS); KafkaTopicRetentionReaderIntegrationSpec `DDC-02.12 ...`: group deleted with its offsets, at least two deliveries, one receipt insert (integration, PASS) |
| DDC-02.13 | A duplicate delivery a day later extends the receipt's `expiresAt` to the later delivery; an earlier expiry never shortens it | MongoConsumerReceiptExpiryIntegrationSpec, two `DDC-02.13` tests against the real `consumer_receipts` validator (integration, PASS) |
| DDC-02.14 | Published outbox rows use the named `PublishedOutboxRetention`, documented as independent of topic retention | `OperationalEventKafkaRuntime.PublishedOutboxRetention` and its scaladoc (static review) |
| DDC-03.1 | `record`: first delivery true, duplicate false, one receipt (existing behavior and tests unchanged) | OperationalEventKafkaRuntimeSpec `DDC-03.1 duplicate delivery acknowledges once…` (renamed, body unchanged; unit, passing) and MongoInterviewErasureIndexIntegrationSpec receipt insert (first true, second false, one stored receipt; integration, passing) |
| DDC-03.3 | Redelivering a received event (three deliveries) stores one receipt and applies the consumer effect once; every delivery is acknowledged | OperationalEventKafkaRuntimeSpec `DDC-03.3 redelivering a received event stores no second receipt and applies no second effect`: fake store mirrors the unique key (duplicate reports false), no pre-check (unit, passing 34/34) |
| DDC-03.2 | The rule lives in the `ConsumerReceiptRepository` scaladoc and `recordDurably` refers to it; no effect type is added; the revisit trigger is recorded here | ConsumerReceiptRepository scaladoc (OperationalEventRepositories.scala) and recordDurably comment; no effect type added (static) |
| DDC-04.1 | Contract: the same notification idempotency key twice gives one delivery and the same receipt | InterviewNotificationProviderContract `DDC-04.1`, run by MongoInterviewNotificationContractIntegrationSpec on the Mongo ledger (PASS) |
| DDC-04.2 | Contract: concurrent calls with one key give one delivery | InterviewNotificationProviderContract `DDC-04.2` (8 concurrent calls, one receipt), Mongo ledger (PASS) |
| DDC-04.3 | Contract: after an unknown outcome, `lookup` returns the recorded effect | Notification and calendar contracts `DDC-04.3` lookup after an unseen outcome, Mongo ledgers (PASS) |
| DDC-04.4 | Contract: the same for calendar reserve and cancel keys; different keys stay independent | InterviewCalendarProviderContract `DDC-04.4` concurrent reserve, concurrent cancel, independent keys; notification `DDC-04.4` independent keys; Mongo ledgers (PASS) |
| DDC-04.5 | Port documentation states the adapter rule of DDC-04 | Scaladoc on InterviewCalendarProvider and InterviewNotificationProvider in InterviewProviders.scala (static review) |
| DDC-05.1 | A claim that took over an expired lease emits one `EMBEDDING_LEASE_RECLAIMED`; a fresh claim emits none | EmbeddingPipelineSpec `DDC-05.1 ...` (unit, PASS) |
| DDC-05.2 | Against Mongo, claiming an expired `Processing` lease reports a reclaim; claiming a `Ready` row does not | MongoEmbeddingWorkIntegrationSpec `DDC-05.2 ...`: also checks the returned token equals the stored one, generation and attempts unchanged (integration, PASS) |

## Documentation impact

| Document | Decision to state | Why it matters | Status |
|---|---|---|---|
| `ARCHITECTURE.md` | Delivery is at-least-once; a lease is not exclusivity; the store's unique key decides duplicates; the consumer refuses to start unless receipts outlive `retention.ms + segment.ms + 1 h`; the check repeats every 5 min while running; a duplicate delivery extends `expiresAt`; a record is readable at most `retention.ms + segment.ms`, also on a quiet topic; reclaimed embedding leases are logged | Duplicates applied twice corrupt notifications and analytics silently | done: "Duplicate delivery containment" paragraph after the Retrieval and durable publication boundaries intro; held-vector paragraph in "Embedding Pipeline". Follow-up done (DDC-02.9 to 02.14, DDC-05). The `ConsumerReceiptWindow` scaladoc states the same quiet-topic bound |
| `docs/domain-language.md` | Terms: consumer receipt, deduplication window, provider idempotency contract | Shared vocabulary for specs, tests and reviews | done: three rows after "Operational event". Follow-up done: receipt expiry is raised by duplicates; the window is re-read every 5 min and stops a running consumer |
| `docs/mongodb-design.md` | Receipts expire after 9 days and why; a duplicate delivery raises the expiry (`$max`, never lower); any consumer effect is written with its receipt in one transaction (revisit trigger); Published outbox rows keep `PublishedOutboxRetention` (7 days), independent of topic retention | A receipt that expires first lets an old event through as new; an outbox re-send after a long outage would otherwise find no receipt | done: consumer receipts paragraph in "Current operational persistence". Follow-up done: receipt extension, and the Published outbox horizon paragraph after it |
| `docs/big-data-architecture.md` | Segment roll 1 day: maximum readable age 8 days instead of 14, also on a quiet topic; erasure worst case unchanged at 15 days | Analytics erasure budget and Kafka disk | done: operational consumer paragraph, "Erasure and limits" (budget term, raw-record age, provisioning values). Follow-up done: removed "while the topic keeps receiving appends" from the raw-record age sentence |
| `docs/infrastructure-cost-and-sizing.md` | Kafka holds at most 8 days instead of 14; receipts +1 day, and a duplicate adds no document; repeat embedding calls avoided within a claim; residual paid-twice case measured by `EMBEDDING_LEASE_RECLAIMED`, with its revisit trigger | Cost: the user pays for neither duplicate storage nor duplicate provider calls | done: Kafka row, Embeddings "Repeat provider calls", Kafka "Held data" and "Consumer receipts" paragraphs. Follow-up done: reclaim signal and revisit trigger in "Repeat provider calls"; receipt-storage line. No reclaim count is recorded |
| `docs/wiki/workflows-and-patterns.md` | Lease vs fencing vs dedupe lesson; receipt/effect ordering table; held-vector reuse; provider adapter rule | Reusable pattern for future consumers and adapters | done: "Duplicate delivery containment" section before the scheduled-interview section. Follow-up done: dedupe row, 5-minute recheck and expiry extension, reclaim signal |
| `docs/wiki/security-and-operations.md` | DESCRIBE_CONFIGS on the events topic only for the reader; rollout order (alter topic first); diagnosing `CONSUMER_RECEIPT_WINDOW_REFUSED`; recheck stops a running consumer; quiet topic and offset resets stated as behavior, not residuals | Least privilege and safe rollout | done: "Kafka consumer receipt window" section before "Health and observability". Follow-up done: recheck, event names as emitted, offset and quiet-topic statements, residuals narrowed to the 14-day transition and the pre-change receipts |
| `docs/wiki/mongodb-and-analytics.md` | Receipt TTL and segment roll values; receipt extension; Published outbox TTL index and horizon | Keeps the wiki's collection and retention facts true | done: receipt TTL in the `consumer_receipts` row and the delivery paragraph; segment roll is not a collection fact here and is stated in the security and operations wiki. Follow-up done: `consumer_receipts` expiry rule and `event_outbox` Published-only TTL row |
| `README.md` | One-line summary of duplicate-delivery guarantees linking the spec | Entry point for readers | done: Kafka paragraph under the local event publication section. Follow-up: no change needed; the paragraph states no quiet-topic or extension caveat |
| `docs/architecture-review.md` | Partial TTL indexes delete only Published outbox rows and Failed `search_session_work` rows; Failed outbox rows have no TTL | A reviewer reading the review would otherwise think failed outbox evidence expires | Follow-up done: line 93 corrected, checked against `MongoHiringIndexSetup.scala` |
| `docs/local-observability.md` | Log events `CONSUMER_RECEIPT_WINDOW_REFUSED` (ERROR) and `EMBEDDING_LEASE_RECLAIMED` (WARN) with fields | Operators search these events when the consumer stops or a reclaim happens | Follow-up done: "Log events for delivery containment". `EMBEDDING_PROVIDER_FAILED` is documented in no canonical document; not added here (outside this spec) |

## Checkpoint

- 2026-11-03: architecture review of DDC-02/03 done. DDC-02 approved with the design above (gate, margin, provisioning
  and test-namespace changes added); DDC-03 changed from a one-case effect type to port documentation plus a revisit
  trigger. DDC-01 and DDC-04 not redesigned.
- 2026-11-03: DDC-01..04 implemented and tested locally. Root unit 1820/1820; affected integration suites 1852/1852
  (unit phase included) against isolated MongoDB and Kafka; analytics full `sbt test` 616 passed, 2 skipped (the
  skipped ones not identified; likely opt-in `assume` guards). Canonical documents and wiki updated (table above).
- 2026-11-03: first review round. Code Review FAIL (stale spec status and checkpoint; stale AEC-02.3 Kafka term;
  gate-script limits), Security FAIL (refusal fields filtered in the real log sink; `.githooks/` unguarded), QA PASS
  with minor findings (DDC-03.1 untagged). All fixed: `windowRefusal` field with sink allow-listing and a
  `SafeDiagnostics` test, DDC-03.1 tag, guard protects `.githooks/`, spec and AEC-02.3 corrected, jitter stated.
- Not verified: the reader starting against a re-provisioned Compose broker (the 2026-11-08 `kafka-acl-init` starts
  are operator-reported, see the 2026-11-08 entry at the end; before that, only the static parse and the equivalent
  `KafkaTestNamespace` grant); the whole `scripts/check-local.sh`; deployed brokers; which two analytics tests are
  skipped.
- 2026-11-03: re-review of the fixes. Code Review PASS, Security PASS (two Low gate-script items then fixed: `-F`
  files read only inside the project up to 64 KiB, and `--no-verify` commits never deferred), QA PASS (root unit
  1820/1820, receipt-window and Kafka integration run 1830/1830, gate tests 29/29). Closed.
- 2026-11-03: follow-up DDC-02.9 to DDC-02.14 and DDC-05 implemented in worktree branch `ddc-residuals` (from
  `b4e216b`). Quiet-topic run 1823/1823 PASS; implementation run (all unit tests plus KafkaTopicRetentionReader,
  MongoConsumerReceiptExpiry, MongoEmbeddingWork and MongoInterviewErasureIndex integration suites) 1856/1856 PASS,
  0 skipped. Documentation updated (table above).
- 2026-11-03: follow-up reviews. Security PASS (one Low doc gap fixed: receipts are outside account erasure, bounded by
  TTL after the last copy). Code Review FAIL (DDC-02.10 test sampled beats after the consumer was released), fixed:
  beats are sampled while the consumer runs, and a mutation (recheck replaced by `IO.unit`) fails DDC-02.10 and
  DDC-02.11; re-review PASS. QA PASS on the final state: docs gate PASS; all unit tests plus the quiet-topic,
  retention-reader, receipt-expiry, embedding-work, erasure-index and Compose integration suites 1859/1859, 0 skipped;
  `scalafmtCheckAll` clean. Not verified: deployed brokers (quiet-topic deletion shown on `apache/kafka:3.9.2` only).
- 2026-11-08 (Technical Writer, documentation only; operator-reported facts). The 2026-11-03 provisioning change in commit `7ac0bf1` (the DDC change) wrote `--operation DESCRIBE_CONFIGS` for the topic grant in `docker/kafka/provision-access.sh`. `kafka-acls.sh` on `apache/kafka:3.9.2` refuses that spelling for a topic, so the first local `kafka-acl-init` start the operator observed stopped at that grant with the container exiting with code 1. Under `set -euo pipefail` the grants after it were not applied by that start, and every local start since 2026-11-03 probably failed at that grant after the grants above it were applied, without a visible signal (an inference: only the analytics workers wait on `kafka-acl-init`). The line is now `DescribeConfigs` (`provision-access.sh` line 98); the operator's rerun exited 0. The evidence is the operator's terminal output only, transcribed into a git-ignored file, not an independent capture. The 2026-11-03 PASS verdicts and the Closed entry above predate the fix and do not cover it. Not verified: the reader starting against a re-provisioned Compose broker. The spec is reopened (`in review`) for DDC-02.6 until independent verdicts cover the fix. The same defect also stopped, in the starts that stopped early, the WRITE removals for the retired `interview_orchestrator` and `interview_worker` principals on `hiring.interview-commands` and `hiring.interview-results` (`provision-access.sh` lines 104-108, which run after line 98). Impact is low because those principals have no SCRAM credential (an inference from the script, which creates none for them). The unused READ grant on group `hiring-operational-events-integration` was removed from the script after commit `72f24ef`, because nothing uses that group; the script has no removal for that grant (its only removals are for the retired `publisher` and interview principals, lines 86-89 and 104-108), so a broker that kept its data volume keeps the old grant until an operator removes it, for example with `kafka-acls.sh --remove --force --allow-principal User:analytics_reader --operation READ --group hiring-operational-events-integration`. Cross-reference: [Kafka flow hardening checkpoint](kafka-flow-hardening.md#checkpoint-and-review).
