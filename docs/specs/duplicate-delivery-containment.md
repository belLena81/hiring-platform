# Duplicate delivery containment

Status: done (local evidence; deployed brokers and a re-provisioned Compose broker not verified). Context: Operational events (Kafka publication and consumption) and AI search
(embedding work), plus the interview provider ports of Interview scheduling.

## Problem

A lease gives probably one worker, not exactly one, and at-least-once delivery hands every consumer duplicates. Data
correctness already holds on every path (receipt unique `_id`, interview inbox in the state transaction, Delta MERGE
by event id, lease-token-fenced completion). Four gaps remain where a duplicate still costs money or can reopen a
correctness hole after a later change:

1. A successful, paid embedding call is repeated when the entity write that follows it fails transiently or loses its
   revision to an unrelated edit of the same entity.
2. "A duplicate key means already handled" holds only while a receipt outlives every message Kafka can still deliver.
   Nothing checks that, and the current defaults already break it: Kafka deletes closed segments only, so a record can
   stay readable for `retention.ms + segment.ms` (7 + 7 = 14 days) while receipts expire after 8 days.
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
  - Provisioning: `provision-access.sh` adds `--operation DESCRIBE_CONFIGS --topic "$topic"` for
    `User:analytics_reader` (the principal the application consumer uses through `KAFKA_READER_USERNAME`; analytics
    shares it, which is acceptable because the grant is on this one topic) and sets the segment default to
    `86400000`. `KafkaTestNamespace` grants DESCRIBE_CONFIGS on the events topic and sets `segment.ms` there (it sets
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

Non-goals: no real email or calendar adapter; no cross-claim embedding cache; no query-embedding cache (a separate
cost question); no Kafka-side fencing tied to the lease; no change to analytics deduplication; no migration (no
stored shape changes).

Unknowns: whether Voyage bills a request that timed out on the client (assumed yes, so a timed-out retry can cost
twice; not avoidable without a provider idempotency key).

Residuals and contract impact (architecture review 2026-11-03):

- `retention.ms + segment.ms` bounds a record's age only while the topic keeps receiving appends: Kafka rolls the
  active segment on the next append after `segment.ms`, and deletes closed segments only. On a quiet topic the last
  records can stay readable beyond the bound (the erasure spec records the same caveat). The gate proves the
  configuration, not the physical log. Group offset expiry (`offsets.retention.minutes`) can also make a returning
  group reread everything still retained. Both are bounded by the same window while appends continue.
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
| DDC-02.6 | Against real Kafka with the provisioned ACLs (script and `KafkaTestNamespace`), the reader reads its topic's config and starts; describing another topic is denied | KafkaTopicRetentionReaderIntegrationSpec against isolated Kafka (3 tests, passing in run-local-tests.sh); provision-access.sh itself is covered by DDC-02.5 static parse only, not run against a re-provisioned Compose broker |
| DDC-02.7 | Margin: receipt TTL 8 days with retention 7 days and segment 23 hours 30 minutes (margin 30 minutes): the consumer does not start; with segment 23 hours it starts | ConsumerReceiptWindowSpec, OperationalEventConsumerReceiptWindowSpec (unit, passing) |
| DDC-02.8 | A refusal followed by a corrected topic config (fake reader changes answer): the consumer starts on a later restart without a process restart | OperationalEventConsumerReceiptWindowSpec (unit, passing) |
| DDC-03.1 | `record`: first delivery true, duplicate false, one receipt (existing behavior and tests unchanged) | OperationalEventKafkaRuntimeSpec `DDC-03.1 duplicate delivery acknowledges once…` (renamed, body unchanged; unit, passing) and MongoInterviewErasureIndexIntegrationSpec receipt insert (first true, second false, one stored receipt; integration, passing) |
| DDC-03.2 | The rule lives in the `ConsumerReceiptRepository` scaladoc and `recordDurably` refers to it; no effect type is added; the revisit trigger is recorded here | ConsumerReceiptRepository scaladoc (OperationalEventRepositories.scala) and recordDurably comment; no effect type added (static) |
| DDC-04.1 | Contract: the same notification idempotency key twice gives one delivery and the same receipt | InterviewNotificationProviderContract `DDC-04.1`, run by MongoInterviewNotificationContractIntegrationSpec on the Mongo ledger (PASS) |
| DDC-04.2 | Contract: concurrent calls with one key give one delivery | InterviewNotificationProviderContract `DDC-04.2` (8 concurrent calls, one receipt), Mongo ledger (PASS) |
| DDC-04.3 | Contract: after an unknown outcome, `lookup` returns the recorded effect | Notification and calendar contracts `DDC-04.3` lookup after an unseen outcome, Mongo ledgers (PASS) |
| DDC-04.4 | Contract: the same for calendar reserve and cancel keys; different keys stay independent | InterviewCalendarProviderContract `DDC-04.4` concurrent reserve, concurrent cancel, independent keys; notification `DDC-04.4` independent keys; Mongo ledgers (PASS) |
| DDC-04.5 | Port documentation states the adapter rule of DDC-04 | Scaladoc on InterviewCalendarProvider and InterviewNotificationProvider in InterviewProviders.scala (static review) |

## Documentation impact

| Document | Decision to state | Why it matters | Status |
|---|---|---|---|
| `ARCHITECTURE.md` | Delivery is at-least-once; a lease is not exclusivity; the store's unique key decides duplicates; the consumer refuses to start unless receipts outlive `retention.ms + segment.ms + 1 h` | Duplicates applied twice corrupt notifications and analytics silently | done: "Duplicate delivery containment" paragraph after the Retrieval and durable publication boundaries intro; held-vector paragraph in "Embedding Pipeline" |
| `docs/domain-language.md` | Terms: consumer receipt, deduplication window, provider idempotency contract | Shared vocabulary for specs, tests and reviews | done: three rows after "Operational event" |
| `docs/mongodb-design.md` | Receipts expire after 9 days and why; any consumer effect is written with its receipt in one transaction (revisit trigger) | A receipt that expires first lets an old event through as new | done: consumer receipts paragraph in "Current operational persistence" |
| `docs/big-data-architecture.md` | Segment roll 1 day: maximum readable age 8 days instead of 14; erasure worst case unchanged at 15 days | Analytics erasure budget and Kafka disk | done: operational consumer paragraph, "Erasure and limits" (budget term, raw-record age, provisioning values) |
| `docs/infrastructure-cost-and-sizing.md` | Kafka holds at most 8 days instead of 14; receipts +1 day; repeat embedding calls avoided within a claim; residual paid-twice case | Cost: the user pays for neither duplicate storage nor duplicate provider calls | done: Kafka row, Embeddings "Repeat provider calls", Kafka "Held data" and "Consumer receipts" paragraphs |
| `docs/wiki/workflows-and-patterns.md` | Lease vs fencing vs dedupe lesson; receipt/effect ordering table; held-vector reuse; provider adapter rule | Reusable pattern for future consumers and adapters | done: "Duplicate delivery containment" section before the scheduled-interview section |
| `docs/wiki/security-and-operations.md` | DESCRIBE_CONFIGS on the events topic only for the reader; rollout order (alter topic first); diagnosing `ConsumerReceiptWindowRefused` | Least privilege and safe rollout | done: "Kafka consumer receipt window" section before "Health and observability" |
| `docs/wiki/mongodb-and-analytics.md` | Receipt TTL and segment roll values | Keeps the wiki's collection and retention facts true | done: receipt TTL in the `consumer_receipts` row and the delivery paragraph; segment roll is not a collection fact here and is stated in the security and operations wiki |
| `README.md` | One-line summary of duplicate-delivery guarantees linking the spec | Entry point for readers | done: Kafka paragraph under the local event publication section |

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
- Not verified: `provision-access.sh` against a re-provisioned Compose broker (static parse and the equivalent
  `KafkaTestNamespace` grant only); the whole `scripts/check-local.sh`; deployed brokers; which two analytics tests
  are skipped.
- 2026-11-03: re-review of the fixes. Code Review PASS, Security PASS (two Low gate-script items then fixed: `-F`
  files read only inside the project up to 64 KiB, and `--no-verify` commits never deferred), QA PASS (root unit
  1820/1820, receipt-window and Kafka integration run 1830/1830, gate tests 29/29). Closed.
