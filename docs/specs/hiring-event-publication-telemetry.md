# Hiring Event Publication And Telemetry

Status: implemented locally; focused and Compose evidence recorded; independent review gates pending refresh.

## Scope

Deliver operational hiring facts through a MongoDB transactional outbox and a local Kafka topic named `hiring.operational-events.v1`. MongoDB mutations must not depend on Kafka availability.

## Non-goals

No Spark, Delta, managed Kafka, Saga workflow, account/profile events, historical backfill, raw credentials, tokens, emails, names, resumes, profile text, headers, or embeddings.

## Acceptance Criteria

- `AC-01`: Successful job/application mutations write the required outbox fact(s) atomically; injected outbox failure rolls back the operational write.
- `AC-02`: Durable search sessions and interaction mutations reject forged actor/result data and repeated client event IDs produce one logical event.
- `AC-03`: Broker outage, publisher restart, expired lease, and lost acknowledgement preserve retryable outbox work without failing completed Mongo operations.
- `AC-04`: Duplicate delivery and consumer restart yield one receipt; invalid or unsupported records quarantine without a successful offset commit.
- `AC-05`: v1 fixtures decode unchanged; unknown versions are not reinterpreted; application ordering includes status-change before candidate-hired.
- `AC-06`: Local Compose integration evidence uses 100 events, a disposable replica set, and one broker; latency is local evidence only against the UC11 30-second p95 target.

## Current Checkpoint

- Source inspected: service protocols, job/application/search services, Mongo repositories/setup/codecs, GraphQL schema/input/model/resolvers, runtime wiring, config, Compose.
- Implemented: sanitized v1 operational envelope JSON, transactional Mongo outbox writes for job/application/account-deletion facts, search sessions with structured filter/model/result metadata but no verbatim free-form query retention, authenticated view/click telemetry, outbox leasing/retry/failure states, Kafka publisher resource, receipt/quarantine repositories, consumer offset discipline, local Kafka Compose service, and additive GraphQL `searchId` support.
- Local unit evidence: `sbt test` passed 264 tests on 2026-09-20 after the Kafka/runtime/schema changes.
- Local Mongo integration evidence: `sbt 'IntegrationTest / testOnly com.example.graphQL.cats.repository.mongo.MongoHiringRepositoriesIntegrationSpec'` passed 18 tests on 2026-09-20 after fixing stale repository API calls and vector transactional setup.
- Local Mongo integration evidence: `sbt 'IntegrationTest / testOnly com.example.graphQL.cats.repository.mongo.MongoHiringRepositoriesIntegrationSpec'` passed 19 tests on 2026-09-20, including transactional job rollback when the outbox insert conflicts.
- Kafka handler evidence: `sbt testOnly com.example.graphQL.cats.infrastructure.kafka.OperationalEventKafkaRuntimeSpec` passed the 267-test unit suite, covering malformed/unsupported quarantine, duplicate acknowledgement, sequence gaps, and first-observed hire rejection.
- Compose evidence: with `docker compose up -d`, `PHASE5_COMPOSE_EVIDENCE=true sbt 'IntegrationTest / testOnly com.example.graphQL.cats.repository.mongo.OperationalEventComposeIntegrationSpec'` passed one test on 2026-09-20 using 100 events from one writer, a disposable replica set, and one broker; clean-topic commit-to-receipt p95 was 8,061 ms. This is local evidence only.
- Remaining evidence: broker-outage/restart/lost-ack and consumer-restart/quarantine offset-discipline scenarios still require live failure-injection runs; full `IntegrationTest / test` still has the unrelated `MongoDatabaseProbeIntegrationSpec` diagnostic-correlation failure. Independent Code Reviewer, Security Engineer, and QA verdicts must be rerun after these changes.
