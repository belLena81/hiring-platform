# Hiring Event Publication And Telemetry

Status: implemented locally; Docker Kafka integration evidence and independent review gates pending.

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
- Implemented: sanitized v1 operational envelope JSON, transactional Mongo outbox writes for job/application/account-deletion facts, search sessions, authenticated view/click telemetry, outbox leasing/retry/failure states, Kafka publisher resource, receipt/quarantine repositories, consumer offset discipline, local Kafka Compose service, and additive GraphQL `searchId` support.
- Local unit evidence: `sbt test` passed 264 tests on 2026-09-20 after the Kafka/runtime/schema changes.
- Local Mongo integration evidence: `sbt 'IntegrationTest / testOnly com.example.graphQL.cats.repository.mongo.MongoHiringRepositoriesIntegrationSpec'` passed 18 tests on 2026-09-20 after fixing stale repository API calls and vector transactional setup.
- Evidence pending: Docker-backed Kafka integration with 100 events and commit-to-receipt latency, full `IntegrationTest / test` still has an unrelated `MongoDatabaseProbeIntegrationSpec` diagnostic-correlation failure, and independent Code Reviewer, Security Engineer, and QA verdicts must be rerun after fixes.
