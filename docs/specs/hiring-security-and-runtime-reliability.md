# Hiring security and runtime reliability

Status: complete with local verification (2026-10-08). Implements the approved eleven-point review plan.

## Decisions and scope

Preserve pure domain lifecycle/Saga decisions, IO/EitherT ports, transactional outboxes and resource ownership. Admin is provisioned only by explicit startup seed configuration (disabled by default). Authentication receipt HMAC keys derive from the JWT secret with domain separation; subsequent JWT rotation invalidates outstanding authentication receipt matches. The user authorized committing all reviewed changes after local completion. Deployment, push, local database reset and lakehouse reset remain outside this task.

## Acceptance

| ID | Required behavior | Evidence |
|---|---|---|
| HRR-01 | Password-bearing account receipt fingerprints are tagged HMACs of an unambiguous operation/scope/input-digest tuple; intermediate plain digest never persists/logs. Login/signup token replay verifies active referenced identity, canonical name and password. | PASS: `HmacAuthenticationFingerprintSpec`, `UserAccountServiceSpec` (protected repository inputs and credential-verified replay); `MongoAccountRegistrationIntegrationSpec`. |
| HRR-02 | Public bootstrap is removed. Disabled-by-default redacted startup seed runs after Mongo setup before workers/HTTP; atomic singleton; same-name existing Admin no-op without password reset; conflicts/inconsistent state fail closed. | PASS: GraphQL SDL/access and config tests; `MongoAdminSeedIntegrationSpec` verifies disabled seed, concurrent singleton, canonical-name no-op/password preservation, conflicting identity and corrupt registry. End-to-end startup seed/login/account-deletion recovery proof passed. |
| HRR-03 | Login/signUp idempotency fingerprints are HMAC-protected with a key independent of JWT signing when configured. No conversion of older receipt shapes pre-MVP. | PASS: `UserAccountServiceSpec` and `AppConfigSpec` receipt-secret cases. Historical migration evidence removed with the migration. |
| HRR-04 | Operational/interview Kafka restart full jitter is capped by validated 1–300 second configuration, default 30; unlimited retry and terminal classification preserved. | PASS: `KafkaRestartConfigSpec`, prolonged-outage policy checks through retry 1000, operational/interview recovery integration suites; unlimited retry and cancellation retained. |
| HRR-05 | Ambiguous analytics mutex insertion reconciles only original owner using majority reads and bounded retries; cancellation safe; unknown ownership fails closed, no takeover. | PASS: `AnalyticsMutexRecoverySpec`, 5/5 including masked 30-second deadline/shared elapsed budget and cancellation finalization; Mongo publisher integration confirms lost/duplicate acknowledgement ownership and release. |
| HRR-06 | Voyage response size bounded before decoding to 64 KiB + 32 * dimension; exactly one finite correctly sized/modelled vector required. | PASS: `VoyageEmbeddingServiceSpec`: exact byte boundary, over-limit chunked input without tail consumption, resource release, multiple-vector rejection, finite/dimension/model validation. |
| HRR-07 | Search work attempts mean prior durably recorded failures; maxAttempts bounds recorded failures. Crash recovery or an unsuccessful persistence transition may repeat an execution. Retry/fail persistence outcomes handled explicitly; lease recovery preserved. | PASS: `SearchSessionHandoffSpec` and `MongoSearchSessionRetryIntegrationSpec` (2/2): final allowed attempt, explicit persistence outcomes, stored counter, lease recovery and stale transition rejection. |
| HRR-08 | Receipt maintenance pages <=500 candidate IDs and checks references across every Delta lineage/revision and active Mongo replay; per-page transaction rechecks controls/eligibility; <=1000 deletions, fixed cutoff/high-water, no starvation behind protected prefixes. Memory bounded, duration not claimed fixed. | PASS: latest publisher/replay integrations (9/9 combined), including 600 protected + 1,200 removable receipts, <=500 candidate pages, 1,000 deletion limit, fixed high-water and changing controls. Final Delta lineage/malformed-reference regressions passed (9/9). |
| HRR-09 | Kafka config structural validation pure; DNS injected and run through blocking effect; loopback/Compose rules preserved. | PASS: pure structural configuration and injected blocking DNS preflight (`KafkaConnectionPreflightSpec`, 2/2; `AnalyticsRuntimeConfigSpec`, 20/20); adapters enforce preflight for direct construction too. |
| HRR-10 | Argon2 interpreter moves to infrastructure preserving semantics; Caffeine allocated per resource acquisition within IO. | PASS: root auth regressions and `GraphQLDocumentCacheSpec` including independent cache on reacquisition; dependency direction reviewed. |
| HRR-11 | Unused analytics cats-retry removed without changing persisted retry policies. | PASS: analytics compilation and full 483-test unit suite after removing unused dependency; no persisted retry-policy change. |

## Contracts and recovery

Remove bootstrap GraphQL field/input/result and update SDL/operations/tests/docs together under pre-MVP single-active-shape policy. Keep login/signup public inputs/idempotency keys. No receipt conversion or old-fingerprint reader exists pre-MVP; older receipts expire by TTL (receipts written before HMAC protection can remain readable to a database operator for up to the 7-day receipt TTL after deploy; a one-off delete of login/signUp receipts removes that window). Preserve other mutation receipts and all users.

First-run seed creates the singleton Admin (name via AccountName canonicalization); once the registry is Initialized with a valid admin identity the seed is a no-op, so later renames, status or password changes never fail startup and the seed never changes an existing account password. Seed emits no token and redacts credentials. Account receipt actor scopes remain unchanged for replay continuity.

Analytics compaction holds existing lakehouse mutex, uses ascending keyset pages and initial eligible high-water, checks all dependency states, fails closed on malformed/unavailable dependencies, and advances beyond protected pages. No new stored cursor or analytics schema. Mutex reconciliation never interprets a single absent read as proof an uncertain insert cannot later appear.

## Owners and sequencing

- Authentication owner: account services/config/runtime, public bootstrap removal, HMAC/migration, hasher relocation and associated tests/fixtures. Root coordinates shared Kafka model edits.
- Operational recovery owner: Kafka retry policy/config, search-session retry semantics and tests.
- Analytics owner: lock recovery, bounded maintenance, DNS boundary, dependency cleanup and tests.
- Root: provider response bound, cache allocation, integration, documentation, final verification and review coordination.

## Verification and reviews

Local Java 17 evidence on 2026-10-08:

- Root `sbt test`: **724 passed**. Root `scalafmtCheckAll`, `scalafmtSbtCheck`, and integration-test compilation passed.
- Full manifest-owned MongoDB/Kafka integration run: **183 passed, 2 failed, 9 skipped**. Both failures were missing fixture idempotency keys colliding with an existing unique index; corrected fixture rerun: **2/2 passed**, with formatting passed. Thus all 185 executed integration cases have passing evidence; the full run itself was not green. Nine opt-in Atlas, Compose report and scaling gates remain skipped.
- Analytics full unit suite: **483 passed** before the final review refinements. Latest affected Mongo integrations: **9 passed**. Final masked mutex/DNS/config rerun: **27 passed**. Final Delta regressions passed (9/9); analytics source and build formatting checks passed.
- Both changed shell proof scripts pass `bash -n`; four embedded streaming-proof Python blocks parse successfully. Opt-in account-deletion recovery proof passed (1/1) against manifest-owned services, including real API startup with configured Admin seed/login and recovery after an intentionally rejected producer fence.
- Independent Code Reviewer and Security Engineer: **PASS** across all changed scopes. Authentication author independently reviewed operational work; operational author independently reviewed auth, provider/cache/migration, and analytics. Independent final QA: **PASS** for HRR-01–11 after reviewing final source, all execution evidence, documentation links, artifact hygiene and both builds’ formatting. No unresolved defects.

Evidence logs remain ignored under `.local/logs/`: `hrr-root-unit.log`, `hrr-root-integration.log`, `hrr-root-migration.log`, `hrr-account-recovery.log`, `hrr-analytics-full-unit.log`, `hrr-analytics-mongo-integration.log`, `hrr-analytics-final.log`, and `hrr-analytics-build-format.log`. Performance evidence establishes bounded materialization/deletions, not latency improvement. No live provider, Atlas, production activation, or deployed acceptance claim.

## Checkpoint

All eleven refactors are implemented. All required local runtime and formatting checks have passing evidence, including final Delta regressions and account-deletion recovery. Independent Code Reviewer, Security Engineer and final QA verdicts are PASS. All eleven acceptance criteria are closed locally; skipped opt-in gates remain unverified. Existing worktree was clean at task start. This commit closes the approved eleven-point plan. No implementation criteria remain open. Opt-in Atlas, Compose report and scaling checks remain separate verification work; no push, deployment, developer database reset or lakehouse reset was performed.
