# Embedding Coverage Report

## Identity and scope

- Capability: Admin-only, read-only report of how well the searchable Jobs and Candidates are covered by current embeddings, and whether the repair queue can explain every gap.
- Roadmap context: supports search evaluation and embedding architecture acceptance (SEB-*) and vector optimization prerequisites (MVR-*). It produces local evidence only; it makes no Atlas performance claim.
- Status: implemented and locally verified 2026-10-08 (unit, service, GraphQL access/contract and Mongo integration evidence below); independent Code Reviewer, Security Engineer and QA verdicts are pending. No Atlas or deployed evidence.

## Source facts (verified 2026-10-08)

- Embedding metadata is `embeddingMeta {model, sourceHash, updatedAt}`; the vector is stored separately as `embedding`. An embedding is current when its model matches and `sourceHash == SourceHash.sha256(SearchableText.job|candidate(current))` (`SearchEligibility.scala`).
- Production eligibility: Jobs require `status == Open`. Candidates require role Candidate, accountStatus Active and a profile. `recruiterSearchOptIn` is **not** an eligibility requirement; it only matters when a country, city or availability filter is supplied. The report population therefore follows production and does not filter on opt-in.
- Queue: collection `embedding_work`, one row per `Job:<id>` / `CandidateProfile:<id>`. States `Ready`, `Retry`, `Processing`, `Failed`; completed rows are deleted. A `Processing` row whose `leaseUntil` is past is claimable again. Failures: `RetryExhausted`, `DocumentTooLarge`, `InvalidWorkKey`, `InvalidResponse`.
- When vector search is disabled there is no work repository, no configured model and no work rows.
- Existing reusable pieces: projection-based eligibility decoding (`MongoSearchEligibilityCodecs`), `SourceHash`, `SearchableText`, the `analyticsReport` Admin query pattern, `EmbeddingWorkInspection`.

## Vocabulary

Freshness and repair state are separate dimensions. Their names never overlap, and a value from one dimension never describes the other.

`EmbeddingFreshness` (does the stored embedding match the entity now?):
- `Current`: embedding present, content hash matches, and the model matches when an expected model was given.
- `ContentChanged`: embedding present, content hash differs from the entity's current text.
- `ModelMismatch`: embedding present, content hash matches, model differs from the expected model. Only produced when an expected model is supplied.
- `NotEmbedded`: no embedding metadata.

`EmbeddingRepairState` (is a repair scheduled for the entity?):
- `NoQueuedWork`: no `embedding_work` row.
- `Waiting`: row in `Ready`.
- `Retrying`: row in `Retry`.
- `InProgress`: row in `Processing` with a live lease.
- `LeaseExpired`: row in `Processing` whose lease has passed (claimable again).
- `Failed`: row in `Failed`, with a separate failure reason.

## Contract

Query `embeddingCoverage(expectedModel: String): EmbeddingCoverageReport!`, Admin only. `expectedModel` is optional; no code path assumes a particular model.

The report contains:
- `cells`: one count per (entity kind `JOB` | `CANDIDATE_PROFILE`, freshness, repair state, failure reason when `Failed`).
- `observedModels`: count of embeddings per distinct model, per entity kind.
- `searchableCount` per kind, `scannedCount` per kind, `truncated` per kind.
- `oldestQueuedWorkAgeSeconds` and `oldestNotCurrentAgeSeconds` (nullable when none).
- `lagSeconds` p50/p95/p99 from entity update to embedding update for `Current` entities, when the entity has an update timestamp (see unknowns). Informational.
- `checks`: the named hard checks below, each `PASSED`, `FAILED` or `INCONCLUSIVE` with an offending count (field `check`, not `name`, so no report field is called `name`).

The report never returns entity ids, names, text, vectors or provider payloads.

Definitions: `scannedCount` is the number of entities classified and counted in the cross-tab (the sum of that kind's cells), not a raw cursor counter; `searchableCount` is an exact server-side count of eligible entities and is never smaller than `scannedCount`. They can differ when the scan is truncated, and also when eligibility changes between the count and the scan.

Implemented additions to the contract: `queueTruncated` (the queue aggregation hit its cap, which makes only `NoStuckWork` inconclusive), `lagEntityKinds` (the kinds that contribute to lag and oldest age; always `[JOB]`, see unknowns), `expectedModel` echo, and per-kind `coverageShare` (`Current` / searchable, null when nothing is searchable). Enum names are `EmbeddingCoverageEntityKind`, `EmbeddingFreshness`, `EmbeddingRepairState`, `EmbeddingFailureReason`, `EmbeddingCoverageCheckName` and `EmbeddingCoverageCheckStatus`. `cells` lists only non-zero cells in enum order. The oldest queued work age is the wait since `availableAt` of the oldest non-failed row, queue-wide, clamped at zero. When a check's scan is truncated the status is `INCONCLUSIVE` even if offenders were already found; the offending count is then a lower bound.
`expectedModel` is trimmed before validation (so `" model-a"` equals `model-a`) and the 128-character limit applies to the trimmed value. The request-context default capability denies every caller as `UNAUTHORIZED`; only a wired service can answer, and it checks the Admin first. `embeddingCoverage` is an expensive root with fixed complexity 600, so a request containing more than one coverage root exceeds the complexity budget of 1000 and is rejected before any resolver or repository call (`HiringGraphQLAccessSpec`, 500 aliases and two aliases, counting repository, zero calls).
Disabled vector search reuses the existing typed `SearchError.VectorSearchUnavailable` (`VECTOR_SEARCH_UNAVAILABLE`); the Admin check runs first so non-Admins still receive `UNAUTHORIZED`.

Hard checks:
- `NoOrphanedGap`: no searchable entity that is not `Current` has `NoQueuedWork`. Such an entity will never be repaired.
- `NoStuckWork`: no queued work (`Waiting`, `Retrying`, `InProgress`, `LeaseExpired`) has waited longer than `3 x durableRetryCapMillis` past its `availableAt`.

Informational until a baseline is measured: coverage share (`Current` / searchable), lag percentiles. An attempts distribution is deferred and not exposed. Promoting any of them to a threshold requires a recorded baseline and a separate spec change.

When vector search is disabled the query fails with a typed unavailable error; it does not return zero counts that would look healthy.

## Bounds and authorization

- Admin check is performed in the service against the live user record (role Admin, status Active), as in `AnalyticsReportingService`. The resolver performs no role logic. This is the RBAC boundary for cloud deployments; no tenant scoping exists or is inferred.
- Reads use primary reads, a projection that excludes the vector, an `_id`-ordered keyset scan, a bounded page size, a maximum scanned count per kind, and `maxTime`. Hitting the cap sets `truncated` and the report states that its counts are partial; checks are then reported as `INCONCLUSIVE`, never `PASSED`.
- The queue is read with a grouped aggregation bounded by the same cap and `maxTime`.
- No new index is added. The scan uses `_id` ordering with the existing role/status and job status indexes; a new index requires measured query-plan evidence.

## Non-goals

- Not an operational alert or Phase 12 metric; the classification is a pure function that can be reused there later.
- No repair actions, no enqueue, no provider calls.
- No recruiter-consent dimension in the first slice.
- No dependence on a specific embedding model or provider.

## Acceptance criteria

| ID | Criterion | Evidence |
|---|---|---|
| ECR-01 | Pure classification maps (stored metadata, current text hash, expected model) to exactly one freshness value, and (work row, now) to exactly one repair state. | Unit spec with every value, including `LeaseExpired` and absent expected model. |
| ECR-02 | The cross-tab counts every searchable entity exactly once; freshness and repair state are never merged. | Unit and Mongo integration spec on a seeded database covering all cells. |
| ECR-03 | `NoOrphanedGap` fails when a non-current entity has no queued work and passes otherwise. | Unit and integration specs. |
| ECR-04 | `NoStuckWork` fails when queued work is older than the configured bound and passes otherwise. | Unit spec with a fixed clock. |
| ECR-05 | Only an Active Admin can run the query; Candidates, Recruiters and deleted users (AccountStatus has only Active and Deleted) are rejected by the service. | Service spec and `HiringGraphQLAccessSpec` cases. |
| ECR-06 | Scan is bounded; a capped scan sets `truncated` and checks become `INCONCLUSIVE`. | Integration spec with a small cap. |
| ECR-07 | The report exposes no ids, names, text, vectors or payloads. | Contract assertion on the response shape. |
| ECR-08 | Disabled vector search yields the typed unavailable error. | Service/runtime spec. |
| ECR-09 | SDL snapshot, validated operation fixture, `docs/api.md` and `docs/schema-evolution.md` change together. | Contract spec passes. |
| ECR-10 | Query plan evidence for the entity scans and queue aggregation is recorded for a seeded local dataset with its size. | Recorded explain output, dataset size and counts in this spec. |

## Unknowns (resolved 2026-10-08)

- Candidate lag: `StoredUser` has no `updatedAt`; the field is written only by a profile edit or deletion (`MongoUserRepository`). A candidate lag would therefore cover only edited profiles and be selection-biased. Lag and oldest-not-current age are reported for Jobs only (`Job.updatedAt`), and the report states this through `lagEntityKinds`.
- Scan cap: `EmbeddingCoverageLimits.default` is a named constant of 50,000 entities per kind, page size 500, `maxTime` 10 s per Mongo command (not a total deadline). It is not yet configuration; promoting it to configuration needs a measured need. Measured cost is under ECR-10.
- Ordering race: a queue read before the entity read creates false gaps (an entity changed and enqueued after the queue read), and an entity read before the queue read creates false gaps when the worker finishes in between. Decision: every entity page is one aggregation under `readConcern: snapshot` that reads the entities and looks up their queue rows by `_id` (`$lookup`) at a single point in time, so the combination cannot occur. This was verified to work with `$lookup` on the local MongoDB 8.0 replica set, so the earlier suspect re-read was deleted; only one mechanism exists. Snapshot reads need a replica set or sharded cluster and are subject to the server snapshot history window (`minSnapshotHistoryWindowInSeconds`, 300 s on the local MongoDB 8.0 stack, read with `db.adminCommand({getParameter:1, minSnapshotHistoryWindowInSeconds:1})`), which bounds the time of one page (500 entities, tens of milliseconds), not of the whole scan. A deployment without snapshot support (a standalone server) fails the page read, so the query fails with the repository-unavailable error instead of returning an unguarded report.

The no-false-gap argument rests on two invariants outside the report code; breaking either reintroduces false `NoOrphanedGap` failures: (1) enqueueing repair work is transactional with the entity write that makes the embedding stale (`MongoJobRepository` create/update ~343/390, `MongoUserRepository` insert/profile update ~269/388, `MongoEmbeddingWorkRepository.requiresTransaction`); (2) the worker writes the embedding before `complete` deletes the row (`EmbeddingPipeline` `updateEmbedding` ~247/264, `work.complete` ~150-151). The same notes are in the class comment of `MongoEmbeddingCoverageRepository`. The queue-wide aggregation for `NoStuckWork` is a separate, non-snapshot read and is independent of the cross-tab.

## Implementation and evidence

Production code: `service/search/EmbeddingCoverage.scala` (pure classification, tally, report assembly), `service/EmbeddingCoverageService.scala` (Admin check, validation, clock), `service/port/EmbeddingCoverageRepository.scala` (read-only port), `repository/mongo/MongoEmbeddingCoverageRepository.scala` (primary-read, projected, `_id`-keyset scan with snapshot-read queue lookup, and queue aggregation), `api/graphql/HiringGraphQLEmbeddingCoverageResolvers.scala`, plus schema, input, type and request-context wiring (expensive-root budget and fixed complexity) and the runtime composition (enabled: live service with `3 x durableRetryCapMillis`; disabled: Admin-checked unavailable service). `EmbeddingWorkState` moved to the port next to `EmbeddingWorkFailure` so the service layer does not depend on the Mongo package.

| ID | Evidence (2026-10-08) |
|---|---|
| ECR-01 | `EmbeddingCoverageClassificationSpec` freshness and repair state tests: every value, absent expected model, lease boundary (strictly past is `LeaseExpired`). |
| ECR-02 | `EmbeddingCoverageClassificationSpec` cross-tab test; `MongoEmbeddingCoverageIntegrationSpec` "cross-tab" test (reads run under snapshot read concern) on a seeded database covering Current/ContentChanged/ModelMismatch/NotEmbedded against NoQueuedWork/Waiting/Retrying/InProgress/LeaseExpired/Failed(both failure reasons) for jobs and candidates, a closed job and a Recruiter excluded, with and without `expectedModel`. |
| ECR-03 | Classification spec and the integration test (one orphan job gives `FAILED` with offending count 1); the paging test (7 orphans over 4 pages of 2). |
| ECR-04 | Classification spec (stuck count, oldest age) and `EmbeddingCoverageServiceSpec` (fixed clock: `stuckBefore` is now minus three retry caps); integration test seeds an old ready row for a closed job and gets `NO_STUCK_WORK` `FAILED` 1 with offending count 2 (a stray row 2 h old and a row 1 ms older than `stuckBefore`); a row exactly at `stuckBefore` (strict comparison), recent waiting rows and failed rows 3 h old are not counted, and the oldest queued work age stays 7200 s (failed rows excluded). A Processing row (live or expired lease) IS counted as stuck work when its `availableAt` is older than the bound: the integration test seeds a Processing row 2 h old (counted, offending total 3) next to recent processing rows (not counted). |
| ECR-05 | `EmbeddingCoverageServiceSpec` (Recruiter, Candidate, deleted Admin, role-claim mismatch, unknown actor rejected with no scan issued) and `HiringGraphQLAccessSpec` (anonymous, Candidate, Recruiter denied; Admin allowed). |
| ECR-06 | Integration test with cap 3 over 5 jobs: searchable 5, scanned 3, `truncated`, queue truncated, both checks `INCONCLUSIVE`; the exact cap passes. Classification spec covers inconclusive with and without offenders. |
| ECR-07 | `MongoEmbeddingCoverageProjectionSpec` asserts the entity projections and page pipeline exclude `embedding`; `HiringGraphQLContractSpec` asserts no `EmbeddingCoverage*` or `EmbeddingObservedModel` field is named id, name, text, vector, embedding, payload or title; the repository projection excludes `embedding`. |
| ECR-08 | `EmbeddingCoverageServiceSpec` (Admin gets `SearchError.VectorSearchUnavailable`, Recruiter still `Unauthorized`) and `HiringGraphQLAccessSpec` (`VECTOR_SEARCH_UNAVAILABLE`). |
| ECR-09 | `hiring.graphql` snapshot, `embedding-coverage.graphql` operation validated against the schema in `HiringGraphQLContractSpec`, `docs/api.md`, `docs/schema-evolution.md`. |
| ECR-10 | Below. |

### ECR-10 query plans (local, disposable database)

The measurement is opt-in like the other scaling specs. Command (the property must reach the forked test JVM): `JAVA_TOOL_OPTIONS=-Dhiring.scaling.measure=true scripts/run-local-tests.sh test '*EmbeddingCoverage*'`. Without it only the ECR-10 case is skipped; the three functional Mongo cases (cross-tab, paging, truncation) always run.

Environment: owned local test stack (`scripts/run-local-tests.sh test '*EmbeddingCoverage*'`), MongoDB 8.0 replica set, standard indexes from `MongoHiringSetup`, no new index. Dataset: 20,000 jobs (18,000 Open), 20,000 users (18,000 Active Candidates), 2,000 `embedding_work` rows (Ready); embeddings mixed Current/ContentChanged/other model/none. Full scan with the default limits: 18,000 jobs and 18,000 candidates scanned, none truncated, 72 pages, wall time 3.8 s to 10.8 s over four runs (the last, with snapshot reads, 3.8 s) (single client thread; snapshot reads; these are single-run observations, not fixed ranges: a later run saw 55 to 87 ms per page and 547 to 586 keys examined; server time per 500-document page was about 33 to 43 ms in the recorded run, so most of the time is client decode and hashing, which should be remembered before raising the cap). This is a local functional measurement, not a performance claim; no concurrency or percentiles were taken. Raw explain JSON was written to the ignored `.local/logs/embedding-coverage/`.

| Query | Winning plan | Counts (`executionStats`) |
|---|---|---|
| Jobs page (`status = Open`, `_id` keyset, limit 500, projected, queue lookup) | LIMIT > PROJECTION_SIMPLE > FETCH(status filter) > IXSCAN `_id_` | returned 500, keys 545 to 566, docs 545 to 566, 33 to 43 ms |
| Candidates page (role, status, `profile` type, `_id` keyset, limit 500) | LIMIT > PROJECTION_DEFAULT > FETCH(filter) > IXSCAN `_id_` | returned 500, keys 549 to 559, docs 549 to 559, 37 to 40 ms |
| Jobs searchable count | GROUP > COUNT_SCAN `jobs_open_created_id` | keys 18,000, docs 0, 5 to 14 ms |
| Candidates searchable count | GROUP > FETCH(`profile` type) > IXSCAN `users_role_accountStatus_created_id` | keys 18,000, docs 18,000, 26 to 35 ms |
| Queue aggregation per kind (`_id` prefix range, `$limit` cap+1, `$group`) | GROUP > LIMIT 50,001 > PROJECTION > FETCH > IXSCAN `_id_` | keys 1,000, docs 1,000 (the 1,000 seeded rows of that kind), 1 to 2 ms |
| Queue lookup inside the jobs page (profiler on a mid page) | IXSCAN `_id_` for the outer scan | keys 573 and docs 573 for 500 returned, about the 545 to 566 of the outer scan plus the rows that matched; a collection-scan lookup would have examined on the order of a million documents |

The `$lookup` sub-pipeline is a `$match` of `$expr: {$eq: ["$_id", "$$key"]}` on the queue `_id`; explain does not itemize the lookup stage on this version, so index use is shown by the profiler counters above. The scan walks the `_id` index with a status/role filter (about 10% overshoot from closed jobs and recruiters), which is why the existing `_id` and role/status indexes suffice and no index was added. A new index needs its own measured plan.

Open sizing decision for the product owner: a full 36,000-entity scan took 3.8 to 10.8 s locally, while the HTTP `request-timeout-ms` is 5000, so at that scale the query can time out (it fails closed, with no partial report). The committed 5 s default was deliberately not changed. Decision (2026-10-08): local runs raise `http.request-timeout-ms` to 30000 in the ignored `src/main/resources/local.conf`; sizing for a cloud deployment (cap, page size or timeout against the real dataset) remains open and must be chosen with a measured dataset and the Admin-only RBAC bound in mind.

Additional evidence: `MongoEmbeddingCoverageIntegrationSpec` asserts via the profiler that the entity pages (the `$lookup` aggregations) carry read concern snapshot and the count and queue aggregations do not, and that a server error inside the scan yields `Left(RepositoryError.Unavailable)`.

Remaining risks: the entity page scan depends on snapshot read concern (not available on a standalone server) and on the server snapshot window; the cap is a constant, not configuration; client-side decoding dominates the 36,000-entity scan; `NoStuckWork` counts a long-held live claim whose `availableAt` is old; candidate lag and age are unavailable until users carry a change timestamp; no recruiter-consent dimension and no Atlas or deployed evidence.

Accepted limitation (Security L2): the candidate scan reuses `MongoSearchEligibilityCodecs.candidateFields`, which projects `name` because that decoder validates it for active candidates. `SearchableText.candidate` needs only skills and the experience summary; a coverage-specific decoder was judged sizeable and was not added. The name is decoded in process and never stored in the tally, returned, or logged.
