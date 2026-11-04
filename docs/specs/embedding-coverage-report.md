# Embedding Coverage Report

## Identity and scope

- Capability: Admin-only, read-only report of how well the searchable Jobs and Candidates are covered by current embeddings, and whether the repair queue can explain every gap.
- Roadmap context: supports search evaluation and embedding architecture acceptance (SEB-*) and vector optimization prerequisites (MVR-*). It produces local evidence only; it makes no Atlas performance claim.
- Status: done (local). Implemented and locally verified 2026-10-08 (unit, service, GraphQL access/contract and Mongo integration evidence below). Review verdicts (see Checkpoint and verdicts): Code Reviewer FAIL 2026-11-03 (documentation and lease boundary), re-review PASS 2026-11-03 after fixes; Security Engineer PASS (static) 2026-11-03; QA PASS 2026-11-03. MVP condition 4 met. No Atlas or deployed evidence.

## Source facts (verified 2026-10-08; reviewers re-read HEAD 2026-11-03)

- Embedding metadata is `embeddingMeta {model, sourceHash, updatedAt}`; the vector is stored separately as `embedding`. An embedding is current when its model matches and `sourceHash == SourceHash.sha256(SearchableText.job|candidate(current))` (`SearchEligibility.scala`).
- Production eligibility: Jobs require `status == Open`. Candidates require role Candidate, accountStatus Active and a profile. `recruiterSearchOptIn` is **not** an eligibility requirement; it only matters when a country, city or availability filter is supplied. The report population therefore follows production and does not filter on opt-in.
- Queue: collection `embedding_work`, one row per `Job:<id>` / `CandidateProfile:<id>`. States `Ready`, `Retry`, `Processing`, `Failed`; completed rows are deleted. A `Processing` row whose `leaseUntil` is past is claimable again. Failures: `RetryExhausted`, `DocumentTooLarge`, `InvalidWorkKey`, `InvalidResponse`.
- Lease rule: a lease is held while `leaseUntil > now` and is reclaimable once `leaseUntil <= now`. The claim decides this on the database clock (`$$NOW`, commit a3645e0, `MongoRepositorySupport`). The report's `now` is the application clock, so near the boundary clock skew between the application and MongoDB can label a row `InProgress` that the database has already reclaimed, or `LeaseExpired` that it still holds. Accepted: both values are queued work, so neither `NoOrphanedGap` nor the cross-tab total changes.
- Lease rule in source (2026-11-03): `service/search/EmbeddingCoverage.scala` classifies a `Processing` row as `InProgress` only when `leaseUntil.exists(_.isAfter(now))`; `leaseUntil == now` is `LeaseExpired`. Boundary test: `EmbeddingCoverageClassificationSpec` "ECR-01 lease boundary mirrors the claim predicate" (`sbt test` 1824 passed, 0 failed).
- Commit 7ac0bf1 added an in-claim held vector (`EmbeddingPipeline`, `HeldVector`, keyed by model and source hash). It is reused only after a transient entity-write failure or revision conflict whose source hash is unchanged, and it is still written before `complete`, so it cannot make a stale hash look Current.
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
- `InProgress`: row in `Processing` with `leaseUntil > now`.
- `LeaseExpired`: row in `Processing` with `leaseUntil <= now` (claimable again). The boundary is the claim predicate; see the lease rule under Source facts.
- `Failed`: row in `Failed`, with a separate failure reason.

## Contract

Query `embeddingCoverage(expectedModel: String): EmbeddingCoverageReport!`, Admin only. `expectedModel` is optional; no code path assumes a particular model.

The report contains:
- `cells`: one count per (entity kind `JOB` | `CANDIDATE_PROFILE`, freshness, repair state, failure reason when `Failed`).
- `observedModels`: count of embeddings per distinct model, per entity kind. Without `expectedModel` no model is treated as the expected one; see the model blind spot under Remaining risks.
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

The no-false-gap argument rests on two invariants outside the report code; breaking either reintroduces false `NoOrphanedGap` failures: (1) enqueueing repair work is transactional with the entity write that makes the embedding stale (`MongoJobRepository` create and update, `MongoUserRepository` insert and profile update, `MongoEmbeddingWorkRepository.requiresTransaction`); (2) the worker persists the vector before `complete` deletes the row (`EmbeddingPipeline` persists through `jobs.updateEmbedding` / `users.updateEmbedding`, then `work.complete`). Symbol names are used instead of line numbers, which drift. The same notes are in the class comment of `MongoEmbeddingCoverageRepository`. The queue-wide aggregation for `NoStuckWork` is a separate, non-snapshot read and is independent of the cross-tab.

## Implementation and evidence

Production code: `service/search/EmbeddingCoverage.scala` (pure classification, tally, report assembly), `service/EmbeddingCoverageService.scala` (Admin check, validation, clock), `service/port/EmbeddingCoverageRepository.scala` (read-only port), `repository/mongo/MongoEmbeddingCoverageRepository.scala` (primary-read, projected, `_id`-keyset scan with snapshot-read queue lookup, and queue aggregation), the `embeddingCoverage` resolver in `api/graphql/HiringGraphQLReportingResolvers.scala` with its types in `api/graphql/HiringGraphQLReportingTypes.scala`, plus schema, input, type and request-context wiring (expensive-root budget and fixed complexity) and the runtime composition (enabled: live service with `3 x durableRetryCapMillis`; disabled: Admin-checked unavailable service). `EmbeddingWorkState` moved to the port next to `EmbeddingWorkFailure` so the service layer does not depend on the Mongo package.

| ID | Evidence (2026-10-08) |
|---|---|
| ECR-01 | `EmbeddingCoverageClassificationSpec` freshness and repair state tests: every value, absent expected model, lease boundary (`leaseUntil <= now` is `LeaseExpired`, matching the claim predicate). Boundary test `ECR-01 lease boundary mirrors the claim predicate`; classification spec 9/9 and `sbt test` 1824/1824 passed 2026-11-03. |
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

Remaining risks (decisions recorded 2026-11-03; none changes the code in this revision):
- Model blind spot (decision recorded, not changed). With no `expectedModel`, any stored model counts as `Current`, while semantic search retrieves only embeddings whose `meta.model` equals the configured model (`MongoSemanticSearchRepository`). After a model switch the default report can show 100% `Current` while search excludes the old embeddings. Operators must pass `expectedModel` set to the configured model. The code does not enforce this. The API reference and the wiki security and operations page state the same advice.
- Snapshot dependency: the entity page scan depends on snapshot read concern (not available on a standalone server) and on the server snapshot window. A deployment without it fails the query with the repository-unavailable error.
- Malformed stored data: one malformed document fails the whole report with `RepositoryError.InvalidStoredData`. This fails closed; no partial report is returned.
- Scan bounds: only the per-command `maxTime` bounds the scan (10 s per Mongo command, not a total deadline), together with the per-kind cap. A full local scan of 36,000 searchable entities took 3.8 to 10.8 s, while the HTTP `request-timeout-ms` default is 5000. At that scale the query can time out and fails closed. The sizing decision (cap, page size or timeout for a cloud deployment) is open and is not decided here. The cap is a constant, not configuration.
- Client-side work dominates the scan: decoding and hashing, not server time (see ECR-10). Raise the cap only after measuring this.
- Candidate name projection (Security L2, accepted, see below): the candidate scan projects `name`. It is decoded in process and never returned, stored in the tally, or logged.
- Database access (deployment gap): the repository uses the shared application MongoDB connection and there is no dedicated read-only database role. Least privilege is enforced by code (projections, a read-only port), not by database grants.
- `NoStuckWork` counts a long-held live claim whose `availableAt` is old.
- Candidate lag and age are unavailable until users carry a change timestamp; `lagEntityKinds` is `[JOB]`.
- No recruiter-consent dimension in the first slice.
- No Atlas or deployed evidence. The ECR-10 measurement (2026-10-08) predates float32 binary vectors, migration `032_embedding_work_key_from_identity` and database-clock leases. It has not been re-measured since.

Accepted limitation (Security L2): the candidate scan reuses `MongoSearchEligibilityCodecs.candidateFields`, which projects `name` because that decoder validates it for active candidates. `SearchableText.candidate` needs only skills and the experience summary; a coverage-specific decoder was judged sizeable and was not added. The name is decoded in process and never stored in the tally, returned, or logged.

## Checkpoint and verdicts

- 2026-10-08: implemented and locally verified (ECR-01 to ECR-10, evidence above). Scope of that evidence: local disposable MongoDB 8.0 replica set; no Atlas or deployed run.
- 2026-11-03 Code Reviewer: FAIL (docs-driven, plus the lease boundary). Findings: stale resolver path and drifted line references; lease wording in ECR-01; glossary terms missing; model blind spot not documented; remaining risks incomplete. Documentation findings are fixed in this revision. The lease boundary was then fixed in source (see Source facts). Re-review 2026-11-03: PASS (code, docs and boundary test checked against the working tree).
- 2026-11-03 Security Engineer: PASS (static). Low findings, recorded above and accepted: the candidate `name` projection (Security L2), and the shared application database connection with no read-only role (deployment gap). No code change was required.
- 2026-11-03 QA: PASS on the final working tree (HEAD `b4e216b` plus the fixes). Each ECR criterion was mapped to a test that asserts it. Focused units `*EmbeddingCoverage* *HiringGraphQLAccessSpec *HiringGraphQLContractSpec` 119/119 passed. `scripts/run-local-tests.sh test '*MongoEmbeddingCoverageIntegrationSpec'` returned `RESULT: PASS tests_total=1830 passed=1829 failed=0 skipped=1`; the skipped case is the opt-in ECR-10 measurement (not run, not passed). Follow-ups applied after QA: the `HiringGraphQLAccessSpec` coverage cases now carry their ECR-05/07/08 tags (D2). Accepted (D3): deleted-Admin denial is tested at the service boundary only, not through GraphQL.
- Open, not gating: ECR-10 re-measurement after float32 vectors, migration 032 and database-clock leases; the request-timeout sizing decision.

## Documentation impact

Updated with this revision (2026-11-03):
- `docs/domain-language.md`: `EmbeddingFreshness`, `EmbeddingRepairState`, orphaned gap and stuck work, with the statement that freshness and repair state are separate dimensions.
- `docs/api.md` (embedding coverage section): `expectedModel` operator advice next to `observedModels`, because the model blind spot changes how the default report reads after a model switch.
- `docs/wiki/security-and-operations.md`: operator entry for the report (Admin-only, read-only, counts only, bounded scan, `expectedModel` advice, fail-closed behavior).
- This specification: status, source facts, lease rule and its boundary test, symbol references, remaining risks, verdicts.

Checked and not changed: `README.md` and `ARCHITECTURE.md` do not name the query; `docs/schema-evolution.md` (the Embedding coverage paragraph) is consistent with the snapshot and no-index facts; `docs/wiki/workflows-and-patterns.md` is not changed because the report is an operations surface, not a reusable pattern here. `docs/development-milestones.md` is excluded by the coordinator, who updates the MVP table.

## Review verdicts

Collected from the 2026-11-03 checkpoint entries so the verdict gate (`scripts/check-spec-verdicts.py`) can read them. No Atlas or deployed evidence.

- Code Reviewer verdict (2026-11-03, re-review after the documentation and lease-boundary fixes): PASS; the first review on the same day was a FAIL.
- Security Engineer verdict (2026-11-03, static): PASS; the low findings are recorded above and accepted.
- Final independent QA verdict (2026-11-03, final working tree at HEAD `b4e216b` plus the fixes): PASS.
