# Hiring MongoDB schema consolidation

## Identity and scope

- Task / use case / roadmap phase: MongoDB/vector optimization (Phase 8) follow-up. Operational schema review against the [MongoDB modeling standard](../mongodb-design.md#modeling-standard). Touches UC04, UC08, UC09, UC11 and account deletion.
- Status: in progress
- Coordinator: Product Manager.
- Owners by area:
  - Data Engineer: migrations, validators, indexes.
  - Scala Developer: repositories, codecs, tests.
  - Big Data Engineer: analytics-owned collections.
  - Technical Writer: documentation.
- User outcome: the stored model follows one written standard. Each field has one meaning. Every query the application issues is backed by an index, or its scan is bounded and the bound is documented. Every collection has a validator. Absent values are omitted. Each fact is stored once. Future changes are checked against the same standard by the data-engineer role.
- Authorized scope:
  - Fix every finding below with the smallest code change that meets the standard.
  - Record every accepted deviation with its bound and revisit trigger.
  - Update the schema documents and the data-engineer skill.
  - The Atlas-gated slices (MSC-08, MSC-09) are in scope only together with Atlas Local evidence. Without that evidence they stay BLOCKED and inactive.
- Non-goals:
  - Binary UUID `_id`s.
  - A per-document `schemaVersion` field.
  - Splitting the polymorphic interview collections.
  - Renaming existing lease fields.
  - TTL on Failed outbox or embedding-work rows.
  - Changes to public GraphQL names or the seven-field event envelope.
  - Cloud Atlas (`.C`) gates.
  - Moving index ownership between builds (decision D3 stays with the user).
- Dependencies:
  - MSC-08 and MSC-09 need the Atlas Local container (`compose.atlas-local.yaml`) and the MVR `.L` procedure in [vector retrieval optimization](mongodb-vector-retrieval-optimization.md).
  - Integration criteria need Docker through `scripts/run-local-tests.sh`.

## Source context and decisions

### Verified implementation

- Names: `MongoNames.scala`.
- Codecs: `MongoHiringPersistenceCodecs.scala`, `MongoHiringCodecs.scala`.
- Indexes: `MongoHiringIndexSetup.scala` (verify, create, verify again; never drops).
- Validators: `MongoHiringValidators.scala` (strict/error via `collMod`).
- Ledger migrations 001–020: `MongoMigrationLedger.scala:19-40`, run order at `MongoHiringMigrations.scala:80-100`.
- All paths are under `src/main/scala/com/example/graphQL/cats/repository/mongo/`.

### Review sources

- *MongoDB in Action, 3rd ed.*: ch5 schema design and patterns, ch7 indexing and the ESR rule, ch8 transactions.
- *Mastering MongoDB 4.x*: ch2 modeling, ch7 indexes.
- *Practical MongoDB Aggregations*: ch3 pipeline performance, ch7 joins.
- MongoDB manual, data-modeling pages: best practices, design patterns, anti-patterns, versioning, schema validation.

### Findings

| ID | Finding (verified in source) | Standard rule |
|---|---|---|
| F-A | `jobs.version` is the content revision checked by job edits (`MongoJobRepository.scala:316-322`) and closures (`MongoRecruiterJobClosure.scala:90-96`). It is also `$inc`'d by every application submission (`MongoApplicationRepository.scala:205-218`) and by interview `touchJobOwner`. `users.version` is `$inc`'d by interview `fenceSubjects` (`MongoInterviewWorkflowStore.scala:72-96`), by the commit participant loop (`MongoInterviewWorkflowRepository.scala:158-168`) and by admin authorization (`MongoEmbeddingWorkRepository.scala:77-93`). Embedding writes also `$inc` version (`MongoJobRepository.scala:340-357`, `MongoUserRepository.scala:146-166`). Job edits and closures replace the whole document, including the vector. | One field, one meaning; targeted operators |
| F-F | The interview erasure purge `subjectFilter` (`MongoInterviewSubjectCleanup.scala:279-291`) ORs six fields across five collections, and most branches have no index. The analytics delta-file read by `requestId` (`analytics/src/main/scala/com/example/hiring/analytics/adapter/mongo/MongoAnalyticsErasureProgress.scala:102-120`) has no index, and rows never expire. | Every query predicate is indexed or bounded |
| F-G | `consumer_receipts` keeps a unique `(consumerGroup, eventId)` index while `_id` is already `consumerGroup:eventId` (`MongoOperationalEventRepositories.scala:683-685`). | No redundant index |
| F-B | Nulls are written for absent values:<br>• `StoredOutboxRecord` and `StoredSearchSession` use `createCodec` (`MongoHiringPersistenceCodecs.scala:170,172`)<br>• `auditActorId` uses `.orNull` (`MongoInterviewWorkflowRepository.scala:434`, `MongoInterviewLifecycleStore.scala:431`)<br>`users.updatedAt` is absent until the first update. | Omit when absent; required timestamps always present |
| F-K | Collections without a validator: `applications`, `application_events`, `embedding_work`, `search_sessions`, `search_session_work`, `mutation_receipts`, `consumer_receipts`, `interview_workflows`, `interview_workflow_inbox`, `interview_calendar_reservations`, `interview_notification_receipts`. The users validator does not type-check name, email or dates. The inbox hiring receipt (`MongoInterviewWorkflowRepository.scala:193-197`) has no `documentType`. | Validator per collection; discriminator per polymorphic document |
| F-D | `event_outbox` stores the event three times: top-level fields, the `payload` JSON string and `envelopeBytes`. `search_session_work.event` repeats `payload`. | Store each fact once |
| F-I | Job `skills` and `location.city` filters compare case-sensitively (`MongoJobRepository.scala:377,509-513`). Candidate filters use canonical sidecars. | Consistent normalization |
| F-V | Embeddings are stored as float64 arrays (`MongoHiringCodecs.scala:519`), but the domain type is `Float`. | Smallest faithful type |
| F-L | The analytics build keeps its own copy of shared collection, field and index names (`analytics/.../AnalyticsCollections.scala:4-67`). | One schema contract |
| F-Doc | Documentation drift:<br>• migrations 003–005 and 020 are missing from `schema-evolution.md`<br>• the wiki inventory lacks `recruiter_job_closure_work`<br>• `ARCHITECTURE.md` uses `actorRole: "RECRUITER"`, but the stored value is `Recruiter`<br>• index lists and shapes are incomplete<br>• the UC04 write list omits the job guard, subject fence and outbox writes<br>• `mongodb-design.md` still calls Phase 8 "planned" | Docs match source |

### Decisions

| ID | Decision | Basis |
|---|---|---|
| D-01 | **Each version or counter field has one meaning.** `version` is the content revision of a user or job: it advances only when content changes. Write-conflict fences are separate fields or documents: `jobs.admissionFence` and `outbox_subject_fences.fencingVersion`. Embedding writes check `version` but never advance it. | F-A; MiA ch8 atomic operators; manual "atomicity" |
| D-02 | **Keep UUID strings for IDs.** Do not switch to BinData subtype 4. | The strings appear inside composite `_id`s, validator patterns, Atlas token filters, Kafka and Delta. The saving is about 20 bytes per ID and fixes no defect. |
| D-03 | **No per-document `schemaVersion`.** One active shape is kept by ledgered migrations plus strict validators, and startup fails closed on validator drift. Revisit after MVP. | The pre-MVP policy forbids the lazy dual-read migration that the Schema Versioning pattern exists for. |
| D-04 | **Polymorphic collections stay** (`interview_workflows`, `interview_workflow_inbox`). Every document carries `documentType`, and the validator has one `oneOf` branch per type. | Polymorphic pattern (MiA p108). Co-located uniqueness, TTL and purge. |
| D-05 | **Number widths.** Counters and revisions are Int64. Constant markers and small attempt counts are Int32. `analytics_erasure_requests.fencingVersion` is a constant marker of 1, so it stays Int32. | One BSON type per field |
| D-06 | **Unindexed reads accepted with a bound.** Each has a documented bound and revisit trigger instead of an index:<br>• the `recruiter_job_closure_work` claim/peek ([measured decision](../mongodb-design.md#durable-work-age-for-readiness))<br>• the Admin embedding coverage scan (pages over `_id` by design, `MongoEmbeddingCoverageRepository.scala:220-260`)<br>• the Admin analytics `repairRequired` inspection (bounded `limit`; rows expire after 31 days) | Index write cost against a bounded scan |
| D-07 | **Accepted retention.** These rows have no TTL:<br>• Failed `event_outbox` and Failed `embedding_work` (one row per entity, kept for Admin repair and audit)<br>• Active `producer_registrations`<br>• participant locks (one per user, purged on deletion) | Existing security and recovery decisions |
| D-08 | **Accepted identity deviations.** These are immutable, and a compound unique index already enforces each identity:<br>• the generation-0 reservation `_id` is the workflow ID<br>• `mutation_receipts` uses a random `_id`<br>• `event_quarantine` uses an ObjectId `_id`<br>New collections follow the standard. | Rewriting `_id`s would need copy-and-delete migrations with no defect fixed. |
| D-09 | **Index ownership.** Shared names are pinned by a contract fixture tested in both builds. Moving index ownership (D3 in [quality consolidation](hiring-platform-quality-consolidation.md)) still awaits the user. | F-L |
| D-10 | **Atlas-gated changes need Atlas Local evidence.** MSC-08 and MSC-09 change what Atlas Search reads. Neither is adopted without MVR-04.L, MVR-07.L and MVR-09.L evidence on Atlas Local, against the adoption limits in [vector retrieval optimization](mongodb-vector-retrieval-optimization.md): Recall@7 ≥ 0.95, ANN p95 ≤ baseline + 10%, write p95 regression ≤ 10%. | Vector spec gates |

### Assumptions

- Local datasets only.
- The write overhead of each new index is measured on the same disposable workload as its read benefit.
- No production data exists. Migrations are still restartable and verified, because operational MongoDB changes require it ([AGENTS.md](../../AGENTS.md)).

## Behavior and contracts

- **Public contracts.** No public GraphQL name, type or event field changes. Exceptions:
  - MSC-08 makes the job `skills` and `city` filters case-insensitive. This is a semantic change, documented in `docs/api.md` and the input descriptions.
  - `Job.updatedAt` no longer changes when an application is submitted. It now reflects content changes only.
- **Migrations.** Operational steps are new, ledgered, restartable and forward-only. Applied migrations are not edited.
  - The startup proofs of completed migrations 013 and 016 compare exact validators. They are extended to accept the documented successor validators. This follows the existing 013/018 succession pattern ([schema evolution](../schema-evolution.md)).
  - Additive indexes go in `MongoHiringIndexSetup.allSpecs` without a ledger step. Dropping or redefining an index needs a ledger step.
- **Upgrades.** Stop incompatible writers before upgrading. Strict readers reject unknown fields, so old binaries fail on the new shapes. Rolling back the application does not roll back data.

| Step | Collections | Expand, backfill, verify, contract |
|---|---|---|
| `021_job_admission_fence` | jobs | Jobs validator V2 allows an optional `admissionFence` long ≥ 0. The audit uses `$nor`. No backfill: absent means 0. |
| `022_interview_subject_attribution` | interview_workflow_inbox | Backfill hiring receipts with `documentType: "hiringReceipt"` and `subjectIds`, in bounded `_id` batches with a checkpoint. Writers emit the same shape. Proof: no inbox row lacks `documentType`. |
| `023_consumer_receipt_identity` | consumer_receipts | Verify every `_id == consumerGroup + ":" + eventId`, then drop the redundant unique index (absent is acceptable). Proof: the index is absent. |
| `024_optional_field_absence` | event_outbox, search_sessions, interview_workflow_inbox, interview lifecycle rows, users | `$unset` stored nulls. Set `updatedAt = createdAt` where it is missing. Proof: zero `$type: "null"` on the affected fields and no user without `updatedAt`. |
| `025_operational_collection_validators` | The F-K collections plus users V2 | Audit each collection with `$nor` (fail naming the collection only), then install. Proof: exact definitions. |
| `026_outbox_envelope_source` | event_outbox, search_session_work | `$unset payload` in batches. Outbox validator V2 forbids `payload`. Proof: no `payload` field remains. The stored-event reader used by migration 003's backfill (`readOperationalEvent`) decodes `envelopeBytes` from this slice on. That backfill runs only while the 013 proof is untrusted, so a database upgraded through 013 never reruns it. |
| `027_job_canonical_filters` (gated) | jobs | Backfill `skillsCanonical` and `location.cityCanonical`. Create new-named indexes, then drop the superseded ones. |
| `028_binary_embedding_vectors` (gated) | users, jobs | Convert `embedding` arrays to BSON binData float32 vectors in batches. Validator: `binData`. |

## Domain language and examples

- **Context:** Hiring operational persistence.
- **Aggregates touched:** Job (Open/Closed admission invariant), Application (no submission to a closed job), User (deleted accounts keep no profile), Interview workflow (erasure purge).
- **Terms to add** to [domain language](../domain-language.md) together with MSC-01:
  - *Content revision*
  - *Admission fence*
- **Term to change** together with MSC-01: *Subject fence*. The interview path will no longer bump `users.version`.

| Rule | Tag | Scenario |
|---|---|---|
| A submission does not stale a recruiter edit | MSC-01.1 | Given Job J at content revision 4 and Recruiter R viewing it, when 16 candidates apply concurrently and R then saves a title edit observed at revision 4, then the edit succeeds at revision 5 and J's `admissionFence` is 16. |
| Close and submit still serialize | MSC-01.2 | Given Open Job J, when R closes J while candidate Ana submits, then exactly one outcome holds: Ana's application exists and J closed after it, or J is Closed and Ana gets Conflict. Never an application created after the close. |
| An embedding completion does not stale an edit | MSC-01.3 | Given candidate Ana at revision 2, when the embedding worker stores her vector observed at revision 2 and Ana then saves her profile observed at revision 2, then both writes succeed and her revision is 3. |
| A stale embedding is rejected | MSC-01.4 | Given Job J edited from revision 4 to 5 after the embedding worker read revision 4, when the worker stores its vector, then the write is rejected as stale and work is re-queued. |
| An interview commit races participant deletion | MSC-01.5 | Given a workflow for Ana and R, when Ana's account deletion commits concurrently with an interview commit, then the commit either precedes the deletion or fails AuthorityRevoked or Conflict. It never commits for a deleted participant, and `users.version` is not changed by the interview. |
| The erasure purge uses indexes | MSC-02.1 | Given 5000 workflows (and their receipts) of 10 other users, when Ana's subject is purged, then every `find`/`delete` the purge issues shows IXSCAN with no COLLSCAN, and Ana's workflows, request receipts, hiring receipts, reservations and notifications are absent. |
| An orphan hiring receipt is purged | MSC-02.2 | Given a hiring receipt for Ana whose workflow already expired, when the purge runs, then the receipt is deleted. |
| Delta-file evidence is indexed and bounded | MSC-02.3 | Given 10000 delta-file rows, when one request's files are read, then the plan is IXSCAN on `requestId`. Every evidence row is either removed when its erasure request completes, or carries an expiry Date no later than its request's `expiresAt`, enforced by a TTL index. The owner records which mechanism was chosen and why. |
| The redundant index is gone, identity still holds | MSC-03.1 | Given migration 023 complete, when a consumer stores receipt `g:e` twice, then the second insert reports "seen", and `consumer_receipts` has no `(consumerGroup, eventId)` index. |
| Absent is omitted | MSC-04.1 | Given an outbox row released from a lease and a search session without a query, when they are stored, then `leaseOwner`, `leaseToken`, `query` and `model` are absent, not null. A new user has `updatedAt == createdAt`. |
| A malformed write is rejected | MSC-05.1 | Given the 025 validators, when an application is inserted with status `Pending` (not a status), then MongoDB rejects it with a validation error. |
| A deleted user keeps no personal data | MSC-05.2 | Given a Deleted user, when a write sets `email` or `embedding`, then the users validator rejects it. |
| Discriminators are complete | MSC-05.3 | Given migrations 022 and 025, when an inbox document without `documentType` is inserted, then it is rejected. |
| The outbox has one source | MSC-06.1 | Given a published event, when the outbox row is read, then it has `envelopeBytes` and the query fields but no `payload`, and claim, duplicate detection and replay of migration 003 behave unchanged. |
| Shared names cannot drift | MSC-07.1 | Given the contract fixture, when either build renames a shared collection, field or ledger ID, then that build's `sbt test` fails. |
| Job filters ignore case (gated) | MSC-08.1 | Given Job J with skill `Scala` in city `Kyiv`, when a candidate filters by `scala` and `kyiv`, then J is returned by list, discovery and vector search. |
| Vectors are float32 (gated) | MSC-09.1 | Given the 028 conversion, when a job vector is read and used as a query vector, then it equals the stored float values exactly. Atlas recall and latency meet D-10. Vector storage is about half the baseline. |
| Startup upgrade is idempotent | MSC-10.1 | Given a database at migration 020, when the application starts, then 021–026 complete. A second start writes nothing to the ledger and reports no validator drift. |
| Standard and docs are current | MSC-11.1 | Given the merged change, when a reviewer checks every finding, then each is fixed or listed as an accepted deviation (D-05 to D-08) in [mongodb-design.md](../mongodb-design.md), and the data-engineer skill requires the standard checklist. |

## Acceptance and evidence

| ID | Criterion | Implementation and test paths | Verification | Actual outcome |
|---|---|---|---|---|
| MSC-01 (01.1–01.5) | Content revision and fences are separate | Implemented under `src/main/scala/.../repository/mongo/`: `MongoJobContentUpdate`, `MongoSubjectFence`, `MongoJobAdmissionFenceMigrations` (021), `jobValidator` V2, submission guard and `touchJobOwner` `$inc admissionFence`, embedding writes without `$inc version`. Closure now serializes with submissions through write-conflict retry on the job document (the submission `$inc admissionFence` and the closure `$inc version` write the same document), not through a version mismatch. Tests: `MongoJobContentUpdateSpec` (including a codec-versus-update drift guard), `ContentRevisionFenceIntegrationSpec` (01.4 also asserts the embedding_work requeue) (01.1, 01.2, 01.3, 01.4), `InterviewLifecycleAccountDeletionIntegrationSpec` (01.5), `MongoHiringSetupStateIntegrationSpec` (MSC-10.1 for 021); updated ARR-03.1/03.2, DHW-16 (Admin fence), `MongoEmbeddingWorkIntegrationSpec`, `MongoInterviewWorkflowRepositoryIntegrationSpec`, `MongoJobSubmissionIntegrationSpec` | `sbt test` 1518 passed; full `scripts/run-local-tests.sh test`: 342 total, 0 failed, 326 passed, 16 skipped (Atlas and scaling specs that do not run on the local stack). Rerun by independent QA on the final state with the same counts | PASS: Code Reviewer, Security Engineer (static) and QA |
| MSC-02 (02.1–02.3) | Erasure and evidence reads use indexes | 02.1/02.2 implemented (operational build, migration 022); 02.3 pending (analytics build, in review) | MSC-02.1/02.2: `MongoInterviewErasureIndexIntegrationSpec` (explain of every purge and absence-check find/delete: no COLLSCAN, only the expected indexes; 5000 other users' rows untouched; orphan hiring receipt purged; migration 022 batches, fail-closed, checkpoint, concurrent start, reopen). The absence check examines 1 document and 1 key per collection with 5000 other workflows (10 other users) present. Workflow lookups reuse the existing `interview_workflows_candidate_id`/`_recruiter_id` indexes and the `_id` range (no new workflow index). The only new index is the inbox partial `interview_workflow_inbox_hiring_subject_ids`: measured about 520–525 KB (524288 bytes before the purge, 520192 bytes after it, in single local runs) for 5000 hiring receipts (the inbox `workflow_identity` index is 512000 bytes); write cost is accepted without a p95 measurement. Bound: one key per hiring receipt, at most one hiring receipt per workflow, written once in the commit transaction. Revisit if the hiring receipt write path shows a measurable regression or a second receipt per workflow is introduced. Also covered: orphan request receipt, Admin-initiated workflow (the Admin is not attributable), second party of a shared workflow, old-shape hiring receipt left to migration 022 until it runs, and a rescan when a late write sits below the saved checkpoint. The 022 completed-proof scan is an accepted deviation in `docs/mongodb-design.md` | See checkpoint |
| MSC-03 (03.1) | Redundant receipt index removed | Implemented (migration 023) | `MongoInterviewErasureIndexIntegrationSpec`: duplicate `record` reports seen and only `_id_` and the expiry index remain; legacy index dropped with receipts kept; restart writes nothing; mismatched `_id` fails closed and keeps the index; reappearing index reopens the step; concurrent start converges | See checkpoint |
| MSC-04 (04.1) | Absent values omitted | Not implemented | Codec unit tests, migration integration test | Not run |
| MSC-05 (05.1–05.3) | Validators on every collection | Not implemented | Validator integration tests, full integration suite under validators | Not run |
| MSC-06 (06.1) | Outbox stores the event once | Not implemented | Publisher and migration integration tests | Not run |
| MSC-07 (07.1) | Shared-name contract | `test-support/src/main/resources/mongo-shared-namespace-contract.json`, `MongoSharedNamespaceContractSpec` (root), `AnalyticsSharedNamespaceContractSpec` (analytics). Analytics shared names are constants: `AnalyticsCollections` fields, `ProducerRegistration` kind/state values, ledger ID 012. They are used in `MongoAnalyticsErasureQueue` | `sbt test` root 1518 passed; analytics 463 passed | Code Reviewer PASS (re-review). Known gap: the main build writes producer-registration `subjectId`, `transactionalId`, `fencedAt` and the kind/state values as inline literals, so the root spec exempts them explicitly. Main-side constants follow with MSC-05 |
| MSC-08 (08.1) | Case-insensitive job filters | Gated (D-10) | Discovery integration tests plus Atlas Local MVR-04.L/09.L | Not run |
| MSC-09 (09.1) | float32 vectors | Gated (D-10) | Atlas Local MVR-07.L/09.L, storage comparison | Not run |
| MSC-10 (10.1) | Idempotent upgrade | 021 covered (`MongoHiringSetupStateIntegrationSpec`: validator V2 accepts absent and non-negative long, rejects negative and int, restart writes nothing to the ledger, a 020-only database upgrades). 022 and 023 covered by `MongoInterviewErasureIndexIntegrationSpec` (restart writes nothing to the ledger, legacy upgrade, concurrent start, fail-closed with checkpoint, reopen) and the fresh-database ledger list; 024–026 not implemented 022/023: restart, concurrent-start, reopen and fail-closed behaviour is covered by the migration tests in `MongoInterviewErasureIndexIntegrationSpec`. They are tagged MSC-02.1, MSC-02.2 and MSC-03.1 by feature, not MSC-10.1. | Setup state integration test | 021–023 passed locally; 024–026 not run |
| MSC-11 (11.1) | Standard and docs | [mongodb-design.md](../mongodb-design.md#modeling-standard), `.claude/skills/data-engineer/SKILL.md` | Independent review | Not run |

## Performance and cost

- **Workload:** bounded local synthetic data only.
- **Index evidence:** each new index records explain output (keys and documents examined vs returned) plus index size and write p95 on the same workload. Local ceilings: write p95 regression ≤ 10%, index storage growth ≤ 25%.
- **Fence decoupling:** expected to remove spurious edit conflicts. Measure conflict counts before and after on the existing popular-job workload. Make no latency claim without that measurement.

## Implementation handoff

Slices, in order:

1. **MSC-11, docs and skill** (this checkpoint). Then run the slices that share no files in parallel:
   - **MSC-07:** test-support fixture and two specs.
   - **MSC-02.3:** analytics build only.
2. **MSC-01:** migration 021, fence decoupling.
3. **MSC-02.1/02.2 and MSC-03:** migrations 022 and 023.
4. **MSC-04 and MSC-05:** migrations 024 and 025.
5. **MSC-06:** migration 026.
6. **MSC-08 and MSC-09:** migrations 027 and 028, in one Atlas Local session.

Ownership rules:
- Slices 2–5 touch shared files (`MongoHiringCodecs`, `MongoHiringIndexSetup`, `MongoHiringMigrations`, `MongoMigrationLedger`), so they run one after another with one owner at a time.
- **Writes allowed:** `src/main/scala/com/example/graphQL/cats/repository/mongo/`, `src/it/scala/com/example/graphQL/cats/repository/mongo/`, `src/test/scala/...` for codec tests, `test-support/`, the named analytics adapter files, and the docs listed in MSC-11.

Reviews:
- **Code Reviewer:** every slice.
- **Security Engineer:** MSC-01, MSC-02, MSC-05 (deleted-user branch), MSC-06, MSC-08.
- **QA:** the final state.

## Checkpoint and review

- **Completed:** MSC-01 (F-A, D-01) implemented and passing locally: job admission fence, content-only `version`/`updatedAt`, targeted job content updates, embedding writes that never advance `version`, subject fence for interview and admin authorization paths, migration 021. Docs updated (schema evolution, domain language, mongodb-design, use cases UC04, ARCHITECTURE, wiki).
- **Implementation notes:**
  - The submission guard no longer has a `lt(admissionFence, Long.MaxValue)` filter: `$lt` does not match a missing field, so it would reject every first submission. `$inc` on a maxed Int64 fails in MongoDB.
  - Migration 021 audits only `admissionFence` values, not whole documents, so a legacy job with an invalid point still fails at its own geo-point proof (006) with its own typed error.
  - No startup proof compared the jobs validator exactly before 021, so no successor handling was needed; 021's proof compares the current V2 validator, which startup installs every time.
  - Existing tests that asserted the old increments were updated to the new contract: publication authorization and interview scheduling no longer stale an in-flight embedding (the embedding completes in one pass), and the first submission leaves the snapshot revision at 0.
  - MSC-01.4 asserts the rejection, that no vector was written, and that the `embedding_work` row was re-queued after the edit.
  - MSC-01.1 asserts that at least one submission was accepted (`accepted >= 1`) and that `admissionFence` equals the accepted count. Contention can make transaction retries run out for some of the 16 concurrent submissions, which then return Conflict. The property under test is that the edit succeeds whatever the accepted count is.
- **Open follow-up:** the unused `revision` of `JobSubmissionSnapshot` and its projection in `MongoJobSubmissionSnapshotCodec` remain; removing it changes the domain model, ports and fixtures, outside the MSC-01 write paths.
- **Completed (MSC-02.1/02.2, MSC-03):** the interview purge and absence check use one indexed predicate per collection (F-F operational part), reusing existing indexes except one inbox partial index; migration 022 attributes hiring receipts (`documentType: "hiringReceipt"`, `subjectIds`; subjects are taken from the receipt itself so orphans are purgeable); migration 023 drops the redundant consumer receipt index (F-G).
  - Verified in source: a workflow's candidate and recruiter are always its `candidateId`/`recruiterId` (`workflowDocument`), so the purge uses those existing indexes (a review removed the planned `subjectIds` workflow index as redundant); the initiator is the job's recruiter or the Admin (`InterviewSchedulingService`), and Admins cannot be deleted, so dropping the `initiatedBy` branch loses nothing; reservation `candidateId`/`recruiterId` are a subset of `participants` and notification `subjectIds` equals `[recipientId]` (single writers); command rows carry no subject fields and are removed via `workflowId`.
  - **Deploy precondition:** stop every writer running an earlier binary before upgrading to 022. Old-shape hiring receipts of expired workflows are not purged until 022 reruns (the next start reopens it).
  - Hiring receipts no longer store `candidateId`/`recruiterId` (replaced by `subjectIds`). Nothing read them: a grep of `src/main` finds no reader, and receipt lookups are by `_id` (`MongoInterviewWorkflowRepository.scala:77,87`, `MongoInterviewNotificationLedger.scala:43`).
  - Command rows and non-hiring inbox rows carry no subject field, so erasure reaches them only through `workflowId` while the workflow exists. This is unchanged: the previous `$or` could not match them either. A command or inbox row whose workflow has already expired is removed by the same completed-evidence TTL (`retentionExpiresAt`) as its workflow, so the bound is that retention window.
  - Migration 022's completed proof is one `_id`-ordered pass over the inbox on each start (bounded by completed-evidence TTL); revisit if the inbox outgrows its retention.
  - The shared-name fixture `mongo-shared-namespace-contract.json` does not list `consumer_receipts` or interview index names, so it needs no change from 023.
  - The F-K validators for the interview collections are still open (MSC-05); the interview command validator already rejects malformed rows.
- **Open follow-up (MSC-02):** workflow and interview notification-receipt documents still write `subjectIds`, but no reader remains: workflows are found by `candidateId`/`recruiterId`, notifications by `recipientId`. These are write-only duplicates ("store each fact once"). They existed before this slice. Fix: stop writing them in `MongoInterviewWorkflowCodec` and `MongoInterviewNotificationLedger`, and add a versioned step that `$unset`s them, with a proof. Owner: Data Engineer, with MSC-04/05.
- **Open follow-up (MSC-07):** add main-build constants for the producer-registration fields `subjectId`, `transactionalId`, `fencedAt` and the kind/state values `Operational`, `Interview`, `Active`, `Fenced`. They are inline in `MongoProducerRegistrations`, `MongoOperationalEventRepositories`, `MongoProducerRegistrationMigrations` and `MongoProducerGenerationMaintenance`. Then remove the root spec's exemption set and assert the fixture `values`. Analytics-owned index names stay documentation-only until they become constants.
- **Next action:** remaining migrations 024–026 (one owner at a time).
- Code Reviewer: MSC-01 PASS (follow-ups applied); MSC-11 PASS; MSC-02.3 PASS on condition of the follow-ups in progress; MSC-07 FAIL (fix in progress).
- Security Engineer: MSC-01 PASS (static; low findings applied or recorded); MSC-02.3 PASS (static).
- Final QA: MSC-01 PASS (unit 1518 passed; integration 342 total, 0 failed). The other slices are pending.
