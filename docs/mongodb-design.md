# MongoDB design

[Authentication receipt protection](specs/hiring-security-and-runtime-reliability.md): login/signUp receipt fingerprints are HMAC-protected; no conversion of older receipt shapes pre-MVP (receipts expire by TTL); user data and non-authentication receipts are unaffected.

See the [current collection inventory](wiki/mongodb-and-analytics.md) for every operational and analytics MongoDB collection, relationships, indexes, retention and use-case links.

## Modeling standard

Every new or changed collection, field, index, validator and query follows this standard. A deviation is allowed only with a recorded bound and revisit trigger, either in the owning spec or under [accepted deviations](#accepted-deviations). The data-engineer role applies it as a checklist; Code Reviewer checks it on every persistence change.

The standard is derived from the MongoDB manual's [data-modeling guidance](https://www.mongodb.com/docs/manual/data-modeling/) (best practices, design patterns, anti-patterns, schema validation), and from three books: *MongoDB in Action* 3rd ed. (MiA), *Mastering MongoDB 4.x* (MM4) and *Practical MongoDB Aggregations* (PMA). The [schema consolidation spec](specs/hiring-mongo-schema-consolidation.md) records the review findings and decisions. Some existing code does not yet meet the standard; it is tracked there as open findings F-A to F-Doc. Do not copy those patterns, for example fences stored in `version`, `null` placeholders, or the duplicated outbox `payload`. (Fixed by MSC-01: `version` bumps used as a fence and embedding writes that advanced `version` are gone; submissions use `jobs.admissionFence`.)

### 1. Shape from access patterns

- **Start from the use case, then the shape.** Before shaping a collection, list each query and write: use case, filter including authorization predicates, sort, projection, frequency and cardinality (MiA §5.1).
- **Embed only what belongs to the parent.** Embed values the parent owns that are bounded, read together and written together: profile, location, embedding metadata, search results capped at 100.
- **Reference what grows or stands alone.** Use a reference for an entity with its own lifecycle, or for anything that grows without a fixed bound. Applications, application events and workflow evidence never become arrays on users or jobs (unbounded-array anti-pattern; MM4 p96-98).
- **Copy data only with an owner for staleness.** Denormalize (Extended Reference, Computed) only when there is a measured read benefit and a named update path. Immutable or temporal copies are acceptable. A copy that must stay fresh needs the same transaction, or a repair job with a staleness bound.
- **Keep documents lean.**
  - Large or rarely read fields (vectors, raw envelopes) are excluded from list and hydration projections.
  - Read shapes are named, not implied. A repository exposes one projected read per consumer and no consumer takes a wider one than it needs: the actor projection (`users.findActor`: `_id, role, accountStatus, adminSingletonKey`), the job ownership projection (`jobs.findOwner`: `_id, recruiterId, status`), the profile or display read (the document minus `embedding`, `embeddingMeta`, `passwordHash`), and the whole-document read (`findVersioned`, minus `passwordHash`) that only the embedding pipeline and search ranking use. List reads (`MongoKeysetPaging.page`) and `$lookup` sub-pipelines apply the same exclusions. Minimal projections decode strictly (`MongoAuthorizationProjectionCodecs`: exactly the projected fields, anything else is `InvalidStoredData`); the profile and display reads use the full-document decoder, which already fails closed on unknown fields, and report `embedding = None`.
  - A write that needs the result returns it from `findOneAndUpdate(ReturnDocument.AFTER)` with the same projection instead of updating and reading again, and a compare-and-set that needs only the revision projects `version` alone.
  - An aggregation that joins an authorization or ownership check after a paged read sorts and limits on the indexed collection first, then checks the actor, so the index orders the page (`findByCandidate`: local `explain` on 1000 candidate applications examines 20 documents, winning plan `LIMIT > FETCH > IXSCAN applications_candidate_created_id`, no `SORT`; the previous shape examined 1000).
  - Updates use targeted `$set`/`$unset`/`$inc` and never rewrite fields they did not change. In particular, a content edit must not rewrite a vector.
  - Single documents must stay far below 16 MB. A capped array states its cap in the code and in the validator.
- **Polymorphic collections need a discriminator.** Every document carries `documentType`, and the validator has one `oneOf` branch per type (MiA p108).

### 2. One meaning per field

- **`version` is the content revision.** It advances only when user-visible content of a user or job changes, and optimistic edits compare against it.
- **Conflict fences are separate.** Fields or documents that exist only to make concurrent transactions conflict are kept apart from `version`: for example `outbox_subject_fences.fencingVersion`, or a per-aggregate admission fence.
- **Derived data never advances the revision.** Embeddings and other derived values may be guarded by the revision but must not advance it. Otherwise background work makes user edits fail.
- **Coordination counters are not schema versions.** Workflow revisions, claim tokens and generations coordinate concurrency and say nothing about the schema.

### 3. Types and encoding

- **IDs:** UUIDs are stored as canonical lower-case strings. Composite keys are deterministic strings documented next to their writer. Do not mix `_id` types within a collection.
- **Time:** instants are BSON Date in UTC, millisecond precision. Never store them as strings. TTL fields are Dates.
- **Enums** are stored as their Scala case names and listed in the validator `enum`.
- **Numbers:**
  - Revisions, counters and offsets are Int64.
  - Small attempt counts and constant markers are Int32.
  - Money would be Decimal128.
  - Vectors use the smallest faithful type.
  - One BSON type per field, always (MM4 p84-85).
- **Absent values are omitted, never stored as `null`.** Readers treat absence as `None`; codecs use `createCodecProviderIgnoreNone`. Required timestamps such as `createdAt` and `updatedAt` are written on insert.
- **Store each fact once.** Do not keep both a parsed and a serialized copy of the same payload. Query fields may be lifted next to an opaque payload only when a query filters on them.
- **Normalize canonically.** Case-insensitive equality uses a canonical sidecar written by the domain normalizer (`*Canonical`), the same way for every collection that is filtered on that concept.
- **Naming:** fields are camelCase, collections and indexes snake_case, and index names describe their keys. New lease or claim fields reuse the shared names `leaseOwner`, `leaseToken`, `leaseUntil`.

### 4. Indexes

- **Every query has an index or a documented bound.** Each read or delete the code issues is either backed by an index (explain shows `IXSCAN`; keys ≈ documents ≈ returned), or listed with its bound and revisit trigger.
- **Each `$or` branch needs its own index;** otherwise the whole query falls back to a collection scan (MiA p174). Prefer one indexed predicate per collection over a generic `$or`.
- **Order compound keys by the ESR rule:** equality, then sort, then range (MiA p154-156). Paged lists end with `(createdAt, _id)` for deterministic keyset paging.
- **Avoid redundant indexes.** Do not add an index whose key is already guaranteed by `_id`, or that is a prefix of another index. Drop indexes nothing uses, and record the evidence.
- **Use partial indexes for lifecycle subsets** (active state, a document type) rather than sparse ones. A query must imply the partial filter to use the index.
- **Expire transient data with TTL.** Receipts, sessions, retention evidence and terminal work use TTL on an `expiresAt`/`retentionExpiresAt` Date. Rows kept without TTL are listed with their bound.
- **Every new index carries a cost record:** explain output plus index size and write p95 on the same workload (local ceilings: write p95 +10%, index storage +25%).

### 5. Validation and evolution

- **Every application-owned collection has a strict `$jsonSchema` validator** (`validationLevel: strict`, `validationAction: error`). Use `additionalProperties: false` where the shape is closed. Startup verifies exact definitions and fails closed on drift. Migration `025_operational_collection_validators` installed them on every operational collection that lacked one; the validator text is in `MongoOperationalValidators` and `MongoHiringValidators`.
- **Schema changes are ledgered migration steps.** Each step is restartable and batched, with a checkpoint and a read-only completion proof ([schema evolution](schema-evolution.md)). Never edit an applied step. When a validator gains a successor, completed proofs that compare validators accept the documented successor.
- **No per-document `schemaVersion` before MVP.** One active shape is kept by migrations plus validators.
- **Every migration step documents:** stop-writer preconditions, backfill bounds, restart behavior, forward-only recovery and post-verification.

### 6. Transactions and consistency

- Prefer single-document atomicity by modeling. Use a transaction only when an invariant spans documents: status plus history plus outbox, admission, deletion fences.
- Keep transactions short and indexed: at most about 1,000 documents, every operation selective.
- Use atomic operators (`$inc`, conditional filters) instead of read-modify-write.
- A write-conflict fence must actually modify the fenced document (an `$inc`, not a no-op `$set`); otherwise it creates no conflict.
- Enforce uniqueness in the store with unique or partial-unique indexes, never by a preflight read.

### 7. Aggregation

- `$match` and `$sort` go first so that they use indexes. Put `$limit` right after `$sort`.
- Use `$set`/`$unset` rather than a wide `$project`.
- In `$lookup`, join on an indexed foreign field and trim the joined fields in a sub-pipeline.
- Avoid `$unwind` followed by `$group` on `_id`; use array operators instead (PMA ch3).
- Frequent `$lookup` is a signal to reconsider embedding or an Extended Reference.

### Accepted deviations

These are recorded with their reasons in [the consolidation spec](specs/hiring-mongo-schema-consolidation.md#decisions):

- **String UUIDs instead of BinData subtype 4.** The strings appear in composite IDs, Atlas filters, Kafka and Delta.
- **No `schemaVersion` field.**
- **Polymorphic interview collections are kept.**
- **Unindexed reads that are bounded:**
  - the `recruiter_job_closure_work` claim/peek (see [durable work age](#durable-work-age-for-readiness));
  - the Admin embedding coverage scan, which pages over `_id` by design;
  - the Admin analytics `repairRequired` inspection: the limit is bounded and rows expire after 31 days.
- **Migration 024 completed proof reads one document per repaired target on every start** (outbox, search sessions, search-session work, inbox, workflows, notification receipts, users; `find` of the pending predicate with `limit(1)` and no supporting index). Bound: collection size; every one but `users` expires by TTL or retention. Revisit when `users` exceeds 1 million rows or startup is measurably slower; then trust the ledger plus the migration 025 validators (they reject the same regressions for users, search sessions, inbox and notification receipts) instead.
- **Migration 022 completed proof scans the interview inbox on every start** (`find` without `documentType`, `limit(1)`). Bound: inbox size, which is the rows of active plus retained workflows (finished workflows expire by TTL; rows of unfinished workflows carry none). Revisit when the inbox exceeds 1 million rows or startup is measurably slower; then trust the ledger plus the partial index instead.
- **No TTL on these rows:**
  - Failed outbox and embedding-work rows;
  - Active producer registrations;
  - participant locks.
- **`outbox_subject_fences` keeps its name and its `deleted`/`deletedAt` fields (consolidation S3, MR-06).** The collection holds the subject fence of applications, interviews and admin authorization as well as the publisher lease, so a neutral name would be more accurate, but the name is pinned by the shared namespace contract with the analytics build and is not renamed. The two deletion facts are written in the same transaction as `users.accountStatus` and cannot diverge, but analytics still reads `outbox_subject_fences.deleted` as its purge gate (`MongoAnalyticsErasureQueue.publisherDrainReady`), and the lease renewal uses the flag to stop a deleted subject's in-flight publication. The claim path no longer reads it (deletion is read from `users`). Revisit when the analytics gate reads `users.accountStatus`: change the analytics build and the contract fixture together, then add a ledger step that unsets the two fields.
- **`account_registry` (one `user-account-registry` row) duplicates the admin existence fact enforced by the unique `adminSingletonKey` index.** It is the bootstrap coordination row of the first-admin transaction, and its state cannot disagree with the index because the index is the authority. Kept; revisit with the bootstrap redesign.
- **`interview_workflow_commands` is a second outbox** (consolidation S3, MR-08). It carries interview intents with their own states (`Pending`, `Claimed`, `Executing`, `ResultPending`, ...), claim fields (`claimOwner`, `claimToken`, `claimUntil`) and result handling that the operational `event_outbox` does not model, and it is claimed by the interview worker with a fencing token per command. Merging the two would couple interview recovery to the Kafka envelope contract. The lease field names are historic and are not renamed.
- **Interview request idempotency receipts expire instead of being erased with the account (MR-08).** They outlive the deletion by up to 7 days and hold a pseudonymous actor UUID and a workflow id; accepted. They are `mutation_receipts` rows (scope, operation, fingerprint, workflow id; 7-day TTL), like every other mutation receipt, rather than documents of the workflow collection that account erasure removed. They hold no profile data. A repeated no-op request (a reschedule request on a workflow that already carries the flag) now stores a receipt for its key where the interview store formerly stored none; the cost is one TTL-bounded row per fresh key, as for any mutation.
- **Historic identity shapes:**
  - the generation-0 reservation `_id`;
  - the random `mutation_receipts` `_id`;
  - the ObjectId `event_quarantine` `_id`;
  - the lease and claim field names in existing interview collections.

## Candidate residence and interview deletion integrity

[Current local corrections](specs/candidate-discovery-and-interview-integrity.md) retain bounded workflow attribution until linked commands/inbox/provider records are deleted. Each batch deletes only selected workflow identities. Migration `016_candidate_residence_integrity` audits the existing residence shape without modifying user data or aggregate revisions; installed validation requires display and canonical city fields together. Completed proof and validator drift fail closed. Stop incompatible writers before cutover and explicitly repair malformed data before rerunning startup.

### Measured admission and publication access

The October 7 [bounded local comparison](specs/hiring-search-publication-reliability.md) retained strict ID/status/revision submission projection: read throughput improved 66–283% across three pairs at concurrency 1/8, with worst write p95 growth 8.8%. Popular-job concurrency-eight writes still encounter expected revision conflicts; the atomic Open/revision guard remains authoritative. These are synthetic repository measurements, not API or deployed SLOs.

Retryable/InFlight partial-index candidates for the native OR outbox claim failed all twelve adoption comparisons. Index growth was 18.4–23.4%, with inconsistent read benefit and shared-recruiter write regressions up to 91%; production indexes remain unchanged. Exact facets and geographic pages were observed on 256 jobs/257 skills only. That dataset does not establish high-volume saturation or justify weaker facet semantics.

Concurrency-eight baseline cohorts recorded about 4200 claim attempts for approximately 500 completed publications, versus 500 attempts at concurrency one; durable subject leases reconcile to zero after each cohort. Scaling must account for contention and scan amplification, not only index selection. Keep subject/deletion fencing and the fair bounded claim scan until a separately measured protocol change demonstrates equivalent safety. The attempt figures above describe the per-row claim; the batch lease described under [the status write and work rows](#status-write-work-rows-and-the-batch-claim) replaced it and keeps the same subject and deletion fencing.

## Retrieval and workflow integrity cutover

See [the reliability specification](specs/hiring-retrieval-publication-reliability.md) for current acceptance evidence. Migration `012_attributable_producer_registrations` transfers existing producer attribution before removing growing fence/cleanup arrays. Active registrations never expire; only broker-confirmed fenced rows receive retention expiry. Cleanup traverses 64 records per batch. Stop incompatible writers before startup; preserve the database and verify the completed migration and strict validators before restarting analytics.

Migration `013_hiring_workflow_integrity` audits stored commands and outbox records once, then verifies exact strict validator definitions on startup before skipping the former command/outbox full audits. Migration `015_interview_cleanup_integrity` provides the equivalent audited cleanup baseline, including its configured physical Kafka topics. Validator or proof drift fails closed. Explicit maintenance auditing uses `sbt 'runMain com.example.graphQL.cats.repository.mongo.MongoWorkflowIntegrityAudit'`; it records bounded progress for repair and rerun. Canonical UUID command identities are required by the new validator.

Cleanup sweeps retain a finite identifier ceiling and per-record failure isolation. The active-state compound partial index supports ordered bounded scans without traversing completed history. Operational outbox inserts validate the complete event list before writing; invalid stored facts retain raw evidence and deletion attribution in terminal Failed state. Receipt deduplication uses the unique insert result without a preflight read. Mongo transactions, unique indexes, leases and conditional writes remain the cross-system integrity boundary.

For embedding work, inspect with `sbt 'runMain com.example.graphQL.cats.repository.mongo.EmbeddingWorkRepair <admin-id> <Job|CandidateProfile> <entity-id>'`; append the inspected generation to repair failed work. The transaction requires an active singleton Admin and uses generation compare-and-set. Deletion removes candidate embedding values and metadata atomically; all writes retain active Candidate and observed-revision predicates.

Normal retirement uses `sbt 'runMain com.example.graphQL.cats.repository.mongo.ProducerGenerationRetirementMain <transactional-id>'`. Stop the selected producer generation first and configure the matching publisher/fencer principal. The command validates its canonical prefix and UUID, obtains broker fencing, then checkpoints registrations in batches of 64. Running it against a live generation fences that process; it must restart with a new identity. A failed broker operation leaves attribution active; producer close alone does not authorize TTL.

Read-only inventory uses the same entrypoint with `list` and an optional returned continuation; each page reads at most 64 registrations and may repeat a generation across subjects. Startup/close diagnostics identify canonical generation IDs without subject data. Stop and prevent automatic restart of the selected publisher before retiring exact IDs; uncertain ownership requires stopping all relevant publishers. Repeat retirement after an interrupted checkpoint. No age-based Active-row expiry is permitted.

Application-admission reads project only `_id`, `status` and the canonical nonnegative BSON-long `version` into a strict immutable snapshot. The submission guard itself no longer compares the projected revision; the snapshot field is retained only because the domain snapshot type and the service ports still carry it, and dropping it (and its projection) is a follow-up that needs a domain-model change. Submission writes the job guard (`_id`, status Open, `$inc admissionFence`; neither `version` nor `updatedAt` changes) transactionally with application, history and outbox, so it conflicts with a concurrent closure but never stales a recruiter edit observed at the content revision. Popular-job and shared-subject serialization remain deliberate correctness boundaries; the [current comparison](specs/hiring-search-publication-reliability.md) records measured limits and candidate-index outcomes before adoption.

The shared permit bounds four costly retrieval operations, rather than four driver commands. RRF can execute two job or three candidate branches per admitted operation, allowing up to twelve branch aggregates at the default bound; authoritative hydration has separate bounded batch size and deadlines. Measure this fan-out before increasing admission.

Lexical Search definitions now use explicit static text/token/date/boolean mappings. Existing same-name definitions are verified and incompatible definitions fail startup; the application never silently drops them. An Atlas cutover needs an explicitly prepared compatible index (or separately configured new index name), queryability verification and the live retrieval acceptance gate before activation.

Production retains existing ANN settings. Disposable Atlas comparisons separate branch limit from candidate budget: branch 100 with candidates 100/500/2000 against ENN. Stored source and scalar quantization remain opt-in benchmark variants until capability, recall, latency and storage/write gates pass. Generated artifacts belong under ignored `.local/data/`.


## Deleted candidate embedding retention

Account deletion unsets vector values and metadata in its transaction; candidate embedding persistence requires Active/Candidate predicates and the observed revision. Migration `014_deleted_account_embeddings` also removes these fields from prior Deleted tombstones, using projected raw IDs, 500-record guarded batches, and acknowledged writes. The unset itself is the restart checkpoint; unrelated fields and revisions are preserved. Completed startup verifies residual absence and fails closed on reintroduced fields. Stop older writers before cutover; see [recovery and validation](schema-evolution.md#deleted-account-embedding-cleanup).

Embedding conflicts reread authoritative searchable source and metadata through the existing pure preparation policy. Absent/ineligible/profileless or current work completes; oversized work records terminal failure; necessary work and failed rereads retain guarded uncharged retry. Real scheduling/provider and deletion/provider regressions belong to the [current follow-up](specs/hiring-retrieval-publication-reliability.md#account-erasure-and-recovery-follow-up).

## Current operational persistence

The core `users`, `jobs`, `applications` and append-only `application_events` collections are implemented. Supporting collections provide account identity, mutation receipts, durable embedding/search work, outbox/fences, consumer receipts/quarantine, and analytics erasure/report controls. [MongoNames](../src/main/scala/com/example/graphQL/cats/repository/mongo/MongoNames.scala) is the complete application-owned name catalog; analytics also owns its runtime control collections.

[Codec models](../src/main/scala/com/example/graphQL/cats/repository/mongo/MongoHiringPersistenceCodecs.scala), validators and [index setup](../src/main/scala/com/example/graphQL/cats/repository/mongo/MongoHiringIndexSetup.scala) define the active stored contract. IDs are UUID strings in BSON; domain IDs remain typed UUIDs. Stored enums use Scala names such as `Open`, `Candidate` and `Interview`, while GraphQL enums use their schema representation. User/job `version` is the content revision that guards optimistic edits and embedding writes; `jobs.admissionFence` and `outbox_subject_fences.fencingVersion` are the conflict fences. A manual or operational change to the Admin user document must also bump or delete `outbox_subject_fences/<adminId>`, so in-flight admin embedding repairs conflict (they fence through that document, not through `users.version`). Status/history and outbox effects commit atomically; unique indexes protect candidate/job applications and singleton Admin.

Mongo write helpers return the driver's result types; absence of a result is not modelled as `Option`. Transactions run through `MongoTransactionRunner`, which retries on the driver's error labels: `UnknownTransactionCommitResult` retries only the commit (up to 3 attempts), and `TransientTransactionError`, including one that also carries the unknown-commit label at commit, retries the whole transaction (up to 3 attempts). Backoff is exponential from 25 ms, capped at 250 ms. Failures to start the session or transaction, and other driver errors, become `RepositoryError.Unavailable`; duplicate keys and write conflicts map to their typed repository errors. Classification is shared through `MongoErrors`.

Three optional stored fields carry newer, additive data without a backfill. `application_events.actorRole` records the acting user's role on new events; `interview_workflows.initiatedByRole` records the initiating role. Both are absent on older documents and read as null. Decoding fails closed on any other stored value, so an unknown role makes the document invalid rather than defaulting. Job `location.city` is optional: an absent field and the legacy empty string both decode as no city, and no city is written when there is none.

Embedding metadata currently stores `model`, `sourceHash` and `updatedAt`. User/job document `version` is the content revision, not an embedding version; embedding writes are guarded by it but do not advance it. Current schema policy and restartable operational migrations are described in [schema evolution](schema-evolution.md). Query/index optimization and new vector tuning are planned in Phase 8; existing indexes alone do not establish an SLO.

Durable embedding enqueue uses one identity-matched conditional update pipeline. Enqueue during Processing advances generation while retaining the active lease; stale completion/failure/retry cannot remove the newer generation. Non-processing work becomes Ready and clears obsolete lease/terminal metadata. The pipeline runs within the owning mutation transaction when supplied a session; transient transaction errors reach the existing transaction retry boundary. Embedding writes retain their original observed aggregate revision, so a concurrent edit or deletion rejects stale persistence. These changes preserve the stored shape and require no migration or index change. Local recovery evidence and remaining gates are recorded in the [search specification](specs/hiring-search-enhancements.md#search-evaluation-and-embedding-architecture).

## Candidate matching search

Candidate profiles persist optional `currentResidence`, `availabilityStatus`, and `recruiterSearchOptIn` fields. Location also stores `countryCanonical` and optional `cityCanonical` sidecars for case-insensitive equality filters. Skills retain their original spelling for profile display and have a `skillsCanonical` sidecar for all-required skill filters. Residence, availability and consent are excluded from embedding text; profile skills remain part of the canonical searchable text.

Candidate search uses the configured Atlas vector index (`vector-search.indexes.candidates`, default `candidates_embedding_vector_match_v1`); candidate matching and vector candidate retrieval use that active configuration. Its filter paths cover role, active-account status, embedding model, canonical skills, consent, canonical country/city, and availability. Candidate lexical search uses the fixed `profile.skills` and `profile.experienceSummary` fields through `candidates_text_search`. Index setup waits for queryability and verifies the returned definition; a same-name incompatible definition fails startup without dropping or replacing existing indexes.

Candidate matches return only ID, name, skills, experience summary, embedding metadata, and score from persistence. Residence/status/consent are available through the authenticated candidate's own profile only. Recruiter search telemetry stores the job identifier but omits private filter values.

Private residence/availability filters use an OR predicate: an opted-in candidate must satisfy every supplied private filter; a profile with opt-in false or absent bypasses only these private predicates. All other retrieval predicates, including Candidate role, Active account, model, and required skills, apply to every branch before its candidate limit.

## Candidate profile migration 002

Before deploying this schema, stop every older application binary. Older profile updates replace the full profile and can erase fields introduced here. The startup migration `002_candidate_search_profile_verification` scans users in bounded `_id` batches, checkpoints after each verified batch, allows absent private fields, rejects malformed present residence/status/consent values, and derives canonical skills from the existing skill list. It marks completion only after the scan. A failed validation leaves the migration incomplete; repair the malformed record and restart. Re-running a completed migration skips the scan. It never infers or backfills location, availability, or consent.

Profile writes validate the same residence limits and enum values. Legacy absent consent decodes as false. The migration is forward-only; application rollback is not a data rollback. Older index names are not a second active read path. Retire an obsolete index only after verifying that no current consumers use it and independently reviewing the change.

## Vector search optimization

Phase 8 establishes the retrieval baseline; Phase 9 evaluates human relevance; Phase 10 adopts justified discovery improvements. Phase 8 is in progress: plain-MongoDB criteria MVR-01 to MVR-03 pass, while Atlas Local (`.L`) gates are pending and cloud (`.C`) gates are deferred. The requirements below are not measured gains or changes to the current search contract.

Detailed query, index, stale-result and verification requirements belong to [MongoDB access and vector retrieval optimization](specs/mongodb-vector-retrieval-optimization.md). Proposed geospatial/facet contracts belong to [hiring discovery and search quality](specs/hiring-discovery-search-quality.md); durable workflow inbox/state/outbox transactions belong to [durable hiring workflows](specs/durable-hiring-workflows.md). Operational collection/index changes retain restartable migration and verification obligations; pre-MVP API/analytics contract changes follow the separate single-active-shape policy.

- Record server/search deployment capabilities, embedding model/version/dimension/similarity, index definitions and readiness, corpus seed/size, query/filter distribution and concurrency. Validate lexical and vector index definitions separately; unsupported capabilities fail closed.
- Keep authorization/visibility, active-account, model and existing private-filter/consent predicates inside indexed retrieval filters where supported, before bounded candidate selection. Preserve the current opt-in OR semantics described above. Recheck authoritative eligibility before returning data when an asynchronously updated index can be stale; bound any refill and expose the agreed short-page behavior. Facets/counts must obey the same visibility policy.
- Separate final top-k, lexical/vector branch limits and ANN `numCandidates`; benchmark candidate budgets across broad, selective and empty filters. Compare ANN with exact nearest neighbors on a bounded corpus using the same filter/model/metric; record recall and latency separately from human-labelled relevance. A fixed candidate multiplier is a starting experiment, not an acceptance threshold.
- Project only authorized result fields; avoid returning vectors, repeated source hydration or unbounded fusion inputs. Keep application RRF and tie-breaking pure and reproducible. Evaluate supported server fusion against the same quality/privacy baseline before adoption. Search cursor behavior must specify tie order and concurrent-index-change limitations rather than assume a stable vector snapshot.
- Evaluate scalar/binary quantization only when supported by the selected deployment/model. Measure memory/index size, build time, recall and rescoring/latency tradeoffs; do not change vector dimensions or similarity without an explicit embedding/index contract. Dedicated search capacity and caching require measured need and a cost ceiling. Any cache needs authorization-aware keys, model/source freshness, invalidation and deletion semantics.
- Compare before/after query plans, examined/returned counts where available, p50/p95/p99, throughput, errors, index write/storage overhead and provider token/request cost. Require independent review and real search-capable integration evidence, including stale embeddings, deletion/consent changes and provider outage recovery. No Atlas provisioning, paid benchmark or index replacement is authorized by this roadmap.

The tuning options follow MongoDB's [vector query documentation](https://www.mongodb.com/docs/atlas/atlas-vector-search/vector-search-stage/) and [quantization guidance](https://www.mongodb.com/docs/atlas/atlas-vector-search/vector-quantization/). Verify capabilities against the actual deployment at implementation time.

## Current bounded authorization and eligibility reads

Operational application/history queries use a validated `HiringReadScope` and check the persisted active actor together with authorized parent selection inside MongoDB. Recruiter application queries select the owned job before joining applications; history selects the authorized application before events. Nested keys carry parent and related IDs and use request-local fetchers. Candidate access to a closed historical job/recruiter requires that candidate’s own application relationship.

Search services retain typed input and current actor/query-entity prechecks. The final eligibility selection independently gates the active actor and, for candidate matching, the owned open job and its source/embedding snapshot. Recommendation selection also gates the current candidate query source. Hit IDs are distinct and bounded by the branch-result budget; inclusion projections exclude embedding vectors and candidate email/resume references. Pure eligibility policy compares current source hashes, metadata, status, consent and supplied filters, preserving surviving rank before final `first` truncation.

The preserved historical local operational baseline recorded production command shapes/counts and sanitized execution statistics at concurrency 1 and 8 under `.local/data/mongodb-access-evaluation/`. It recorded full operational document hydration where retained, combined winning and rejected explain index names, and marked resource/vector-index telemetry unavailable. Ordinary index setup verifies ordered keys, unique/sparse/partial/TTL definitions, preserves integrity indexes on mismatch, and supports repeated/interrupted execution.

The extended operational measurement suite preserves that historical baseline and adds dedicated warmup, filter/status/later-page variants, separate winning/rejected/executed index references, metric locations, scoped Mongo-container CPU/memory boundary samples and isolated transactional writes. Outbox empty claims are supported contention outcomes: attempted throughput and completion-only publication latency/counts remain distinct, with durable state and lease reconciliation after each phase. The separate structured GraphQL suite measures authenticated loopback HTTP through services and real MongoDB, including forbidden ownership/role outcomes; its resource usage is not inferred from the repository workload. Generated reports remain ignored local artifacts. Selected local optimization ceilings are 10% write p95 regression and 25% ordinary index-storage growth on identical before/after workloads; these ceilings and descriptive local timings do not establish paired optimization, deployed SLO or Atlas acceptance.

The cleanup sweep uses the partial `interview_cleanup_active_identity` index on `_id, requestedAt` for active states, preserving its finite identity ceiling and failure isolation. A dedicated MongoDB 8.0.32 replica set with 256 MiB WiredTiger cache, 2 GiB tmpfs and 3 GiB memory limit measured 10000 Complete plus 33 active rows at concurrency one, with 200 warmup and 1000 measured samples. Native winning/executed plans examined at most 33 active records without sorting, versus at least 10000 completed records in the baseline. With Spark idle, read p95 was 21.57 to 0.82 ms, write-pair p95 2.90 to 2.08 ms and total physical index storage 552960 to 573440 bytes (3.7%). Identical workloads and native fsync boundaries were used; failed earlier runs and passing raw reports remain under ignored `.local/data/cleanup-access-evaluation/`. These local measurements meet the selected cost ceilings; concurrent-workload variance and production scale remain separate acceptance work. No other integrity/filter index was removed.

See the [Phase 8 evidence specification](specs/mongodb-vector-retrieval-optimization.md) for current verification and deferred Atlas/performance gates; this architecture description alone is not runtime acceptance.

## Local geographic and interview storage

Job coordinates are optional GeoJSON Points in `location.point`, ordered longitude then latitude. The verified `2dsphere` index supports first-stage `$geoNear`; eligibility and structured filters apply before bounded pagination. Facets aggregate the full eligible set with each skill counted once per job. Operational migration `006_job_geo_points` validates existing coordinates without fabricating points.

Interview stores keep workflows, inbox, outgoing commands, fake reservations, participant locks and per-recipient notification receipts. Request idempotency is a `mutation_receipts` row written in the transaction of the guarded write (operation `interviewWorkflow`, so a key reused for another action or input is a conflict), and the hiring commit is evidenced by its deterministic `application_events` row (`StableIds.uuid("<workflowId>:status")`); neither has a document of its own in the interview collections any more (migrations 027 and 028). Migration `007_interview_workflow_storage` introduces their namespaces and indexes; `008_interview_subject_cleanup` introduces restartable deletion barriers. `009_interview_inbox_identity` retires the known unfiltered inbox index and verifies a partial identity index so quarantine and inbox receipts do not collide on absent fields. No unfinished workflow has TTL. Stable identities, transactional participant-lock writes and revision/claim checks protect replay, overlap and stale workers. Stop incompatible writers before cutover; initialization verifies index definitions. [Workflow specification](specs/durable-hiring-workflows.md) records acceptance evidence.

Participant availability uses the ordinary compound `participants`, `startsAt`, `endsAt` index and an explicit unreleased-reservation query predicate. MongoDB rejects an absent-field (`$exists: false`) partial-index expression, so reservation eligibility remains a query condition. Participant lock documents serialize the authoritative overlap check and reservation write in the provider transaction.

### Interview recovery contracts

Migration `010_interview_workflow_attempts` initializes conservative durable execution accounting independently of publication attempts, preserves existing records and treats interrupted execution as uncertain. Notification receipt lookup stores its participant explicitly; only guarded Admin repair resets the execution epoch. Migration `011_interview_publication_fencing` validates cleanup rows and initializes typed state/revision and immutable producer snapshots. Legacy rows, including Complete, restart proof from Pending with fresh barriers. Both are bounded, repeatable startup steps; malformed rows fail closed before workers start. No local data reset is required.

Migration `019_interview_request_receipt_index` created the sparse `requestWorkflowId` index that `028_interview_request_receipt_removal` later drops together with the receipts it served; the completed proof of 019 accepts 028 as its successor.

Migrations `017_interview_ledger_collections` (renames the calendar and notification ledgers from their `fake_`-prefixed names to `interview_calendar_reservations`, `interview_calendar_participant_locks` and `interview_notification_receipts`), `018_interview_cancellation_reschedule` (extends the command validator and makes reservation identity unique per workflow and key) and `019_interview_request_receipt_index` (sparse `requestWorkflowId` index) support cancellation and rescheduling. Their details and the forward-only recovery rule are in [schema evolution](schema-evolution.md#interview-ledger-names-and-cancellation-storage).

Interview publication authorization and account deletion serialize writes on the permanent subject-fence row. The `interviewTransactionalIds` field is separate from operational `transactionalIds`; cleanup cannot register a replacement generation after deletion. Cleanup uses Pending → ProducersFenced → MongoPurged → AwaitingRetention → Complete with expected revision/state CAS. Purge and absence checks run one indexed predicate per collection: workflows by `candidateId` or `recruiterId`, reservations by `participants` and notification receipts by `recipientId`. Commands and inbox rows carry no subject and are removed through `workflowId`; the hiring and request receipts that once needed their own subject predicates no longer exist (migrations 027 and 028). No branch is unindexed, so the purge never scans a collection (migration 022; explain evidence in `MongoInterviewErasureIndexIntegrationSpec`). Before any local evidence of a workflow is purged, every reservation of it that is still held (original, replacement and compensated generations) is cancelled with the provider's confirmation under its own cancel key; a failed or unknown provider answer stops the purge and the next sweep retries, so a slot is never leaked. The cancel, reschedule proposal, request flag, expiry and informational notification state of a deleted participant's workflow is purged with it and nothing is sent. Maintenance must stop and revoke old nontransactional writers before applying these migrations; see [recovery acceptance](specs/hiring-workflow-recovery.md).

Cleanup selection sweeps ascending raw `_id` values in pages of at most 32 through the existing `_id` index. Each sweep captures its greatest eligible key and request-time cutoff, then wraps after exhaustion. Scalar request Dates after the cutoff wait for the next sweep; malformed timestamps and identities remain independently decoded evidence. Aggregation comparisons use literal BSON bounds to preserve mixed-type ordering. The immutable opaque continuation stays in the worker stream, while state/revision CAS and permanent fences retain durable authority. A bad row does not stop valid rows; a failed page read does not advance selection. No stored field or index migration is introduced. Query-plan and failure evidence belongs to the [boundary reliability specification](specs/embedding-workflow-boundary-reliability.md).

## Status write, work rows and the batch claim

Consolidation S3 (MR-04 to MR-08, MR-12, FD-03, FD-05; [spec](specs/hiring-model-and-retrieval-consolidation.md)). Stored-shape retirement uses ledger steps 027 to 030; step 026 is reserved for the outbox envelope work of the schema consolidation.

**Status write (`ApplicationStatusWrite`).** The API status change, the interview hiring commit and the interview cancellation all call one session-scoped write: `$set status, updatedAt` on the application, guarded by `_id`, `jobId`, `candidateId` and the previous status (a stale or forged write matches nothing and is a `Conflict`); then the append-only `application_events` insert; then the outbox inserts. The document is never replaced, so the other fields cannot be overwritten by a caller's copy. Invariant, asserted by `ApplicationStatusInvariant` at the end of every status-changing integration test: `applications.status` equals the `newStatus` of the latest history row, and the history is a chain. Access pattern: write by `_id` (primary key); no new index.

**Search session work (MR-04).** `search_session_work` holds the query-free session only. The `SearchPerformed` event is rebuilt from it when the work completes (`OperationalEvents.searchPerformed(session)`, identity `search-performed:<searchId>`), so the result list is stored once in the work row, then once in `search_sessions` and once in the outbox payload. The non-queued `MongoSearchSessionRepository.save` path was not wired in `MongoHiringRuntime` and was deleted with its fakes. Migration 029 unsets `event` from existing work rows and installs the successor validator.

**Embedding work (MR-12).** The row's kind and entity are the two parts of its `_id` (`Job:<id>`, `CandidateProfile:<id>`; `EmbeddingWorkKey.fromId`). No query, index or coverage join filters on `kind` or `entityId` (the claim index is `state, availableAt, leaseUntil` and the coverage `$lookup` matches `_id`), so migration 030 unsets them. Vectors stay on `users` and `jobs`.

**Outbox batch claim (MR-06).** One page scan, then one transaction per page: read the subject leases still held and drop their events; lease the rest with `updateMany` under one claim token and read back the ones this claim won; read the deleted accounts among their subjects (`users.accountStatus` is the only source) and end those events and unreadable ones as `Failed`; take the subject leases and register the producer generation with one ordered bulk write each. Per event the cost falls with the batch size (see the measurement below). Rules that did not change: at-least-once delivery; a subject has one in-flight event (a later event of a subject in the batch waits for the next claim, `MongoOutboxClaimBatch.select`); the token and event identity fence a stale holder (`markPublished`, `releaseForRetry`, `markFailed` and `renewLease` match `_id`, state `InFlight` and the token, and the subject leases match the event identity as well, so completing one event of a batch releases only its own subjects). A lost race on a subject lease is a typed `Conflict` and the page retries with fewer events up to three times. A stored row without readable attribution, attempt counter or subject list, or whose event cannot be interpreted, is ended alone as `Failed` with `INVALID_EVENT_CONTRACT` (evidence kept, no lease) and never blocks the rest of the batch or the cursor. Fairness is unchanged: a hot subject gets one event per cursor cycle, the others wait for its release. The grant of a subject lease relies on the deletion read of `users.accountStatus` made in the same transaction; its filter also refuses a fence that deletion already marked (a typed `Conflict`), as defence in depth. The subject lease is taken for every claimed event, not only for batches with several events of one subject: analytics' purge gate (`publisherDrainReady`) reads the lease on the fence document to know that no send is in flight, so a lease-free single event would break the erasure protocol. This differs from the "only when a batch holds more than one event for a subject" wording of MR-06 on purpose.

**Measurement (AC-08.4, `OutboxBatchClaimIntegrationSpec`, MongoDB replica-set test container).** 40 events with distinct subjects, one `claim(limit = 40)`: the claim issued 10 commands, 0.25 per event: five `find` (claim cursor, page scan, held subject leases, read-back of the leased events, deleted accounts), four `update` (lease the events, subject leases in one bulk write, producer registrations in one bulk write, claim cursor checkpoint) and one `commitTransaction`. Before: the per-row claim issued, per event with one subject, the claim `findAndModify`, the deletion read, the subject-lease `findAndModify`, the producer-registration `update` and the commit (5 commands, more with several subjects), plus three per page (cursor read, page scan, cursor write): about 203 commands for the same 40 events. The "before" figure is derived from the removed code, not re-measured on the old binary.

**Shared lease helpers (FD-03, FD-05).** `MongoLeaseQueue.claimReady` is the claim of the embedding and search-session work queues (states, sort and decoder are the only differences). `LeaseRequest.validate` is the one check of a claim or renewal (worker 1 to 128 characters, lease ending after it starts); an invalid request is `InvalidEvent` on every queue. `LeaseHeartbeat.guard` renews every third of the lease while work runs and answers `Left(LeaseLost)` when a renewal reports a loss; the interview publisher, the embedding pipeline and the Kafka publisher map it to their existing outcomes.

**Ledger steps 027 to 030.** Each is a restartable batched scan (500 rows, `_id` order, checkpoint after every page) with a conditional write per page, a rescan of the whole collection once if rows remain, a fail-closed check for rows that do not look like the retired shape, an audit against the successor validator before installing it and a read-only proof: successor validator installed exactly and no retired row (otherwise the step reopens).

| Step | Retires | Also |
|---|---|---|
| `027_interview_hiring_receipt_removal` | inbox documents `documentType: "hiringReceipt"` (ids `<workflowId>:hiring`) | drops `interview_workflow_inbox_hiring_subject_ids`; inbox validator without the hiring branch |
| `028_interview_request_receipt_removal` | workflow-collection documents `documentType: "requestReceipt"` (ids `request:<actor>:<key>`) | drops `interview_workflows_request_workflow`; workflows validator without the request branch; 019's proof accepts 028 |
| `029_search_session_work_single_copy` | field `event` of `search_session_work` | successor validator |
| `030_embedding_work_key_from_identity` | fields `kind`, `entityId` of `embedding_work` | successor validator |

Migration 025 installs and audits its own generation of the four validators (`MongoOperationalValidators.installedBy025`); its completed proof accepts that generation or the current one (`all`) per collection, so a database that completed 025 starts, runs 027 to 030 and ends at the current definitions. Stop-writer precondition: stop every writer running an earlier binary first; it still stores the retired shapes (the reopened proof repairs a late write on the next start, and a stored hiring or request receipt written after the step is removed by the rerun). Recovery is forward-only; application rollback does not restore the removed data or the removed indexes. Request receipts held by an in-flight request when the step runs are lost: the retried request finds no receipt and is judged by the guarded write itself (a repeated schedule meets the unique `applicationId` index, a repeated lifecycle request meets the revision guard).

## Durable work age for readiness

`/ready` reports the age of waiting durable work (see [API readiness](api.md#readiness)). Both reads are one document with `limit(1)`, a one-field projection and `maxTimeMS` 500, so they stay constant-cost. Oldest due outbox entry: filter `state = Retryable` and `availableAt <= now`, sort `availableAt`; the age is `now - occurredAt`. The existing `event_outbox_claim` index (`state`, `availableAt`, `leaseUntil`, `occurredAt`, `_id`) covers it (there is no separate partial index): local explain on 350 entries shows `IXSCAN` on `event_outbox_claim`, no `SORT`, 1 key and 0 documents examined (`MongoPendingWorkAgeIntegrationSpec`). Entries leased in flight and entries waiting out a retry delay are not counted. Known limitation: a publisher whose sends keep failing still beats its heartbeat, because a failed send is absorbed (`releaseForRetry`, then `markFailed` after the maximum attempts), and `Failed` outbox rows appear nowhere in readiness. A broker outage therefore surfaces as `outbox-age` only while rows are still due, and exhausted rows go unreported. Claim-time `SUBJECT_DELETED` suppression also marks rows `Failed`, so a plain "Failed rows exist" component would be noisy and was not added; a follow-up could report the rate of `Failed` rows that were not suppressed. Oldest pending closure: `requestedAt` ascending on `recruiter_job_closure_work`; the collection deliberately has no index (neither `requestedAt` nor `(notBefore, requestedAt, _id)`), so the read is a `COLLSCAN` with one document examined while the collection holds one row (the worker peek, which filters `notBefore <= now` and sorts `(notBefore, requestedAt, _id)`, has the same shape with an in-memory sort). A row exists only for a deleted recruiter with more than one page (1000) of open jobs whose closure has not finished, and is deleted on completion, so the collection is bounded by the number of such deletions in flight (expected to be a handful). An index would cost a write on every enqueue, page and retry rotation for a scan over a few documents; revisit with measured explain output if the pending count ever reaches the thousands. Alert on `outbox-age` and `closure-work-age` (a stuck first row blocks later closures, see the head-of-line note under the closure section) as well as on the worker names.

## Deleted recruiter job closure

Account deletion closes at most one page (1000, `recruiter-job-closure.page-size`) of the recruiter's open jobs inside its transaction. If open jobs remain, the same transaction upserts one row in `recruiter_job_closure_work` (`_id` = recruiter id, `requestedAt`, `notBefore`, `attempts`; strict validator, migration `020_recruiter_job_closure_work`). A background worker then closes the rest, one transaction per page: job replacements, exactly one `JOB_CLOSED` outbox event per job (event id derived from job and deletion time) and, on the last page, removal of the row commit together. A crash or abort leaves the previous state, so resuming never duplicates an event. The read follows `jobs_recruiter_status_created_id` (explain on 2500 open jobs: `LIMIT`, `FETCH`, `IXSCAN`, 1000 keys and documents examined for a 1000-document page, 1 for the "jobs remain" check and for each submit-gate read).

**Poison rows.** The head is the earliest eligible row by `(notBefore, requestedAt, _id)`, peeked without a transaction so an idle worker opens no session. When a page fails (for example a malformed job), the transaction rolls back and only that row gets `attempts + 1` and `notBefore` pushed out by a capped doubling delay (`row-retry-*`). The row is retried forever and never dropped, later recruiters are served meanwhile, and the deletion receipt stays `PENDING` until its row is gone. The log carries the attempt count, never a user id.

**Submission window.** A deleted recruiter's jobs stay `Open` until the follow-up reaches them. A submission transaction rejects a job whose recruiter is not an active account (Conflict), so after the deletion commits no new application is accepted. The recruiter's user document is read, not written (fencing it would serialize every submission to that recruiter's jobs), so under snapshot isolation a submission that read the recruiter as active before the deletion committed, for a job beyond the first page, can still commit afterwards. It is equivalent to a submission serialized just before the deletion: the application exists on a job that the follow-up then closes. Jobs in the first page conflict on the job document itself and retry.

**Operations.**

- The interview publisher worker is not restarted by the closure worker or by cleanup supervision; if it has died, restart the application process.
- Re-enqueue is a manual repair, for a deleted recruiter whose open jobs have no work row (for example a lost row). Find them with a full scan, in maintenance only:

  ```js
  db.jobs.aggregate([
    {$match: {status: "Open"}},
    {$lookup: {from: "users", localField: "recruiterId", foreignField: "_id", as: "r"}},
    {$match: {"r.accountStatus": "Deleted"}},
    {$group: {_id: "$recruiterId", open: {$sum: 1}}}
  ])
  ```

  Then, for each `_id`:

  ```js
  db.recruiter_job_closure_work.updateOne(
    {_id: "<recruiter-uuid>"},
    {$setOnInsert: {requestedAt: new Date(), notBefore: new Date(), attempts: 0}},
    {upsert: true}
  )
  ```

  To retry a backed-off row immediately, set `attempts: 0` and `notBefore: new Date()` on it with `$set`.
- A local database that ran an earlier draft of migration 020 (before the required retry fields) fails closed with a validator mismatch: drop `recruiter_job_closure_work` and the `020_recruiter_job_closure_work` row of `hiring_migration_ledger`, then restart. Nothing was released with that draft.
