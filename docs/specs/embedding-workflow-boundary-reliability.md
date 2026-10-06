# Embedding and Interview Workflow Boundary Reliability

## Authorized outcome

Status: complete for the approved local functional cleanup and policy refactoring. Historical BWR-01 through BWR-05 evidence below belongs to the earlier local implementation; it does not certify BWR-06 through BWR-11. The user approved the follow-up plan on October 6, 2026.

Retain pure embedding preparation, aggregate lifecycle programs and capability-specific Saga decisions. Correct provider vector validation, isolate subject cleanup failures, connect worker diagnostics and reuse the application lifecycle during interview commits. Preserve public GraphQL/event contracts, typed IO/EitherT ports, stored schemas, transaction/fencing boundaries and provider idempotency keys. No dependency, migration, deployment, real provider activation or ranking adoption is authorized.

Review baseline facts: Voyage response validation checked dimension only; configured Circe Float decoding accepts null as NaN and overflowing numeric values as infinity. Cleanup's EitherT batch traversal stopped at its first subject error. Runtime worker construction omitted diagnostics and selected noop. The interview commit duplicated Accepted-to-Interview aggregate/history construction instead of using ApplicationLifecycle.changeStatus. Existing recovery improvements remain present; see [workflow recovery](hiring-workflow-recovery.md).

## Acceptance criteria

| ID | Observable behavior | Required verification | Evidence |
|---|---|---|---|
| BWR-01 | Non-finite response vectors return InvalidResponse; valid finite vectors retain existing behavior and invalid responses do not persist embeddings. | Raw JSON null/positive and negative overflow cases and worker persistence regression. | PASS: provider red gate reproduced 3 invalid successes; focused green 30/30. Final unit suite 577/577. |
| BWR-02 | A failed cleanup subject remains pending while subsequent healthy subjects in the selected batch advance; the first error is returned after the batch and failed subjects retry on the next poll. | Multi-subject worker tests, existing cancellation/fencing tests and real Mongo cleanup checks. | Focused cleanup 6/6 PASS, including typed/unexpected failures, first-error precedence, retry and cancellation; real Mongo cleanup 11/11 PASS. |
| BWR-03 | Runtime worker diagnostics are explicitly supplied; a claim failure emits the existing sanitized diagnostic. | Required constructor argument, recording diagnostics regression and integration compilation. | PASS: required diagnostics at all callers; recording publication claim test passed. Final integration compilation PASS. |
| BWR-04 | Interview status/history/outbox values derive from the existing pure application lifecycle; replay remains idempotent and authorization/fencing/receipt/notification writes remain atomic. | Lifecycle parity/replay regression and real Mongo workflow/recovery checks. | PASS: independent source review, 16 workflow Mongo cases including lifecycle parity/replay and 4 recovery cases. |
| BWR-05 | Final source passes local unit/format/integration gates and independent Code Reviewer, Security and final QA reviews. | Configured root checks and relevant Mongo/Kafka recovery integrations. | Final unit suite 577/577; formatting PASS. Independent Code Reviewer and Security PASS across separate scopes excluding each author. All 52 selected Mongo/Kafka integrations PASS, zero ignored; independent final QA PASS for BWR-01 through BWR-05. |
| BWR-06 | A bounded subject-ID sweep visits later pending cleanup even when earlier subjects fail or await retention; restart and competing workers preserve durable guards. | Real Mongo 33-subject regressions, page boundaries, insertions, cancellation and restart checks. | PASS: final cleanup worker 7/7, real Mongo cleanup 20/20 and recovery 4/4; full integration includes final mixed-key boundary and Kafka worker restart. |
| BWR-07 | Per-row decode failures remain incomplete and diagnosed while healthy subjects advance; corrupt ordering identities cannot abort selection. | Mixed BSON identities/timestamps/payloads and healthy-row integration; page-read failure and sanitized diagnostics tests. | PASS: behavioral red reproduced both defects; final cleanup 20/20 and managed failure cursor unit passed, including dollar-prefixed continuation boundary. |
| BWR-08 | Pure application message policy preserves command/result/expired admission, mappings, acknowledgment ordering and notification revisions. | Characterization before extraction, pure policy cases and workflow/replay regressions. | Pre-extraction characterization 28/28 PASS; post-extraction focused 42/42 includes 11 worker and 7 pure policy cases; recovery 4/4 PASS. |
| BWR-09 | Nearby criteria encoding moves into the cursor codec with identical hashes and cursor bytes. | Golden cursor/hash fixtures, normalized/delimiter-bearing criteria and existing cursors. | Post-extraction discovery 11/11 PASS, including exact cursor/hash fixtures. |
| BWR-10 | Named rank/facet values replace positional access without changing scores, tie ordering, counts or truncation. | Branch/tie/empty/truncation characterization and discovery/ranking tests. | Post-extraction rank 6/6 and discovery 11/11 PASS. |
| BWR-11 | Final follow-up passes local checks and independent Code Reviewer, Security and final QA verdicts. | Java 17 root unit/format checks, integrations and applicable Kafka replay/recovery checks. | 601/601 units and format PASS; full integration exit 0, 115 executed successes, four Atlas skipped and two Compose disabled. Independent Code Reviewer, Security Engineer and final QA: PASS. |

## Approved functional refactoring

The user selected subject-ID ordering through the existing `_id` index, with no schema/index migration. Sweep tokens are immutable service-owned values; only the Mongo adapter encodes/decodes BSON keys. A sweep captures its greatest eligible raw `_id` and start time, returns at most 32 rows per poll, and wraps after exhaustion. Scalar Date request times newer than that start wait for the next sweep; malformed timestamps remain visible as decode failures. Mixed BSON keys use aggregation comparisons with literal bounds. Selection uses raw ordering keys independently of domain decoding; corrupt evidence is never promoted to completion.

The cleanup worker exposes `runOnce(cursor)` returning next scheduling state plus the first typed subject failure. A sequential resource-owned stream threads this immutable state. Subject/decode failures advance selection after the page is observed; page-read failure retains the previous cursor. Cancellation/restart can repeat effects under existing idempotency, revision CAS and permanent fences. The worker clock is injected. Cursor state is scheduling only, not durable cleanup evidence.

Interview message admission/mappings move into pure application functions because their inputs are service-owned ports. Preserve the distinct command/result/expired rules, timely existing-result acknowledgment ordering, stored-result equality and outstanding notification revisions. Search extraction preserves exact canonical fingerprint bytes, floating-point addition order and deterministic ranking/facet outputs.

Implementation owners: cleanup port/adapter/worker and related tests; message policy/worker and specification; search codec/rank/facet values and characterization. All SBT commands are serialized by the coordinator. Independent reviewers do not author the inspected changes. Generated verification output stays in ignored `.local/logs/` and disposable fixture data in `.local/data/`.

## Implementation decisions

- Extend the existing pure provider validator with finite-value validation and retain InvalidResponse and the current retry owner.
- Interpret cleanup outcomes per subject at the worker boundary, sequentially, logging failures without weakening each subject's required ordering. Return the first typed failure after the batch. Preserve cancellation.
- Make workflow diagnostics a required internal constructor argument. Runtime passes its configured sink; tests explicitly choose noop or recording diagnostics.
- Decode the selected application inside the existing transaction and run ApplicationLifecycle.changeStatus with Interview, initiating actor, supplied time and absent feedback/reason. Map lifecycle rejection to RepositoryError.Conflict. Persist the returned aggregate and transition values with the same deterministic event identity.

## Evidence and handoff

Before implementation, sbt scalafmtCheckAll scalafmtSbtCheck test passed 571 units and both format checks. That baseline does not validate these changes. Files are assigned to exclusive implementation owners; review must exclude each author's changes. Final QA follows Code Reviewer and Security verdicts.

Local fixtures remain bounded and disposable. MongoDB integration tests require a replica set; Kafka recovery tests require disposable broker infrastructure. Blocked infrastructure is unverified. Atlas, real-provider guarantees, deployed retention and existing analytics activation gates remain separate.

Historical BWR-02 isolated failures within the selected oldest-32 batch. BWR-06/BWR-07 now address the separately reviewed selection-fairness and batch-decoding gaps; their follow-up evidence must be recorded independently below.

Implementation checkpoint: finite response validator, selected-batch failure isolation, explicit diagnostics and pure lifecycle reuse are implemented. Focused failures were reproduced before corrections. Coordinator final unit/format log: `.local/logs/embedding-workflow-unit.log`. No commit or deployment performed.

Reproducible final root checks (Java 17):

```bash
sbt scalafmtAll scalafmtSbt scalafmtCheckAll scalafmtSbtCheck test 'IntegrationTest / compile'
```

The integration selection uses disposable Mongo replica sets and an isolated SASL Kafka broker started by `scripts/run-interview-kafka-proof.sh start` with `INTERVIEW_KAFKA_PROOF_ROOT=.local/data/embedding-workflow-proof`, `INTERVIEW_KAFKA_PROOF_PROJECT=hiring-embedding-workflow-proof` and an unused explicit test subnet. Export the ignored fixture's `runtime.env` and its proof-root selector, then run:

```bash
sbt 'IntegrationTest / testOnly com.example.graphQL.cats.repository.mongo.MongoInterviewWorkflowRepositoryIntegrationSpec com.example.graphQL.cats.repository.mongo.InterviewWorkflowRecoveryIntegrationSpec com.example.graphQL.cats.repository.mongo.MongoInterviewSubjectCleanupIntegrationSpec com.example.graphQL.cats.repository.mongo.MongoEmbeddingWorkIntegrationSpec com.example.graphQL.cats.repository.mongo.MongoHiringRepositoryTransactionIntegrationSpec com.example.graphQL.cats.infrastructure.kafka.InterviewKafkaRestartIntegrationSpec com.example.graphQL.cats.repository.mongo.InterviewPublicationFencingIntegrationSpec com.example.graphQL.cats.repository.mongo.InterviewSchedulingWorkerIntegrationSpec'
```

Integration output: `.local/logs/embedding-workflow-integration.log`. Credentials and generated fixture data remain ignored; normal Kafka/Mongo data is not mounted into the fixture.

Final local execution on October 6, 2026: Java 17 root command exited 0 with 577/577 units, both formatting checks and integration compilation PASS. The selected integration command exited 0 with 52/52, zero ignored: 16 workflow Mongo, 4 recovery, 11 cleanup, 4 embedding, 10 transactional hiring, 2 Kafka restart, 4 publication fencing and one 32-interview/two-worker/restart drill. Intentional ProducerFencedException observations belong to successful fencing tests. These bounded local executions make no latency/SLO, live Atlas, real provider or deployed retention claim.

Independent review coverage: Code Reviewer PASS for lifecycle/cleanup/diagnostics/docs from the embedding author and embedding/cleanup/diagnostics/docs from the lifecycle author. Security PASS for embedding/cleanup/diagnostics/docs from the lifecycle author and lifecycle/embedding/docs from the cleanup author. No author approved their own changes. Independent QA is a separate final gate.

Independent final QA: PASS for BWR-01 through BWR-05 after inspecting final formatted source, regressions, root unit/format/compile log and all selected integration outcomes. No required defects. Final git diff --check passed; no generated files are staged.

The disposable broker containers and network were removed after successful execution. Generated credentials/logs remain ignored; existing services and normal data were preserved.

## Functional refactoring execution evidence

The follow-up reproduced both cleanup defects against real MongoDB before implementation: blocked oldest subjects starved the 33rd subject, and malformed records aborted healthy subject processing. The red log is `.local/logs/functional-cleanup-red.log`. Pre-extraction worker/search characterization passed 28/28 (`functional-policy-characterization.log`). After extraction, focused checks passed 42/42 units and 24/24 Mongo cleanup/recovery cases (`functional-refactor-focused.log`). The final mixed-BSON case was subsequently strengthened to put a dollar-prefixed stored key at the continuation boundary; the full integration run passed that final test revision.

Final Java 17 unit/format command:

```bash
sbt -java-home /usr/lib/jvm/java-17-openjdk-amd64 scalafmtAll scalafmtSbt scalafmtCheckAll scalafmtSbtCheck test
```

Result: PASS, 601/601 units and both formatting checks (`.local/logs/functional-refactor-unit.log`). Independent Code Reviewer and Security Engineer each returned PASS for the final source and strengthened test, separately from final QA.

Full local integration command uses the isolated broker at `.local/data/functional-workflow-proof`, project `hiring-functional-workflow-proof`, with its ignored runtime environment exported and `INTERVIEW_KAFKA_PROOF_ROOT` set; unset `ATLAS_TEST_URI` for this local-only run:

```bash
sbt -java-home /usr/lib/jvm/java-17-openjdk-amd64 'IntegrationTest / test'
```

Full integration exited 0: reported 121 total, 117 passed and four Atlas skipped. Two older opt-in operational Compose cases returned without executing their disabled bodies, leaving 115 executed successes. This includes 20 cleanup, four recovery, two Kafka restart, four publication fencing and the 32-interview/two-worker/restart drill. Independent final QA PASS for BWR-06 through BWR-11 after final source, criteria, logs and snapshot audit. Atlas/live provider/deployed acceptance remains separate. No commit, deployment, dependency or stored-schema change.

Closure: independent final QA confirmed the final tests and reviewed scope against the saved baseline. The initial repository/runtime fixes remain preserved; changed baseline files are confined to authorized paths. The disposable broker containers/network were removed; the two pre-existing analytics workers remain running. Final diff whitespace check passed, no files are staged, and logs/generated credentials remain ignored. These bounded local checks establish neither deployed activation nor latency/SLO acceptance.
