# Analytics erasure safety

Status: scoped repair accepted; operational rollout gates remain open

## Outcome and constraints

Account erasure and analytics publication retain exclusive writer ownership until driver work has stopped, reject malformed deletion markers, and cannot overlook abandoned temporary Delta copies. This implements the September 30 review findings. Preserve public GraphQL/event contracts, persisted schemas, configured retention horizons, existing proof volumes, and concurrent streaming edits. No new dependencies, data migration, generic workflow engine, or operational key retirement is in scope.

The existing domain/service/adapter architecture remains. Internal analytics ports use `F[_]` and Cats Effect capabilities; the application entrypoints select `IO`. Driver mutation remains private to the infrastructure boundary and must be allocated per effect evaluation.

## Acceptance

| ID | Required behavior | Verification |
| --- | --- | --- |
| AES-01 | Every Spark effect evaluation reads the current attachment and owns fresh job state; repetition, retry, and concurrent evaluation are independent. | Executor regressions including construction before context attachment. |
| AES-02 | Cancellation of running driver work waits for worker cleanup before releasing its surrounding resources. Queued cancellation prevents work from starting without waiting for unrelated work. | Controlled interruption-resistant worker, lock finalizer/second-owner assertions, queued cancellation, existing Spark cancellation tests. |
| AES-03 | Under the lakehouse mutex, bounded recovery removes only recognized root-contained temporary rewrite directories and verifies absence. Incomplete, unsafe, or excessive discovery fails closed before processing/completion; read-only retirement audit rejects leftovers. | Restart-style abandoned-copy recovery, failure/overflow/unsafe-path tests, preservation of unrelated paths, maintenance wiring checks. |
| AES-04 | Unknown, missing, null, and wrong-type marker states reach validation and fail before HMAC/report/Delta changes. Only valid expired completed markers are omitted. | Real Mongo marker integration and unchanged expired-marker/overflow cases. |
| AES-05 | Retention tests independently cover data/log deadlines before, at, and after equality, UTC rollover, and Kafka offsets below/equal/above its barrier. | Simulated-clock worker tests against production gate. |
| AES-06 | Disabled opt-in proofs report skipped rather than passed; the internal-import layering rule passes. | Integration reports and architecture suite. |
| AES-07 | Current documentation separates local implementation acceptance from real operational rollout gates and describes implemented effect boundaries. | Independent review of canonical docs and final evidence. |

## Implementation ownership and checks

- Spark executor and resource regressions: Scala implementation agent.
- Temporary Delta recovery and tests: analytics implementation agent.
- Mongo marker query, boundary tests, opt-in reporting, and documentation: coordinator.
- Independent Code Reviewer and Security Engineer verdicts precede final independent QA. Authors do not approve their own code.
- Run Java 17 root and analytics unit suites, analytics integration suites, configured formatting checks, and affected physical/recovery evidence. Serialize SBT invocations. Preserve generated output in ignored locations.

## Historical repair evidence — September 30

The results below certify the scoped repair at that checkpoint. Later local HAL completion is recorded in [Hiring Analytics Lakehouse](hiring-analytics-lakehouse.md#current-local-completion-evidence--october-3); current streaming obligations belong to [Continuous Hiring Analytics](continuous-hiring-analytics.md#current-evidence-summary--october-5). Earlier formatting failures and Phase 6 recommendations below are not current checkout verdicts.

The earlier local smoke verified 43 captured paths absent; this is historical evidence, not a rerun on these changes. The earlier root 425/425, analytics 174/174, and analytics integration 25 passing results likewise do not certify this checkout. The review found a current streaming wildcard-import failure and insufficient isolated retention-edge coverage.

Production activation still requires actual Kafka earliest-offset passage, genuine Delta retention/reclamation, external-writer exclusion, guarded key-retirement authorization and restart, final HAL audit, and operational signoff. This repair does not shorten or waive those gates.

Implementation checkpoint (September 30): AES-01–06 code and regressions are implemented; canonical documentation is aligned for AES-07. The first focused run exposed Hadoop local symlink detection following links; local paths now use NIO `NOFOLLOW_LINKS`, with additional dangling-link and ancestor coverage. Local enumeration uses a closed NIO `DirectoryStream`, because Hadoop's local iterator eagerly allocates a directory array. Discovery charges queued entries immediately; an instrumented nested-tree test enforces the bound plus one overflow sentinel before any deletion. The cancellation regression's finalizer observation is now itself deferred in `IO`; all original ownership assertions remain. Interrupted driver calls are reported through the effect callback instead of escaping the executor thread.

Executed checks:

- Java 17 root `sbt test`: 425 passed, 0 failed.
- Java 17 analytics focused executor/resource/rewrite/retention/layering suites: 33 passed, 0 failed. Log: ignored `.local/logs/analytics-safety/analytics-focused.log`.
- Real-Mongo marker integration: 11 passed, 0 failed, including the five malformed-state cases.
- After the final discovery corrections, isolated-build `DeltaPurgeRewriteSpec`: 9 passed, 0 failed.
- Final scoped analytics unit run: 222 passed, 0 failed, including all nine rewrite recovery tests. Log: ignored `.local/logs/analytics-safety/analytics-verified.log`. Verification uses `analytics/target/erasure-safety-validation` to avoid interference with concurrent builds. The erasure safety sources stayed stable; unrelated streaming sources changed during verification. This evidence does not certify those evolving streaming changes.
- Subsequent full analytics integration run: 24 passed, 0 failed, 7 ignored. Log: ignored `.local/logs/analytics-safety/analytics-integration-verified.log`. The disabled opt-in proofs are explicitly ignored. Earlier transient streaming compilation errors are superseded by this successful integration run.
- Root `scalafmtCheckAll`: failed on 12 unchanged files (one production and 11 test files); `git diff --name-only -- src` was empty. This repository baseline issue remains separate from this repair. No unrelated root sources were reformatted.
- Analytics `scalafmtCheckAll`: one concurrently added streaming test (`StreamingCheckpointReconciliationSpec.scala`) was unformatted; no scoped erasure file was reported. A subsequent standalone analytics `scalafmtSbtCheck` passed.
- Fresh Compose API/worker handoff: 1 passed, 0 failed, 0 ignored (September 30, 21:10:32 local). The nonce-specific image was rebuilt from source; host builds used separate `erasure-compose-validation` targets. XML: `analytics/target/erasure-compose-validation/it-reports/TEST-com.example.hiring.analytics.AccountDeletionComposeIntegrationSpec.xml`; summarized output: ignored `.local/logs/analytics-safety/compose-verified.log`.
- That proof's wrapper exited 1 during cleanup of container-owned Spark scratch files, after the test passed. Cleanup now mounts the already validated nonce directory, covering both lakehouse and scratch files. `bash -n` passed; the corrected guarded cleanup succeeded against the actual leftovers and verified directory absence. The whole wrapper was not rerun after this shell-only correction. Named proof volumes remain preserved.
- Independent Code Reviewer: PASS. Security Engineer: PASS. Final independent QA: PASS for AES-01–07, including the fresh Compose result, actual cleanup directory absence, unchanged scoped source hashes, and the evidence distinctions above. External formatting issues and evolving streaming work remain outside this acceptance. Final `git diff --check` passed; no task files were staged or committed.

Current planning boundary: Phase 6 local implementation subsequently closed on October 3. This repair alone did not establish that closure. Phase 7 local functional acceptance remains open in its own specification; genuine production retention and rollout gates remain prerequisites for production activation, not prerequisites for recognizing the later accepted local Phase 6 completion.
