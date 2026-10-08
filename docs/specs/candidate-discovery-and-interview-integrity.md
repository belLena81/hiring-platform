# Candidate discovery and interview integrity

## Outcome and contracts

Status: locally complete. Implements the user-approved local audit remediation for discovery and durable interview workflows. Preserve public GraphQL/event shapes, pure State/StateT decisions, typed IO/EitherT ports, provider reconciliation, Mongo transactions, fencing and Resource ownership. No ranking adoption, real provider, paid provisioning, deployment, commit or existing-data reset is in scope.

Baseline findings: cleanup selected bounded attribution but deleted unselected parents; Kafka rejection identities could exceed Mongo's 256-character limit; residence validation permitted an absent canonical city. Existing migration 002, geographic and broker-outage evidence omitted selected failure cases. Source/build remain authoritative; historical passes do not certify this implementation. Apply [engineering quality](../engineering-quality.md) and [schema evolution](../schema-evolution.md).

## Acceptance and evidence

| ID | Observable acceptance | Evidence |
|---|---|---|
| CDI-01 | Cleanup preserves unselected attribution, removes linked children before parents, converges across more than 128 receipt-only/provider-only workflows and interruption, and preserves unrelated subjects. | PASS: focused real Mongo cleanup 23/23, including three new regressions. |
| CDI-02 | Transport quarantine identities are deterministic and bounded for valid 249-character topics; real Mongo quarantine precedes acknowledgment, replay deduplicates, and later valid work progresses. | PASS: two units and dedicated maximum-topic Kafka/Mongo quarantine regression. |
| CDI-03 | Migration 016 audits residence integrity in bounded restartable batches, including completed-002 databases; valid absent/paired cities pass, malformed/mismatched values fail without data/revision changes; strict collection validation prevents unpaired new writes. | PASS: four pure units and relevant cases in the 10/10 real Mongo migration suite. |
| CDI-04 | Unchanged migration 002 has runtime interrupted-batch, malformed residence/availability/consent, repair/restart and absent-consent evidence. | PASS: three 501-record malformed-field repair/restart cases in the migration suite. |
| CDI-05 | Real Mongo nearby/facets agree for antimeridian, poles and negative longitude; zero-distance ties page without gaps/duplicates; radius boundary behavior is observed within the agreed one-metre tolerance. | PASS: geographic discovery 9/9, including four new edge cases. |
| CDI-06 | A dedicated disposable broker process outage leaves Mongo hiring acceptance available and durable work converges after broker restart without duplicated effects. | PASS: final formatted outage case 1/1, including message/request replay and exact workflow/receipt/provider/history counts. |
| CDI-07 | Focused regressions, full root units/integrations, formatting and independent Code/Security/final QA pass on the final task state. | PASS: units 707/707, integrations 180/180 executed, nine explicit skips; formatting and independent Code/Security/final QA PASS. |

## Migration and recovery

Use next unused operational identity `016_candidate_residence_integrity`; preserve applied migration 002. Install paired-presence validation before auditing existing residences. Validate canonical values with the application's Locale.ROOT normalization, preserve data/revisions, and fail closed with sanitized errors. Checkpoints advance only after successful bounded batches and acknowledged writes. Completed proof and installed validator must be verified before skipping the audit. Stop incompatible writers before cutover; malformed existing data requires explicit repair and restart. Application rollback does not relax installed constraints or constitute data rollback.

## Handoff and checkpoint

Coordinator owns this spec, geographic tests and canonical documentation. Exclusive owners: cleanup adapter/tests; residence migration/validator/setup and migration tests; Kafka quarantine and dedicated outage tests. All sbt checks are serialized by the coordinator using isolated test services/builds. Agents preserve existing edits in MongoHiringMigrations.scala and MongoRepositorySupport.scala. Synthetic fixtures and logs stay ignored under .local/; existing durable proof/lakehouse data remains untouched.

Baseline audit: 701 units and 161 integrations passed; nine cases skipped; formatting passed. These results precede implementation. Atlas, human relevance, real providers, deployed retention/SLO and opt-in optimization acceptance remain separate gates.

Historical regression checkpoint: the first integration compilation failed on two unused imports; these were removed before claiming behavioral evidence. The rerun executed 24 cases: 20 passed and four intended regressions failed. Missing canonical city still passed startup after Complete002; the three cleanup cases left linked commands/inbox rows after apparent completion. The quarantine unit red executed two cases: one passed and maximum-topic identity bounds failed (371 characters versus the intended 74). Selected-ID cleanup was implemented at that checkpoint; other fixes and coverage were still proceeding. No completion was claimed at that point.

Historical focused checkpoint: transport quarantine (2 cases) and residence integrity (4 cases) passed with the implemented fixes. The real Mongo focused run passed 42/42 cases: residence/migration recovery 10, subject cleanup 23 and geographic discovery 9. Exact boundary observation was 1.1131884502140803 km with one match. Commands used the private build root `.local/data/test-builds/discovery-integrity-red`: `testOnly *InterviewTransportQuarantineSpec *CandidateResidenceIntegritySpec` and `IntegrationTest / testOnly *MongoCandidateResidenceIntegrityIntegrationSpec *MongoInterviewSubjectCleanupIntegrationSpec *MongoJobDiscoveryIntegrationSpec`. Kafka and final full-suite evidence were pending at that checkpoint.

Broker fixture checkpoint: initial execution exposed string-command tokenization; the supervisor now uses the single-argument varargs overload. The next outage run exposed broker self-RPC routing through a host-mapped address; an authenticated internal localhost listener corrects that fixture error. Recovery then completed, and the final count assertion was corrected to distinguish a workflow from its request receipt in their shared collection. The final formatted outage regression passed 1/1 in 16 seconds; maximum-topic quarantine passed. `scalafmtCheckAll` and `scalafmtSbtCheck` passed. These failed fixture runs are superseded for the affected criteria, not counted as passes.

Final full-suite wrapper `bash scripts/run-local-tests.sh test` exited 0 on October 8: units 707/707 passed; integrations 180/180 executed passed, with nine skips, in 566 seconds. The final 32-interview actual worker/consumer/publisher restart case passed in 158 seconds. Its log is `.local/logs/candidate-discovery-integrity-final.log`. The initial sandbox Docker-socket denial was resolved by approval before this run. Skips are four Atlas cases, one analytics-report Compose gate, one publication-scaling measurement and three persistence-scaling measurements. These are unverified, not passes. A separate complete unit run also passed 707/707. No performance or deployment acceptance is inferred.

Final formatting rerun `sbt -Dhiring.test.buildRoot=.local/data/test-builds/discovery-integrity-red scalafmtCheckAll scalafmtSbtCheck` exited 0 on October 8 at 07:49:03; log `.local/logs/candidate-discovery-integrity-format.log`. An initial read-only sbt boot-lock failure was an environment error, resolved by approval before that run. Final `git diff --check` passed; the index is empty and generated output remains ignored. The two unrelated pre-existing Scala edits remain preserved. No commit or deployment performed.

Code Reviewer: PASS refreshed on final frozen source. Security Engineer: PASS refreshed on final frozen source. Final independent QA: PASS after inspecting actual full-suite results, criterion mapping, final source and formatting evidence.
