# Hiring Workflow Recovery and Discovery Validation

## Authorized scope

Status: complete for the authorized local refactoring. The user approved the functional refactoring plan on October 6, 2026.

Keep immutable capability policies, concrete Cats Effect IO and typed EitherT ports. Preserve hiring status/history transactions, stable provider idempotency keys, public GraphQL fields/errors, discovery cursor wire shape and the seven-field operational event envelope. Existing Phase 9 relevance, Atlas, deployed retention and production activation gates remain independent.

The clean source baseline is ecddf7d. The preceding read-only review ran 533 unit tests and both configured formatting checks successfully; that evidence does not verify this implementation. Source review found publication exhaustion/applicability mismatch, execution budget gaps, elapsed-time cleanup, missing cleanup CAS, bypassable cursor validation, permissive facet decoding and adapter-owned repair decisions.

## Acceptance criteria

| ID | Required observable behavior | Verification | Current evidence |
|---|---|---|---|
| HWR-01 | Obsolete committed results terminate without further sends or reversing hiring; required-result publication exhaustion enters repair; failed terminal persistence cannot cause extra sends. | Pure policy and worker/Mongo regressions. | PASS: 15 domain tests and 16 workflow Mongo regressions include obsolete hiring results, required stored results and failed terminal persistence. |
| HWR-02 | Durable execution budgets cover interruptions; expired execution reconciles with stable keys; concurrent claims cannot overdraw a capability/recipient budget. | Pure policy, real Mongo claim/restart tests. | PASS: real concurrent capability claims, prepaid slot and interrupted execution regressions in 16 Mongo tests plus 4 recovery tests. |
| HWR-03 | Repair, publication disposition and advancement causes are typed pure decisions; all retained commands have coherent interpretation. | Domain tests and independent source review. | PASS: typed policy/cause tests, 5 command migration tests and independent Code Reviewer/Security source verdicts. |
| HWR-04 | Initialized producer generations register transactionally before sending; deletion captures and broker-fences all attributable generations before barrier capture. | Paused sender, open transaction, registration race, generation replacement and scoped ACL tests against disposable Kafka/Mongo. | PASS: 4 actual Kafka/Mongo proofs; both publisher roles and multiple paused generations, open transaction invisibility, deletion race, replacement and scoped ACL denials. Final rerun 4/4. |
| HWR-05 | Cleanup uses typed states/barriers and expected-state/revision CAS; cancellation/restart and concurrent cleaners cannot regress progress or lose proof. | Pure policy, real Mongo cleanup/restart tests. | PASS: 8 cleanup policy/worker tests and 11 real Mongo cleanup tests, including cancellation, restart and competing cleaners. |
| HWR-06 | Operational migrations preserve records/budgets/tombstones, resume safely and reject malformed state before worker startup. | Disposable repeat/concurrent migration tests. | PASS: real repeated/concurrent migrations, completed-ledger validation, malformed typed rows/ledgers and mixed-type IDs beyond a batch boundary; records remain preserved. |
| HWR-07 | Direct discovery and GraphQL share pure bounded normalization and cursor validation; invalid inputs never invoke discovery storage. | Unit, recording-port and executable GraphQL tests. | PASS: 24 discovery tests include recorded storage calls and actual GraphQL execution, plus 4 real Mongo discovery tests. |
| HWR-08 | Facets retain exact valid counts/order/truncation and fail closed on malformed aggregation data. | Decoder and real Mongo discovery tests. | PASS: 4 strict decoder tests and 4 real Mongo discovery tests retain counts, ordering and truncation. |
| HWR-09 | Final source passes local checks and independent Code Reviewer, Security and QA gates. | Java 17 test/integration/format plus separate verdicts. | PASS: 571 units, 56 selected integrations, final fencing rerun 4/4, configured formatting and integration compilation; independent Code Reviewer, Security Engineer and final QA PASS across scopes excluding each author’s own code. |

## Contracts and implementation ownership

- Workflow owner: pure workflow decisions, typed advancement cause, independent publication/execution accounting, participant-aware notification lookup, worker and Mongo workflow adapter; migration 010 helper and affected tests.
- Cleanup owner: pure cleanup states, typed barrier/fencer ports, Mongo cleanup CAS/snapshots, root-build broker fencer; migration 011 helper and affected tests.
- Discovery owner: shared validation, typed cursor identity/errors, strict facet decoding and executable resolver coverage.
- Coordinator: producer-generation contract, transactional Kafka/runtime composition, typed credentials/ACL fixtures, migration runner, canonical documents and final verification.

One owner per file; no shared-file formatting during concurrent edits. Reviews are independent of authors.

## Recovery and cutover

Execution slot migration conservatively preserves existing command-count budgets; prepaid queued commands use their slot once. Unknown executions reconcile. Only guarded Admin repair resets an epoch. Existing provider/hiring receipts remain authoritative.

Interview producer generations register in a sibling subject-fence field, leaving operational publisher identities and analytics fencing unchanged. Replacement generations initialize before fresh authorization. Cleanup proceeds Pending -> ProducersFenced -> MongoPurged -> AwaitingRetention -> Complete, with broker-confirmed fencing and physical topic retention.

Old nontransactional publishers must be stopped and lose topic-write credentials at maintenance cutover before the new protocol is activated. Migrations 010/011 gate new workers and restart legacy cleanup proof using fresh barriers; no mixed writers, reset, production migration, service restart or deployment is performed by this local implementation. Broker/config changes are prepared and exercised only in disposable test infrastructure.

## Evidence and next action

Final functional evidence on October 6, 2026:

- Java 17 `sbt scalafmtAll scalafmtSbt scalafmtCheckAll scalafmtSbtCheck test 'IntegrationTest / compile'`: 571/571 unit tests, zero ignored, formatting and compilation PASS. Log: `.local/logs/workflow-recovery-final-unit.log`.
- Fencing/workflow/recovery selection: 24/24 integrations, zero ignored (4 Kafka/Mongo fencing, 16 workflow Mongo, 4 recovery). Log: `.local/logs/interview-publication-fencing.log`.
- Cleanup/discovery/embedding/lifecycle/Kafka restart/worker selection: 32/32 integrations, zero ignored (11 cleanup, 4 discovery, 4 embedding, 10 transactional lifecycle, 2 Kafka restart, one 32-interview/two-worker/two-restart drill). Log: `.local/logs/workflow-recovery-final-integration.log`.
- Final formatting and affected fencing rerun after aligning the standard fixture path and clarifying the retention comment: 4/4 PASS, zero ignored. Log: `.local/logs/workflow-recovery-final-fencing.log`.
- Accelerated physical retention: command beginning offset 280 reached barrier 280; result beginning offset 206 reached barrier 206. Script exit 0. Log: `.local/logs/workflow-recovery-physical-retention.log`.
- Independent Code Reviewer, Security Engineer and final QA verdicts: PASS across separate scopes excluding each author's own files; combined scopes cover the complete implementation.

The disposable Kafka project `hiring-workflow-recovery-proof` has scoped ACLs and no normal data volumes. Existing services and normal data remain untouched. Docker default address pools were exhausted, so the proof uses an inspected unused, configurable subnet. The accelerated physical-retention proof passed; it does not establish seven elapsed production days or deployed erasure acceptance. Logs and generated evidence belong under ignored `.local/logs` and `.local/data`. Workload is bounded local fixtures and disposable services; no paid infrastructure, relevance adoption or performance/SLO claim.

The disposable broker is removed after testing; generated credentials/logs remain ignored. `git diff --check` passed and no generated files are staged. No commit, deployment, real provider activation or deployed migration was performed. Deployed activation still requires the documented maintenance cutover, migrations and target-environment acceptance.
