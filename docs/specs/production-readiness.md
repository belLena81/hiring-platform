# Production Readiness

## Identity, outcome, and scope

- Roadmap: Phase 14 in [development milestones](../development-milestones.md).
- Status: **draft**. Provider, deployment topology, recovery objectives, retention evidence and operational owners remain unresolved.
- Coordinator: Product Manager. Owners: Architect for deployment boundaries, Security Engineer for authorization/secrets/retention, Data Engineer for MongoDB restoration, Big Data Engineer for Kafka/Delta and key retirement, Scala Developer for any scoped runtime changes.
- Outcome: the platform can be operated and restored in an explicitly selected real environment while preserving hiring authorization, durable publication, deletion and analytics ownership.
- Current authorization covers this specification, not deployment, infrastructure purchases, production data access, key removal or a destructive restoration.
- Production prerequisites apply **whenever production streaming is proposed**, including before this roadmap milestone. This document does not grant activation or weaken [lakehouse](hiring-analytics-lakehouse.md), [continuous analytics](continuous-hiring-analytics.md), or [erasure safety](analytics-erasure-safety.md) gates.
- Non-goals: legal/compliance certification, choosing a cloud without requirements, adding tenants/roles, unrelated feature development, a frontend or CI/CD pipeline, blanket backup deletion, or local proofs presented as deployed evidence.

## Source context and existing contracts

| Source | Verified boundary |
|---|---|
| [Analytics architecture](../big-data-architecture.md) | Kafka retention barriers, Delta data/log reclamation, permanent HMAC continuity and shared Mongo mutex are distinct controls; unmanaged writers are outside that mutex. |
| [StreamingActivationGate.scala](../../analytics/src/main/scala/com/example/hiring/analytics/service/streaming/StreamingActivationGate.scala) | Runtime must validate the selected immutable activation grant and its expiry. |
| [HmacKeyRetirementAuthorization.scala](../../analytics/src/main/scala/com/example/hiring/analytics/service/keyretirement/HmacKeyRetirementAuthorization.scala) | Authorization binds exact lakehouse/key/verifier/evidence digest; reads/writes require the shared mutex. |
| [AnalyticsKeyRetirementAuditMain.scala](../../analytics/src/main/scala/com/example/hiring/analytics/cli/AnalyticsKeyRetirementAuditMain.scala) | Production audit entrypoint exists; local proof and authorization entrypoints remain test-scoped. Presence of a test command is not production retirement support. |
| [AnalyticsReportingService.scala](../../src/main/scala/com/example/graphQL/cats/service/AnalyticsReportingService.scala) | Operational analytics access remains an authenticated application service concern. |
| [Retention integration](../../analytics/src/it/scala/com/example/hiring/analytics/AnalyticsRetentionProofIntegrationSpec.scala) and [key-retirement integration](../../analytics/src/it/scala/com/example/hiring/analytics/AnalyticsKeyRetirementIntegrationSpec.scala) | Local checks exist; selected environment, real retention passage, writer exclusion and new-key-only deployed restart need separate evidence. |

Operational MongoDB remains truth. Hiring mutations commit independently of Kafka/Spark availability through the durable outbox. Candidate, Recruiter and singleton Admin remain the complete role model. Existing GraphQL, public receipt and report suppression contracts remain owned by their capability specifications.

Historical accelerated/local success is evidence only for its stated scope. An audit showing safe current rows is insufficient if old Kafka segments, Delta files/logs, rewrite copies, checkpoints, reports, backups or external writers remain uncontrolled.

## Desired deployed behavior

### Deployment inventory and access

The operator must identify the application, Mongo replica set, Kafka brokers, analytics driver/executors, report store, storage roots, backups, secrets and all principals that can access them. Record environment identity, software/artifact/configuration identities, region/network boundaries, data paths and operational owners without recording credentials.

- Expose only required application endpoints; protect infrastructure endpoints through the selected private network and authenticated transport arrangement. The final topology must demonstrate its allowed paths and denied paths.
- Use separate least-privilege application, publisher, reader, fencer, analytics, backup and operator identities where those responsibilities require different permissions. Verify effective permissions, not only configuration text.
- Derive actors from trusted authentication and retain service/resource authorization. Test Candidate/Recruiter cross-owner denial, Admin-only reporting and singleton Admin provisioning under concurrency.
- Bound request size, GraphQL depth/complexity, pagination, rate limits, execution and connection resources in the selected deployment. Admission controls must reject overload without exposing internals or preventing authorized recovery.
- Validate typed configuration at startup; fail before serving when required secrets, secure endpoints, schema/index setup or identities are invalid. Keep machine settings, data, logs and backups in their documented ignored locations locally; deployed equivalents are selected explicitly.
- Store secrets outside committed config, restrict reading, document rotation/revocation and test restart after rotation. Sanitized logs contain correlation and categories, not tokens, resumes, raw event bodies or secret values.
- Review dependency provenance and the built artifact for the selected stack. Findings require a documented disposition; a passing build is not a vulnerability assessment.

### Recovery and resource ownership

1. Detect an outage using the [observability](hiring-observability-resilience.md) signals and use the named operator runbook. Record affected component, last durable progress and uncertainty without assuming in-flight effects failed.
2. Restore connectivity or restart the failed component. Preserve durable outbox, workflow/journal state, receipts, revisions, deletion controls and snapshot generation guards.
3. Reconcile unknown commits and replay idempotently. Stop retrying invalid configuration or unverified state through a bounded, actionable failure outcome.
4. A stale shared lakehouse mutex is not a timed lease. Before exact owner-row removal, stop and verify all possible owners and running driver work. Do not add automatic takeover merely to improve availability.
5. Cancellation and shutdown retain resource ownership until driver work and cleanup finish. An operator timeout cannot silently release storage exclusivity while old work continues.
6. Revalidate activation evidence after restore/configuration change. Do not reuse an expired grant or assume old evidence applies to a new storage root, principal set or artifact.

Recovery objectives (RPO/RTO) are explicit unresolved product/operations inputs. Measure drills against selected objectives; do not invent passing targets after observing recovery times.

### Real retention and physical reclamation

Prepare an evidence manifest scoped to the exact deployed environment, dataset/subject fixture, input coordinates, observation instants and source/configuration identities. Use synthetic subjects in the real environment only with explicit authorization and isolate their identifiable artifacts.

- Confirm publisher fencing and bounded send drain before capturing the subject's Kafka barrier. Include every relevant stream/partition if [workflow routing](durable-hiring-workflows.md) changed the publication topology.
- Observe actual earliest offsets pass the captured Kafka barrier under the configured retention policy. Artificially advancing offsets or the clock demonstrates a mechanism, not this real-horizon criterion.
- Wait the configured Delta data and log horizons using actual elapsed time. Capture specific obsolete file/log paths and verify physical absence, including recognized temporary rewrite copies and other storage versions covered by the selected provider policy.
- Preserve unrelated control subjects and active snapshots; prove targeted reclamation did not remove unrelated data. Verify reports remain hidden while deletion gates are incomplete.
- Record evidence that restarted workers resume the same request, do not duplicate completion, and reveal only the generation permitted by current deletion controls.
- A denied listing, incomplete inventory, unverified object version, unavailable broker or clock discrepancy produces incomplete evidence and keeps the gate open. No zero-row query substitutes for physical reclamation.
- Keep production retention values unchanged for the proof unless a separately approved retention-policy change explicitly authorizes them. Accelerated local evidence remains separately labeled.

### Unmanaged writers and key retirement

Inventory scheduled jobs, old binaries, notebooks, manual credentials, object-store writers and restored hosts. Stop them and demonstrate effective access revocation or another independently reviewed exclusion mechanism in the actual environment. A repository mutex excludes only cooperating repository processes.

Under proven writer exclusion and shared ownership, run the existing read-only audit against the exact lakehouse. Validate permanent continuity anchors, all retained token-bearing artifacts, unresolved rewrite directories, deletion/report state and Kafka/Delta retention evidence. Missing or malformed evidence blocks retirement.

Operational retirement needs an explicitly reviewed production procedure and permission boundary; do not promote test-scoped authorization helpers just by invoking them with production credentials. Preserve the existing authorization binding to lakehouse, key ID, original verifier, facts/digest and time. Record non-secret evidence and the authorized operator identity in the selected protected audit mechanism.

After independently approved authorization, remove the retired key only through the scoped retirement procedure. Restart all relevant services with the new key ring, prove new-key-only processing/deletion/replay and reject changed material under an existing key ID. Preserve the permanent continuity registry and retirement evidence. Missing authorization or an unrelated-lakehouse authorization fails closed.

Revocation and retirement are not undone by casually restoring old credentials or keys. Recovery after partial retirement must use the documented state and security decision; never restore retired key material into routine operation merely to make startup succeed.

### Deletion-safe backup and restoration

1. Inventory backups, snapshots, object versions, logs and exports with their retention/access controls. A backup that contains an erased subject cannot be treated as an unrestricted replay source.
2. Select a backup and an isolated restore target. Preserve the original source, disable application serving/publishing and external writers, and verify target identity before any write.
3. Establish an authoritative **current** deletion/tombstone/completion/retirement view independent of the older backup. The mechanism to recover this view is a blocking design decision, not an assumption that the backup contains it.
4. Reconcile restored operational records, outbox/workflows and analytics against current deletion controls before replay or exposure. Preserve singleton Admin, ownership, migration/revision and uniqueness invariants. Do not resurrect deleted accounts, applications, reports or embeddings.
5. Validate continuity anchors and retired-key state; restored credentials and old binaries remain disabled. Rebuild derived analytics only from permitted retained facts using the supported registered-lakehouse paths; ordinary batch remains denied on a streaming lakehouse.
6. Check Mongo transaction consistency, Kafka identity/offset assumptions, Delta/checkpoint compatibility, report publication generations and deletion receipts together. Do not assume independently restored stores form a consistent snapshot.
7. Verify authenticated access, negative privacy cases and durable progress in isolation. Enable service only after the selected recovery gates and independent signoff; incomplete current deletion evidence keeps service/replay disabled.
8. Record actual recovery point/time, missing data and operator actions. Retain or clean up the isolated restore only within explicit authorization and backup policy.

## Architecture and code style

Keep pure validation, retention predicates and recovery decisions in immutable domain/policy code. Pass clock/IDs/observations explicitly and use typed states/errors rather than ad-hoc strings or exceptions for expected outcomes. Use `ValidatedNec` for independent configuration defects and `Either` for ordered decisions.

Analytics services preserve `F[_]` and their `AnalyticsError` channel; normal deferred/operator-required states remain explicit values. The main application keeps `IO`, `UseCaseIO` and `RepositoryIO`. Adapter-owned SDK/BSON/JSON and driver work never enter the domain. `Resource` owns clients, streams, mutexes and processes; cancellation awaits cleanup before exclusivity releases.

Deploy/runbook scripts must validate exact environment/root/owner identities, fail closed on incomplete inventories, use bounded enumeration and sanitize diagnostics. A proof command must not create permission to delete data, revoke external accounts, rotate production credentials or buy infrastructure. Keep evidence under ignored runtime paths, with hashes/references in reviewable summaries.

No blanket new abstraction, cluster operator, secret manager, cloud SDK or backup service is mandated. Add an adapter only after the chosen environment reveals a concrete capability gap. No public API change is required. New operational Mongo metadata needs versioned repeatable migrations; pre-MVP API/analytics changes update their single active shape directly. Unsupported restored shapes are preserved and rejected; no implicit backfill/reset/dual-read is authorized. See [schema evolution](../schema-evolution.md).

## Deployment decisions and alternatives

| Decision | Alternatives and tradeoffs | Owner and required evidence | Work blocked |
|---|---|---|---|
| Hosting | Self-managed hosts/containers offer control but require patching/on-call; managed services reduce some operations and add service constraints/cost | Product Manager + Architect: workload, failure domains, staffing, secure networking and budget | Deployment topology/provisioning |
| Kafka/Mongo operation | Self-managed replica/broker operation versus supported managed offerings | Data/Big Data Engineers: transaction/fencing/ACL/retention capability, restore drill and recurring cost | Production service selection |
| Delta storage | Local/attached durable storage versus object storage | Architect/Security: writer exclusion, version retention, listing/reclamation guarantees, recovery and egress | Physical reclamation and external-writer proof |
| Backup strategy | Coordinated snapshots versus logical backup plus durable recovery controls | Data Engineer/Security: restore consistency, authoritative current deletion state, isolation and RPO/RTO | Restore implementation/acceptance |
| Deletion authority after disaster | Protected independent durable deletion ledger/control copy versus another proven recovery source | Security + Data Engineer: freshness, tamper/access protections and restoration under regional/host loss | Restored service activation |
| Secrets and audit | Existing platform facility versus separately provisioned managed facility | Security/operations: rotation, least privilege, evidence integrity and access review | Final secret/audit wiring |
| Objectives/retention/budget | Explicit business-selected RPO/RTO, actual retention horizons and spending ceilings | Product Manager/operator approve inputs; no fabricated prices or legal assertions | Deployed acceptance and paid work |

Default preparation uses existing local Compose/tests and small synthetic fixtures. It does not select a production provider. Compare compute and idle time, storage and backup copies, retained versions, I/O/egress, licensing/support and operator burden with dated quotes once provider selection is in scope.

## Acceptance and evidence

| ID | Given / When / Then | Implementation/tests or evidence owner | Planned method | Actual outcome |
|---|---|---|---|---|
| PRD-01 | Given the selected topology, when access is exercised, then only documented principal/network paths succeed and required denied paths fail. | Deployment configuration and Security-owned evidence | Effective permission and connectivity matrix | Not run |
| PRD-02 | Given unauthorized actors, concurrent Admin provisioning and overload, when requests execute, then RBAC/singleton/resource limits hold with sanitized errors. | Application auth/GraphQL and integration suites | Negative direct/nested/mutation/report cases in selected environment | Not run |
| PRD-03 | Given missing/rotated secrets or invalid config, when restarting, then invalid instances fail closed and authorized rotation recovers without logging secrets. | Config/auth adapters and deployment runbook | Controlled rotation/revocation and log inspection | Not run |
| PRD-04 | Given component loss/uncertain commits/cancellation, when recovering, then durable outbox/journals/receipts and writer ownership remain correct. | Existing recovery/cancellation tests and operator runbook | Process/broker/database/storage drill with durable readback | Not run |
| PRD-05 | Given the captured Kafka barrier and production policy, when real retention elapses, then actual earliest offsets pass every required partition barrier. | Existing retention adapters/proof plus deployed manifest | Actual broker observations over genuine horizon | Not run |
| PRD-06 | Given populated Delta data/logs and production horizons, when elapsed, then captured obsolete artifacts are physically absent and unrelated controls survive. | Delta retention and selected storage adapter | Real elapsed retention, physical inventory and restart | Not run |
| PRD-07 | Given every managed/unmanaged writer, when exclusion is claimed, then old jobs/credentials cannot write and incomplete inventory blocks the claim. | Operations/Security | Writer inventory, revocation and denied-write probes | Not run |
| PRD-08 | Given completed retention/exclusion evidence, when retiring a key, then exact authorization is required and a new-key-only restart preserves processing/deletion. | Key-retirement audit/procedure and integration | Deployed audit, denied wrong authorization, authorized restart | Not run |
| PRD-09 | Given an older backup and current deletions, when restored in isolation, then no erased subject becomes accessible/replayable and missing deletion authority blocks exposure. | Proposed `HiringRestoreDeletionSafetyIntegrationSpec` and runbook | Synthetic deleted/control subjects across older backup/current deletion race | Not run |
| PRD-10 | Given independently restored stores or retired credentials, when checked, then inconsistency blocks service until resolved without ordinary-batch bypass. | Restore runbook; existing activation/checkpoint tests | Cross-store restart/offset/generation/continuity checks | Not run |
| PRD-11 | Given the selected RPO/RTO and budget, when a drill finishes, then measured recovery and cost are compared with those predeclared limits. | Operations/Product Manager | Timed restore/outage record and resource/cost report | Not run |
| PRD-12 | Given requested production streaming, when activation is evaluated, then current HAL/HCS gates, genuine deployed evidence and independent signoff apply regardless of roadmap timing. | Activation gate and protected evidence manifest | Missing/expired/mismatched grant rejection and approval review | Not run |

Future runtime implementation requires root `sbt test`, affected root/analytics integration suites and configured formatting checks. Local proof outputs, actual elapsed retention and deployed signoff are separate evidence rows. A skipped opt-in test remains skipped.

## Ordered handoff and checkpoint

1. Architect/Security/operations resolve topology, principal inventory, deletion-authority recovery and objectives; produce sanitized runbooks/config examples. Do not provision from this spec alone.
2. Scala/Data/Big Data Engineers implement only identified hardening gaps, with focused tests and migration/contract updates where required.
3. Operations executes authorized outage/restore and genuine retention/writer-exclusion drills; preserve immutable evidence references and all failed/incomplete outcomes.
4. Security and Big Data Engineer complete the scoped retirement procedure and new-key-only restart; reviewers check exact evidence applicability.
5. Independent Code Reviewer and Security Engineer issue separate verdicts; final independent QA validates the complete selected environment and unresolved gates. Product Manager records operational signoff and any blocked activation.

No implementation criterion is complete. The current deliverable is documentation; no deployment, reset, key removal or real-horizon test ran as part of this specification. Documentation delivery checks are recorded in the [central readiness index](../development-milestones.md#specification-ownership-and-readiness).

Next action: review deployment/restore decision requirements and allocate an explicitly scoped preparation slice. Implementation Code Reviewer: pending. Security Engineer: pending. Final independent implementation QA: pending. Missing environment, objectives, authority or real evidence blocks only the dependent work, and always blocks production activation where required by existing gates.
