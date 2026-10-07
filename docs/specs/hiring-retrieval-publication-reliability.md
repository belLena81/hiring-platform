# Hiring retrieval and publication reliability

Status: local implementation verified; full acceptance partial / external gates open. User authorized implementation of the October 7 MongoDB-informed plan. This specification owns the new acceptance evidence; historical local evidence does not validate these changes.

## Outcome and boundaries

Protect deletion, embedding convergence and durable publication first; reduce retrieval work and eliminate growing producer attribution documents next. Preserve pure immutable decisions, the existing StateT lifecycle, concrete IO/EitherT ports, Mongo transactions, stable provider keys and Resource-owned workers. No generic Saga engine, paid activation, deployment, destructive reset, public event versioning or new frontend. MongoDB remains authoritative; Search/index data only supplies retrieval evidence.

## Acceptance

| ID | Required observable behavior | Evidence |
| --- | --- | --- |
| HRP-01 | Candidate deletion removes embeddings and metadata; active Candidate + observed revision guards every embedding write | Root unit and real Mongo deletion/late-write gates PASS |
| HRP-02 | Unrelated interview revision conflicts retain embedding work until current/deleted/absent | Root unit PASS: nine conflicts then convergence, attempts unchanged; real Mongo authorization/write conflict PASS |
| HRP-03 | Null Kafka values quarantine durably before offsets advance | Root unit and isolated Kafka tombstone/restart gates PASS |
| HRP-04 | Producer initialization failure/cancellation closes exactly once | Root unit PASS, including blocked initialization cancellation and exactly one close |
| HRP-05 | Claim immediately before send, renew owned lease, charge authorized sends; queued work does not exhaust attempts | Root unit and real Mongo/Kafka gates PASS: immediate renewal, typed lease outcomes, cancellation and generation fencing |
| HRP-06 | Near-future commands defer without acknowledgment within configured five-second tolerance; invalid future work has durable repair evidence | Root unit and applicable integration gates PASS |
| HRP-07 | Authoritative actor selection gates nearby/facets; revoked actor differs from authorized empty result | Real Mongo discovery gate PASS (6 tests including revoked actor and empty results) |
| HRP-08 | Lexical Search prefilters preserve equality, all-skills and consent semantics with explicit static mappings | Root static mapping/filter tests PASS; live Atlas unavailable |
| HRP-09 | Bounded retrieval hits avoid unnecessary vectors and duplicate hydration; current authorization/eligibility remains authoritative | Root typed projection/dedup/current hydration tests PASS; Atlas unavailable |
| HRP-10 | Expensive roots have request budget four, process permits four and Mongo deadline 2000 ms; exact facets retain counts and fail typed on limits | Root GraphQL/permit tests PASS; real Mongo maxTime failure and recovery PASS |
| HRP-11 | Outbox scans at most 64 candidates and skips busy subjects without weakening deletion fences | Real Mongo fair-scan regression PASS: 128 busy heads then independent work |
| HRP-12 | Separate producer registrations are transactionally fenced; deletion checkpoints bounded traversal; only broker-confirmed retirement permits cleanup | Real Mongo registration regression PASS: 130 generations traversed as 64/64/2; analytics 7 integrations PASS |
| HRP-13 | New repeatable operational migration preserves data, tolerates interruption, verifies shape and requires old writers stopped | Real Mongo interrupted 012/013 restart and preservation PASS; strict legacy write rejection PASS |
| HRP-14 | Embedding transient exhaustion has capped durable retry, terminal failures have explicit repair, lease renewal and observed worker recovery/health | Root lease/retry/health tests and real Mongo repair/renewal gates PASS |
| HRP-15 | Kafka processes partitions concurrently with sequential durable progress within each partition | Root eight-partition regression PASS; actual Kafka restart and 32-interview recovery workload PASS |
| HRP-16 | Geo cursor lower bound conservatively uses meters; ties and beyond-radius empty pages remain correct | Real Mongo tie/radius/empty-cursor regression PASS |
| HRP-17 | Completed migrations avoid repeated full audits after validated cutover; explicit integrity audit remains resumable | Partial: 003/010 skip after strict 013 proof; 011 audit retained. 501-row explicit audit resume PASS |
| HRP-18 | Index/retrieval changes carry bounded workload evidence; ANN budgets 100/500/2000 compare with ENN; optional stored source/quantization stay gated | Local bounded workload PASS; Atlas ANN/ENN and paired cost/performance adoption BLOCKED (URI absent) |

## Data and contract decisions

Existing GraphQL names, status matrix, cursor encoding and seven-field unversioned operational envelope remain active. New internal retrieval, authority, retry and registration progress types stay behind ports. Operational Mongo storage evolves via a new migration (never edit applied migration contents), stopped incompatible writers, bounded durable progress and verification. Pre-MVP API/analytics have one shape and no legacy readers. Existing data is preserved.

Producer registration must touch the same subject fence transactionally; marking a subject deleted excludes concurrent registration. Cleanup must not copy the registry into another unbounded array. Active/unresolved registrations never expire through TTL. Restart after broker fencing but before Mongo acknowledgment repeats safe fencing.

Search mapping uses exact token equality without silently normalizing original job fields. Required skills use conjunction; consent false or missing retains its current bypass. Exact structured facet counts remain authoritative despite Search's newer facet capabilities. Stored source, scalar quantization and changed ANN settings are optional experiments, disabled until live capability and paired no-regression gates pass.

## Verification and handoff

Run Java 17 root unit/format checks, affected real replica-set and Kafka integration checks, migration failure/restart checks, skewed bounded workloads and independent Code/Security/QA reviews. Source inspection, compilation, skipped Atlas tests and earlier PASS verdicts are separate evidence. No production capacity or phase-completion claims without the owning external gates.

Initial workload is bounded local synthetic data and existing concurrency 1/8 harnesses; no cost ceiling for new paid services has been provided, so none are provisioned. Capture latency percentiles, throughput, errors, query plans, write/index overhead and environment. Keep generated evidence in ignored `.local/data/`.

## Progress

- October 7: clean baseline inspected; specialists assigned exclusive Kafka/publication and discovery/search paths. Coordinator owns embedding recovery, configuration, integration and producer storage until another specialist slot is available. Third-agent routing initially blocked by tool thread limit; independent final reviews remain required.

- October 7 final-source checkpoint: root Java 17 unit suite passed 650/650. Both independent Code Reviewer and Security Engineer reviews passed after fixing uncharged revision-conflict rescheduling and immediate publication lease renewal. Full real MongoDB/Kafka integration is running; formatting and final QA closure remain pending.
- Analytics focused evidence: 22 unit checks and seven real MongoDB erasure adapter checks passed on final analytics source; full analytics unit gate is pending.
- HRP-17 safety decision: new 013 permits skipping completed 003/010 full scans only after one audited baseline and exact strict validator verification. Existing 011 cleanup audit remains until equivalent store-enforced cleanup semantics can be established; this portion is partial rather than claimed complete.
- HRP-18: Atlas URI is absent. Optional index experiments remain disabled; ANN/ENN, live resource/index costs and paired performance adoption are unverified. Revised local geographic workload execution is included in the integration gate.

- Final analytics build: full Java 17 unit suite PASS, 470/470; formatting checks PASS. Focused operational migration/discovery real Mongo gate PASS, 13/13. Earlier fresh-database namespace and empty JSON Schema required-array failures were fixed, independently reviewed and rerun successfully.

## Local scaling checkpoint

Disposable MongoDB 8.0.32, Java 17, 128 jobs, 32 candidates and 20 fixed queries; 640 service/driver requests at each concurrency 1 and 8, no warmup. Both runs had zero errors. p95 was 6.97/14.71 ms, p99 7.99/16.30 ms, throughput 207/660 requests/s. The current actor→jobs query used the geo index with zero collection scans; actor selection examined one key/document and the nested lookup examined 117 keys/173 documents for the explained query. Jobs index footprint after seeding was 188,416 bytes. Cgroup boundary samples are included in the ignored artifact.

Evidence: `.local/data/discovery-evaluation/geographic-1791361818613.json`; structured HTTP evidence: `.local/data/mongodb-access-evaluation/structured-http-1791361713548-4ed90dcd-4863-4f39-8d42-2c9fdb3a6be7.json`. Independent QA accepted measurement integrity. This is a bounded local characterization; it excludes HTTP/GraphQL for the geographic timings, has no paired before/after adoption comparison and establishes no deployed capacity/SLO.

Shared permits bound four costly operations. Candidate RRF can fan out to three aggregates per operation (up to twelve branch aggregates); bounded authoritative hydration is additional work. Increasing these limits requires workload evidence rather than assuming four concurrent driver queries.

## Final local verification

- Java 17 root unit suite: 650/650 PASS on final production source.
- Final full integration invocation: 125 executed checks PASS; four Atlas checks skipped, seven Kafka checks skipped and scheduling failed because the isolated proof configuration was absent. After safely recovering only the owned broker configuration, affected Kafka gates PASS: restart/tombstone 3/3, 32-interview recovery 1/1, strengthened generation fencing 4/4. This is composite evidence from the full run and affected reruns; the earlier failed invocation is not relabeled successful.
- Analytics final source: full suite 471/471 PASS; real Mongo erasure integrations 8/8 PASS; focused codec/worker checks 23/23 PASS.
- Root and analytics formatting checks PASS; final whitespace check PASS. Generated artifacts and recovered broker credentials remain ignored; no secrets were printed or staged.
- Independent Code Reviewer and Security Engineer: PASS after all reported fixes. Independent QA: PASS for local correctness/recovery, with full specification completion partial/externally blocked.
- Findings fixed during verification: fresh collection creation before collMod, omission of invalid empty JSON Schema required arrays, strict BSON Long proof validation on both operational and analytics boundaries, typed publication lease outcomes, and current-claim assertions in broker fencing fixtures. Applied operational migration bodies were preserved.
- Remaining acceptance: cleanup migration 011 still audits startup (HRP-17 partial), four live Atlas capabilities and paired ANN/ENN/optimization adoption are unverified (HRP-18 external). No production activation, paid provisioning, data reset or commit is part of this task.
