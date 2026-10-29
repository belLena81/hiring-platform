# Hiring end-to-end acceptance run

## Identity and scope

- Task: one scripted, repeatable run that exercises UC01 to UC13 on a real local stack and records an observed result per use case. It is condition 2 of the MVP (local) definition in [development milestones](../development-milestones.md#current-refactoring-and-path-to-mvp-checked-2026-10-29).
- Status: **draft, not started.** Created 2026-10-29. No criterion has run.
- Coordinator: Product Manager. Scala Developer writes the suite and the pipeline script. Data Engineer reviews the seed data and the Atlas Local index setup. Big Data Engineer owns the analytics step. Independent Code Reviewer, Security Engineer (the role matrix HEA-14 and the secrets handling) and QA review the final state.
- User outcome: a single named run answers "does the product do what the 13 use cases promise, end to end?" with a log a reviewer can read, instead of many focused suites that each prove one slice.
- Actors: Candidate, Recruiter, singleton Admin, and the system (events, analytics).
- Non-goals: performance or SLO claims, load, deployed or cloud evidence, real Voyage or calendar providers, Atlas cloud, human relevance judgement, the guided discovery features of Phase 11A, Kafka-over-TLS from a container, frontend. The run proves behavior on a bounded synthetic dataset on one machine.
- Dependencies: the closed analytics refactoring (shapes must be final before the run is recorded as evidence); Atlas Local for UC02, UC06 and UC10.

## Source context and decisions

Source facts read on 2026-10-29 at branch `refactor`, HEAD `321c2eb`:

| Boundary | Fact |
|---|---|
| Existing coverage | Many focused suites (`src/it`, `analytics/src/it`) and `MainProcessSpec`, which starts the real `Main` for startup and failure cases only. No test or script drives all 13 use cases through the real HTTP API and then the analytics output. `scripts/generate-local-traffic.sh` sends only read-only health and invalid queries. |
| Test stack | `compose.test.yaml` (owned by `scripts/run-local-tests.sh`) provides Mongo 8 (community) and the SASL Kafka broker. It has no Atlas Search or Vector Search. |
| Atlas Local | `compose.atlas-local.yaml` is opt-in. The harness guard denies agents `docker compose` for every file except `compose.test.yaml`, and protects the compose files from edits. So an agent cannot start Atlas Local, the full `compose.yaml`, or the analytics profile. The user runs those. |
| Embeddings | The only provider is Voyage (`voyage-4-lite`, 1024 dimensions). Its endpoint is configurable (`application.conf` `voyage.endpoint`). Test fakes exist only inside unit suites. |
| Analytics | The batch reads one explicit Kafka partition and offset range per run, writes Delta locally and publishes the report. The report period is at most 30 days. Distributions are suppressed below K=10 distinct candidate subjects. The containerised API runs with Kafka publication disabled (D6 of the [container runtime](hiring-container-runtime.md)), so UC11 to UC13 run with the API on the host (`sbt run`), not in the API container. |
| Roles | Candidate, Recruiter, singleton Admin seeded from explicit configuration (README "Trusted Admin initialization"). Every mutation needs an `idempotencyKey`. |

Decisions (proposed; revisable by the user):

- D1 Two tiers.
  - **Tier A, agent-runnable:** an integration suite against the real `Main` over HTTP on the test stack. It covers every use case that does not need Atlas search (UC01, UC03, UC04, UC05, UC07 to UC09, UC11 publication) and the role matrix.
  - **Tier B, user-run:** a script that starts Atlas Local, Kafka and the API on the host, runs the same story plus UC02, UC06, UC10, then the analytics batch and the Admin report (UC11 consumption, UC12, UC13). Agents write it; the user runs it and pastes the log path into this document.
- D2 Embeddings in the run come from a small local HTTP stub that implements the Voyage endpoint contract and returns deterministic 1024-dimension vectors from hashed words, so similar texts are close. No paid call, no API key, no flakiness. One separate opt-in smoke with a real Voyage key is optional and is not part of the acceptance claim.
- D3 One story, one dataset, fixed seed. A scripted narrative with fixed names and IDs (below). Re-running on a clean stack must give the same counts.
- D4 The run asserts business outcomes, not implementation details: GraphQL results, history, event counts and report numbers.
- D5 The dataset has 12 candidates so the K=10 suppression is both lifted for the main report and checked as suppressed for a smaller slice.
- Open questions: Q1 Atlas Local as the only Mongo in Tier B, assuming it provides the replica set that transactions need (verify in the first slice). Q2 whether Tier A should also run against Atlas Local when the user starts it. Q3 report timing: batch is one-shot, so the run reads the report after the batch exits (no freshness claim).

## Behavior and contracts

### The story (seed and order)

All names are fictional. Operations are the checked GraphQL contract (`src/test/resources/graphql/hiring.graphql`) and use the existing operation fixtures where they exist.

1. Start: Admin is seeded from configuration. Admin signs in.
2. Accounts: Recruiters Rita (R1) and Ravi (R2); candidates C01 to C12, among them Ana (C01), Bo (C02) and Cy (C03).
3. Jobs (UC07): R1 creates and publishes J1 "Scala Backend, Berlin, skills Scala and Kafka", J2 "Kotlin Services, Berlin", J3 "Java Developer, Munich"; R2 creates and publishes J4 "Data Engineer, Berlin". R1 updates J3, then closes J3. R1 creates J5 and leaves it unpublished.
4. Embeddings: the embedding worker processes jobs and candidate profiles. Admin `embeddingCoverage` shows no missing or stale entries before search is exercised.
5. Discovery: UC01, UC02, UC03, UC06.
6. Applications: C01 to C12 apply to J1 (UC04); C01 also to J2. Duplicate and closed-job attempts are made. Candidates read their own applications (UC05).
7. Recruiter review: R1 lists J1 applications (UC08) and moves them through the lifecycle (UC09): C01 Created, Accepted, Interview, Hired; C02 Created, Rejected with feedback; C03 Created, Accepted, Interview, Rejected; C04 Created, Declined by the candidate with a reason; C05 Created, Accepted; C06 to C12 stay Created.
8. Candidate matching (UC10): R1 asks for matches for J1.
9. Events and analytics: all above published (UC11); the batch consumes the range; Admin reads the report (UC12, UC13).

### Acceptance rules

Each use case has at least one typical, one forbidden and one failure or boundary example. Tags are `HEA-nn.m`; the test or script step name starts with the tag.

## Domain language and examples

Terms used are in [domain language](../domain-language.md); no new term is needed.

| Use case | Tag | Scenario |
|---|---|---|
| UC01 | HEA-01.1 | Given J1, J2 and J4 are open in Berlin and J3 is closed, when Ana filters city Berlin with skills Scala, then only J1 is returned, newest first, with a cursor that returns the next page without gaps |
| UC01 | HEA-01.2 | Given J5 is unpublished and J3 is closed, then neither appears for any candidate |
| UC02 | HEA-02.1 | Given the embeddings of J1 to J4 are current, when Ana searches "functional JVM backend", then results are ordered by the fused ranking, carry mode HYBRID, model and `searchId`, and exclude J3 and J5 (Atlas Local) |
| UC02 | HEA-02.2 | Given the stub provider is stopped, when Ana searches, then the answer is a keyword result marked degraded, not an error |
| UC03 | HEA-03.1 | When Ana opens J1 and records a view, then the job detail is returned and exactly one view event exists |
| UC03 | HEA-03.2 | When Ana requests closed J3 or unpublished J5, then the answer is as for an unknown job |
| UC04 | HEA-04.1 | When Ana submits to J1, then one application exists in Created with a first history entry (actor Ana, UTC time) |
| UC04 | HEA-04.2 | When Ana submits to J1 again, then the second submission is rejected and the history is unchanged |
| UC04 | HEA-04.3 | Given two concurrent submissions by Bo to J2, then exactly one application exists |
| UC04 | HEA-04.4 | When Cy applies to closed J3, then the submission is rejected |
| UC05 | HEA-05.1 | When Ana lists her applications, then she sees her two, and Bo's application is not visible by list or by direct ID |
| UC06 | HEA-06.1 | Given Ana's profile embedding is current, when she asks for recommendations, then only open, visible jobs are returned (Atlas Local) |
| UC06 | HEA-06.2 | Given Ana's embedding is stale, then the answer is `SearchUnavailable`, not a wrong list |
| UC07 | HEA-07.1 | When R2 tries to update or close J1, then the answer is forbidden and J1 is unchanged |
| UC07 | HEA-07.2 | When R1 closes J3, then it leaves discovery and still lists under `myJobs` |
| UC08 | HEA-08.1 | When R1 lists J1 applications, then all twelve are returned in pages; R2 requesting the same list is forbidden |
| UC09 | HEA-09.1 | When R1 moves C01 through Accepted, Interview and Hired, then each step appends history with previous and new status, actor and time, atomically with the status |
| UC09 | HEA-09.2 | When R1 rejects C02 without feedback, then the call fails with a validation error and the status is unchanged; with feedback it succeeds |
| UC09 | HEA-09.3 | When R1 tries to hire C06 who is in Created, then the transition is refused |
| UC09 | HEA-09.4 | When C04 declines without a reason, the call fails; with a reason it succeeds; C04 cannot decline C05's application |
| UC10 | HEA-10.1 | When R1 asks for matches for open J1, then candidates are ranked with `matchedSkills`, and no residence, availability, email or consent is returned (Atlas Local) |
| UC10 | HEA-10.2 | When R2 asks for matches for J1, or a candidate asks, then the call is forbidden and the stub records no embedding call |
| UC11 | HEA-11.1 | After the story, the number of published events per action equals the number of committed actions (applications, status changes, views, clicks); none are missing |
| UC11 | HEA-11.2 | When the same Kafka range is read twice by the batch, then the report numbers do not change (idempotent consumption) |
| UC12 | HEA-12.1 | Given the J1 story below, then the report counts equal the expected table and the conversion rates are the exact ratios of those counts |
| UC12 | HEA-12.2 | When a candidate or recruiter requests the report, then it is forbidden; Admin only |
| UC13 | HEA-13.1 | Given 12 distinct candidates, then time-to-hire and stage durations are present and consistent with the timestamps in history |
| UC13 | HEA-13.2 | Given a slice with fewer than 10 distinct candidates, then the distribution is suppressed, not shown |

Expected values come from this table, the oracle, and are not copied from the aggregation code. Final statuses of the 12 J1 applications: Hired 1 (C01), Rejected 2 (C02, C03), Declined 1 (C04), Accepted 1 (C05), Created 7 (C06 to C12). Stages reached: applications 12; accepted at some point 3 (C01, C03, C05); interview at some point 2 (C01, C03); hired 1; rejected 2. Application-to-interview is 2/12 and interview-to-hire is 1/2, if the report defines them over stages reached; slice 3 confirms the report's definition in UC12 before the numbers are frozen, and any difference is recorded as a defect or a documented definition, never adjusted silently. C01 also applies to J2 (a second application, outside the J1 counts), so the system-wide report totals are computed from the full story in slice 3.

## Acceptance and evidence

| ID | Given / When / Then | Tier | Implementation and test paths | Actual outcome |
|---|---|---|---|---|
| HEA-01 | UC01 structured discovery and pagination (tags .1 to .2) | A | Not implemented | Not run |
| HEA-02 | UC02 hybrid search and degraded fallback (.1 to .2) | B | Not implemented | Not run |
| HEA-03 | UC03 detail, hidden jobs, one view event (.1 to .2) | A | Not implemented | Not run |
| HEA-04 | UC04 submission, duplicate, race, closed job (.1 to .4) | A | Not implemented | Not run |
| HEA-05 | UC05 own applications only (.1) | A | Not implemented | Not run |
| HEA-06 | UC06 recommendations and stale refusal (.1 to .2) | B | Not implemented | Not run |
| HEA-07 | UC07 job lifecycle and ownership (.1 to .2) | A | Not implemented | Not run |
| HEA-08 | UC08 recruiter list and ownership (.1) | A | Not implemented | Not run |
| HEA-09 | UC09 transitions, required feedback and reason (.1 to .4) | A | Not implemented | Not run |
| HEA-10 | UC10 candidate matches and privacy (.1 to .2) | B | Not implemented | Not run |
| HEA-11 | UC11 publication equals actions; idempotent consumption (.1 to .2) | A for publication, B for consumption | Not implemented | Not run |
| HEA-12 | UC12 funnel report equals the story (.1 to .2) | B | Not implemented | Not run |
| HEA-13 | UC13 time to hire and suppression (.1 to .2) | B | Not implemented | Not run |
| HEA-14 | Role matrix: every query and mutation tried as Candidate, Recruiter (owner and non-owner), Admin and anonymous; expected allow or forbid; errors sanitized | A | Not implemented | Not run |
| HEA-15 | Reproducibility: two runs on a clean stack give the same counts; the run leaves no data outside `.local/` and commits nothing | A and B | Not implemented | Not run |
| HEA-16 | Evidence: each use case has a recorded pass or fail with the log location in this document; a failed or skipped step is listed, never dropped | A and B | Not implemented | Not run |

A step that cannot run (for example Atlas Local not started) is recorded as BLOCKED, not as a pass. A pass in Tier A does not cover the Tier B use cases.

## Performance and cost

Not applicable: the run makes no latency, throughput or capacity claim. It uses a bounded dataset (5 jobs, 12 candidates, about 40 mutations) and no paid service. The stub provider replaces Voyage.

## Implementation handoff

| Slice | Content | Owner and write paths | Gates |
|---|---|---|---|
| 0 | Verify Atlas Local gives a replica set for transactions (Q1); define the story data as a fixture | Data Engineer, Scala Developer: a fixture under `src/it/resources/`, notes in this spec | QA |
| 1 | Tier A suite: HEA-01, 03, 04, 05, 07, 08, 09, 14 and HEA-11.1 over the real `Main` and HTTP, on the wrapper-owned test stack | Scala Developer: `src/it/scala/.../acceptance/` named for the capability (for example `HiringStoryAcceptanceIntegrationSpec`) | Code, Security (role matrix), QA |
| 2 | Embedding stub server and Tier B script: Atlas Local, Kafka, API on the host, the story, HEA-02, 06, 10 | Scala Developer: stub and script under `scripts/`; no edits to protected compose files | Code, Security, QA |
| 3 | Analytics step: batch over the produced range, report read, HEA-11.2, 12, 13 | Big Data Engineer: script step and expected-value table | Code, QA |
| 4 | Full run by the user on a clean stack, twice; evidence recorded (HEA-15, 16); independent QA | User runs; QA reviews logs | QA |

Agents cannot run Tier B. The user runs it and records the log path and result here.

Risks: the Atlas Local container image is large and slow to become ready; the story's expected numbers must be written down first so the run cannot be tuned to match; if the analytics shape changes after the run, the run is repeated; a Tier A pass must not be read as the MVP claim.

## Checkpoint and review

- Completed criteria and changed files: none; this document only.
- Latest commands/results: none run. Source facts above were read, not executed.
- Blockers and next action: confirm D1 to D5 and Q1 to Q3, then start slice 0.
- Code Reviewer verdict: pending
- Security Engineer verdict: pending
- Final independent QA verdict: pending
