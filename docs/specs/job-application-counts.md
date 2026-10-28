# Job application counts

Status: implemented locally. Independent reviews: Security PASS, QA PASS, Code Review FAIL on the first pass (vacuous AC-14.8 recruiter case, measurement test locked in the N+1). Both addressed below; the Code Review re-run is PASS.

## Problem

A recruiter dashboard lists owned jobs and needs to show how many applications each job holds in each status. The
nested list `Job.applications` runs one authorized read per job (an N+1 by design: see `QueryAdmission`), so a page of
jobs costs one read per job and the counts cannot be derived without loading the applications.

## Contract

`Job.applicationCounts: JobApplicationCounts` (nullable)

```graphql
type JobApplicationCounts { total: Int!  byStatus: [ApplicationStatusCount!]! }
type ApplicationStatusCount { status: ApplicationStatus!  count: Int! }
```

- `byStatus` always lists every `ApplicationStatus`, in declaration order, with zero for statuses without applications.
- Visible only to the owning Recruiter and Admin. A Candidate, a non-owning Recruiter and an unknown job all resolve to
  `null`, never to an error that reveals the job.
- One batched read per request for all jobs selected (`jobApplicationCountsFetcher`, batch size `PageSize.Max`).

Non-goals: no change to `Job.applications`, no per-parent paging batch, no stored or denormalized counters (counts are
computed from the operational truth on every read), no UI.

## Acceptance criteria and evidence

| ID | Criterion | Evidence |
|---|---|---|
| AC-14.6 | Diagnostic: nested `Job.applications` of ten jobs costs ten reads | `HiringGraphQLRelationBatchingSpec` |
| AC-14.7 | Counts of ten jobs are one read, match stored applications, all six statuses present | `HiringGraphQLRelationBatchingSpec` |
| AC-14.8 | Counts are null for a Candidate and for a Recruiter who does not own the job, reached through `job(id:)` so the job node is actually returned | `HiringGraphQLRelationBatchingSpec` |
| AC-14.9 | Against MongoDB: counts match stored data; only managed jobs are reported; unknown job is absent; an owner whose account is gone after the scope was granted reads nothing; an unreadable stored status fails the counts closed with `InvalidStoredData` | `MongoJobApplicationCountsIntegrationSpec` |
| AC-14.10 | One hundred jobs are one aggregate that examines no application document; opt-in before/after measurement (`HIRING_JOB_COUNTS_MEASURE` set, test tagged `measurement`) | `MongoJobApplicationCountsIntegrationSpec`, results below |
| AC-14.11 | One job selected under two aliases is a single counts read | `HiringGraphQLRelationBatchingSpec` |
| AC-14.12 | An Admin reads the counts of a job owned by a Recruiter, through GraphQL on the in-memory repository and against MongoDB with the singleton Admin provisioned by the startup seed | `HiringGraphQLRelationBatchingSpec`, `MongoJobApplicationCountsIntegrationSpec` |
| AC-14.13 | 101 distinct jobs in one request are split into batches of 100 and 1 | `HiringGraphQLRelationBatchingSpec` |

Complexity: the field is priced with `nestedFieldComplexity` (base 5 plus children), because each job runs a lookup and a group.

Nothing in the contract is known to be uncovered. A corrupt stored status fails the whole page of counts closed, by design (AC-14.9). The Admin is provisioned through `MongoAdminSeed` before any other account, because the seed refuses to bootstrap once other users exist.

## Measurement (AC-14.10)

The figures come from the committed opt-in test, run with `HIRING_JOB_COUNTS_MEASURE` set, from the owner's terminal (4 passed, 0 ignored). An earlier run of a first harness version gave 255.3 ms / 394.5 ms against 22.1 ms / 26.1 ms; run-to-run variation on one machine is therefore visible, and the ratio (about 9 to 12 times at the median) is the stable claim, not the milliseconds.

Workload: disposable MongoDB 8.0.32 from the local test services, one machine, repository level (no HTTP), 100 jobs
of one recruiter, 50 applications each (5000 documents, statuses cycling), indexes from `MongoHiringSetup`, 3 warm-up
and 10 measured runs, sequential, no concurrency, no errors.

| Shape | MongoDB aggregates per page | Median | p95 |
|---|---|---|---|
| `findByJob` first 5 per job (what `Job.applications(first: 5)` runs) | 100 | 188.5 ms | 346.4 ms |
| `applicationCounts` for all 100 jobs | 1 | 20.1 ms | 24.3 ms |

Explain of the counts aggregate: `totalDocsExamined` 0 at every stage (covered by the `jobId`/`status` index), 5000 index
keys examined for the lookup. The baseline returns at most five applications per job and no counts, so the comparison is
the cost of the dashboard's data need through each shape, not of equal payloads. Absolute times are local and
sequential; they say nothing about production latency.

## Unknowns

- The recruiter UI does not exist; the field follows common applicant-tracking dashboards (a job row with per-stage
  counts, click-through to one job's pipeline through `jobApplications`). Revisit when a client exists.
- Behavior under concurrent dashboard load is not measured.
