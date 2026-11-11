# MVP Use Cases

Authentication supporting these use cases now uses trusted startup Admin seeding and credential-verified login/signup receipt replay. [Security/runtime reliability](specs/hiring-security-and-runtime-reliability.md) tracks these contract changes and the recovery refinements.

The [architecture wiki operation map](wiki/domain-and-use-cases.md) traces these use cases through the current GraphQL API, services, domain policies and persistence. See [wiki home](wiki/README.md) for storage and workflow details.

The [discovery and interview integrity follow-up](specs/candidate-discovery-and-interview-integrity.md) covers candidate city-filter correctness, geographic boundary/pagination evidence, complete interview deletion across attribution batches and local hiring acceptance during an isolated broker-process outage. Existing consent, ownership and lifecycle rules remain authoritative.

## Retrieval and recovery implementation checkpoint

The [reliability specification](specs/hiring-retrieval-publication-reliability.md) records the current changes and pending gates. Nearby jobs and exact facets validate the active trusted actor in their database query, preserve authorized empty results and bound costly request roots, process concurrency and query duration. Search hydrates current public data after deduplicating bounded hits. These limits do not change visibility, consent, required-skills semantics or exact facet counts.

Transient embedding failures retain durable work within the configured retry budget; revision-only conflicts reschedule without charging that budget; invalid provider responses become terminal. Active singleton Admin maintenance can inspect and repair failed work with generation compare-and-set. Candidate deletion removes embedding data and rejects later embedding writes. Local recovery evidence does not certify real embedding providers or search relevance.


These use cases and SLOs describe product intent; see [current implementation](../README.md) and the [current roadmap](development-milestones.md). Query/event sketches below are design examples and may omit active fields or use conceptual names. Use the [API reference](api.md) and [checked schema](../src/test/resources/graphql/hiring.graphql) for executable contracts; the sketches do not authorize new operations or lifecycle transitions.

## Current capability mapping

| Use cases | Implemented API or boundary | Acceptance boundary |
|---|---|---|
| UC01–UC03 | `jobs`, `semanticJobSearch`, `job`, explicit `recordJobView` | Visibility, bounded retrieval and search-provider evidence |
| UC04–UC05 | `submitApplication`, `myApplications` | Atomic submission/history and candidate ownership |
| UC06 | `recommendedJobs` | Model/source freshness and relevance validation |
| UC07–UC09 | Job lifecycle mutations, `myJobs`, `jobApplications`, action-specific application mutations | Mongo transactions, role/ownership and explicit transition matrix |
| UC10 | `candidateMatches` | Owned open job; minimized candidate results and private-filter consent semantics |
| UC11 | Mongo transactional outbox → Kafka; explicit search/click telemetry | Durable at-least-once publication and idempotent consumption |
| UC12–UC13 | Admin `analyticsReport` over guarded projections | Phase 6 local acceptance; Phase 7 functional closure and production/SLO gates remain separately tracked |

Current transitions are `Created → Accepted / Declined / Rejected`, `Accepted → Interview`, and `Interview → Hired / Rejected`. `Declined`, `Hired` and `Rejected` are terminal. Action-specific mutations enforce these rules; no generic status-change operation or GraphQL subscription is implemented. The current analytics report is systemwide transition activity, time-to-hire and skill posting activity; recruiter/job/location dimensions described as goals require separate event-time enrichment design.

## Planned Guided Discovery and Follow-up (Phase 11A)

Partly implemented: GDF-02 (fit reasons) is committed as `48f271a` (2026-11-11; Code Reviewer, Security and QA PASS). GDF-01 (zero-result recovery of the candidate job search) is implemented on the uncommitted tree; Code Reviewer, Security and QA PASS on the uncommitted tree (Security after the receipt lock fix); remaining: commit, owner Docker runs, owner `scripts/check-local.sh` on the committed tree. The other items are planned, not implemented. The [guided discovery specification](specs/guided-discovery-and-followup.md) owns behavior and acceptance. It extends UC01, UC02, UC03, UC06, UC09 and UC10 without a language model and without changing ranking:

- UC01: an empty filtered job search runs at most two relaxed searches (drop `createdAfter`, then `skills` from all-of to any-of) and names each relaxed filter in `relaxedFilters`; a relaxed page is final (implemented on the uncommitted tree, GDF-01; Code Reviewer, Security and QA PASS on the uncommitted tree).
- UC02: hits show matched and missing skills (implemented, GDF-02, 2026-11-11: `matchedSkills` and `missingSkills` derived only from the job's required skills and the candidate's own profile skills, with no provider call); the query yields suggested filters that are not applied automatically (planned, GDF-05); a candidate can save a search and receive alerts through the notification port (planned, GDF-06).
- UC03: a job page offers similar open jobs from the stored job embedding (planned, GDF-03).
- UC09: a recruiter or candidate can request a draft message for an existing status action (planned, GDF-04). A draft sends nothing and changes no status; the existing mutation keeps its required feedback or reason.
- UC10: matches show matched and missing skills of the owned job (implemented, GDF-02, 2026-11-11: derived only from the owned job's required skills and the candidate skills already in the result, so no new private candidate data is shown).

The post-MVP LLM option (Phase 16) is described only in that specification.

## Planned Discovery Extensions (Phase 10)

The following are later extensions to structured, semantic, and hybrid discovery; they preserve the core hiring invariants described below. They are sequenced in Phase 10 after MongoDB optimization (Phase 8) and Search Evaluation (Phase 9), before further Spark work. Phase 10 local scope is Completed (local) by owner decision of 2026-11-09, kept by owner decision of 2026-11-10. Its Atlas Local rows are observed, including the HDQ-06.L lexical branch on 2026-11-10; the cloud rows are Phase 10C (Not done, deferred). See the [README roadmap](../README.md#roadmap). Establish relevance measures and a bounded, representative baseline before evaluating personalized ranking.

The [hiring discovery specification](specs/hiring-discovery-search-quality.md) owns detailed desired behavior, public-contract alternatives, boundaries and acceptance criteria. [Durable hiring workflows](specs/durable-hiring-workflows.md) separately defines conditional interview scheduling; it does not change the current `moveApplicationToInterview` contract or add application-status transitions. See the [remaining specification index](development-milestones.md#remaining-delivery-order-and-acceptance) for operational resilience and analytics delivery.

- Radius-based job search combines distance with existing filters, candidate visibility, and deterministic cursor pagination. Inputs include structured coordinates; geocoding is out of scope.
- Richer lexical search and facets use Atlas Search alongside existing vector and hybrid search. Lexical-query facets (HDQ-12) are implemented with `coverageLimit` labelling; the Atlas Local rows were observed on 2026-11-01, and the HDQ-06.L lexical branch on 2026-11-10. Richer analyzers and the Atlas cloud rows remain open.
- Personalized ranking remains deferred under the conservative policy (HDQ-08).
- MongoDB query plans and indexes are evaluated against representative queries and measured workloads, accounting for read gains and write/storage costs.
- Personalized ranking is compared with the relevance baseline and is adopted only if evidence shows a quality improvement; otherwise, defer it.

Atlas is required for this stage's Atlas Search and Vector Search capabilities. Local MongoDB Community remains suitable for core workflows and local transaction tests; the opt-in `compose.atlas-local.yaml` (MongoDB Atlas Local, mongot) runs `$search`, `$vectorSearch`, `$rankFusion` and `$scoreFusion` for correctness evidence only, while native `$rerank`, Atlas-scale latency/recall and index-size figures remain Atlas-only. This roadmap adds no Atlas provisioning or spend.

## Purpose

Use cases are the starting point for:

```text
Business Use Case
       ↓
GraphQL Operation
       ↓
MongoDB Access Pattern
       ↓
Collection / Index Design
       ↓
Domain Event
       ↓
Kafka
       ↓
Spark / Delta
       ↓
Business Metric
```

The architecture must be derived from these workloads rather than designing collections and pipelines independently.

---

# UC01 — Search Jobs by Structured Filters

**Actor:** Candidate

**Goal:** Find currently available jobs using structured filters.

### Input

```text
status
location
skills
createdAfter
first
after
```

### GraphQL

```graphql
query SearchJobs(
  $filter: JobFilter!
  $first: Int!
  $after: Cursor
) {
  jobs(
    filter: $filter
    first: $first
    after: $after
  ) {
    edges {
      node {
        id
        title
        location
        skills
        status
      }
      cursor
    }

    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
```

### Business Rules

- Only `OPEN` jobs are visible to candidates.
- Page size is bounded.
- Pagination uses cursor/keyset pagination.
- Results have deterministic ordering.
- An empty first page of a filtered search may be recovered with at most two relaxed searches. Each relaxed filter is named in `relaxedFilters`, and a recovered page is final (`hasNextPage` is false). A city is never relaxed, and a non-empty page is never relaxed. The rule and its cost are in the [API reference](api.md) and the [guided discovery specification](specs/guided-discovery-and-followup.md) (GDF-01; implemented on the uncommitted tree; Code Reviewer, Security and QA PASS on the uncommitted tree).

### MongoDB Access Pattern

Typical query:

```text
status = OPEN
location.city = ?
skills contains ?
ORDER BY createdAt DESC, _id DESC
LIMIT N
```

The exact compound indexes are derived from measured filter combinations rather than creating an index for every possible field.

### Events

```text
SEARCH_PERFORMED
```

Capture:

```text
searchId
candidateId?
searchMode = FILTER
filters
resultIds
resultCount
occurredAt
```

### Metrics

Business:

```text
searches
zero-result rate
results/search
search → job-view conversion
```

Technical:

```text
p50 latency
p95 latency
p99 latency
docs examined / returned
keys examined / returned
```

### Initial SLO

```text
p95 < 150 ms
p99 < 300 ms
```

excluding external network latency.

---

# UC02 — Semantic / Hybrid Job Search

**Actor:** Candidate

**Goal:** Find relevant jobs using natural-language intent rather than exact field matching.

Example:

```text
"functional Scala backend role working
on distributed systems, preferably remote"
```

### Input

```text
query
location?
status?
skills?
first
```

### GraphQL

```graphql
query SemanticJobSearch(
  $query: String!
  $filter: JobFilter
  $first: Int!
) {
  semanticJobSearch(
    query: $query
    filter: $filter
    first: $first
  ) {
    job {
      id
      title
      skills
      location
    }

    score
  }
}
```

### Business Rules

- Only jobs visible to the candidate can be returned.
- Closed jobs must not appear.
- Structured authorization/filtering is independent of semantic similarity.
- Search model must be identifiable for evaluation.

### Data Flow

```text
Query
  ↓
EmbeddingService
  ↓
MongoDB Vector Search
  ↓
Metadata filters
  ↓
Ranked Jobs
```

### Events

```text
SEARCH_PERFORMED
SEARCH_RESULT_CLICKED
SEARCH_RECORDING_FAILED
```

`SEARCH_RECORDING_FAILED` is emitted only when a search ran but its session could not be recorded. It carries the search id, search kind and a bounded error code or exception class name, never query text or result ids; its publication is best-effort and never fails the search.

Capture:

```text
searchId
query
searchMode
embeddingModel
resultIds
resultRanks
scores
filters
```

### Metrics

```text
Recall@K
Precision@K
MRR
NDCG@K

p95 search latency

CTR@K
search → application conversion
```

### Initial SLO

```text
p95 < 500 ms
```

Quality has no arbitrary initial threshold. Establish a baseline dataset first and compare retrieval strategies against it.

---

# UC03 — View Job Details

**Actor:** Candidate

**Goal:** View complete information for a selected job.

A job the caller may not see (a Draft or Closed job that is not theirs to manage) is answered exactly as a job that does not exist: `job` is `null` with `NOT_FOUND`, the same code and message, so the answer does not reveal whether the job exists.

### GraphQL

```graphql
query Job($id: JobID!) {
  job(id: $id) {
    id
    title
    description
    requirements
    skills
    location
    recruiter {
      id
      name
    }
  }
}
```

### MongoDB Access Pattern

```text
jobs._id = jobId

users._id = recruiterId
```

Recruiter resolution should use request-scoped batching when multiple jobs are requested.

### Event

```text
JOB_VIEWED
```

Capture:

```text
jobId
candidateId?
searchId?
rank?
occurredAt
```

`searchId` connects search results to downstream behavior.

### Metrics

```text
views/job
unique viewers
search → view conversion
view → application conversion
```

### Initial SLO

```text
p95 < 100 ms
```

---

# UC04 — Submit Application

**Actor:** Candidate

**Goal:** Apply to an open job.

### GraphQL

```graphql
mutation SubmitApplication(
  $input: SubmitApplicationInput!
) {
  submitApplication(input: $input) {
    ... on Application {
      id
      status
      createdAt
    }
    ... on ValidationError {
      code
      message
    }
    ... on DomainError {
      code
      message
    }
  }
}
```

### Business Rules

- Candidate must be authenticated.
- Job must exist.
- Job must be `OPEN`.
- Candidate cannot apply to the same job twice.
- Initial status is `CREATED`.

Duplicate protection must exist at database level:

```text
UNIQUE(candidateId, jobId)
```

### Writes

One Mongo transaction, shared with the mutation receipt context (`ApplicationService.submitApplication` via `idempotent.executeFor`; `MongoApplicationRepository.createForOpenJobWithEvents`), in this order:

```text
outbox_subject_fences  upsert, $inc fencingVersion (actor fence; also checks an active
                       Candidate and an active recruiter of the job; MongoApplicationWriteFence)
jobs                   guard: _id matches and status is Open; $inc admissionFence
                       (version and updatedAt are content-only and not written)
                       (the observed revision is not matched)
applications           INSERT (candidate/job unique index)
application_events     INSERT (initial CREATED event)
event_outbox           INSERT (APPLICATION_CREATED)
```

These belong to one logical transaction. A failed job guard, a duplicate application or a failed fence rolls the whole transaction back.

### Events

```text
APPLICATION_CREATED
```

### Metrics

```text
applications/job
applications/day
duplicate attempts
application failure rate
view → application conversion
```

### Initial SLO

```text
p95 < 200 ms
```

---

# UC05 — View Candidate Applications

**Actor:** Candidate

**Goal:** Track own applications and lifecycle history.

### GraphQL

```graphql
query MyApplications(
  $status: ApplicationStatus
  $first: Int!
  $after: Cursor
) {
  myApplications(
    status: $status
    first: $first
    after: $after
  ) {
    edges {
      node {
        id
        status

        job {
          id
          title
        }

        createdAt
      }
    }

    pageInfo {
      hasNextPage
    }
  }
}
```

### MongoDB Access Pattern

```text
candidateId = authenticatedUser
status = ?
ORDER BY createdAt DESC, _id DESC
```

### Security Rule

`candidateId` comes from authenticated context, not from a client-controlled GraphQL argument.

### Metrics

Primarily technical:

```text
p95 latency
docs examined / returned
batch-loader efficiency
```

### Initial SLO

```text
p95 < 150 ms
```

---

# UC06 — Recommend Jobs to Candidate

**Actor:** Candidate

**Goal:** Discover jobs matching the candidate's profile without explicitly searching.

### Input

```text
candidateId = authenticated candidate
first
```

### GraphQL

```graphql
query RecommendedJobs($first: Int!) {
  recommendedJobs(first: $first) {
    job {
      id
      title
      skills
    }

    score
  }
}
```

### Retrieval

Initial MVP:

```text
Candidate Profile
      ↓
Candidate Embedding
      ↓
MongoDB Vector Search
      ↓
OPEN jobs
```

Do not introduce ML ranking initially.

### Events

Reuse:

```text
JOB_VIEWED
APPLICATION_CREATED
```

with recommendation/search context.

### Metrics

```text
recommendation CTR
recommendation → application conversion

Recall@K
NDCG@K
```

### Initial SLO

```text
p95 < 300 ms
```

---

# Recruiter candidate matching

Recruiters search only candidates for jobs they own while the job is open. Matching can combine the owned job's embedding with an optional recruiter query and candidate-profile lexical search. Required skills are ANDed across the candidate's skills.

Current residence and availability are private profile data. Filters on either field apply only to candidates who set `recruiterSearchOptIn`; opted-out or legacy profiles retain their normal vector-search inclusion. Candidates read these fields only in their own profile. Recruiter match results contain name, skills, experience summary, deterministic `matchedSkills` and `missingSkills` (fit reasons derived from the owned job's skills and the candidate skills already in the result; see [API](api.md#ai-search-outcomes)), and score; they do not include residence, availability, email, resume reference, or consent.

Candidate location/status are structured retrieval filters and are excluded from embedding text. The default ranking remains application-side reciprocal-rank fusion. Native MongoDB fusion and reranking require explicit experimental configuration. Automated Embedding is an isolated synthetic-data comparison, not an application write path.

---

# UC07 — Manage Job

**Actor:** Recruiter

**Goal:** Create, modify, publish, or close a job.

`createJob` creates a **Draft** owned by the calling Recruiter. A Draft is visible and editable only to its owner and the Admin (`myJobs`, `job`, `updateJob`, `closeJob`); it does not appear in `jobs`, `nearbyJobs`, `jobDiscoveryFacets`, `semanticJobSearch` or `recommendedJobs` and accepts no applications. `publishJob` moves a Draft to Open, which is the moment the job becomes visible and the first `JOB_CREATED` event is published. Closing a Draft is allowed and leaves a Closed job. A Draft is private, so creating, editing or closing one publishes no operational event; the embedding work item is queued from creation (keyed by the job, so publishing re-queues the same item) and a Draft is never eligible for search until it is Open.

### GraphQL

```graphql
mutation CreateJob($input: CreateJobInput!) {
  createJob(input: $input) {
    job {
      id
      status
    }
  }
}
```

Additional operations:

```text
updateJob
publishJob
closeJob
```

### Business Rules

- Recruiter must be authenticated.
- Recruiter owns created jobs.
- Recruiter may modify only owned jobs.
- Closing a job prevents new applications.
- A job the caller does not manage is answered as a missing one (`NOT_FOUND`); a Candidate calling these Recruiter-only operations is `FORBIDDEN`.

### Events

```text
JOB_CREATED
JOB_UPDATED
JOB_CLOSED
```

Job content changes may also trigger asynchronous embedding regeneration.

```text
JOB_UPDATED
     │
     ▼
Embedding Pipeline
     │
     ▼
MongoDB embedding update
```

### Metrics

```text
jobs created/day
open jobs
average job lifetime
embedding regeneration latency
```

### Initial SLO

```text
mutation p95 < 200 ms
```

Embedding generation is asynchronous and is not included in mutation latency.

---

# UC08 — Recruiter Lists Applications for Job

**Actor:** Recruiter

**Goal:** Review applications for one owned job.

### GraphQL

```graphql
query JobApplications(
  $jobId: JobID!
  $status: ApplicationStatus
  $first: Int!
  $after: Cursor
) {
  jobApplications(
    jobId: $jobId
    status: $status
    first: $first
    after: $after
  ) {
    edges {
      node {
        id
        status

        candidate {
          id
          name
          profile {
            ... on CandidateProfile {
              skills
            }
          }
        }

        createdAt
      }
    }
  }
}
```

### Business Rules

Recruiter must own the requested job.

### MongoDB Access Pattern

```text
jobId = ?
status = ?
ORDER BY createdAt DESC, _id DESC
```

Candidate objects are loaded through request-scoped batching.

### Key Performance Metrics

```text
p95 / p99 latency

totalDocsExamined
totalKeysExamined
nReturned

number of Mongo queries / GraphQL request

DataLoader batch size
```

### Initial SLO

```text
p95 < 150 ms

no N+1 behavior
```

---

# UC09 — Change Application Status

**Actor:** Recruiter (owning the application's job) or Admin for every action; the application's own **Candidate** for `declineApplication` only.

**Goal:** Move an application through its hiring lifecycle.

`declineApplication` is also the Candidate's withdrawal: the candidate who owns the application can decline it, with a required reason, under the same permitted-transition matrix (only `CREATED` can be declined). The history entry records the candidate as the actor. A candidate has no other status power; `acceptApplication`, `moveApplicationToInterview`, `rejectApplication` and `hireApplication` answer a Candidate `FORBIDDEN`. Another candidate, and a recruiter who does not own the job, get `NOT_FOUND`, exactly the answer for an unknown application, and nothing is changed.

### GraphQL

Prefer action-oriented mutations:

```graphql
mutation MoveToInterview($input: ApplicationActionInput!) {
  moveApplicationToInterview(input: $input) {
    ... on Application {
      id
      status
    }
    ... on ValidationError { code message field }
    ... on DomainError { code message }
  }
}
```

Similar mutations:

```text
acceptApplication
declineApplication
rejectApplication
hireApplication
```

### Business Rules

Transitions are explicitly validated.

Example:

```text
CREATED
   │
   ├──► ACCEPTED ──► INTERVIEW ──► HIRED
   │                       │
   ├──► DECLINED           └──► REJECTED
   │
   └──► REJECTED
```

Exact permitted transitions should be encoded as domain rules.

Additional rules from the original specification include feedback/reason requirements for rejection/decline.

### Transaction

```text
BEGIN

validate current status
        ↓
update applications.status
        +
insert application_event

COMMIT
```

### Event

```text
APPLICATION_STATUS_CHANGED
```

### Metrics

```text
transition count/status
invalid transition attempts
transition latency

stage conversion
stage duration
```

### Initial SLO

```text
p95 < 200 ms
```

### Interview cancellation and rescheduling

Contract, authorization matrix and error codes: [API reference](api.md#interview-scheduling-cancellation-and-rescheduling). Behavior and decisions: [durable hiring workflows](specs/durable-hiring-workflows.md).

- **Cancel is a rejection.** `cancelInterview` by the application's candidate, the owning recruiter or Admin moves the application `Interview → Rejected` through the existing permitted transition (no new transition) in one transaction with its history entry, the existing `statusChanged` event and the durable provider-cancel intent. The rejection feedback is system-generated per initiator (for example "Interview cancelled by the candidate."); no text is accepted. The calendar slot counts as released only after the provider confirms the cancel, and both participants are notified after that. A candidate can cancel only through this mutation; they still cannot use `rejectApplication`.
- **Candidate request.** `requestInterviewReschedule` is a bare flag with a timestamp that notifies the recruiter; it changes no slot or status. The owning recruiter or Admin clears it with `dismissInterviewRescheduleRequest`, or by proposing.
- **Proposal flow.** The owning recruiter or Admin calls `proposeInterviewReschedule` with a new UTC interval. Nothing is held yet: the old slot is the only reservation. The candidate then accepts or declines; the recruiter or Admin may withdraw. The proposal also expires at the earliest of `now + proposal-ttl-seconds` (72 hours by default), the current interview start and the proposed start. Decline, withdrawal and expiry return the workflow to `Completed` with the old slot intact and notify the other party. On acceptance the replacement is held first, the live interval swaps once, the old slot is confirm-cancelled and both participants are notified; if the replacement conflicts, the old slot stays and both are told. One proposal may be open at a time.
- **Races and retries.** Every action is a revision compare-and-set plus an idempotent request key: of competing cancel, propose, accept, decline, withdraw and expiry exactly one wins; the others get a typed outcome and no provider effect.
- **Delivery guarantee.** The local ledger keeps one receipt per idempotency key, so each notification is recorded once and a repeat returns the first receipt. A provider with its own idempotency mechanism is applied at most once per key. The prepared Amazon SES email adapter has neither a key nor a lookup, so it is at least once (owner decision 2026-11-10): an unknown outcome with no receipt may be sent again, a duplicate notification is possible and a silent loss is not. An exhausted notification becomes visible repair for Admin. See [provider idempotency contract](wiki/workflows-and-patterns.md#duplicate-delivery-containment) and the [SES operations note](ses-notification-operations.md).
- **Abuse limit.** Interview actions are rate-limited per authenticated actor, counting each aliased action.
- **Known gap.** `rejectApplication` and `hireApplication` do not touch a live interview slot.

---

# UC10 — Semantic Candidate Search

**Actor:** Recruiter

**Goal:** Find candidates relevant to an owned job.

### GraphQL

```graphql
query CandidateMatches(
  $jobId: JobID!
  $first: Int!
) {
  candidateMatches(
    jobId: $jobId
    first: $first
  ) {
    candidate {
      id
      name
      profile {
        ... on CandidateMatchProfile {
          skills
        }
      }
    }

    score
  }
}
```

### Business Rules

Recruiter must own the job.

Candidate visibility rules must be applied before exposing candidate information.

### Retrieval

```text
Job Embedding
     ↓
MongoDB Vector Search
     ↓
Candidate Profiles
     ↓
Ranked Candidates
```

### Metrics

```text
Recall@K
NDCG@K
MRR

result → application-review rate
result → interview rate

p95 latency
```

### Initial SLO

```text
p95 < 500 ms
```

---

# UC11 — Publish and Process Domain Events

**Actor:** System

**Goal:** Reliably transfer operational events into analytical pipelines without coupling transactional operations to Spark/Databricks.

### Event Flow

```text
Domain Operation
       │
       ▼
MongoDB
       │
       ▼
Event Publication
       │
       ▼
Kafka
       │
       ▼
Consumers
```

### Event Envelope

```text
eventId
eventType
occurredAt
aggregateType
aggregateId
actorId
payload
```

### Delivery Model

Assume:

```text
at-least-once delivery
        +
idempotent consumers
```

Application events use:

```text
partition key = applicationId
```

when per-application ordering is required.

### Metrics

```text
events/sec
consumer lag
duplicate events
failed events
retry count
dead-letter/quarantine count
end-to-end event latency
```

### Initial SLO

```text
no acknowledged event loss

p95 operational-event
availability in Bronze < 30 sec
```

---

# UC12 — Hiring Funnel Analytics

**Actor:** Admin / Analytics

**Goal:** Analyze movement through the hiring lifecycle.

### Input

```text
date range (at most 30 days)
```

### Output

Daily, system-wide counts of the stages applications reached (UTC day of the event):

```text
created
accepted
declined
interview
hired
rejected
```

A count is the number of distinct applications that entered that status on that day, so an application rejected after an interview counts in both `interview` and `rejected`. A day is published only when every non-zero count comes from at least 10 distinct candidates; otherwise the whole day is withheld.

Post-MVP (not built): dimensions (job, recruiter, location, skill) and the conversion rates (application → interview, interview → hire, hire rate, rejection rate).

### Pipeline

```text
Bronze Events
      ↓
Silver Application Events
      ↓
Spark Aggregation
      ↓
Gold hiring_funnel_metrics
```

### Spark Topics Exercised

```text
groupBy
joins
aggregations
partition pruning
shuffle analysis
```

### Metrics

Pipeline:

```text
input rows
output rows
shuffle size
execution duration
records rejected
```

### Initial SLO

For batch analytics:

```text
Gold datasets refreshed < 15 min
after scheduled pipeline start
```

---

# UC13 — Time-to-Hire Analytics

**Actor:** Admin / Analytics

**Goal:** Measure how long candidates spend in each hiring stage.

### Source

Ordered application lifecycle events.

```text
applicationId | status     | occurredAt
---------------------------------------
A1            | CREATED    | T1
A1            | ACCEPTED   | T2
A1            | INTERVIEW  | T3
A1            | HIRED      | T4
```

### Derived Metrics

```text
timeToAccept     = T2 - T1
timeToInterview  = T3 - T1
timeToHire       = T4 - T1

acceptedToInterview = T3 - T2
interviewToHire     = T4 - T3
```

Calculate:

```text
median
P75
P90
P95
```

rather than relying only on averages.

### Spark Features

```text
Window
partitionBy(applicationId)
orderBy(occurredAt)

lag / lead
aggregations
percentiles
```

### Gold Dataset

```text
time_to_hire_metrics
```

The dimensions for this dataset remain to be specified in its implementation slice.

## Geographic discovery and scheduled interviews

Authenticated discovery combines onsite radius with city, skills and creation-date filters and returns deterministic distance/ID ordering. Exact structured facets count the complete eligible filter set before pagination. Candidates and Admin may also pass an optional text `query` to `jobDiscoveryFacets`; Recruiters are denied. With a query, counts cover the Open jobs matching the text and the same structured filter, recounted from the authoritative jobs collection rather than the search index. The `coverageLimit` field states the scope: null means every match was counted, while a value (default 1000) means only the highest-scoring matches up to that limit were counted and the counts are never a total. Facets ignore the embedding-model predicate, and a job opened after the last search index update is absent until indexed. Separately requested hits and facets have no shared-snapshot promise. Private candidate attributes are neither discovery facets nor ranking features.

A recruiter schedules an Accepted application for an owned job with a future UTC interval and idempotency key. The durable workflow reserves both participants, atomically advances Accepted to Interview with history and notification intent, then delivers both notifications. Candidates inspect their own workflows; recruiters inspect owned-job workflows; Admin repairs with expected revision and idempotency after reconciliation. Repair does not reopen released reservations or extend the original deadline; such work can return to visible repair without a hiring commit. Adjacent calendar reservations are permitted; overlapping participant intervals are rejected. Cancellation and rescheduling are verified locally (DHW-14–34, independent QA PASS); the real calendar provider and live SES remain deferred; the SES adapter is prepared (stub-tested, at least once) and not activatable.
