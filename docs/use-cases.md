# MVP Use Cases

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

## Planned Discovery Extensions (Phase 10)

The following are later extensions to structured, semantic, and hybrid discovery; they preserve the core hiring invariants described below. They are sequenced in Phase 10 after MongoDB optimization (Phase 8) and Search Evaluation (Phase 9), before further Spark work. This promotes the formerly post-MVP discovery stage without declaring full MVP complete. Establish relevance measures and a bounded, representative baseline before evaluating personalized ranking.

The [hiring discovery specification](specs/hiring-discovery-search-quality.md) owns detailed desired behavior, public-contract alternatives, boundaries and acceptance criteria. [Durable hiring workflows](specs/durable-hiring-workflows.md) separately defines conditional interview scheduling; it does not change the current `moveApplicationToInterview` contract or add application-status transitions. See the [remaining specification index](development-milestones.md#remaining-delivery-order-and-acceptance) for operational resilience and analytics delivery.

- Radius-based job search combines distance with existing filters, candidate visibility, and deterministic cursor pagination. Inputs include structured coordinates; geocoding is out of scope.
- Richer lexical search and facets use Atlas Search alongside existing vector and hybrid search. Validate lexical, vector, and hybrid behavior in an Atlas-capable environment.
- MongoDB query plans and indexes are evaluated against representative queries and measured workloads, accounting for read gains and write/storage costs.
- Personalized ranking is compared with the relevance baseline and is adopted only if evidence shows a quality improvement; otherwise, defer it.

Atlas is required for this stage's Atlas Search and Vector Search capabilities. Local MongoDB Community remains suitable for core workflows and local transaction tests. This roadmap adds no Atlas provisioning or spend.

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
  $after: String
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
```

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

### GraphQL

```graphql
query Job($id: ID!) {
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

```text
applications INSERT

application_events INSERT
```

These belong to one logical transaction.

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
  $after: String
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

Current residence and availability are private profile data. Filters on either field apply only to candidates who set `recruiterSearchOptIn`; opted-out or legacy profiles retain their normal vector-search inclusion. Candidates read these fields only in their own profile. Recruiter match results contain name, skills, experience summary, deterministic `matchedSkills`, and score; they do not include residence, availability, email, resume reference, or consent.

Candidate location/status are structured retrieval filters and are excluded from embedding text. The default ranking remains application-side reciprocal-rank fusion. Native MongoDB fusion and reranking require explicit experimental configuration. Automated Embedding is an isolated synthetic-data comparison, not an application write path.

---

# UC07 — Manage Job

**Actor:** Recruiter

**Goal:** Create, modify, publish, or close a job.

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
closeJob
```

### Business Rules

- Recruiter must be authenticated.
- Recruiter owns created jobs.
- Recruiter may modify only owned jobs.
- Closing a job prevents new applications.

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
  $jobId: ID!
  $status: ApplicationStatus
  $first: Int!
  $after: String
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

**Actor:** Recruiter

**Goal:** Move an application through its hiring lifecycle.

### GraphQL

Prefer action-oriented mutations:

```graphql
mutation MoveToInterview(
  $applicationId: ID!
) {
  moveApplicationToInterview(
    applicationId: $applicationId
  ) {
    application {
      id
      status
    }
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

---

# UC10 — Semantic Candidate Search

**Actor:** Recruiter

**Goal:** Find candidates relevant to an owned job.

### GraphQL

```graphql
query CandidateMatches(
  $jobId: ID!
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
date range

optional dimensions:
job
recruiter
location
skill
```

### Output

```text
applications
accepted
interviews
hired
rejected

application → interview conversion
interview → hire conversion
```

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

Business:

```text
stage conversion rates
hire rate
rejection rate
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

Authenticated discovery combines onsite radius with city, skills and creation-date filters and returns deterministic distance/ID ordering. Exact structured facets count the complete eligible filter set before pagination; separately requested hits and facets have no shared-snapshot promise. Private candidate attributes are neither discovery facets nor ranking features.

A recruiter schedules an Accepted application for an owned job with a future UTC interval and idempotency key. The durable workflow reserves both participants, atomically advances Accepted to Interview with history and notification intent, then delivers both notifications. Candidates inspect their own workflows; recruiters inspect owned-job workflows; Admin repairs with expected revision and idempotency after reconciliation. Repair does not reopen released reservations or extend the original deadline; such work can return to visible repair without a hiring commit. Adjacent calendar reservations are permitted; overlapping participant intervals are rejected. Real providers and cancellation/rescheduling remain deferred.
