# MongoDB Design

Target design and acceptance criteria; see [core architecture](../ARCHITECTURE.md) and [development plan](../INITIAL_DEVELOPMENT_PLAN.md). These are not claims of implemented features.

# 25. MongoDB Schema Design Patterns

MongoDB persistence is designed around **GraphQL access patterns, relationship cardinality, data ownership, and update frequency** rather than relational normalization.

The default strategy is:

> **Embed bounded data with the same lifecycle. Reference independently changing or high-cardinality data. Denormalize only when measured read performance justifies the additional consistency cost.**

This keeps documents efficient while minimizing unnecessary duplication.

## 25.1 Pattern Overview

| Collection | Patterns | Purpose |
|---|---|---|
| `users` | Polymorphic + Embedded | Store all user roles together while embedding user-owned profile data |
| `jobs` | Reference + Embedded | Reference recruiter while embedding job-owned value objects |
| `applications` | Reference | Model high-cardinality Candidate ↔ Job relationships |
| `application_events` | Reference / Unbounded Data Separation | Keep append-only history outside application documents |
| Embeddings | Embedded | Keep vectors directly with searchable entities |
| Analytics | Computed, later | Precompute expensive aggregates if required |
| All collections | Schema Versioning, optional | Support document evolution without mandatory bulk migrations |

---

## 25.2 Reference Pattern

Independent entities are connected using identifiers rather than duplicated documents.

```text
User
  ▲
  │ recruiterId
Job
  ▲
  │ jobId
Application ───────► User
  │                  candidateId
  │
  ▼
ApplicationEvent
```

Example:

```javascript
// jobs
{
  _id: UUID,
  recruiterId: UUID,
  title: "Senior Scala Developer"
}

// applications
{
  _id: UUID,
  candidateId: UUID,
  jobId: UUID,
  status: "INTERVIEW"
}
```

### Why

`User`, `Job`, and `Application`:

- have independent lifecycles
- may change independently
- participate in high-cardinality relationships
- may be queried independently
- require independent pagination

For example, applications must not be embedded inside jobs:

```javascript
// Avoid
{
  _id: jobId,

  applications: [
    {...},
    {...},
    // potentially thousands
  ]
}
```

This would introduce:

- unbounded arrays
- growing documents
- write contention
- difficult pagination
- unnecessary document reads
- potential document-size problems

References keep these relationships scalable.

---

## 25.3 Embedded Document Pattern

Small, bounded value objects owned by one aggregate are embedded.

Example:

```javascript
{
  _id: jobId,

  location: {
    country: "Ukraine",
    city: "Kyiv",
    remote: true
  },

  requirements: [
    "Scala",
    "Functional programming"
  ],

  skills: [
    "Scala",
    "Cats Effect",
    "MongoDB"
  ]
}
```

Candidate profile data follows the same rule:

```javascript
{
  _id: candidateId,

  profile: {
    skills: [...],
    experienceSummary: "...",
    resumeRef: "..."
  }
}
```

### Why

These values:

- belong exclusively to their parent
- are bounded in size
- are usually read together with the parent
- do not require independent identity
- do not require independent lifecycle management

GraphQL can therefore resolve:

```graphql
job {
  title
  location
  skills
  requirements
}
```

from one MongoDB document.

---

## 25.4 Polymorphic Pattern

Candidates, recruiters, and admins share one collection:

```text
users
```

rather than:

```text
candidates
recruiters
admins
```

Example:

```javascript
{
  _id: UUID,
  role: "CANDIDATE",
  email: "candidate@example.com",
  profile: {...}
}
```

or:

```javascript
{
  _id: UUID,
  role: "RECRUITER",
  email: "recruiter@example.com"
}
```

### Why

All roles share:

- identity
- authentication
- email
- common user metadata
- authorization concepts

It also allows one database constraint:

```javascript
db.users.createIndex(
  { email: 1 },
  { unique: true }
)
```

instead of coordinating uniqueness across several collections.

Role-specific data can remain optional embedded structures where appropriate.

---

## 25.5 Unbounded Data Separation

Application history is append-heavy and potentially unbounded.

Avoid:

```javascript
{
  _id: applicationId,

  status: "INTERVIEW",

  history: [
    {...},
    {...},
    {...}
  ]
}
```

Instead use:

```text
applications
        │
        │ applicationId
        ▼
application_events
```

Event:

```javascript
{
  _id: UUID,

  applicationId: UUID,

  type: "STATUS_CHANGED",

  previousStatus: "ACCEPTED",
  newStatus: "INTERVIEW",

  changedBy: UUID,

  feedback: null,
  reason: null,

  createdAt: ISODate(...)
}
```

Index:

```javascript
{
  applicationId: 1,
  createdAt: -1
}
```

### Why

This prevents continuously growing application documents and provides efficient:

- history pagination
- auditing
- chronological queries
- append-heavy writes

Application status and history are updated atomically using a MongoDB transaction.

```text
Transaction

update application status
          +
insert application event
```

---

## 25.6 Subset Pattern

The Subset Pattern is intentionally **not used initially**.

For example, we could duplicate candidate information into applications:

```javascript
{
  _id: applicationId,

  candidateId: UUID,

  candidateSnapshot: {
    name: "John Smith",
    headline: "Senior Scala Engineer"
  }
}
```

This could make application-list queries faster.

However, candidate updates would create synchronization problems.

Initial implementation therefore uses:

```text
Application
     │
     │ candidateId
     ▼
    User
```

GraphQL relationship resolution uses request-scoped batching.

The Subset Pattern should only be introduced when profiling demonstrates that batched lookups are a meaningful bottleneck.

---

## 25.7 Extended Reference Pattern

Extended References are another possible future optimization.

Initial design:

```javascript
{
  recruiterId: UUID
}
```

Potential optimized design:

```javascript
{
  recruiter: {
    id: UUID,
    name: "Jane Smith",
    companyName: "Acme"
  }
}
```

This can eliminate a lookup when displaying jobs.

However:

```text
Recruiter changes name
          │
          ▼
Potentially update N jobs
```

The initial MVP therefore uses normal references.

Extended References are introduced only when:

```text
read-performance benefit
          >
duplication + synchronization cost
```

---

## 25.8 Computed Pattern

Admin analytics initially use MongoDB aggregation pipelines.

Example:

```text
applications
      │
      ▼
$match
      │
      ▼
$group
      │
      ▼
status counts
```

If dashboard queries become expensive, introduce precomputed statistics:

```javascript
{
  _id: "platform-stats",

  jobs: {
    total: 15231,
    open: 4312
  },

  applications: {
    total: 186532,
    hired: 5281,
    rejected: 81231
  },

  updatedAt: ISODate(...)
}
```

### Why

Analytics data often changes much less frequently than it is read.

The Computed Pattern trades additional write-side processing for:

- constant-time dashboard reads
- fewer large aggregation pipelines
- predictable GraphQL latency

It should only be introduced after profiling demonstrates the need.

---

## 25.9 Embedded Vector Pattern

Vector embeddings live directly with the entity they represent.

Job:

```javascript
{
  _id: UUID,

  title: "...",
  description: "...",
  requirements: [...],
  skills: [...],

  embedding: [...],

  embeddingMeta: {
    model: "...",
    version: 1,
    sourceHash: "...",
    updatedAt: ISODate(...)
  }
}
```

Candidate:

```javascript
{
  _id: UUID,

  profile: {...},

  embedding: [...],

  embeddingMeta: {
    model: "...",
    version: 1,
    sourceHash: "...",
    updatedAt: ISODate(...)
  }
}
```

### Why

Vector Search returns the actual domain document directly:

```text
Vector Search
      │
      ▼
     Job
```

rather than:

```text
Vector Search
      │
      ▼
Embedding Document
      │
      ▼
additional Job lookup
```

This reduces query complexity and avoids an unnecessary collection.

A separate embedding collection should only be considered if one entity eventually requires many independent embeddings, chunk-level retrieval, or substantially different embedding lifecycles.

---

## 25.10 Schema Versioning Pattern

Documents may optionally contain:

```javascript
{
  schemaVersion: 1,
  ...
}
```

This becomes useful when document structure evolves.

Example:

```text
schemaVersion 1
      │
      ▼
decoderV1

schemaVersion 2
      │
      ▼
decoderV2
```

It allows gradual document evolution without requiring every schema change to trigger an immediate full-collection migration.

This is particularly useful for evolving AI metadata and candidate profiles.

---

# 26. Index Design Strategy

Indexes are derived from **GraphQL access patterns**, not individual fields.

Do not create indexes simply because fields are searchable.

For every important GraphQL operation, document:

```text
GraphQL Operation
       │
       ▼
MongoDB Query Shape
       │
       ▼
Sort Order
       │
       ▼
Compound Index
       │
       ▼
explain("executionStats")
```

---

## 26.1 Jobs

### Public job search

```graphql
jobs(
  status: OPEN
  location: "Kyiv"
  first: 20
  after: "..."
)
```

Index:

```javascript
{
  status: 1,
  "location.city": 1,
  createdAt: -1,
  _id: -1
}
```

This supports:

- equality filtering
- cursor ordering
- deterministic pagination

---

### Recruiter's jobs

```graphql
recruiter {
  jobs(first: 20)
}
```

Index:

```javascript
{
  recruiterId: 1,
  createdAt: -1,
  _id: -1
}
```

---

## 26.2 Applications

### Duplicate prevention

A candidate may apply to the same job only once.

```javascript
db.applications.createIndex(
  {
    candidateId: 1,
    jobId: 1
  },
  {
    unique: true
  }
)
```

This is both an index and a database-enforced domain invariant.

---

### Candidate application list

```graphql
candidate {
  applications(
    status: INTERVIEW
    first: 20
  )
}
```

Index:

```javascript
{
  candidateId: 1,
  status: 1,
  createdAt: -1,
  _id: -1
}
```

---

### Applications for a job

```graphql
job {
  applications(
    status: CREATED
    first: 20
  )
}
```

Index:

```javascript
{
  jobId: 1,
  status: 1,
  createdAt: -1,
  _id: -1
}
```

---

## 26.3 Application Events

```graphql
application {
  history(first: 20)
}
```

Index:

```javascript
{
  applicationId: 1,
  createdAt: -1,
  _id: -1
}
```

---

# 27. Index Ordering Rule

Compound indexes should generally follow:

```text
Equality
   ↓
Sort
   ↓
Range
```

For example:

```text
candidateId = ?
status      = ?
ORDER BY createdAt DESC, _id DESC
```

maps naturally to:

```javascript
{
  candidateId: 1,
  status: 1,
  createdAt: -1,
  _id: -1
}
```

The `_id` field provides deterministic ordering when multiple documents have identical timestamps.

---

# 28. Avoid Over-Indexing

Every index improves some reads but increases:

- write latency
- storage consumption
- memory pressure
- index maintenance
- update cost

Therefore avoid speculative indexes such as:

```javascript
{ status: 1 }

{ createdAt: 1 }

{ location: 1 }

{ skills: 1 }

{ title: 1 }
```

unless an actual query requires them.

Prefer a smaller number of compound indexes designed around concrete GraphQL operations.

---

# 29. Index Verification

Every performance-sensitive repository query should be tested using:

```javascript
.explain("executionStats")
```

Monitor:

```text
executionTimeMillis

totalKeysExamined

totalDocsExamined

nReturned
```

A useful efficiency signal is:

```text
totalDocsExamined
──────────────────
    nReturned
```

For selective indexed queries this ratio should remain reasonably close to `1`.

Large differences indicate that the query/index combination should be investigated.

---

# 30. Final Modeling Rules

Use these rules when introducing new MongoDB data:

1. **Embed** bounded data owned exclusively by the parent.

2. **Reference** entities with independent lifecycles.

3. Never embed potentially **unbounded collections**.

4. Prefer references for **high-cardinality relationships**.

5. Use GraphQL batching before introducing data duplication to solve N+1 problems.

6. Use **Extended Reference / Subset patterns only after measuring a real bottleneck**.

7. Keep vectors with their owning entity while one vector represents one entity.

8. Use the **Computed Pattern** only for expensive, frequently requested aggregates.

9. Design indexes from complete **query shapes**, not isolated fields.

10. Enforce critical invariants such as duplicate applications with **unique indexes**.

11. Prefer cursor/keyset pagination over `skip`.

12. Validate important queries with `explain("executionStats")`.

13. Treat every additional index and every duplicated field as a performance trade-off that must have a concrete access-pattern justification.
