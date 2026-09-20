# Big Data Architecture

Target design and acceptance criteria; see [core architecture](../ARCHITECTURE.md) and [development plan](../INITIAL_DEVELOPMENT_PLAN.md). These are not claims of implemented features.

# 31. Big Data Architecture

The operational and analytical workloads are intentionally separated.

MongoDB remains the source of truth for current application state and low-latency GraphQL operations.

Historical domain events are processed independently by the analytical platform.

```text
                        ┌─────────────────┐
                        │ GraphQL/Sangria │
                        └────────┬────────┘
                                 │
                          Application
                          Cats Effect
                                 │
                    ┌────────────┴────────────┐
                    ▼                         ▼
              ┌───────────┐              ┌─────────┐
              │  MongoDB  │              │  Kafka  │
              │   OLTP    │              │ Events  │
              └─────┬─────┘              └────┬────┘
                    │                         │
                    │                         ▼
                    │                Spark Structured
                    │                    Streaming
                    │                         │
                    │                         ▼
                    │                 ┌──────────────┐
                    │                 │  Delta Lake  │
                    │                 └──────┬───────┘
                    │                        │
                    │               ┌────────┼────────┐
                    │               ▼        ▼        ▼
                    │            Bronze    Silver    Gold
                    │
                    └────► MongoDB Vector Search
```

This architecture deliberately separates:

```text
OLTP                         OLAP

MongoDB                      Databricks / Delta Lake
current state                historical events
point/range queries          large analytical scans
low latency                  distributed computation
GraphQL operations           Spark SQL/DataFrames
vector retrieval             offline search evaluation
```

The analytical platform must never become a dependency of transactional operations such as submitting an application or changing its status.

---

# 32. Domain Event Architecture

Important domain operations produce immutable events.

Kafka-facing work follows an event-driven architecture pattern. Operational services own current state in MongoDB and expose immutable domain events through a durable handoff, such as an outbox, before Kafka publication. Kafka consumers must be idempotent, replayable, version-aware, and tolerant of duplicate, late, and out-of-order records.

Examples:

```text
JOB_CREATED
JOB_UPDATED
JOB_VIEWED

SEARCH_PERFORMED

APPLICATION_CREATED
APPLICATION_ACCEPTED
APPLICATION_INTERVIEWED
APPLICATION_REJECTED
CANDIDATE_HIRED
```

Canonical envelope:

```json
{
  "eventId": "...",
  "eventType": "APPLICATION_STATUS_CHANGED",
  "schemaVersion": 1,
  "occurredAt": "...",

  "applicationId": "...",
  "candidateId": "...",
  "jobId": "...",
  "recruiterId": "...",

  "payload": {
    "previousStatus": "ACCEPTED",
    "newStatus": "INTERVIEW"
  }
}
```

Every event has a globally unique `eventId`.

Consumers must be designed to tolerate duplicate delivery.

The architecture therefore assumes:

> at-least-once delivery + idempotent processing

rather than relying on unrealistic global exactly-once assumptions.

---

# 33. Kafka Design

Kafka acts as the boundary between transactional and analytical workloads.

Partition keys must preserve ordering where ordering is required.

Phase 5 uses one local topic:

```text
hiring.operational-events.v1
```

Application lifecycle events use `key = applicationId`, job facts use `key = jobId`, and search/session interaction facts use `key = searchId`. This guarantees that events for one aggregate key are assigned to the same partition. There is no ordering guarantee across different keys.

Different applications can still be processed concurrently.

Operational services write MongoDB state and immutable outbox records in the same transaction. A resource-owned fs2-kafka publisher uses an idempotent producer with `acks=all`; acknowledgement-loss retry republishes the same `eventId`. The Phase 5 consumer records a Mongo receipt keyed by `(consumerGroup, eventId)` before committing the Kafka offset. Malformed, unsupported-version, or invalid-ordering records are written to a seven-day quarantine with topic, partition, offset, category, and raw bytes.

Important concepts intentionally demonstrated:

- topic design
- partition keys
- consumer groups
- ordering guarantees
- retries
- duplicate delivery
- idempotency
- backpressure
- failure recovery

Kafka is not used as a replacement for MongoDB.

MongoDB owns current application state.

Kafka represents the event stream.

---

# 34. Lakehouse Architecture

Databricks and Delta Lake use a Medallion architecture.

```text
Kafka
  │
  ▼
BRONZE
  │
  │ validation
  ▼
SILVER
  │
  │ aggregation
  ▼
GOLD
```

## Bronze

Contains raw immutable events with minimal transformation.

```text
eventId
eventType
schemaVersion
occurredAt
ingestedAt
payload
```

Bronze provides:

- replay capability
- debugging
- historical auditability
- recovery from transformation bugs

---

## Silver

Contains validated, deduplicated, normalized and enriched events.

Example application-event dataset:

```text
eventId
applicationId
candidateId
jobId
recruiterId

previousStatus
newStatus

jobTitle
jobSkills
location

eventTimestamp
```

Processing includes:

- schema validation
- duplicate removal
- timestamp normalization
- reference enrichment
- invalid-record detection

---

## Gold

Contains datasets optimized for analytical consumption.

Examples:

```text
hiring_funnel_daily

time_to_hire_metrics

job_conversion_metrics

skill_demand_trends

search_quality_experiments
```

Gold tables are derived data and can be rebuilt from lower layers.

---

# 35. Spark Batch Processing

Spark handles workloads where distributed processing provides a concrete benefit.

Primary examples:

### Hiring Funnel

Calculate:

```text
Applications
     ↓
Accepted
     ↓
Interview
     ↓
Hired
```

Metrics include:

```text
application_to_interview_rate
interview_to_hire_rate
rejection_rate
time_to_interview
time_to_hire
```

Dimensions include:

```text
job
recruiter
location
skill
time period
```

---

### Time-to-Hire

Application events provide an ordered lifecycle:

```text
CREATED
   │
   ▼
ACCEPTED
   │
   ▼
INTERVIEW
   │
   ▼
HIRED
```

Spark window functions reconstruct lifecycle durations and calculate distributions across large datasets.

This demonstrates:

- partitioning
- window functions
- joins
- aggregations
- shuffle behavior
- execution-plan analysis

---

# 36. Spark Structured Streaming

Near-real-time metrics are processed directly from Kafka.

```text
Kafka
   │
   ▼
Spark Structured Streaming
   │
   ├── deduplicate
   ├── validate
   ├── watermark
   ├── aggregate
   │
   ▼
Delta Lake
```

The implementation explicitly handles:

- event time vs processing time
- late-arriving events
- watermarks
- checkpointing
- duplicate events
- restart recovery

These concerns are part of the architecture rather than hidden behind a trivial streaming example.

---

# 37. Data Quality Pipeline

Invalid events must not silently enter analytical datasets.

```text
                     Raw Events
                         │
                         ▼
                    Validation
                    ┌────┴────┐
                    ▼         ▼
                  VALID     INVALID
                    │         │
                    ▼         ▼
                  Silver   Quarantine
```

Validation includes:

```text
valid event schema

unique eventId

valid timestamps

required identifiers

valid application statuses

valid lifecycle transitions
```

Quality metrics are recorded for every pipeline execution:

```text
records_received
records_valid
records_rejected
duplicate_events
late_events
invalid_schema
```

---

# 38. Search Evaluation Pipeline

Vector search quality is measured offline.

The evaluation dataset contains:

```text
query
relevantJobIds
```

Different retrieval strategies are executed against the same dataset:

```text
                    Evaluation Queries
                           │
             ┌─────────────┼─────────────┐
             ▼             ▼             ▼
          Keyword        Vector        Hybrid
           Search         Search        Search
             │             │             │
             └─────────────┼─────────────┘
                           ▼
                         Spark
                           │
                           ▼
                Retrieval Quality Metrics
```

Metrics include:

```text
Precision@K

Recall@K

Mean Reciprocal Rank

NDCG@K
```

Every experiment records:

```text
experimentId

embeddingModel
embeddingVersion

retrievalStrategy

searchParameters

datasetVersion

precisionAtK
recallAtK
mrr
ndcg

createdAt
```

This makes search improvements reproducible and measurable.

---

# 39. Large-Scale Test Data

The project includes deterministic synthetic-data generation.

Target datasets can contain:

```text
1M candidates

500K jobs

20M applications

100M+ domain events
```

Large datasets are not required for normal local development.

They exist specifically for:

- Spark partitioning experiments
- join-strategy analysis
- shuffle analysis
- storage-layout experiments
- MongoDB index benchmarking
- streaming throughput tests
- vector-search evaluation

Generation must be deterministic so benchmark results remain reproducible.

---

# 40. Performance Engineering

Big Data work must include measurement rather than only functional correctness.

Spark pipelines should inspect:

```text
physical execution plans

partition counts

shuffle read/write

data skew

join strategies

predicate pushdown

partition pruning

spill

task duration
```

MongoDB workloads independently measure:

```text
executionTimeMillis

totalKeysExamined

totalDocsExamined

nReturned
```

Streaming workloads measure:

```text
input rows / second

processed rows / second

batch duration

consumer lag

late events

failed events
```

The objective is not simply to use distributed technologies but to demonstrate why particular partitioning, indexing, storage, and processing decisions were made.

---

# 41. Big Data Scope Rules

The Big Data subsystem follows several constraints.

1. MongoDB remains the operational source of truth.

2. Kafka transports domain events; it does not replace transactional persistence.

3. Spark is used only for workloads that benefit from distributed processing.

4. Bronze data is immutable.

5. Silver data is validated and deduplicated.

6. Gold data is derived and rebuildable.

7. Consumers tolerate duplicate event delivery.

8. Event ordering is guaranteed only where the partitioning strategy guarantees it.

9. Streaming pipelines explicitly handle late events and recovery.

10. Search quality is measured using reproducible datasets and retrieval metrics.

11. Performance claims must be supported by measurements.

12. New infrastructure must solve an identified architectural problem rather than merely increase the number of technologies used.
