# Local observability

Opt-in, local-workstation-only stack for looking at the API's metrics, traces and logs. Signal definitions, privacy and cardinality rules live in [hiring-observability-resilience](specs/hiring-observability-resilience.md); this page is the runbook and the record of what is and is not verified.

## Components

| Signal | Producer | Transport | Store | Viewer |
|---|---|---|---|---|
| HTTP metrics | `TelemetryRuntime` (otel4s) | OTLP/HTTP to the collector | Prometheus (scrapes the collector) | Grafana "Hiring API - RED" |
| Backlog metrics | `BacklogSampler` / `BacklogMetrics` | same | Prometheus | Grafana "Hiring Backlogs" |
| Traces | otel4s `ServerMiddleware` | same, then OTLP/gRPC to Tempo | Tempo | Grafana Explore → Tempo |
| Logs | API container stdout | Alloy via the Docker API | Loki | Grafana Explore → Loki, `{service_name="api"}` |

Files: `compose.observability.yaml`, `docker/observability/**`, `scripts/generate-local-traffic.sh`.

## Metric list (application instruments)

OpenTelemetry names as defined in code. The Prometheus names below were **observed** on a running stack on 2026-11-02 (HEAD 5193b81; first-run checklist): dots become underscores, a unit suffix is added, counters get `_total`. Observed examples: `hiring_graphql_root_field_executions_total{graphql_operation_type,hiring_graphql_root_field,hiring_graphql_outcome}` (seen: `_invalid`/`rejected`, `_introspection`/`success`), `db_client_operation_duration_seconds_{bucket,count,sum}{db_operation_name,db_collection_name,hiring_mongo_client}`, `jvm_*` (memory, gc, threads, class, cpu), `cats_effect_runtime_cpu_starvation_*`, `cats_effect_runtime_wstp_compute_*`. Series not yet seen under traffic (for example `error_type`, the other outcome classes) are still derived.

| Instrument (OpenTelemetry) | Type, unit | Attributes (closed sets) | Source |
|---|---|---|---|
| `hiring.backlog.pending`, `hiring.backlog.oldest_pending_age`, `hiring.backlog.sample_age` | gauges, `{item}`, `s`, `s` | `hiring.capability` = `embedding_work`, `operational_event_outbox` | `BacklogMetrics` |
| `hiring.graphql.root_field.executions` | counter, `{execution}` | `graphql.operation.type` = `query`, `mutation`, `_unknown`; `hiring.graphql.root_field` = a Query or Mutation field name of the schema, or `_introspection`, `_unknown`, `_invalid`; `hiring.graphql.outcome` = `success`, `field_error`, `denied`, `unauthenticated`, `rejected`, `internal` | `GraphQLOutcomeMetrics`, counted in `GraphQLHttpRoutes` (rule: `GraphQLOperationOutcomes`) |
| `db.client.operation.duration` | histogram, `s` (buckets 1 ms to 10 s) | `db.system.name` = `mongodb`; `db.operation.name` = fixed command list or `other`; `db.collection.name` = a persisted collection or `other` (absent for commands without one); `hiring.mongo.client` = `hiring`, `backlog_sampler`; `error.type` (failures only) = `duplicate_key`, `write_conflict`, `timeout`, `unavailable`, `write_error`, `command_error` | `MongoCommandTelemetry` (driver command listener) |
| `jvm.memory.used`, `jvm.memory.committed`, `jvm.memory.limit`, `jvm.memory.used_after_last_gc`, `jvm.gc.duration`, `jvm.thread.count`, `jvm.class.loaded`, `jvm.class.unloaded`, `jvm.class.count`, `jvm.cpu.time`, `jvm.cpu.count`, `jvm.cpu.recent_utilization` | OpenTelemetry JVM semantic conventions | `jvm.memory.type`, `jvm.memory.pool.name`, `jvm.gc.name`, `jvm.gc.action`, `jvm.thread.daemon`, `jvm.thread.state` | `RuntimeMetrics` (`opentelemetry-runtime-telemetry`) |
| `cats.effect.runtime.cpu.starvation.count`, `.clock.drift.current`, `.clock.drift.max`; `cats.effect.runtime.wstp.compute.thread.count`, `.thread.active.count`, `.thread.searching.count`, `.thread.blocked.count`, `.fiber.enqueued.count`, `.fiber.suspended.count` | counter and gauges (`{event}`, `ms`, `{thread}`, `{fiber}`) | `pool.id` (compute metrics only) | `RuntimeMetrics` (`otel4s-instrumentation-metrics`) |

The existing HTTP server metrics and spans come from the http4s otel4s middleware. Observed HTTP series: `http_server_request_duration_seconds_{bucket,count,sum}` with `http_request_method`, `http_response_status_code`, `http_phase` and `classifier`. There is **no `http_route` label**: the http4s otel4s metrics middleware (0.19.0) never emits one; the route template is carried by its `classifier` attribute, which `TelemetryRuntime.routeLabel` fills from the fixed public path list (`/graphql`, `/health`, ...) or `_unmatched`, so no raw path can reach a label. Dashboards use `classifier`; no application change was needed. Every request records two samples: `http_phase="headers"` (no status code) and `http_phase="body"` (with status code). Queries must select `http_phase="body"`, otherwise request counts and the error-ratio denominator double and latencies mix two measurements.

Mongo client spans (`<command> <collection>`, kind CLIENT, the same attributes) are children of the current request span only when the JVM runs with `-Dcats.effect.trackFiberContext=true` (set for `sbt run` and tests). The container deliberately does not set it (decision D13 of the container runtime specification), so in the container Mongo operations record the duration histogram without span parenting and create no span; the same holds for background workers (outbox publisher, embedding workers, backlog sampler). When the flag is off, `Main` selects the standard OpenTelemetry thread-local context storage (`TelemetryRuntime.selectContextStorage`), because the fiber-backed storage that `otel4s-oteljava-context-storage` registers throws on every otel4s instrument call without the flag. GraphQL requests rejected before a document exists (malformed JSON, unsupported media type, HTTP authentication failures, payload too large) appear only in the HTTP server metrics, not in the GraphQL counter; parse failures, invalid documents and persisted-query misses are the `_invalid` count. Field-level GraphQL errors return HTTP 200, so use `hiring.graphql.root_field.executions` by `hiring.graphql.outcome`, not the HTTP status, for GraphQL failure ratios; a typed business payload (for example an interview action refusal) is not an error entry and counts as `success`.

## Run

Create the secret files described in the README ("Container runtime"), then:

```bash
docker compose -f compose.yaml -f compose.observability.yaml --profile api up -d --build
scripts/generate-local-traffic.sh 120
```

`generate-local-traffic.sh [seconds=60] [base-url=http://127.0.0.1:8080]` first checks `/health` (BLOCKED, exit 2, if there is no answer), then sends the request cycle for exactly the given duration (each request limited to 3 s), prints `ok/4xx/5xx/failed` counts every 10 s and ends with `RESULT: PASS|FAIL|BLOCKED ...`. One 4xx per cycle is the intentional invalid query; the run fails on any 5xx or failed request. `--help` lists the exit codes (see "Operator scripts" in `engineering-quality.md`).

Grafana: `http://127.0.0.1:3000`, user `admin`, password `$GRAFANA_ADMIN_PASSWORD` (default `admin`). Prometheus: `http://127.0.0.1:9090`. Tear down with `docker compose -f compose.yaml -f compose.observability.yaml --profile api down`; Tempo and Loki data is on tmpfs and disappears with the containers.

## First-run verification checklist

Run by the coordinator on 2026-11-02 (HEAD 5193b81, api container healthy, `scripts/generate-local-traffic.sh 120`); results are recorded in the observability spec's MVP slice checkpoint. Repeat after stack changes.

1. [x] `docker compose ... ps`: every service is running; `grafana`, `prometheus`, `tempo`, `loki`, `alloy` and `otel-collector` did not exit (the collector, Tempo and Loki run with all capabilities dropped; Tempo and Loki use tmpfs).
2. [x] Prometheus → Status → Targets: `otel-collector` is up.
3. [x] Prometheus: query `http_server_request_duration_seconds_count`. Observed: labels `http_request_method`, `http_response_status_code` (and `classifier`, `http_phase`; no `http_route`). `alerts.yaml` and `hiring-api-red.json` were fixed accordingly.
4. [x] Prometheus: query `hiring_backlog_pending`, `hiring_backlog_oldest_pending_age_seconds`, `hiring_backlog_sample_age_seconds`; both `hiring_capability` values appear. An empty backlog shows `pending = 0` and no oldest-age series.
5. [x] Stop MongoDB (`docker compose ... stop mongodb`): within about one minute the backlog series disappear (collector `metric_expiration` is 45 s, sample freshness is three sampling intervals) and `HiringBacklogUnobserved` fires after its `for` period. A flat zero line means unobserved is being shown as zero: stop and fix.
6. [x] Grafana → Explore → Tempo: search for service `hiring-api`; a request from the traffic script appears as a trace.
7. [x] Grafana → Explore → Loki: `{service_name="api"}` returns log lines; confirm none contain tokens, resumes or raw GraphQL (masking must stay on).
8. [x] Prometheus → Alerts: the four rules load without errors (health ok).

Observed results: (1) all services up, api healthy; (2) target `otel-collector` up; (3) GET 237 all 200, POST 226 x 200 and 226 x 400 (the script's deliberate `{ notAField }`); (4) `hiring_backlog_pending` 0 for `embedding_work` and `operational_event_outbox`, `hiring_backlog_sample_age_seconds` present, no oldest-age series with an empty backlog; (5) MongoDB stopped about 07:52Z: both backlog series disappeared (no flat zero), `HiringBacklogUnobserved` pending 07:53:45Z, firing 07:55:48Z; (6) Tempo has `POST /graphql` and `GET /health` traces for `hiring-api`; (7) Loki `{service_name="api"}` 718 lines, 0 matches for JWT/bearer, password, email, Mongo URI, raw GraphQL text (masking enabled); (8) four rules loaded, health ok.

## Known limitations and open items

| Item | Status | Why it matters | Next step |
|---|---|---|---|
| Runtime behavior of the whole stack, metric names and Alloy discovery | Observed 2026-11-02 (local, one run) | Names above are observed; the run does not establish load behavior or any deployed result | Repeat the checklist after stack changes |
| Alloy mounts `/var/run/docker.sock` and runs as root | Accepted for local use | `:ro` does not restrict Docker API calls; a compromised Alloy is host root, can read every container's environment (Mongo credentials, API secrets) and can start containers. Only the `api` service's logs are collected (`alloy.config` keeps `service=api`; Mongo and Kafka logs are excluded), but the socket gives access to all containers regardless | Replace with a Docker socket proxy limited to container list/logs, or `loki.source.file` on a read-only bind mount of the container log directory |
| Backlog sampler uses the API's Mongo URI (same privileges as the hiring client) | Open before non-local use | "Read-only" is enforced only by code | Dedicated database user with `find` on `embedding_work` and the outbox collection, supplied through its own setting |
| No authentication or TLS on Prometheus, Tempo, Loki, collector links; Grafana default password `admin` | Accepted for local use | The 127.0.0.1 binding does not stop other local users or processes on a shared workstation from using the default password. Set `GRAFANA_ADMIN_PASSWORD` before running on a shared machine. Not a production design | Do not reuse outside a workstation; production needs its own scoped infrastructure task (managed backends, TLS, auth, retention) |
| Logs carry masked trace IDs | By design | No log-to-trace jump in Grafana | Only revisit with a reviewed exception to `logging.mask-sensitive`; unmasked stdout must not go to Loki (`local.conf` turns masking off) |
| GraphQL field errors return HTTP 200 | Open | The 5xx alert and RED dashboard miss resolver failures and denials; they appear only as `outcome=FIELD_ERROR` in `GRAPHQL_COMPLETED` log lines | Root-field outcome counter ([start-now slice](specs/hiring-observability-resilience.md#mvp-observability-slice)) |
| Alert thresholds are provisional | Guarded, not calibrated | `HiringApiSlowRequests` and `HiringApiHighErrorRate` evaluate only when the `http_phase="body"` request rate excluding `/health` probes (`classifier!="/health"`) is at least 0.1 req/s (6/min) over 1 m; below it they produce no data (neither firing nor reporting healthy). Probes are excluded because idle `/health` checks alone run at about 0.11 req/s and would pass the floor and dilute the ratio and quantile. **Accepted limitation:** below the guard a total 5xx outage does not fire; the values are provisional local ones The 5% / 300 ms / 5 min values are local placeholders, not SLOs | Calibrate after the overhead baseline (HOR-10) and an accepted workload |
| HTTP, two backlog capabilities, GraphQL root-field outcomes, Mongo command duration and JVM/Cats Effect runtime are instrumented | Partial | Other catalog rows (embedding source-to-current age, attempts, committed publication outcomes, consumer lag, quarantine, workflows, providers, analytics maintenance) are not exported | Follow the spec's catalog (HOR-02, HOR-03, HOR-04) |
| No load or overhead measurement (HOR-10) | Unmeasured | The sampler's Mongo cost and the exporter overhead are assumptions | Record workload, dataset size and p50/p95/p99 with and without the overlay |
| Trace sampling and exporter bounds | Configured in `compose.observability.yaml`, not yet observed running | Parent-based 100% sampler, span queue 2048, batch 512, 2 s delay, 10 s export timeout; policy in the spec's "Sampling/bounds". No dropped-span counter has been observed in this setup | Observe after restart (see below); measure overhead (item 7) |
| Kafka and Spark/analytics metrics | Not included | Analytics runs in a separate build; the API's Kafka publication is disabled in Compose | Separate scoped slice (JMX/Kafka exporter, Spark metrics) |
| Data retention | Local defaults | Prometheus 2 d (container layer, no size cap), Tempo/Loki 48 h on tmpfs (512 MiB cap) | Revisit if the stack is kept running |
