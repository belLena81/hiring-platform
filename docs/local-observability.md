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

## Run

Create the secret files described in the README ("Container runtime"), then:

```bash
docker compose -f compose.yaml -f compose.observability.yaml --profile api up -d --build
scripts/generate-local-traffic.sh 120
```

Grafana: `http://127.0.0.1:3000`, user `admin`, password `$GRAFANA_ADMIN_PASSWORD` (default `admin`). Prometheus: `http://127.0.0.1:9090`. Tear down with `docker compose -f compose.yaml -f compose.observability.yaml --profile api down`; Tempo and Loki data is on tmpfs and disappears with the containers.

## First-run verification checklist

None of this has been run; the stack was reviewed statically only. Tick each item and record the result in the observability spec.

1. `docker compose ... ps`: every service is running; `grafana`, `prometheus`, `tempo`, `loki`, `alloy` and `otel-collector` did not exit (the collector, Tempo and Loki run with all capabilities dropped; Tempo and Loki use tmpfs).
2. Prometheus → Status → Targets: `otel-collector` is up.
3. Prometheus: query `http_server_request_duration_seconds_count`. If the name or the `http_route` / `http_response_status_code` labels differ, fix `alerts.yaml` and `hiring-api-red.json`.
4. Prometheus: query `hiring_backlog_pending`, `hiring_backlog_oldest_pending_age_seconds`, `hiring_backlog_sample_age_seconds`; both `hiring_capability` values appear. An empty backlog shows `pending = 0` and no oldest-age series.
5. Stop MongoDB (`docker compose ... stop mongodb`): within about one minute the backlog series disappear (collector `metric_expiration` is 45 s, sample freshness is three sampling intervals) and `HiringBacklogUnobserved` fires after its `for` period. A flat zero line means unobserved is being shown as zero: stop and fix.
6. Grafana → Explore → Tempo: search for service `hiring-api`; a request from the traffic script appears as a trace.
7. Grafana → Explore → Loki: `{service_name="api"}` returns log lines; confirm none contain tokens, resumes or raw GraphQL (masking must stay on).
8. Prometheus → Alerts: the four rules load without errors.

## Known limitations and open items

| Item | Status | Why it matters | Next step |
|---|---|---|---|
| Runtime behavior of the whole stack, including metric names in Prometheus and Alloy discovery with dropped capabilities | Unverified | Dashboards and alerts use names derived from the instrument definitions, not observed ones | Run the checklist above |
| Alloy mounts `/var/run/docker.sock` and runs as root | Accepted for local use | `:ro` does not restrict Docker API calls; a compromised Alloy is host root, can read every container's environment (Mongo credentials, API secrets) and can start containers. Only the `api` service's logs are collected (`alloy.config` keeps `service=api`; Mongo and Kafka logs are excluded), but the socket gives access to all containers regardless | Replace with a Docker socket proxy limited to container list/logs, or `loki.source.file` on a read-only bind mount of the container log directory |
| Backlog sampler uses the API's Mongo URI (same privileges as the hiring client) | Open before non-local use | "Read-only" is enforced only by code | Dedicated database user with `find` on `embedding_work` and the outbox collection, supplied through its own setting |
| No authentication or TLS on Prometheus, Tempo, Loki, collector links; Grafana default password `admin` | Accepted for local use | The 127.0.0.1 binding does not stop other local users or processes on a shared workstation from using the default password. Set `GRAFANA_ADMIN_PASSWORD` before running on a shared machine. Not a production design | Do not reuse outside a workstation; production needs its own scoped infrastructure task (managed backends, TLS, auth, retention) |
| Logs carry masked trace IDs | By design | No log-to-trace jump in Grafana | Only revisit with a reviewed exception to `logging.mask-sensitive`; unmasked stdout must not go to Loki (`local.conf` turns masking off) |
| GraphQL field errors return HTTP 200 | Open | The 5xx alert and RED dashboard miss resolver failures and denials; they appear only as `outcome=FIELD_ERROR` in `GRAPHQL_COMPLETED` log lines | Root-field outcome counter ([start-now slice](specs/hiring-observability-resilience.md#mvp-observability-slice)) |
| Alerts have no minimum-traffic guard | Open | At sparse local traffic `histogram_quantile` and the 5xx ratio are noisy or empty, so `HiringApiSlowRequests` and `HiringApiHighErrorRate` may flap or never evaluate | Add a request-rate floor to both expressions |
| Only HTTP and two backlog capabilities are instrumented | Partial | Other catalog rows (Mongo operation duration, embedding source-to-current age, attempts, committed publication outcomes, consumer lag, quarantine, workflows, providers, analytics maintenance) are not exported | Follow the spec's catalog (HOR-02, HOR-03, HOR-04) |
| No load or overhead measurement (HOR-10) | Unmeasured | The sampler's Mongo cost and the exporter overhead are assumptions | Record workload, dataset size and p50/p95/p99 with and without the overlay |
| Trace sampling, exporter batch/queue bounds and dropped-telemetry counters | Not configured | Spec requires documented overload behavior | Set bounds through standard `OTEL_*` settings and record the policy |
| Kafka and Spark/analytics metrics | Not included | Analytics runs in a separate build; the API's Kafka publication is disabled in Compose | Separate scoped slice (JMX/Kafka exporter, Spark metrics) |
| Data retention | Local defaults | Prometheus 2 d (container layer, no size cap), Tempo/Loki 48 h on tmpfs (512 MiB cap) | Revisit if the stack is kept running |
