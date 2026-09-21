# Running Foundation locally

Foundation runs the Scala application on the host and one single-node replica-set MongoDB service in Docker Compose. Prerequisites: Java 17+, sbt 1.11.1, Docker Engine, and Docker Compose. No application image, cloud service, hiring dataset, or SQL database is required.

## Start and stop

From the repository root:

```bash
docker compose up -d mongodb
sbt run
```

In another terminal:

```bash
curl -sS http://127.0.0.1:8080/health
curl -sS http://127.0.0.1:8080/ready
curl -sS http://127.0.0.1:8080/graphql \
  -H 'Content-Type: application/json' \
  --data '{"query":"{ health { status } readiness { status } }"}'
```

The local run example binds to loopback. `health` returns UP without querying MongoDB. Readiness returns READY only when the configured database responds and hiring setup succeeds. The local Compose replica set supports the transaction-backed hiring write path. HTTP readiness uses 200/503; an executed GraphQL readiness query uses HTTP 200 even for NOT_READY.

Stop/start MongoDB with `docker compose stop mongodb` and `docker compose start mongodb` to observe NOT_READY and recovery while the app remains live. Stop the app with Ctrl-C. Ember waits up to ten seconds for in-flight responses before canceling remaining requests and finalizing resources. `docker compose down` stops the database; the ignored `.local/data/mongodb` bind mount survives ordinary stop/down/restart. Do not delete it as a routine reset.

## Configuration and API tools

The application uses PureConfig's `ConfigSource.default`, which delegates configuration loading and precedence to Typesafe Config. `application.conf` is the packaged base; standard Typesafe Config selectors such as `config.file` and `config.resource` can provide an override source, and HOCON merges nested objects with later values taking precedence. HTTP port and admission permits are decoded as Iron refined types. The checked-in configuration is a deployment template: it may require environment-backed cloud/runtime values and must not hide missing local setup behind fake placeholders. Keep machine-specific local run values in ignored `src/main/resources/local.conf`; do not commit credential-bearing overrides or package that file into deployable artifacts.

```bash
cat > src/main/resources/local.conf <<'EOF'
http {
  host = "127.0.0.1"
  port = 8080
  admission-permits = 16
}
mongo {
  uri = ${?MONGODB_URI}
  database = "hiring"
}
logging {
  mask-sensitive = true
}
EOF
MONGODB_URI='mongodb://127.0.0.1:27017/?replicaSet=rs0&directConnection=true' sbt -Dconfig.file=src/main/resources/local.conf run
```

The repository is backend-only. Download `http://127.0.0.1:8080/schema.graphql` or use an external API client against `/graphql` with introspection. No UI assets, frontend build, or Node tooling are included. See the [API reference](api.md) for operations and error contracts.

For persistent local settings, keep `src/main/resources/local.conf` ignored and review it before running the app. Pass it through the standard `config.file` selector as shown above. Never load untrusted configuration files. Mongo credentials belong in process environment variables referenced by `${?VAR}` config overrides, never committed examples or command transcripts.

| Variable | Local value source | Validation |
|---|---|---|
| HTTP_HOST | `src/main/resources/local.conf` or environment | Numeric IPv4/IPv6 address |
| HTTP_PORT | `src/main/resources/local.conf` or environment | Integer 1..65535 |
| HTTP_ADMISSION_PERMITS | `application.conf`/`local.conf` resource or environment | Integer 1..1024; default 16 |
| `http.trusted-proxy-cidrs` | `application.conf`/`local.conf` resource | CIDR list for immediate reverse-proxy peers; defaults to `[]` |
| MONGODB_URI | `src/main/resources/local.conf` or environment | Valid Mongo connection string |
| MONGODB_DATABASE | `application.conf`/`local.conf` resource | Valid nonempty database name |
| `logging.mask-sensitive` | `application.conf`/`local.conf` resource | strict boolean; defaults to `true` |
Malformed configuration exits unsuccessfully with a safe category and configuration key. Logback XML, not application configuration, selects TRACE/DEBUG/INFO/WARN/ERROR severity. An unavailable or unauthenticated database leaves HTTP running with NOT_READY. Application JSON logs include searchable markers, concise messages, controlled diagnostic fields and safe trace-ID correlation. Sensitive metadata is masked by default. Framework/driver raw output and request payloads remain suppressed. The IOApp runtime failure reporter always uses masked RUNTIME_FAILED diagnostics without exception messages or raw stacks. The active trace ID is the request correlation ID; W3C propagation is owned by the HTTP server middleware. See [logging flags, filters and disclosure limits](logging.md).

For authentication rate limiting, the default empty `http.trusted-proxy-cidrs` list uses the TCP peer address and ignores forwarding headers. When the application is reachable only through known reverse proxies, configure their immediate peer CIDRs, for example `http.trusted-proxy-cidrs = ["10.0.0.0/8"]`. A configured proxy must strip or replace client-supplied forwarding headers before appending its observed connection peer. After the TCP peer matches the allowlist, the application prefers concrete RFC 7239 `Forwarded: for=` hops and falls back to typed `X-Forwarded-For`; both chains are scanned from right to left, skipping malformed, unknown, obfuscated, incomplete, or configured-proxy addresses. If no usable address exists, it falls back to the peer address. Allowlist only networks that can directly connect to the application, restrict direct access at the network layer, and never use `/0`. Token buckets remain process-local, and `max-buckets` bounds memory by replacing an existing entry instead of rejecting legitimate new clients.

## HTTP contract and budgets

POST `/graphql` accepts JSON with required string `query`, optional object `variables`, and optional string `operationName`. Omitted/null optional values are supported. JSON batches and GET query execution are unsupported. Responses negotiate between `application/graphql-response+json` and `application/json`, preferring the former when `Accept` is absent; request charset parameters are accepted. Errors never include raw exception, query, variable, or connection-string details.

| Condition | HTTP status |
|---|---|
| Executed query, including NOT_READY or field errors | 200 |
| Invalid body/query/operation/variables or query budget | 400 |
| Unsupported method / response media | 405 / 406 |
| Oversized body / unsupported request media | 413 / 415 |
| Unexpected server failure / admission full / deadline | 500 / 503 / 504 |

The http4s runtime admits `http.admission-permits` concurrent GraphQL requests with immediate overload rejection. The checked-in default is sixteen permits and can be overridden with `HTTP_ADMISSION_PERMITS`. GET `/health` and `/ready` bypass this gate; GraphQL health does not. Request body maximum is 64 KiB streamed, with the default five-second `http.request-timeout-ms` deadline starting before consumption. Resolver-owned service effects have a separate default four-second `http.resolver-timeout-ms` bound, which must be positive, bounded, and shorter than the request deadline. GraphQL syntax and document validation use Sangria's parser and validator. Selected operations are bounded by Sangria execution reducers: maximum depth sixteen and maximum complexity 1000. These conservative local budgets include the standard introspection fixture and are not measured performance guarantees.

Sangria executes leaf actions as `Future`s, so the HTTP deadline cannot cancel a Future after it has crossed from Cats Effect. The resolver bound cancels the underlying service `IO` where the effect cooperates with cancellation, but a timed-out mutation has an unknown outcome and clients must not blindly retry it. Repository and provider timeouts remain required at their own boundaries.

http4s owns the concurrency middleware lifecycle and the GraphQL context resource; the dispatcher is server-lifetime infrastructure. Neither middleware cancellation nor the HTTP deadline can cancel Sangria Futures that have already started, so the shorter resolver-service bound is the application-level mitigation. The installed Ember transport awaits response computation before its next socket operation: a peer disconnect alone is not an immediate cancellation signal. Disconnected probes are bounded by the two-second probe deadline; stalled request bodies by the five-second request deadline. Real socket-reset tests check eventual cleanup and subsequent successful requests.

MongoDB uses one resource-managed client, pool maximum ten, bounded driver timeouts, and an outer two-second probe deadline. Fixed limits override conflicting URI pool/timeout options. Readiness aliases share one probe per request; no result is cached between requests.

## Local checks and phase boundaries

```bash
sbt test
sbt 'IntegrationTest / test'
```

The unit suite is Docker-independent. Integration tests bind disposable local HTTP ports and use disposable MongoDB containers; missing Docker is a blocked check. `bash scripts/check-local.sh` validates project skills/JDK and runs the unit suite. There is no configured Scala formatter or CI/CD pipeline.

The served SDL and representative operations live under `src/test/resources/graphql/`. Contract tests compare deterministic SDL and execute health/readiness/introspection fixtures. Unused SQL adapters, user-query scaffolds, demo hiring models, and their tests/dependencies have been removed at the user's request. Phase 2 will implement the actual hiring domain. No database schema/data migration occurs in Foundation.

Sangria 4.2.19 validates missing required variables before applying their defaults. Supply values for required variables explicitly, or use nullable variables with defaults as in the readiness fixture. This dependency limitation is recorded in the spec; Foundation does not claim complete GraphQL conformance.

See [acceptance and review evidence](specs/phase-1-foundation.md) for completed checks, blocked gates, and remaining risks.
