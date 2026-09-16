# Running Foundation locally

Foundation runs the Scala application on the host and one standalone MongoDB service in Docker Compose. Prerequisites: Java 17+, sbt 1.11.1, Docker Engine, and Docker Compose. No application image, cloud service, hiring dataset, or SQL database is required.

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

The default host is loopback. `health` returns UP without querying MongoDB. Readiness returns READY only when the configured database responds to a ping. It proves connectivity, not schema, transaction, or hiring-workflow readiness. HTTP readiness uses 200/503; an executed GraphQL readiness query uses HTTP 200 even for NOT_READY.

Stop/start MongoDB with `docker compose stop mongodb` and `docker compose start mongodb` to observe NOT_READY and recovery while the app remains live. Stop the app with Ctrl-C. Shutdown stops admission, allows ten seconds to drain, then cancels remaining requests and finalizes resources. `docker compose down` stops the database; the ignored `.local/data/mongodb` bind mount survives ordinary stop/down/restart. Do not delete it as a routine reset.

## Configuration and API tools

The application reads process environment only. Defaults and sanitized examples are in `.env.example`. `.env` and `.local/config/` are storage conventions, not automatic configuration sources.

```bash
HTTP_PORT=8080 sbt run
```

The repository is backend-only. Download `http://127.0.0.1:8080/schema.graphql` or use an external API client against `/graphql` with introspection. No UI assets, frontend build, or Node tooling are included. See the [API reference](api.md) for operations and error contracts.

For persistent local shell settings, copy the sanitized example into ignored `.local/config/` and explicitly export only reviewed values in your shell before `sbt run`. Never source untrusted configuration files. Mongo credentials belong in the process environment/local ignored configuration, never committed examples or command transcripts.

| Variable | Default | Validation |
|---|---|---|
| HTTP_HOST | 127.0.0.1 | Numeric IPv4/IPv6 address |
| HTTP_PORT | 8080 | Integer 1..65535 |
| MONGODB_URI | mongodb://127.0.0.1:27017 | Valid Mongo connection string |
| MONGODB_DATABASE | hiring | Valid nonempty database name |
| LOG_LEVEL | INFO | INFO, WARN, ERROR |

Malformed configuration exits unsuccessfully with a safe category. An unavailable or unauthenticated database leaves HTTP running with NOT_READY. Application JSON logs contain UTC timestamp, severity, category, and safe request correlation. Framework/driver raw output is suppressed. API responses include a generated X-Request-ID; incoming IDs are not trusted.

## HTTP contract and budgets

POST `/graphql` accepts JSON with required string `query`, optional object `variables`, and optional string `operationName`. Omitted/null optional values are supported. JSON batches and GET query execution are unsupported. Responses use application/json; request charset parameters are accepted. Errors never include raw exception, query, variable, or connection-string details.

| Condition | HTTP status |
|---|---|
| Executed query, including NOT_READY or field errors | 200 |
| Invalid body/query/operation/variables or query budget | 400 |
| Unsupported method / response media | 405 / 406 |
| Oversized body / unsupported request media | 413 / 415 |
| Unexpected server failure / admission full / deadline | 500 / 503 / 504 |

The runtime admits sixteen GraphQL/readiness requests, with immediate overload rejection. GET `/health` bypasses this gate; GraphQL health does not. Request body maximum is 64 KiB streamed, with a five-second deadline starting before consumption. Parsing allows 4096 tokens/nesting 32; document/fragment work is bounded before schema validation. Selected operations allow field depth sixteen (root one), 1000 expanded field occurrences, and 32 expanded aliases. Fragment containers do not add field depth. These conservative local budgets include the standard introspection fixture and are not measured performance guarantees.

Each request owns its resolver effects. Resource release/caller cancellation cancels and joins them before returning its admission permit. The installed Ember transport awaits response computation before its next socket operation: a peer disconnect alone is not an immediate cancellation signal. Disconnected probes are bounded by the two-second probe deadline; stalled request bodies by the five-second request deadline. Finalizer and scheduler time follow those budgets. Real socket-reset tests check eventual cleanup and subsequent successful requests.

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
