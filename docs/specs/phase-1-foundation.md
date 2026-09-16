# Phase 1 — Foundation

Status: in progress

## Identity and scope

User outcome: run the Scala application with `sbt run`, MongoDB with Docker Compose, and inspect application health/readiness through HTTP and GraphQL. The user's subsequent backend-only instruction removes the earlier GraphiQL UI requirement. Swagger-style API tracking uses schema export, introspection, operation fixtures, and API documentation.

The user approved a health-only runtime, a host application with Docker-only MongoDB, and HTTP availability during database outages. During implementation the user explicitly requested removing unused dependencies such as Doobie and keeping code aligned with the current phase. That supersedes the original source-scaffold preservation decision: remove the unused SQL adapters, legacy user schema/models/tests and illustrative hiring models. This is the first served HTTP schema; no existing served user operation is removed.

Non-goals: hiring workflows/models, authentication/admin provisioning, SQL data migration, Mongo collections/indexes, application containers, replica-set transactions, analytics, paid infrastructure, and performance claims.

Coordinator: Product Manager. Scala Developer owns runtime/build/tests; Data Engineer owns MongoDB adapter/Compose/database tests. Shared files have one writer. Architect, Code Reviewer, Security Engineer, and QA reviewed the planning contract; implementation needs fresh independent reviews.

## Source context and decisions

Inspected baseline: `build.sbt` pins Scala 3.9.0, Cats Effect, Sangria/http4s Ember, Circe, MongoDB driver, MUnit, and Testcontainers. `Main.run` is a no-op. `schemas/QueryType.scala` contains unserved legacy user fields. `daos/FutureDao` uses a global unsafe bridge. `models/Role.scala` contains a SQL codec. Existing three tests pass. No Compose or integration harness exists.

Decision sources: Phase 1 in `docs/development-milestones.md`, `ARCHITECTURE.md` boundaries, `docs/schema-evolution.md`, `docs/engineering-quality.md`, the approved plan, and the subsequent dependency/scaffold cleanup request. Remain in one sbt project; use constructor injection and inward package dependencies. Depend directly on the MongoDB reactive driver and Testcontainers core used by the adapter/harness; remove Doobie/PostgreSQL, the unused Scala driver wrapper, and the unused Testcontainers Mongo module. Phase 2 will introduce its agreed domain models and authorized API in its own slice.

## Behavior and contracts

`IOApp` owns Mongo client and Ember via `Resource`. Invalid configuration, client construction, or HTTP bind failure emits a sanitized diagnostic, unwinds acquisition, and exits nonzero. Unreachable MongoDB leaves HTTP running. Shutdown stops admission, drains for ten seconds, then cancels remaining requests; finalization follows rather than being skipped to meet a hard ten-second deadline.

Runtime SDL:

```graphql
enum HealthStatus { UP }
enum ReadinessStatus { READY NOT_READY }
type Health { status: HealthStatus! }
type Readiness { status: ReadinessStatus! }
type Query {
  health: Health!
  readiness: Readiness!
}
```

`health` performs no database work. `readiness` shares one bounded Mongo ping per request across aliases. Failure/authentication/timeout returns NOT_READY; the next successful check returns READY. Executed GraphQL readiness returns HTTP 200 even when NOT_READY. Export deterministic SDL and representative executable fixtures. The runtime and source contain no retired user-query schema or PostgreSQL/global-runtime bridge.

HTTP: POST `/graphql` accepts/returns application/json (request charset permitted). Accept absent, wildcard, or including application/json is supported; otherwise 406. Required query is a string; missing/null variables means an empty object; missing/null operationName means absent. Invalid shapes, batches, syntax, schema/operation/variable validation, and query budgets return 400. GET `/graphql` returns 405; unsupported request media 415, oversized bodies 413, overload 503, deadline 504, unexpected server failure 500. Normal field execution errors use sanitized GraphQL errors with HTTP 200. No raw exception details are exposed.

GET `/health` returns 200 and `{ "status": "UP" }`, bypassing the database-work admission gate. GET `/ready` returns 200/503 with READY/NOT_READY. GraphQL health still uses ordinary GraphQL admission. GET `/schema.graphql` exports the same deterministic SDL checked by contract tests without database I/O. GraphQL introspection and representative operations support external API tools. No UI routes/assets/Node build are included; `/graphiql` returns 404. No wildcard CORS.

Configuration loads process environment only: HTTP_HOST=127.0.0.1, HTTP_PORT=8080, MONGODB_URI=mongodb://127.0.0.1:27017, MONGODB_DATABASE=hiring, LOG_LEVEL=INFO. Validate numeric IP, port 1..65535, database name, URI, and INFO/WARN/ERROR log level. `.env` and `.local/config/` are not auto-loaded; supply safe examples and explicit export instructions.

Fixed local safety budgets: 16 admitted GraphQL/readiness requests, fail-fast excess rejection; 64 KiB streamed bodies; five-second total request deadline including body consumption; parser tokens 4096, nesting 32; validation/fragment traversal work 4096; selected field depth 16, complexity 1000, aliases 32. Root field depth is one; fragments do not add depth; scalar/object field occurrences cost one, including expanded fragments; aliases count expanded field aliases. Syntax/schema validation covers the whole document. Parsing/preflight and traversal are bounded before effects. Standard introspection is a contract fixture. Admission precedes body processing, and cleanup precedes permit reuse.

Mongo pool maximum ten and total readiness deadline two seconds. Parse connection URI first, then override pool/driver connect-selection-read limits so URI options cannot relax budgets. An outer two-second timeout covers the complete probe. Request scope owns cancellable resolver IO/subscriptions, rejects late submissions after closing, cancels registration-racing subscriptions, joins IO, then releases admission. Do not use FutureDao.

Structured logs contain UTC time, severity, safe event category, and generated request ID (also returned in X-Request-ID). Categories include CONFIG_INVALID, MONGO_UNAVAILABLE, MONGO_AUTH_FAILED, REQUEST_REJECTED, STARTUP_FAILED, SHUTDOWN. Configure logging before client acquisition; suppress raw framework/driver emitters for all supported application levels. Exclude bodies, queries, variables, URIs, credentials, and exception messages.

## Local infrastructure and compatibility

One standalone MongoDB Compose service, loopback-published port, ignored `.local/data/mongodb` storage, same pinned image in disposable Testcontainers tests. Ordinary stop/restart retains data; destructive reset remains explicit. No schema migration/backfill is applicable: the runtime only pings and introduces no stored hiring data. A small disposable synthetic fixture verifies retention. Do not claim transaction readiness.

Unit tests remain Docker-independent with `sbt test`. Add explicit `IntegrationTest` configuration with `sbt 'IntegrationTest / test'`. Missing Docker is BLOCKED, never a pass. Local workload is one app/one database, zero hiring records and bounded fixtures, concurrency sixteen/pool ten. Defaults are not measured SLOs. Disk/download/idle memory are local costs; no price or paid-resource claims.

## Acceptance and evidence

| ID | Required observable result | Verification | Actual outcome |
|---|---|---|---|
| P1-AC01 | Validated configuration/defaults/overrides, effective Mongo limits and redaction | Configuration and settings tests | Not run |
| P1-AC02 | IOApp startup, acquisition/bind failure cleanup, shutdown and restart | Lifecycle/unit and live server tests | Not run |
| P1-AC03 | Executable new contract, deterministic SDL, absent retired/runtime user fields | GraphQL/contract tests | Not run |
| P1-AC04 | Mongo-independent health, unavailable/auth recovery, alias ping sharing | Probe/HTTP/integration tests | Not run |
| P1-AC05 | Bounded malformed/large/slow/deep/fragment-heavy/overload/deadline requests, sanitized errors and permit reuse | Negative and concurrency tests | Not run |
| P1-AC06 | Real MongoDB probes; cancellation registration races and actual Ember disconnect cleanup | Unit and IntegrationTest | Not run |
| P1-AC07 | Compose plus sbt startup, restart/recovery and fixture retention | Local Compose smoke and disposable fixture | Not run |
| P1-AC08 | Backend-only repository, no UI routes/assets/toolchain; schema export and introspection for API tooling | Source/dependency and API/contract checks | Not run |
| P1-AC09 | Parseable JSON logs, correlation, no synthetic secrets on failure paths | Captured log/error checks | Not run |
| P1-AC10 | Inward dependencies, no retired SQL/wrapper dependencies or scaffolds, current startup/schema/evidence docs | Compilation, dependency tree, source/references/review | Not run |

## Implementation handoff and checkpoint

Order: config/boundaries/lifecycle; Mongo/probes/HTTP/GraphQL/schema export; Compose/integration/recovery; independent reviews/fixes/final QA.

Planning baseline: three unit tests and eight local skills passed; Compose CLI is available. Planning Code Reviewer, Security Engineer and QA verdicts were PASS for specification readiness only. No runtime acceptance is completed by those verdicts.

Current work: authorized implementation started; specification recorded. No data migration or deployment authorized/required.

Implementation decisions discovered through source/tests:

- The user's backend-only/API-testing instructions replace the original UI slice. All GraphiQL assets, npm dependencies/lockfile, browser tooling, and UI configuration are removed. API documentation uses SDL/introspection with a standard query fixture; AC01/03/08 and affected reviews are reopened. No frontend browser pass is claimed as final-phase evidence.

- User-directed dependency cleanup reopens AC03/10 and relevant reviews. The original three scaffold tests and the interim SQL codec test are removed with their retired behavior; Foundation contract and runtime tests provide current-phase coverage. No SQL database or stored data is accessed or migrated by deleting unused source/dependencies.

- Ember 0.23.37 `ServerHelpers.runApp/runConnection` awaits response computation before the next socket write/read. Peer disconnect cleanup is therefore deadline-driven (probe two seconds/request five seconds, plus finalizer/scheduler time), not an immediate transport cancellation promise. Caller/request IO cancellation still immediately initiates scope cleanup. Real RST tests verify cleanup and capacity reuse; do not add a custom socket reader with different half-close semantics. Affected AC02/05/06 are reopened for independent review.
- Sangria 4.2.19 `ValueCoercionHelper.getVariableValue` checks non-null input before considering an omitted variable's default. The consumer fixture uses the supported nullable Boolean with default; clients can also supply explicit required-variable values. No library migration or general GraphQL coercion patch is included. Required-variable defaults remain a documented dependency limitation, not proof of full GraphQL conformance.
- Cats Effect's unhandled-fiber reporter bypasses SLF4J. `Main.reportFailure` now emits sanitized RUNTIME_FAILED JSON, including for Ember's bind-failure background fiber. MUnit uses its own runtime; actual forked Main output requires separate verification.

Implementation Code Reviewer: pending. Security Engineer: pending. Final independent QA: pending.
