# Phase 1 — Foundation

Status: done

The completed baseline evidence below predates the user-requested [diagnostics extension](hiring-platform-diagnostics.md). That extension is now complete: 87 unit and 28 integration tests passed, followed by independent Code Reviewer, Security Engineer and QA PASS. The reopened configuration/logging and affected request-lifecycle checks are closed; the extension does not add hiring workflows or change the GraphQL schema.

## Identity and scope

User outcome: run the Scala application with `sbt run`, MongoDB with Docker Compose, and inspect application health/readiness through HTTP and GraphQL. The user's subsequent backend-only instruction removes the earlier GraphiQL UI requirement. Swagger-style API tracking uses schema export, introspection, operation fixtures, and API documentation.

The user approved a health-only runtime, a host application with Docker-only MongoDB, and HTTP availability during database outages. During implementation the user explicitly requested removing unused dependencies such as Doobie and keeping code aligned with the current phase. That supersedes the original source-scaffold preservation decision: remove the unused SQL adapters, legacy user schema/models/tests and illustrative hiring models. This is the first served HTTP schema; no existing served user operation is removed.

Non-goals: hiring workflows/models, authentication/admin provisioning, SQL data migration, Mongo collections/indexes, application containers, replica-set transactions, analytics, paid infrastructure, and performance claims.

Coordinator: Product Manager. Scala Developer owns runtime/build/tests; Data Engineer owns MongoDB adapter/Compose/database tests. Shared files have one writer. Planning and final implementation review evidence are recorded below; independent Code Reviewer, Security Engineer, and final QA gates passed.

## Source context and decisions

Historical baseline before Foundation implementation: `build.sbt` pinned Scala 3.9.0, Cats Effect, Sangria/http4s Ember, Circe, MongoDB driver, MUnit, and Testcontainers. `Main.run` was a no-op. `schemas/QueryType.scala` contained unserved legacy user fields. `daos/FutureDao` used a global unsafe bridge. `models/Role.scala` contained a SQL codec. The three scaffold tests passed. No Compose or integration harness existed. These are historical facts, not the current source state.

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

HTTP: POST `/graphql` accepts/returns application/json (request charset permitted). Accept absent is supported; otherwise select the most-specific matching range (application/json, application/*, */*) and require positive quality. Equally specific matches use the highest quality. Quality defaults to 1 and must be 0..1 with at most three decimal places; malformed/duplicate quality parameters or excluded JSON return 406. Required query is a string; missing/null variables means an empty object; missing/null operationName means absent. Invalid shapes, batches, syntax, schema/operation/variable validation, and query budgets return 400. GET `/graphql` returns 405; unsupported request media 415, oversized bodies 413, overload 503, deadline 504, unexpected server failure 500. Normal field execution errors use sanitized GraphQL errors with HTTP 200. No raw exception details are exposed.

GET `/health` returns 200 and `{ "status": "UP" }`, bypassing the database-work admission gate. GET `/ready` returns 200/503 with READY/NOT_READY. GraphQL health still uses ordinary GraphQL admission. GET `/schema.graphql` exports the same deterministic SDL checked by contract tests without database I/O. GraphQL introspection and representative operations support external API tools. No UI routes/assets/Node build are included; `/graphiql` returns 404. No wildcard CORS.

Configuration loads `application.conf` defaults and overlays ignored `local.conf` when present. Non-sensitive local values live in config; sensitive values such as credential-bearing MongoDB URIs are supplied only through explicit config placeholders such as `{$MONGODB_URI}`. Defaults remain HTTP_HOST=127.0.0.1, HTTP_PORT=8080, MONGODB_URI=mongodb://127.0.0.1:27017, MONGODB_DATABASE=hiring, LOG_LEVEL=INFO, LOG_MASK_SENSITIVE=true and LOG_REQUEST_PAYLOADS=false. Disabling masking requires a loopback bind address; request payload capture additionally requires masking disabled. Validate numeric IP, port 1..65535, database name, URI, INFO/WARN/ERROR log level and strict logging-policy flags. `.env` files are not auto-loaded; supply safe examples and explicit export instructions.

Fixed local safety budgets: 16 admitted GraphQL/readiness requests, fail-fast excess rejection; 64 KiB streamed bodies; five-second total request deadline including body consumption; parser tokens 4096, nesting 32; validation/fragment traversal work 4096; selected field depth 16, complexity 1000, aliases 32. Root field depth is one; fragments do not add depth; scalar/object field occurrences cost one, including expanded fragments; aliases count expanded field aliases. Syntax/schema validation covers the whole document. Parsing/preflight and traversal are bounded before effects. Standard introspection is a contract fixture. Admission precedes body processing, and cleanup precedes permit reuse.

Mongo pool maximum ten and total readiness deadline two seconds. Parse connection URI first, then override pool/driver connect-selection-read limits so URI options cannot relax budgets. An outer two-second timeout covers the complete probe. Request scope owns cancellable resolver IO/subscriptions, rejects late submissions after closing, cancels registration-racing subscriptions, joins IO, then releases admission. Do not use FutureDao.

Structured logs contain UTC time, severity, safe event category, generated request ID (also returned in X-Request-ID), stable markers, concise messages and controlled details. Categories include CONFIG_INVALID, MONGO_UNAVAILABLE, MONGO_AUTH_FAILED, REQUEST_REJECTED, STARTUP_FAILED, SHUTDOWN. Configure logging before client acquisition; suppress raw framework/driver emitters for all supported application levels. Exclude raw bodies/query text, raw variable values, full URIs, credentials and exception messages. Only the explicitly authorized, filtered local payload reconstruction described in the [diagnostics extension](hiring-platform-diagnostics.md) is permitted; production omits payload capture.

## Local infrastructure and compatibility

One standalone MongoDB Compose service, loopback-published port, ignored `.local/data/mongodb` storage, same pinned image in disposable Testcontainers tests. Ordinary stop/restart retains data; destructive reset remains explicit. No schema migration/backfill is applicable: the runtime only pings and introduces no stored hiring data. A small disposable synthetic fixture verifies retention. Do not claim transaction readiness.

Unit tests remain Docker-independent with `sbt test`. Add explicit `IntegrationTest` configuration with `sbt 'IntegrationTest / test'`. Missing Docker is BLOCKED, never a pass. Local workload is one app/one database, zero hiring records and bounded fixtures, concurrency sixteen/pool ten. Defaults are not measured SLOs. Disk/download/idle memory are local costs; no price or paid-resource claims.

## Acceptance and evidence

| ID | Required observable result | Verification | Actual outcome |
|---|---|---|---|
| P1-AC01 | Validated configuration/defaults/overrides, effective Mongo limits and redaction | AppConfigSpec, MongoDatabaseProbeSpec, MainProcessSpec | PASS: configuration/settings and actual invalid-config child JVM |
| P1-AC02 | IOApp startup, acquisition/bind failure cleanup, shutdown and restart | HiringPlatformServerSpec, MainProcessSpec, actual local smoke | PASS: acquisition/bind cleanup, shutdown/rebind, actual Main exit and shutdown diagnostics |
| P1-AC03 | Executable new contract, deterministic SDL, absent retired/runtime user fields | HiringGraphQLContractSpec, HiringApiRoutesSpec, SDL/introspection smoke | PASS: schema/fixtures, whole-document validation including unselected operations, genuine sanitized field errors |
| P1-AC04 | Mongo-independent health, unavailable/auth recovery, alias ping sharing | HealthServiceSpec, HiringApiRoutesSpec, HiringPlatformServerSpec, MongoDatabaseProbeIntegrationSpec | PASS: fake and real probes, auth classification, recovery and request-local sharing |
| P1-AC05 | Bounded malformed/large/slow/deep/fragment-heavy/overload/deadline requests, sanitized errors and permit reuse | InputBudgetSpec, HiringApiRoutesSpec, RequestContextSpec | PASS: compact spreads/signed numbers/leading zeros, quality precedence, limits, 500/field errors, barrier-proven recovery of all 16 permits |
| P1-AC06 | Real MongoDB probes; cancellation registration races and actual Ember disconnect cleanup | PublisherBridgeSpec, RequestContextSpec, HiringApiRoutesSpec, HiringPlatformServerSpec, MongoDatabaseProbeIntegrationSpec | PASS: registration races, rejection during blocked teardown, cleanup before permit reuse, deadline-driven real socket RST cleanup |
| P1-AC07 | Compose plus sbt startup, restart/recovery and fixture retention | Actual Compose smoke and MongoDatabaseProbeIntegrationSpec | PASS: workflow below; disposable fixture survives database restart; original local stopped state/data preserved |
| P1-AC08 | Backend-only repository, no UI routes/assets/toolchain; schema export and introspection for API tooling | HiringGraphQLContractSpec, HiringApiRoutesSpec, source/build/index inspection | PASS: SDL/introspection, UI 404, no Node toolchain or tracked node_modules |
| P1-AC09 | Parseable JSON logs, correlation, no synthetic secrets on failure paths | SafeDiagnosticsSpec, HiringApiRoutesSpec, MainProcessSpec | PASS: captured request correlation, sanitized 500/field errors, actual Main config/bind/runtime reporter stdout/stderr |
| P1-AC10 | Inward dependencies, no retired SQL/wrapper dependencies or scaffolds, current startup/schema/evidence docs | Compilation, source/build inspection, documentation and independent review | PASS: build/source/docs checks and independent Code Reviewer, Security and final QA verdicts |

Final implementation checks (2026-09-16): `sbt test 'IntegrationTest / test'` exited 0 on Java 17.0.20/sbt 1.11.1: **49 unit tests and 13 integration tests passed**, none skipped. Integration breakdown: 8 live HTTP lifecycle tests, 2 disposable MongoDB tests, 3 actual Main child-process tests. The intentional bind-conflict test under MUnit emits its own runtime stack trace; separate actual Main process tests verify empty stderr and sanitized JSON stdout. `python3 scripts/check-skills.py` validated all 8 project-local skills; `git diff --check` and `git diff --cached --check` passed. No formatter is configured. The build has no Doobie/PostgreSQL, Scala Mongo wrapper, or unused Mongo Testcontainers module; no dependencies changed in this completion patch. All acceptance evidence remains subject to the final independent review gates below.

## Implementation handoff and checkpoint

Order: config/boundaries/lifecycle; Mongo/probes/HTTP/GraphQL/schema export; Compose/integration/recovery; independent reviews/fixes/final QA.

Planning baseline: three unit tests and eight local skills passed; Compose CLI is available. Planning Code Reviewer, Security Engineer and QA verdicts were PASS for specification readiness only. No runtime acceptance is completed by those verdicts.

Completed work: corrected parser/media-negotiation/teardown/runtime-logging defects; added regression coverage and reran checks; verified actual Compose plus application startup/outage/recovery; reconciled evidence and obtained independent implementation reviews and final QA. No data migration or deployment was performed. Next roadmap dependency: Phase 2 domain/MongoDB specification; no Phase 2 implementation is included here.

Completion checkpoint (2026-09-16): working tree was clean at `6f07e55` before these fixes. The compact-fragment token regression reproduced the original undercount (`testOnly *InputBudgetSpec`: one failure, four passes before the fix). The existing Compose MongoDB container was stopped and ports 8080/27017 were unused before smoke testing; preserve its bind-mounted data and restore its stopped state afterward. Parser fixes are coordinator-owned; request scope/media negotiation and runtime logging/process tests have disjoint specialist owners. Earlier 40-unit/10-integration passes are historical evidence, not final verification of this completion patch.

Actual local smoke (2026-09-16, Java 17.0.20, sbt 1.11.1, pinned standalone MongoDB, loopback ports 8080/27017): `docker compose config --quiet` and `docker compose up -d mongodb` passed; MongoDB became healthy. `sbt run` emitted STARTED. HTTP health/readiness returned 200 UP/READY; GraphQL returned 200 with both statuses. Downloaded SDL matched the checked fixture byte-for-byte; the standard introspection fixture returned Query with no errors. JSON quality-zero plus a positive wildcard returned 406. After `docker compose stop mongodb`, health stayed 200 UP, HTTP readiness returned 503 NOT_READY, and GraphQL returned 200 with NOT_READY. After `docker compose start mongodb`, both readiness paths returned READY without restarting the app. Ctrl-C released port 8080; a second `sbt run` rebound it and served health even with MongoDB stopped. SIGTERM to that test-owned child JVM emitted SHUTDOWN and exited 143 (expected signal termination, reported by sbt as a nonzero runner exit). The existing database was restored to its original stopped state; no local data was seeded or deleted. Retention verification remains in the disposable-container integration suite.

Implementation decisions discovered through source/tests:

- The user's backend-only/API-testing instructions replace the original UI slice. All GraphiQL assets, npm dependencies/lockfile, browser tooling, and UI configuration are removed. API documentation uses SDL/introspection with a standard query fixture; AC01/03/08 and affected reviews are reopened. No frontend browser pass is claimed as final-phase evidence.

- User-directed dependency cleanup reopens AC03/10 and relevant reviews. The original three scaffold tests and the interim SQL codec test are removed with their retired behavior; Foundation contract and runtime tests provide current-phase coverage. No SQL database or stored data is accessed or migrated by deleting unused source/dependencies.

- Ember 0.23.37 `ServerHelpers.runApp/runConnection` awaits response computation before the next socket write/read. Peer disconnect cleanup is therefore deadline-driven (probe two seconds/request five seconds, plus finalizer/scheduler time), not an immediate transport cancellation promise. Caller/request IO cancellation still immediately initiates scope cleanup. Real RST tests verify cleanup and capacity reuse; do not add a custom socket reader with different half-close semantics. Affected AC02/05/06 are reopened for independent review.
- Sangria 4.2.19 `ValueCoercionHelper.getVariableValue` checks non-null input before considering an omitted variable's default. The consumer fixture uses the supported nullable Boolean with default; clients can also supply explicit required-variable values. No library migration or general GraphQL coercion patch is included. Required-variable defaults remain a documented dependency limitation, not proof of full GraphQL conformance.
- Cats Effect's unhandled-fiber reporter bypasses SLF4J. `Main.reportFailure` now emits sanitized RUNTIME_FAILED JSON, including for Ember's bind-failure background fiber. MUnit uses its own runtime; the three passing MainProcessSpec tests separately verify actual forked Main configuration/bind failure output and its runtime reporter using a synthetic secret-bearing exception.

Completion review decisions: the independent architect approved the localized fixes and retained the deadline-based Ember contract. Code review found a further leading-zero scanner undercount; consuming a leading zero separately and regression tests closed it. QA's coverage audit added barrier-proven recovery of all sixteen permits, genuine 200 field-error and 500 failure tests with diagnostic correlation, and unselected-operation schema validation. These changes are included in the final 49-unit/13-integration run above.

Implementation Code Reviewer: **PASS** (independent read-only final source, tests/report and evidence review, 2026-09-16). Security Engineer: **PASS** (separate verdict covering Foundation controls/boundaries, same independent reviewer, 2026-09-16). Both confirmed all four findings and the leading-zero follow-up resolved, checked 49-unit/13-integration reports with no skips, and reviewed parent-recorded Compose smoke evidence without claiming an independent rerun.

Final independent QA: **PASS — P1-AC01–10** (2026-09-16). After Code/Security approval, QA independently executed `sbt 'testOnly *InputBudgetSpec *RequestContextSpec *HiringApiRoutesSpec' 'IntegrationTest / test'`: **23 focused tests plus all 13 integration tests passed**, exit 0 at 14:44:21 local. QA verified aggregate reports for 49 unit plus 13 integration tests with zero failures/errors/skips, unchanged source/build/Compose fingerprints, all 8 skills, both diff checks, and parent-recorded smoke/restoration evidence. No concrete blockers remain. These approvals certify scoped Foundation acceptance, not production readiness, hiring persistence, performance SLOs, or complete GraphQL conformance.
