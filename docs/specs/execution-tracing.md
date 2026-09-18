# Hiring execution tracing

Status: in progress

## Scope and decisions

Add source-correlated structured execution diagnostics for incident investigation. The scope covers the HTTP, authentication, GraphQL, application-service, MongoDB, request-cache, admission, setup, and embedding-work boundaries. It does not change GraphQL SDL, hiring-domain behavior, MongoDB schema, or external observability infrastructure.

Severity is configured only by the packaged `src/main/resources/logback.xml`. It defaults to `INFO`; enabling `DEBUG` or `TRACE` requires changing the packaged XML and redeploying. Application HOCON no longer has a log-level setting. `logging.mask-sensitive` remains the disclosure policy and defaults to `true`.

When masking is disabled, only an explicit approved field set is revealed: UUID actor/entity identifiers, operation/use-case names, job title, country, city, skills, booleans, enum/status values, counts, pagination size, collection/operation names, versions/retries, queue state, and durations. Credentials, headers, cookies, tokens, raw GraphQL documents/variables/bodies, job descriptions/requirements, feedback/reasons, resumes, search text, vectors, BSON filters, Mongo URIs, raw exceptions, and stacks are never diagnostic fields.

`TRACE` has no per-request cap and logging remains synchronous by explicit product decision. The appender rolls ten 20 MiB files, and the operational cost of slow file I/O during a short production investigation is accepted. An unmasked production appender must be a restrictive, service-owned rolling-file destination; startup rejects an unsafe destination.

## Source facts and implementation boundary

- `Diagnostics` is the existing application-facing logging port; `SafeDiagnostics` renders bounded JSON and invokes SLF4J.
- `HiringApiRoutes` creates the current request UUID; `RequestContext` bridges Cats Effect into Sangria futures; Mongo publisher bridge, admission, setup, and embedding pipeline are the meaningful current concurrency boundaries.
- The domain remains pure and does not receive a tracing dependency. Instrumentation belongs in transport, service/repository decorators, and infrastructure adapters.

## Acceptance and evidence

| ID | Given / When / Then | Implementation and test paths | Verification | Actual outcome |
|---|---|---|---|---|
| ETR-01 | Given packaged Logback defaults to INFO, when DEBUG/TRACE calls exist, then they are suppressed by XML rather than application configuration and matching SLF4J levels are used when enabled. | Pending implementation | Focused logging tests | Not run |
| ETR-02 | Given an HTTP GraphQL request, when it crosses resolver, service, repository, and Mongo boundaries, then records share a trace ID with parent spans and ordered sequence values. | Pending implementation | Focused route/GraphQL/Mongo tests | Not run |
| ETR-03 | Given concurrent resolvers, cancellation, admission, setup, or embedding work, when trace is enabled, then lifecycle outcomes are observable without changing ownership or cleanup. | Pending implementation | Focused concurrency/cancellation tests | Not run |
| ETR-04 | Given masking is enabled or disabled, when approved and forbidden values are submitted, then only approved values may be revealed and forbidden data never appears. | Pending implementation | Adversarial renderer/config tests | Not run |
| ETR-05 | Given an unmasked production configuration, when the application destination is not a restrictive rolling file, then startup fails safely; valid rolling configuration retains ten 20 MiB files. | Pending implementation | Process/config/logging tests | Not run |
| ETR-06 | Given all changed code and documentation, when local validation runs, then unit/integration suites, skill checks, whitespace checks, independent code/security review, and final QA are recorded separately. | Pending implementation | Local commands and reviews | Not run |

## Workload and risks

The existing server allows at most 64 connections and defaults to 16 admitted GraphQL/readiness requests. No performance SLO is claimed. Verification will record event count, latency, errors, and resource impact for a bounded local 16-concurrent representative workload. Synchronous file output can increase latency when storage is slow; this accepted risk is documented and tested rather than hidden.

## Handoff and review

- Scala implementation owner: runtime/config/source/tests under `src/`.
- Coordinator: this specification and logging documentation only.
- Architect design review: completed during planning; implementation changes require final independent review.
- Code Reviewer: pending after implementation.
- Security Engineer: pending after implementation.
- Final QA: pending after review fixes and required checks.
