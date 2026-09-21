# Hiring platform diagnostics and local masking

Status: done

## Scope and decisions

Add concise searchable diagnostics to the current backend without UI, new dependencies, HTTP/GraphQL schema changes, persistence changes, or an observability platform. Preserve the existing uncommitted hiring platform baseline patch. Existing logs contain only timestamp, severity, category and requestId; request rejection reasons and driver failure evidence are currently lost.

The current implementation intentionally avoids local GraphQL request payload reconstruction. Passwords, tokens, authorization/cookie headers, URI credentials/options, raw exception messages, full stack traces, raw bodies/query text, and raw variable values are never logged. Debugging relies on generated request IDs, fixed log markers, operation names, status/outcome fields, readiness/probe details, SDL/operation fixtures, and schema-backed GraphQL execution rather than a custom query/payload walker.

Masking defaults on from `application.conf`; an ignored `local.conf` may override it for local startup. `LOG_MASK_SENSITIVE` accepts only `true`/`false`. Disabling masking requires a numeric loopback HTTP bind address; unsafe combinations fail startup without echoing values. Configuration rendering stays redacted in all modes. Emit a visible local-unmasked warning and include masking mode in each structured log record. All API errors remain sanitized regardless of logging flags.

## Contract and implementation

- Keep existing JSON keys/categories. Add stable `marker` (`HP.<component>.<category>`), `component`, fixed human-readable `message`, `masking`, and bounded structured `details`. Use actual SLF4J severity and marker as well as their JSON equivalents.
- Production level checks use log4cats `SelfAwareStructuredLogger[IO]`; JSON/detail rendering remains by-name and happens only after the level is enabled. The final raw SLF4J emission retains the actual marker required by the contract, so structured laziness does not duplicate records or discard marker-based filtering.
- Application-owned typed field keys classify sensitivity and validate values centrally. Public fields: allowlisted method/route/reason/configuration key, bounded numeric status/duration, allowlisted exception type and one allowlisted application source filename/positive line number (never arbitrary StackTraceElement rendering, method names or paths). Unknown public values become `[FILTERED]`; unknown errors become OtherException/unavailable. Sensitive fields: GraphQL operation name, MongoDB hosts/database, HTTP bind host. Mask sensitive values as `[REDACTED]` by default. Explicit local disclosure can reveal arbitrary text inside those selected metadata values; no universal secret detector is claimed.
- Bound ordinary metadata values to 128 characters, each record to 12 detail fields and 8192 UTF-8 bytes, and Mongo host extraction to four parsed hosts without userinfo/options. Normalize controls to spaces; preserve valid single-line JSON rather than truncating encoded records. Field names, markers and messages are fixed, never request-derived.
- HTTP server lifecycle is owned by the otel4s http4s middleware: server spans, active-request metrics, duration metrics, abnormal termination metrics and W3C propagation are not duplicated as application log events. Emit a rejection record with a bounded reason where applicable and correlate it with the active trace ID. Operation names come from the already parsed Sangria operation model; no custom GraphQL parser or payload sanitizer is maintained.
- Preserve trace-ID correlation across HTTP, readiness service and Mongo probe diagnostics. Keep the existing no-argument probe check and add a request-context overload with a default implementation for compatibility. Log safe driver error classification/location and elapsed time at the adapter boundary; never pass a Throwable or URI to the renderer.
- Main reports configuration key names, safe startup/runtime failure class/location, startup bind metadata, and shutdown. The unhandled-runtime reporter always uses masked diagnostics, including before valid configuration exists.
- Every call site uses a non-recursive best-effort emission boundary that catches synchronous/effectful sink failures without changing HTTP/probe outcomes or swallowing cancellation. The production sink isolates blocking logger calls. Cancellation is logged only after required request cleanup, with an explicit cancelled outcome and no invented/delivered HTTP status. Completion denotes application response creation, not confirmed network delivery. The local-unmasked warning bypasses ordinary severity filtering, including ERROR.

## Acceptance and verification

| ID | Required result | Evidence |
|---|---|---|
| LOG-01 | Stable markers, concise messages, correct levels and bounded parseable JSON | SafeDiagnosticsSpec covers actual Logback marker/level, Unicode bounds, throwing sinks, and disabled-level field-thunk laziness |
| LOG-02 | Useful HTTP/probe failure reasons, timing and shared request correlation without raw payloads | HiringApiRoutesSpec: 24; HealthServiceSpec: 8; MongoDatabaseProbeSpec: 5; real Mongo integration: 2 passed, including shared deadlines/correlation |
| LOG-03 | Masking on by default; explicit loopback-local opt-out; unsafe/invalid config rejected | AppConfigSpec and actual MainProcessSpec |
| LOG-04 | Forbidden credential/raw-body/raw-query/raw-variable/exception-message sources excluded; forged public metadata filtered; throwing sinks preserve outcomes/cancellation | Renderer, route and process adversarial checks, including genuine field-error completion diagnostics and throwing cancellation diagnostics after cleanup |
| LOG-05 | Current runbook/examples/specs, full regression suite and independent review gates | Runbook and examples updated; 87 unit + 28 integration tests passed; eight skills validated; diff check clean; independent Code, Security and QA PASS |

Run focused tests, `sbt test`, `sbt 'IntegrationTest / test'`, skill validation and diff checks. Preserve the existing SDL and fixtures. Local bounded workload only; request logging adds one completion event per request, no performance/SLO claims. No dependency upgrades, local data writes, or paid provisioning are required.

Product Manager coordinates. One writer per file: coordinator owns diagnostics model/renderer/tests and documentation; Scala specialists own config/Main/process tests and HTTP/service/Mongo instrumentation/tests respectively. Architect and Security review design first; independent Code Reviewer and Security verdicts precede final independent QA. Source changes reopen the affected hiring platform logging/configuration acceptance evidence until these gates pass.

## Checkpoint

Architecture direction PASS and revised Security design readiness PASS (2026-09-16). Required clarifications incorporated: controlled public values, best-effort non-recursive emission, global bounds, and explicit local disclosure limitations.

Initial implementation validation: the baseline logging/config suites passed 8 tests before changes. A strict unused-parameter compiler warning on the compatibility probe overload was fixed with a scoped annotation. `sbt test 'IntegrationTest / testOnly *MainProcessSpec'` then exited 0: 83 unit tests and 18 actual Main process tests passed. Final QA coverage additions (actual SLF4J marker/level, real adapter/service deadline correlation, and throwing cancellation diagnostics) are being integrated; these initial results are not final acceptance evidence. Previous hiring platform baseline changes and user staging are preserved. No deployment or local database data changes performed.

Final implementation verification (2026-09-16): `sbt test 'IntegrationTest / test'` produced passing reports for 87 unit tests and 28 integration tests (8 server, 2 Mongo, 18 Main process), with zero failures, errors or skips. This includes all four additional QA coverage cases. `python3 scripts/check-skills.py` validated eight project-local skills; `git diff --check` passed. Independent final static Code Reviewer PASS and separate Security Engineer PASS were returned after source inspection and unit-report verification. Independent final QA remains the closure gate. No public SDL, dependencies, persistence schema, UI or Node tooling changed.

Closure (2026-09-16): independent final QA PASS for LOG-01–05 after rerunning 67 focused unit tests and 20 Mongo/Main integration tests, all passing with exit 0. QA reused the unchanged eight server tests from the full regression run and confirmed aggregate reports, documentation links, skill validation and clean diff checks. An initial QA class-loading failure was resolved by the escalated rerun, not counted as a pass. All required independent gates now pass; the Product Manager closes this extension and the reopened hiring platform criteria. No deployment or performance certification is implied.
