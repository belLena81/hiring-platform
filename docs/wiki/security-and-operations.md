# Security and operations

[Wiki home](README.md) · [Domain/use cases](domain-and-use-cases.md) · [Storage](mongodb-and-analytics.md) · [Workflows/patterns](workflows-and-patterns.md)

## Identity and authorization

[JwtActorAuthenticator](../../src/main/scala/com/example/graphQL/cats/api/auth/JwtActorAuthenticator.scala) accepts a single Bearer credential, verifies HS256 signature, issuer, audience, expiration and not-before, and parses a typed subject ID. It resolves the actor through [UserAuthenticationService](../../src/main/scala/com/example/graphQL/cats/service/auth/UserAuthenticationService.scala). Stored user state determines role and active identity. Missing credentials allow public fields; protected fields still require authorization. Invalid credentials are rejected; authentication-store unavailability is distinguished from invalid identity.

[UserAccountService](../../src/main/scala/com/example/graphQL/cats/service/auth/UserAccountService.scala) owns registration, login, self-service changes and deletion. The application-owned [PasswordHasher port](../../src/main/scala/com/example/graphQL/cats/service/auth/PasswordHasher.scala) is implemented by the infrastructure [Argon2 adapter](../../src/main/scala/com/example/graphQL/cats/infrastructure/auth/Argon2PasswordHasher.scala), with configured cost, bounded concurrency and blocking native execution. Public signup cannot provision Admin; trusted startup configuration invokes [MongoAdminSeed](../../src/main/scala/com/example/graphQL/cats/repository/mongo/MongoAdminSeed.scala) before workers/HTTP. It is disabled by default, preserves an existing same-name Admin without resetting credentials, and rejects conflicting identity/registry state.

Authentication receipts store domain-separated HMAC fingerprints keyed by the receipt secret (JWT secret when unset); token-producing replay verifies current credentials and the referenced active identity. Changing the key causes old auth receipt conflicts rather than legacy fallback. [AuthRateLimiter](../../src/main/scala/com/example/graphQL/cats/api/admission/AuthRateLimiter.scala) bounds login/signup attempts, and [ClientAddressResolver](../../src/main/scala/com/example/graphQL/cats/api/http/ClientAddressResolver.scala) applies trusted-proxy policy.

| Actor | Main capabilities | Boundary |
|---|---|---|
| Candidate | Discover eligible jobs, apply, inspect own applications/history, manage own account and inspect own interviews | Candidate identity derives from authenticated actor; private profiles are not unrestricted search results |
| Recruiter | Manage owned jobs/applications, match visible candidates for an owned job, schedule/inspect owned-job interviews | Ownership and visibility checks in services and scoped persistence access |
| Admin | Authorized cross-user hiring access, user management, reporting and workflow repair | Singleton persistence constraint and the trusted seed boundary described above; repair uses expected revision, idempotency and reconciliation |

The [operation map](domain-and-use-cases.md) gives field-specific rules. [ActorAuthorization](../../src/main/scala/com/example/graphQL/cats/service/auth/ActorAuthorization.scala), [HiringReadScope](../../src/main/scala/com/example/graphQL/cats/service/read/HiringReadScope.scala), and [MongoAuthorizedReadQueries](../../src/main/scala/com/example/graphQL/cats/repository/mongo/MongoAuthorizedReadQueries.scala) enforce checks below resolvers. Request-local batching does not create a shared cross-user entity cache. Mutation predicates/revisions preserve relevant checks under concurrent writes.

## Resource controls

| Boundary | Current mechanism | Source |
|---|---|---|
| HTTP body | 64 KiB entity limit | [HttpMiddleware](../../src/main/scala/com/example/graphQL/cats/api/http/HttpMiddleware.scala) |
| Execution | Configured admission permits and timeout; probes bypass application admission | [HiringApiRoutes](../../src/main/scala/com/example/graphQL/cats/api/http/HiringApiRoutes.scala) |
| Server | 64 connections, 8,192-byte headers, 5-second header timeout, 10-second idle/shutdown timeouts | [HiringPlatformServer](../../src/main/scala/com/example/graphQL/cats/runtime/HiringPlatformServer.scala) |
| GraphQL | Depth 16, complexity 1,000, page-aware costs, configurable discovery-root limit | [Schema assembly](../../src/main/scala/com/example/graphQL/cats/api/graphql/HiringGraphQLSchemaAssembly.scala) |
| Parsed document cache | 256 entries, 60-second expiry; validated document reuse | [GraphQLDocumentCache](../../src/main/scala/com/example/graphQL/cats/api/graphql/GraphQLDocumentCache.scala) |
| Database discovery | Shared permits and Mongo maximum query time | [DiscoveryQueryPolicy](../../src/main/scala/com/example/graphQL/cats/repository/mongo/DiscoveryQueryPolicy.scala) |
| Workers | Bounded parallelism/batches, leases, retry limits, provider timeouts, resource-owned fibers | [Workflow patterns](workflows-and-patterns.md) |

[CursorCodec](../../src/main/scala/com/example/graphQL/cats/api/graphql/CursorCodec.scala) signs ordinary cursors using a derived HMAC key, expiration, issuer/audience and entity kind. [NearbyJobCursorCodec](../../src/main/scala/com/example/graphQL/cats/service/search/NearbyJobCursorCodec.scala) handles discovery continuation. Cursors do not grant authorization: scoped queries still apply. Keyset ordering and bounded pages avoid unrestricted list materialization.

## Errors and sensitive data

[GraphQLFailureCatalog](../../src/main/scala/com/example/graphQL/cats/api/graphql/GraphQLFailureCatalog.scala) and resolver support translate expected errors into meaningful GraphQL failures. Unexpected framework/driver errors become sanitized client messages plus internal diagnostics. [HTTP routes](../../src/main/scala/com/example/graphQL/cats/api/http/GraphQLHttpRoutes.scala) distinguish malformed requests, media negotiation, authentication and unavailable service. GraphQL field errors can appear within HTTP 200 responses.

Event payloads, candidate projections, quarantine and diagnostics have separate minimization rules. [SafeDiagnostics](../../src/main/scala/com/example/graphQL/cats/infrastructure/logging/SafeDiagnostics.scala) and [logging documentation](../logging.md) define the diagnostic boundary. Account erasure includes embeddings, interviews and analytical copies: a user tombstone alone does not establish physical deletion. Kafka/Delta retention and guarded producer/key retirement are downstream barriers; see [storage](mongodb-and-analytics.md).

These are implemented controls, not an OWASP audit, dependency advisory scan or certification of arbitrary deployment settings.

## Configuration and runtime ownership

[AppConfig](../../src/main/scala/com/example/graphQL/cats/config/AppConfig.scala) loads typed PureConfig/HOCON settings and validates them before runtime construction. [Packaged defaults](../../src/main/resources/application.conf), normal `config.file`/`config.resource` selectors and HOCON substitutions control loading. `.local/config/` is an ignored storage convention, not an automatically loaded directory.

Mongo setup creates/verifies owned collections, indexes, validators and versioned migrations. Startup preserves data unless `mongo.reset-on-start` is explicitly enabled. Vector search, operational Kafka publisher/consumer and interview processing have configuration-dependent activation. Voyage provides embeddings when enabled. Interview runtime wires durable fake calendar/notification providers. Spark/Delta runs separately with batch and opt-in continuous modes. Enabling a component does not establish external acceptance.

Use the [README](../../README.md) for startup, [MongoDB design](../mongodb-design.md) for migration/index evidence and [analytics architecture](../big-data-architecture.md) for activation gates. Machine-specific settings, datasets, logs and backups belong in ignored local paths. Existing packaged logging uses `_logs/`, as documented in [engineering quality](../engineering-quality.md).

## Health and observability

`GET /health` reports liveness. `GET /ready` uses Mongo/setup readiness and enabled embedding-worker health. It does not prove Kafka freshness, real interview delivery, analytical retention completion or search relevance. [TelemetryRuntime](../../src/main/scala/com/example/graphQL/cats/infrastructure/telemetry/TelemetryRuntime.scala) composes OpenTelemetry tracing/metrics; [Diagnostics](../../src/main/scala/com/example/graphQL/cats/service/Diagnostics.scala) defines structured events/fields and correlation.

Durable failure states, quarantines, revision conflicts and repair states expose recovery needs. Repair should follow the owning workflow contract rather than arbitrary document edits. Analytics persistent writer/maintenance ownership can intentionally require operator reconciliation instead of automatic takeover.

## Validation and readiness

The [root build](../../build.sbt) separates unit `test` from live HTTP/disposable MongoDB `IntegrationTest / test`; analytics has its own build. Local checks establish only exercised behavior on that source/environment. Atlas fusion/reranking, real providers, genuine Kafka/Delta retention, external-writer exclusion, key retirement and deployed SLOs have additional [roadmap gates](../development-milestones.md).

This wiki is source-derived documentation. Its validation covers source correspondence, local links and required root unit/format checks. It does not activate services, reset data, rerun live provider/Atlas checks, or refresh historical analytics benchmarks.
