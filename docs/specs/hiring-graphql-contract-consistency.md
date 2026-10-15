# Hiring GraphQL contract consistency

## Identity and scope

- Task: close public-schema gaps found by a review against GraphQL books. Post-Phase-15 API hardening; no new roadmap phase.
- Status: done (Slices A, B, C, D integrated; Code Review, Security, Data Engineer and QA verdicts PASS)
- Coordinator / owners: Product Manager; Scala Developer (Slices A, C), Data Engineer + Scala Developer (Slice B), Security Engineer review (Slice C and AC-08/AC-12), docs owner (Slice D).
- User outcome: one coherent, self-describing, typed schema that clients can map errors and pagination against, plus an optional production allowlist of queries.
- Actors: Candidate, Recruiter, singleton Admin, anonymous (login/signup, pre-auth).
- Non-goals: subscriptions; Node interface / global IDs; new pagination in the semantic search service; versioned endpoints; deprecations or dual shapes (pre-MVP: one active shape, change directly); hash-only (APQ) registration; frontend assets; changing HTTP `/health` and `/ready`.
- Dependencies: none external. Slice B touches operational MongoDB data shape (Data Engineer gate).

## Source context and decisions

- Verified implementation: schema code in `src/main/scala/com/example/graphQL/cats/api/graphql/`; snapshot `src/test/resources/graphql/hiring.graphql`; contract tests `HiringGraphQLContractSpec`, `GraphQLDocumentCacheSpec`. Facts checked while writing: `profileFor` (`HiringGraphQLAccountResolvers.scala:136-176`) builds the whole profile from the input, so `updateMyProfile` is full replacement, not a partial update; `ApplicationEvent` stores only `actorId: UserId` (`domain/model/Application.scala:37`), no actor role; `GraphQLDocumentCache` is the only document-parse point, wired in `Main.scala` and `HiringApiRoutes.scala`; no persisted-query code exists.
- Target requirements: AGENTS.md (pre-MVP contract policy, deny-by-default, sanitized errors), `docs/schema-evolution.md`, `docs/engineering-quality.md`.
- Assumptions: no external clients exist beyond repo fixtures/examples (validate: grep `docs/api.md`, `src/test/resources/graphql`, README); Sangria 4.2.19 validates `@oneOf` inputs and renders it in SDL (validate first in Slice A; if SDL rendering lacks `@oneOf`, declare it via a schema directive and record the fact).

### Decisions (D)

| # | Decision | Basis | AC |
|---|---|---|---|
| D1 | `ErrorCode` enum is generated from the `GraphQLFailureCatalog` entries (one source); a unit test asserts every catalog code is an enum value and vice versa. `PERSISTED_QUERY_NOT_FOUND` is an HTTP-level error outside `UserError`, so it is not in the enum | One source avoids drift | AC-03, AC-12 |
| D2 | Ranked results stay a documented bounded top-N list: no `pageInfo` (recommended over the brief's "pageInfo with hasNextPage false") | A constant `false` is a fake pagination signal and invites clients to build paging against it; descriptions state the bound | AC-04 |
| D3 | `updateMyProfile` is full replacement of the profile for the caller's role; descriptions say so. `profile` is required on `UpdateMyProfileInput`. On `SignUpInput`, `profile` is required for Candidate/Recruiter and absent for Admin (Admin signup stays rejected by existing rules) | Matches code today; partial update would be a semantic change not requested | AC-05 |
| D4 | `RecruiterProfileInput.organizationName` becomes `String!` (today absent maps silently to ""). Recommended; it follows from the new typed input | Removes an invalid-state default | AC-05 |
| D5 | `ValidationError` gets `field: String` = field of the first domain error (null if none) and keeps the joined `message`. No `fields` list | Simplest; one field lets clients mark an input, remaining errors are fixed on resubmission. Revisit if clients need all fields at once | AC-10 |
| D6 | (User decision Q1) The acting user's role is persisted on new `ApplicationEvent`s: domain `actorRole: Option[UserRole]` (set by `ApplicationLifecycle` callers from the trusted actor), `StoredApplicationEvent` gets an optional field (codec already ignores None, so old documents decode as None, no backfill). GraphQL `ApplicationEvent.actorRole: UserRole` is nullable (pre-MVP local events lack it) and replaces `actorId`, which is no longer exposed. `ApplicationEventFields` strict-decode lists must treat the new field as optional. Operational/Kafka and analytics contracts are unchanged: verified that `OperationalEvents.statusChanged`/`candidateHired` copy only `event.actorId` into the existing envelope `actorId` and build explicit payloads, so no ApplicationEvent document shape reaches Kafka or analytics (re-verify in Slice B with a grep of `analytics/` for `actorRole`/event-document use, and an envelope contract test showing no change) | User decision; role-only exposure leaks no recruiter identifiers to candidates | AC-08, AC-09-B |
| D7 | Persisted-query allowlist accepts full documents only. The hash is lowercase hex SHA-256 over the exact UTF-8 bytes of the `query` string, no normalization. Manifest is read once at startup; `allowlist` mode with a missing, unreadable or empty manifest fails startup (typed config error). The check runs before `GraphQLDocumentCache.document`, applies to every operation except introspection (user decision Q3: introspection is exempt and governed only by `graphql.schema-exposure`; detected by operation inspection after a bounded parse that is only reached when the document is not in the manifest, so the exemption must accept only documents whose every root selection is `__schema`/`__type`/`__typename`, with the document-size limit already applied), and returns HTTP 400 `PERSISTED_QUERY_NOT_FOUND` with no echo of the document. `off` leaves behavior unchanged. | Hash-only registration needs a writable store and an unauthenticated registration path; allowlist is a read-only set | AC-12 |
| D8 | `Location.city` becomes `Option[String]`. Codec: decode missing, null or `""` as `None`; encode `None` by omitting the field. No backfill code. Search text for embeddings must render a missing city exactly as `""` rendered before, so existing `sourceHash` values do not churn (verify in Slice B) | Pre-MVP no-backfill policy; avoids mass re-embedding | AC-09 |
| D9 | `retryAfterSeconds` is the ceiling of the remaining window, minimum 1, taken from the same allowance calculation that rejects the call. Consequence: `FixedWindowRateLimiter` now reports the remaining window (ceil, min 1s), which also changes login/signup and query-cost-budget `Retry-After`/`retryAfter` from the full window to the remaining window; Security Engineer review covers this | One computation, no drift | AC-11 |
| D10 | `scalar Long` is declared with a description and serializes as a JSON number; coercion rejects non-integral and out-of-range input. `InterviewParticipant` enum has exactly Candidate, Recruiter | Declared types make the SDL self-contained | AC-02, AC-03 |
| D11 | A fixture-backed contract test replaces `health.graphql`/`readiness.graphql` coverage with an executed fixture on another operation, so the executed-fixture mechanism keeps at least one health-independent case | Preserves test mechanism | AC-13 |

### Open questions

- Resolved by the user: Q1 persist `actorRole` (D6); Q2 single `field` (D5); Q3 introspection exempt (D7); Q4 full-replacement `updateMyProfile` stands and is documented (D3).
- None open. Remaining unknown: whether Sangria renders `@oneOf` in SDL (Assumptions).

## Behavior and contracts

All changes are API-layer except AC-09 (domain/persistence) and AC-12 (HTTP/config). Authorization is unchanged; no resolver may loosen a service check. Public names change as listed with schema, fixtures, `docs/api.md` and examples updated in the same slice (single active shape).

- AC-01 Descriptions: every root Query/Mutation field, argument, input type and field, union and enum value has a non-blank description via a helper in `HiringGraphQLDsl.scala`. A test walks the introspected schema and fails on any missing description (excluding built-in introspection types). Login/signup descriptions state they report errors at top level because the caller is unauthenticated.
- AC-02 `scalar Long` declared and used by `expectedRevision` inputs and reporting types; replaces Sangria `LongType`.
- AC-03 `InterviewWorkflow.progress: InterviewProgress!` (15 values from `InterviewWorkflowPhase`, mapping exhaustive by compiler match); `notifiedParticipants: [InterviewParticipant!]!`; `UserError.code: ErrorCode!`. Top-level `extensions.code` strings are unchanged.
- AC-04 `NearbyJobsConnection { edges { node: Job!, distanceKm, cursor }, pageInfo }`; cursor logic unchanged. Shared `connectionType` loses `searchId`; `JobSearchConnection` (returned by `jobs`) adds `searchId: SearchID`. Ranked lists: D2.
- AC-05 `profile: ProfileInput @oneOf { candidate: CandidateProfileInput, recruiter: RecruiterProfileInput }`; `ResidenceInput { country: String!, city: String }`. A variant that does not match the caller/target role keeps `PROFILE_ROLE_MISMATCH` (service); Admin keeps `PROFILE_UNSUPPORTED_FOR_ROLE` (resolver). The old "city without country" validation disappears by construction.
- AC-06 One `JobFilter` input; `NearbyJobsFilter` deleted. `jobs(filter, first, after, searchId)` and `semanticJobSearch` validate through `JobDiscoveryValidation.filter`; failure is `INVALID_SEARCH_FILTER` (UserError or error as today for the same operation family; keep existing placement per operation).
- AC-07 `interviewWorkflow(workflowId: InterviewWorkflowID!): InterviewWorkflow` nullable. Not found and not-visible-to-actor both return null (no existence oracle); other failures remain GraphQL errors.
- AC-08 Typed scalars `InterviewWorkflowID`, `SearchID`, `ApplicationEventID`, `DeletionReceiptID` on inputs and outputs; `CandidateMatchCandidate.id: UserID!`; `ApplicationEvent.actorId` removed and replaced by nullable `actorRole: UserRole` (D6; persistence in Slice B).
- AC-09 `UpdateJobInput.job` (renamed from `patch`; still full replacement, said in the description). `InteractionSuccess { eventId: UUID! }`. `Location.city: String` nullable in GraphQL, domain, Mongo codec (D8), discovery facets (null cities excluded from city facets), search filters and embedding text.
- AC-10 `ValidationError.field: String` (D5).
- AC-11 `DomainError.retryAfterSeconds: Int` set only for interview-action `RATE_LIMITED`; null elsewhere (D9).
- AC-12 Config `graphql.persisted-queries { mode = off|allowlist, manifest = <path> }` in typed config (`GraphQLConfigReader`, `GraphQLConfigValidation`, `AppConfigModels`); default `off` in `application.conf`; manifest path is a local config value (`.local/config/` for machine paths, never committed). Manifest is generated from `src/test/resources/graphql/*.graphql` by a test-scope main (a fresh generation must equal the committed manifest, enforced by a test). Rate limits and the query cost budget still apply to allowed documents.
- AC-13 Remove health/readiness fields, types, enums, inputs and fixtures; HTTP `/health` and `/ready` unchanged.

Schema evolution: pre-MVP single active shape; no migration of the API. Persistence: only AC-09 (D8) and the optional stored event `actorRole` (D6) touch stored data (both read old documents; no backfill); AC-05 and others do not. Recovery for AC-09: the codec reads both old (`""`) and new (absent) documents, so application rollback is safe for reads; data rollback is not claimed.

## Domain language and examples

- Contexts touched: Hiring (operational) API and Job aggregate (`Location`); Search and embedding (city in filters/text). Invariants: Job lifecycle and Application invariants unchanged; User role-specific profile invariant (AC-05) enforced as before in domain constructors.
- Terms added to `docs/domain-language.md`: Persisted-query allowlist, Error code, Bounded top-N list.

| Rule | Tag | Scenario |
|---|---|---|
| Descriptions complete | AC-01.1 typical | Given the introspected schema, when every non-built-in element is checked, then none has a blank description |
| | AC-01.2 failure | Given a test field added without description, then the description test fails naming it |
| Long declared | AC-02.1 typical | `expectedRevision: 3` is accepted and the SDL contains `scalar Long` |
| | AC-02.2 boundary/forbidden | `expectedRevision: 1.5` or `"3"` is rejected by coercion |
| Enum progress/participants/code | AC-03.1 typical | A workflow in `PROPOSAL_PENDING` renders its `InterviewProgress` value; all 15 domain phases map |
| | AC-03.2 typical | Rate-limited interview action returns `UserError.code = RATE_LIMITED` |
| | AC-03.3 boundary | Top-level auth error has `extensions.code = "UNAUTHORIZED"` (string unchanged) |
| Pagination | AC-04.1 typical | `nearbyJobs(first:2)` returns edges with `node.id`, `distanceKm`, `cursor`; `after` of the last cursor returns the next page |
| | AC-04.2 boundary | `jobs(first:2).searchId` exists; `ApplicationConnection` has no `searchId` field |
| | AC-04.3 typical | Ranked results expose no `pageInfo`; description states the bound |
| Profile oneOf | AC-05.1 typical | Recruiter signs up with `profile:{recruiter:{organizationName:"Acme"}}` and gets a recruiter profile |
| | AC-05.2 forbidden | Both `candidate` and `recruiter` set, or neither, fails input validation (Sangria oneOf validation covers variables typed directly as `ProfileInput`; `ProfileInput` nested inside another input is covered by the resolver check) |
| | AC-05.3 forbidden | Candidate `updateMyProfile` with `{recruiter:...}` returns `PROFILE_ROLE_MISMATCH`; Admin returns `PROFILE_UNSUPPORTED_FOR_ROLE` |
| | AC-05.4 boundary | `residence:{country:"DE"}` stores city absent; omitted `residence` clears residence (full replacement) |
| JobFilter | AC-06.1 typical | `jobs(filter:{city:"Berlin", skills:["scala"]})` returns matching open jobs |
| | AC-06.2 failure | A blank `city` or a list of more than 100 skills yields `INVALID_SEARCH_FILTER`, same as `discoveryFilter` |
| | AC-06.3 boundary | `semanticJobSearch` rejects the same invalid filter |
| Nullable workflow | AC-07.1 typical | Unknown `workflowId` returns `data.interviewWorkflow = null`, no errors |
| | AC-07.2 forbidden | Another candidate's workflow also returns null |
| | AC-07.3 failure | Store failure still returns a GraphQL error, not null |
| Typed IDs | AC-08.1 typical | A malformed `SearchID` is rejected at coercion |
| | AC-08.2 typical | Recruiter Rita moves Ana's application to Accepted; `applicationHistory` shows `actorRole = RECRUITER`; the schema has no `actorId` on `ApplicationEvent` |
| | AC-08.3 boundary | A pre-existing local event without a stored role reads as `actorRole = null`; the document still decodes |
| | AC-08.4 forbidden | A Candidate cannot read another candidate's history (existing access check unchanged) |
| | AC-08.5 typical | Admin rejection stores `ADMIN`; the Kafka envelope `actorId` and payload are byte-identical to before |
| Naming/payload | AC-09.1 typical | `updateJob(input:{job:{...}})` replaces the job; `recordInteraction` returns `eventId` |
| | AC-09.2 boundary | Job created without city has `location.city = null`; a stored `""` reads as null |
| | AC-09.3 typical | City facet counts exclude city-less jobs |
| Validation field | AC-10.1 typical | Blank title returns `ValidationError { field: "title" }` |
| | AC-10.2 boundary | Two invalid fields return the first field and a joined message |
| Retry hint | AC-11.1 typical | The 6th interview action in a window of 5/min returns `RATE_LIMITED` with `retryAfterSeconds` in 1..60 |
| | AC-11.2 boundary | Other errors have `retryAfterSeconds` null |
| Allowlist | AC-12.1 typical | Mode `allowlist`, manifest contains hash of `jobs.graphql`: request runs |
| | AC-12.2 forbidden | Same query with one extra space: HTTP 400 `PERSISTED_QUERY_NOT_FOUND`, not parsed, document not cached |
| | AC-12.3 failure | Mode `allowlist` with missing manifest: startup fails with a config error |
| | AC-12.4 boundary | Mode `off`: any valid query runs as before |
| | AC-12.6 boundary | Allowlist mode: a pure `{ __schema { types { name } } }` document runs without being in the manifest (subject to schema-exposure); `{ __schema{...} jobs{...} }` mixing a real field is rejected |
| | AC-12.5 forbidden | A request with only `extensions.persistedQuery.sha256Hash` and no `query` is rejected without lookup |
| Health removed | AC-13.1 typical | SDL has no `health`/`readiness`; `GET /health` and `/ready` still return as before |

## Acceptance and evidence

Evidence is filled only after observation. Test names start with the tag.

| ID | Given / When / Then | Implementation and test paths | Verification | Actual outcome |
|---|---|---|---|---|
| AC-01 | AC-01.1-.2 | `api/graphql/HiringGraphQLDsl.scala`; `HiringGraphQLContractSpec` | `sbt test` | Slice A done: `sbt test` 1348 pass; `SchemaDescriptionCheck` + AC-01.1-.3 in `HiringGraphQLContractSpec` |
| AC-02 | AC-02.1-.2 | `HiringGraphQLTypes`, `HiringGraphQLReportingTypes`, `HiringGraphQLModel` | `sbt test`; SDL regenerated | Slice A done: AC-02.1/.2 in `HiringGraphQLContractSpec`; SDL declares `scalar Long` (renderer filter changed to `SchemaFilter.withoutGraphQLBuiltIn`, because the default filter hides any scalar named Long) |
| AC-03 | AC-03.1-.3 | `HiringGraphQLTypes`, `GraphQLFailureCatalog`, `domain/workflow` mapping | `sbt test` | Slice A done: AC-03.0-.3 in `HiringGraphQLContractSpec`, `HiringGraphQLInterviewActionSpec`, `HiringGraphQLAccessSpec`; extensions.code is `UNAUTHORIZED` (spec text said UNAUTHENTICATED; code string unchanged) |
| AC-04 | AC-04.1-.3 | `HiringGraphQLSearchResolvers`, `HiringGraphQLJobResolvers`, `HiringGraphQLTypes` | `sbt test`; `sbt 'IntegrationTest / test'` | Slice A done: AC-04.1 `HiringGraphQLDiscoveryContractSpec`; AC-04.2/.3 `HiringGraphQLAccessSpec` |
| AC-05 | AC-05.1-.4 | `HiringGraphQLInputs`, `HiringGraphQLAccountResolvers` | `sbt test`; integration | Slice A done: AC-05.0-.4 `HiringGraphQLAccessSpec`, `HiringGraphQLContractSpec` |
| AC-06 | AC-06.1-.3 | `HiringGraphQLInputs`, `HiringGraphQLSearchResolvers`, `HiringGraphQLResolverSupport` | `sbt test` | Slice A done: AC-06.1-.3 `HiringGraphQLDiscoveryContractSpec`, `HiringGraphQLAccessSpec`; `createdAfter` in the far future is NOT rejected by `JobDiscoveryValidation.filter` (only blank/oversized city and >100 or oversized skills are), so AC-06.2 is tested with those |
| AC-07 | AC-07.1-.3 | `HiringGraphQLInterviewResolvers` | `sbt test` | Slice A done: AC-07.1-.3 `HiringGraphQLInterviewActionSpec` |
| AC-08 | AC-08.1-.5 | `HiringGraphQLInputs`, `HiringGraphQLTypes` (Slice A); `domain/model/Application.scala`, `domain/policy/ApplicationLifecycle.scala`, `MongoHiringPersistenceCodecs`/`MongoHiringCodecs` (Slice B) | `sbt test`; `sbt 'IntegrationTest / test'`; envelope contract test | Slice A done (typed IDs, `actorRole`, `ApplicationEvent` without `actorId`): AC-08.1-.4 in `HiringGraphQLAccessSpec`; Slice B persistence evidence as before; `sbt test` 1387 pass, 0 fail; `sbt 'IntegrationTest / test'` 316 total, 290 pass, 25 skipped, 1 timing-benchmark failure (`MongoInterviewCleanupAccessIntegrationSpec` write p95 ratio, unrelated; passed on rerun); scalafmtCheckAll Compile/Test/IntegrationTest green |
| AC-09 | AC-09.1-.3 | `domain/model/Job.scala`, `repository/mongo` codec, discovery/facets, search text, job resolvers | `sbt test`; `sbt 'IntegrationTest / test'` | Slice B (domain/persistence: actorRole, city) done: `sbt test` 1312 pass; `IntegrationTest / test` 290 pass, 25 skipped, 0 fail; tests `MongoJobLocationAndEventActorRoleSpec`, AC-09.3/.2 in JobDiscoverySpec/DomainValidationSpec; API parts pending Slice A; AC-09.5 explain (MongoJobDiscoveryIntegrationSpec, 12 seeded jobs: 5 Nicosia, 4 Limassol, 3 city-less, limit 3): LIMIT > FETCH > IXSCAN `jobs_open_city_created_id`, no SORT; keysExamined=3, docsExamined=3, nReturned=3; AC-08.3 negative decode tests (unknown actorRole -> InvalidField, unknown initiatedByRole -> InvalidStoredData) in MongoJobLocationAndEventActorRoleSpec; Slice A renames (`job`, `eventId`): AC-09.1 in `HiringGraphQLAccessSpec`; `sbt test` 1387 pass, 0 fail; `sbt 'IntegrationTest / test'` 316 total, 290 pass, 25 skipped, 1 timing-benchmark failure (`MongoInterviewCleanupAccessIntegrationSpec` write p95 ratio, unrelated; passed on rerun); scalafmtCheckAll Compile/Test/IntegrationTest green |
| AC-10 | AC-10.1-.2 | `GraphQLFailureCatalog.classifyValidation` | `sbt test` | Slice A done: `ValidationError.field` is the GraphQL input path per operation (`InputFieldPaths`); AC-10.1-.3 in `HiringGraphQLAccessSpec`, `HiringGraphQLResolverSupportSpec` (domain field), message still joins domain names; `sbt test` 1387 pass, 0 fail; `sbt 'IntegrationTest / test'` 316 total, 290 pass, 25 skipped, 1 timing-benchmark failure (`MongoInterviewCleanupAccessIntegrationSpec` write p95 ratio, unrelated; passed on rerun); scalafmtCheckAll Compile/Test/IntegrationTest green |
| AC-11 | AC-11.1-.2 | `GraphQLFailureCatalog`, `RequestContext` | `sbt test` | Slice A done: AC-11.1/.2 `InterviewActionRateLimiterSpec`, `HiringGraphQLInterviewActionSpec`, `HiringApiRoutesSpec`; limiter now reports the remaining window (also changes auth and cost-budget Retry-After from full window to remaining window) |
| AC-12 | AC-12.1-.5 | `config/*`, `api/http/GraphQLHttpRoutes`, new `PersistedQueryAllowlist`, test-scope manifest generator, manifest file | `sbt test`; manifest regeneration test | Slice C integrated (coordinator patch); manifest regenerated with `PersistedQueryManifestCapture`, drift test green in `sbt test` 1387 pass, 0 fail; `sbt 'IntegrationTest / test'` 316 total, 290 pass, 25 skipped, 1 timing-benchmark failure (`MongoInterviewCleanupAccessIntegrationSpec` write p95 ratio, unrelated; passed on rerun); scalafmtCheckAll Compile/Test/IntegrationTest green |
| AC-13 | AC-13.1 | `HiringGraphQLSchemaAssembly`, `HiringGraphQLTypes`, `HiringGraphQLInputs`, fixtures | `sbt test` | Slice A done: AC-13.1 `HiringGraphQLContractSpec`, `HiringApiRoutesSpec`; dead `RequestContext.readiness`/probe parameter and the per-request `service.readiness` removed; `sbt test` 1387 pass, 0 fail; `sbt 'IntegrationTest / test'` 316 total, 290 pass, 25 skipped, 1 timing-benchmark failure (`MongoInterviewCleanupAccessIntegrationSpec` write p95 ratio, unrelated; passed on rerun); scalafmtCheckAll Compile/Test/IntegrationTest green |

Global evidence: SDL regenerated with `sbt "Test/runMain com.example.graphQL.cats.api.graphql.HiringGraphQLContractCapture"` and the diff reviewed; `sbt test`; `sbt 'IntegrationTest / test'` (replica-set Mongo; unavailable means unverified); `scripts/check-local.sh`. Every operation fixture and `docs/api.md` example must still validate against the new SDL.

## Additional criteria: bounded batches and per-request validation

Both belong to Slice A (`api/graphql/**`, reviewers as Slice A; Security Engineer reviews AC-14 authorization). Single active shape; no schema change.

- AC-14 Bounded relationship batches: every Sangria `Fetcher` (`HiringGraphQLFetchers.scala`: `usersFetcher`, `jobsFetcher`, `emailVisibilityFetcher`, built via `HiringGraphQLDsl.resultFetcher`) is configured with `FetcherConfig.maxBatchSize(MaxFetchBatchSize)`. `MaxFetchBatchSize` is one named constant, 100, aligned with the maximum page size (if the page-size constant is reachable, derive from it; otherwise assert equality in a test). Larger id sets are split into batches; each batch calls the repository once with its own ids and the same actor/authorization predicate; results and ordering seen by resolvers are unchanged. No unbounded id list reaches MongoDB from one request.
- AC-15 Per-request validation of cached documents: `GraphQLDocumentCache` keeps caching parsed `Document`s (parse cost avoided) but validation is never cached; the execution path always runs `QueryValidator.default` (including variable-dependent rules such as oneOf exactly-one-field, unique operation names, variable types) on every request. Cost is one validation pass per request, measured only if a performance claim is later made.

| Tag | Scenario |
|---|---|
| AC-14.1 typical | Nested `application.job` for 3 applications issues one `jobsFetcher` repository call with 3 ids |
| AC-14.2 boundary | 100 ids make one repository call; 101 ids make two (100 + 1), results returned in request order |
| AC-14.3 forbidden | A Candidate resolving jobs/users across two batches still gets only authorized rows in each batch; `emailVisibilityFetcher` hides emails in every batch |
| AC-14.4 failure | A repository failure in the second batch surfaces as a GraphQL error for that field, not a partial silent drop |
| AC-15.1 typical | The same valid document run twice executes both times and the second run does not re-parse (cache hit counter or parse-call spy) |
| AC-15.2 forbidden | A document cached on a valid first request, then sent with an invalid oneOf variable (`{candidate:..., recruiter:...}`), returns a validation error on the second request |
| AC-15.3 boundary | A cached document that is schema-valid but missing a required variable fails validation per request |

| ID | Implementation and test paths | Verification | Actual outcome |
|---|---|---|---|
| AC-14 (AC-14.1-.4) | `HiringGraphQLFetchers.scala`, `HiringGraphQLDsl.scala`; new `HiringGraphQLFetcherBatchingSpec` | `sbt test` | Done: `FetcherMaxBatchSize = PageSize.Max`; AC-14.1/.3/.4 with the real fetchers in `HiringGraphQLRelationBatchingSpec` (3 applications -> one jobs call; 120 users -> two batches of <=100, recruiter emails hidden in all, Admin sees all; second batch failure -> UNAVAILABLE errors, first batch resolved); AC-14.2 in `HiringGraphQLFetcherBatchSpec`; `sbt test` 1387 pass, 0 fail; `sbt 'IntegrationTest / test'` 316 total, 290 pass, 25 skipped, 1 timing-benchmark failure (`MongoInterviewCleanupAccessIntegrationSpec` write p95 ratio, unrelated; passed on rerun); scalafmtCheckAll Compile/Test/IntegrationTest green |
| AC-15 (AC-15.1-.3) | `GraphQLDocumentCache.scala`, `GraphQLDocumentCacheSpec`, execution path in `HiringGraphQLSangriaAdapter.scala` | `sbt test` | Done: cached documents are validated again (`GraphQLDocument` no longer carries a validator); AC-15.1 `GraphQLDocumentCacheSpec` (cached parse reused), AC-15.2/.3 `HiringGraphQLAccessSpec`; resolver-level oneOf check tested with validation bypassed (AC-05.2); `sbt test` 1387 pass, 0 fail; `sbt 'IntegrationTest / test'` 316 total, 290 pass, 25 skipped, 1 timing-benchmark failure (`MongoInterviewCleanupAccessIntegrationSpec` write p95 ratio, unrelated; passed on rerun); scalafmtCheckAll Compile/Test/IntegrationTest green |

## Slice A findings

- Sangria 4.2.19 supports `@oneOf`: `InputObjectType.astDirectives` with `oneOf` enables rule `ExactlyOneOfFieldGiven` (query validation only, literal objects and a variable typed directly as the oneOf input, not nested inside another input variable) and schema validation (fields must be optional, no defaults). `SchemaRenderer` renders `input ProfileInput @oneOf`; introspection has no `isOneOf`, so the rule is also stated in the description. The resolver re-checks it because validation can be bypassed.
- AC-14 `FetcherConfig.maxBatchSize(100)` on all fetchers (`HiringGraphQLFetcherBatchSpec`); AC-15 `GraphQLDocumentCache` returns `QueryValidator.default` for cached documents too (`GraphQLDocumentCacheSpec`, `HiringGraphQLAccessSpec` AC-15.1).

## Performance and cost

Not applicable as a measured claim. Note: AC-12 adds one SHA-256 and a set lookup per request (negligible; the document cache is untouched). AC-09 must keep city filters/facets index-served; Data Engineer confirms index use with an explain on the city filter.

## Implementation handoff

Order: B lands the domain field before A's `actorRole` type is wired (A may stub until B merges); A and D start together; B and C are independent of A except for shared SDL regeneration, so serialize the final snapshot regeneration (A last or rebase). 

| Slice | AC | Owner and write paths | Reviewers |
|---|---|---|---|
| A API-layer types | 01-08, 10, 11, 13 | Scala Developer: `api/graphql/**`, `src/test/resources/graphql/**`, `src/test/scala/**/api/graphql/**` | Code Reviewer; Security Engineer (AC-07, AC-08 actor visibility, error mapping); QA |
| B Location.city and event actor role | 09 (city part; renames and `eventId` payload go in A) and 08 (persisted `actorRole`) | Data Engineer + Scala Developer: `domain/model/Job.scala`, `domain/model/Application.scala`, `domain/policy/ApplicationLifecycle.scala`, service callers that pass the actor, `repository/mongo/**` job and event codecs/discovery, `service/search/**` text and filters, related tests and `src/it` | Data Engineer, Code Reviewer, QA |
| C Persisted-query allowlist | 12 | Scala Developer: `config/**`, `api/http/GraphQLHttpRoutes.scala`, new allowlist class and test-scope generator, `application.conf` | Security Engineer (mandatory), Code Reviewer, QA |
| D Docs | all | `docs/api.md`, `README.md`, `docs/use-cases.md` only where affected, this spec's evidence | Code Reviewer |

Note for Slice A/B split: AC-09 renames (`job`, `eventId`) are API-layer and belong to Slice A; Slice B owns only city.

Risks: SDL churn is large (single snapshot conflict between slices); oneOf support in Sangria rendering (see Assumptions); embedding hash churn from city change (D8). Recovery: slices are independent commits; revert the slice and regenerate SDL.

## Checkpoint and review

- Completed: Slices A (API layer incl. AC-14, AC-15), B (city, event actor role, interview initiator role) and C (persisted-query allowlist) implemented and integrated; Slice D docs and wiki updated; review findings F1-F8 fixed plus two re-review follow-ups (single `@oneOf` declaration assertion, resolver comment).
- Final QA run (`scripts/run-local-tests.sh test`): `sbt test` 1387 pass; IntegrationTest 316 total, 301 pass, 14 skipped (9 Atlas live gates without `ATLAS_TEST_URI`, 5 `hiring.scaling.measure` opt-ins), 1 failed: `MongoInterviewCleanupAccessIntegrationSpec` write-p95 benchmark under concurrent load, passed when rerun alone (classified flaky timing). Skipped counts depend on the environment (an earlier author run without Kafka/compose gates skipped 25). scalafmtCheckAll and `scripts/check-skills.py` green. AC-09.5 plan: IXSCAN `jobs_open_city_created_id`, no SORT, keysExamined=3 docsExamined=3 nReturned=3.
- Not verified: Atlas live gates, scaling measurement opt-ins, analytics build and merged JaCoCo coverage (`scripts/check-local.sh` not run end to end), a single full IT run with zero failures.
- Next: merge into `refactor` as requested by the user.
- Code Reviewer verdict: PASS (re-review of F1-F8; two low follow-ups applied)
- Security Engineer verdict: PASS (static) for the full change; Slice C PASS after fixes
- Data Engineer verdict (Slice B): PASS (static); plan evidence recorded by AC-09.5
- Final independent QA verdict: PASS with the timing-benchmark caveat above
