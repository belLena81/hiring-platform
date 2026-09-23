# Hiring Service Composition

## Identity and scope

- Status: complete
- Coordinator: Product Manager
- Implementation owner: Scala Developer
- User outcome: hiring behavior remains unchanged while Mongo adapters, runtime capability wiring, and mutation replay ownership follow the repository and service boundaries.
- Authorized scope: Mongo repository file separation, shared Mongo session helpers, embedding capability assembly, service-owned idempotency, `UseCaseIO`, affected tests, and canonical architecture documentation.
- Non-goals: GraphQL or stored contract changes, Mongo indexes or migrations, dependency changes, `ConfigError` renaming, tagless-final services, deployment, or unrelated analytics work.
- Dependencies: Scala 3.9 on Java 17+, Cats Effect, Sangria, Mongo reactive streams, MUnit, and replica-set MongoDB for transaction evidence.

## Decisions and invariants

- Concrete Mongo repository class names, packages, protocols, query shapes, CAS predicates, error mappings, and transaction ownership remain unchanged; only source ownership and proven session helpers move.
- `EmbeddingCapability` is a private runtime ADT. Its smart constructor does not invoke provider or adapter factories when disabled. When enabled it validates the API key before acquisition, owns a plain semantic repository, shares one durable embedding-work repository between transactional writes and the worker pipeline, and resource-manages provider/pipeline release on success, failure, and cancellation.
- Account, hiring-read, job, application, search, interaction, and analytics ports, implementations, and test doubles return `UseCaseIO[A] = EitherT[IO, UseCaseError, A]`. Repository ports and `UserAuthenticator` retain their existing `IO[Either[RepositoryError, A]]` contracts.
- GraphQL translates an idempotency UUID and the existing canonical fingerprint input into an opaque `IdempotencyRequest`. Services own receipt execution, actor scope, entity references, replay authorization, and the private `MutationWriteContext` callback.
- Existing receipt operation names, scope normalization, entity references, seven-day expiry, replay behavior, and failure mapping remain compatibility invariants. Fingerprint serialization uses the canonical JSON contract below.
- Business timestamps and generated entity/event IDs are evaluated inside the first-write callback. Receipt replay never generates write IDs and never repeats a domain write, event, outbox insert, erasure request, or embedding-work insert.
- Typed rejection removes the provisional receipt and remains retryable. Repository failure rolls back the receipt and every participating write. Fingerprint mismatch remains `RepositoryError.Conflict`; an in-progress receipt remains `RepositoryError.Unavailable`; every first-write repository call receives the exact context supplied by the receipt repository.
- Public GraphQL SDL, response payloads, error codes/messages, HTTP status behavior, Mongo documents/indexes, events, configuration, and dependencies do not change.

## Mutation compatibility matrix

`input-json` means compact JSON encoded by Circe from the full GraphQL mutation input, with object keys sorted recursively before hashing. Arrays retain input order and absent optional fields encode as `null`. Status actions use a JSON object containing the application ID, uppercase status, feedback, and reason, with the same canonical printer.

The fingerprint format changes immediately. Existing receipts contain only the previous fingerprint hash, so replays using those keys can return a fingerprint conflict until MongoDB's TTL process removes the expired receipts. No legacy fingerprint fallback or receipt rewrite is performed.

| Operation | Scope | Fingerprint source | Entity reference | Replay |
|---|---|---|---|---|
| `signUp`, `bootstrapAdmin`, `login` | `public:` plus `name.trim.toLowerCase(Locale.ROOT)` | input-json | `user` / stored user ID | Load the current active user and issue a fresh token; login replay does not recheck the password |
| `updateMyProfile` | authenticated user ID | input-json | `user` / updated user ID | Re-run current `me` authorization/read only |
| `deleteMyAccount` | authenticated user ID | input-json | `user` / actor user ID | Return unit and invalidate the request viewer; do not repeat deletion or erasure enqueue |
| `createJob`, `updateJob`, `publishJob`, `closeJob` | authenticated user ID | input-json | `job` / job ID | Re-run current `viewJob` authorization/read only |
| `submitApplication` | authenticated user ID | input-json | `application` / application ID | Re-run application visibility authorization, then load the current application |
| `acceptApplication`, `moveApplicationToInterview`, `hireApplication`, `rejectApplication`, `declineApplication` | authenticated user ID | status-action bytes | `application` / application ID | Re-run application visibility authorization, then load the current application |
| `recordJobView`, `recordSearchResultClick` | authenticated user ID | input-json | `interaction` / supplied event ID | Return unit; do not repeat interaction persistence |

## Acceptance and evidence

| ID | Given / When / Then | Verification | Actual outcome |
|---|---|---|---|
| HSC-01 | Mongo hiring adapters are inspected | Each user, job, semantic-search, and application adapter has one source file; transaction support is separate; semantic-result and repository-specific logic stay with their owners; only genuinely shared helpers remain package-private support | Passed: one adapter per source file, separate transaction/support files, production compile and Mongo integration tests green |
| HSC-02 | Vector search is disabled, enabled, fails acquisition, or is cancelled | Disabled wiring invokes no provider/search/work factory; enabled wiring validates first, shares one work repository, and finalizes provider/pipeline while the plain semantic adapter needs no finalizer | Passed: runtime capability tests cover disabled/enabled, validation, acquisition failure, downstream failure, and cancellation |
| HSC-03 | Any mutation is first executed, rejected, replayed, mismatched, in progress, or fails in storage | The owning service preserves the compatibility matrix, exact context, rollback/removal behavior, error mapping, and no-second-write invariant; GraphQL sees neither receipt repository nor write context | Passed: `IdempotentSpec`, service tests, GraphQL tests, and three Mongo transaction integration tests |
| HSC-04 | Any account, hiring-read, job, application, search, interaction, or analytics use case returns an expected failure or value | Its port exposes `UseCaseIO`; GraphQL unwraps it once and preserves the exhaustive typed-payload versus exceptional-error classification; repository/authenticator contracts remain unchanged | Passed: production/test compilation under `-Werror` and full unit suite |
| HSC-05 | Existing tests and disposable Mongo checks run | Unit, GraphQL, runtime, repository, transaction, and full integration checks preserve current observable behavior | Partial: 342 unit tests and 26/27 integration tests passed; `MainProcessSpec` independently reproduces one unrelated repeated-runtime-diagnostic assertion failure |

## Implementation handoff

- Mongo adapter owner: split `MongoHiringRepositories.scala`, centralize only proven session/decoding/event helpers, and preserve repository-owned plus receipt-owned transaction behavior.
- Runtime owner: add `EmbeddingCapability`, share embedding work, flatten resource assembly, and inject receipt-backed idempotency into mutation services.
- Service/API owner: add `UseCaseIO`, opaque idempotency input and executor; update all use-case ports, implementations, resolvers, request context, and test adapters without changing wire contracts.
- Test owner: move receipt orchestration coverage to service tests; add account/application receipt-owned rollback evidence beside the existing job replica-set case; retain exhaustive GraphQL error mapping tests.
- Code Reviewer and Security Engineer inspect the integrated state independently; QA runs final acceptance after required fixes.

## Checkpoint

- Completed criteria: HSC-01 through HSC-05. Independent QA accepted the scoped evidence; one unrelated repository-wide process-lifecycle assertion remains red.
- Current evidence: Java 17; `Test / compile` passed under `-Werror`; `sbt test` passed 342/342; `IntegrationTest / compile` passed; Mongo receipt transaction integration passed 3/3, including an `Idempotent`-driven repository-failure rollback; full integration passed 26/27. The failing `MainProcessSpec` assertion expected one `RUNTIME_FAILED` event and observed 17 both in the full run and isolated rerun; its other 9 tests passed. Independent Code Reviewer and Security Engineer verdicts are PASS after transaction-channel and replay-authorization fixes.
- `ConfigError` keys remain explicit by design in this slice: deriving externally observed configuration keys from Scala product names would create a public diagnostic-contract change and requires its own compatibility decision.
- Review gates: Code Reviewer PASS, Security Engineer PASS, QA Engineer PASS.
- Follow-up debt: migrate credential-bearing receipt fingerprints from unkeyed SHA-256 to a server-keyed construction under a separately authorized compatibility plan; investigate the pre-existing repeated `RUNTIME_FAILED` process diagnostic.
