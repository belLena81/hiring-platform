# Repository Error Observability

## Current state — 2026-10-05

The diagnostics/error classification change remains implemented. Subsequent [repository composition](hiring-repository-composition.md) moved repository ports and internal helpers to `RepositoryIO`; the raw `IO[Either]` port descriptions below describe this earlier task baseline, not the active API. Unwrapping now belongs at driver/effect-control boundaries, as described in [engineering quality](../engineering-quality.md).

The remaining task evidence is retained as a historical record unless explicitly identified as a current source fact. Roadmap sequencing follows [development milestones](../development-milestones.md).

## Task and outcome

- Status: complete
- User outcome: Mongo repository failures retain safe diagnostic evidence and precise internal classification while existing service and GraphQL behavior remains stable.
- Scope: repository error protocol, Mongo repository adapters/support/runtime wiring, repository-facing test doubles and callers, focused diagnostics/error tests, and this specification.
- Non-goals: Mongo schema/index or migration changes, GraphQL wire changes, service/domain redesign, and changes to the pre-existing analytics worktree edit.

## Original source baseline and decisions

- Repository ports currently use `IO[Either[RepositoryError, A]]`; services compose these through `UseCaseIO`.
- Mongo adapters map many thrown failures directly to `Unavailable`, and stored-document decoding also loses its specific failure category.
- `Diagnostics.emit` is best-effort and `LogFields.failure` emits allowlisted exception type and source-location metadata without the exception message.
- Use `RepositoryIO[A] = EitherT[IO, RepositoryError, A]` in the shared Mongo exception guard and unwrap with `.value` at existing repository port signatures. A broader port migration would also change transaction, mutation receipt, service composition, and test helper effect sequencing; that structural change is outside this narrow error-boundary fix.
- `Unavailable` covers infrastructure/unexpected driver failures; codec failures use `InvalidStoredData`; missing write publisher results use `MissingWriteResult`. Existing public GraphQL failures remain sanitized and unchanged.

## Acceptance and evidence

| ID | Given / When / Then | Evidence |
|---|---|---|
| REO-01 | A Mongo operation fails exceptionally; a shared support guard emits a repository diagnostic with safe failure metadata before mapping it, and sink failure does not change the repository result. | Focused guard and diagnostics tests |
| REO-02 | A stored document cannot decode or a write publisher emits no result; the repository returns `InvalidStoredData` or `MissingWriteResult`. Valid absent reads remain `Right(None)` and stale/duplicate conflicts retain their current classifications. | Focused repository unit tests and Mongo integration tests where available |
| REO-03 | Mongo exception mapping uses `RepositoryIO` internally and unwraps only at existing repository ports; service code retains its current composition and GraphQL codes/messages/retry behavior. | Compile, focused service/GraphQL tests, full unit suite |
| REO-04 | The production Mongo runtime passes its configured `Diagnostics` to repository adapters; no repository failure path discards a throwable without passing through the guard. | Runtime wiring review, source search, runtime/diagnostics tests |

## Verification checkpoint

- Implemented: the shared diagnostics-aware Mongo guard reports safe failure metadata before mapping; all operational Mongo adapters use it for exceptional repository failures; transaction commit, operation, and abort cleanup failures are reported; stored-data decoding and missing write results have precise internal errors; production diagnostics are threaded through runtime wiring.
- Scope decision: `RepositoryIO` wraps the Mongo guard internally and is unwrapped at the existing `IO[Either]` repository ports. The port and service composition migration remains out of scope for this narrow error-boundary fix.
- Error semantics preserved: GraphQL failures remain sanitized; retry and conflict mapping remains unchanged; absent reads remain `Right(None)`.
- Verification: `sbt compile` passed; `sbt test` passed 399/399; `sbt 'IntegrationTest / test'` passed 37/37; focused `MongoTransactionRetrySpec` passed 10/10 and `MongoHiringRepositoryTransactionIntegrationSpec` passed 6/6; `git diff --check` passed.
- Reviews: independent Code Reviewer PASS, Security Engineer PASS, QA REO-04 PASS. The full QA gate passed after the Mongo integration suite completed.
- Formatting: changed Scala files were formatted. Aggregate `scalafmtCheckAll` reports unrelated pre-existing formatting in `MongoHiringSetup.scala` and `JwtActorAuthenticatorSpec.scala`; those untouched files were left unchanged.
- Worktree: unrelated analytics edits were preserved.
- Preserve unrelated worktree changes. No schema migration or compatibility mechanism is needed because the Mongo document and GraphQL wire shapes do not change.
