# Local engineering quality

This project uses local verification and independent agent review. There is no CI/CD pipeline. Neither a commit nor an agent's summary substitutes for running the applicable checks.

## Pure functional core

- Domain functions operate on immutable values and return values or typed errors. Validation and lifecycle transitions must not read clocks, generate IDs, log, access configuration, or perform I/O. Pass time/identity and other required inputs explicitly.
- Represent legitimate absence with `Option`, expected rejection with `Either` or an explicit error ADT, and side effects with Cats Effect. Do not use `null`, sentinel strings, thrown business exceptions, partial pattern matches, or unchecked `.get` to model ordinary outcomes. Preserve existing API absence semantics rather than silently changing them.
- For analytics, actual business failures use the `AnalyticsError` throwable channel; orchestration outcomes such as deferred or released work use typed values. Field-level configuration and value parsing accumulate with `ValidatedNec` and convert to `Either`/IO at the owning boundary.
- Model closed states and expected business outcomes with explicit enums/ADTs. Use `Validated`/`ValidatedNec` when independent input errors should accumulate, then convert to `Either` when sequencing is needed. Do not throw or raise an effect error for an expected domain/repository result and immediately recover it with `attempt`; keep unexpected exceptions in the effect channel and translate them once at the responsible framework or infrastructure boundary.
- Services sequence effects and enforce authorization/atomic use cases. Infrastructure adapters perform database/network work and translate boundary failures. A function returning `IO` is not a pure business function merely because effects are wrapped.
- Use `Resource` for ownership and cancellation-safe cleanup, injected/effectful clock and ID generation at service boundaries, and `Ref`/other appropriate effect primitives for shared state. Avoid mutable global state, detached fibers, `Await`, and unsafe runners inside the application. When framework interop requires execution, isolate it at one documented runtime boundary.
- Use the least powerful abstraction that supports the actual operation. Prefer concrete domain functions; use `F[_]` constraints where they express a useful boundary. Do not build a generic framework, free algebra, or transformer stack solely to appear functional.
- The main Hiring application deliberately uses concrete Cats Effect `IO` in its service and repository ports. Use case ports expose `UseCaseIO[A] = EitherT[IO, UseCaseError, A]`; operational repository ports expose `RepositoryIO[A] = EitherT[IO, RepositoryError, A]`. This keeps composition aligned with the single `IOApp` runtime and avoids adding an unneeded effect abstraction. Tests retain seams through typed ports, fake adapters, and injected time/ID effects. This choice reduces portability to another effect runtime; revisit it if a second runtime becomes a real requirement or if a concrete testability need cannot be met through those seams.
- Keep expected repository and use-case errors in their transformer aliases across private helpers and transaction callbacks. Lift raw driver effects once; unwrap only where an external adapter or effect-control boundary must observe outcomes. Mongo exception guards and transaction/session retry control own their required `IO` crossings; background workers may interpret typed outcomes at their processing boundary. Use sequential `traverse_` for ordered writes that must stop on the first expected failure.
- The analytics runtime intentionally uses concrete Cats Effect `IO`; its tested ports and injected collaborators provide the needed seams without a tagless-final layer.
- Optimize through algorithms, batching, indexes, bounded concurrency, and allocation measurement. Encapsulated mutation inside a necessary third-party adapter must not leak mutable state into the domain. Any deliberate departure from these rules needs a concrete rationale and review, not a claim of speed without evidence.

## Typed configuration and dependency use

- Put packaged runtime defaults in HOCON and load through the normal Typesafe Config/PureConfig boundary. Use typed raw and validated configuration values; validate once at startup and return typed failures rather than letting malformed settings reach business code. Isolate blocking file/config parsing in the owning effect boundary where required.
- Use standard `config.file`/`config.resource` selectors and HOCON substitutions, including environment substitutions when needed. Production Scala code must not read environment variables directly or invent another source-merging/precedence layer without an explicit requirement. Test-only environment switches remain test-only.
- Prefer existing, configured dependency capabilities and their idiomatic APIs. Add a dependency only to address a concrete gap; consider Scala/JDK compatibility, maintenance and provenance, transitive surface, and operational cost. Keep integrations behind the appropriate adapter and avoid duplicating library behavior without measured or security-based justification.

## Authorization and query efficiency

- Use deny-by-default RBAC. Derive actor identity and roles from trusted authentication context; enforce roles and resource ownership in services, not only resolvers. Carry authorization predicates into writes/queries when required to prevent races between an authorization read and a data operation. Keep nested GraphQL loading and request caches authorization-safe.
- Preserve Candidate, Recruiter, and singleton Admin as the current role model. Community/tenant scoping is future work: before activating it, define trusted membership and ensure every affected service, repository query, and unique constraint carries the scope. Never treat a caller-provided community or tenant ID as proof of membership.
- Shape database queries around selective predicates, authorization filters, projection, deterministic order, and bounded pagination. Derive indexes from real access patterns and assess write/storage cost as well as read benefit. In application code, prevent N+1 access, repeated fetch/decode/hydration, unbounded materialization, and avoidable CPU/allocation work.
- For claimed query/performance improvements, capture representative explain/query-plan evidence and compare a reproducible workload before and after, including rows/documents examined versus returned, latency, and relevant resource/write costs. Do not add indexes, caches, or denormalization without a demonstrated use-case benefit.

## Develop and debug through observable behavior

For a behavioral bug, first reproduce the failure with a focused regression test or a documented observation. Trace the failing data through API → service → repository/external boundary, form a specific hypothesis, change one cause, and rerun the relevant check. If an attempt fails, use the new evidence to revise the hypothesis; avoid speculative batches of fixes.

For new domain behavior, use a focused red/green/refactor cycle when practical: see the test fail for the intended missing behavior, implement the general rule, then simplify with tests passing. A compilation/setup failure is not proof that a regression test detects the bug. Do not retrofit ceremony onto prose edits. Tests can be corrected when evidence shows they are wrong, with the changed expectation recorded and independently reviewed; never weaken a test merely to get green.

Exercise real pure functions and the actual effect runtime. Fake external ports for service tests; use a real disposable database for queries, constraints, transactions, and migration correctness. Control time and randomness; synchronize concurrency tests with explicit signals rather than sleeps. Verify results and durable effects, not only that a mock method was called. Test useful laws/invariants, such as rejected transitions leaving state unchanged and replay preserving deduplicated outcomes, where applicable.

## Local settings and data

Keep machine-specific files under the ignored root `.local/` directory:

| Path | Purpose |
|---|---|
| `.local/config/` | Application overrides and local service settings |
| `.local/data/` | Database bind mounts, generated datasets, caches, and analytical checkpoints |
| `_logs/` | Runtime logs and local diagnostic output at the project root |
| `.local/backups/` | Database dumps and migration recovery copies |

The local environment may use ignored `.env`/`.env.*` files where tooling supports them; sanitized `.env.example` and `.env.<name>.example` files remain shareable. Keep local Codex preferences at the ignored `.codex/config.toml`; shared agent rules and skills remain versioned. IDE `.idea/` directories are ignored at every depth.

Commit safe defaults/examples, migration scripts, schema definitions, and deliberately small synthetic test fixtures in their normal source directories. Do not put shared configuration inside `.local/`. Never store real credentials or personal data in example files, and never force-add ignored local data. These are storage conventions, not implemented configuration loading: set up explicit loading/mounts in the relevant application or infrastructure slice. Ignore rules do not protect files already tracked by Git.

## Local checks and readiness

Run from the project root:

```bash
bash scripts/check-local.sh
```

The command uses Python 3.9+ to validate project-local skills, checks Java 17+ (using `JAVA_HOME` when set), then runs the current MUnit `sbt test` suite, including compilation required by sbt. It is an explicit local command, not a hook, CI job, deploy command, schema migration, or complete readiness certification. It runs without requiring a Git repository or staged files or any global skill installation.

| Change | Additional evidence before completion |
|---|---|
| Domain/service behavior | Focused positive and negative tests; cancellation/resource tests where relevant |
| DB or resolver behavior | Real-DB and GraphQL integration checks; add the harness in that slice if absent |
| Stored data or wire contract | [Migration and compatibility checks](schema-evolution.md) |
| Performance | Same reproducible before/after workload, query plans/counts, latency/resource/cost comparison |
| Docs/skills | Local references, skill validation, and independent scenario review |

Scala compiler warnings are checked by the build. Scalafmt is configured with standard Scalafmt rules for the Scala 3.9 application and separate Scala 3.7.4 analytics build. At the repository root, run `sbt scalafmtAll scalafmtSbt` to format the application and its build definitions, then `cd analytics && sbt scalafmtAll scalafmtSbt` to format analytics. Check them with `sbt scalafmtCheckAll scalafmtSbtCheck` in each build. Foundation adds the explicit `sbt 'IntegrationTest / test'` task for live HTTP and disposable MongoDB checks; run it separately from the Docker-independent local unit command. GraphQL SDL and operation fixtures are checked in the unit suite. Transactions and migrations require disposable MongoDB tests; connectivity tests cannot certify those guarantees. Missing integration/migration/compatibility tests cannot be deferred to a future CI pipeline.

## Review before and after coding

Before substantial implementation, relevant specialists check the ready spec for missing flows, unnecessary complexity, pure/effect boundaries, access patterns, migration/compatibility impact, and testability. Reuse the existing Architect, Data Engineer, Big Data Engineer, Security Engineer, Code Reviewer, and QA roles rather than spawning duplicate reviewer personas.

After coding, review correctness, completeness, security, simplicity, and measured performance. Verify feedback against current source; fix supported blockers or record a source-based explanation for disagreement and obtain the relevant reviewer's updated verdict. Optional ideas become explicit follow-up scope, not silent expansion. The coordinator cannot override a failed gate. End with independent QA of the final state.

## Reused template ideas

Adapted from the supplied `.claude` workflow, `review-spec`, testing guide, performance/completeness reviewers, and review-feedback/systematic-debugging patterns. The generic template's exception-first style is replaced with typed FP errors; ownership checks follow this project's Candidate/Recruiter/Admin model rather than importing an unrelated tenancy model. No Claude-specific slash commands, automatic commits/pushes, worktree hooks, CI watchers, or placeholder passing gates are installed.
