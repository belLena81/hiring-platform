# Local engineering quality

This project uses local verification and independent agent review. There is no CI/CD pipeline. Neither a commit nor an agent's summary substitutes for running the applicable checks.

## Pure functional core

- Domain functions operate on immutable values and return values or typed errors. Validation and lifecycle transitions must not read clocks, generate IDs, log, access configuration, or perform I/O. Pass time/identity and other required inputs explicitly.
- Represent legitimate absence with `Option`, expected rejection with `Either` or an explicit error ADT, and side effects with Cats Effect. Do not use `null`, sentinel strings, thrown business exceptions, partial pattern matches, or unchecked `.get` to model ordinary outcomes. Preserve existing API absence semantics rather than silently changing them.
- Services sequence effects and enforce authorization/atomic use cases. Infrastructure adapters perform database/network work and translate boundary failures. A function returning `IO` is not a pure business function merely because effects are wrapped.
- Use `Resource` for ownership and cancellation-safe cleanup, injected/effectful clock and ID generation at service boundaries, and `Ref`/other appropriate effect primitives for shared state. Avoid mutable global state, detached fibers, `Await`, and unsafe runners inside the application. When framework interop requires execution, isolate it at one documented runtime boundary.
- Use the least powerful abstraction that supports the actual operation. Prefer concrete domain functions; use `F[_]` constraints where they express a useful boundary. Do not build a generic framework, free algebra, or transformer stack solely to appear functional.
- Optimize through algorithms, batching, indexes, bounded concurrency, and allocation measurement. Encapsulated mutation inside a necessary third-party adapter must not leak mutable state into the domain. Any deliberate departure from these rules needs a concrete rationale and review, not a claim of speed without evidence.

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
| `.local/logs/` | Runtime logs and local diagnostic output |
| `.local/backups/` | Database dumps and migration recovery copies |

The local environment may use ignored `.env`/`.env.*` files where tooling supports them; sanitized `.env.example` and `.env.<name>.example` files remain shareable. Keep local Codex preferences at the ignored `.codex/config.toml`; shared agent rules and skills remain versioned. IDE `.idea/` directories are ignored at every depth.

Commit safe defaults/examples, migration scripts, schema definitions, and deliberately small synthetic test fixtures in their normal source directories. Do not put shared configuration inside `.local/`. Never store real credentials or personal data in example files, and never force-add ignored local data. These are storage conventions, not implemented configuration loading: set up explicit loading/mounts in the relevant application or infrastructure slice. Ignore rules do not protect files already tracked by Git.

## Local checks and readiness

Run from the project root:

```bash
bash scripts/check-local.sh
```

The command uses Python 3.9+ to validate project-local skills, then runs the current `sbt test` suite, including compilation required by sbt. It is an explicit local command, not a hook, CI job, deploy command, schema migration, or complete readiness certification. It runs without requiring a Git repository or staged files or any global skill installation.

| Change | Additional evidence before completion |
|---|---|
| Domain/service behavior | Focused positive and negative tests; cancellation/resource tests where relevant |
| DB or resolver behavior | Real-DB and GraphQL integration checks; add the harness in that slice if absent |
| Stored data or wire contract | [Migration and compatibility checks](schema-evolution.md) |
| Performance | Same reproducible before/after workload, query plans/counts, latency/resource/cost comparison |
| Docs/skills | Local references, skill validation, and independent scenario review |

Formatting/linting and integration tasks are not currently configured. When a slice introduces them, update this guide and the local command with real supported tasks. Until then, report those limitations; do not claim nonexistent checks passed. Missing integration/migration/compatibility tests cannot be deferred to a future CI pipeline.

## Review before and after coding

Before substantial implementation, relevant specialists check the ready spec for missing flows, unnecessary complexity, pure/effect boundaries, access patterns, migration/compatibility impact, and testability. Reuse the existing Architect, Data Engineer, Big Data Engineer, Security Engineer, Code Reviewer, and QA roles rather than spawning duplicate reviewer personas.

After coding, review correctness, completeness, security, simplicity, and measured performance. Verify feedback against current source; fix supported blockers or record a source-based explanation for disagreement and obtain the relevant reviewer's updated verdict. Optional ideas become explicit follow-up scope, not silent expansion. The coordinator cannot override a failed gate. End with independent QA of the final state.

## Reused template ideas

Adapted from the supplied `.claude` workflow, `review-spec`, testing guide, performance/completeness reviewers, and review-feedback/systematic-debugging patterns. The generic template's exception-first style is replaced with typed FP errors; ownership checks follow this project's Candidate/Recruiter/Admin model rather than importing an unrelated tenancy model. No Claude-specific slash commands, automatic commits/pushes, worktree hooks, CI watchers, or placeholder passing gates are installed.
