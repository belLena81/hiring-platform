# Service Port Dependency Direction

## Current state — 2026-10-05

Operational ports now live in `service.port`; `RepositoryError` and reporting results remain service-owned, pagination lives in `domain.pagination`, and events/search contracts live in service packages. `service.port.DatabaseProbe` is the port; `service.DatabaseProbe` contains its result model. Later [repository composition](hiring-repository-composition.md) records passing compilation/unit/integration evidence after the historical Mongo migration blockers below. Independent task-specific review was not recorded here and is not inferred from later work.

The remaining task evidence is retained as a historical record unless explicitly identified as a current source fact. Roadmap sequencing follows [development milestones](../development-milestones.md).

## Identity and scope

- Status: in progress
- User outcome: application and API packages consume inward-owned contracts, while persistence and external adapters implement those contracts.
- Authorized scope: relocate operational ports and their error/result contracts, move domain-dependent event/pagination/search contracts out of `shared`, update references and canonical architecture docs, and verify compilation/tests.
- Non-goals: behavior changes, GraphQL or persisted schema changes, Mongo migrations/indexes, dependency changes, or changes to runtime resource ownership.

## Original source baseline and decisions

- Operational contracts are declared in `repository.protocol` and imported by service, API, runtime, infrastructure, and Mongo adapter code.
- `DatabaseProbe` is currently declared in `service`; `MongoDatabaseProbe` implements it from the Mongo adapter package.
- `shared.events`, `shared.pagination`, and `shared.search` contain contracts/algorithms coupled to domain and service concepts. Operational event JSON uses Circe, so these contracts belong in service rather than the pure domain.
- Move operational ports and `RepositoryIO` to `service.port`, with `RepositoryError` and report DTOs owned by `service`; keep `ProbeResult` as a service result and place the `DatabaseProbe` interface in `service.port`. Put events in `service.events`, pure pagination contracts in `domain.pagination` because `UserPageRequest` is a domain model, and search contracts/evaluation helpers in `service.search`.
- Keep the API dependent on service use cases and service result/error models only. Remove its unused direct search-session repository field.
- Keep all observable behavior and persisted/wire shapes unchanged. No compatibility aliases for the removed repository protocol namespace are required.

## Acceptance and evidence

| ID | Given / When / Then | Verification | Outcome |
|---|---|---|---|
| SPD-01 | Given API and service production code, when imports are inspected, then API depends on service protocols/models only and service has no dependency on `repository.*`; repository and infrastructure adapters implement `service.port`. | Production compile and import scan | Import scan passed: API has no `service.port` imports and service has no `repository.*` imports. Compile blocked by unrelated Mongo migration/type errors in the dirty checkout. |
| SPD-02 | Given event, pagination, and search contracts, when package declarations and all consumers are inspected, then they live under service packages and no old package references remain. | Source-reference scan and unit suite | Source scan passed: no old package references remain in main, test, or integration sources. |
| SPD-03 | Given current application behavior and contracts, when unit tests and integration-source compilation run, then behavior, GraphQL contracts, and stored shapes remain unchanged. | `sbt test`; `sbt "IntegrationTest / compile"` | Blocked before tests: `sbt test` failed in main compilation with 119 errors; integration-source compilation failed with 115. Failures are in the dirty Mongo4Cats/reactive-driver migration (mixed client/session APIs and related type errors); no unit or integration tests executed. |
| SPD-04 | Given the active architecture docs, when the refactor is complete, then diagrams and package descriptions show service-owned ports and adapters depending inward. | Review `ARCHITECTURE.md` and `README.md` against source | Passed by source/document review. |

## Checkpoint

- Package moves, import updates, API port decoupling, and canonical documentation are complete. Import scans found no `repository.protocol`, old `shared.events`/`pagination`/`search`, or API `service.port` references; `git diff --check` passed. The moved port declaration, Mongo probe adapter, and affected GraphQL access spec were formatted. The full formatting check reports outstanding formatting differences across dirty Mongo adapters and test/integration sources. Both build gates are blocked by the Mongo migration compile failures; tests did not execute. Independent Code Reviewer and QA verdicts remain pending because delegation is unavailable.
