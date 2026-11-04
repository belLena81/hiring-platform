---
paths:
  - "src/**/*.scala"
  - "analytics/src/**/*.scala"
  - "test-support/**/*.scala"
  - "build.sbt"
  - "analytics/build.sbt"
---

# Engineering and GraphQL

- Separate domain, application, API, and infrastructure; dependencies point inward. Domain logic stays free of GraphQL/DB/HTTP/JSON/auth/AI SDK dependencies.
- Prefer typed IDs, ADTs/enums, explicit validation, and `Either`/typed effect errors. Use UTC `Instant`. Keep GraphQL inputs separate from domain and persistence models separate when needed.
- Keep domain functions pure and immutable: pass time/IDs as values, express absence with `Option`, and use typed failures rather than business exceptions, nulls, or partial operations. Services sequence effects; adapters isolate I/O and any necessary framework interop. See the quality guide for testing and effect-boundary rules.
- Model closed domain states and expected business outcomes with explicit ADTs. Use `Validated` when independent input/configuration errors should accumulate, and `Either` when the caller must sequence a decision. Keep thrown exceptions for unexpected or framework-boundary failures and translate them once at the owning boundary.
- Use Cats Effect `Resource` for clients/pools/servers, cancellation-safe ownership, bounded concurrency, and FS2 backpressure. Keep blocking work off compute threads and unsafe execution out of business logic.
- Use configured libraries idiomatically before adding a dependency or hand-implementing parsing, validation, rendering, scheduling, protocol, or collection behavior. Add a dependency only for a concrete capability gap; check compatibility, maintenance/provenance, and operational cost, and isolate it at the appropriate boundary.
- Use design patterns for concrete problems; avoid speculative frameworks, microservices, event sourcing, caches, or denormalization.
- Mutations use input types and meaningful typed payloads rather than raw booleans. Preserve action-oriented lifecycle operations and sanitized meaningful errors.
- Batch nested relationships per request with authorization-safe caches. Bound pagination, depth, complexity, request size, and execution resources.