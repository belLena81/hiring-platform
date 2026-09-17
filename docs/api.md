# API reference

This is a backend-only GraphQL API. The GraphQL equivalent of an OpenAPI contract is its schema (SDL), complemented by introspection and executable operations. The server exports the schema directly; no UI or Node toolchain is included in this repository.

Default base URL: `http://127.0.0.1:8080`.

| Endpoint | Purpose |
|---|---|
| POST `/graphql` | Execute a JSON-encoded GraphQL operation, including introspection |
| GET `/schema.graphql` | Download the current SDL without querying MongoDB |
| GET `/health` | Application liveness, HTTP 200 with UP |
| GET `/ready` | MongoDB connectivity, HTTP 200 READY or 503 NOT_READY |

## Browse and track the contract

An external GraphQL API client can load `/graphql` through introspection or import the SDL from `/schema.graphql`. An HTTP-only client can execute the examples below. Do not use OpenAPI generation as a substitute for the GraphQL schema: GraphQL operations select their own fields and share one execution endpoint.

The checked schema snapshot is [hiring.graphql](../src/test/resources/graphql/hiring.graphql). Backend contract tests compare it with the live schema definition, verify the download endpoint matches, and execute [health](../src/test/resources/graphql/health.graphql), [readiness](../src/test/resources/graphql/readiness.graphql), and [standard introspection](../src/test/resources/graphql/introspection.graphql) fixtures. Update schema and consumer fixtures together when contracts evolve; a schema diff alone does not establish compatible behavior.

The schema now includes Phase 3 hiring operations and typed payloads for jobs, applications, status transitions, cursor connections, and application history. HTTP does not yet include an authentication source that can build trusted `ActorContext`; hiring operations therefore return sanitized unauthorized payloads through the served endpoint until a separate auth slice is implemented. Resolver tests inject trusted actors below the HTTP boundary to verify the GraphQL contract against Phase 2 services.

## Execute operations

```bash
curl -sS http://127.0.0.1:8080/schema.graphql
curl -sS http://127.0.0.1:8080/graphql \
  -H 'Content-Type: application/json' \
  --data '{"query":"query Foundation { health { status } readiness { status } }"}'
```

With MongoDB available:

```json
{"data":{"health":{"status":"UP"},"readiness":{"status":"READY"}}}
```

During a database outage the same executed GraphQL operation still returns HTTP 200, with `readiness.status` equal to `NOT_READY`. Liveness does not access MongoDB.

Named operations and variables:

```json
{
  "query": "query Readiness($include: Boolean!) { readiness @include(if: $include) { status } }",
  "operationName": "Readiness",
  "variables": {"include": true}
}
```

Send `Content-Type: application/json`; responses use JSON. An absent `Accept` header is supported. When supplied, the most-specific matching range determines JSON quality (`application/json`, then `application/*`, then `*/*`); explicit JSON quality zero excludes it even with a positive wildcard. Quality values must be 0 through 1 with at most three decimal places; malformed or duplicate quality parameters return 406. Equally specific matching ranges use their highest quality, and omitted quality defaults to 1.

`query` is required. Missing/null `variables` means an empty object, and missing/null `operationName` means no selected name. When the document contains several operations, provide an operation name. Supply required variables explicitly; the pinned Sangria version has a documented limitation with omitted required-variable defaults.

## Errors and limits

Errors use a sanitized `errors` array and a generated `X-Request-ID` response header. Invalid syntax/schema/variables/operation selection or query budgets produce 400; unsupported method/media produce 405/406/415; oversized body 413; overload 503; deadline 504; unexpected failures 500. Normal field errors and typed hiring payload errors use GraphQL's HTTP 200 response convention. Hiring payloads expose stable codes such as `UNAUTHORIZED`, `FORBIDDEN`, `NOT_FOUND`, `DUPLICATE_APPLICATION`, `INVALID_STATUS_TRANSITION`, and validation codes without raw driver messages or stack traces.

The [runbook](foundation.md#http-contract-and-budgets) specifies request, nesting, complexity, alias, concurrency, and deadline bounds. Introspection uses those same limits. Run `sbt test` for contract checks and `sbt 'IntegrationTest / test'` for real HTTP/database checks.
