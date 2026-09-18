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

The schema includes Hiring GraphQL operations and typed payloads for jobs, applications, status transitions, cursor connections, application history, and account lifecycle. `User.profile` is the nullable `UserProfile` union of `CandidateProfile` and `RecruiterProfile`; it is null for the singleton Admin and profile-less deleted accounts, while active Candidates and Recruiters expose only their matching union member. Clients select role-specific fields with inline fragments. `bootstrapAdmin` creates the first singleton Admin and cannot recreate one later; `signUp` then accepts only Candidates and Recruiters with exactly one matching profile, while Admin signup returns `ADMIN_BOOTSTRAP_ONLY`. `login` issues short-lived HS256 bearer tokens when `AUTH_JWT_HS256_SECRET` is configured. `me`, `updateMyProfile`, and `deleteMyAccount` derive identity from the token subject. Admin profile updates return `PROFILE_UNSUPPORTED_FOR_ROLE`; malformed Candidate or Recruiter profile variants return `PROFILE_ROLE_MISMATCH`. The token `sub` must match a currently Active stored user, and authorization derives the actor role from that stored user rather than from role claims. Missing, invalid, expired, wrong issuer/audience, deleted-user, or unknown-user tokens return sanitized unauthorized payloads. Passwords are stored only as Argon2id hashes; email is optional and is not a login identity.

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

Send `Content-Type: application/json`; GraphQL responses use `application/graphql-response+json`. An absent `Accept` header is supported. When supplied, it must allow `application/graphql-response+json`; unsupported response media returns 406. GraphQL request envelopes are decoded by http4s/Circe, while GraphQL document parsing remains owned by Sangria.

`query` is required. Missing/null `variables` means an empty object, and missing/null `operationName` means no selected name. When the document contains several operations, provide an operation name. Supply required variables explicitly; the pinned Sangria version has a documented limitation with omitted required-variable defaults.

## Errors and limits

Errors use a sanitized `errors` array and a generated `X-Request-ID` response header. Invalid syntax/schema/variables/operation selection or query budgets produce 400; unsupported method/media produce 405/406/415; oversized body 413; overload 503; deadline 504; unexpected failures 500. Normal field errors and typed hiring payload errors use GraphQL's HTTP 200 response convention. Hiring payloads expose stable codes such as `UNAUTHORIZED`, `FORBIDDEN`, `NOT_FOUND`, `DUPLICATE_APPLICATION`, `INVALID_STATUS_TRANSITION`, `PROFILE_ROLE_MISMATCH`, and `PROFILE_UNSUPPORTED_FOR_ROLE`, without raw driver messages or stack traces.

The [runbook](foundation.md#http-contract-and-budgets) specifies request size, Sangria depth/complexity reducers, concurrency, and deadline bounds. Introspection uses those same limits. Run `sbt test` for contract checks and `sbt 'IntegrationTest / test'` for real HTTP/database checks.
