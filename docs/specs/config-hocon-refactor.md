# Config HOCON Refactor

Status: implemented locally

## Goal

Replace the project-owned flat config parser with a Scala 3 compatible config library stack that supports grouped HOCON configuration, scoped environment substitution, typed decoding, and the existing validation behavior.

Recommended stack: Typesafe Config for HOCON parsing and substitution, with PureConfig for typed Scala case-class decoding. The implementation should remove custom parsing code instead of extending it.

## Non-goals

- Do not change runtime behavior for MongoDB, JWT auth, logging, or vector search.
- Do not introduce a frontend, deployment pipeline, secrets manager, or new runtime service.
- Do not auto-load `.env` files.
- Do not commit local secrets or credential-bearing local config.
- Do not hide missing required configuration with local-only placeholder defaults. Local runnable values belong in ignored root `local.conf`; packaged `application.conf` is allowed to require environment-backed cloud/runtime values.

## Target Config Shape

`src/main/resources/application.conf` should group related settings:

```hocon
http {
  host = ${?HTTP_HOST}
  port = ${?HTTP_PORT}
}

mongo {
  uri = ${?MONGODB_URI}
  database = "hiring"
}

logging {
  level = "INFO"
  level = ${?LOG_LEVEL}
  mask-sensitive = true
}

auth.jwt {
  hs256-secret = ${?AUTH_JWT_HS256_SECRET}
  issuer = "hiring-platform-local"
  audience = "hiring-graphql-api"
}

vector-search {
  enabled = false
  enabled = ${?VECTOR_SEARCH_ENABLED}

  voyage {
    api-key = ${?VOYAGE_API_KEY}
    endpoint = "https://api.voyageai.com/v1/embeddings"
    model = "voyage-4-lite"
    model = ${?VOYAGE_MODEL}
    dimension = 1024
  }

  embedding {
    version = 1
    queue-size = 128
    parallelism = 4
    timeout-ms = 5000
  }

  indexes {
    jobs = "jobs_embedding_vector"
    candidates = "candidates_embedding_vector"
  }

  num-candidates = 100
}
```

Root `local.conf` remains ignored and should use the same grouped shape. It provides local run values such as HTTP bind address and local MongoDB URI, and it overrides packaged values before final validation. Packaged `application.conf` should contain environment-backed values for cloud/runtime deployment settings, sensitive credentials, provider API keys, and startup-time knobs that are intentionally changed without editing packaged config, such as log level, feature enablement, and model selection. Optional `${?VAR}` substitutions remove the target path when the variable is absent; for required paths this must surface as a sanitized startup configuration error rather than being hidden by invalid local placeholder defaults. Values are resolved at application startup in this slice; runtime reload/hot swap is out of scope.

## Acceptance Criteria

| ID | Acceptance |
|---|---|
| CFG-AC01 | `application.conf` uses grouped HOCON sections for `http`, `mongo`, `logging`, `auth.jwt`, and `vector-search`. |
| CFG-AC02 | `AppConfig.load` reads packaged `application.conf`, overlays ignored root `local.conf` when present, resolves env substitutions, and returns the existing `AppConfig` model. |
| CFG-AC03 | Custom flat parsing helpers are removed from `AppConfig`; config parsing is delegated to the selected library stack. |
| CFG-AC04 | Existing validation semantics are preserved: numeric IP host, port range, Mongo URI/database rules, strict booleans, safe logging combinations, JWT secret rules, vector-search bounds, key-required behavior when vector search is enabled, and accepted log levels `TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR`. |
| CFG-AC05 | Local startup is supported by ignored `local.conf`, not by fake local defaults in packaged config; missing required packaged/env/local values fail startup with sanitized config errors. |
| CFG-AC06 | Existing tests are migrated from flat fixtures to grouped fixtures, with added coverage for `local.conf` overriding packaged values, env-backed cloud values resolving when present, optional disabled-provider credentials being absent-safe, and required env-backed paths failing safely when absent. |
| CFG-AC07 | Docs mention HOCON grouping, scoped startup-time `${?VAR}` usage, ignored `local.conf`, required-value failure behavior, and the no `.env` auto-load rule. |

## Implementation Path

1. Add Scala 3 compatible config dependencies, preferring PureConfig plus Typesafe Config.
2. Introduce raw decoded case classes that mirror the grouped config shape.
3. Convert raw decoded values into the existing public `AppConfig`, `JwtAuthConfig`, and `VectorSearchConfig` after running current validation.
4. Replace custom parser entry points in tests with library-backed helpers.
5. Rewrite packaged `application.conf` and generated test `local.conf` snippets to grouped HOCON, keeping local run values in ignored local config and env substitutions scoped to approved cloud/sensitive/startup-varying values.
6. Update docs and config specs.
7. Run `sbt test`, affected integration tests, `python3 scripts/check-skills.py`, and `git diff --check`.

## Risks

- HOCON optional substitution removes the path when a variable is absent. Use `${?VAR}` only for approved env-backed keys, and test that missing required env-backed values fail with sanitized diagnostics.
- PureConfig error messages should be mapped to sanitized `ConfigError` values so startup diagnostics do not leak secrets.
- Boolean and numeric coercion must remain strict enough to reject values like `TRUE`, `1`, and malformed ports if the current tests require that behavior.

## Checkpoint

- Implemented grouped HOCON `application.conf`, root `local.conf` overlay support, PureConfig-backed raw config decoding, retained `AppConfig` public model, and removed project-owned flat config parsing.
- Accepted log levels are `TRACE`, `DEBUG`, `INFO`, `WARN`, and `ERROR`; structured diagnostic events still use INFO/WARN/ERROR severities.
- Local evidence: `sbt 'testOnly com.example.graphQL.cats.config.AppConfigSpec com.example.graphQL.cats.infrastructure.logging.SafeDiagnosticsSpec'` passed 26/26; `sbt 'IntegrationTest / testOnly com.example.graphQL.cats.runtime.MainProcessSpec'` passed 14/14; `sbt test` passed 172/172.
