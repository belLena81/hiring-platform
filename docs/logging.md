# Filtering and debugging hiring platform logs

Application records are single-line JSON. Existing `timestamp`, `severity`, `category` and generated `requestId` remain; `marker`, `component`, `message`, `masking` and bounded `details` add searchable evidence. SLF4J also receives the matching marker and severity. Raw framework/driver loggers stay disabled.

Useful markers:

| Marker | Meaning and evidence |
|---|---|
| `HP.HTTP.REQUEST_REJECTED` | Reason code and status; correlate with the request completion |
| `HP.HTTP.REQUEST_COMPLETED` | Normalized route/method, application status, elapsed milliseconds |
| `HP.HTTP.REQUEST_CANCELLED` | Cancellation after cleanup; no invented HTTP status |
| `HP.GRAPHQL.GRAPHQL_COMPLETED` | Operation result and sensitive operation name |
| `HP.MONGO.MONGO_PROBE_FAILED` | Safe driver classification/location, duration and sensitive target metadata |
| `HP.READINESS.MONGO_UNAVAILABLE` | Readiness failure correlated with the HTTP request and Mongo probe |
| `HP.RUNTIME.STARTUP_FAILED` / `HP.RUNTIME.RUNTIME_FAILED` | Safe exception class and an allowlisted application source location |
| `HP.CONFIG.CONFIG_INVALID` | Invalid configuration key, never its supplied value |
| `HP.GRAPHQL.REQUEST_PAYLOAD` | Explicitly enabled local filtered request capture |

`message` is a short fixed description; `details.reason` provides a stable failure code instead of a raw exception message. Public metadata uses controlled values; unknown public values are `[FILTERED]`. Fields such as operation names, MongoDB hosts/database and bind host are `[REDACTED]` by default. Unsupported source locations appear as `unavailable` rather than an arbitrary path/stack trace. Completion means an application response was created, not that the peer received it.

## Capture and search locally

Run from the repository root. Logs are ignored local data and can be sensitive:

```bash
mkdir -p .local/logs
sbt -Dsbt.color=false -Dsbt.supershell=false run 2>&1 | tee .local/logs/backend.log
```

Filter by marker or by the `X-Request-ID` returned to Postman:

```bash
rg 'HP.HTTP.REQUEST_REJECTED|HP.MONGO.MONGO_PROBE_FAILED' .local/logs/backend.log
rg '"requestId":"YOUR-RESPONSE-REQUEST-ID"' .local/logs/backend.log
```

If `jq` is available, strip sbt's prefix and select structured records:

```bash
jq -R 'sub("^[^{]*"; "") | fromjson? | select(.component == "MONGO")' .local/logs/backend.log
jq -R 'sub("^[^{]*"; "") | fromjson? | select(.severity == "ERROR")' .local/logs/backend.log
```

A driver failure, readiness result and HTTP completion share the same generated request ID. Client-supplied correlation IDs, raw request paths/query strings, headers and error messages are not trusted as diagnostic evidence. Numeric detail values are encoded as decimal strings; use `tonumber` in `jq` for numerical comparisons.

The service and adapter each retain their two-second deadline. The service can cancel the adapter before a driver diagnostic is emitted; in that case use the correlated `PROBE_TIMEOUT` readiness record. It proves a readiness deadline expired, not a specific underlying network/database root cause.

## Masking and payload flags

| Variable | Default | Policy |
|---|---|---|
| `LOG_MASK_SENSITIVE` | `true` | `false` requires loopback HTTP binding |
| `LOG_REQUEST_PAYLOADS` | `false` | `true` additionally requires masking disabled |
| `LOG_LEVEL` | `INFO` | `INFO`, `WARN` or `ERROR`; payload/completion records are INFO |

Safe defaults apply even on a developer laptop. To reveal selected diagnostic metadata locally:

```bash
cat > local.conf <<'EOF'
LOG_MASK_SENSITIVE=false
EOF
sbt run
```

To additionally capture filtered GraphQL requests locally:

```bash
cat > local.conf <<'EOF'
LOG_MASK_SENSITIVE=false
LOG_REQUEST_PAYLOADS=true
LOG_LEVEL=INFO
EOF
sbt run
```

Keep the default loopback bind (`127.0.0.1`, or IPv6 `::1`). Wildcard/non-loopback bindings reject these unsafe options. Missing/invalid flags fail closed; request payload capture requires masking disabled. `local.conf` is ignored and overlaid on `application.conf`; `.env` files are not automatically loaded. Runtime/configuration failure fallback logging always remains masked.

## What local payload capture contains

This is a reconstructed diagnostic payload, **not the original body or a replayable request**. Replaced literals can change input types. It is produced only after GraphQL schema validation, operation selection and variable coercion succeed. Invalid/malformed requests are never dumped.

- The selected operation and reachable fragments retain query structure; literal values, defaults, directives' literal values, comments and source-text attachments are removed or redacted.
- Variable containers retain bounded structure. All scalar values are redacted except top-level JSON Boolean values declared as Boolean/Boolean! in the selected operation. Credential-like keys remain redacted even for Boolean values.
- This strict allowlist matches today's health-only API. New hiring-input value allowlists need their own review; local capture does not automatically expose arbitrary future user data.
- Payloads are limited to 2 KiB of UTF-8 JSON with bounded traversal and omission/truncation indicators. Sanitization failure omits capture rather than falling back to the raw request.
- Default/masked loggers drop payload records/fields even if accidentally supplied, and disabled producers do not construct payloads.

Passwords, tokens, authorization/cookie headers, URI credentials/options, raw exception messages and full stack traces are excluded in both modes. This is not universal secret detection: explicitly unmasked operation/database/host names and payload key names can themselves contain sensitive text. Do not put secrets into those names, share local logs without review, or deploy local-only configuration. Production safety depends on correct configuration; these flags do not certify an entire deployment secure.

Ordinary metadata values are limited to 128 characters, 12 detail fields and 8 KiB per JSON record. Mongo target metadata contains at most four parsed hosts, never the original URI. Logging errors are best-effort and do not recursively log themselves or change application results; cancellation and mandatory cleanup remain owned by the original request.

See [diagnostics acceptance evidence](specs/hiring-platform-diagnostics.md).
