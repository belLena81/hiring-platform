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
| `LOG_LEVEL` | `INFO` | `TRACE`, `DEBUG`, `INFO`, `WARN` or `ERROR`; completion records are INFO |

Safe defaults apply even on a developer laptop. To reveal selected diagnostic metadata locally:

```bash
cat > local.conf <<'EOF'
logging {
  mask-sensitive = false
}
EOF
sbt run
```

Keep the default loopback bind (`127.0.0.1`, or IPv6 `::1`). Wildcard/non-loopback bindings reject unmasked metadata. Missing/invalid flags fail closed. `local.conf` is ignored and overlaid on `application.conf`; `.env` files are not automatically loaded. Runtime/configuration failure fallback logging always remains masked.

Passwords, tokens, authorization/cookie headers, URI credentials/options, raw request bodies, raw GraphQL query text, variable values, raw exception messages and full stack traces are excluded. This is not universal secret detection: explicitly unmasked operation/database/host names can themselves contain sensitive text. Do not put secrets into those names, share local logs without review, or deploy local-only configuration.

Ordinary metadata values are limited to 128 characters, 12 detail fields and 8 KiB per JSON record. Mongo target metadata contains at most four parsed hosts, never the original URI. Logging errors are best-effort and do not recursively log themselves or change application results; cancellation and mandatory cleanup remain owned by the original request.

See [diagnostics acceptance evidence](specs/hiring-platform-diagnostics.md).
