# Filtering and tracing hiring platform logs

Application diagnostics are single-line JSON records in `_logs/hiring-platform.log` at the project root. The packaged [logback.xml](../src/main/resources/logback.xml) is the only severity filter: it defaults to `INFO`, writes synchronously, rolls at 20 MiB, and retains ten previous `.log` files. To investigate an incident with `DEBUG` or `TRACE`, change the packaged logger level, rebuild, and redeploy. There is no application `LOG_LEVEL` setting and no live reload.

The application owns event severity. TRACE records model span lifecycle, while DEBUG records approved typed parameters and INFO/WARN/ERROR retain the established request, readiness, and runtime diagnostics. HTTP requests retain their generated `requestId` and add a trace ID, span ID, parent span ID, sequence, span name, and duration where tracing is enabled.

```bash
rg '"requestId":"YOUR-RESPONSE-REQUEST-ID"' _logs/hiring-platform.log
rg 'HP.TRACE.SPAN_(STARTED|SUCCEEDED|FAILED|CANCELLED)' _logs/hiring-platform.log
```

## Disclosure policy

`logging.mask-sensitive = true` is the default. It redacts operation names, Mongo targets, HTTP host, trace/span IDs, actor IDs, and entity IDs. Set it to `false` only when the Logback appender is a pre-provisioned private rolling file: the application rejects a missing, non-file, writable-by-group/other, or readable-by-group/other destination before unmasked diagnostics start.

With masking disabled, diagnostics may reveal approved typed values only: UUID actor/entity IDs, operation/use-case and span names, title, country, city, skills, booleans, statuses, counts, pagination size, collection/operation names, version/retry state, queue state, and durations. This mode is for controlled troubleshooting and its files must not be shared.

Credentials, authorization and cookie headers, JWTs, raw GraphQL documents, request bodies/variables, descriptions, requirements, feedback, decline reasons, resumes, search text, vectors, BSON filters, Mongo URIs, raw exception messages, and stack traces are never emitted, even with masking disabled.

Logging remains best effort: a failing sink cannot change an HTTP result, cancellation, resource finalizer, or admission permit release. Output is synchronous by the selected operational policy, so TRACE can add storage latency during a short investigation; the rolling-file retention bound limits disk use but does not guarantee a latency SLO.
