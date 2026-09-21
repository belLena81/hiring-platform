# Filtering and tracing hiring platform logs

Application diagnostics are single-line JSON records in `_logs/hiring-platform.log` at the project root. The packaged [logback.xml](../src/main/resources/logback.xml) is the only severity filter: it defaults to `INFO`, enqueues records through a bounded asynchronous appender, rolls at 20 MiB, and retains ten previous `.log` files. To investigate an incident with `DEBUG` or `TRACE`, change the packaged logger level, rebuild, and redeploy. There is no application `LOG_LEVEL` setting and no live reload.

The application uses log4cats `StructuredLogger` for structured fields and otel4s for tracing. The active OpenTelemetry trace ID is the request correlation ID. HTTP server spans, W3C propagation, active-request metrics and duration metrics are owned by the http4s otel4s middleware. Application diagnostics retain bounded rejection, GraphQL, readiness and child-operation records without synthetic HTTP completion/cancellation events.

```bash
rg '"requestId":"YOUR-RESPONSE-REQUEST-ID"' _logs/hiring-platform.log
rg 'traceId|voyage.embeddings' _logs/hiring-platform.log
```

## Tracing

Tracing is initialized through the OpenTelemetry Java SDK's standard environment configuration, including `OTEL_SDK_DISABLED`, `OTEL_SERVICE_NAME`, `OTEL_EXPORTER_OTLP_ENDPOINT`, and `OTEL_TRACES_EXPORTER`. Disable the SDK with `OTEL_SDK_DISABLED=true` or `OTEL_TRACES_EXPORTER=none`; the application does not duplicate those controls. Cats Effect fiber-context propagation is enabled before initializing otel4s, so no additional JVM property is required. Incoming W3C `traceparent` headers are joined by `ServerMiddleware`, and response propagation is owned by the same middleware. Child spans and application diagnostics share the server trace ID. Mongo/HTTP readiness does not depend on a collector being available.

## Disclosure policy

`logging.mask-sensitive = true` is the default. It redacts operation names, Mongo targets, HTTP host, trace/span IDs, actor IDs, and entity IDs. Set it to `false` only when the Logback appender is a pre-provisioned private rolling file: the application rejects a missing, non-file, writable-by-group/other, or readable-by-group/other destination before unmasked diagnostics start.

With masking disabled, diagnostics may reveal approved typed values only: UUID actor/entity IDs, operation/use-case and span names, title, country, city, skills, booleans, statuses, counts, pagination size, collection/operation names, version/retry state, queue state, and durations. This mode is for controlled troubleshooting and its files must not be shared.

Credentials, authorization and cookie headers, JWTs, raw GraphQL documents, request bodies/variables, descriptions, requirements, feedback, decline reasons, resumes, search text, vectors, BSON filters, Mongo URIs, raw exception messages, and stack traces are never emitted, even with masking disabled. Trace attributes follow the same restriction.

Logging remains best effort: a failing sink cannot change an HTTP result, cancellation, resource finalizer, or http4s concurrency middleware cleanup. The bounded asynchronous queue prevents routine file I/O from adding storage latency to application fibers; the rolling-file retention bound limits disk use but does not guarantee a latency SLO.
