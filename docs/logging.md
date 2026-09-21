# Filtering and tracing hiring platform logs

Application diagnostics are single-line JSON records in `_logs/hiring-platform.log` at the project root. The packaged [logback.xml](../src/main/resources/logback.xml) is the only severity filter: it defaults to `INFO`, writes synchronously, rolls at 20 MiB, and retains ten previous `.log` files. To investigate an incident with `DEBUG` or `TRACE`, change the packaged logger level, rebuild, and redeploy. There is no application `LOG_LEVEL` setting and no live reload.

The application uses log4cats `StructuredLogger` for structured fields and otel4s for tracing. HTTP requests retain their generated `requestId`; trace and span identifiers are owned by OpenTelemetry and are exported through the configured OTLP exporter when tracing is enabled. Span start/success/failure/cancellation is recorded by the tracer, not by synthetic application log events.

```bash
rg '"requestId":"YOUR-RESPONSE-REQUEST-ID"' _logs/hiring-platform.log
rg 'http.request|voyage.embeddings' _logs/hiring-platform.log
```

## Tracing

Tracing is disabled unless `OTEL_TRACES_EXPORTER` is set to a value other than `none`. When enabled, the OpenTelemetry Java SDK uses its standard environment configuration, including `OTEL_SERVICE_NAME`, `OTEL_EXPORTER_OTLP_ENDPOINT`, and `OTEL_TRACES_EXPORTER`. The application enables Cats Effect fiber-context propagation before initializing otel4s, so no additional JVM property is required. Incoming W3C `traceparent` headers are joined, and child spans propagate through the http4s request boundary. Mongo/HTTP readiness does not depend on a collector being available.

## Disclosure policy

`logging.mask-sensitive = true` is the default. It redacts operation names, Mongo targets, HTTP host, trace/span IDs, actor IDs, and entity IDs. Set it to `false` only when the Logback appender is a pre-provisioned private rolling file: the application rejects a missing, non-file, writable-by-group/other, or readable-by-group/other destination before unmasked diagnostics start.

With masking disabled, diagnostics may reveal approved typed values only: UUID actor/entity IDs, operation/use-case and span names, title, country, city, skills, booleans, statuses, counts, pagination size, collection/operation names, version/retry state, queue state, and durations. This mode is for controlled troubleshooting and its files must not be shared.

Credentials, authorization and cookie headers, JWTs, raw GraphQL documents, request bodies/variables, descriptions, requirements, feedback, decline reasons, resumes, search text, vectors, BSON filters, Mongo URIs, raw exception messages, and stack traces are never emitted, even with masking disabled. Trace attributes follow the same restriction.

Logging remains best effort: a failing sink cannot change an HTTP result, cancellation, resource finalizer, or http4s concurrency middleware cleanup. Output is synchronous by the selected operational policy, so TRACE can add storage latency during a short investigation; the rolling-file retention bound limits disk use but does not guarantee a latency SLO.
