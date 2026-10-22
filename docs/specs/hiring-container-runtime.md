# Hiring container runtime

## Identity and scope

- Task: container images and local Compose runtime for the hiring API and analytics workers (operational hardening; cross-cuts Production hardening).
- Status: in review
- Coordinator: Product Manager (main session). Implementation: main session. Reviews: independent Code Reviewer, Security Engineer, QA Engineer.
- User outcome: the hiring API and each analytics worker run from a reproducible, minimal, non-root image; local Compose keeps secrets out of container environment metadata, bounds resources and logs, and starts services in a dependable order.
- Authorized scope: the nine containerisation findings accepted by the user on 2026-10-22 (multi-stage analytics image, API image, file-based secrets, least privilege, resource limits, Compose structure, log rotation, opt-in image scanning, `.dockerignore`).
- Non-goals: Kubernetes or another orchestrator; registries, image signing, pushing or deploying; ELK/Prometheus/Falco stacks; CI; changes to `compose.test.yaml` and `compose.atlas-local.yaml` (the test wrapper owns their secrets); application or analytics behaviour; enabling Kafka publication from the containerised API (see D6).
- Dependencies: Docker Engine with BuildKit and Compose 2.24+. The harness guard denies agents `docker run`, `docker compose up` and any Compose file other than `compose.test.yaml`, so runtime acceptance of `compose.yaml` services is a user-run check (see Acceptance).

## Source context and decisions

Verified implementation before the change:

- `analytics/Dockerfile` and `analytics/Dockerfile.erasure` were identical apart from the main class; both shipped sbt, JDK, sources and `test-support` and ran `sbt runMain` as root. With `Compile / run / fork := true`, sbt started a child JVM that did not reliably receive the container stop signal.
- No image existed for the API (`com.example.graphQL.cats.Main`); it ran only through `sbt run` on the host.
- `compose.yaml` passed Kafka, HMAC and JWT secrets as environment variables, including whole JAAS strings, which are visible through `docker inspect`. It had no resource limits, log rotation or Kafka healthcheck, and `kafka-acl-init` carried a long inline script.
- `src/main/resources/logback.xml` writes only to the relative file `_logs/hiring-platform.log`.
- `KafkaConfigValidation.scala:130` rejects SASL without TLS unless every bootstrap server is loopback.

Decisions:

- D1 Packaging: each build gains a `stageContainerRuntime` task that stages the application jar, the ordered runtime dependency jars and a Java `@argfile` classpath. Built-in sbt keys cover the need, so no packaging plugin is added. Dependencies and the application jar are separate image layers.
- D2 Secrets: Compose `secrets:` are backed by files in the ignored `.local/config/secrets/`. Services receive `NAME_FILE=/run/secrets/<secret>`. The image entrypoint `docker/load-secret-files.sh` exports `NAME` from the file, but only for paths under `/run/secrets/`, then `exec`s the JVM. Application config is unchanged, and host `sbt run` keeps plain environment variables.
- D3 Kafka broker: `docker/kafka/broker-entrypoint.sh` builds the JAAS settings inside the container from secret files and then hands control to the image's own entrypoint. `docker/kafka/provision-access.sh` holds the topic and ACL provisioning, mounted read-only.
- D4 Secret file permissions: the host directory is `0700` and its files `0644`. Plain Compose ignores `uid/gid/mode` for file secrets, and containers run as different UIDs; the `0700` parent keeps other host users out.
- D5 Container logging: the API image selects the bundled `logback-container.xml`, which keeps the same loggers but writes to stdout. Compose then rotates the logs (json-file, 10 MiB × 3).
- D6 The containerised API runs with `KAFKA_ENABLED=false`. Publishing to the local SASL_PLAINTEXT broker over the Compose network would require relaxing a security validation or adding TLS to the local broker; that is a separate, security-reviewed scope. Host `sbt run` keeps publishing as before.
- D7 The API service is opt-in through the `api` profile, so `docker compose up` behaves as before.
- D8 The Kafka healthcheck is a TCP probe of the internal listener (no JVM start per probe). `kafka-acl-init` keeps its retry loop as the authoritative readiness check.
- D9 The existing ACL revocations for the retired `publisher` principal and the `hiring.phase6.runtime` topic are kept unchanged. They are idempotent, and removing them could leave stale grants on an older local broker. The topic is an existing resource name.
- D10 Image scanning is opt-in (`scripts/scan-container-images.sh`, using a local `trivy`) and not part of `scripts/check-local.sh`, which must keep working offline and without extra tools.

- D11 (review round 1): The analytics image takes `RUNTIME_UID`/`RUNTIME_GID` build args. Compose passes `HIRING_HOST_UID`/`HIRING_HOST_GID` (default 1000), so workers own their files in the bind-mounted `.local/data/analytics` without a chown of existing data; the base image's uid 1000 passwd entry is reused, which Hadoop requires. The API image keeps uid 10001, since it has no bind mounts. Spark's warehouse directory moves to the data volume (`spark-jvm.args`), and `HOME=/tmp`.
- D12 (review round 1): The previous HMAC key reaches the workers only through `compose.analytics-key-rotation.yaml`, as a secret file; the base file no longer passes it as an environment value. The entrypoint rejects empty secret files (a missing cloud environment secret fails at startup) and ignores `*_FILE` paths containing `..`.
- D13 (review round 1): `.dockerignore` excludes `local.conf`, `.env*` and key/keystore files at any depth, so the git-ignored `src/main/resources/local.conf` is not packaged into the API image. The analytics heap is 60% of the container limit. The API image no longer sets the `trackFiberContext` debugging flag.
- Behaviour changes accepted with D2: the analytics batch now requires the HMAC secret file (it previously defaulted to empty and failed later in config validation), and starting the local broker requires the seven Kafka secret files.
- Exception to AC-04: MongoDB keeps its default capability set because its entrypoint starts as root and drops to the `mongodb` user; the opt-in `atlas-local` image is unchanged apart from log rotation.

Resource limits are local-footprint assumptions, not measured sizing: MongoDB 2 GiB, Kafka 1 GiB (heap 512 MiB), API 1 GiB / 2 CPUs, analytics workers 4 GiB / 2 CPUs. JVM heaps follow the container limit (`-XX:MaxRAMPercentage=75`).

## Behavior and contracts

No API, event, analytics or MongoDB contract changes; [schema evolution](../schema-evolution.md) is not applicable. Changed operator contracts:

- Secret inputs move from `.env` variables to files in `.local/config/secrets/`: `kafka_broker_password`, `kafka_publisher_password`, `kafka_reader_password`, `kafka_fencer_password`, `kafka_interview_orchestrator_password`, `kafka_interview_worker_password`, `kafka_interview_fencer_password`, `analytics_hmac_secret_base64`, `auth_jwt_hs256_secret`, `auth_receipt_fp_secret`.
- `compose.cloud.yaml` sources the analytics secrets from the operator's environment (Compose `environment:` secret sources), so workers still read files.
- `analytics/Dockerfile.erasure` is removed; one analytics image serves batch, streaming and erasure through `command:`.

## Domain language and examples

Infrastructure only; no hiring context, aggregate or invariant is touched, and no glossary term is added.

| Rule | Example tag | Scenario |
|---|---|---|
| Secrets reach the process, not the container metadata | AC-03.1 | Given `ANALYTICS_KAFKA_PASSWORD_FILE=/run/secrets/kafka_reader_password`, when the worker starts, then the JVM sees `ANALYTICS_KAFKA_PASSWORD` and `docker inspect` shows only the file path |
| | AC-03.2 | Given `HIRING_RECOVERY_CLASSPATH_FILE=/etc/hostname` (not under `/run/secrets/`), when the entrypoint runs, then the variable is left untouched and `HIRING_RECOVERY_CLASSPATH` stays unset |
| | AC-03.3 | Given both `X` and `X_FILE` are set, the entrypoint exits non-zero without printing either value |
| | AC-03.4 | Given `X_FILE` names a missing or unreadable file, the entrypoint exits 66 |
| | AC-03.5 | Given `X_FILE` names an empty file, the entrypoint exits 65 |
| | AC-03.6 | Given `X_FILE=/run/secrets/../secrets/x`, the entrypoint leaves it untouched and `X` stays unset |
| Stop reaches the JVM | AC-01.2 | Given a running erasure worker, when `docker compose stop` is issued, then PID 1 is `java` and it receives SIGTERM |

## Acceptance and evidence

| ID | Given / When / Then | Implementation and test paths | Verification | Actual outcome |
|---|---|---|---|---|
| AC-01 | The analytics image is multi-stage: the runtime stage has a JRE, no sbt, compiler or sources, runs as a non-root user, and `java` is PID 1 (`AC-01.1` build, `AC-01.2` stop signal) | `analytics/Dockerfile`, `analytics/build.sbt` | `docker build`; `docker image inspect` (user, entrypoint, size) | See checkpoint |
| AC-02 | An API image exists with the same properties, writes logs to stdout and has a healthcheck on `/health` | `Dockerfile`, `build.sbt`, `src/main/resources/logback-container.xml` | `docker build`; `docker image inspect` | See checkpoint |
| AC-03 | Secrets are delivered as files; the entrypoint behaves per AC-03.1–AC-03.4 | `docker/load-secret-files.sh`, `compose.yaml`, `compose.cloud.yaml` | Shell tests of the entrypoint (`scripts/check-container-runtime.sh`) | See checkpoint |
| AC-04 | Compose services drop all capabilities, set `no-new-privileges`, use a read-only root filesystem where the image allows it, and have memory/CPU/PID limits | `compose.yaml` | Static review; user-run `docker compose config` | See checkpoint |
| AC-05 | Kafka has a healthcheck; dependants wait for health; provisioning is a versioned, read-only script | `compose.yaml`, `docker/kafka/*.sh` | Static review; `bash -n`; user-run stack start | See checkpoint |
| AC-06 | Every `compose.yaml` service rotates json-file logs | `compose.yaml` | Static review | See checkpoint |
| AC-07 | Base images are pinned by digest; an opt-in scan script exists | Dockerfiles, `scripts/scan-container-images.sh` | Static review; `bash -n` | See checkpoint |
| AC-08 | The build context excludes logs, docs and tool directories | `.dockerignore` | Context size during `docker build` | See checkpoint |
| AC-09 | Existing unit tests still pass | — | `sbt test` (root and analytics) | See checkpoint |

## Implementation handoff

- One slice; all files above. Shared-file owner: main session.
- Recovery: revert the listed files; no data or contract migration. Existing `.local/data` volumes are unchanged. MongoDB and Kafka keep their images and data paths.

## Checkpoint and review

- Implemented: all files listed under Acceptance, plus `compose.analytics-key-rotation.yaml`. Committed at the user's request with re-review and QA still pending.
- Evidence (2026-10-22):
  - `scripts/check-container-runtime.sh` passes AC-03.1–AC-03.6.
  - `sbt test` (root): 1650 passed, 0 failed (before review round 1; the later root changes touch only build staging and image files).
  - `sbt scalafmtSbtCheck stageContainerRuntime` passes on root. Analytics staging (216 dependency jars, 316 MB) was verified on a clean `git archive HEAD` copy, because the working tree's concurrently edited `analytics/src` fails to compile in `AnalyticsErasureRepairMain.scala` (not part of this change).
  - Both staged classpaths start their main class and fail fast on missing configuration; the API logs JSON to stdout with `logback-container.xml`.
  - Round-1 images built: `hiring-api:local` 357 MB, `hiring-analytics:local` 594 MB, user 10001, `java` exec entrypoint, 1.78 MB build context.
  - Kafka image entrypoint confirmed by `docker image inspect`: `/__cacert_entrypoint.sh`, CMD `/etc/kafka/docker/run`.
  - Post-review rebuild: both images built (analytics with `RUNTIME_UID/GID=1000`). The application jar in `hiring-api:local`, extracted with `docker save`, contains no `local.conf`.
- Not run (guard-denied to agents; user-run): `docker compose config` for `compose.yaml` with each overlay; starting `mongodb kafka kafka-acl-init`, `--profile api` and each analytics profile; `docker compose stop` signal check (AC-01.2); Trivy scan. Analytics `sbt test` is blocked by the concurrent analytics source edits.
- Code Reviewer verdict, round 1: FAIL. Fixed: H1 data-directory ownership (D11), H2 warehouse dir (D11), M1 previous HMAC key (D12), M3 heap headroom (D13), L1 mtime preservation, L3 debug flag, L4 test gap. Documented: M2 (behaviour changes), the AC-04 MongoDB exception. M4 confirmed by inspect. Not changed: L2. The `filterNot(_ == applicationJar)` is required, because `Runtime / dependencyClasspathAsJars` includes the project jar (observed in the first staging run). Re-review: pending.
- Security Engineer verdict, round 1: FAIL. Fixed: M1 `local.conf` in image (D13), L1 previous HMAC key (D12), L2 empty secrets (D12), L3 `..` paths (D12), the `IgnoreUnrecognizedVMOptions` flag and the trailing newline. Re-review: pending.
- Final independent QA verdict: pending.
- Next action: Code Reviewer and Security Engineer re-review, user-run Compose checks, then QA.
