# syntax=docker/dockerfile:1
# Hiring analytics image for the batch, streaming and erasure workers; Compose selects the main class through `command:`.
# Build from the repository root: docker build -f analytics/Dockerfile -t hiring-analytics .
# The build stage compiles with sbt; the runtime stage holds only a JRE, the staged jars and the secret-file entrypoint.

FROM sbtscala/scala-sbt:eclipse-temurin-17.0.20_8_1.13.0_2.13.18@sha256:a61ef13627e06508c96317a267726f6071908f8273af1661a85b6d1d6c4844e2 AS build
WORKDIR /workspace/analytics
COPY analytics/build.sbt ./
COPY analytics/project/build.properties analytics/project/plugins.sbt ./project/
RUN --mount=type=cache,target=/root/.cache/coursier --mount=type=cache,target=/root/.sbt/boot \
    sbt --batch update
COPY analytics/src/main ./src/main
RUN --mount=type=cache,target=/root/.cache/coursier --mount=type=cache,target=/root/.sbt/boot \
    sbt --batch stageContainerRuntime

FROM eclipse-temurin:17-jre-noble@sha256:07fa0356cf354c5252947575f42ed42aa6fd921f36438b7bc77ad81c31c537dd
# The workers write a bind-mounted host directory, so Compose builds them with the host owner's uid/gid.
# Hadoop resolves the process user by name, so the runtime uid needs a passwd entry (an existing one, such as the
# base image's uid 1000 user, is reused).
ARG RUNTIME_UID=10001
ARG RUNTIME_GID=10001
RUN if ! getent group "$RUNTIME_GID" >/dev/null; then groupadd --system --gid "$RUNTIME_GID" hiring; fi \
    && if ! getent passwd "$RUNTIME_UID" >/dev/null; then \
         useradd --system --uid "$RUNTIME_UID" --gid "$RUNTIME_GID" --no-create-home --home-dir /nonexistent \
           --shell /usr/sbin/nologin hiring; \
       fi
WORKDIR /app
COPY --chmod=0755 docker/load-secret-files.sh /usr/local/bin/load-secret-files
COPY analytics/spark-jvm.args ./
COPY --from=build /workspace/analytics/target/container-runtime/lib ./lib
COPY --from=build /workspace/analytics/target/container-runtime/classpath.args ./
COPY --from=build /workspace/analytics/target/container-runtime/app ./app
USER ${RUNTIME_UID}:${RUNTIME_GID}
# The heap stays at 60% of the container limit: Spark's off-heap and native memory and the /tmp tmpfs share the cgroup.
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=60 -XX:+ExitOnOutOfMemoryError -Djava.io.tmpdir=/tmp" \
    HOME=/tmp
ENTRYPOINT ["/usr/local/bin/load-secret-files", "java", "@spark-jvm.args", "@classpath.args"]
CMD ["com.example.hiring.analytics.cli.HiringAnalyticsBatchMain"]
