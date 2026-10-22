# syntax=docker/dockerfile:1
# Hiring GraphQL API image. Build: docker build -t hiring-api .
# The build stage compiles with sbt; the runtime stage holds only a JRE, the staged jars and the secret-file entrypoint.

FROM sbtscala/scala-sbt:eclipse-temurin-17.0.20_8_1.13.0_2.13.18@sha256:a61ef13627e06508c96317a267726f6071908f8273af1661a85b6d1d6c4844e2 AS build
WORKDIR /workspace
COPY build.sbt ./
COPY project/build.properties project/plugins.sbt ./project/
RUN --mount=type=cache,target=/root/.cache/coursier --mount=type=cache,target=/root/.sbt/boot \
    sbt --batch update
COPY src/main ./src/main
RUN --mount=type=cache,target=/root/.cache/coursier --mount=type=cache,target=/root/.sbt/boot \
    sbt --batch stageContainerRuntime

FROM eclipse-temurin:17-jre-noble@sha256:07fa0356cf354c5252947575f42ed42aa6fd921f36438b7bc77ad81c31c537dd
RUN groupadd --system --gid 10001 hiring \
    && useradd --system --uid 10001 --gid hiring --no-create-home --home-dir /nonexistent --shell /usr/sbin/nologin hiring
WORKDIR /app
COPY --chmod=0755 docker/load-secret-files.sh /usr/local/bin/load-secret-files
COPY --from=build /workspace/target/container-runtime/lib ./lib
COPY --from=build /workspace/target/container-runtime/classpath.args ./
COPY --from=build /workspace/target/container-runtime/app ./app
USER 10001:10001
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError -Djava.io.tmpdir=/tmp" \
    HTTP_HOST=0.0.0.0 \
    HTTP_PORT=8080
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=3 \
    CMD ["bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/${HTTP_PORT} && printf 'GET /health HTTP/1.0\\r\\nHost: localhost\\r\\n\\r\\n' >&3 && head -n 1 <&3 | grep -q ' 200 '"]
ENTRYPOINT ["/usr/local/bin/load-secret-files", "java", "-Dlogback.configurationFile=logback-container.xml", "@classpath.args"]
CMD ["com.example.graphQL.cats.Main"]
