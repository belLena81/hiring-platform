FROM sbtscala/scala-sbt:eclipse-temurin-17.0.20_8_1.13.0_2.13.18

WORKDIR /workspace/analytics

COPY analytics/build.sbt ./
COPY analytics/project ./project
RUN sbt update

COPY analytics/src ./src
COPY test-support /workspace/test-support

ENTRYPOINT ["sbt", "runMain com.example.hiring.analytics.cli.HiringAnalyticsBatchMain"]
