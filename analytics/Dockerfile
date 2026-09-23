FROM sbtscala/scala-sbt:eclipse-temurin-17.0.13_11_1.11.7_2.13.16

WORKDIR /workspace/analytics

COPY analytics/build.sbt ./
COPY analytics/project ./project
RUN sbt update

COPY analytics/src ./src

ENTRYPOINT ["/bin/sh", "-ec", ": \"${ANALYTICS_RUN_ID:?ANALYTICS_RUN_ID is required}\"; : \"${ANALYTICS_PARTITION:?ANALYTICS_PARTITION is required}\"; : \"${ANALYTICS_START_OFFSET:?ANALYTICS_START_OFFSET is required}\"; : \"${ANALYTICS_END_OFFSET_EXCLUSIVE:?ANALYTICS_END_OFFSET_EXCLUSIVE is required}\"; : \"${MONGODB_URI:?MONGODB_URI is required}\"; exec sbt \"runMain com.example.hiring.analytics.HiringAnalyticsBatchMain $ANALYTICS_RUN_ID $ANALYTICS_BOOTSTRAP_SERVERS $ANALYTICS_LAKEHOUSE_ROOT $ANALYTICS_TOPIC $ANALYTICS_PARTITION $ANALYTICS_START_OFFSET $ANALYTICS_END_OFFSET_EXCLUSIVE\""]
