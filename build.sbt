ThisBuild / scalaVersion := "3.9.0"

lazy val catsVersion = "2.13.0"
lazy val catsEffectVersion = "3.7.1"
lazy val circeVersion = "0.14.16"
lazy val fs2Version = "3.14.0"
lazy val fs2KafkaVersion = "3.9.1"
lazy val kafkaClientsVersion = "4.3.1"
lazy val http4sVersion = "0.23.37"
lazy val otel4sVersion = "1.1.0"
lazy val http4sOtelMiddlewareVersion = "0.19.0"
lazy val opentelemetryVersion = "1.66.0"
lazy val jwtScalaVersion = "11.0.4"
lazy val argon2Version = "2.12"
lazy val log4catsVersion = "2.8.0"
lazy val catsRetryVersion = "4.0.0"
lazy val caffeineVersion = "3.3.0"
lazy val logbackVersion = "1.6.3"
lazy val mongoVersion = "5.12.0"
lazy val mongoScalaBsonVersion = "5.12.0"
lazy val munitVersion = "1.3.6"
lazy val munitCatsEffectVersion = "2.2.1"
lazy val pureConfigVersion = "0.17.10"
lazy val ironVersion = "3.3.2"
lazy val sangriaVersion = "4.2.19"
lazy val sangriaCirceVersion = "1.3.2"
lazy val testcontainersVersion = "2.0.5"

lazy val IntegrationTest = config("it") extend Test

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(inConfig(IntegrationTest)(Defaults.testSettings))
  .settings(
    name := "hiring-graphql-platform",
    description := "Hiring Management Platform with Cats Effect and Sangria.",
    version := "0.1.0",
    publish / skip := true,
    Compile / run / fork := true,
    Compile / run / javaOptions += "-Dcats.effect.trackFiberContext=true",
    IntegrationTest / scalaSource := baseDirectory.value / "src" / "it" / "scala",
    IntegrationTest / resourceDirectory := baseDirectory.value / "src" / "it" / "resources",
    IntegrationTest / parallelExecution := false,
    Test / fork := true,
    Test / javaOptions += "-Dcats.effect.trackFiberContext=true",
    IntegrationTest / javaOptions += "-Dcats.effect.trackFiberContext=true",
    scalacOptions ++= Seq(
      "-encoding",
      "utf-8",
      "-release:17",
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Wunused:all",
      "-Wvalue-discard",
      "-Werror"
    ),
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-core" % catsVersion,
      "org.typelevel" %% "cats-effect" % catsEffectVersion,
      "co.fs2" %% "fs2-core" % fs2Version,
      "co.fs2" %% "fs2-io" % fs2Version,
      "co.fs2" %% "fs2-reactive-streams" % fs2Version,
      "com.github.fd4s" %% "fs2-kafka" % fs2KafkaVersion,
      "org.apache.kafka" % "kafka-clients" % kafkaClientsVersion,
      "org.sangria-graphql" %% "sangria" % sangriaVersion,
      "org.sangria-graphql" %% "sangria-circe" % sangriaCirceVersion,
      "org.http4s" %% "http4s-dsl" % http4sVersion,
      "org.http4s" %% "http4s-ember-client" % http4sVersion,
      "org.http4s" %% "http4s-ember-server" % http4sVersion,
      "org.http4s" %% "http4s-circe" % http4sVersion,
      "org.http4s" %% "http4s-otel4s-middleware-trace-server" % http4sOtelMiddlewareVersion,
      "org.http4s" %% "http4s-otel4s-middleware-metrics" % http4sOtelMiddlewareVersion,
      "org.typelevel" %% "otel4s-oteljava" % otel4sVersion,
      "org.typelevel" %% "otel4s-oteljava-context-storage" % otel4sVersion,
      "io.opentelemetry" % "opentelemetry-exporter-otlp" % opentelemetryVersion % Runtime,
      "io.opentelemetry" % "opentelemetry-sdk-extension-autoconfigure" % opentelemetryVersion % Runtime,
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion,
      "com.github.jwt-scala" %% "jwt-circe" % jwtScalaVersion,
      "de.mkammerer" % "argon2-jvm" % argon2Version,
      "org.mongodb" % "mongodb-driver-reactivestreams" % mongoVersion,
      "org.mongodb.scala" %% "mongo-scala-bson" % mongoScalaBsonVersion,
      "com.github.pureconfig" %% "pureconfig-core" % pureConfigVersion,
      "io.github.iltotore" %% "iron" % ironVersion,
      "io.github.iltotore" %% "iron-pureconfig" % ironVersion,
      "org.typelevel" %% "log4cats-slf4j" % log4catsVersion,
      "com.github.cb372" %% "cats-retry" % catsRetryVersion,
      "com.github.ben-manes.caffeine" % "caffeine" % caffeineVersion,
      "ch.qos.logback" % "logback-classic" % logbackVersion % Runtime,
      "org.scalameta" %% "munit" % munitVersion % Test,
      "org.typelevel" %% "munit-cats-effect" % munitCatsEffectVersion % Test,
      "org.testcontainers" % "testcontainers" % testcontainersVersion % Test
    )
  )
