ThisBuild / scalaVersion := "3.9.0"

lazy val catsVersion           = "2.13.0"
lazy val catsEffectVersion     = "3.7.1"
lazy val circeVersion          = "0.14.16"
lazy val fs2Version            = "3.14.0"
lazy val http4sVersion         = "0.23.37"
lazy val jwtScalaVersion       = "11.0.4"
lazy val log4catsVersion       = "2.8.0"
lazy val logbackVersion        = "1.6.3"
lazy val mongoVersion          = "5.11.1"
lazy val munitVersion          = "1.3.6"
lazy val munitCatsEffectVersion = "2.2.0"
lazy val pureConfigVersion     = "0.17.10"
lazy val sangriaVersion        = "4.2.19"
lazy val sangriaCirceVersion   = "1.3.2"
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
    IntegrationTest / scalaSource := baseDirectory.value / "src" / "it" / "scala",
    IntegrationTest / resourceDirectory := baseDirectory.value / "src" / "it" / "resources",
    IntegrationTest / parallelExecution := false,
    Test / fork := true,
    scalacOptions ++= Seq(
      "-encoding", "utf-8", "-release:17", "-deprecation", "-feature",
      "-unchecked", "-Wunused:all", "-Wvalue-discard", "-Werror"
    ),
    libraryDependencies ++= Seq(
      "org.typelevel"       %% "cats-core"              % catsVersion,
      "org.typelevel"       %% "cats-effect"            % catsEffectVersion,
      "co.fs2"              %% "fs2-core"               % fs2Version,
      "co.fs2"              %% "fs2-io"                 % fs2Version,
      "org.sangria-graphql" %% "sangria"                % sangriaVersion,
      "org.sangria-graphql" %% "sangria-circe"          % sangriaCirceVersion,
      "org.http4s"          %% "http4s-dsl"             % http4sVersion,
      "org.http4s"          %% "http4s-ember-server"    % http4sVersion,
      "org.http4s"          %% "http4s-circe"           % http4sVersion,
      "io.circe"            %% "circe-core"             % circeVersion,
      "io.circe"            %% "circe-parser"           % circeVersion,
      "com.github.jwt-scala" %% "jwt-circe"             % jwtScalaVersion,
      "org.mongodb"         % "mongodb-driver-reactivestreams" % mongoVersion,
      "com.github.pureconfig" %% "pureconfig-core"       % pureConfigVersion,
      "org.typelevel"       %% "log4cats-slf4j"         % log4catsVersion,
      "ch.qos.logback"       % "logback-classic"        % logbackVersion % Runtime,
      "org.scalameta"       %% "munit"                  % munitVersion % Test,
      "org.typelevel"       %% "munit-cats-effect"       % munitCatsEffectVersion % Test,
      "org.testcontainers"   % "testcontainers"         % testcontainersVersion % Test
    )
  )
