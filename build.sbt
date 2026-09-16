ThisBuild / scalaVersion := "2.13.16"

lazy val catsEffectVersion   = "3.6.1"
lazy val catsVersion         = "2.13.0"
lazy val circeVersion        = "0.15.0"
lazy val doobieVersion       = "1.0.0-RC9"
lazy val fs2Version          = "3.12.0"
lazy val log4catsVersion     = "2.7.1"
lazy val sangriaCirceVersion = "1.3.2"
lazy val sangriaVersion      = "4.2.10"
lazy val http4sVersion       = "0.23.17"
lazy val slf4jVersion        = "2.0.17"

lazy val scalacSettings = Seq(
  scalacOptions ++=
    Seq(
      "-deprecation",
      "-encoding",
      "utf-8",
      "-explaintypes",
      "-feature",
      "-language:existentials",
      "-language:higherKinds",
      "-language:implicitConversions",
      "-unchecked",
      "-Xcheckinit",
      "-Xlint:adapted-args",
      "-Xlint:constant",
      "-Xlint:delayedinit-select",
      "-Xlint:doc-detached",
      "-Xlint:inaccessible",
      "-Xlint:infer-any",
      "-Xlint:missing-interpolator",
      "-Xlint:nullary-unit",
      "-Xlint:option-implicit",
      "-Xlint:package-object-classes",
      "-Xlint:poly-implicit-overload",
      "-Xlint:private-shadow",
      "-Xlint:stars-align",
      "-Xlint:type-parameter-shadow",
      "-Ywarn-dead-code",
      "-Ywarn-extra-implicit",
      "-Ywarn-numeric-widen",
      "-Ywarn-unused:implicits",
      "-Ywarn-unused:imports",
      "-Ywarn-unused:locals",
      "-Ywarn-unused:params",
      "-Ywarn-unused:privates",
      "-Ywarn-value-discard",
      "-Ywarn-macros:before",
      "-Yrangepos"
    ),
  Compile / console / scalacOptions --= Seq("-Xfatal-warnings", "-Ywarn-unused:imports", "-Yno-imports")
)

lazy val commonSettings = scalacSettings ++ Seq(
  organization := "org.tpolecat",
  licenses ++= Seq(("MIT", url("http://opensource.org/licenses/MIT")))
)

lazy val root = (project in file("."))
  .settings(commonSettings)
  .settings(
    name := "hiring-graphql-platform",
    description := "Sangria example with doobie backend.",
    version := "0.1.0",
    libraryDependencies ++= Seq(
      "org.sangria-graphql" %% "sangria"                         % sangriaVersion,
      "org.sangria-graphql" %% "sangria-circe"                   % sangriaCirceVersion,
      "org.sangria-graphql" %% "sangria-cats-effect-experimental" % sangriaVersion,
      "org.http4s"          %% "http4s-dsl"                      % http4sVersion,
      "org.http4s"          %% "http4s-blaze-server"             % http4sVersion,
      "org.http4s"          %% "http4s-circe"                    % http4sVersion,
      "co.fs2"              %% "fs2-core"                       % fs2Version,
      "co.fs2"              %% "fs2-io"                         % fs2Version,
      "io.circe"            %% "circe-optics"                   % circeVersion,
      "org.tpolecat"        %% "doobie-core"                    % doobieVersion,
      "org.tpolecat"        %% "doobie-postgres"                % doobieVersion,
      "org.tpolecat"        %% "doobie-hikari"                  % doobieVersion,
      "org.typelevel"       %% "cats-effect"                    % catsEffectVersion,
      "org.typelevel"       %% "cats-core"                      % catsVersion,
      "org.typelevel"       %% "log4cats-slf4j"                 % log4catsVersion,
      "org.slf4j"            % "slf4j-simple"                   % slf4jVersion,
      "org.scalatest"       %% "scalatest"                      % "3.2.19" % Test
    )
  )
