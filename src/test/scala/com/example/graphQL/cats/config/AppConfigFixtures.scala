package com.example.graphQL.cats.config

import cats.data.NonEmptyList
import com.typesafe.config.ConfigFactory
import scala.jdk.CollectionConverters.*

/** Test-only environment layer: `${?NAME}` placeholders in `raw` resolve against `env`, never the real process. */
object AppConfigFixtures {
  def fromConfig(raw: String, env: Map[String, String]): Either[NonEmptyList[ConfigError], AppConfig] =
    AppConfig.parse(raw).map(_.withFallback(ConfigFactory.parseMap(env.asJava))).flatMap(AppConfig.fromParsed)

  /** The packaged `application.conf` plus the minimal local values it leaves unset, then `overrides`. */
  def withPackagedDefaults(overrides: String): Either[NonEmptyList[ConfigError], AppConfig] =
    fromConfig(
      "include classpath(\"application.conf\")\nhttp.host=\"127.0.0.1\"\nhttp.port=8080\nmongo.uri=\"mongodb://127.0.0.1:27018\"\nauth.jwt.hs256-secret=\"synthetic-test-signing-key-material\"\n" + overrides,
      Map.empty
    )
}
