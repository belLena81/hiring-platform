package com.example.graphQL.cats.config

import munit.FunSuite

/** AC-12: the persisted-query allowlist is typed configuration, off by default. */
final class PersistedQueryConfigSpec extends FunSuite {
  private def persisted(overrides: String, env: Map[String, String] = Map.empty) =
    AppConfigFixtures.withPackagedDefaults(overrides, env).map(_.graphql.persistedQueries)

  test("AC-12.4 the packaged default is off with no manifest") {
    assertEquals(persisted(""), Right(PersistedQueryConfig(PersistedQueryMode.Off, None)))
  }

  test("AC-12.1 allowlist mode with a manifest path is accepted, from configuration or the environment") {
    assertEquals(
      persisted("graphql.persisted-queries { mode = allowlist, manifest = \"queries.manifest\" }"),
      Right(PersistedQueryConfig(PersistedQueryMode.Allowlist, Some("queries.manifest")))
    )
    assertEquals(
      persisted(
        "",
        Map(
          "GRAPHQL_PERSISTED_QUERIES_MODE" -> "allowlist",
          "GRAPHQL_PERSISTED_QUERIES_MANIFEST" -> "queries.manifest"
        )
      ),
      Right(PersistedQueryConfig(PersistedQueryMode.Allowlist, Some("queries.manifest")))
    )
  }

  test("AC-12.3 allowlist mode without a manifest path is rejected by key") {
    assertEquals(
      AppConfigFixtures
        .withPackagedDefaults("graphql.persisted-queries.mode = allowlist")
        .left
        .map(_.toList.map(_.key)),
      Left(List("GRAPHQL_PERSISTED_QUERIES_MANIFEST"))
    )
  }

  test("AC-12.3 an unknown mode is rejected by key") {
    assertEquals(
      AppConfigFixtures
        .withPackagedDefaults("graphql.persisted-queries.mode = hash-only")
        .left
        .map(_.toList.map(_.key)),
      Left(List("GRAPHQL_PERSISTED_QUERIES_MODE"))
    )
  }
}
