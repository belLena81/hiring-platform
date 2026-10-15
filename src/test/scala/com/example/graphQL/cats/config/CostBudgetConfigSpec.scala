package com.example.graphQL.cats.config

import munit.FunSuite

/** ARR-14: the cost budget is typed configuration with packaged defaults and accumulated validation. */
final class CostBudgetConfigSpec extends FunSuite {
  test("ARR-14.1 packaged defaults carry the cost budget") {
    assertEquals(
      AppConfigFixtures.withPackagedDefaults("").map(_.graphql.costBudget),
      Right(CostBudgetConfig(60, 10000, maxActorBuckets = 10000, maxClientBuckets = 10000, overflowBuckets = 256))
    )
  }

  test("ARR-14.1 the cost budget can be overridden") {
    val overrides =
      "graphql.cost-budget { window-seconds = 30, max-cost = 1500, max-actor-buckets = 50, max-client-buckets = 60, overflow-buckets = 8 }"
    assertEquals(
      AppConfigFixtures.withPackagedDefaults(overrides).map(_.graphql.costBudget),
      Right(CostBudgetConfig(30, 1500, 50, 60, 8))
    )
  }

  test("ARR-14.1 non-positive values are rejected and every violation is reported") {
    val overrides =
      "graphql.cost-budget { window-seconds = 0, max-cost = -1, max-actor-buckets = 0, max-client-buckets = 0, overflow-buckets = 0 }"
    val keys = AppConfigFixtures.withPackagedDefaults(overrides).left.map(_.toList.map(_.key).toSet)
    assertEquals(
      keys,
      Left(
        Set(
          "GRAPHQL_COST_BUDGET_WINDOW_SECONDS",
          "GRAPHQL_COST_BUDGET_MAX_COST",
          "GRAPHQL_COST_BUDGET_MAX_ACTOR_BUCKETS",
          "GRAPHQL_COST_BUDGET_MAX_CLIENT_BUCKETS",
          "GRAPHQL_COST_BUDGET_OVERFLOW_BUCKETS"
        )
      )
    )
  }

  test("ARR-14.1 a budget below the per-query complexity cap is rejected") {
    val overrides =
      "graphql.cost-budget { window-seconds = 60, max-cost = 999, max-actor-buckets = 50, max-client-buckets = 50, overflow-buckets = 8 }"
    assertEquals(
      AppConfigFixtures.withPackagedDefaults(overrides).left.map(_.toList.map(_.key)),
      Left(List("GRAPHQL_COST_BUDGET_MAX_COST"))
    )
    assertEquals(
      AppConfigFixtures
        .withPackagedDefaults(
          "graphql.cost-budget { window-seconds = 60, max-cost = 1000, max-actor-buckets = 50, max-client-buckets = 50, overflow-buckets = 8 }"
        )
        .map(_.graphql.costBudget.maxCost),
      Right(1000)
    )
  }

  test("ARR-15.1 schema exposure is off in the packaged defaults") {
    assertEquals(AppConfigFixtures.withPackagedDefaults("").map(_.graphql.schemaExposure), Right(false))
  }

  test("ARR-15.2 schema exposure can be switched on by configuration and by the environment override") {
    assertEquals(
      AppConfigFixtures.withPackagedDefaults("graphql.schema-exposure = true").map(_.graphql.schemaExposure),
      Right(true)
    )
    assertEquals(
      AppConfigFixtures
        .withPackagedDefaults("", Map("GRAPHQL_SCHEMA_EXPOSURE" -> "true"))
        .map(_.graphql.schemaExposure),
      Right(true)
    )
  }

  test("ARR-15.1 a non-boolean schema exposure value is rejected by key") {
    assertEquals(
      AppConfigFixtures.withPackagedDefaults("graphql.schema-exposure = maybe").left.map(_.toList.map(_.key)),
      Left(List("GRAPHQL_SCHEMA_EXPOSURE"))
    )
  }
}
