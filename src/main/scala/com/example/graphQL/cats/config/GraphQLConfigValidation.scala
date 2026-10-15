package com.example.graphQL.cats.config

import cats.data.ValidatedNel
import cats.syntax.all.*

private[config] object GraphQLConfigValidation {
  private val Windows = 1 to 3600
  // A budget below the per-query cap would refuse a query the schema itself admits.
  private val MaxCosts = GraphQLConfig.MaxQueryComplexity to 1000000
  private val Buckets = 1 to 100000
  private val OverflowBuckets = 1 to 4096

  def read(raw: RawGraphQLConfig): ValidatedNel[ConfigError, GraphQLConfig] =
    (
      within(raw.costBudget.windowSeconds, Windows, ConfigError.InvalidCostBudgetWindow),
      within(raw.costBudget.maxCost, MaxCosts, ConfigError.InvalidCostBudgetMaxCost),
      within(raw.costBudget.maxActorBuckets, Buckets, ConfigError.InvalidCostBudgetActorBuckets),
      within(raw.costBudget.maxClientBuckets, Buckets, ConfigError.InvalidCostBudgetClientBuckets),
      within(raw.costBudget.overflowBuckets, OverflowBuckets, ConfigError.InvalidCostBudgetOverflowBuckets)
    ).mapN((window, maxCost, actors, clients, overflow) =>
      GraphQLConfig(CostBudgetConfig(window, maxCost, actors, clients, overflow), raw.schemaExposure)
    )

  private def within(value: Int, range: Range, error: ConfigError): ValidatedNel[ConfigError, Int] =
    Either.cond(range.contains(value), value, error).toValidatedNel
}
