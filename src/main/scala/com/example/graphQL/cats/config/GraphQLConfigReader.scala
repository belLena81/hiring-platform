package com.example.graphQL.cats.config

import io.github.iltotore.iron.pureconfig.given
import _root_.pureconfig.*

/** Bounds are decoded by the refined types; pureconfig reports every failing field, so one startup error still lists
  * every violation.
  */
private[config] final case class RawCostBudgetConfig(
    windowSeconds: CostBudgetWindowSeconds,
    maxCost: CostBudgetMaxCost,
    maxActorBuckets: CostBudgetBuckets,
    maxClientBuckets: CostBudgetBuckets,
    overflowBuckets: CostBudgetOverflowBuckets
) derives ConfigReader

private[config] final case class RawPersistedQueryConfig(mode: String, manifest: Option[String]) derives ConfigReader

private[config] final case class RawGraphQLConfig(
    costBudget: RawCostBudgetConfig,
    schemaExposure: Boolean,
    persistedQueries: Option[RawPersistedQueryConfig]
) derives ConfigReader
