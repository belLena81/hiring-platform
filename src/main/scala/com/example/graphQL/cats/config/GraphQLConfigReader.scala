package com.example.graphQL.cats.config

import _root_.pureconfig.*

/** Plain numbers: ranges are validated together so one startup error lists every violation. */
private[config] final case class RawCostBudgetConfig(
    windowSeconds: Int,
    maxCost: Int,
    maxActorBuckets: Int,
    maxClientBuckets: Int,
    overflowBuckets: Int
) derives ConfigReader

private[config] final case class RawGraphQLConfig(costBudget: RawCostBudgetConfig, schemaExposure: Boolean)
    derives ConfigReader
