package com.example.graphQL.cats.config

import cats.data.ValidatedNel
import cats.syntax.all.*

private[config] object GraphQLConfigValidation {

  /** Per-field bounds are decoded by the refined raw types; the rules here relate settings to each other. */
  def read(raw: RawGraphQLConfig): ValidatedNel[ConfigError, GraphQLConfig] =
    (budgetAdmitsMaximalQuery(raw.costBudget), persistedQueries(raw.persistedQueries)).mapN { (budget, persisted) =>
      GraphQLConfig(
        CostBudgetConfig(
          budget.windowSeconds,
          budget.maxCost,
          budget.maxActorBuckets,
          budget.maxClientBuckets,
          budget.overflowBuckets
        ),
        raw.schemaExposure,
        persisted
      )
    }

  /** A budget below the per-query cap would refuse a query the schema itself admits. */
  private def budgetAdmitsMaximalQuery(budget: RawCostBudgetConfig): ValidatedNel[ConfigError, RawCostBudgetConfig] =
    Either
      .cond(budget.maxCost >= GraphQLConfig.MaxQueryComplexity, budget, ConfigError.InvalidCostBudgetMaxCost)
      .toValidatedNel

  /** An absent section is `off`; `allowlist` needs a non-blank manifest path (the file itself is read at startup). */
  private def persistedQueries(raw: Option[RawPersistedQueryConfig]): ValidatedNel[ConfigError, PersistedQueryConfig] =
    raw.fold(PersistedQueryConfig().validNel[ConfigError]) { section =>
      val manifest = section.manifest.map(_.trim).filter(_.nonEmpty)
      PersistedQueryMode.parse(section.mode.trim) match {
        case None                                                   => ConfigError.InvalidPersistedQueryMode.invalidNel
        case Some(PersistedQueryMode.Allowlist) if manifest.isEmpty =>
          ConfigError.InvalidPersistedQueryManifest.invalidNel
        case Some(mode) => PersistedQueryConfig(mode, manifest).validNel
      }
    }

}
