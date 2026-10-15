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
      within(raw.costBudget.overflowBuckets, OverflowBuckets, ConfigError.InvalidCostBudgetOverflowBuckets),
      persistedQueries(raw.persistedQueries)
    ).mapN((window, maxCost, actors, clients, overflow, persisted) =>
      GraphQLConfig(CostBudgetConfig(window, maxCost, actors, clients, overflow), raw.schemaExposure, persisted)
    )

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

  private def within(value: Int, range: Range, error: ConfigError): ValidatedNel[ConfigError, Int] =
    Either.cond(range.contains(value), value, error).toValidatedNel
}
