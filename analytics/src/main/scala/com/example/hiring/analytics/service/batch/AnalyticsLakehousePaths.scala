package com.example.hiring.analytics.service.batch

import cats.data.ValidatedNec
import cats.syntax.all.*
import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.any.Not
import io.github.iltotore.iron.constraint.string.Blank

type LakehouseRoot = String :| Not[Blank]

final class AnalyticsLakehousePaths private (val root: LakehouseRoot) {
  private val normalizedRoot = root.stripSuffix("/")
  val bronze: String = s"$normalizedRoot/bronze/operational_events"
  val silver: String = s"$normalizedRoot/silver/operational_events"
  val quarantine: String = s"$normalizedRoot/quarantine/operational_events"
  val funnelGold: String = s"$normalizedRoot/gold/application_funnel"
  val timeToHireGold: String = s"$normalizedRoot/gold/time_to_hire"
  val skillsGold: String = s"$normalizedRoot/gold/job_skills"
  val manifests: String = s"$normalizedRoot/control/run_manifests"
  val streamingProgress: String = s"$normalizedRoot/control/streaming_progress"
  val hmacKeyRegistry: String = s"$normalizedRoot/control/hmac_key_registry"
}

object AnalyticsLakehousePaths {
  def from(root: String): ValidatedNec[String, AnalyticsLakehousePaths] =
    root
      .refineEither[Not[Blank]]
      .leftMap(_ => "lakehouse root must be non-empty")
      .toValidatedNec
      .map(validRoot => new AnalyticsLakehousePaths(validRoot))
}
