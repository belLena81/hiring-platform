package com.example.hiring.analytics

import com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths

/** Checked lakehouse path construction for unit fixtures. */
private[analytics] object TestAnalyticsLakehousePaths {
  def unsafe(root: String): AnalyticsLakehousePaths =
    AnalyticsLakehousePaths
      .from(root)
      .toEither
      .fold(
        errors => throw new IllegalArgumentException(errors.toNonEmptyList.toList.mkString("; ")),
        identity
      )
}
