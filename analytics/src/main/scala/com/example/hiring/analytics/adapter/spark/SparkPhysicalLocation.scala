package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.domain.AnalyticsLakehouseIdentity
import com.example.hiring.analytics.errors.AnalyticsError

import java.net.URI
import java.nio.file.Paths

/** Spark/Hadoop String paths are physical names; file URI escapes must be decoded once at their boundary. */
private[analytics] object SparkPhysicalLocation {
  def resolve(value: String): String =
    if (value.regionMatches(true, 0, "file:", 0, 5))
      AnalyticsLakehouseIdentity
        .from(value)
        .fold(
          _ => throw AnalyticsError.InvalidConfiguration("local analytical storage URI is not canonical"),
          _ => Paths.get(new URI(value).normalize()).normalize().toString
        )
    else value
}
