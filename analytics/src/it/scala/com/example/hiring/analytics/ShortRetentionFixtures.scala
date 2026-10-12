package com.example.hiring.analytics

import com.example.hiring.analytics.adapter.spark.DeltaLogFactory
import com.example.hiring.analytics.config.AnalyticsOperationalSettings
import org.apache.spark.sql.SparkSession

import scala.concurrent.duration.*

/** Retention horizons that real wall-clock time can pass within a test. */
private[analytics] object ShortRetentionFixtures {

  /** Vacuum and log retention of one second let physical reclamation be observed without waiting for real days. */
  def shortRetention(operational: AnalyticsOperationalSettings): AnalyticsOperationalSettings =
    operational.copy(retention = operational.retention.copy(deltaVacuumSafety = 1.second, deltaLogRetention = 1.second))

  /** Delta log cleanup that sees the table the given number of days in the future. */
  def shiftedLogs(days: Long): DeltaLogFactory = new DeltaLogFactory {
    override def apply(spark: SparkSession, path: String) =
      org.apache.spark.sql.delta.HiringAnalyticsRetentionClock.forTable(spark, path, days * 86400L * 1000L)
  }
}
