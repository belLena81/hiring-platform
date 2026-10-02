package com.example.hiring.analytics.adapter.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog

/** Infrastructure seam for deterministic Delta cleanup clocks in isolated tests. */
private[analytics] trait DeltaLogFactory {
  def apply(spark: SparkSession, path: String): DeltaLog
}

private[analytics] object DeltaLogFactory {
  val system: DeltaLogFactory = new DeltaLogFactory {
    override def apply(spark: SparkSession, path: String): DeltaLog =
      DeltaLog.forTable(spark, SparkPhysicalLocation.resolve(path))
  }
}
