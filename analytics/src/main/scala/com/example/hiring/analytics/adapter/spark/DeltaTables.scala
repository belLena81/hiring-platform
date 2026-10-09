package com.example.hiring.analytics.adapter.spark

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}

/** The one place that opens Delta tables by logical analytics location (resolved to a physical Spark path once). */
private[analytics] object DeltaTables {
  def read(spark: SparkSession, location: String): DataFrame =
    spark.read.format("delta").load(SparkPhysicalLocation.resolve(location))

  def forPath(spark: SparkSession, location: String): DeltaTable =
    DeltaTable.forPath(spark, SparkPhysicalLocation.resolve(location))

  def exists(spark: SparkSession, location: String): Boolean =
    DeltaTable.isDeltaTable(spark, SparkPhysicalLocation.resolve(location))

  def readIfExists(spark: SparkSession, location: String): Option[DataFrame] =
    Option.when(exists(spark, location))(read(spark, location))
}
