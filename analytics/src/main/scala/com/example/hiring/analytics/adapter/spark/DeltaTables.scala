package com.example.hiring.analytics.adapter.spark

import io.delta.tables.{DeltaMergeBuilder, DeltaTable}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

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

  /** Path-based Delta table identifier for SQL statements, with backticks escaped. */
  def sqlIdentifier(location: String): String =
    s"delta.`${SparkPhysicalLocation.resolve(location).replace("`", "``")}`"

  /** A merge of `source` into the table, matching rows whose key columns are equal. */
  def mergeOn(spark: SparkSession, location: String, source: DataFrame, keys: Seq[String]): DeltaMergeBuilder =
    forPath(spark, location)
      .as("target")
      .merge(source.as("source"), keys.map(key => col(s"target.$key") === col(s"source.$key")).reduce(_ && _))
}
