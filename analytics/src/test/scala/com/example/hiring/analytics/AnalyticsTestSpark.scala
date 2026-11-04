package com.example.hiring.analytics

import org.apache.spark.sql.classic.SparkSession

/** The one test Spark session configuration for tiny Delta tables.
  *
  * Production-semantic settings stay identical to the runtime: UTC session time zone, the vacuum retention window check
  * stays enforced, and the Delta extension and catalog are installed. The remaining settings only remove fixed overhead
  * on tiny data: two shuffle and snapshot partitions instead of 200 and 50, no UI, and a loopback driver address so a
  * changing host network address cannot break executor-to-driver class loading.
  */
private[analytics] object AnalyticsTestSpark {

  /** Settings shared by every test session; callers may add or override entries before `getOrCreate`. */
  val settings: Vector[(String, String)] = Vector(
    "spark.ui.enabled" -> "false",
    "spark.ui.showConsoleProgress" -> "false",
    "spark.driver.bindAddress" -> "127.0.0.1",
    "spark.driver.host" -> "127.0.0.1",
    "spark.sql.shuffle.partitions" -> "2",
    "spark.sql.session.timeZone" -> "UTC",
    "spark.databricks.delta.snapshotPartitions" -> "2",
    "spark.databricks.delta.vacuum.retentionWindowIgnore.enabled" -> "false",
    "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog"
  )

  def builder(appName: String, master: String = "local[2]"): SparkSession.Builder =
    settings.foldLeft(SparkSession.builder().master(master).appName(appName)) { case (builder, (key, value)) =>
      builder.config(key, value)
    }

  /** The `spark.sql.*` and `spark.databricks.delta.*` entries of `settings` that a live session accepts (the extensions
    * and catalog are static), for a session obtained through production wiring such as `SparkRuntime.sparkMongo`.
    */
  def runtimeSettings: Vector[(String, String)] = settings.filter { case (key, _) =>
    (key.startsWith("spark.sql.") || key.startsWith("spark.databricks.delta.")) &&
    key != "spark.sql.extensions" && key != "spark.sql.catalog.spark_catalog"
  }

  def applyRuntimeSettings(session: org.apache.spark.sql.SparkSession): Unit =
    runtimeSettings.foreach { case (key, value) => session.conf.set(key, value) }

  def session(appName: String): SparkSession = builder(appName).getOrCreate()
}
