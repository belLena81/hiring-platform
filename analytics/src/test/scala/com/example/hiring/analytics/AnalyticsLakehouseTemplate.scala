package com.example.hiring.analytics

import com.example.hiring.analytics.domain.SubjectPseudonymizer
import com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths

import cats.effect.unsafe.implicits.global
import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, Path, StandardCopyOption}
import scala.jdk.CollectionConverters.*

/** An empty, constrained lakehouse that a suite builds once and copies for each test.
  *
  * The template holds exactly what the first steps of every batch create: the HMAC key registry for the suite's
  * pseudonymizer and the subject-attributed tables with their declared constraints and retention properties. It is
  * built by the production initialization (`validateHmacConfiguration`, `configureRawTables`), so a copy is the state
  * the batch would otherwise reach after creating the tables and committing their constraints. Delta transaction logs
  * address data files relative to the table, so a copy of the directory is an independent table at a new path. Tests
  * that assert table creation or repair, a lakehouse before initialization, or that seed tables before the first batch
  * must not use a template.
  */
private[analytics] final class AnalyticsLakehouseTemplate private (template: Path) {

  /** A new temporary lakehouse directory that starts as a copy of the template. */
  def freshDirectory(prefix: String): Path = {
    val target = Files.createTempDirectory(prefix)
    copyTree(template, target)
    target
  }

  private def copyTree(from: Path, to: Path): Unit = {
    val entries = Files.walk(from)
    try
      entries.iterator().asScala.foreach { entry =>
        val destination = to.resolve(from.relativize(entry).toString)
        if (Files.isDirectory(entry)) Files.createDirectories(destination)
        else {
          val _ = Files.copy(entry, destination, StandardCopyOption.COPY_ATTRIBUTES)
        }
      }
    finally entries.close()
  }
}

private[analytics] object AnalyticsLakehouseTemplate {

  /** A template that `initialize` prepares once, inside an empty temporary lakehouse. */
  def build(initialize: AnalyticsLakehousePaths => Unit): AnalyticsLakehouseTemplate = {
    val directory = Files.createTempDirectory("analytics-lakehouse-template-")
    initialize(TestAnalyticsLakehousePaths.unsafe(directory.toUri.toString.stripSuffix("/")))
    new AnalyticsLakehouseTemplate(directory)
  }

  /** The state every batch reaches before it reads any data. */
  def forBatch(spark: SparkSession, pseudonymizer: SubjectPseudonymizer): AnalyticsLakehouseTemplate =
    build { paths =>
      // The batch validates key continuity (creating the HMAC key registry) before it creates the raw tables.
      AnalyticsBatchTestSupport
        .newKeyContinuityStage(paths, pseudonymizer)
        .validateHmacConfiguration(spark)
        .unsafeRunSync()
      AnalyticsBatchTestSupport
        .newMaintenance(spark, paths, pseudonymizer)
        .configureRawTables(Vector.empty)
        .unsafeRunSync()
    }
}
