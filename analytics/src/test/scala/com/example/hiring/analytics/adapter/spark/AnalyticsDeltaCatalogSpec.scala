package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.{AnalyticsTestOperationalConfig, TestAnalyticsLakehousePaths}
import com.example.hiring.analytics.domain.{AnalyticsAggregateType, AnalyticsApplicationStatus, AnalyticsEventType}
import com.example.hiring.analytics.errors.AnalyticsError

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.delta.tables.DeltaTable
import munit.FunSuite
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.*

import java.nio.file.Files
import java.sql.Timestamp
import java.time.Instant
import scala.concurrent.duration.*

/** AC-01: one catalog defines every Delta table; targets are validated strictly and constraints are enforced. */
final class AnalyticsDeltaCatalogSpec extends FunSuite {
  override val munitTimeout: FiniteDuration = 20.minutes
  private val execution = SparkBlockingExecution.forTests[IO](scala.concurrent.ExecutionContext.parasitic)

  private lazy val spark: SparkSession = org.apache.spark.sql.classic.SparkSession
    .builder()
    .master("local[2]")
    .appName("AnalyticsDeltaCatalogSpec")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.databricks.delta.vacuum.retentionWindowIgnore.enabled", "false")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  override def afterAll(): Unit =
    if (!spark.sparkContext.isStopped) spark.stop()

  private def freshPath(name: String): String =
    Files.createTempDirectory(s"analytics-catalog-$name").resolve("table").toUri.toString

  private def ts(value: String): Timestamp = Timestamp.from(Instant.parse(value))

  private def append(path: String, spec: DeltaTableSpec, rows: Row*): Unit =
    spark
      .createDataFrame(spark.sparkContext.parallelize(rows), AnalyticsTableSchemas.struct(spec.shape))
      .write
      .format("delta")
      .mode("append")
      .save(SparkPhysicalLocation.resolve(path))

  private def silverRow(
      eventId: String | Null = "11111111-1111-1111-1111-111111111111",
      eventType: String = AnalyticsEventType.ApplicationCreated.wire
  ): Row =
    Row(
      eventId,
      eventType,
      ts("2026-01-01T10:00:00Z"),
      AnalyticsAggregateType.Application.wire,
      "22222222-2222-2222-2222-222222222222",
      "22222222-2222-2222-2222-222222222222",
      "33333333-3333-3333-3333-333333333333",
      null,
      null,
      "k1_token",
      Seq("k1_token"),
      "f" * 64,
      ts("2026-01-01T10:00:00Z"),
      ts("2026-01-31T10:00:00Z")
    )

  private def bronzeRow(offset: Long): Row =
    Row(
      "hiring.operational-events",
      Int.box(0),
      Long.box(offset),
      ts("2026-01-01T10:00:00Z"),
      "11111111-1111-1111-1111-111111111111",
      AnalyticsEventType.ApplicationCreated.wire,
      ts("2026-01-01T10:00:00Z"),
      AnalyticsAggregateType.Application.wire,
      "22222222-2222-2222-2222-222222222222",
      "f" * 64,
      null,
      null,
      null,
      null,
      "k1_token",
      Seq("k1_token"),
      ts("2026-01-01T10:00:00Z"),
      ts("2026-01-31T10:00:00Z")
    )

  private def causes(error: Throwable): Vector[Throwable] =
    Iterator.iterate(error)(_.getCause).takeWhile(_ != null).toVector

  private def violation(thunk: => Unit): String = {
    val error = intercept[Throwable](thunk)
    causes(error).map(_.toString).mkString(" <- ")
  }

  private def assertDeltaRejects(thunk: => Unit, expected: String): Unit = {
    val rendered = violation(thunk)
    assert(rendered.contains("InvariantViolation"), rendered)
    assert(rendered.toLowerCase.contains(expected.toLowerCase), rendered)
  }

  /** A Silver table whose declaration differs from the catalog, as an older build would have created it. */
  private def createLooseTable(path: String, spec: DeltaTableSpec, nullable: Boolean, withChecks: Boolean): Unit = {
    val schema =
      if (nullable) StructType(spec.schema.fields.map(_.copy(nullable = true))) else spec.schema
    val builder = DeltaTable.createIfNotExists(spark).location(SparkPhysicalLocation.resolve(path)).addColumns(schema)
    val _ = spec.properties.foldLeft(builder) { case (current, (key, value)) => current.property(key, value) }.execute()
    if (withChecks)
      spec.checks.foreach(check =>
        spark.sql(
          s"ALTER TABLE ${DeltaTables.sqlIdentifier(path)} ADD CONSTRAINT ${check.name} CHECK (${check.expression})"
        )
      )
  }

  test("AC-01.1 a catalog table is created with its declared nullability, constraints and properties") {
    AnalyticsDeltaCatalog.all.foreach { spec =>
      val path = freshPath(spec.name)
      AnalyticsTableSchemas.createOrValidate(spark, path, spec)
      val metadata = DeltaLogFactory.system(spark, path).update().metadata
      assertEquals(
        metadata.schema.fields.toVector.map(field => (field.name, field.nullable)),
        spec.columns.map(column => (column.name, column.nullable)),
        spec.name
      )
      assertEquals(
        metadata.configuration.keys.filter(_.startsWith("delta.constraints.")).toSet,
        spec.checks.map(check => s"delta.constraints.${check.name}").toSet,
        spec.name
      )
      spec.properties.foreach { case (key, value) => assertEquals(metadata.configuration.get(key), Some(value)) }
      // Revalidating an existing table is idempotent.
      AnalyticsTableSchemas.createOrValidate(spark, path, spec)
    }
  }

  test("AC-01.1 an existing Silver table whose eventId is nullable fails the batch with DeltaSchemaMismatch") {
    val paths =
      TestAnalyticsLakehousePaths.unsafe(Files.createTempDirectory("analytics-catalog-nullable").toUri.toString)
    val silver = AnalyticsDeltaCatalog.Silver
    val loose = StructType(silver.schema.fields.map { field =>
      if (field.name == Columns.EventId) field.copy(nullable = true) else field
    })
    val builder =
      DeltaTable.createIfNotExists(spark).location(SparkPhysicalLocation.resolve(paths.silver)).addColumns(loose)
    silver.properties.foldLeft(builder) { case (current, (key, value)) => current.property(key, value) }.execute()
    silver.checks.foreach(check =>
      spark.sql(
        s"ALTER TABLE ${DeltaTables.sqlIdentifier(paths.silver)} ADD CONSTRAINT ${check.name} CHECK (${check.expression})"
      )
    )

    intercept[AnalyticsError.DeltaSchemaMismatch](AnalyticsTableSchemas.createOrValidate(spark, paths.silver, silver))
    val source = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(silverRow())),
      AnalyticsTableSchemas.struct(silver.shape)
    )
    intercept[AnalyticsError.DeltaSchemaMismatch] {
      new DeltaBatchWriter[IO](paths, execution)
        .merge(source, paths.silver, "target.eventId = source.eventId")
        .unsafeRunSync()
    }
  }

  test("AC-01.1 a table missing a declared constraint or holding an extra one is rejected") {
    val silver = AnalyticsDeltaCatalog.Silver
    val missing = freshPath("missing-constraint")
    createLooseTable(missing, silver, nullable = false, withChecks = false)
    append(missing, silver, silverRow())
    intercept[AnalyticsError.DeltaSchemaMismatch](AnalyticsTableSchemas.createOrValidate(spark, missing, silver))

    val extra = freshPath("extra-constraint")
    AnalyticsTableSchemas.createOrValidate(spark, extra, silver)
    spark.sql(s"ALTER TABLE ${DeltaTables.sqlIdentifier(extra)} ADD CONSTRAINT extra_rule CHECK (`aggregateId` <> '')")
    intercept[AnalyticsError.DeltaSchemaMismatch](AnalyticsTableSchemas.createOrValidate(spark, extra, silver))

    val changed = freshPath("changed-constraint")
    createLooseTable(changed, silver.copy(checks = silver.checks.drop(1)), nullable = false, withChecks = true)
    spark.sql(s"ALTER TABLE ${DeltaTables.sqlIdentifier(changed)} ADD CONSTRAINT event_type_valid CHECK (true)")
    intercept[AnalyticsError.DeltaSchemaMismatch](AnalyticsTableSchemas.createOrValidate(spark, changed, silver))
  }

  test("AC-01.1 source frames are compared by names and types only") {
    val paths = TestAnalyticsLakehousePaths.unsafe(Files.createTempDirectory("analytics-catalog-source").toUri.toString)
    val silver = AnalyticsDeltaCatalog.Silver
    val nullableSource = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(silverRow())),
      AnalyticsTableSchemas.struct(silver.shape)
    )
    assert(nullableSource.schema.fields.forall(_.nullable))
    new DeltaBatchWriter[IO](paths, execution)
      .merge(nullableSource, paths.silver, "target.eventId = source.eventId")
      .unsafeRunSync()
    assertEquals(spark.read.format("delta").load(paths.silver).count(), 1L)

    val retyped = nullableSource.withColumn(Columns.EventId, nullableSource(Columns.EventId).cast(IntegerType))
    intercept[AnalyticsError.DeltaSchemaMismatch] {
      new DeltaBatchWriter[IO](paths, execution)
        .merge(retyped, paths.silver, "target.eventId = source.eventId")
        .unsafeRunSync()
    }
  }

  test("AC-01.2 the event and aggregate type constraints are derived from the domain enums") {
    def expression(spec: DeltaTableSpec, name: String): String = spec.checks.find(_.name == name).get.expression
    val eventTypes = AnalyticsEventType.values.toVector.map(_.wire)
    val aggregateTypes = AnalyticsAggregateType.values.toVector.map(_.wire)
    Vector(AnalyticsDeltaCatalog.Silver, AnalyticsDeltaCatalog.Bronze, AnalyticsDeltaCatalog.LateFacts).foreach {
      spec =>
        assertEquals(
          "'([A-Z_]+)'".r.findAllMatchIn(expression(spec, "event_type_valid")).map(_.group(1)).toVector,
          eventTypes
        )
        assertEquals(
          "'([A-Za-z]+)'".r.findAllMatchIn(expression(spec, "aggregate_type_valid")).map(_.group(1)).toVector,
          aggregateTypes
        )
        val statuses = "'([A-Za-z]+)'".r.findAllMatchIn(expression(spec, "new_status_valid")).map(_.group(1)).toVector
        assertEquals(statuses, "Created" +: AnalyticsApplicationStatus.values.toVector.map(_.wire))
    }
  }

  test("AC-01.2 a Silver row with eventType BOGUS is rejected by Delta, a valid row is accepted") {
    val path = freshPath("bogus")
    AnalyticsTableSchemas.createOrValidate(spark, path, AnalyticsDeltaCatalog.Silver)
    append(path, AnalyticsDeltaCatalog.Silver, silverRow())
    assertDeltaRejects(append(path, AnalyticsDeltaCatalog.Silver, silverRow(eventType = "BOGUS")), "event_type_valid")
    assertEquals(spark.read.format("delta").load(SparkPhysicalLocation.resolve(path)).count(), 1L)
  }

  test("AC-01.2 a Silver row with a null eventId is rejected by Delta") {
    val path = freshPath("null-event-id")
    AnalyticsTableSchemas.createOrValidate(spark, path, AnalyticsDeltaCatalog.Silver)
    assertDeltaRejects(append(path, AnalyticsDeltaCatalog.Silver, silverRow(eventId = null)), "eventId")
  }

  test("AC-01.2 a Bronze row with a negative offset is rejected by Delta") {
    val path = freshPath("negative-offset")
    AnalyticsTableSchemas.createOrValidate(spark, path, AnalyticsDeltaCatalog.Bronze)
    append(path, AnalyticsDeltaCatalog.Bronze, bronzeRow(0L))
    assertDeltaRejects(append(path, AnalyticsDeltaCatalog.Bronze, bronzeRow(-1L)), "offset_non_negative")
  }

  test("AC-01.2 Gold counts and percentile order are enforced by Delta") {
    val funnel = freshPath("funnel")
    AnalyticsTableSchemas.createOrValidate(spark, funnel, AnalyticsDeltaCatalog.FunnelGold)
    assertDeltaRejects(
      append(
        funnel,
        AnalyticsDeltaCatalog.FunnelGold,
        Row(java.sql.Date.valueOf("2026-01-01"), 1L, -1L, 0L, 0L, 0L, 0L)
      ),
      "accepted_non_negative"
    )
    val timeToHire = freshPath("time-to-hire")
    AnalyticsTableSchemas.createOrValidate(spark, timeToHire, AnalyticsDeltaCatalog.TimeToHireGold)
    append(timeToHire, AnalyticsDeltaCatalog.TimeToHireGold, Row(1.0, 2.0, 3.0, 4.0, 10L, 0L))
    assertDeltaRejects(
      append(timeToHire, AnalyticsDeltaCatalog.TimeToHireGold, Row(5.0, 2.0, 3.0, 4.0, 10L, 0L)),
      "percentiles_ordered"
    )
    assertDeltaRejects(
      append(timeToHire, AnalyticsDeltaCatalog.TimeToHireGold, Row(1.0, 2.0, 3.0, 4.0, -1L, 0L)),
      "eligiblecount_non_negative"
    )
  }

  test("AC-01.2 the catalog covers every Delta table in the storage inventory with the same classification") {
    val paths = TestAnalyticsLakehousePaths.unsafe("file:///tmp/hiring-catalog")
    val byLocation = AnalyticsDeltaCatalog.locations(paths).toMap
    assertEquals(byLocation.keySet, paths.inventory.delta.map(_.location).toSet)
    paths.inventory.delta.foreach { surface =>
      assertEquals(byLocation(surface.location).subjectAttributed, surface.subjectAttributed, surface.location)
    }
    assertEquals(byLocation.values.map(_.name).toSet.size, byLocation.size)
  }

  private def silverEvents(): DataFrame = {
    val rows = (1 to 10).toVector.flatMap { index =>
      val token = s"token-$index"
      val application = s"application-$index"
      def event(id: String, eventType: String, status: String | Null, at: String) = Row(
        id,
        eventType,
        ts(at),
        "Application",
        application,
        application,
        s"job-$index",
        status,
        null,
        token,
        Seq(token),
        s"fingerprint-$id"
      )
      Vector(
        event(s"created-$index", AnalyticsEventType.ApplicationCreated.wire, null, "2026-01-01T23:30:00Z"),
        event(
          s"hired-$index",
          AnalyticsEventType.ApplicationStatusChanged.wire,
          AnalyticsApplicationStatus.Hired.wire,
          "2026-01-02T00:30:00Z"
        )
      )
    }
    spark.createDataFrame(
      spark.sparkContext.parallelize(rows),
      AnalyticsTableSchemas.struct(AnalyticsTableSchemas.silver)
    )
  }

  test("AC-01.3 Gold day is a DateType and extraction yields the UTC start-of-day instant") {
    val paths = TestAnalyticsLakehousePaths.unsafe(Files.createTempDirectory("analytics-catalog-gold").toUri.toString)
    AnalyticsGoldStage.rebuild[IO](paths, silverEvents(), execution).unsafeRunSync()

    assertEquals(
      DeltaTables.read(spark, paths.funnelGold).schema(Columns.Day).dataType,
      DateType
    )
    assertEquals(DeltaTables.read(spark, paths.skillsGold).schema(Columns.Day).dataType, DateType)
    val report = AnalyticsGoldStage.extract[IO](spark, paths, Instant.EPOCH, execution).unsafeRunSync()
    assertEquals(
      report.funnel.map(_.day),
      Vector(Instant.parse("2026-01-01T00:00:00Z"), Instant.parse("2026-01-02T00:00:00Z"))
    )
    assertEquals(report.funnel.map(_.created), Vector(10L, 0L))
    assertEquals(report.funnel.map(_.hired), Vector(0L, 10L))
    assertEquals(report.timeToHire.map(_.eligibleCount), Some(10L))
  }

  test("AC-01.3 a Gold table whose schema drifted is not overwritten and the rebuild fails") {
    val paths = TestAnalyticsLakehousePaths.unsafe(Files.createTempDirectory("analytics-catalog-drift").toUri.toString)
    val oldShape = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(ts("2026-01-01T00:00:00Z"), 1L, 0L, 0L, 0L, 0L, 0L))),
      StructType.fromDDL(
        "day TIMESTAMP, created BIGINT, accepted BIGINT, declined BIGINT, interview BIGINT, hired BIGINT, rejected BIGINT"
      )
    )
    oldShape.write.format("delta").save(SparkPhysicalLocation.resolve(paths.funnelGold))

    intercept[AnalyticsError.DeltaSchemaMismatch] {
      AnalyticsGoldStage.rebuild[IO](paths, silverEvents(), execution).unsafeRunSync()
    }
    assertEquals(DeltaTables.read(spark, paths.funnelGold).schema(Columns.Day).dataType, TimestampType)
    assertEquals(DeltaTables.read(spark, paths.funnelGold).count(), 1L)
  }

  test("AC-01.3 extraction fails with InvalidGoldSchema on a null cell") {
    val paths = TestAnalyticsLakehousePaths.unsafe(Files.createTempDirectory("analytics-catalog-null").toUri.toString)
    val loose = StructType.fromDDL("day DATE, skill STRING, postings BIGINT")
    spark
      .createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(java.sql.Date.valueOf("2026-01-01"), "scala", null))),
        loose
      )
      .write
      .format("delta")
      .save(SparkPhysicalLocation.resolve(paths.skillsGold))
    val result = AnalyticsGoldStage.extract[IO](spark, paths, Instant.EPOCH, execution).attempt.unsafeRunSync()
    assertEquals(result.swap.toOption, Some(AnalyticsError.InvalidGoldSchema))
  }

  test("AC-01.1 an interrupted creation without constraints is completed on reopen when the table is empty") {
    val silver = AnalyticsDeltaCatalog.Silver
    val path = freshPath("interrupted-creation")
    createLooseTable(path, silver, nullable = false, withChecks = false)
    assertEquals(
      DeltaLogFactory
        .system(spark, path)
        .update()
        .metadata
        .configuration
        .keys
        .count(_.startsWith("delta.constraints.")),
      0
    )

    AnalyticsTableSchemas.createOrValidate(spark, path, silver)
    assertEquals(
      DeltaLogFactory
        .system(spark, path)
        .update()
        .metadata
        .configuration
        .keys
        .filter(_.startsWith("delta.constraints."))
        .toSet,
      silver.checks.map(check => s"delta.constraints.${check.name}").toSet
    )
    assertDeltaRejects(append(path, silver, silverRow(eventType = "BOGUS")), "event_type_valid")
  }

  test("AC-01.1 a partially constrained empty table is completed but a different stored constraint is not") {
    val silver = AnalyticsDeltaCatalog.Silver
    val partial = freshPath("partial-constraints")
    createLooseTable(partial, silver.copy(checks = silver.checks.take(2)), nullable = false, withChecks = true)
    AnalyticsTableSchemas.createOrValidate(spark, partial, silver)

    val nullable = freshPath("interrupted-nullable")
    createLooseTable(nullable, silver, nullable = true, withChecks = false)
    intercept[AnalyticsError.DeltaSchemaMismatch](AnalyticsTableSchemas.createOrValidate(spark, nullable, silver))
  }

  test("AC-01.1 nested array nullability is compared structurally") {
    val progress = AnalyticsDeltaCatalog.StreamingProgress
    val path = freshPath("nested-nullability")
    val schema = StructType(progress.schema.fields.map { field =>
      field.dataType match {
        case ArrayType(element, _) if field.name == Columns.SourceEndOffsets =>
          field.copy(dataType = ArrayType(element, containsNull = false))
        case _ => field
      }
    })
    val builder = DeltaTable.createIfNotExists(spark).location(SparkPhysicalLocation.resolve(path)).addColumns(schema)
    progress.properties.foldLeft(builder) { case (current, (key, value)) => current.property(key, value) }.execute()
    intercept[AnalyticsError.DeltaSchemaMismatch](AnalyticsTableSchemas.createOrValidate(spark, path, progress))
  }

  test("AC-01.1 the expired-file vacuum rewrite keeps NOT NULL and the constraints") {
    val paths = TestAnalyticsLakehousePaths.unsafe(Files.createTempDirectory("analytics-catalog-vacuum").toUri.toString)
    val silver = AnalyticsDeltaCatalog.Silver
    AnalyticsTableSchemas.createOrValidate(spark, paths.silver, silver)
    append(paths.silver, silver, silverRow())
    val retention = new AnalyticsDeltaRetention[IO](
      paths,
      AnalyticsTestOperationalConfig.operational,
      execution,
      org.typelevel.log4cats.slf4j.Slf4jLogger.getLogger[IO]
    )
    retention.configureRawTablePrivacy(spark).unsafeRunSync()
    retention.vacuumExpiredFiles(spark).unsafeRunSync()

    AnalyticsTableSchemas.createOrValidate(spark, paths.silver, silver)
    assertEquals(spark.read.format("delta").load(SparkPhysicalLocation.resolve(paths.silver)).count(), 1L)
    assertDeltaRejects(append(paths.silver, silver, silverRow(eventId = null)), "eventId")
    assertDeltaRejects(append(paths.silver, silver, silverRow(eventType = "BOGUS")), "event_type_valid")
  }
}
