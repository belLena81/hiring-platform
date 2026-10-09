package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.AnalyticsTestClocks

import cats.effect.{Deferred, IO}
import cats.effect.unsafe.implicits.global
import com.example.hiring.analytics.{AnalyticsTestSubjectPseudonymizer, TestAnalyticsLakehousePaths}
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.*
import io.delta.tables.DeltaTable
import munit.FunSuite
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit}

import java.nio.file.Files
import java.sql.Timestamp
import java.time.{Clock, Instant, ZoneId, ZoneOffset}
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

final class HiringAnalyticsLateFactReplayStagesSpec extends FunSuite {
  override val munitTimeout: FiniteDuration = 5.minutes
  private val execution = SparkBlockingExecution.forTests[IO](scala.concurrent.ExecutionContext.parasitic)
  private val Now = Instant.parse("2026-10-01T12:00:00Z")
  private val OriginalIngested = Now.minusSeconds(25L * 86400L)
  private val OriginalExpiry = Now.plusSeconds(5L * 86400L)
  private val pseudonymizer = AnalyticsTestSubjectPseudonymizer.fromSecret(Array.fill[Byte](32)(7))

  private lazy val spark: SparkSession = org.apache.spark.sql.classic.SparkSession
    .builder()
    .master("local[2]")
    .appName("HiringAnalyticsLateFactReplayStagesSpec")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  override def afterAll(): Unit = if (!spark.sparkContext.isStopped) spark.stop()

  test("coordinate-selected replay preserves original expiry, merges once, and uses shared suppressed Gold formulas") {
    val paths = TestAnalyticsLakehousePaths.unsafe(Files.createTempDirectory("hiring-late-replay").toUri.toString)
    val writer = new DeltaBatchWriter[IO](paths, execution)
    seed(paths, writer)
    val stages = stage(paths, writer)
    val request = selected("explicit-ten", (0 until 10).toVector)
    stages.validateSelectedFacts(request, Vector.empty, Now).unsafeRunSync()
    stages.mergeSelectedFacts(request, Vector.empty, Now).unsafeRunSync()
    stages.mergeSelectedFacts(request, Vector.empty, Now.plusSeconds(60)).unsafeRunSync()
    val silver = spark.read.format("delta").load(paths.silver)
    assertEquals(silver.count(), 10L)
    assertEquals(silver.select(Columns.IngestedAt).distinct().head().getTimestamp(0).toInstant, OriginalIngested)
    assertEquals(silver.select(Columns.ExpiresAt).distinct().head().getTimestamp(0).toInstant, OriginalExpiry)
    assert(!silver.columns.contains("rawValue"))
    assertEquals(spark.read.format("delta").load(paths.lateFacts).count(), 11L)
    val report = stages.rebuildGoldAndExtractReport(Now).unsafeRunSync()
    assertEquals(report.funnel.size, 1)
    assertEquals(report.funnel.head.created, 10L)
    assert(!DeltaTable.isDeltaTable(spark, paths.streamingProgress))
    assert(!DeltaTable.isDeltaTable(spark, paths.streamingDecisions))
  }

  test("missing, expired, deleted, conflicting, and malformed selected facts fail before Silver writes") {
    val paths =
      TestAnalyticsLakehousePaths.unsafe(Files.createTempDirectory("hiring-late-replay-reject").toUri.toString)
    val writer = new DeltaBatchWriter[IO](paths, execution)
    seed(paths, writer)
    val stages = stage(paths, writer)
    def rejected(
        request: AnalyticsLateFactReplayRequest,
        tokens: Vector[SubjectToken] = Vector.empty,
        at: Instant = Now
    ): Unit = {
      val result = stages.mergeSelectedFacts(request, tokens, at).attempt.unsafeRunSync()
      assertEquals(result.left.toOption, Some(AnalyticsError.LateFactReplayRejected))
      assert(!DeltaTable.isDeltaTable(spark, paths.silver))
    }
    rejected(selected("missing", Vector(100)))
    rejected(selected("expired", Vector(0)), at = OriginalExpiry)
    rejected(
      selected("deleted", Vector(0)),
      Vector(AnalyticsTestSubjectPseudonymizer.token(pseudonymizer, "candidate-0"))
    )
    val conflict = lateRows()
      .filter(col(Columns.Offset) === lit(0L))
      .withColumn(Columns.Offset, lit(20L))
      .withColumn(Columns.EventFingerprint, lit("a" * 64))
    writer
      .merge(
        conflict,
        paths.lateFacts,
        "target.topic = source.topic AND target.partition = source.partition AND target.offset = source.offset"
      )
      .unsafeRunSync()
    rejected(selected("conflict", Vector(0)))
    DeltaTable.forPath(spark, paths.lateFacts).delete(col(Columns.Offset) === lit(20L))
    DeltaTable.forPath(spark, paths.lateFacts).updateExpr("offset = 0", Map(Columns.EventFingerprint -> "'malformed'"))
    rejected(selected("malformed", Vector(0)))
  }

  test("valid selected facts support Spark mutable array carriers at the row boundary") {
    val paths = TestAnalyticsLakehousePaths.unsafe(
      Files.createTempDirectory("hiring-late-replay-array-carrier").toUri.toString
    )
    val writer = new DeltaBatchWriter[IO](paths, execution)
    seed(paths, writer)
    val stored = spark.read
      .format("delta")
      .load(paths.lateFacts)
      .filter(col(Columns.Offset) === lit(0L))
      .head()
    val tokens = stored.getAs[scala.collection.Seq[String]](Columns.SubjectTokens)
    assert(
      tokens.isInstanceOf[scala.collection.mutable.ArraySeq[?]],
      "the physical Spark row must exercise its mutable array carrier"
    )
    assertEquals(tokens.toVector, Vector(stored.getAs[String](Columns.SubjectToken)))

    val stages = stage(paths, writer)
    val request = selected("mutable-array-carrier", Vector(0))
    stages.validateSelectedFacts(request, Vector.empty, Now).unsafeRunSync()
    stages.mergeSelectedFacts(request, Vector.empty, Now).unsafeRunSync()
    val silver = spark.read.format("delta").load(paths.silver)
    assertEquals(silver.count(), 1L)
    assertEquals(silver.select(Columns.ExpiresAt).head().getTimestamp(0).toInstant, OriginalExpiry)
  }

  test("conflicting existing Silver event identity rejects replay without overwriting existing facts") {
    val paths =
      TestAnalyticsLakehousePaths.unsafe(Files.createTempDirectory("hiring-late-replay-silver-conflict").toUri.toString)
    val writer = new DeltaBatchWriter[IO](paths, execution)
    seed(paths, writer)
    val silver = lateRows()
      .filter(col(Columns.Offset) === lit(0L))
      .select((AnalyticsTableSchemas.silver ++ AnalyticsTableSchemas.expiry).map { case (name, _) => col(name) }*)
      .withColumn(Columns.EventFingerprint, lit("b" * 64))
    writer.merge(silver, paths.silver, "target.eventId = source.eventId").unsafeRunSync()
    val result = stage(paths, writer)
      .mergeSelectedFacts(selected("silver-conflict", Vector(0)), Vector.empty, Now)
      .attempt
      .unsafeRunSync()
    assertEquals(result.left.toOption, Some(AnalyticsError.LateFactReplayRejected))
    assertEquals(spark.read.format("delta").load(paths.silver).count(), 1L)
    assertEquals(
      spark.read.format("delta").load(paths.silver).select(Columns.EventFingerprint).head().getString(0),
      "b" * 64
    )
  }

  test("facts expiring while the sink waits for its serialized executor cannot be merged") {
    val paths =
      TestAnalyticsLakehousePaths.unsafe(Files.createTempDirectory("hiring-late-replay-expired-sink").toUri.toString)
    val normalWriter = new DeltaBatchWriter[IO](paths, execution)
    seed(paths, normalWriter)
    val clockValue = new AtomicReference[Instant](Now)
    val clock = new Clock {
      override def getZone: ZoneId = ZoneOffset.UTC
      override def withZone(zone: ZoneId): Clock = this
      override def instant(): Instant = clockValue.get()
    }
    (for {
      queued <- Deferred[IO, Unit]
      release <- Deferred[IO, Unit]
      delayed = new SparkExecution[IO] {
        override def apply[A](work: => A): IO[A] = queued.complete(()).void *> release.get *> execution(work)
        override def either[A](work: => Either[AnalyticsError, A]): IO[A] =
          queued.complete(()).void *> release.get *> execution.either(work)
      }
      writer = new DeltaBatchWriter[IO](paths, delayed)
      running <- stage(paths, writer, clock)
        .mergeSelectedFacts(selected("expires-at-sink", Vector(0)), Vector.empty, Now)
        .attempt
        .start
      _ <- queued.get
      _ <- IO(clockValue.set(OriginalExpiry))
      _ <- release.complete(())
      result <- running.joinWithNever
      _ <- IO(assertEquals(result.left.toOption, Some(AnalyticsError.LateFactReplayRejected)))
    } yield ()).unsafeRunSync()
    assertEquals(spark.read.format("delta").load(paths.silver).count(), 0L)
  }

  private def stage(
      paths: AnalyticsLakehousePaths,
      writer: DeltaWriter[IO],
      mergeClock: Clock = Clock.fixed(Now, ZoneOffset.UTC)
  ): SparkAnalyticsLateFactReplayStages[IO] = {
    val maintenance = new AnalyticsBatchMaintenance[IO] {
      override def validateHmacConfigurationLocked: IO[Unit] = IO.unit
      override def configureRawTables: IO[Unit] = IO.unit
      override def applyActiveDeletions(tokens: Vector[SubjectToken]): IO[Unit] = IO.unit
      override def expireStored(at: Instant): IO[Unit] = IO.unit
    }
    new SparkAnalyticsLateFactReplayStages[IO](
      spark,
      paths,
      execution,
      new DeltaBatchReader[IO](execution),
      writer,
      maintenance,
      AnalyticsTestClocks.fixed(Now),
      mergeClock
    )
  }

  private def seed(paths: AnalyticsLakehousePaths, writer: DeltaWriter[IO]): Unit =
    writer
      .merge(
        lateRows(),
        paths.lateFacts,
        "target.topic = source.topic AND target.partition = source.partition AND target.offset = source.offset"
      )
      .unsafeRunSync()

  private def selected(id: String, offsets: Vector[Int]): AnalyticsLateFactReplayRequest =
    AnalyticsLateFactReplayRequest.from(id, offsets.map(value => ("hiring-events", 0, value.toLong))).toOption.get

  private def lateRows(): DataFrame = {
    val rows = (0 until 11).map { number =>
      val token = AnalyticsTestSubjectPseudonymizer.tokenValue(pseudonymizer, s"candidate-$number")
      val event = s"event-$number"
      Row(
        event,
        AnalyticsDigest.sha256Hex(event.getBytes(java.nio.charset.StandardCharsets.UTF_8)),
        AnalyticsEventType.ApplicationCreated.wire,
        "hiring-events",
        0,
        number.toLong,
        Timestamp.from(OriginalIngested.minusSeconds(86400L)),
        "Application",
        s"application-$number",
        s"application-$number",
        "job-1",
        "Created",
        Vector("Scala"),
        token,
        Vector(token),
        "CLOSED_DAY",
        Timestamp.from(OriginalIngested),
        Timestamp.from(OriginalExpiry)
      )
    }
    spark.createDataFrame(rows.asJava, AnalyticsTableSchemas.struct(AnalyticsTableSchemas.lateFacts))
  }
}
