package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.{
  AnalyticsBatchTestSupport,
  AnalyticsTestOperationalConfig,
  AnalyticsTestSubjectPseudonymizer,
  TestAnalyticsLakehousePaths
}
import com.example.hiring.analytics.domain.{
  AnalyticsAggregateType,
  AnalyticsApplicationStatus,
  AnalyticsEventType,
  AnalyticsHiringStageMetric
}
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import munit.FunSuite
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.*

import java.nio.file.Files
import java.sql.Timestamp
import java.time.{Duration, Instant}
import scala.concurrent.duration.*

/** AC-03: application stage times, their merges, backfill, retention, erasure and the Gold durations. */
final class ApplicationStageTimesSpec extends FunSuite {
  override val munitTimeout: FiniteDuration = 20.minutes
  private val execution = SparkBlockingExecution.forTests[IO](scala.concurrent.ExecutionContext.parasitic)
  private val retention = AnalyticsTestOperationalConfig.operational.retention

  private lazy val spark: SparkSession = org.apache.spark.sql.classic.SparkSession
    .builder()
    .master("local[2]")
    .appName("ApplicationStageTimesSpec")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.databricks.delta.vacuum.retentionWindowIgnore.enabled", "false")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  override def afterAll(): Unit =
    if (!spark.sparkContext.isStopped) spark.stop()

  private val Day0 = Instant.parse("2026-01-01T00:00:00Z")
  private def day(n: Int): Instant = Day0.plus(Duration.ofDays(n.toLong))

  private val Created = AnalyticsEventType.ApplicationCreated.wire
  private val Changed = AnalyticsEventType.ApplicationStatusChanged.wire
  private val Accepted = AnalyticsApplicationStatus.Accepted.wire
  private val Interview = AnalyticsApplicationStatus.Interview.wire
  private val Hired = AnalyticsApplicationStatus.Hired.wire
  private val Rejected = AnalyticsApplicationStatus.Rejected.wire

  private lazy val noMarkers: DataFrame = AnalyticsTableSchemas.markerFrame(spark, Vector.empty)

  private def freshPaths(name: String): AnalyticsLakehousePaths =
    TestAnalyticsLakehousePaths.unsafe(Files.createTempDirectory(s"stage-times-$name").toUri.toString)

  /** A Silver-shaped lifecycle fact. Every lifecycle event carries the candidate; status changes also an actor. */
  private def event(
      application: String,
      status: Option[String],
      at: Instant,
      candidate: String = null,
      actor: Option[String] = None
  ): Row = {
    val candidateToken = Option(candidate).getOrElse(s"token-$application")
    val tokens = candidateToken +: actor.toVector
    val id = s"$application-${status.getOrElse("created")}-$at"
    Row(
      id,
      if (status.isEmpty) Created else Changed,
      Timestamp.from(at),
      AnalyticsAggregateType.Application.wire,
      application,
      application,
      "job-1",
      status.orNull,
      null,
      candidateToken,
      tokens,
      s"fingerprint-$id",
      Timestamp.from(at),
      Timestamp.from(at.plus(Duration.ofDays(30)))
    )
  }

  private def facts(rows: Seq[Row]): DataFrame =
    spark.createDataFrame(
      spark.sparkContext.parallelize(rows),
      AnalyticsTableSchemas.struct(AnalyticsTableSchemas.silver ++ AnalyticsTableSchemas.expiry)
    )

  private def merge(paths: AnalyticsLakehousePaths, rows: Row*): Unit =
    new DeltaBatchWriter[IO](paths, execution).mergeStageTimes(facts(rows), retention, None).unsafeRunSync()

  private def stored(paths: AnalyticsLakehousePaths): Vector[Row] =
    DeltaTables.read(spark, paths.applicationStageTimes).collect().toVector

  private def storedRow(paths: AnalyticsLakehousePaths, application: String): Row =
    stored(paths).filter(_.getAs[String](Columns.ApplicationId) == application) match {
      case Vector(row) => row
      case other       => fail(s"expected one row for $application but found ${other.size}")
    }

  private def instant(row: Row, column: String): Option[Instant] =
    Option(row.getAs[Timestamp](column)).map(_.toInstant)

  private def lastMergeMetrics(paths: AnalyticsLakehousePaths): Map[String, String] =
    DeltaTables
      .forPath(spark, paths.applicationStageTimes)
      .history(1)
      .select("operationMetrics")
      .head()
      .getMap[String, String](0)
      .toMap

  /** Ten applications of distinct candidates, each with the given stages at the given days after day 0. */
  private def cohort(count: Int, prefix: String, stages: Seq[(Option[String], Int)]): Seq[Row] =
    (1 to count).flatMap { index =>
      stages.map { case (status, at) => event(s"$prefix-$index", status, day(at)) }
    }

  private def rebuildAndExtract(paths: AnalyticsLakehousePaths, asOf: Instant) = {
    AnalyticsGoldStage
      .rebuild[IO](spark, paths, None, Some(DeltaTables.read(spark, paths.applicationStageTimes)), asOf, execution)
      .unsafeRunSync()
    AnalyticsGoldStage.extract[IO](spark, paths, asOf, execution).unsafeRunSync()
  }

  test("AC-03.1 a hire after 45 days counts although Silver keeps only 30 days") {
    val paths = freshPaths("long-hire")
    merge(paths, cohort(10, "long", Seq(None -> 0, Some(Hired) -> 45))*)
    val report = rebuildAndExtract(paths, day(46))
    assertEquals(report.timeToHire.map(_.p50Hours), Some(1080.0))
    assertEquals(report.timeToHire.map(_.eligibleCount), Some(10L))
    val hire = report.stageDurations.find(_.metric == AnalyticsHiringStageMetric.TimeToHire)
    assertEquals(hire.map(_.p50Hours), Some(1080.0))
    assertEquals(hire.map(_.eligibleCount), Some(10L))
    // Terminal retention keeps the row 30 days after the hire (day 75), and Gold ignores rows past that.
    assertEquals(rebuildAndExtract(paths, day(76)).timeToHire, None)
    assertEquals(rebuildAndExtract(paths, day(76)).stageDurations, Vector.empty)
  }

  test("AC-03.2 the merge is idempotent, keeps the earliest time and pins the first candidate token") {
    val paths = freshPaths("idempotent")
    val created = event("app-a", None, day(0))
    val interview = event("app-a", Some(Interview), day(3), actor = Some("token-recruiter-1"))
    merge(paths, created, interview)
    merge(paths, created, interview)
    val metrics = lastMergeMetrics(paths)
    assertEquals(metrics("numTargetRowsUpdated"), "0", "an unchanged replay is not rewritten")
    assertEquals(metrics("numTargetRowsInserted"), "0")

    merge(paths, event("app-a", Some(Interview), day(2), candidate = "token-other", actor = Some("token-recruiter-2")))
    val row = storedRow(paths, "app-a")
    assertEquals(stored(paths).size, 1)
    assertEquals(instant(row, Columns.InterviewAt), Some(day(2)))
    assertEquals(instant(row, Columns.CreatedAt), Some(day(0)))
    assertEquals(row.getAs[String](Columns.SubjectToken), "token-app-a")
    assertEquals(
      row.getAs[scala.collection.Seq[String]](Columns.SubjectTokens).toSet,
      Set("token-app-a", "token-other", "token-recruiter-1", "token-recruiter-2")
    )
    // A later duplicate never moves a stage forward.
    merge(paths, event("app-a", Some(Interview), day(9)))
    assertEquals(instant(storedRow(paths, "app-a"), Columns.InterviewAt), Some(day(2)))
  }

  test("AC-03.2 out-of-order delivery gives the same row as in-order delivery") {
    val ordered = freshPaths("ordered")
    val shuffled = freshPaths("shuffled")
    val rows = Seq(
      event("app-b", None, day(0)),
      event("app-b", Some(Accepted), day(1)),
      event("app-b", Some(Interview), day(4)),
      event("app-b", Some(Hired), day(7))
    )
    rows.foreach(merge(ordered, _))
    rows.reverse.foreach(merge(shuffled, _))
    def comparable(paths: AnalyticsLakehousePaths) = {
      val row = storedRow(paths, "app-b")
      (
        StageColumnsOf(row),
        instant(row, Columns.LastEventAt),
        instant(row, Columns.TerminalAt),
        instant(row, Columns.ExpiresAt)
      )
    }
    assertEquals(comparable(shuffled), comparable(ordered))
    assertEquals(instant(storedRow(ordered, "app-b"), Columns.LastEventAt), Some(day(7)))
  }

  private def StageColumnsOf(row: Row): Vector[Option[Instant]] =
    ApplicationStageTimes.StageColumns.map(instant(row, _))

  test("AC-03.3 a terminal application expires 30 days after it ended and an open one 180 days after its last event") {
    val paths = freshPaths("expiry")
    merge(
      paths,
      event("rejected", None, day(0)),
      event("rejected", Some(Rejected), day(10)),
      event("open", None, day(0)),
      event("open", Some(Accepted), day(5))
    )
    val rejected = storedRow(paths, "rejected")
    assertEquals(instant(rejected, Columns.TerminalAt), Some(day(10)))
    assertEquals(instant(rejected, Columns.ExpiresAt), Some(day(40)))
    val open = storedRow(paths, "open")
    assertEquals(instant(open, Columns.TerminalAt), None)
    assertEquals(instant(open, Columns.ExpiresAt), Some(day(185)))
    // A terminal event arriving later turns the open row into a terminal row.
    merge(paths, event("open", Some(Hired), day(20)))
    assertEquals(instant(storedRow(paths, "open"), Columns.ExpiresAt), Some(day(50)))
    // An earlier duplicate terminal event moves the expiry earlier.
    merge(paths, event("open", Some(Hired), day(12)))
    assertEquals(instant(storedRow(paths, "open"), Columns.ExpiresAt), Some(day(42)))
  }

  test("AC-03.4 an erasure marker removes the candidate's row and, for a recruiter, every application they acted on") {
    val paths = freshPaths("erasure")
    merge(
      paths,
      event("app-a", None, day(0), candidate = "token-candidate-a"),
      event("app-a", Some(Accepted), day(1), candidate = "token-candidate-a", actor = Some("token-recruiter")),
      event("app-b", None, day(0), candidate = "token-candidate-b"),
      event("app-c", None, day(0), candidate = "token-candidate-c")
    )
    val erasure = new AnalyticsBatchErasureStage[IO](paths, execution, _ => IO.unit, 1000)
    def marker(token: String): DataFrame =
      spark.createDataFrame(
        java.util.List.of(Row(token)),
        StructType(Seq(StructField(Columns.SubjectToken, StringType, nullable = false)))
      )
    erasure.purgeMarkedSubjectRows(spark, paths.applicationStageTimes, marker("token-candidate-b")).unsafeRunSync()
    assertEquals(stored(paths).map(_.getAs[String](Columns.ApplicationId)).toSet, Set("app-a", "app-c"))
    erasure.purgeMarkedSubjectRows(spark, paths.applicationStageTimes, marker("token-recruiter")).unsafeRunSync()
    assertEquals(stored(paths).map(_.getAs[String](Columns.ApplicationId)), Vector("app-c"))
    erasure.verifyMarkedSubjectsAbsent(spark, marker("token-recruiter")).unsafeRunSync()
    assert(paths.inventory.subjectDelta.exists(_.location == paths.applicationStageTimes))
  }

  test("AC-03.5 a metric with 9 eligible subjects is absent and with 10 it is present") {
    val paths = freshPaths("suppression")
    val interviewed = Seq(None -> 0, Some(Interview) -> 2, Some(Hired) -> 5)
    merge(paths, cohort(9, "full", interviewed)*)
    merge(paths, cohort(1, "direct", Seq(None -> 0, Some(Hired) -> 4))*)
    val nine = rebuildAndExtract(paths, day(10))
    assert(!nine.stageDurations.exists(_.metric == AnalyticsHiringStageMetric.InterviewToHire))
    assert(nine.stageDurations.exists(_.metric == AnalyticsHiringStageMetric.TimeToHire))
    assert(!nine.stageDurations.exists(_.metric == AnalyticsHiringStageMetric.TimeToInterview))

    merge(paths, event("direct-1", Some(Interview), day(1)))
    val ten = rebuildAndExtract(paths, day(10))
    val interviewToHire = ten.stageDurations.find(_.metric == AnalyticsHiringStageMetric.InterviewToHire)
    assertEquals(interviewToHire.map(_.eligibleCount), Some(10L))
    assertEquals(
      ten.stageDurations.map(_.metric),
      Vector(
        AnalyticsHiringStageMetric.TimeToInterview,
        AnalyticsHiringStageMetric.TimeToHire,
        AnalyticsHiringStageMetric.InterviewToHire
      )
    )
  }

  /** The real Silver stage with a Silver writer that fails, as when the process stops between the two commits. */
  private def silverStage(paths: AnalyticsLakehousePaths, failSilver: Boolean): AnalyticsBatchSilverStage[IO] = {
    val native = new DeltaBatchWriter[IO](paths, execution)
    val writer = new DeltaWriter[IO] {
      override def merge(frame: DataFrame, path: String, condition: String): IO[Unit] =
        if (failSilver && path == paths.silver) IO.raiseError(new IllegalStateException("injected Silver failure"))
        else native.merge(frame, path, condition)
      override def mergeWhenFresh(frame: DataFrame, path: String, condition: String, at: () => Instant): IO[Unit] =
        native.mergeWhenFresh(frame, path, condition, at)
      override def withExpiry(frame: DataFrame, now: Instant, days: Int): DataFrame =
        native.withExpiry(frame, now, days)
      override def mergeStageTimes(
          facts: DataFrame,
          retention: com.example.hiring.analytics.config.AnalyticsRetentionSettings,
          freshness: Option[() => Instant]
      ): IO[Unit] = native.mergeStageTimes(facts, retention, freshness)
    }
    new AnalyticsBatchSilverStage[IO](
      paths,
      AnalyticsTestSubjectPseudonymizer.fromSecret(Array.fill[Byte](32)(7)),
      execution,
      writer,
      new DeltaBatchReader[IO](execution),
      QuarantineIdentifier,
      retention
    )
  }

  private def prepared(rows: Seq[Row]): AnalyticsPreparedEvents = {
    val incoming = spark.createDataFrame(
      spark.sparkContext.parallelize(rows.map(row => Row.fromSeq(row.toSeq.take(12)))),
      AnalyticsTableSchemas.struct(AnalyticsTableSchemas.silver)
    )
    val noConflicts = spark.createDataFrame(
      spark.sparkContext.emptyRDD[Row],
      StructType(Seq(StructField(Columns.EventId, StringType, nullable = true)))
    )
    AnalyticsPreparedEvents(incoming, noConflicts, rows.size.toLong, 0L, 0L, 0L)
  }

  test("AC-03.6 a crash after the stage times and before Silver is repaired by the replay without duplicates") {
    val paths = freshPaths("crash")
    val rows = Seq(event("app-a", None, day(0)), event("app-a", Some(Accepted), day(1)))
    val startedAt = Instant.parse("2026-01-10T00:00:00Z")
    val failure = intercept[IllegalStateException] {
      silverStage(paths, failSilver = true).mergeSilver(prepared(rows), startedAt).unsafeRunSync()
    }
    assertEquals(failure.getMessage, "injected Silver failure")
    // Stage times were committed first: reversing the order of the two merges leaves no row here.
    assertEquals(stored(paths).size, 1)
    assert(!DeltaTables.exists(spark, paths.silver), "Silver was never written")

    silverStage(paths, failSilver = false).mergeSilver(prepared(rows), startedAt).unsafeRunSync()
    assertEquals(stored(paths).size, 1)
    assertEquals(DeltaTables.read(spark, paths.silver).count(), 2L)
    assertEquals(instant(storedRow(paths, "app-a"), Columns.AcceptedAt), Some(day(1)))
    assertEquals(instant(storedRow(paths, "app-a"), Columns.CreatedAt), Some(day(0)))
  }

  test("AC-03.6 the CANDIDATE_HIRED event is not a stage") {
    val paths = freshPaths("candidate-hired")
    val hired = event("app-h", None, day(5)).toSeq.updated(1, AnalyticsEventType.CandidateHired.wire)
    merge(paths, event("app-h", None, day(0)), Row.fromSeq(hired.updated(0, "candidate-hired-event")))
    val row = storedRow(paths, "app-h")
    assertEquals(instant(row, Columns.HiredAt), None)
    assertEquals(instant(row, Columns.TerminalAt), None)
    assertEquals(instant(row, Columns.LastEventAt), Some(day(0)))
  }

  test("AC-03.2 a stale freshness guard rejects the replay and merges no stage times") {
    val paths = freshPaths("stale-guard")
    val writer = new DeltaBatchWriter[IO](paths, execution)
    val frame = facts(Seq(event("app-s", None, day(0))))
    // The fact expires at day 30; a clock past it makes the guard fail.
    val result = writer.mergeStageTimes(frame, retention, Some(() => day(31))).attempt.unsafeRunSync()
    assertEquals(result.left.toOption, Some(AnalyticsError.LateFactReplayRejected))
    assert(!DeltaTables.exists(spark, paths.applicationStageTimes))
    writer.mergeStageTimes(frame, retention, Some(() => day(29))).unsafeRunSync()
    assertEquals(stored(paths).size, 1)
  }

  test("AC-03.5 one candidate with several applications counts once toward eligibleCount") {
    val paths = freshPaths("one-candidate")
    // Nine candidates hold one hired application each; the tenth candidate holds three.
    merge(paths, cohort(9, "single", Seq(None -> 0, Some(Hired) -> 3))*)
    (1 to 3).foreach { index =>
      merge(
        paths,
        event(s"multi-$index", None, day(0), candidate = "token-multi"),
        event(s"multi-$index", Some(Hired), day(3), candidate = "token-multi")
      )
    }
    val ten = rebuildAndExtract(paths, day(10))
    assertEquals(ten.stageDurations.map(_.eligibleCount), Vector(10L))
    assertEquals(ten.timeToHire.map(_.eligibleCount), Some(10L))

    val shared = freshPaths("one-candidate-only")
    (1 to 12).foreach { index =>
      merge(
        shared,
        event(s"only-$index", None, day(0), candidate = "token-only"),
        event(s"only-$index", Some(Hired), day(3), candidate = "token-only")
      )
    }
    assertEquals(rebuildAndExtract(shared, day(10)).stageDurations, Vector.empty)
  }

  test("AC-03.8 initialization through configureRawTables backfills before Gold rebuilds") {
    val paths = freshPaths("initialization")
    new DeltaBatchWriter[IO](paths, execution)
      .merge(
        facts(cohort(10, "old", Seq(None -> 0, Some(Hired) -> 3))),
        paths.silver,
        "target.eventId = source.eventId"
      )
      .unsafeRunSync()
    val keys = AnalyticsTestSubjectPseudonymizer.fromSecret(Array.fill[Byte](32)(7))
    val maintenance = AnalyticsBatchTestSupport.newMaintenance(spark, paths, keys)
    maintenance.configureRawTables(Vector.empty).unsafeRunSync()
    assertEquals(stored(paths).size, 10)
    val report = maintenance.rebuildGoldAndExtractReport(day(10)).unsafeRunSync()
    assertEquals(report.timeToHire.map(_.eligibleCount), Some(10L))
    assertEquals(report.stageDurations.map(_.metric), Vector(AnalyticsHiringStageMetric.TimeToHire))
  }

  test("AC-03.8 an existing stage-times table is never backfilled again, not even when it is empty") {
    val paths = freshPaths("no-rebackfill")
    new DeltaBatchWriter[IO](paths, execution)
      .merge(
        facts(cohort(10, "old", Seq(None -> 0, Some(Hired) -> 3))),
        paths.silver,
        "target.eventId = source.eventId"
      )
      .unsafeRunSync()
    val keys = AnalyticsTestSubjectPseudonymizer.fromSecret(Array.fill[Byte](32)(7))
    val maintenance = AnalyticsBatchTestSupport.newMaintenance(spark, paths, keys)
    maintenance.configureRawTables(Vector.empty).unsafeRunSync()
    DeltaTables.forPath(spark, paths.applicationStageTimes).delete()
    assertEquals(stored(paths).size, 0)
    maintenance.configureRawTables(Vector.empty).unsafeRunSync()
    assertEquals(stored(paths).size, 0)
  }

  test("AC-03.4 the backfill leaves out every application involving an active deletion marker") {
    val paths = freshPaths("backfill-marker")
    val rows = Seq(
      event("app-a", None, day(0), candidate = "token-candidate-a"),
      event("app-a", Some(Accepted), day(1), candidate = "token-candidate-a", actor = Some("token-recruiter")),
      event("app-b", None, day(0), candidate = "token-candidate-b"),
      event("app-c", None, day(0), candidate = "token-candidate-c", actor = Some("token-recruiter"))
    )
    new DeltaBatchWriter[IO](paths, execution)
      .merge(facts(rows), paths.silver, "target.eventId = source.eventId")
      .unsafeRunSync()
    val frame = spark.createDataFrame(
      java.util.List.of(Row("token-recruiter")),
      StructType(Seq(StructField(Columns.SubjectToken, StringType, nullable = false)))
    )
    ApplicationStageTimes.backfillFromSilver(spark, paths, retention, frame)
    assertEquals(stored(paths).map(_.getAs[String](Columns.ApplicationId)), Vector("app-b"))
    val candidate = freshPaths("backfill-candidate-marker")
    new DeltaBatchWriter[IO](candidate, execution)
      .merge(facts(rows), candidate.silver, "target.eventId = source.eventId")
      .unsafeRunSync()
    val candidateMarker = spark.createDataFrame(
      java.util.List.of(Row("token-candidate-a")),
      StructType(Seq(StructField(Columns.SubjectToken, StringType, nullable = false)))
    )
    ApplicationStageTimes.backfillFromSilver(spark, candidate, retention, candidateMarker)
    assert(!stored(candidate).exists(_.getAs[String](Columns.SubjectToken) == "token-candidate-a"))
    assertEquals(stored(candidate).size, 2)
  }

  test("AC-03.7 an application that skipped a stage contributes to no duration and is excluded from time to hire") {
    val paths = freshPaths("skipped")
    merge(paths, cohort(10, "hired", Seq(None -> 0, Some(Hired) -> 3))*)
    merge(paths, cohort(10, "skipped", Seq(None -> 0, Some(Rejected) -> 2))*)
    val report = rebuildAndExtract(paths, day(10))
    assertEquals(report.stageDurations.map(_.metric), Vector(AnalyticsHiringStageMetric.TimeToHire))
    assertEquals(report.stageDurations.map(_.eligibleCount), Vector(10L))
    assertEquals(report.timeToHire.map(row => (row.eligibleCount, row.excludedCount)), Some((10L, 10L)))
  }

  test("AC-03.7 negative durations are ignored") {
    val paths = freshPaths("negative")
    merge(paths, cohort(10, "negative", Seq(None -> 5, Some(Hired) -> 2))*)
    val report = rebuildAndExtract(paths, day(10))
    assertEquals(report.stageDurations, Vector.empty)
    assertEquals(report.timeToHire, None)
  }

  test("AC-03.8 an absent stage-times table is backfilled from Silver before Gold rebuilds") {
    val paths = freshPaths("backfill")
    val rows = cohort(10, "old", Seq(None -> 0, Some(Hired) -> 3))
    new DeltaBatchWriter[IO](paths, execution)
      .merge(facts(rows), paths.silver, "target.eventId = source.eventId")
      .unsafeRunSync()
    assert(!DeltaTables.exists(spark, paths.applicationStageTimes))
    ApplicationStageTimes.backfillFromSilver(spark, paths, retention, noMarkers)
    assertEquals(stored(paths).size, 10)
    assertEquals(instant(storedRow(paths, "old-1"), Columns.HiredAt), Some(day(3)))
    ApplicationStageTimes.backfillFromSilver(spark, paths, retention, noMarkers)
    val metrics = lastMergeMetrics(paths)
    assertEquals(metrics("numTargetRowsUpdated"), "0", "a repeated backfill rewrites no row")
    assertEquals(metrics("numTargetRowsInserted"), "0")
    assertEquals(rebuildAndExtract(paths, day(10)).timeToHire.map(_.eligibleCount), Some(10L))
  }

  test("AC-03.8 Gold rebuilds from the stage times when Silver is absent and clears only when both are absent") {
    val paths = freshPaths("no-silver")
    merge(paths, cohort(10, "app", Seq(None -> 0, Some(Hired) -> 3))*)
    assert(!DeltaTables.exists(spark, paths.silver))
    AnalyticsGoldStage.rebuildFromStoredTables[IO](spark, paths, day(10), execution).unsafeRunSync()
    assertEquals(
      AnalyticsGoldStage.extract[IO](spark, paths, day(10), execution).unsafeRunSync().timeToHire.map(_.eligibleCount),
      Some(10L)
    )
    val empty = freshPaths("nothing")
    AnalyticsGoldStage.rebuildFromStoredTables[IO](spark, empty, day(10), execution).unsafeRunSync()
    assertEquals(
      AnalyticsGoldStage.extract[IO](spark, empty, day(10), execution).unsafeRunSync().stageDurations,
      Vector.empty
    )
  }

  private def goldRow(metric: String, p50: Double, p75: Double, p90: Double, p95: Double, count: Long): Row =
    Row(metric, p50, p75, p90, p95, count)

  private def appendGold(paths: AnalyticsLakehousePaths, rows: Row*): Unit = {
    AnalyticsTableSchemas.createOrValidate(
      spark,
      paths.hiringStageDurationsGold,
      AnalyticsDeltaCatalog.HiringStageDurationsGold
    )
    spark
      .createDataFrame(
        spark.sparkContext.parallelize(rows),
        AnalyticsTableSchemas.struct(AnalyticsDeltaCatalog.HiringStageDurationsGold.shape)
      )
      .write
      .format("delta")
      .mode("append")
      .save(SparkPhysicalLocation.resolve(paths.hiringStageDurationsGold))
  }

  private def rejects(expected: String)(thunk: => Unit): Unit = {
    val error = intercept[Throwable](thunk)
    val rendered = Iterator.iterate(error)(_.getCause).takeWhile(_ != null).map(_.toString).mkString(" <- ")
    assert(rendered.contains("InvariantViolation"), rendered)
    assert(rendered.toLowerCase.contains(expected), rendered)
  }

  test("AC-03.5 the Gold durations table enforces ordered percentiles, non-negative counts, known metrics and K") {
    val belowK = freshPaths("gold-k")
    rejects("eligible_count_minimum")(appendGold(belowK, goldRow("TIME_TO_HIRE", 1.0, 2.0, 3.0, 4.0, 9L)))
    val paths = freshPaths("gold-checks")
    rejects("percentiles_ordered")(appendGold(paths, goldRow("TIME_TO_HIRE", 5.0, 4.0, 6.0, 7.0, 10L)))
    rejects("violated")(appendGold(paths, goldRow("TIME_TO_HIRE", 1.0, 2.0, 3.0, 4.0, -1L)))
    rejects("metric_valid")(appendGold(paths, goldRow("TIME_TO_EAT", 1.0, 2.0, 3.0, 4.0, 10L)))
    appendGold(paths, goldRow("TIME_TO_HIRE", 1.0, 2.0, 3.0, 4.0, 10L))
    assertEquals(DeltaTables.read(spark, paths.hiringStageDurationsGold).count(), 1L)
  }

  test("AC-03.5 extraction rejects a stored stage-duration row below K even when the table has no CHECK") {
    val paths = freshPaths("decode-k")
    spark
      .createDataFrame(
        spark.sparkContext.parallelize(Seq(goldRow("TIME_TO_HIRE", 1.0, 2.0, 3.0, 4.0, 9L))),
        AnalyticsTableSchemas.struct(AnalyticsDeltaCatalog.HiringStageDurationsGold.shape)
      )
      .write
      .format("delta")
      .save(SparkPhysicalLocation.resolve(paths.hiringStageDurationsGold))
    val result = AnalyticsGoldStage.extract[IO](spark, paths, day(10), execution).attempt.unsafeRunSync()
    assertEquals(result.swap.toOption, Some(AnalyticsError.InvalidGoldSchema))
  }

  test("AC-03.4 initialization through configureRawTables leaves out the marked subject's applications") {
    val paths = freshPaths("initialization-marker")
    val markedToken = s"k1_${"a" * 43}"
    val rows = Seq(
      event("app-a", None, day(0), candidate = markedToken),
      event("app-b", None, day(0), candidate = s"k1_${"b" * 43}")
    )
    new DeltaBatchWriter[IO](paths, execution)
      .merge(facts(rows), paths.silver, "target.eventId = source.eventId")
      .unsafeRunSync()
    val keys = AnalyticsTestSubjectPseudonymizer.fromSecret(Array.fill[Byte](32)(7))
    val maintenance = AnalyticsBatchTestSupport.newMaintenance(spark, paths, keys)
    val marker = com.example.hiring.analytics.domain.SubjectToken.fromHmac(markedToken).fold(fail(_), identity)
    maintenance.configureRawTables(Vector(marker)).unsafeRunSync()
    val tokens = stored(paths).flatMap(_.getAs[Seq[String]](Columns.SubjectTokens))
    assertEquals(stored(paths).map(_.getAs[String](Columns.ApplicationId)), Vector("app-b"))
    assert(!tokens.contains(markedToken), tokens.toString)
  }

  test("AC-03.3 the stage-times table is subject-attributed, gathers no token statistics and checks its expiry") {
    val spec = AnalyticsDeltaCatalog.ApplicationStageTimes
    assert(spec.subjectAttributed)
    assertEquals(spec.properties.get(AnalyticsDeltaCatalog.SkippingNumIndexedColumnsProperty), Some("0"))
    val paths = freshPaths("inventory")
    assert(paths.inventory.subjectDelta.exists(_.location == paths.applicationStageTimes))
    assert(paths.inventory.delta.exists(_.location == paths.hiringStageDurationsGold))
  }
}
