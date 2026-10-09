package com.example.hiring.analytics.adapter.spark
import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.TestAnalyticsLakehousePaths

import com.example.hiring.analytics.domain.{AnalyticsEventType, AnalyticsApplicationStatus}
import com.example.hiring.analytics.errors.AnalyticsError

import munit.FunSuite
import cats.effect.IO
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, DataTypes, StringType, StructField, StructType}
import cats.effect.unsafe.implicits.global

import java.sql.Timestamp
import java.nio.file.Files
import java.time.Instant
import scala.concurrent.duration.*

class HiringAnalyticsSchemaContractsSpec extends FunSuite {
  override val munitTimeout: FiniteDuration = 5.minutes
  private val sparkExecution = SparkBlockingExecution.forTests[IO](scala.concurrent.ExecutionContext.parasitic)

  private lazy val spark: SparkSession = org.apache.spark.sql.classic.SparkSession
    .builder()
    .master("local[2]")
    .appName("HiringAnalyticsSchemaContractsSpec")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  override def afterAll(): Unit =
    if (!spark.sparkContext.isStopped) spark.stop()

  test("merge targets resolve their stored shape and raw-record scope from the lakehouse paths") {
    val paths = TestAnalyticsLakehousePaths.unsafe("file:///tmp/hiring-targets")
    val raw = Vector(
      paths.bronze -> AnalyticsTableSchemas.bronze,
      paths.quarantine -> AnalyticsTableSchemas.quarantine,
      paths.lateFacts -> AnalyticsTableSchemas.lateFacts
    )
    raw.foreach { case (path, shape) => assertEquals(AnalyticsTableSchemas.targetOf(paths, path), (shape, true)) }
    assertEquals(
      AnalyticsTableSchemas.targetOf(paths, paths.silver),
      (AnalyticsTableSchemas.silver ++ AnalyticsTableSchemas.expiry, false)
    )
  }

  private val SilverSchema = StructType(
    Seq(
      StructField("eventId", StringType, nullable = true),
      StructField("eventType", StringType, nullable = true),
      StructField("occurredAt", DataTypes.TimestampType, nullable = true),
      StructField("aggregateType", StringType, nullable = true),
      StructField("aggregateId", StringType, nullable = true),
      StructField("applicationId", StringType, nullable = true),
      StructField("jobId", StringType, nullable = true),
      StructField("newStatus", StringType, nullable = true),
      StructField("jobSkills", ArrayType(StringType), nullable = true),
      StructField("subjectToken", StringType, nullable = true),
      StructField("subjectTokens", ArrayType(StringType, containsNull = false), nullable = true),
      StructField("eventFingerprint", StringType, nullable = true)
    )
  )

  private def silverEvents(): org.apache.spark.sql.DataFrame = {
    val start = Instant.parse("2026-01-01T10:00:00Z")
    val rows = (1 to 10).toVector.flatMap { index =>
      val suffix = index.toString
      val token = s"token-$suffix"
      Vector(
        Row(
          s"created-$suffix",
          AnalyticsEventType.ApplicationCreated.wire,
          ts(start),
          "Application",
          s"application-$suffix",
          s"application-$suffix",
          s"job-$suffix",
          null,
          null,
          token,
          Seq(token),
          s"fingerprint-created-$suffix"
        ),
        Row(
          s"hired-$suffix",
          AnalyticsEventType.ApplicationStatusChanged.wire,
          ts(start.plusSeconds(3600)),
          "Application",
          s"application-$suffix",
          s"application-$suffix",
          s"job-$suffix",
          AnalyticsApplicationStatus.Hired.wire,
          null,
          token,
          Seq(token),
          s"fingerprint-hired-$suffix"
        ),
        Row(
          s"job-$suffix",
          AnalyticsEventType.JobCreated.wire,
          ts(start),
          "Job",
          s"job-$suffix",
          null,
          s"job-$suffix",
          null,
          Seq("Scala"),
          token,
          Seq(token),
          s"fingerprint-job-$suffix"
        )
      )
    }
    spark.createDataFrame(spark.sparkContext.parallelize(rows), SilverSchema)
  }

  test("Silver schema validation and Gold transforms preserve columns and aggregate metrics") {
    import spark.implicits.*

    val silver = silverEvents()
    val persistedSilver = OperationalEventTransforms
      .validateSilverSchema(silver)
      .fold(
        error => fail(error.toString),
        identity
      )
    assertEquals(persistedSilver.schema.fieldNames.toSeq, SilverSchema.fieldNames.toSeq)
    assertEquals(
      persistedSilver.schema.fields.map(_.dataType.simpleString).toSeq,
      Seq(
        "string",
        "string",
        "timestamp",
        "string",
        "string",
        "string",
        "string",
        "string",
        "array<string>",
        "string",
        "array<string>",
        "string"
      )
    )
    assertEquals(persistedSilver.count(), 30L)

    val storedSilver = silver
      .withColumn(Columns.IngestedAt, org.apache.spark.sql.functions.current_timestamp())
      .withColumn(Columns.ExpiresAt, org.apache.spark.sql.functions.current_timestamp())
    assert(OperationalEventTransforms.validateSilverSchema(storedSilver).isRight)
    val unexpectedSuffix = silver.withColumn("unexpected", org.apache.spark.sql.functions.lit("value"))
    assertEquals(
      OperationalEventTransforms.validateSilverSchema(unexpectedSuffix).swap.toOption,
      Some(AnalyticsError.InvalidSilverSchema)
    )

    val funnel = HiringGoldTransforms.funnelActivity(silver).toOption.get
    assertEquals(funnel.schema.fieldNames.toSeq, Seq("day", "eventType", "newStatus", "contributingApplications"))
    assertEquals(
      funnel.schema.fields.map(_.dataType.simpleString).toSeq,
      Seq("timestamp", "string", "string", "bigint")
    )
    assertEquals(funnel.select("contributingApplications").as[Long].collect().toSeq, Seq(10L, 10L))

    val wide = HiringGoldTransforms.wideFunnelDay(silver).toOption.get
    assertEquals(
      wide.schema.fieldNames.toSeq,
      Seq("day", "created", "accepted", "declined", "interview", "hired", "rejected")
    )
    assertEquals(
      wide.schema.fields.map(_.dataType.simpleString).toSeq,
      Seq.fill(1)("timestamp") ++ Seq.fill(6)("bigint")
    )
    val wideRow = wide.head()
    assertEquals(wideRow.getAs[Long]("created"), 10L)
    assertEquals(wideRow.getAs[Long]("hired"), 10L)

    val skills = HiringGoldTransforms.skillPostingActivity(silver).toOption.get
    assertEquals(skills.schema.fieldNames.toSeq, Seq("day", "skill", "postings"))
    assertEquals(skills.schema.fields.map(_.dataType.simpleString).toSeq, Seq("timestamp", "string", "bigint"))
    assertEquals(
      skills.select("skill", "postings").collect().toSeq.map(r => r.getString(0) -> r.getLong(1)),
      Seq("scala" -> 10L)
    )

    val timeToHire = HiringGoldTransforms.timeToHireAction[IO](silver, sparkExecution).unsafeRunSync()
    assertEquals(
      timeToHire.schema.fieldNames.toSeq,
      Seq("p50Hours", "p75Hours", "p90Hours", "p95Hours", "eligibleCount", "excludedCount")
    )
    assertEquals(
      timeToHire.schema.fields.map(_.dataType.simpleString).toSeq,
      Seq.fill(4)("double") ++ Seq.fill(2)("bigint")
    )
    assertEquals(timeToHire.head().getAs[Long]("eligibleCount"), 10L)
    assertEquals(timeToHire.head().getAs[Long]("excludedCount"), 0L)
  }

  test("Silver schema validation rejects field order and type drift") {
    val silver = silverEvents()
    val reordered = silver.select(
      "eventType",
      "eventId",
      "occurredAt",
      "aggregateType",
      "aggregateId",
      "applicationId",
      "jobId",
      "newStatus",
      "jobSkills",
      "subjectToken",
      "subjectTokens",
      "eventFingerprint"
    )
    val wrongType = silver.withColumn("occurredAt", org.apache.spark.sql.functions.lit("not-a-timestamp"))

    assertEquals(
      OperationalEventTransforms.validateSilverSchema(reordered).swap.toOption,
      Some(AnalyticsError.InvalidSilverSchema)
    )
    assertEquals(
      OperationalEventTransforms.validateSilverSchema(wrongType).swap.toOption,
      Some(AnalyticsError.InvalidSilverSchema)
    )
    assertEquals(
      HiringGoldTransforms.funnelActivity(wrongType),
      Left(AnalyticsError.InvalidSilverSchema)
    )
    assertEquals(HiringGoldTransforms.wideFunnelDay(wrongType), Left(AnalyticsError.InvalidSilverSchema))
    assertEquals(HiringGoldTransforms.skillPostingActivity(wrongType), Left(AnalyticsError.InvalidSilverSchema))
    assertEquals(
      HiringGoldTransforms.timeToHireAction[IO](wrongType, sparkExecution).attempt.unsafeRunSync().swap.toOption,
      Some(AnalyticsError.InvalidSilverSchema)
    )
  }

  test("report extraction schema contract detects field and type drift") {
    val invalid = StructType.fromDDL("day STRING, created BIGINT")
    val valid = StructType.fromDDL(
      "day TIMESTAMP, created BIGINT, accepted BIGINT, declined BIGINT, interview BIGINT, hired BIGINT, rejected BIGINT"
    )
    val expected = Vector(
      "day" -> DataTypes.TimestampType,
      "created" -> DataTypes.LongType,
      "accepted" -> DataTypes.LongType,
      "declined" -> DataTypes.LongType,
      "interview" -> DataTypes.LongType,
      "hired" -> DataTypes.LongType,
      "rejected" -> DataTypes.LongType
    )
    assertEquals(AnalyticsGoldStage.validateOutputSchema(invalid, expected), Left(AnalyticsError.InvalidGoldSchema))
    assertEquals(AnalyticsGoldStage.validateOutputSchema(valid, expected), Right(()))
  }

  test("report extraction rejects a persisted Gold table with a drifted schema") {
    val paths =
      TestAnalyticsLakehousePaths.unsafe(Files.createTempDirectory("analytics-gold-schema-drift").toUri.toString)
    val frame = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("not-a-timestamp", Long.box(1L)))),
      StructType.fromDDL("day STRING, created BIGINT")
    )
    frame.write.format("delta").save(paths.funnelGold)

    val result = AnalyticsGoldStage.extract[IO](spark, paths, Instant.EPOCH, sparkExecution).attempt.unsafeRunSync()
    assertEquals(result.swap.toOption, Some(AnalyticsError.InvalidGoldSchema))
  }

  test("report extraction rejects null values in a persisted Gold row") {
    val paths =
      TestAnalyticsLakehousePaths.unsafe(Files.createTempDirectory("analytics-gold-null-output").toUri.toString)
    val schema = StructType.fromDDL(
      "p50Hours DOUBLE, p75Hours DOUBLE, p90Hours DOUBLE, p95Hours DOUBLE, eligibleCount BIGINT, excludedCount BIGINT"
    )
    val frame = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(Row(null, Double.box(2.0), Double.box(3.0), Double.box(4.0), Long.box(10L), Long.box(0L)))
      ),
      schema
    )
    frame.write.format("delta").save(paths.timeToHireGold)

    val result = AnalyticsGoldStage.extract[IO](spark, paths, Instant.EPOCH, sparkExecution).attempt.unsafeRunSync()
    assertEquals(result.swap.toOption, Some(AnalyticsError.InvalidGoldSchema))
  }

  test("quarantine first write and replay merge once and reject an incompatible existing table") {
    val paths =
      TestAnalyticsLakehousePaths.unsafe(Files.createTempDirectory("analytics-quarantine-first-write").toUri.toString)
    val row = Row(
      "hiring.operational-events",
      Int.box(0),
      Long.box(1L),
      "hash",
      Seq("token"),
      "quarantine-id",
      "INVALID_OPERATIONAL_EVENT_ENVELOPE",
      ts(Instant.parse("2026-10-01T00:00:00Z"))
    )
    val frame = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(row)),
      AnalyticsTableSchemas.struct(AnalyticsTableSchemas.quarantine)
    )
    val writer = new DeltaBatchWriter[IO](paths, sparkExecution)
    val condition = "target.quarantineId = source.quarantineId"

    writer.merge(frame, paths.quarantine, condition).unsafeRunSync()
    writer.merge(frame, paths.quarantine, condition).unsafeRunSync()
    assertEquals(spark.read.format("delta").load(paths.quarantine).count(), 1L)

    val drifted =
      TestAnalyticsLakehousePaths.unsafe(Files.createTempDirectory("analytics-quarantine-drift").toUri.toString)
    spark
      .createDataFrame(
        spark.sparkContext.parallelize(Seq(Row("quarantine-id"))),
        StructType.fromDDL("quarantineId STRING")
      )
      .write
      .format("delta")
      .save(drifted.quarantine)
    val driftedWriter = new DeltaBatchWriter[IO](drifted, sparkExecution)
    intercept[AnalyticsError.DeltaSchemaMismatch] {
      driftedWriter.merge(frame, drifted.quarantine, condition).unsafeRunSync()
    }
  }

  private def ts(value: Instant): Timestamp = Timestamp.from(value)
}
