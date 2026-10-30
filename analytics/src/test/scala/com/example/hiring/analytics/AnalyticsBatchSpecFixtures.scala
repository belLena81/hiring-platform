package com.example.hiring.analytics
import cats.effect.unsafe.implicits.global
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.domain.*

import com.example.hiring.analytics.adapter.spark.*

import cats.effect.{Clock, IO}
import munit.FunSuite
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

import java.nio.file.Files
import java.time.Instant
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

abstract class AnalyticsBatchSpecFixtures extends FunSuite {
  override val munitTimeout: FiniteDuration = 10.minutes

  protected val sparkExecution = SparkBlockingExecution.forTests[IO](scala.concurrent.ExecutionContext.parasitic)
  protected def hmacKey(seed: String): Array[Byte] = seed.padTo(32, 'x').getBytes("UTF-8")
  protected val pseudonymizer = AnalyticsTestSubjectPseudonymizer.fromSecret(hmacKey("analytics-test-secret"))
  protected def subjectToken(raw: String): SubjectToken =
    SubjectToken
      .fromHmac(
        "test_" + java.util.Base64.getUrlEncoder
          .withoutPadding()
          .encodeToString(
            java.security.MessageDigest.getInstance("SHA-256").digest(raw.getBytes("UTF-8"))
          )
      )
      .toOption
      .get
  protected lazy val spark: SparkSession = AnalyticsTestSpark.session(getClass.getSimpleName)

  protected lazy val lakehouseTemplate = AnalyticsLakehouseTemplate.forBatch(spark, pseudonymizer)
  protected def lakehouseDirectory(prefix: String): java.nio.file.Path = lakehouseTemplate.freshDirectory(prefix)

  override def afterAll(): Unit =
    if (!spark.sparkContext.isStopped) spark.stop()

  protected def records(values: Seq[(String, Int, Long, String)]) = {
    val schema = StructType(
      Seq(
        StructField("topic", StringType, nullable = false),
        StructField("partition", IntegerType, nullable = false),
        StructField("offset", LongType, nullable = false),
        StructField("value", StringType, nullable = true)
      )
    )
    spark
      .createDataFrame(
        values.map { case (topic, partition, offset, value) =>
          Row(topic, partition, offset, value)
        }.asJava,
        schema
      )
      .withColumn("value", org.apache.spark.sql.functions.encode(org.apache.spark.sql.functions.col("value"), "UTF-8"))
      .withColumn("timestamp", org.apache.spark.sql.functions.current_timestamp())
  }

  protected def validatedManifest(runId: String, ranges: Vector[PartitionOffsetRange]): AnalyticsRunManifest =
    TestPartitionOffsetRange.manifestOf(runId, ranges).toEither.fold(errors => fail(errors.toString), identity)

  protected def fixedClock(instant: Instant): Clock[IO] = new Clock[IO] {
    override val applicative: cats.Applicative[IO] = summon[cats.Applicative[IO]]
    override def realTime: IO[FiniteDuration] = IO.pure(instant.toEpochMilli.millis)
    override def monotonic: IO[FiniteDuration] = IO.pure(0.nanos)
  }

  protected def markerFrame(tokens: Seq[String]) =
    spark.createDataFrame(
      tokens.map(Row(_)).asJava,
      StructType(
        Seq(
          StructField("subjectToken", StringType, nullable = false)
        )
      )
    )

  protected def event(
      id: String,
      eventType: String,
      aggregateType: String = "Application",
      aggregateId: String = "a053804b-8231-3cb4-8074-834a69aaccab",
      payload: Option[String] = None,
      actorId: String = "d3d4e2cb-8e0c-3a66-8cc2-1379ac3a7686"
  ): String = {
    val defaultPayload =
      s"""{"applicationId":"$aggregateId","candidateId":"19b46da1-bee5-3018-b5f7-abe9a9fdfb29","jobId":"ac15a52e-59f3-33d6-9344-2a598ffab484","newStatus":"Hired"}"""
    AnalyticsOperationalEventFixtures.complete(
      s"""{"eventId":"$id","eventType":"$eventType","occurredAt":"2026-09-22T10:00:00Z","aggregateType":"$aggregateType","aggregateId":"$aggregateId","actorId":"$actorId","payload":${payload
          .getOrElse(defaultPayload)}}"""
    )
  }

  protected def emptyMarkers = AnalyticsSubjectPrivacy.emptyMarkers(records(Seq.empty))

  protected def silverFrame(
      valid: DataFrame,
      pseudonymizer: SubjectPseudonymizer,
      markers: DataFrame
  ): DataFrame =
    OperationalEventTransforms.silver(valid, pseudonymizer, markers).fold(error => fail(error.toString), identity)

  protected def bronzeStage(paths: AnalyticsLakehousePaths) = new AnalyticsBatchIngestionStage[IO](
    paths,
    pseudonymizer,
    sparkExecution,
    new AnalyticsRunManifestStore[IO] {
      override def persist(
          manifest: AnalyticsRunManifest,
          status: AnalyticsManifestStatus,
          updatedAt: Instant
      ): IO[Unit] =
        fail("parsed-frame ingestion must not create a batch manifest")
    },
    new DeltaBatchWriter[IO](paths, sparkExecution),
    AnalyticsTestOperationalConfig.operational.retention,
    cats.effect.Clock[IO]
  )

  protected def ingestIntoBronze(
      paths: AnalyticsLakehousePaths,
      values: Seq[(String, Int, Long, String)]
  ): DataFrame = {
    val parsed = OperationalEventTransforms.parseKafkaRecords(records(values))
    bronzeStage(paths)
      .persistParsed(parsed, emptyMarkers, IO.unit, IO.pure(Instant.parse("2026-09-30T10:00:00Z")), _ => IO.unit)
      .unsafeRunSync()
    spark.read.format("delta").load(paths.bronze)
  }

  protected def newLakehouse(prefix: String): AnalyticsLakehousePaths =
    TestAnalyticsLakehousePaths.unsafe(Files.createTempDirectory(prefix).toUri.toString.stripSuffix("/"))

  protected def stageTimesOf(silver: DataFrame): DataFrame =
    ApplicationStageTimes.source(silver, AnalyticsTestOperationalConfig.operational.retention).toOption.get

}
