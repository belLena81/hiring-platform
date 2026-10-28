package com.example.hiring.analytics

import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.domain.AnalyticsRunManifest
import com.example.hiring.analytics.service.batch.{
  AnalyticsLakehousePaths,
  AnalyticsManifestStatus,
  AnalyticsRunManifestStore
}

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import munit.FunSuite
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp, encode}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

import java.nio.file.Files
import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Local wall-clock evidence for the parse, Bronze and Silver admission stages on one fixed fixture. It runs only when
  * the `timing.records` system property or `TIMING_RECORDS` holds a record count, so ordinary test runs stay fast. The
  * numbers are local evidence on one machine, not a benchmark.
  */
class OperationalEventIngestionTimingSpec extends FunSuite {
  override val munitTimeout: FiniteDuration = 20.minutes

  private val execution = SparkBlockingExecution.forTests[IO](scala.concurrent.ExecutionContext.parasitic)
  private val keys = AnalyticsTestSubjectPseudonymizer.fromSecret(Array.fill[Byte](32)(5))
  private lazy val spark: SparkSession = org.apache.spark.sql.classic.SparkSession
    .builder()
    .master("local[2]")
    .appName("OperationalEventIngestionTimingSpec")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  override def afterAll(): Unit = if (!spark.sparkContext.isStopped) spark.stop()

  private def uuid(kind: String, index: Int): String =
    UUID.nameUUIDFromBytes(s"$kind-$index".getBytes("UTF-8")).toString

  private def eventJson(index: Int): String = {
    val actor = uuid("actor", index % 500)
    val at = "2026-10-02T10:00:00Z"
    val event = uuid("event", index)
    index % 5 match {
      case 0 | 1 | 2 =>
        val app = uuid("app", index)
        val candidate = uuid("candidate", index % 2000)
        val job = uuid("job", index % 100)
        s"""{"eventId":"$event","eventType":"APPLICATION_STATUS_CHANGED","occurredAt":"$at","aggregateType":"Application","aggregateId":"$app","actorId":"$actor","payload":{"applicationId":"$app","candidateId":"$candidate","jobId":"$job","previousStatus":"Created","newStatus":"Accepted"}}"""
      case 3 =>
        val search = uuid("search", index)
        val results = (1 to 5)
          .map(rank => s"""{"resultId":"${uuid("result", index * 5 + rank)}","rank":$rank,"score":0.5}""")
          .mkString(",")
        s"""{"eventId":"$event","eventType":"SEARCH_PERFORMED","occurredAt":"$at","aggregateType":"Search","aggregateId":"$search","actorId":"$actor","payload":{"searchId":"$search","searchKind":"candidateMatches","results":[$results]}}"""
      case _ if index % 100 == 4 => "not-json"
      case _                     =>
        val job = uuid("job-event", index)
        s"""{"eventId":"$event","eventType":"JOB_CREATED","occurredAt":"$at","aggregateType":"Job","aggregateId":"$job","actorId":"$actor","payload":{"job":{"jobId":"$job","status":"Open","skills":["Scala","Spark","Kafka"]}}}"""
    }
  }

  private def timed[A](work: => A): (A, Long) = {
    val start = System.nanoTime()
    val result = work
    (result, (System.nanoTime() - start) / 1000000L)
  }

  test("local wall time of parse, Bronze and Silver admission on a fixed fixture") {
    val size = sys.props.get("timing.records").orElse(sys.env.get("TIMING_RECORDS")).flatMap(_.toIntOption)
    assume(size.isDefined, "set the timing.records system property or TIMING_RECORDS to run")
    val count = size.get
    val paths: AnalyticsLakehousePaths =
      TestAnalyticsLakehousePaths.unsafe(Files.createTempDirectory("analytics-timing-").toUri.toString.stripSuffix("/"))
    val schema = StructType(
      Seq(
        StructField("topic", StringType, nullable = false),
        StructField("partition", IntegerType, nullable = false),
        StructField("offset", LongType, nullable = false),
        StructField("value", StringType, nullable = true)
      )
    )
    val raw = spark
      .createDataFrame(
        (0 until count).map(i => Row("hiring.operational-events", i % 4, (i / 4).toLong, eventJson(i))).asJava,
        schema
      )
      .withColumn("value", encode(col("value"), "UTF-8"))
      .withColumn("timestamp", current_timestamp())
      .repartition(4)
      .cache()
    assertEquals(raw.count(), count.toLong)
    val markers: DataFrame = spark.createDataFrame(
      (0 until 50).map(i => Row(AnalyticsTestSubjectPseudonymizer.tokenValue(keys, uuid("candidate", i)))).asJava,
      StructType(Seq(StructField("subjectToken", StringType, nullable = false)))
    )
    val writer = new DeltaBatchWriter[IO](paths, execution)
    val observed = Instant.parse("2026-10-02T12:00:00Z")
    val retention = AnalyticsTestOperationalConfig.operational.retention
    val bronzeStage = new AnalyticsBatchIngestionStage[IO](
      paths,
      keys,
      execution,
      new AnalyticsRunManifestStore[IO] {
        override def persist(
            manifest: AnalyticsRunManifest,
            status: AnalyticsManifestStatus,
            updatedAt: Instant
        ): IO[Unit] =
          IO.raiseError(new IllegalStateException("timing fixture has no manifest"))
      },
      writer,
      retention,
      cats.effect.Clock[IO]
    )
    val silverStage = new AnalyticsBatchSilverStage[IO](
      paths,
      keys,
      execution,
      writer,
      new DeltaBatchReader[IO](execution),
      QuarantineIdentifier,
      retention
    )
    val (parsed, parseMs) = timed {
      val frame = TimingParse.persisted(raw, keys)
      frame.count()
      frame
    }
    val (bronze, bronzeMs) = timed {
      bronzeStage.persistParsed(parsed, markers, IO.unit, IO.pure(observed), _ => IO.unit).unsafeRunSync()
    }
    val (prepared, quarantineMs) = timed {
      silverStage.separateQuarantine(spark, bronze, markers, activeMarkersPresent = false).unsafeRunSync()
    }
    val (_, silverMs) = timed(silverStage.mergeSilver(prepared, observed).unsafeRunSync())
    println(
      s"TIMING records=$count parse=${parseMs}ms bronze=${bronzeMs}ms quarantine=${quarantineMs}ms silver=${silverMs}ms total=${parseMs + bronzeMs + quarantineMs + silverMs}ms valid=${bronze.validRecords} malformed=${bronze.malformedRecords}"
    )
    // Same-code comparison of the compute cost alone, with no Delta writes: four consumers read the frame, once
    // recomputing validity, fingerprint and tokens per consumer (a parse-only frame), once reading the columns that
    // `analyzed` computed once.
    def consumers(frame: DataFrame): Long = {
      val valid = OperationalEventTransforms.validEvents(frame)
      val tokenized = AnalyticsSubjectPrivacy.ensureSubjectTokens(valid, keys)
      val safe = AnalyticsSubjectPrivacy.excludeActiveDeletionMarkers(tokenized, markers).toOption.get
      val silver = OperationalEventTransforms.silver(safe, keys, markers).toOption.get
      safe.count() + OperationalEventTransforms.malformedEvents(frame).count() +
        OperationalEventTransforms.conflictingEventIds(safe).count() + silver.count()
    }
    val recompute = OperationalEventTransforms.parseKafkaRecords(raw).persist()
    recompute.count()
    val (_, recomputeMs) = timed(consumers(recompute))
    val once = TimingParse.persisted(raw, keys)
    val (_, onceMs) = timed { once.count(); consumers(once) }
    println(
      s"TIMING-CONSUMERS records=$count recomputePerConsumer=${recomputeMs}ms computedOnceIncludingMaterialization=${onceMs}ms"
    )
    recompute.unpersist(blocking = true)
    once.unpersist(blocking = true)
    parsed.unpersist(blocking = true)
    raw.unpersist(blocking = true)
  }
}

object TimingParse {
  def persisted(raw: DataFrame, keys: com.example.hiring.analytics.domain.SubjectPseudonymizer): DataFrame =
    OperationalEventTransforms.analyzed(OperationalEventTransforms.parseKafkaRecords(raw), keys).persist()
}
