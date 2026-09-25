package com.example.hiring.analytics

import com.example.hiring.analytics.batch.*
import com.example.hiring.analytics.erasure.*
import com.example.hiring.analytics.mongo.*

import cats.Applicative
import cats.effect.{Clock, IO, Resource}
import cats.effect.unsafe.implicits.global
import io.delta.tables.DeltaTable
import munit.FunSuite
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, encode}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

import java.nio.file.{Files, Path}
import java.sql.Timestamp
import java.time.{Instant, LocalDate}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.util.Using

/** Run explicitly with `HAL07_WORKLOAD=1 sbt -java-home /usr/lib/jvm/java-17-openjdk-amd64 "testOnly
  * com.example.hiring.analytics.HiringAnalyticsWorkloadSpec"`.
  *
  * This is a bounded local reproducibility baseline, not a throughput benchmark or SLO.
  */
class HiringAnalyticsWorkloadSpec extends FunSuite {
  override val munitTimeout: FiniteDuration = 30.minutes

  private val Seed = 424242
  private val TotalRecords = 100000
  private val Topic = "hiring.operational-events"
  private val FixedNow = Instant.parse("2026-09-23T12:00:00Z")
  private val Pseudonymizer =
    SubjectPseudonymizer.fromSecret("hal07-local-fixture".padTo(32, 'x').getBytes("UTF-8"))

  private val RecordSchema = StructType(
    Seq(
      StructField("topic", StringType, nullable = false),
      StructField("partition", IntegerType, nullable = false),
      StructField("offset", LongType, nullable = false),
      StructField("timestamp", org.apache.spark.sql.types.TimestampType, nullable = false),
      StructField("value", StringType, nullable = true)
    )
  )

  private val MarkerSchema = StructType(Seq(StructField("subjectToken", StringType, nullable = false)))

  private val FixedClock: Clock[IO] = new Clock[IO] {
    override val applicative: Applicative[IO] = summon[Applicative[IO]]
    override def realTime: IO[FiniteDuration] = IO.pure(FixedNow.toEpochMilli.millis)
    override def monotonic: IO[FiniteDuration] = IO.pure(0.nanos)
  }

  private def sparkResource: Resource[IO, SparkSession] =
    Resource.make(IO.blocking {
      org.apache.spark.sql.classic.SparkSession
        .builder()
        .master("local[4]")
        .appName("HiringAnalyticsWorkloadSpec")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    })(spark => IO.blocking(spark.stop()))

  private def rootResource: Resource[IO, Path] =
    Resource.make(IO.blocking(Files.createTempDirectory("hiring-analytics-hal07-"))) { root =>
      IO.blocking {
        Using.resource(Files.walk(root)) { paths =>
          paths.iterator().asScala.toVector.sortBy(_.toString.length).reverse.foreach(Files.deleteIfExists)
        }
      }.void
    }

  private def candidate(index: Int): String = s"candidate-${Math.floorMod(index * 31 + Seed, 10000)}"

  private def occurredAt(index: Int): String =
    LocalDate.of(2026, 9, 23).minusDays(index % 30).toString + "T12:00:00Z"

  private def created(index: Int): String =
    s"""{"eventId":"application-created-$index","eventType":"APPLICATION_CREATED","occurredAt":"${occurredAt(
        index
      )}","aggregateType":"Application","aggregateId":"application-$index","actorId":"${candidate(
        index
      )}","payload":{"applicationId":"application-$index","candidateId":"${candidate(
        index
      )}","jobId":"job-${index % 8000}"}}"""

  private def status(index: Int, eventId: String, newStatus: String): String =
    s"""{"eventId":"$eventId","eventType":"APPLICATION_STATUS_CHANGED","occurredAt":"${occurredAt(
        index
      )}","aggregateType":"Application","aggregateId":"application-$index","actorId":"recruiter-${index % 1000}","payload":{"applicationId":"application-$index","candidateId":"${candidate(
        index
      )}","jobId":"job-${index % 8000}","newStatus":"$newStatus"}}"""

  private def job(index: Int): String =
    s"""{"eventId":"job-created-$index","eventType":"JOB_CREATED","occurredAt":"${occurredAt(
        index
      )}","aggregateType":"Job","aggregateId":"job-$index","actorId":"recruiter-${index % 1000}","payload":{"jobId":"job-$index","job":{"skills":["Scala","MongoDB"]}}}"""

  private def valueAt(offset: Int): String =
    if (offset < 80000) created(offset)
    else if (offset < 90000) {
      val index = offset - 80000
      status(index, s"application-status-$index", "Hired")
    } else if (offset < 98000) job(offset - 90000)
    else if (offset < 99000) "{" + s"\"eventId\":\"malformed-$offset\""
    else if (offset < 99500) created(offset - 99000 + 500)
    else {
      val index = offset - 99500
      status(index, s"application-created-$index", "Rejected")
    }

  private def records(spark: SparkSession): org.apache.spark.sql.DataFrame = {
    val timestamp = Timestamp.from(FixedNow)
    val rows = (0 until TotalRecords).iterator.map { offset =>
      Row(Topic, 0, offset.toLong, timestamp, valueAt(offset))
    }.toVector
    spark.createDataFrame(rows.asJava, RecordSchema).withColumn("value", encode(col("value"), "UTF-8"))
  }

  private def emptyMarkers(spark: SparkSession): org.apache.spark.sql.DataFrame =
    spark.createDataFrame(Vector.empty[Row].asJava, MarkerSchema)

  private final case class FilesOnDisk(total: Long, parquet: Long, deltaLog: Long, bytes: Long)

  private def filesOnDisk(root: Path): FilesOnDisk =
    Using.resource(Files.walk(root)) { paths =>
      paths.iterator().asScala.filter(path => Files.isRegularFile(path)).foldLeft(FilesOnDisk(0L, 0L, 0L, 0L)) {
        (counts, path) =>
          val name = path.getFileName.toString
          counts.copy(
            total = counts.total + 1L,
            parquet = counts.parquet + (if (name.endsWith(".parquet")) 1L else 0L),
            deltaLog = counts.deltaLog + (if (path.toString.contains("_delta_log")) 1L else 0L),
            bytes = counts.bytes + Files.size(path)
          )
      }
    }

  private def runWorkload(spark: SparkSession, root: Path): IO[Unit] = {
    val manifest = AnalyticsRunManifest
      .validated("hal07-fixed-seed-424242", Vector(PartitionOffsetRange(Topic, 0, 0L, TotalRecords.toLong)))
      .toEither
      .fold(errors => fail(errors.toString), identity)
    val paths = AnalyticsLakehousePaths(root.toString)
    for {
      generated <- IO.blocking {
        val generationStart = System.nanoTime()
        val raw = records(spark)
        (raw, (System.nanoTime() - generationStart) / 1000000L)
      }
      (raw, generatedInMs) = generated
      markers <- IO.blocking(emptyMarkers(spark))
      plans <- IO.blocking {
        val valid = OperationalEventTransforms.validEvents(OperationalEventTransforms.parseKafkaRecords(raw))
        val silver = OperationalEventTransforms.silver(valid, Pseudonymizer, markers)
        (
          silver.queryExecution.executedPlan.toString,
          HiringGoldTransforms.wideFunnelDay(silver).queryExecution.executedPlan.toString
        )
      }
      (silverPlan, goldPlan) = plans
      heapBefore <- IO(java.lang.management.ManagementFactory.getMemoryMXBean.getHeapMemoryUsage.getUsed)
      started <- IO.monotonic
      publication <- new HiringAnalyticsBatch(paths, Pseudonymizer, DataFrameDeletionMarkerSource(markers), FixedClock)
        .run(spark, DataFrameBatchSource(raw), manifest)
      elapsedMs <- IO.monotonic.map(now => (now - started).toMillis)
      heapAfter <- IO(java.lang.management.ManagementFactory.getMemoryMXBean.getHeapMemoryUsage.getUsed)
      _ <- IO.blocking {
        assertEquals(publication.outcome, AnalyticsRunOutcome.QualityBlocked)
        assertEquals(publication.bronzeRecords, 100000L)
        assertEquals(publication.validRecords, 99000L)
        assertEquals(publication.suppressedRecords, 0L)
        assertEquals(publication.quarantinedRecords, 2000L)
        assertEquals(publication.conflictingEventIds, 500L)
        assertEquals(spark.read.format("delta").load(paths.bronze).count(), 99000L)
        assertEquals(spark.read.format("delta").load(paths.silver).count(), 97500L)
        assertEquals(spark.read.format("delta").load(paths.quarantine).count(), 2000L)
        assertEquals(
          spark.read.format("delta").load(paths.manifests).select("status").collect().map(_.getString(0)).toSet,
          Set("QUALITY_BLOCKED")
        )
        assert(!DeltaTable.isDeltaTable(spark, paths.funnelGold))

        val files = filesOnDisk(root)
        val runtime = Runtime.getRuntime
        val host = sys.env.getOrElse("HOSTNAME", "unknown")
        val shuffleNodes = (silverPlan + "\n" + goldPlan).linesIterator.count(_.contains("Exchange"))
        println(
          s"""HAL-07 local workload: seed=$Seed host=$host os=${System
              .getProperty("os.name")} java=${System.getProperty(
              "java.version"
            )} scala=${scala.util.Properties.versionNumberString} spark=${spark.version} deltaBuild=4.0.0 cores=${runtime
              .availableProcessors()} shufflePartitions=${spark.conf.get("spark.sql.shuffle.partitions")}
             |generated: total=100000 applicationCreated=80000 applicationStatus=10000 jobCreated=8000 malformed=1000 identicalRetries=500 conflictingRetries=500 eventDays=30 partition=0 offsets=[0,100000)
             |reconciled: source=${publication.bronzeRecords} valid=${publication.validRecords} suppressed=${publication.suppressedRecords} quarantined=${publication.quarantinedRecords} conflictingEventIds=${publication.conflictingEventIds} bronzeDelta=99000 silverDelta=97500 quarantineDelta=2000 outcome=QUALITY_BLOCKED
             |measured: generationMs=$generatedInMs batchMs=$elapsedMs exchangePlanNodes=$shuffleNodes deltaFiles=${files.total} parquetFiles=${files.parquet} deltaLogFiles=${files.deltaLog} storedBytes=${files.bytes} heapBeforeBytes=$heapBefore heapAfterBytes=$heapAfter maxHeapBytes=${runtime
              .maxMemory()}
             |Silver physical plan:
             |$silverPlan
             |Gold funnel physical plan:
             |$goldPlan""".stripMargin
        )
      }
    } yield ()
  }

  if (sys.env.get("HAL07_WORKLOAD").contains("1")) {
    test("fixed 100000 record hiring analytics workload reports reconciliation and resource use") {
      rootResource
        .flatMap(root => sparkResource.map(spark => (root, spark)))
        .use { case (root, spark) =>
          runWorkload(spark, root)
        }
        .unsafeRunSync()
    }
  } else {
    test("fixed 100000 record hiring analytics workload reports reconciliation and resource use".ignore) { () }
  }
}
