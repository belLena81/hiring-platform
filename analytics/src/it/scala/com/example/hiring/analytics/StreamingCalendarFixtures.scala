package com.example.hiring.analytics

import cats.effect.IO
import com.example.hiring.analytics.HiringAnalyticsRecoveryTestSupport.Harness
import com.example.hiring.analytics.app.StreamingDurableBoundaryObserver
import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.service.streaming.*
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, encode, lit}
import org.apache.spark.sql.types.*

import java.sql.Timestamp
import java.time.Instant
import scala.jdk.CollectionConverters.*

/** Hiring event batches with controlled event times, processed through the real streaming coordinator. */
private[analytics] object StreamingCalendarFixtures {
  def processAt(
      harness: Harness,
      frame: DataFrame,
      index: Long,
      observed: Instant,
      records: Int = 12,
      observer: Option[StreamingDurableBoundaryObserver[IO]] = None,
      priorWatermark: Option[Option[Instant]] = None
  ): IO[StreamingCoordinatorResult] =
    harness.lock.resource(harness.paths.root).use { _ =>
      for {
        _ <- harness.maintenance.validateHmacConfigurationLocked
        plain = harness.journal
        store = observer.fold[StreamingBatchJournal[IO]](plain)(StreamingDurableBoundaryObserver.journal(plain, _))
        prior <- priorWatermark.fold(plain.latestWatermark(harness.preparation().identity.lineage))(IO.pure)
        preparation = harness
          .preparation(index)
          .copy(
            observedAt = observed,
            priorWatermark = prior,
            inputFingerprint = RangeFingerprint
              .from(AnalyticsDigest.sha256Hex(s"calendar-source-$index-$records".getBytes("UTF-8")))
              .toOption
              .get,
            sourceEndOffsets = (0 until 3).toVector
              .map(p => StreamingPartitionEndOffset.from(harness.topic, p, index * 4L + records / 3L).toOption.get),
            deliveredOffsets =
              if (records == 0) Vector.empty
              else
                (0 until 3).toVector.map(p =>
                  StreamingPartitionSummary
                    .from(harness.topic, p, index * 4L, index * 4L + records / 3L - 1L, records / 3L)
                    .toOption
                    .get
                )
          )
        sink = observer.fold[DeltaWriter[IO]](harness.writer)(
          StreamingDurableBoundaryObserver.writer(harness.writer, harness.paths, preparation.identity, _)
        )
        result <- harness
          .streamingStages(frame, sink)
          .map(stages => observer.fold(stages)(StreamingDurableBoundaryObserver.stages(stages, _)))
          .use(stages =>
            new StreamingBatchCoordinator[IO](store, harness.markers, stages, harness.checkpoint(store), IO.unit)
              .process(preparation)
          )
      } yield result
    }

  def timedFrame(
      harness: Harness,
      batch: Long,
      occurred: Instant,
      eventType: String = "JOB_CREATED",
      skill: String = "Scala",
      distinctSubjects: Int = 12,
      eventKey: Option[Long] = None,
      occurredAt: Option[Int => Instant] = None
  ): IO[DataFrame] = harness.execution {
    val key = eventKey.getOrElse(batch)
    val rows = (0 until 12).map { n =>
      val application = AnalyticsOperationalEventFixtures.id(s"calendar-application-$n")
      val aggregate =
        if (eventType == "JOB_CREATED") AnalyticsOperationalEventFixtures.id(s"calendar-job-$key-$n")
        else application
      val aggregateType = if (eventType == "JOB_CREATED") "Job" else "Application"
      val actor = harness.subjects(n % distinctSubjects)
      val job = AnalyticsOperationalEventFixtures.id("calendar-job")
      val eventId = AnalyticsOperationalEventFixtures.id(s"calendar-event-$key-$n")
      val at = occurredAt.fold(occurred)(_(n))
      val payload = eventType match {
        case "JOB_CREATED" =>
          io.circe.Json.obj(
            "job" -> io.circe.Json.obj(
              "jobId" -> io.circe.Json.fromString(aggregate),
              "status" -> io.circe.Json.fromString("Open"),
              "skills" -> io.circe.Json.arr(io.circe.Json.fromString(skill))
            )
          )
        case "APPLICATION_CREATED" | "APPLICATION_STATUS_CHANGED" =>
          val identity = io.circe.Json.obj(
            "applicationId" -> io.circe.Json.fromString(application),
            "candidateId" -> io.circe.Json.fromString(actor),
            "jobId" -> io.circe.Json.fromString(job)
          )
          if (eventType == "APPLICATION_CREATED")
            identity.deepMerge(
              io.circe.Json.obj("status" -> io.circe.Json.fromString("Created"))
            )
          else
            identity.deepMerge(
              io.circe.Json.obj(
                "previousStatus" -> io.circe.Json.fromString("Interview"),
                "newStatus" -> io.circe.Json.fromString("Hired")
              )
            )
        case unsupported => throw new IllegalArgumentException(s"Unsupported timed fixture event: $unsupported")
      }
      val raw =
        s"""{"eventId":"$eventId","eventType":"$eventType","occurredAt":"$at","aggregateType":"$aggregateType","aggregateId":"$aggregate","actorId":"$actor","payload":${payload.noSpaces}}"""
      Row(harness.topic, n % 3, batch * 4L + n / 3L, raw)
    }
    val schema = StructType(
      Vector(
        StructField("topic", StringType, false),
        StructField("partition", IntegerType, false),
        StructField("offset", LongType, false),
        StructField("value", StringType, true)
      )
    )
    val source = harness.spark
      .createDataFrame(rows.asJava, schema)
      .withColumn("value", encode(col("value"), "UTF-8"))
      .withColumn("timestamp", lit(Timestamp.from(occurred)))
    OperationalEventTransforms.parseKafkaRecords(source)
  }
}
