package com.example.hiring.analytics.cli

import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.all.*
import com.example.hiring.analytics.app.AppModule
import com.example.hiring.analytics.adapter.kafka.{KafkaProducerFencer, KafkaRetentionAdapter}
import com.example.hiring.analytics.adapter.mongo.*
import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.config.*
import com.example.hiring.analytics.domain.{AnalyticsTopic, SubjectPseudonymizer}
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.erasure.*
import com.mongodb.client.MongoClients
import com.mongodb.client.model.{Filters, Updates}
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.delta.{DeltaLog, HiringAnalyticsRetentionClock}
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.{Instant, Duration}
import java.time.temporal.ChronoUnit

/** Test classpath only: advances the Delta calendar after actual short data retention has elapsed. */
object AnalyticsErasureCalendarProofMain extends IOApp {
  private val logger = Slf4jLogger.getLogger[IO]

  private def offset(settings: AnalyticsWorkerSettings, subjectId: String): IO[Long] = IO.blocking {
    val database = settings.common.mongoDatabase
    val topic = AnalyticsTopic.unwrap(settings.topic)
    val isolated =
      (database.matches("hiring_erasure_smoke_[a-f0-9]{16}") &&
        topic == "hiring.erasure.smoke." + database.stripPrefix("hiring_erasure_smoke_")) ||
        (database.matches("account_deletion_[a-f0-9]{16}") &&
          topic == "hiring.deletion." + database.stripPrefix("account_deletion_"))
    require(isolated, "nonce-bound isolated database and topic required")
    require(settings.common.lakehouseRoot == "file:///var/lib/hiring-analytics/lakehouse", "isolated volume required")
    require(java.util.UUID.fromString(subjectId).toString == subjectId, "canonical synthetic subject required")
    val client = MongoClients.create(settings.common.mongoUri)
    try {
      val request = Option(
        client
          .getDatabase(settings.common.mongoDatabase)
          .getCollection("analytics_erasure_requests")
          .find(Filters.eq("_id", subjectId))
          .first()
      )
        .getOrElse(throw new IllegalArgumentException("proof request is absent"))
      require(request.getString("phase") == "DeltaPurged", "restart must begin at durable DeltaPurged")
      val purged = Option(request.getDate("deltaPurgedAt"))
        .map(_.toInstant)
        .getOrElse(throw new IllegalArgumentException("purge timestamp is absent"))
      val wall = Instant.now()
      require(
        !wall.isBefore(purged.plusMillis(settings.common.operational.retention.deltaVacuumSafety.toMillis)),
        "actual data-file retention has not elapsed"
      )
      val calendar = purged
        .truncatedTo(ChronoUnit.DAYS)
        .plus(2, ChronoUnit.DAYS)
        .plusMillis(settings.common.operational.retention.deltaLogRetention.toMillis)
      val shift = math.max(0L, Duration.between(wall, calendar).toMillis)
      client
        .getDatabase(settings.common.mongoDatabase)
        .getCollection("analytics_retention_proof")
        .updateOne(
          Filters.eq("subjectId", subjectId),
          Updates.combine(
            Updates.set("simulatedCalendarShiftMillis", shift),
            Updates.set("calendarRestartedAt", java.util.Date.from(wall))
          )
        )
      shift
    } finally client.close()
  }

  override def run(args: List[String]): IO[ExitCode] = args match {
    case List(subjectId) =>
      (for {
        settings <- AnalyticsRuntimeConfig.loadWorker[IO]
        shift <- offset(settings, subjectId)
        _ <- IO.println(
          s"ERASURE_CALENDAR_PROOF simulatedCalendarShiftMillis=$shift dataRetention=real kafkaRetention=real"
        )
        now = IO.realTimeInstant.map(_.plusMillis(shift))
        factory = new DeltaLogFactory {
          override def apply(spark: SparkSession, path: String): DeltaLog =
            HiringAnalyticsRetentionClock.forTable(spark, path, shift)
        }
        paths <- IO.fromEither(AppModule.resolveLakehousePaths(settings.common.lakehouseRoot))
        pseudonymizer <- IO.fromEither(
          SubjectPseudonymizer
            .validateFromBase64(
              Some(settings.common.hmac.secretBase64),
              settings.common.hmac.keyId,
              settings.common.hmac.previousKeyId,
              settings.common.hmac.previousSecretBase64
            )
            .toEither
            .leftMap(errors => AnalyticsError.InvalidConfiguration(errors.toChain.toList.mkString("; ")))
        )
        _ <- AppModule
          .sparkMongo[IO](
            settings.common.mongoUri,
            settings.common.sparkMaster,
            "hiring-erasure-calendar-proof",
            sparkLocalDirectory = settings.common.sparkLocalDirectory
          )
          .use { case (spark, client, execution) =>
            for {
              database <- client.getDatabase(settings.common.mongoDatabase)
              streams = new MongoPublisherStream(settings.common.operational)
              lock = new MongoAnalyticsLakehouseLock[IO](database, streams, Some(now))
              lakehouseExecution = new LakehouseOperation[IO](execution)
              maintenance = new DeltaAnalyticsErasureLakehouse[IO](
                spark,
                paths,
                pseudonymizer,
                lock,
                new MongoHmacKeyRetirementAuthorizationStore[IO](database, streams),
                settings.common.operational,
                lakehouseExecution,
                logger,
                Some(now),
                factory
              )
              _ <- MongoAnalyticsErasureStores.resource(client, database, streams).use { stores =>
                new AnalyticsErasureWorker[IO](
                  stores.queue,
                  stores.progress,
                  stores.barrier,
                  AnalyticsErasureKafkaRuntime(
                    settings.fencerKafka,
                    KafkaProducerFencer[IO](execution),
                    KafkaRetentionAdapter.liveRetention[IO](settings.common.kafka, settings.topic, execution)
                  ),
                  paths,
                  new MongoAnalyticsReportPublisher[IO](client, database, settings.common.operational),
                  new MongoActiveDeletionMarkerSource[IO](database, pseudonymizer, streams),
                  maintenance,
                  lock,
                  logger,
                  AnalyticsErasureWorkerPolicy(
                    settings.common.operational.retention,
                    settings.common.operational.erasureWorkerTimings
                  ),
                  Some(now)
                ).run
              }
            } yield ()
          }
      } yield ExitCode.Success).handleErrorWith { error =>
        logger.error(s"erasure calendar proof failed (${error.getClass.getSimpleName})").as(ExitCode.Error)
      }
    case _ => logger.error("expected a canonical synthetic subject UUID").as(ExitCode.Error)
  }
}
