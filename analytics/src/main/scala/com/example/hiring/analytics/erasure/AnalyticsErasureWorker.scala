package com.example.hiring.analytics.erasure

import com.example.hiring.analytics.*
import com.example.hiring.analytics.batch.*
import com.example.hiring.analytics.mongo.*

import cats.effect.{Clock, ExitCode, IO, IOApp, Resource}
import cats.Monad
import cats.syntax.all.*
import com.mongodb.client.{MongoClient, MongoClients, MongoDatabase}
import com.example.hiring.analytics.mongo.MongoAnalyticsLakehouseLock
import org.apache.spark.sql.SparkSession
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.security.MessageDigest
import java.time.Instant
import scala.concurrent.duration.*

/** Resumable erasure lifecycle. A marker stays active until both replay horizons have passed. */
final class AnalyticsErasureWorker(
    spark: SparkSession,
    database: MongoDatabase,
    store: AnalyticsErasureStore,
    kafka: KafkaConnection,
    fencerKafka: KafkaConnection,
    topic: String,
    paths: AnalyticsLakehousePaths,
    pseudonymizer: SubjectPseudonymizer,
    publisher: AnalyticsReportPublisher,
    clock: Clock[IO] = Clock[IO],
    leaseDuration: FiniteDuration = 90.seconds,
    deliveryTimeout: FiniteDuration = 30.seconds,
    pollInterval: FiniteDuration = 5.seconds,
    producerFencer: TransactionalProducerFencer = KafkaProducerFencer,
    kafkaRetention: KafkaRetention = KafkaRetentionBarrier.liveRetention
) {
  private val logger = Slf4jLogger.getLogger[IO]
  private val lakehouseLock = new MongoAnalyticsLakehouseLock(database)
  private val markers = new MongoActiveDeletionMarkerSource(database, pseudonymizer)
  private val batch = new HiringAnalyticsBatch(paths, pseudonymizer, markers, lakehouseLock = lakehouseLock)
  private def now: IO[Instant] = clock.realTimeInstant
  private def leaseUntil: IO[Instant] = now.map(_.plusMillis(leaseDuration.toMillis))

  def run: IO[Unit] =
    for {
      _ <- store.preflight
      _ <- batch.validateHmacConfiguration(spark)
      _ <- KafkaRetentionBarrier.capture(kafka, topic)
      _ <- IO.blocking(spark.range(1L).count()).void
      current <- now
      until <- leaseUntil
      _ <- store.heartbeat(current, until)
      _ <- pollForever
    } yield ()

  private[analytics] def pollForever: IO[Unit] =
    for {
      current <- now
      until <- leaseUntil
      _ <- store.heartbeat(current, until)
      claims <- store.claim(current, until, limit = 1)
      _ <- claims.headOption.fold(IO.sleep(pollInterval))(claim =>
        logger.info("claimed analytics erasure request") *> runClaim(claim)
      )
      _ <- pollForever
    } yield ()

  private def runClaim(claim: ErasureClaim): IO[Unit] = {
    val work = for {
      current <- now
      reservation <- publisher.reserve(
        "analytics-erasure-" + claim.requestId,
        sha256("analytics-erasure:" + claim.requestId),
        current
      )
      _ <- process(claim, reservation)
    } yield ()
    IO.race(work, renewForever(claim))
      .flatMap {
        case Left(_)  => IO.unit
        case Right(_) => IO.raiseError(AnalyticsError.ErasureNotReady)
      }
      .handleErrorWith {
        case AnalyticsError.ErasureDeferred => IO.unit
        case AnalyticsError.ErasureNotReady => now.flatMap(store.releaseForOtherRequests(claim, _)).void
        case error                          =>
          val nextAttempt = claim.attemptCount + 1
          val category = error match {
            case _: AnalyticsError.MarkerStorageFailure | _: AnalyticsError.MongoConnectionFailure =>
              ErasureFailureCategory.TransientStorage
            case _: AnalyticsError.SourceReadFailure | _: AnalyticsError.LakehouseFailure =>
              ErasureFailureCategory.TransientSource
            case AnalyticsError.MalformedMarker | AnalyticsError.InvalidGoldSchema | _: AnalyticsError.InvalidConfiguration =>
              ErasureFailureCategory.InvalidState
            case _ => ErasureFailureCategory.Unknown
          }
          val retryable = category match {
            case ErasureFailureCategory.InvalidState => false
            case ErasureFailureCategory.Unknown      => nextAttempt < 3
            case _                                   => nextAttempt < 8
          }
          now.flatMap { current =>
            val scheduled = Option.when(retryable)(current.plusMillis(retryDelay(nextAttempt).toMillis))
            store.recordFailure(claim, category, nextAttempt, scheduled, current).flatMap {
              case true =>
                logger.error(
                  "analytics erasure failed; persisted category=" + category.persistedName + ", attempt=" + nextAttempt
                )
              case false =>
                logger.warn("analytics erasure failure could not be recorded because the lease is no longer owned")
            }
          }
      }
  }

  private def retryDelay(attempt: Int): FiniteDuration =
    math.min(300L, 5L * (1L << math.min(attempt - 1, 6))).seconds

  private def renewForever(claim: ErasureClaim): IO[Nothing] =
    (IO.sleep(leaseDuration / 3) *> (now, leaseUntil).tupled.flatMap { case (current, until) =>
      store.renew(claim, current, until).flatMap {
        case false => IO.raiseError(AnalyticsError.ErasureNotReady)
        case true  => store.heartbeat(current, until)
      }
    }).foreverM

  private[analytics] def process(claim: ErasureClaim, reservation: AnalyticsReportReservation): IO[Unit] =
    Monad[IO].tailRecM(claim)(current => step(current, reservation))

  private def step(
      claim: ErasureClaim,
      reservation: AnalyticsReportReservation
  ): IO[Either[ErasureClaim, Unit]] =
    claim.phase match {
      case ErasurePhase.Requested =>
        for {
          next <- IO.fromEither(
            claim.advanceTo(ErasurePhase.PublisherDrained).leftMap(_ => AnalyticsError.ErasureNotReady)
          )
          transactionalIds <- store.transactionalIds(claim.requestId)
          _ <- producerFencer.fence(fencerKafka, transactionalIds)
          current <- now
          ready <- store.publisherDrainReady(claim.requestId, current, deliveryTimeout)
          _ <- if (ready) IO.unit else defer(claim, pollInterval)
          _ <- advance(claim, ErasurePhase.PublisherDrained)
        } yield Left(next)

      case ErasurePhase.PublisherDrained =>
        for {
          next <- IO.fromEither(claim.advanceTo(ErasurePhase.OutboxPurged).leftMap(_ => AnalyticsError.ErasureNotReady))
          current <- now
          purged <- store.purgeOutbox(claim.requestId, current, deliveryTimeout)
          _ <- if (purged) IO.unit else defer(claim, pollInterval)
          barrier <- store.readBarrier(claim.requestId).flatMap {
            case Some(value) => IO.fromEither(KafkaRetentionBarrier.validate(value))
            case None        =>
              kafkaRetention
                .capture(kafka, topic)
                .flatTap(value =>
                  now
                    .flatMap(store.persistBarrier(claim, value, _))
                    .flatMap(saved => IO.raiseWhen(!saved)(AnalyticsError.ErasureNotReady))
                )
          }
          _ <- IO.fromEither(KafkaRetentionBarrier.validate(barrier))
          _ <- advance(claim, ErasurePhase.OutboxPurged)
        } yield Left(next)

      case ErasurePhase.OutboxPurged =>
        for {
          next <- IO.fromEither(claim.advanceTo(ErasurePhase.DeltaPurged).leftMap(_ => AnalyticsError.ErasureNotReady))
          _ <- purgeAndRecordDelta(claim, spark, reservation.generation)
          _ <- advance(claim, ErasurePhase.DeltaPurged)
        } yield Left(next)

      case ErasurePhase.DeltaPurged =>
        for {
          next <- IO.fromEither(claim.advanceTo(ErasurePhase.GoldRebuilt).leftMap(_ => AnalyticsError.ErasureNotReady))
          barrier <- store
            .readBarrier(claim.requestId)
            .flatMap(
              _.fold[IO[KafkaRetentionBarrier]](
                IO.raiseError(AnalyticsError.MalformedMarker)
              )(IO.pure)
            )
          purgedAt <- store
            .readDeltaPurgedAt(claim.requestId)
            .flatMap(
              _.fold[IO[Instant]](
                IO.raiseError(AnalyticsError.MalformedMarker)
              )(IO.pure)
            )
          ready <- replayHorizonsPassed(barrier, purgedAt)
          _ <- if (ready) IO.unit else defer(claim, 5.minutes)
          markerFrame <- markers.activeSubjectTokens(spark)
          _ <- lakehouseLock.resource(paths.root).use { _ =>
            for {
              _ <- batch.reclaimRetainedFiles(spark)
              _ <- batch.verifyMarkedSubjectsAbsent(spark, markerFrame)
            } yield ()
          }
          affectedRows <- store.readAffectedRows(claim.requestId)
          affectedFiles <- store.readDeltaFiles(claim.requestId)
          _ <- IO.raiseWhen(affectedRows > 0L && affectedFiles.isEmpty)(AnalyticsError.PhysicalReclamationUnverified)
          _ <- batch.verifyFilesAbsent(spark, affectedFiles)
          _ <- advance(claim, ErasurePhase.GoldRebuilt)
        } yield Left(next)

      case ErasurePhase.GoldRebuilt => advance(claim, ErasurePhase.ReadyToPublish).as(Right(()))

      case ErasurePhase.ReadyToPublish =>
        for {
          otherWork <- store.hasNonReadyOtherRequests(claim.requestId)
          _ <- IO.raiseWhen(otherWork)(AnalyticsError.ErasureNotReady)
          current <- now
          refreshed <- publisher.reserve(reservation.runId, reservation.rangeFingerprint, current)
          deltaGeneration <- store.readDeltaGeneration(claim.requestId)
          _ <-
            if (deltaGeneration.contains(refreshed.generation)) IO.unit
            else refreshErasureProjection(claim, refreshed.generation)
          markerFrame <- markers.activeSubjectTokens(spark)
          report <- lakehouseLock.resource(paths.root).use { _ =>
            for {
              _ <- batch.verifyMarkedSubjectsAbsent(spark, markerFrame)
              asOf <- now
              result <- batch.rebuildGoldAndExtractReport(spark, asOf)
            } yield result
          }
          completedAt <- now
          expiry = completedAt.plusSeconds(AnalyticsRetention.PublishedSnapshotDays.toLong * 86400L)
          _ <- publisher.publishErasure(refreshed, report, expiry, claim, completedAt)
          _ <- logger.info("analytics erasure completed and the snapshot was safely revealed")
        } yield Right(())

      case ErasurePhase.ReportPublished => IO.pure(Right(()))
    }

  private def purgeAndRecordDelta(claim: ErasureClaim, spark: SparkSession, generation: Long): IO[Unit] =
    for {
      _ <- lakehouseLock.resource(paths.root).use { _ =>
        for {
          markerFrame <- markers.activeSubjectTokens(spark)
          affectedRows <- batch.countMarkedRows(spark, markerFrame)
          affectedFiles <- batch.captureMarkedFiles(spark, markerFrame)
          countedAt <- now
          counted <- store.persistAffectedRows(claim, affectedRows, countedAt)
          _ <- IO.raiseWhen(!counted)(AnalyticsError.ErasureNotReady)
          filesSaved <- store.persistDeltaFiles(claim, affectedFiles, countedAt)
          _ <- IO.raiseWhen(!filesSaved)(AnalyticsError.ErasureNotReady)
          _ <- batch.applyDeletionMarkers(spark, markerFrame)
          retiredLogs <- batch.checkpointPurgedRawLogs(spark)
          checkpointedAt <- now
          logsSaved <- store.persistDeltaFiles(claim, retiredLogs, checkpointedAt)
          _ <- IO.raiseWhen(!logsSaved)(AnalyticsError.ErasureNotReady)
        } yield ()
      }
      purgedAt <- now
      savedAt <- store.persistDeltaPurgedAt(claim, purgedAt, purgedAt)
      _ <- IO.raiseWhen(!savedAt)(AnalyticsError.ErasureNotReady)
      savedGeneration <- store.persistDeltaGeneration(claim, generation, purgedAt)
      _ <- IO.raiseWhen(!savedGeneration)(AnalyticsError.ErasureNotReady)
    } yield ()

  private def refreshErasureProjection(claim: ErasureClaim, generation: Long): IO[Unit] =
    for {
      _ <- purgeAndRecordDelta(claim, spark, generation)
      barrier <- store
        .readBarrier(claim.requestId)
        .flatMap(
          _.fold[IO[KafkaRetentionBarrier]](
            IO.raiseError(AnalyticsError.MalformedMarker)
          )(IO.pure)
        )
      purgedAt <- store
        .readDeltaPurgedAt(claim.requestId)
        .flatMap(
          _.fold[IO[Instant]](
            IO.raiseError(AnalyticsError.MalformedMarker)
          )(IO.pure)
        )
      ready <- replayHorizonsPassed(barrier, purgedAt)
      _ <- if (ready) IO.unit else defer(claim, 5.minutes)
      currentMarkers <- markers.activeSubjectTokens(spark)
      _ <- lakehouseLock.resource(paths.root).use { _ =>
        batch.reclaimRetainedFiles(spark) *> batch.verifyMarkedSubjectsAbsent(spark, currentMarkers)
      }
      allAffectedRows <- store.readAffectedRows(claim.requestId)
      allAffectedFiles <- store.readDeltaFiles(claim.requestId)
      _ <- IO.raiseWhen(allAffectedRows > 0L && allAffectedFiles.isEmpty)(AnalyticsError.PhysicalReclamationUnverified)
      _ <- batch.verifyFilesAbsent(spark, allAffectedFiles)
    } yield ()

  private def replayHorizonsPassed(barrier: KafkaRetentionBarrier, deltaPurgedAt: Instant): IO[Boolean] =
    for {
      current <- now
      kafkaExpired <- kafkaRetention.retentionPassed(kafka, barrier)
      deltaExpired = !current.isBefore(
        deltaPurgedAt.plusSeconds(AnalyticsRetention.DeltaLogRetentionDays.toLong * 86400L)
      )
    } yield kafkaExpired && deltaExpired

  private def defer(claim: ErasureClaim, delay: FiniteDuration): IO[Unit] =
    now.flatMap { current =>
      store.defer(claim, current.plusMillis(delay.toMillis), current).flatMap {
        case true  => IO.raiseError(AnalyticsError.ErasureDeferred)
        case false => IO.raiseError(AnalyticsError.ErasureNotReady)
      }
    }

  private def advance(claim: ErasureClaim, phase: ErasurePhase): IO[Unit] =
    now.flatMap(store.advance(claim, phase, 0, _)).flatMap {
      case true  => IO.unit
      case false => IO.raiseError(AnalyticsError.ErasureNotReady)
    }

  private def sha256(value: String): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8))
      .map(byte => String.format("%02x", Byte.box(byte)))
      .mkString
}

object AnalyticsErasureWorkerMain extends IOApp {
  private val logger = Slf4jLogger.getLogger[IO]

  private def resources(uri: String, sparkMaster: String): Resource[IO, (SparkSession, MongoClient)] =
    HiringAnalyticsBatchMain.managedResources(
      IO.blocking(
        org.apache.spark.sql.classic.SparkSession
          .builder()
          .appName("hiring-analytics-erasure-worker")
          .master(sparkMaster)
          .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
          .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
          .getOrCreate()
      ),
      IO.blocking(MongoClients.create(uri))
    )

  private def program: IO[Unit] =
    AnalyticsRuntimeConfig.loadWorker.flatMap { configured =>
      val common = configured.common
      resources(common.mongoUri, common.sparkMaster).use { case (spark, client) =>
        IO.blocking(client.getDatabase(common.mongoDatabase)).flatMap { database =>
          val store = new MongoAnalyticsErasureWorkerStore(client, database)
          val publisher = new MongoAnalyticsReportPublisher(client, database)
          new AnalyticsErasureWorker(
            spark,
            database,
            store,
            common.kafka,
            configured.fencerKafka,
            configured.topic,
            common.lakehousePaths,
            common.pseudonymizer,
            publisher
          ).run
        }
      }
    }

  override def run(args: List[String]): IO[ExitCode] =
    (if (args.nonEmpty)
       IO.raiseError[Unit](
         AnalyticsError.InvalidConfiguration(
           "worker settings are loaded from HOCON; command-line arguments are not accepted"
         )
       )
     else program).attempt.flatMap {
      case Right(_)                    => IO.pure(ExitCode.Success)
      case Left(error: AnalyticsError) => logger.error(error.getMessage).as(ExitCode.Error)
      case Left(error)                 =>
        logger.error("analytics erasure worker failed (" + error.getClass.getSimpleName + ")").as(ExitCode.Error)
    }
}
