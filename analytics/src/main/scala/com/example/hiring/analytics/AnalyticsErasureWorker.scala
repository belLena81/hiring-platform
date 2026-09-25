package com.example.hiring.analytics

import cats.effect.{Clock, ExitCode, IO, IOApp, Resource}
import cats.syntax.all.*
import com.mongodb.client.{MongoClient, MongoClients, MongoDatabase}
import org.apache.spark.sql.SparkSession
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.security.MessageDigest
import java.time.Instant
import scala.concurrent.duration.*
import scala.util.control.NonFatal

/** Resumable erasure lifecycle. A marker stays active until both replay horizons have passed. */
final class AnalyticsErasureWorker(
    spark: SparkSession,
    database: MongoDatabase,
    store: MongoAnalyticsErasureWorkerStore,
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
  private val markers = new MongoActiveDeletionMarkerSource(database, pseudonymizer)
  private val batch = new HiringAnalyticsBatch(paths, pseudonymizer, markers)
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
          logger.error(
            "analytics erasure attempt failed (" + error.getClass.getSimpleName + "); durable claim will be retried after lease expiry"
          )
      }
  }

  private def renewForever(claim: ErasureClaim): IO[Nothing] =
    (IO.sleep(leaseDuration / 3) *> (now, leaseUntil).tupled.flatMap { case (current, until) =>
      store.renew(claim, current, until).flatMap {
        case false => IO.raiseError(AnalyticsError.ErasureNotReady)
        case true  => store.heartbeat(current, until)
      }
    }).foreverM

  private[analytics] def process(claim: ErasureClaim, reservation: AnalyticsReportReservation): IO[Unit] =
    claim.phase match {
      case ErasurePhase.Requested =>
        for {
          transactionalIds <- store.transactionalIds(claim.requestId)
          _ <- producerFencer.fence(fencerKafka, transactionalIds)
          current <- now
          ready <- store.publisherDrainReady(claim.requestId, current, deliveryTimeout)
          _ <- if (ready) IO.unit else defer(claim, pollInterval)
          _ <- advance(claim, ErasurePhase.PublisherDrained)
          next = claim.copy(
            phase = ErasurePhase.PublisherDrained,
            progress = 0,
            progressKey = phaseKey(ErasurePhase.PublisherDrained)
          )
          _ <- process(next, reservation)
        } yield ()

      case ErasurePhase.PublisherDrained =>
        for {
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
          next = claim.copy(
            phase = ErasurePhase.OutboxPurged,
            progress = 0,
            progressKey = phaseKey(ErasurePhase.OutboxPurged)
          )
          _ <- process(next, reservation)
        } yield ()

      case ErasurePhase.OutboxPurged =>
        for {
          _ <- AnalyticsLakehouseLock.resource(paths.root).use { _ =>
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
          saved <- store.persistDeltaPurgedAt(claim, purgedAt, purgedAt)
          _ <- IO.raiseWhen(!saved)(AnalyticsError.ErasureNotReady)
          generationSaved <- store.persistDeltaGeneration(claim, reservation.generation, purgedAt)
          _ <- IO.raiseWhen(!generationSaved)(AnalyticsError.ErasureNotReady)
          _ <- advance(claim, ErasurePhase.DeltaPurged)
          next = claim.copy(
            phase = ErasurePhase.DeltaPurged,
            progress = 0,
            progressKey = phaseKey(ErasurePhase.DeltaPurged)
          )
          _ <- process(next, reservation)
        } yield ()

      case ErasurePhase.DeltaPurged =>
        for {
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
          _ <- AnalyticsLakehouseLock.resource(paths.root).use { _ =>
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
          next = claim.copy(
            phase = ErasurePhase.GoldRebuilt,
            progress = 0,
            progressKey = phaseKey(ErasurePhase.GoldRebuilt)
          )
          _ <- process(next, reservation)
        } yield ()

      case ErasurePhase.GoldRebuilt => advance(claim, ErasurePhase.ReadyToPublish)

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
          report <- AnalyticsLakehouseLock.resource(paths.root).use { _ =>
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
        } yield ()

      case ErasurePhase.ReportPublished => IO.unit
    }

  private def refreshErasureProjection(claim: ErasureClaim, generation: Long): IO[Unit] =
    for {
      _ <- AnalyticsLakehouseLock.resource(paths.root).use { _ =>
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
      barrier <- store
        .readBarrier(claim.requestId)
        .flatMap(
          _.fold[IO[KafkaRetentionBarrier]](
            IO.raiseError(AnalyticsError.MalformedMarker)
          )(IO.pure)
        )
      ready <- replayHorizonsPassed(barrier, purgedAt)
      _ <- if (ready) IO.unit else defer(claim, 5.minutes)
      currentMarkers <- markers.activeSubjectTokens(spark)
      _ <- AnalyticsLakehouseLock.resource(paths.root).use { _ =>
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

  private def phaseKey(phase: ErasurePhase): Long =
    phase.ordinal.toLong * MongoAnalyticsErasureWorkerStore.ProgressPerPhase

  private def sha256(value: String): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8))
      .map(byte => String.format("%02x", Byte.box(byte)))
      .mkString
}

object AnalyticsErasureWorkerMain extends IOApp {
  private val logger = Slf4jLogger.getLogger[IO]

  private def required(name: String): IO[String] =
    IO.delay(sys.env.get(name).filter(_.trim.nonEmpty))
      .flatMap(
        _.fold[IO[String]](IO.raiseError(AnalyticsError.InvalidConfiguration(name + " is required")))(IO.pure)
      )

  private def resources(uri: String): Resource[IO, (SparkSession, MongoClient)] =
    HiringAnalyticsBatchMain.managedResources(
      IO.blocking(
        org.apache.spark.sql.classic.SparkSession
          .builder()
          .appName("hiring-analytics-erasure-worker")
          .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
          .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
          .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
          .getOrCreate()
      ),
      IO.blocking(MongoClients.create(uri))
    )

  private def program: IO[Unit] =
    for {
      mongoUri <- required("MONGODB_URI")
      databaseName <- IO.delay(sys.env.getOrElse("MONGODB_DATABASE", "hiring"))
      root <- required("ANALYTICS_LAKEHOUSE_ROOT")
      topic <- required("ANALYTICS_TOPIC")
      brokers <- required("ANALYTICS_BOOTSTRAP_SERVERS")
      username <- required("ANALYTICS_KAFKA_USERNAME")
      password <- required("ANALYTICS_KAFKA_PASSWORD")
      fencerUsername <- required("ANALYTICS_KAFKA_FENCER_USERNAME")
      fencerPassword <- required("ANALYTICS_KAFKA_FENCER_PASSWORD")
      secret <- required("HIRING_ANALYTICS_HMAC_SECRET_BASE64")
      keyId <- IO.delay(sys.env.getOrElse("HIRING_ANALYTICS_HMAC_KEY_ID", "hmac-v1"))
      previousHmac <- IO.delay(
        sys.env.get("HIRING_ANALYTICS_HMAC_PREVIOUS_KEY_ID").filter(_.trim.nonEmpty) ->
          sys.env.get("HIRING_ANALYTICS_HMAC_PREVIOUS_SECRET_BASE64").filter(_.trim.nonEmpty)
      )
      paths <- IO.fromEither(
        AnalyticsLakehousePaths
          .validate(AnalyticsLakehousePaths(root))
          .toEither
          .leftMap(AnalyticsError.InvalidInput.apply)
      )
      connection <- IO.fromEither(
        KafkaConnection
          .validate(KafkaConnection(brokers, Some(username), Some(password)))
          .toEither
          .leftMap(AnalyticsError.InvalidInput.apply)
      )
      fencerConnection <- IO.fromEither(
        KafkaConnection
          .validate(KafkaConnection(brokers, Some(fencerUsername), Some(fencerPassword)))
          .toEither
          .leftMap(AnalyticsError.InvalidInput.apply)
      )
      pseudonymizer <- IO
        .delay(
          SubjectPseudonymizer.fromBase64(secret, keyId, previousHmac._1, previousHmac._2)
        )
        .adaptError { case NonFatal(_) =>
          AnalyticsError.InvalidConfiguration("HIRING_ANALYTICS_HMAC_SECRET_BASE64 is invalid")
        }
      _ <- resources(mongoUri).use { case (spark, client) =>
        IO.blocking(client.getDatabase(databaseName)).flatMap { database =>
          val store = new MongoAnalyticsErasureWorkerStore(client, database)
          val publisher = new MongoAnalyticsReportPublisher(client, database)
          new AnalyticsErasureWorker(
            spark,
            database,
            store,
            connection,
            fencerConnection,
            topic,
            paths,
            pseudonymizer,
            publisher
          ).run
        }
      }
    } yield ()

  override def run(args: List[String]): IO[ExitCode] =
    program.attempt.flatMap {
      case Right(_)                    => IO.pure(ExitCode.Success)
      case Left(error: AnalyticsError) => logger.error(error.getMessage).as(ExitCode.Error)
      case Left(error)                 =>
        logger.error("analytics erasure worker failed (" + error.getClass.getSimpleName + ")").as(ExitCode.Error)
    }
}
