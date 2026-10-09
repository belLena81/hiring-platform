package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.adapter.kafka.KafkaClientProperties
import com.example.hiring.analytics.config.{AnalyticsStreamingSettings, KafkaConnection}
import com.example.hiring.analytics.domain.{
  AnalyticsDigest,
  AnalyticsOffset,
  AnalyticsPartition,
  AnalyticsTopic,
  StreamingActivationIdentity,
  StreamingBatchId
}
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.errors.AnalyticsErrorTranslation.translating
import com.example.hiring.analytics.service.batch.{AnalyticsLakehouseLock, AnalyticsStreamingRegistry}
import com.example.hiring.analytics.service.streaming.StreamingActivationGate
import com.example.hiring.analytics.service.streaming.StreamingCheckpointAcknowledgement

import cats.effect.{Async, Deferred, Resource}
import cats.effect.syntax.all.*
import cats.effect.std.Dispatcher
import cats.syntax.all.*
import io.circe.Json
import org.apache.hadoop.fs.FileAlreadyExistsException
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

import java.util.concurrent.atomic.AtomicReference
import java.nio.charset.StandardCharsets
import java.time.Instant
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.*
import scala.util.control.NonFatal

/** Owns a Structured Streaming query; callback effects are bridged only at Spark's synchronous adapter boundary. */
private[analytics] final class SparkHiringAnalyticsStream[F[_]: Async](
    spark: SparkSession,
    execution: SparkExecution[F],
    driverExecution: SparkBlockingExecution[F],
    connection: KafkaConnection,
    topic: AnalyticsTopic,
    settings: AnalyticsStreamingSettings,
    activationGate: StreamingActivationGate[F],
    streamingRegistry: AnalyticsStreamingRegistry[F],
    lakehouseLock: AnalyticsLakehouseLock[F],
    lakehouseRoot: String,
    checkpointAcknowledgement: StreamingCheckpointAcknowledgement[F],
    processBatch: (
        DataFrame,
        StreamingBatchId,
        com.example.hiring.analytics.domain.StreamingLineage,
        Map[(String, Int), Long],
        F[Unit]
    ) => F[Unit],
    resolveSourceIdentity: () => F[(String, String)],
    maintenance: Option[
      (
          StreamingActivationIdentity,
          com.example.hiring.analytics.domain.StreamingLineage,
          () => F[Vector[com.example.hiring.analytics.service.streaming.StreamingCheckpointBatch]],
          F[Unit]
      ) => Resource[F, F[Unit]]
    ] = None,
    queryFactoryOverride: Option[StreamingQueryFactory[F]] = None
) {
  private val effect = Async[F]
  private val activeCallbackCancellation = new AtomicReference[Option[() => Future[Unit]]](None)
  private val callbackLifecycle = new Object
  private var stopping = false

  def resource: Resource[F, Unit] =
    Resource
      .eval(resolveSourceIdentity())
      .flatMap { case (clusterId, topicId) =>
        Resource
          .eval(
            effect
              .fromEither(settings.activationIdentity(clusterId, topicId, AnalyticsTopic.unwrap(topic), lakehouseRoot))
          )
          .flatMap { identity =>
            Resource.eval(activationGate.requireAuthorized(identity, settings.activationGrantId)) *> Resource.eval(
              lakehouseLock
                .resource(lakehouseRoot)
                .use(_ =>
                  activationGate.requireAuthorized(identity, settings.activationGrantId) *>
                    streamingRegistry.registerLakehouse(lakehouseRoot) *>
                    activationGate.requireAuthorized(identity, settings.activationGrantId).void
                )
            ) *> streamOwnerResource *> Resource
              .eval(
                activationGate.requireAuthorized(identity, settings.activationGrantId)
              )
              .flatMap { grantExpiresAt =>
                Dispatcher.parallel[F].flatMap { dispatcher =>
                  val factory = queryFactoryOverride.getOrElse(new StreamingQueryFactory[F] {
                    override def start: F[StreamingQueryHandle[F]] =
                      SparkHiringAnalyticsStream.this.start(dispatcher, identity).map { query =>
                        new StreamingQueryHandle[F] {
                          override def awaitTermination: F[Unit] =
                            effect.interruptibleMany(query.awaitTermination()).void
                          override def stop: F[Unit] = SparkHiringAnalyticsStream.this.stop(query)
                        }
                      }
                  })
                  Resource.eval(lineageFor(identity)).flatMap { lineage =>
                    val authorize = activationGate.requireAuthorized(identity, settings.activationGrantId).void
                    val background = maintenance.fold(Resource.pure[F, F[Unit]](effect.never[Unit]))(run =>
                      run(identity, lineage, () => checkpointBatches, authorize)
                    )
                    StreamingQueryLifecycle.resource(factory, failAtGrantExpiry(grantExpiresAt), background)
                  }
                }
              }
          }
      }

  private def streamOwnerResource: Resource[F, Unit] =
    Resource
      .eval(effect.fromEither(AnalyticsStreamingRegistry.ownerLockRoot(lakehouseRoot)))
      .flatMap(ownerRoot => lakehouseLock.resource(ownerRoot))
      .void

  private def start(dispatcher: Dispatcher[F], activationIdentity: StreamingActivationIdentity): F[StreamingQuery] =
    for {
      _ <- effect.delay {
        callbackLifecycle.synchronized {
          stopping = false
          activeCallbackCancellation.set(None)
        }
      }
      partitions <- KafkaOffsetRangeSource.availablePartitions(connection, topic, driverExecution)
      _ <- effect.fromEither(AnalyticsStreamingSettings.validatePartitionCoverage(settings, partitions))
      checkpointEstablished <- validateCheckpointIdentity(activationIdentity)
      checkpointBatches <- checkpointBatches
      lineage <- lineageFor(activationIdentity)
      _ <- checkpointAcknowledgement.reconcile(lineage, checkpointBatches, checkpointEstablished)
      properties <- effect.fromEither(KafkaClientProperties.sparkOptions(connection))
      _ <- activationGate.requireAuthorized(activationIdentity, settings.activationGrantId).void
      queryIdentityReady <- Deferred[F, Either[Throwable, Unit]]
      query <- execution {
        spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", connection.bootstrapServers)
          .options(properties)
          .option("kafka.group.id", AnalyticsStreamingSettings.ConsumerGroupId)
          .option("subscribe", AnalyticsTopic.unwrap(topic))
          .option("startingOffsets", startingOffsets)
          .option("maxOffsetsPerTrigger", (settings.maxOffsetsPerTrigger: Int))
          .option("failOnDataLoss", "true")
          .option("kafka.isolation.level", "read_committed")
          .load()
          .writeStream
          .queryName(settings.streamId)
          .option("checkpointLocation", SparkPhysicalLocation.resolve(settings.checkpointLocation))
          .trigger(Trigger.ProcessingTime(settings.triggerInterval))
          .foreachBatch((frame: DataFrame, batchNumber: Long) =>
            runCallback(dispatcher, frame, batchNumber, lineage, activationIdentity, queryIdentityReady)
          )
          .start()
      }
        .translating(AnalyticsError.SourceReadFailure(_))
        .handleErrorWith { error =>
          queryIdentityReady.complete(Left(error)).void *>
            stopQueryByName.attempt.flatMap {
              case Right(_)           => effect.raiseError[StreamingQuery](error)
              case Left(cleanupError) =>
                effect.delay(cleanupError.addSuppressed(error)) *>
                  effect.raiseError[StreamingQuery](AnalyticsError.LakehouseFailure(cleanupError))
            }
        }
        .onCancel(
          queryIdentityReady.complete(Left(invalidCheckpoint)).void *>
            stopQueryByName
        )
      _ <- (persistQueryIdentity(activationIdentity, query.id.toString) *>
        queryIdentityReady.complete(Right(())).void)
        .handleErrorWith { error =>
          queryIdentityReady.complete(Left(error)).void *>
            stop(query) *>
            effect.raiseError[Unit](error)
        }
        .onCancel(
          queryIdentityReady.complete(Left(invalidCheckpoint)).void *>
            stop(query)
        )
    } yield query

  private def lineageFor(
      activationIdentity: StreamingActivationIdentity
  ): F[com.example.hiring.analytics.domain.StreamingLineage] =
    effect.fromEither(
      com.example.hiring.analytics.domain.StreamingLineage
        .from(AnalyticsDigest.sha256Hex(activationIdentity.canonical))
        .leftMap(_ => invalidCheckpoint)
    )

  private def validateCheckpointIdentity(activationIdentity: StreamingActivationIdentity): F[Boolean] =
    execution
      .either {
        val checkpoint = new Path(SparkPhysicalLocation.resolve(settings.checkpointLocation))
        val fileSystem = checkpoint.getFileSystem(spark.sparkContext.hadoopConfiguration)
        val identityFile = new Path(checkpoint, "_hiring_stream_identity")
        val expected = activationIdentity.canonical
        val lineageDirectory =
          new Path(new Path(SparkPhysicalLocation.resolve(lakehouseRoot)), "control/streaming_lineage")
        val lineageFile = lineagePath(lineageDirectory, activationIdentity)
        val establishedFile = establishedPath(lineageDirectory, activationIdentity)
        val queryIdentityFile = queryIdentityPath(lineageDirectory, activationIdentity)

        def readExisting: Either[AnalyticsError, Unit] =
          Either
            .catchNonFatal {
              val input = fileSystem.open(identityFile)
              try new String(input.readAllBytes(), StandardCharsets.UTF_8)
              finally input.close()
            }
            .leftMap(_ => invalidCheckpoint)
            .flatMap(identity => Either.cond(identity == expected, (), invalidCheckpoint))

        def writeNewIdentity(): Either[AnalyticsError, Unit] =
          try {
            val output = fileSystem.create(identityFile, false)
            try {
              output.write(expected.getBytes(StandardCharsets.UTF_8))
              output.hflush()
              output.hsync()
            } finally output.close()
            Right(())
          } catch {
            case _: FileAlreadyExistsException => readExisting
            case NonFatal(_)                   => Left(invalidCheckpoint)
          }

        def validateEstablishedCheckpoint(): Either[AnalyticsError, Unit] = {
          val metadata = new Path(checkpoint, "metadata")
          val offsets = new Path(checkpoint, "offsets")
          val commits = new Path(checkpoint, "commits")
          Either
            .catchNonFatal(
              fileSystem.getFileStatus(metadata).isFile &&
                fileSystem.getFileStatus(offsets).isDirectory &&
                fileSystem.getFileStatus(commits).isDirectory
            )
            .leftMap(_ => invalidCheckpoint)
            .flatMap(valid => Either.cond(valid, (), invalidCheckpoint))
        }

        val registeredIdentities = Either
          .catchNonFatal {
            if (!fileSystem.exists(lineageDirectory)) Vector.empty
            else
              fileSystem
                .listStatus(lineageDirectory)
                .filter(_.isFile)
                .filter(_.getPath.getName.endsWith(".identity"))
                .toVector
                .map { status =>
                  val input = fileSystem.open(status.getPath)
                  try new String(input.readAllBytes(), StandardCharsets.UTF_8)
                  finally input.close()
                }
          }
          .leftMap(_ => invalidCheckpoint)
        registeredIdentities.flatMap { identities =>
          val previousIdentityMatches = identities.forall(_ == expected)
          val previouslyRegistered =
            fileSystem.exists(lineageFile) || identities.nonEmpty || fileSystem.exists(establishedFile)
          if (!previousIdentityMatches) Left(invalidCheckpoint)
          else if (!fileSystem.exists(checkpoint)) {
            if (fileSystem.exists(establishedFile)) Left(invalidCheckpoint)
            else if (!fileSystem.mkdirs(checkpoint)) Left(invalidCheckpoint)
            else writeNewIdentity().map(_ => false)
          } else if (previouslyRegistered && !fileSystem.exists(identityFile)) Left(invalidCheckpoint)
          else if (!fileSystem.exists(identityFile)) {
            if (fileSystem.listStatus(checkpoint).nonEmpty) Left(invalidCheckpoint)
            else writeNewIdentity().map(_ => false)
          } else
            readExisting.flatMap { _ =>
              val commits = new Path(checkpoint, "commits")
              val hasCommittedBatches = fileSystem.exists(commits) && fileSystem.listStatus(commits).exists(_.isFile)
              val established = fileSystem.exists(establishedFile) || hasCommittedBatches
              val checkpointState = if (!established) Right(()) else validateEstablishedCheckpoint()
              checkpointState
                .flatMap(_ => Either.cond(!established || fileSystem.exists(queryIdentityFile), (), invalidCheckpoint))
                .map(_ => established)
            }
        }
      }
      .flatMap { established =>
        activationGate.requireAuthorized(activationIdentity, settings.activationGrantId) *>
          persistLineage(activationIdentity).as(established)
      }

  private def checkpointBatches: F[Vector[com.example.hiring.analytics.service.streaming.StreamingCheckpointBatch]] =
    execution.either {
      val checkpoint = new Path(SparkPhysicalLocation.resolve(settings.checkpointLocation))
      val fileSystem = checkpoint.getFileSystem(spark.sparkContext.hadoopConfiguration)
      SparkCheckpointLogs.read(
        fileSystem,
        checkpoint,
        AnalyticsTopic.unwrap(topic),
        settings.initialOffsets.map(offset => AnalyticsPartition.unwrap(offset.partition)).toSet
      )
    }

  private def registerCheckpointLineage(identity: StreamingActivationIdentity): F[Unit] =
    execution
      .either {
        val checkpoint = new Path(SparkPhysicalLocation.resolve(settings.checkpointLocation))
        val fileSystem = checkpoint.getFileSystem(spark.sparkContext.hadoopConfiguration)
        val metadata = new Path(checkpoint, "metadata")
        val offsets = new Path(checkpoint, "offsets")
        val commits = new Path(checkpoint, "commits")
        val established = Either
          .catchNonFatal(
            fileSystem.getFileStatus(metadata).isFile &&
              fileSystem.getFileStatus(offsets).isDirectory &&
              fileSystem.getFileStatus(commits).isDirectory
          )
          .leftMap(_ => invalidCheckpoint)
          .flatMap(valid => Either.cond(valid, (), invalidCheckpoint))
        established
      }
      .flatMap(_ => persistLineage(identity) *> persistEstablished(identity))

  private def persistEstablished(identity: StreamingActivationIdentity): F[Unit] = execution.either {
    val fileSystem =
      new Path(SparkPhysicalLocation.resolve(lakehouseRoot)).getFileSystem(spark.sparkContext.hadoopConfiguration)
    val directory = new Path(new Path(SparkPhysicalLocation.resolve(lakehouseRoot)), "control/streaming_lineage")
    val file = establishedPath(directory, identity)
    val expected = identity.canonical
    try {
      if (!fileSystem.mkdirs(directory) && !fileSystem.exists(directory)) Left(invalidCheckpoint)
      else if (fileSystem.exists(file)) {
        val input = fileSystem.open(file)
        val value = try new String(input.readAllBytes(), StandardCharsets.UTF_8)
        finally input.close()
        Either.cond(value == expected, (), invalidCheckpoint)
      } else {
        val output = fileSystem.create(file, false)
        try {
          output.write(expected.getBytes(StandardCharsets.UTF_8))
          output.hflush()
          output.hsync()
        } finally output.close()
        Right(())
      }
    } catch {
      case _: FileAlreadyExistsException =>
        Either
          .catchNonFatal {
            val input = fileSystem.open(file)
            try new String(input.readAllBytes(), StandardCharsets.UTF_8)
            finally input.close()
          }
          .leftMap(_ => invalidCheckpoint)
          .flatMap(value => Either.cond(value == expected, (), invalidCheckpoint))
      case NonFatal(_) => Left(invalidCheckpoint)
    }
  }

  private def persistQueryIdentity(identity: StreamingActivationIdentity, queryId: String): F[Unit] = execution.either {
    val fileSystem =
      new Path(SparkPhysicalLocation.resolve(lakehouseRoot)).getFileSystem(spark.sparkContext.hadoopConfiguration)
    val directory = new Path(new Path(SparkPhysicalLocation.resolve(lakehouseRoot)), "control/streaming_lineage")
    val file = queryIdentityPath(directory, identity)
    val expected = scala.util.Try(java.util.UUID.fromString(queryId).toString).toOption.toRight(invalidCheckpoint)
    expected.flatMap { normalized =>
      try {
        if (!fileSystem.mkdirs(directory) && !fileSystem.exists(directory)) Left(invalidCheckpoint)
        else if (fileSystem.exists(file)) {
          val input = fileSystem.open(file)
          val existing = try new String(input.readAllBytes(), StandardCharsets.UTF_8)
          finally input.close()
          Either.cond(existing == normalized, (), invalidCheckpoint)
        } else {
          val output = fileSystem.create(file, false)
          try {
            output.write(normalized.getBytes(StandardCharsets.UTF_8))
            output.hflush()
            output.hsync()
          } finally output.close()
          Right(())
        }
      } catch {
        case _: FileAlreadyExistsException =>
          Either
            .catchNonFatal {
              val input = fileSystem.open(file)
              try new String(input.readAllBytes(), StandardCharsets.UTF_8)
              finally input.close()
            }
            .leftMap(_ => invalidCheckpoint)
            .flatMap(value => Either.cond(value == normalized, (), invalidCheckpoint))
        case NonFatal(_) => Left(invalidCheckpoint)
      }
    }
  }

  private def persistLineage(identity: StreamingActivationIdentity): F[Unit] = execution.either {
    val fileSystem = new Path(SparkPhysicalLocation.resolve(settings.checkpointLocation))
      .getFileSystem(spark.sparkContext.hadoopConfiguration)
    val expected = identity.canonical
    val directory = new Path(new Path(SparkPhysicalLocation.resolve(lakehouseRoot)), "control/streaming_lineage")
    val file = lineagePath(directory, identity)
    try {
      if (!fileSystem.mkdirs(directory) && !fileSystem.exists(directory)) Left(invalidCheckpoint)
      else if (fileSystem.exists(file)) {
        val input = fileSystem.open(file)
        val registered = try new String(input.readAllBytes(), StandardCharsets.UTF_8)
        finally input.close()
        Either.cond(registered == expected, (), invalidCheckpoint)
      } else {
        val output = fileSystem.create(file, false)
        try {
          output.write(expected.getBytes(StandardCharsets.UTF_8))
          output.hflush()
          output.hsync()
        } finally output.close()
        Right(())
      }
    } catch {
      case _: FileAlreadyExistsException =>
        Either
          .catchNonFatal {
            val input = fileSystem.open(file)
            try new String(input.readAllBytes(), StandardCharsets.UTF_8)
            finally input.close()
          }
          .leftMap(_ => invalidCheckpoint)
          .flatMap(value => Either.cond(value == expected, (), invalidCheckpoint))
      case NonFatal(_) => Left(invalidCheckpoint)
    }
  }

  private def runCallback(
      dispatcher: Dispatcher[F],
      frame: DataFrame,
      batchNumber: Long,
      lineage: com.example.hiring.analytics.domain.StreamingLineage,
      activationIdentity: StreamingActivationIdentity,
      queryIdentityReady: Deferred[F, Either[Throwable, Unit]]
  ): Unit = {
    val authorize = activationGate.requireAuthorized(activationIdentity, settings.activationGrantId).void
    val effectForBatch = StreamingBatchId
      .from(batchNumber)
      .leftMap(problem => AnalyticsError.InvalidInput.one(problem))
      .liftTo[F]
      .flatMap(batchId =>
        checkpointBatches.flatMap { batches =>
          batches.find(_.batchId == batchId).toRight(invalidCheckpoint).liftTo[F].flatMap { checkpointBatch =>
            lakehouseLock
              .resource(lakehouseRoot)
              .use(_ =>
                queryIdentityReady.get.flatMap(_.liftTo[F]) *> authorize *> processBatch(
                  frame,
                  batchId,
                  lineage,
                  checkpointBatch.endOffsets,
                  authorize
                ) *> registerCheckpointLineage(activationIdentity)
              )
          }
        }
      )
    val (completed, cancel) = dispatcher.unsafeToFutureCancelable(effectForBatch)
    // Spark's foreachBatch contract is synchronous: returning before this future completes would acknowledge offsets.
    val cancelImmediately = callbackLifecycle.synchronized {
      if (stopping) true
      else {
        activeCallbackCancellation.set(Some(cancel))
        false
      }
    }
    if (cancelImmediately) Await.result(cancel(), Duration.Inf)
    try Await.result(completed, Duration.Inf)
    finally callbackLifecycle.synchronized { activeCallbackCancellation.set(None) }
  }

  private def stop(query: StreamingQuery): F[Unit] =
    effect
      .delay {
        callbackLifecycle.synchronized {
          stopping = true
          activeCallbackCancellation.getAndSet(None)
        }
      }
      .flatMap(_.fold(effect.unit)(cancel => effect.fromFuture(effect.delay(cancel())).void))
      .handleErrorWith(_ => effect.unit) *> effect
      .blocking(query.stop())
      .void

  /** Spark may finish registering a query after its blocking start call is cancelled but before it returns a handle. */
  private def stopQueryByName: F[Unit] =
    for {
      callbackCancellation <- effect.delay {
        callbackLifecycle.synchronized {
          stopping = true
          activeCallbackCancellation.getAndSet(None)
        }
      }
      callbackResult <- callbackCancellation.traverse_(cancel => effect.fromFuture(effect.delay(cancel())).void).attempt
      queryResult <- driverExecution
        .blocking(spark.streams.active.filter(_.name.contains(settings.streamId)).foreach(_.stop()))
        .translating(AnalyticsError.LakehouseFailure(_))
        .attempt
      _ <- (callbackResult, queryResult) match {
        case (Right(_), Right(_))              => effect.unit
        case (Left(callbackError), Right(_))   => effect.raiseError[Unit](callbackError)
        case (callbackError, Left(queryError)) =>
          callbackError.left.foreach(queryError.addSuppressed)
          effect.raiseError[Unit](queryError)
      }
    } yield ()

  private def startingOffsets: String = {
    val partitions = settings.initialOffsets
      .groupBy(_ => AnalyticsTopic.unwrap(topic))
      .toVector
      .sortBy(_._1)
      .map { case (name, offsets) =>
        name -> Json.fromFields(
          offsets
            .sortBy(offset => AnalyticsPartition.unwrap(offset.partition))
            .map(offset =>
              AnalyticsPartition.unwrap(offset.partition).toString -> Json
                .fromLong(AnalyticsOffset.unwrap(offset.offset))
            )
        )
      }
    Json.fromFields(partitions).noSpaces
  }

  private val invalidCheckpoint = AnalyticsError.InvalidConfiguration(
    "analytics streaming checkpoint is missing, malformed, or bound to another stream identity"
  )

  private def lineagePath(directory: Path, identity: StreamingActivationIdentity): Path =
    new Path(directory, s"${AnalyticsDigest.sha256Hex(identity.streamId)}.identity")

  private def establishedPath(directory: Path, identity: StreamingActivationIdentity): Path =
    new Path(directory, s"${AnalyticsDigest.sha256Hex(identity.streamId)}.established")

  private def queryIdentityPath(directory: Path, identity: StreamingActivationIdentity): Path =
    new Path(directory, s"${AnalyticsDigest.sha256Hex(identity.streamId)}.query-id")

  private def failAtGrantExpiry(expiresAt: Instant): F[Unit] =
    effect.realTimeInstant.flatMap { now =>
      val remainingNanos =
        if (!expiresAt.isAfter(now)) 0L
        else scala.util.Try(java.time.Duration.between(now, expiresAt).toNanos).getOrElse(Long.MaxValue)
      effect.sleep(FiniteDuration(remainingNanos, NANOSECONDS)) *>
        effect.raiseError[Unit](AnalyticsError.InvalidConfiguration("analytics streaming activation grant expired"))
    }
}
