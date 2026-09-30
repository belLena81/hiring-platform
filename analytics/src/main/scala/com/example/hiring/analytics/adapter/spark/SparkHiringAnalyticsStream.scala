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
import com.example.hiring.analytics.service.batch.{AnalyticsLakehouseLock, AnalyticsStreamingRegistry}
import com.example.hiring.analytics.service.streaming.StreamingActivationGate
import com.example.hiring.analytics.service.streaming.StreamingCheckpointAcknowledgement

import cats.effect.{Async, Resource}
import cats.effect.std.Dispatcher
import cats.syntax.all.*
import io.circe.Json
import org.apache.hadoop.fs.FileAlreadyExistsException
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

import java.util.concurrent.atomic.AtomicReference
import java.nio.charset.StandardCharsets
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters.*
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
    lakehouseLock: AnalyticsLakehouseLock[F],
    lakehouseRoot: String,
    checkpointAcknowledgement: StreamingCheckpointAcknowledgement[F],
    processBatch: (DataFrame, StreamingBatchId, com.example.hiring.analytics.domain.StreamingLineage) => F[Unit],
    resolveSourceIdentity: () => F[(String, String)]
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
            Resource.eval(activationGate.requireAuthorized(identity)) *>
              streamOwnerResource *>
              Dispatcher.parallel[F].flatMap { dispatcher =>
                Resource.make(start(dispatcher, identity))(stop).flatMap { query =>
                  Resource.eval(effect.interruptibleMany(query.awaitTermination()).void)
                }
              }
          }
      }

  private def streamOwnerResource: Resource[F, Unit] =
    Resource
      .make(
        effect
          .fromEither(AnalyticsStreamingRegistry.ownerLockRoot(lakehouseRoot))
          .flatMap(ownerRoot =>
            lakehouseLock.resource(lakehouseRoot).use(_ => lakehouseLock.resource(ownerRoot).allocated)
          )
      )(_._2)
      .void

  private def start(dispatcher: Dispatcher[F], activationIdentity: StreamingActivationIdentity): F[StreamingQuery] =
    for {
      partitions <- KafkaOffsetRangeSource.availablePartitions(connection, topic, driverExecution)
      _ <- effect.fromEither(AnalyticsStreamingSettings.validatePartitionCoverage(settings, partitions))
      checkpointEstablished <- validateCheckpointIdentity(activationIdentity)
      committedBatchIds <- checkpointCommittedBatchIds
      lineage <- lineageFor(activationIdentity)
      _ <- checkpointAcknowledgement.reconcile(lineage, committedBatchIds, checkpointEstablished)
      properties <- effect.fromEither(KafkaClientProperties.sparkOptions(connection))
      query <- execution {
        spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", connection.bootstrapServers)
          .options(properties)
          .option("kafka.group.id", AnalyticsStreamingSettings.ConsumerGroupId)
          .option("subscribe", AnalyticsTopic.unwrap(topic))
          .option("startingOffsets", startingOffsets)
          .option("maxOffsetsPerTrigger", settings.maxOffsetsPerTrigger.asInstanceOf[Int])
          .option("failOnDataLoss", "true")
          .option("kafka.isolation.level", "read_committed")
          .load()
          .writeStream
          .queryName(settings.streamId)
          .option("checkpointLocation", settings.checkpointLocation)
          .trigger(Trigger.ProcessingTime(settings.triggerInterval))
          .foreachBatch((frame: DataFrame, batchNumber: Long) =>
            runCallback(dispatcher, frame, batchNumber, lineage, activationIdentity)
          )
          .start()
      }
        .adaptError {
          case error: AnalyticsError => error
          case NonFatal(cause)       => AnalyticsError.SourceReadFailure(cause)
        }
    } yield query

  private def lineageFor(
      activationIdentity: StreamingActivationIdentity
  ): F[com.example.hiring.analytics.domain.StreamingLineage] =
    effect.fromEither(
      com.example.hiring.analytics.domain.StreamingLineage
        .from(AnalyticsDigest.sha256Hex(activationIdentity.canonical.getBytes(StandardCharsets.UTF_8)))
        .leftMap(_ => invalidCheckpoint)
    )

  private def validateCheckpointIdentity(activationIdentity: StreamingActivationIdentity): F[Boolean] =
    execution.either {
      val checkpoint = new Path(settings.checkpointLocation)
      val fileSystem = checkpoint.getFileSystem(spark.sparkContext.hadoopConfiguration)
      val identityFile = new Path(checkpoint, "_hiring_stream_identity")
      val expected = activationIdentity.canonical
      val lineageDirectory = new Path(new Path(lakehouseRoot), "control/streaming_lineage")
      val lineageFile = new Path(
        lineageDirectory,
        s"${AnalyticsDigest.sha256Hex(expected.getBytes(StandardCharsets.UTF_8))}.identity"
      )

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

      def registerLineage(): Either[AnalyticsError, Unit] =
        try {
          if (!fileSystem.mkdirs(lineageDirectory) && !fileSystem.exists(lineageDirectory)) Left(invalidCheckpoint)
          else if (fileSystem.exists(lineageFile)) {
            val input = fileSystem.open(lineageFile)
            val registered = try new String(input.readAllBytes(), StandardCharsets.UTF_8)
            finally input.close()
            Either.cond(registered == expected, (), invalidCheckpoint)
          } else {
            val output = fileSystem.create(lineageFile, false)
            try {
              output.write(expected.getBytes(StandardCharsets.UTF_8))
              output.hflush()
              output.hsync()
            } finally output.close()
            Right(())
          }
        } catch {
          case _: FileAlreadyExistsException => registerLineage()
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

      val previouslyRegistered = fileSystem.exists(lineageFile)
      if (!fileSystem.exists(checkpoint)) {
        if (previouslyRegistered) Left(invalidCheckpoint)
        else if (!fileSystem.mkdirs(checkpoint)) Left(invalidCheckpoint)
        else writeNewIdentity().map(_ => false)
      } else if (previouslyRegistered && !fileSystem.exists(identityFile)) Left(invalidCheckpoint)
      else if (!fileSystem.exists(identityFile)) {
        if (fileSystem.listStatus(checkpoint).nonEmpty) Left(invalidCheckpoint)
        else writeNewIdentity().map(_ => false)
      } else
        readExisting.flatMap { _ =>
          val sparkArtifacts = fileSystem.listStatus(checkpoint).filterNot(_.getPath == identityFile)
          if (sparkArtifacts.isEmpty && previouslyRegistered) Left(invalidCheckpoint)
          else {
            val established = sparkArtifacts.nonEmpty
            val checkpointState = if (!established) Right(()) else validateEstablishedCheckpoint()
            checkpointState.flatMap(_ => registerLineage()).map(_ => established)
          }
        }
    }

  private def checkpointCommittedBatchIds: F[Set[StreamingBatchId]] = execution.either {
    val checkpoint = new Path(settings.checkpointLocation)
    val fileSystem = checkpoint.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val commits = new Path(checkpoint, "commits")
    if (!fileSystem.exists(commits)) Right(Set.empty)
    else {
      val parsed = fileSystem
        .listStatus(commits)
        .toVector
        .filter(_.isFile)
        .map(_.getPath.getName)
        .traverse(name => scala.util.Try(name.toLong).toEither.leftMap(_ => invalidCheckpoint))
      parsed.flatMap(_.traverse(id => StreamingBatchId.from(id).leftMap(_ => invalidCheckpoint))).map(_.toSet)
    }
  }

  private def registerCheckpointLineage(identity: StreamingActivationIdentity): F[Unit] = execution.either {
    val checkpoint = new Path(settings.checkpointLocation)
    val fileSystem = checkpoint.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val expected = identity.canonical
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
    established.flatMap { _ =>
      val directory = new Path(new Path(lakehouseRoot), "control/streaming_lineage")
      val file =
        new Path(directory, s"${AnalyticsDigest.sha256Hex(expected.getBytes(StandardCharsets.UTF_8))}.identity")
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
  }

  private def runCallback(
      dispatcher: Dispatcher[F],
      frame: DataFrame,
      batchNumber: Long,
      lineage: com.example.hiring.analytics.domain.StreamingLineage,
      activationIdentity: StreamingActivationIdentity
  ): Unit = {
    val effectForBatch = StreamingBatchId
      .from(batchNumber)
      .leftMap(problem => AnalyticsError.InvalidInput(cats.data.NonEmptyChain.one(problem)))
      .liftTo[F]
      .flatMap(batchId =>
        registerCheckpointLineage(activationIdentity) *>
          lakehouseLock.resource(lakehouseRoot).use(_ => processBatch(frame, batchId, lineage))
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
      .handleErrorWith {
        case error: AnalyticsError => effect.raiseError(error)
        case NonFatal(cause)       => effect.raiseError(AnalyticsError.LakehouseFailure(cause))
      }

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
}
