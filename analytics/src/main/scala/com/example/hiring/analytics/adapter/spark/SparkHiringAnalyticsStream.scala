package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.adapter.kafka.KafkaClientProperties
import com.example.hiring.analytics.config.{AnalyticsStreamingSettings, KafkaConnection}
import com.example.hiring.analytics.domain.{
  AnalyticsOffset,
  AnalyticsPartition,
  AnalyticsTopic,
  StreamingActivationIdentity,
  StreamingBatchId
}
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.{AnalyticsLakehouseLock, AnalyticsStreamingRegistry}
import com.example.hiring.analytics.service.streaming.StreamingActivationGate

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
    processBatch: (DataFrame, StreamingBatchId) => F[Unit]
) {
  private val effect = Async[F]
  private val activeCallbackCancellation = new AtomicReference[Option[() => Future[Unit]]](None)
  private val callbackLifecycle = new Object
  private var stopping = false

  def resource: Resource[F, Unit] =
    Resource
      .eval(
        effect.fromEither(
          settings.activationIdentity(connection.bootstrapServers, AnalyticsTopic.unwrap(topic), lakehouseRoot)
        )
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
      _ <- validateCheckpointIdentity(activationIdentity)
      properties <- effect.fromEither(KafkaClientProperties.sparkOptions(connection))
      query <- execution {
        spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", connection.bootstrapServers)
          .options(properties)
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
          .foreachBatch((frame: DataFrame, batchNumber: Long) => runCallback(dispatcher, frame, batchNumber))
          .start()
      }
        .adaptError {
          case error: AnalyticsError => error
          case NonFatal(cause)       => AnalyticsError.SourceReadFailure(cause)
        }
    } yield query

  private def validateCheckpointIdentity(activationIdentity: StreamingActivationIdentity): F[Unit] =
    execution.either {
      val checkpoint = new Path(settings.checkpointLocation)
      val fileSystem = checkpoint.getFileSystem(spark.sparkContext.hadoopConfiguration)
      val identityFile = new Path(checkpoint, "_hiring_stream_identity")
      val expected = activationIdentity.canonical

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

      if (!fileSystem.exists(checkpoint)) {
        if (!fileSystem.mkdirs(checkpoint)) Left(invalidCheckpoint)
        else writeNewIdentity()
      } else if (fileSystem.exists(identityFile))
        readExisting.flatMap { _ =>
          val sparkArtifacts = fileSystem.listStatus(checkpoint).filterNot(_.getPath == identityFile)
          if (sparkArtifacts.isEmpty) Right(())
          else validateEstablishedCheckpoint()
        }
      else if (fileSystem.listStatus(checkpoint).nonEmpty) Left(invalidCheckpoint)
      else writeNewIdentity()
    }

  private def runCallback(dispatcher: Dispatcher[F], frame: DataFrame, batchNumber: Long): Unit = {
    val effectForBatch = StreamingBatchId
      .from(batchNumber)
      .leftMap(problem => AnalyticsError.InvalidInput(cats.data.NonEmptyChain.one(problem)))
      .liftTo[F]
      .flatMap(batchId => lakehouseLock.resource(lakehouseRoot).use(_ => processBatch(frame, batchId)))
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
