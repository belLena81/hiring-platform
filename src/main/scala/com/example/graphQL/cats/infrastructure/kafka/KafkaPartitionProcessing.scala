package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.IO
import cats.effect.std.Semaphore
import fs2.{Pipe, Stream}
import fs2.kafka.{CommittableOffset, CommittableOffsetBatch}

import scala.concurrent.duration.FiniteDuration

/** How many processed offsets of one partition are committed together, and how long a partial batch may wait. */
final case class CommitBatching(size: Int, interval: FiniteDuration)

private[kafka] object KafkaPartitionProcessing {

  /** Open every assigned partition while bounding record effects, so long-lived streams cannot starve later partitions.
    *
    * `process` makes a record durable and answers its offset; a failure there ends the partition stream before the
    * offset exists, so no later offset of that partition can be committed. The permit covers only `process`: waiting
    * for a commit batch never holds a work permit. Offsets then flow per partition through size-or-time batches, one
    * Kafka commit each. A commit lost to a rebalance is handed to `onRebalancedCommit` and dropped (the records are
    * redelivered and absorbed by the durable receipt); every other commit failure ends the stream.
    */
  def apply[A](
      partitions: Stream[IO, Stream[IO, A]],
      concurrency: Int,
      batching: CommitBatching,
      onRebalancedCommit: Throwable => IO[Unit]
  )(process: A => IO[CommittableOffset[IO]]): Stream[IO, Unit] =
    Stream.eval(Semaphore[IO](concurrency.toLong)).flatMap { permits =>
      partitions
        .map(
          _.evalMap(record => permits.permit.use(_ => process(record)))
            .through(commitBatches(batching, onRebalancedCommit))
        )
        .parJoinUnbounded
    }

  private def commitBatches(
      batching: CommitBatching,
      onRebalancedCommit: Throwable => IO[Unit]
  ): Pipe[IO, CommittableOffset[IO], Unit] =
    _.groupWithin(batching.size, batching.interval).evalMap { offsets =>
      CommittableOffsetBatch.fromFoldable(offsets).commit.handleErrorWith {
        case error if KafkaRuntimeSupport.isRebalanceCommitFailure(error) => onRebalancedCommit(error)
        case error                                                        => IO.raiseError(error)
      }
    }
}
