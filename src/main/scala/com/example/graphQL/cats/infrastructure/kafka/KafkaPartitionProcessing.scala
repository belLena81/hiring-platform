package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.IO
import cats.effect.std.Semaphore
import fs2.Stream

private[kafka] object KafkaPartitionProcessing {

  /** Open every assigned partition while bounding record effects, so long-lived streams cannot starve later partitions.
    */
  def apply[A](partitions: Stream[IO, Stream[IO, A]], concurrency: Int)(process: A => IO[Unit]): Stream[IO, Unit] =
    Stream.eval(Semaphore[IO](concurrency.toLong)).flatMap { permits =>
      partitions.map(_.evalMap(record => permits.permit.use(_ => process(record)))).parJoinUnbounded
    }
}
