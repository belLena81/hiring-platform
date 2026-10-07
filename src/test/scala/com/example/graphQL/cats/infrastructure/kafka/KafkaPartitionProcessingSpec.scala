package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.{IO, Ref, Deferred}
import cats.syntax.all.*
import fs2.Stream
import munit.CatsEffectSuite
import scala.concurrent.duration.*

final class KafkaPartitionProcessingSpec extends CatsEffectSuite {
  test("more assigned partitions than work permits progress while per-partition order remains sequential") {
    for {
      completed <- Deferred[IO, Unit]
      seen <- Ref.of[IO, Map[Int, Vector[Int]]](Map.empty)
      active <- Ref.of[IO, Int](0)
      peak <- Ref.of[IO, Int](0)
      records = Stream.emits(
        (1 to 8).map(partition => Stream.emits(List(partition -> 1, partition -> 2)).covary[IO] ++ Stream.never[IO])
      )
      fiber <- KafkaPartitionProcessing(records.covary[IO], 4) { case (partition, offset) =>
        active.updateAndGet(_ + 1).flatMap(count => peak.update(_.max(count))) *>
          seen
            .updateAndGet(values => values.updated(partition, values.getOrElse(partition, Vector.empty) :+ offset))
            .flatMap(values => if (values.values.map(_.size).sum == 16) completed.complete(()).void else IO.unit)
            .guarantee(active.update(_ - 1))
      }.compile.drain.start
      _ <- completed.get.timeout(5.seconds).guarantee(fiber.cancel)
      result <- seen.get
      maximum <- peak.get
    } yield {
      assertEquals(result.keySet, (1 to 8).toSet)
      assert(result.values.forall(_ == Vector(1, 2)))
      assert(maximum <= 4)
    }
  }
}
