package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.{IO, Ref, Deferred}
import cats.syntax.all.*
import fs2.Stream
import munit.CatsEffectSuite
import org.apache.kafka.clients.consumer.{CommitFailedException, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.RebalanceInProgressException

import scala.concurrent.duration.*

final class KafkaPartitionProcessingSpec extends CatsEffectSuite {
  private val topic = "hiring.operational-events"

  /** Every commit call as the offsets it carried, in order. */
  private final class Commits(calls: Ref[IO, Vector[Map[TopicPartition, OffsetAndMetadata]]]) {
    def commit(offsets: Map[TopicPartition, OffsetAndMetadata]): IO[Unit] = calls.update(_ :+ offsets)
    def all: IO[Vector[Map[TopicPartition, OffsetAndMetadata]]] = calls.get
    def positions: IO[Vector[Long]] = calls.get.map(_.flatMap(_.values.map(_.offset)))
  }

  private def commits: IO[Commits] =
    Ref.of[IO, Vector[Map[TopicPartition, OffsetAndMetadata]]](Vector.empty).map(new Commits(_))

  private def records(partition: Int, count: Int): Stream[IO, (Int, Int)] =
    Stream.range(0, count).map(offset => partition -> offset).covary[IO]

  private def process(commits: Commits)(record: (Int, Int)) =
    IO.pure(KafkaFixtures.offset(topic, record._1, record._2.toLong, "g")(commits.commit))

  private val ignoreDropped: Throwable => IO[Unit] = _ => IO.unit

  test("more assigned partitions than work permits progress while per-partition order remains sequential") {
    for {
      completed <- Deferred[IO, Unit]
      seen <- Ref.of[IO, Map[Int, Vector[Int]]](Map.empty)
      active <- Ref.of[IO, Int](0)
      peak <- Ref.of[IO, Int](0)
      observed <- commits
      partitions = Stream.emits(
        (1 to 8).map(partition => records(partition, 2) ++ Stream.never[IO])
      )
      fiber <- KafkaPartitionProcessing(partitions.covary[IO], 4, CommitBatching(1, 10.millis), ignoreDropped) {
        case (partition, offset) =>
          active.updateAndGet(_ + 1).flatMap(count => peak.update(_.max(count))) *>
            seen
              .updateAndGet(values => values.updated(partition, values.getOrElse(partition, Vector.empty) :+ offset))
              .flatMap(values => if (values.values.map(_.size).sum == 16) completed.complete(()).void else IO.unit)
              .guarantee(active.update(_ - 1)) *> process(observed)((partition, offset))
      }.compile.drain.start
      _ <- completed.get.timeout(5.seconds).guarantee(fiber.cancel)
      result <- seen.get
      maximum <- peak.get
    } yield {
      assertEquals(result.keySet, (1 to 8).toSet)
      assert(result.values.forall(_ == Vector(0, 1)))
      assert(maximum <= 4)
    }
  }

  test("KFH-02.1 ten durable records with batch size five commit exactly twice") {
    for {
      observed <- commits
      _ <- KafkaPartitionProcessing(
        Stream.emit(records(0, 10)).covary[IO],
        4,
        CommitBatching(5, 1.hour),
        ignoreDropped
      )(process(observed)).compile.drain
      calls <- observed.all
    } yield {
      assertEquals(calls.size, 2)
      assertEquals(calls.map(_.values.map(_.offset).toList), Vector(List(5L), List(10L)))
    }
  }

  test("KFH-02.2 a record that is not durable stops its partition and no later offset is committed") {
    final case class RecordNotDurable(offset: Int) extends RuntimeException(s"record $offset is not durable")
    for {
      observed <- commits
      result <- KafkaPartitionProcessing(
        Stream.emit(Stream.range(1, 11).map(offset => 0 -> offset).covary[IO]).covary[IO],
        4,
        CommitBatching(1, 1.hour),
        ignoreDropped
      ) { case record @ (_, offset) =>
        if (offset == 3) IO.raiseError(RecordNotDurable(offset)) else process(observed)(record)
      }.compile.drain.attempt
      positions <- observed.positions
    } yield {
      assertEquals(result, Left(RecordNotDurable(3)))
      // A committed position is the record offset + 1: records 1 and 2 commit, records 3..10 never do.
      assertEquals(positions, Vector(2L, 3L))
    }
  }

  test("KFH-02.3 a partial batch is committed once after the interval") {
    for {
      observed <- commits
      fiber <- KafkaPartitionProcessing(
        Stream.emit(records(0, 2) ++ Stream.never[IO]).covary[IO],
        4,
        CommitBatching(100, 100.millis),
        ignoreDropped
      )(process(observed)).compile.drain.start
      _ <- IO.sleep(600.millis)
      calls <- observed.all
      _ <- fiber.cancel
    } yield {
      assertEquals(calls.size, 1)
      assertEquals(calls.head.values.map(_.offset).toList, List(2L))
    }
  }

  test("KFH-02.4 a commit lost to a rebalance is reported and dropped without ending the stream") {
    List[Throwable](
      new RebalanceInProgressException("rebalance"),
      new CommitFailedException("group rebalanced"),
      new RuntimeException("recovery gave up", new RebalanceInProgressException("rebalance"))
    ).traverse_ { failure =>
      for {
        observed <- commits
        dropped <- Ref.of[IO, Vector[Throwable]](Vector.empty)
        first <- Ref.of[IO, Boolean](true)
        result <- KafkaPartitionProcessing(
          Stream.emit(records(0, 4)).covary[IO],
          4,
          CommitBatching(2, 1.hour),
          error => dropped.update(_ :+ error)
        ) { case (partition, offset) =>
          IO.pure(
            KafkaFixtures.offset(topic, partition, offset.toLong, "g") { offsets =>
              first
                .getAndSet(false)
                .flatMap(isFirst => if (isFirst) IO.raiseError(failure) else observed.commit(offsets))
            }
          )
        }.compile.drain.attempt
        losses <- dropped.get
        positions <- observed.positions
      } yield {
        assertEquals(result, Right(()))
        assertEquals(losses, Vector(failure))
        assertEquals(positions, Vector(4L))
      }
    }
  }

  test("KFH-02.4 any other commit failure still ends the stream") {
    for {
      dropped <- Ref.of[IO, Int](0)
      result <- KafkaPartitionProcessing(
        Stream.emit(records(0, 2)).covary[IO],
        4,
        CommitBatching(2, 1.hour),
        _ => dropped.update(_ + 1)
      ) { case (partition, offset) =>
        IO.pure(
          KafkaFixtures.offset(topic, partition, offset.toLong, "g")(_ =>
            IO.raiseError(new IllegalStateException("broker"))
          )
        )
      }.compile.drain.attempt
      count <- dropped.get
    } yield {
      assert(result.isLeft)
      assertEquals(count, 0)
    }
  }

  test("KFH-02.1 a pending commit never holds a work permit, so other partitions keep processing") {
    for {
      stuck <- Deferred[IO, Unit]
      processedOther <- Deferred[IO, Unit]
      fiber <- KafkaPartitionProcessing(
        Stream
          .emits(
            List(Stream.emit(0 -> 0).covary[IO] ++ Stream.never[IO], Stream.emit(1 -> 0).covary[IO] ++ Stream.never[IO])
          )
          .covary[IO],
        1,
        CommitBatching(1, 10.millis),
        ignoreDropped
      ) { case (partition, offset) =>
        val committable = KafkaFixtures.offset(topic, partition, offset.toLong, "g") { _ =>
          if (partition == 0) stuck.complete(()) *> IO.never[Unit] else IO.unit
        }
        (if (partition == 1) processedOther.complete(()).void else IO.unit).as(committable)
      }.compile.drain.start
      _ <- stuck.get.timeout(5.seconds).guarantee(IO.unit)
      _ <- processedOther.get.timeout(5.seconds).guarantee(fiber.cancel)
    } yield ()
  }
}
