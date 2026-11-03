package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.example.graphQL.cats.config.{KafkaConfig, KafkaConsumerConfig, KafkaPublisherConfig}
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField, WorkerAlive}
import com.example.graphQL.cats.service.port.{ConsumerReceiptRepository, EventQuarantineRepository, RepositoryF}
import munit.CatsEffectSuite
import scala.concurrent.duration.*

/** The consumer refuses to start unless its receipts outlive every message the topic can still deliver. */
final class OperationalEventConsumerReceiptWindowSpec extends CatsEffectSuite {
  override def munitIOTimeout: FiniteDuration = 30.seconds

  private def config(receiptTtlDays: Int) = KafkaConfig(
    enabled = true,
    bootstrapServers = "127.0.0.1:9092",
    topic = "hiring.operational-events",
    consumerGroup = "test-consumer",
    publisher = KafkaPublisherConfig("test", 10, 30, 1, 3, 100),
    consumer = KafkaConsumerConfig(enabled = true, receiptTtlDays = receiptTtlDays, quarantineTtlDays = 7)
  )

  private val receipts = new ConsumerReceiptRepository[IO] {
    def record(
        group: String,
        event: com.example.graphQL.cats.service.events.OperationalEventEnvelope,
        now: java.time.Instant,
        expiresAt: java.time.Instant
    ): RepositoryF[IO, Boolean] =
      RepositoryF.pure[IO, Boolean](true)
  }
  private val quarantine = new EventQuarantineRepository[IO] {
    def save(record: com.example.graphQL.cats.service.port.EventQuarantineRecord): RepositoryF[IO, Unit] =
      RepositoryF.pure[IO, Unit](())
  }

  private final class Harness(
      val events: Ref[IO, Vector[(LogEvent, Map[LogField, String])]],
      val beats: Ref[IO, Int],
      val reads: Ref[IO, Int]
  )

  private val harness: IO[Harness] =
    (
      Ref.of[IO, Vector[(LogEvent, Map[LogField, String])]](Vector.empty),
      Ref.of[IO, Int](0),
      Ref.of[IO, Int](0)
    ).mapN(new Harness(_, _, _))

  private def diagnostics(h: Harness): Diagnostics[IO] = new Diagnostics[IO] {
    override def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]): IO[Unit] =
      h.events.update(_ :+ (event -> fields))
  }

  /** Runs the consumer while `until` is false (bounded), with the given sequence of topic answers per read. */
  private def run(
      receiptTtlDays: Int,
      answers: Int => IO[TopicRetention],
      recheck: FiniteDuration = OperationalEventKafkaRuntime.ReceiptWindowRecheck,
      whileRunning: Harness => IO[Unit] = _ => IO.unit
  )(
      until: Harness => IO[Boolean]
  ): IO[Harness] =
    harness.flatMap { h =>
      val alive = WorkerAlive(h.beats.update(_ + 1), 50.millis)
      val gate: KafkaConfig => IO[TopicRetention] =
        _ => h.reads.getAndUpdate(_ + 1).flatMap(answers)
      OperationalEventKafkaRuntime
        .consumerResource(
          config(receiptTtlDays),
          receipts,
          quarantine,
          diagnostics(h),
          alive = alive,
          topicRetention = gate,
          receiptWindowRecheck = recheck
        )
        .use(_ =>
          until(h)
            .flatMap(done => IO.sleep(100.millis).whenA(!done).as(done))
            .iterateUntil(identity)
            .timeout(15.seconds) *> whileRunning(h)
        )
        .as(h)
    }

  private val refusedOnce: Harness => IO[Boolean] =
    _.events.get.map(_.exists(_._1 == LogEvent.ConsumerReceiptWindowRefused))

  private val sevenAndOne = TopicRetention(7.days.toMillis, 1.day.toMillis)

  test("DDC-02.1 retention 7 days, segment 1 day, receipt TTL 9 days: the consumer starts and beats") {
    run(9, _ => IO.pure(sevenAndOne))(_.beats.get.map(_ > 0)).flatMap(h => h.events.get.map(e => assert(e.isEmpty)))
  }

  test("DDC-02.2 receipt TTL 8 days with 7 + 1: no start, no beat, one diagnostics event with the three durations") {
    run(8, _ => IO.pure(sevenAndOne))(refusedOnce).flatMap { h =>
      for {
        beats <- h.beats.get
        events <- h.events.get
      } yield {
        assertEquals(beats, 0)
        val refused = events.collect { case (LogEvent.ConsumerReceiptWindowRefused, fields) => fields }
        assert(refused.nonEmpty)
        assertEquals(
          refused.head.map { case (k, v) => k.key -> v },
          Map(
            "retentionMs" -> 7.days.toMillis.toString,
            "segmentMs" -> 1.day.toMillis.toString,
            "receiptTtlMs" -> 8.days.toMillis.toString,
            "windowRefusal" -> "WindowExceedsReceiptTtl"
          )
        )
      }
    }
  }

  test("DDC-02.3 retention -1 (unbounded): the consumer does not start") {
    run(9, _ => IO.pure(TopicRetention(-1L, 1.day.toMillis)))(refusedOnce).flatMap { h =>
      h.beats.get.map(assertEquals(_, 0)) *> h.events.get.map(e =>
        assert(e.exists(_._1 == LogEvent.ConsumerReceiptWindowRefused))
      )
    }
  }

  test("DDC-02.4 an unreadable topic configuration does not start the consumer and is read again on restart") {
    run(9, _ => IO.raiseError(TopicConfigUnreadable("hiring.operational-events")))(h =>
      (h.reads.get, h.events.get).mapN((reads, events) => reads >= 2 && events.exists(_._1 == LogEvent.RuntimeFailed))
    ).flatMap { h =>
      for {
        beats <- h.beats.get
        events <- h.events.get
      } yield {
        assertEquals(beats, 0)
        val failed = events.collect { case (LogEvent.RuntimeFailed, fields) => fields }
        assert(failed.nonEmpty)
        assertEquals(
          failed.head.get(LogField.ErrorType),
          Some(classOf[TopicConfigUnreadable].getName)
        )
      }
    }
  }

  test("DDC-02.7 the margin decides: segment 23h30m with receipt TTL 8 days refuses, 23h starts") {
    val refused = run(8, _ => IO.pure(TopicRetention(7.days.toMillis, (23.hours + 30.minutes).toMillis)))(
      refusedOnce
    ).flatMap(_.beats.get.map(assertEquals(_, 0)))
    val started = run(8, _ => IO.pure(TopicRetention(7.days.toMillis, 23.hours.toMillis)))(_.beats.get.map(_ > 0))
      .flatMap(_.events.get.map(e => assert(e.isEmpty)))
    refused *> started
  }

  test("DDC-02.8 a refusal followed by a corrected topic starts the consumer on a later restart") {
    val answers: Int => IO[TopicRetention] = {
      case 0 => IO.pure(TopicRetention(7.days.toMillis, 7.days.toMillis))
      case _ => IO.pure(sevenAndOne)
    }
    run(9, answers)(_.beats.get.map(_ > 0)).flatMap { h =>
      for {
        reads <- h.reads.get
        events <- h.events.get
      } yield {
        assert(reads >= 2)
        assertEquals(events.count(_._1 == LogEvent.ConsumerReceiptWindowRefused), 1)
      }
    }
  }

  test("DDC-02.10 a retention raise on a running consumer stops it at the next recheck and records no further beat") {
    val answers: Int => IO[TopicRetention] = {
      case 0 => IO.pure(sevenAndOne)
      case _ => IO.pure(TopicRetention(30.days.toMillis, 1.day.toMillis))
    }
    for {
      observed <- Ref.of[IO, Option[(Int, Int, Int)]](None)
      // Read while the consumer resource is still alive: past the 1 s restart delay, so at least one start gate ran.
      sample = (h: Harness) =>
        IO.sleep(300.millis) *> h.beats.get.flatMap(settled =>
          IO.sleep(2500.millis) *> (h.beats.get, h.reads.get)
            .mapN((later, reads) => Some((settled, later, reads)))
            .flatMap(observed.set)
        )
      h <- run(9, answers, recheck = 300.millis, whileRunning = sample)(refusedOnce)
      sampled <- observed.get
      events <- h.events.get
    } yield {
      val (settled, later, reads) = sampled.getOrElse(fail("the consumer was not observed while running"))
      // The first read (the start gate) passed, so the refusal came from a recheck of the running consumer.
      assert(settled > 0, "the consumer beat before the raise")
      assertEquals(later, settled, "no beat after the recheck refused, across the restart delay")
      assert(reads >= 3, s"start gate, recheck and a restart gate were read, saw $reads")
      assert(events.exists(_._1 == LogEvent.ConsumerReceiptWindowRefused))
    }
  }

  test(
    "DDC-02.11 a recheck that cannot read the topic configuration stops the consumer; a corrected answer starts it"
  ) {
    val answers: Int => IO[TopicRetention] = {
      case 0 => IO.pure(sevenAndOne)
      case 1 => IO.raiseError(TopicConfigUnreadable("hiring.operational-events"))
      case _ => IO.pure(sevenAndOne)
    }
    for {
      atFailure <- Ref.of[IO, Option[Int]](None)
      h <- run(9, answers, recheck = 300.millis) { h =>
        (h.events.get, h.beats.get).flatMapN { (events, beats) =>
          if (events.exists(_._1 == LogEvent.RuntimeFailed))
            atFailure.modify(seen => (seen.orElse(Some(beats)), seen.exists(beats > _)))
          else IO.pure(false)
        }
      }
      failed <- h.events.get.map(_.collect { case (LogEvent.RuntimeFailed, fields) => fields })
      snapshot <- atFailure.get
      beats <- h.beats.get
    } yield {
      assertEquals(failed.head.get(LogField.ErrorType), Some(classOf[TopicConfigUnreadable].getName))
      assert(snapshot.exists(beats > _), "the corrected answer restarted the consumer, which beat again")
    }
  }
}
