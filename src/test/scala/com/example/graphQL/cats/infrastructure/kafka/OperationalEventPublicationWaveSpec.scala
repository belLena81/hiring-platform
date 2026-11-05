package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.{IO, Ref}
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.example.graphQL.cats.config.{KafkaConfig, KafkaConsumerConfig, KafkaPublisherConfig}
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.model.JobStatus
import com.example.graphQL.cats.domain.policy.CircuitBreakerPolicy
import com.example.graphQL.cats.infrastructure.events.{OperationalEventHeaders, OperationalEventWireCodec}
import com.example.graphQL.cats.service.{Diagnostics, RepositoryError}
import com.example.graphQL.cats.service.events.*
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.resilience.CircuitBreaker
import fs2.kafka.ProducerRecords
import munit.CatsEffectSuite
import org.apache.kafka.common.errors.{ProducerFencedException, RecordTooLargeException, TimeoutException}

import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*

/** KFH-01: one Kafka transaction per publication wave, and what each failure does to the claims of that wave. */
final class OperationalEventPublicationWaveSpec extends CatsEffectSuite {
  override def munitIOTimeout: FiniteDuration = 30.seconds

  private val now = Instant.parse("2026-11-04T00:00:00Z")
  private val actor = UserId(UUID.fromString("00000000-0000-0000-0000-000000000201"))
  private val config = KafkaConfig(
    enabled = true,
    bootstrapServers = "127.0.0.1:9092",
    topic = OperationalEventEnvelope.Topic,
    consumerGroup = "test-consumer",
    publisher = KafkaPublisherConfig(ProducerWorkerId.parse("test").toOption.get, 10, 30, 1, 3, 50),
    consumer = KafkaConsumerConfig(enabled = false, receiptTtlDays = 9, quarantineTtlDays = 7)
  )

  private def claimOf(key: String, attempts: Int = 1): ClaimedOperationalEvent = {
    val id = UUID.randomUUID()
    val aggregate = UUID.nameUUIDFromBytes(key.getBytes(StandardCharsets.UTF_8))
    val value = OperationalEventEnvelope(
      id,
      OperationalEventType.JOB_UPDATED,
      now,
      OperationalAggregateType.Job,
      aggregate.toString,
      actor,
      OperationalEventPayload.JobFact(
        OperationalEventPayload.JobKind.Updated,
        OperationalEventPayload.JobSnapshot(aggregate, List("Scala"), JobStatus.Open)
      )
    )
    ClaimedOperationalEvent(value, OperationalEventWireCodec.bytes(value), key, s"lease-$key", attempts)
  }

  private enum Step {
    case Claimed(generation: ProducerGeneration, limit: Int)
    case Renewed(key: String)
    case Published(key: String)
    case Released(key: String, immediately: Boolean)
    case Refunded(key: String)
    case Failed(key: String)
  }

  /** An outbox whose due queue honours the claim limit and takes released claims back at the front. */
  private final class FakeOutbox(
      known: List[ClaimedOperationalEvent],
      due: Ref[IO, List[ClaimedOperationalEvent]],
      steps: Ref[IO, Vector[Step]],
      renewal: ClaimedOperationalEvent => IO[Either[RepositoryError, Unit]] = _ => IO.pure(Right(())),
      published: ClaimedOperationalEvent => IO[Either[RepositoryError, Unit]] = _ => IO.pure(Right(()))
  ) extends OperationalEventOutboxRepository[IO] {
    private def byToken(token: String): IO[ClaimedOperationalEvent] = IO(known.find(_.leaseToken == token).get)

    def claim(
        workerId: String,
        generation: ProducerGeneration,
        at: Instant,
        leaseDuration: FiniteDuration,
        limit: Int
    ): RepositoryF[IO, List[ClaimedOperationalEvent]] =
      RepositoryF.fromFEither(
        steps.update(_ :+ Step.Claimed(generation, limit)) *>
          due.modify(queue => (queue.drop(limit), Right(queue.take(limit))))
      )

    def renewLease(
        id: UUID,
        token: String,
        subjects: List[String],
        leaseDuration: FiniteDuration
    ): RepositoryF[IO, Unit] =
      RepositoryF.fromFEither(
        byToken(token).flatMap(c => steps.update(_ :+ Step.Renewed(c.partitionKey)) *> renewal(c))
      )

    def markPublished(id: UUID, token: String, at: Instant, expires: Instant): RepositoryF[IO, Unit] =
      RepositoryF.fromFEither(byToken(token).flatMap { c =>
        published(c).flatTap(result =>
          if (result.isRight) steps.update(_ :+ Step.Published(c.partitionKey)) else IO.unit
        )
      })

    def releaseForRetry(id: UUID, token: String, at: Instant, available: Instant): RepositoryF[IO, Unit] =
      RepositoryF.fromFEither(byToken(token).flatMap { c =>
        steps.update(_ :+ Step.Released(c.partitionKey, immediately = !available.isAfter(at))) *>
          due.update(c :: _).as(Right(()))
      })

    def releaseUncharged(id: UUID, token: String, at: Instant, available: Instant): RepositoryF[IO, Unit] =
      RepositoryF.fromFEither(byToken(token).flatMap { c =>
        steps.update(_ :+ Step.Refunded(c.partitionKey)) *> due.update(c :: _).as(Right(()))
      })

    def markFailed(id: UUID, token: String, at: Instant, reason: String): RepositoryF[IO, Unit] =
      RepositoryF.fromFEither(
        byToken(token).flatMap(c => steps.update(_ :+ Step.Failed(c.partitionKey))).as(Right(()))
      )
  }

  private def outbox(
      queue: List[ClaimedOperationalEvent],
      renewal: ClaimedOperationalEvent => IO[Either[RepositoryError, Unit]] = _ => IO.pure(Right(())),
      published: ClaimedOperationalEvent => IO[Either[RepositoryError, Unit]] = _ => IO.pure(Right(()))
  ): IO[(FakeOutbox, Ref[IO, Vector[Step]])] =
    for {
      due <- Ref.of[IO, List[ClaimedOperationalEvent]](queue)
      steps <- Ref.of[IO, Vector[Step]](Vector.empty)
    } yield {
      (new FakeOutbox(queue, due, steps, renewal, published), steps)
    }

  private val generation = ProducerGeneration("hiring-publisher-test", ProducerGenerationId(UUID.randomUUID()))

  private def publish(
      fake: FakeOutbox,
      send: ProducerRecords[String, Array[Byte]] => IO[Unit],
      breaker: CircuitBreaker[IO] = CircuitBreaker.disabled[IO],
      retrySingly: Ref[IO, Int]
  ) =
    OperationalEventKafkaRuntime.publishBatch(
      config,
      fake,
      Diagnostics.noop,
      generation,
      send,
      cats.effect.Clock[IO],
      breaker,
      com.example.graphQL.cats.infrastructure.telemetry.KafkaClientMetrics.noop,
      retrySingly
    )

  private def keys(steps: Vector[Step])(select: PartialFunction[Step, String]): Vector[String] = steps.collect(select)

  test("KFH-01.1 four claimed events are sent as one transactional call and all become Published") {
    val claims = List("a", "b", "c", "d").map(claimOf(_))
    for {
      (fake, steps) <- outbox(claims)
      sends <- Ref.of[IO, Vector[List[(String, Map[String, String])]]](Vector.empty)
      singly <- Ref.of[IO, Int](0)
      _ <- publish(
        fake,
        records =>
          sends.update(
            _ :+ records.toList.map(r =>
              r.key -> r.headers.toChain.toList.map(h => h.key -> new String(h.value, StandardCharsets.UTF_8)).toMap
            )
          ),
        retrySingly = singly
      )
      calls <- sends.get
      recorded <- steps.get
    } yield {
      assertEquals(calls.size, 1)
      assertEquals(calls.head.map(_._1).toSet, Set("a", "b", "c", "d"))
      assert(calls.head.forall(_._2.get(OperationalEventHeaders.SchemaVersion).contains("1")), clue(calls))
      assertEquals(keys(recorded) { case Step.Published(key) => key }.toSet, Set("a", "b", "c", "d"))
      assertEquals(recorded.collect { case Step.Claimed(_, limit) => limit }.head, 4)
    }
  }

  test("KFH-01.2 a broker timeout releases all four claims for retry and counts one breaker failure") {
    val claims = List("a", "b", "c", "d").map(claimOf(_))
    val breakerSettings = CircuitBreakerPolicy.Settings(3, 10.seconds, 100.millis, 1.second, 1)
    for {
      breaker <- CircuitBreaker.create[IO](breakerSettings, _ => IO.unit)
      (fake, steps) <- outbox(claims)
      _ <- OperationalEventKafkaRuntime
        .publishWave(config, fake, Diagnostics.noop, claims)(
          OperationalEventKafkaRuntime.guardedSend(breaker, IO.raiseError(new TimeoutException("broker down")))
        )
      state <- breaker.current
      recorded <- steps.get
    } yield {
      state match {
        case CircuitBreakerPolicy.State.Closed(failures) => assertEquals(failures.size, 1)
        case other                                       => fail(s"unexpected breaker state $other")
      }
      assertEquals(keys(recorded) { case Step.Released(key, false) => key }.toSet, Set("a", "b", "c", "d"))
      assertEquals(keys(recorded) { case Step.Failed(key) => key }, Vector.empty[String])
    }
  }

  test("KFH-01.3 a lease lost before the send sends nothing, releases the other claims and ends with an error") {
    val claims = List("a", "b", "c", "d").map(claimOf(_))
    for {
      (fake, steps) <- outbox(
        claims,
        renewal = c => IO.pure(if (c.partitionKey == "b") Left(RepositoryError.Conflict) else Right(()))
      )
      sent <- Ref.of[IO, Boolean](false)
      result <- OperationalEventKafkaRuntime
        .publishWave(config, fake, Diagnostics.noop, claims)(sent.set(true))
        .attempt
      didSend <- sent.get
      recorded <- steps.get
    } yield {
      assert(result.isLeft)
      assert(!didSend)
      assertEquals(keys(recorded) { case Step.Refunded(key) => key }.toSet, Set("a", "c", "d"))
      assertEquals(keys(recorded) { case Step.Published(key) => key }, Vector.empty[String])
    }
  }

  test("KFH-01.4 a lease lost while the send runs cancels it, leaves that claim and releases the others uncharged") {
    val claims = List("a", "b", "c", "d").map(claimOf(_))
    val shortLease = config.copy(publisher = config.publisher.copy(leaseSeconds = 3))
    for {
      renewals <- Ref.of[IO, Map[String, Int]](Map.empty)
      (fake, steps) <- outbox(
        claims,
        renewal = c =>
          renewals
            .updateAndGet(m => m.updated(c.partitionKey, m.getOrElse(c.partitionKey, 0) + 1))
            .map(m => if (c.partitionKey == "c" && m(c.partitionKey) > 1) Left(RepositoryError.Conflict) else Right(()))
      )
      cancelled <- Ref.of[IO, Boolean](false)
      result <- OperationalEventKafkaRuntime
        .publishWave(shortLease, fake, Diagnostics.noop, claims)(IO.never[Unit].onCancel(cancelled.set(true)))
        .attempt
        .timeout(10.seconds)
      wasCancelled <- cancelled.get
      recorded <- steps.get
    } yield {
      assert(result.isLeft)
      assert(wasCancelled)
      assertEquals(
        keys(recorded) { case Step.Refunded(key) => key }.toSet,
        Set("a", "b", "d")
      )
      assertEquals(keys(recorded) { case Step.Published(key) => key }, Vector.empty[String])
      assertEquals(keys(recorded) { case Step.Failed(key) => key }, Vector.empty[String])
    }
  }

  test("KFH-01.5 a fenced producer releases every claim uncharged and the wave ends with a fencing error") {
    val claims = List("a", "b", "c", "d").map(claimOf(_))
    for {
      (fake, steps) <- outbox(claims)
      result <- OperationalEventKafkaRuntime
        .publishWave(config, fake, Diagnostics.noop, claims)(IO.raiseError(new ProducerFencedException("fenced")))
        .attempt
      recorded <- steps.get
    } yield {
      assert(result.left.exists(KafkaRuntimeSupport.isProducerFenced), clue(result))
      assertEquals(
        keys(recorded) { case Step.Refunded(key) => key }.toSet,
        Set("a", "b", "c", "d")
      )
      assertEquals(keys(recorded) { case Step.Failed(key) => key }, Vector.empty[String])
    }
  }

  test("KFH-01.5 the successor generation claims afresh with the same transactional id and a new generation id") {
    val first = claimOf("first")
    val second = claimOf("second")
    for {
      due <- Ref.of[IO, List[ClaimedOperationalEvent]](List(first))
      steps <- Ref.of[IO, Vector[Step]](Vector.empty)
      fake = new FakeOutbox(List(first, second), due, steps)
      opened <- Ref.of[IO, Vector[ProducerGeneration]](Vector.empty)
      sentKeys <- Ref.of[IO, Vector[(Int, String)]](Vector.empty)
      singly <- Ref.of[IO, Int](0)
      open = (g: ProducerGeneration) =>
        cats.effect.Resource.eval(opened.updateAndGet(_ :+ g)).map { all =>
          val index = all.size
          (records: ProducerRecords[String, Array[Byte]]) =>
            sentKeys.update(_ ++ records.toList.map(r => index -> r.key)) *>
              (if (index == 1) IO.raiseError[Unit](new ProducerFencedException("fenced")) else IO.unit)
        }
      generationStream = OperationalEventKafkaRuntime.publisherGeneration(
        config,
        fake,
        Diagnostics.noop,
        cats.effect.Clock[IO],
        UUIDGen[IO],
        CircuitBreaker.disabled[IO],
        IO.unit,
        com.example.graphQL.cats.infrastructure.telemetry.KafkaClientMetrics.noop,
        singly,
        open
      )
      _ <- generationStream.compile.drain
      // The fenced wave released the first claim back to the store; only the successor's own claim call takes it.
      _ <- due.update(_ :+ second)
      _ <- generationStream
        .interruptWhen(
          (IO.sleep(20.millis) *> sentKeys.get).iterateUntil(_.exists(_._1 == 2)).as(Either.right[Throwable, Unit](()))
        )
        .compile
        .drain
      generations <- opened.get
      sends <- sentKeys.get
      recorded <- steps.get
    } yield {
      assertEquals(generations.map(_.transactionalId).distinct, Vector("hiring-publisher-test"))
      assertEquals(generations.map(_.generationId).distinct.size, 2)
      assertEquals(sends.head, 1 -> "first")
      assertEquals(sends.filter(_._1 == 2).map(_._2).toSet, Set("first", "second"))
      assertEquals(recorded.collect { case Step.Claimed(g, _) => g }.distinct.size, 2)
    }
  }

  test("KFH-04.5 a generation whose producer cannot initialize claims and registers nothing") {
    for {
      (fake, steps) <- outbox(List(claimOf("a")))
      ids <- Ref.of[IO, Vector[ProducerGenerationId]](Vector.empty)
      singly <- Ref.of[IO, Int](0)
      open = (g: ProducerGeneration) =>
        cats.effect.Resource.eval(
          ids.update(_ :+ g.generationId) *> IO.raiseError[ProducerRecords[String, Array[Byte]] => IO[Unit]](
            new IllegalStateException("initTransactions failed")
          )
        )
      run = OperationalEventKafkaRuntime
        .publisherGeneration(
          config,
          fake,
          Diagnostics.noop,
          cats.effect.Clock[IO],
          UUIDGen[IO],
          CircuitBreaker.disabled[IO],
          IO.unit,
          com.example.graphQL.cats.infrastructure.telemetry.KafkaClientMetrics.noop,
          singly,
          open
        )
        .compile
        .drain
      _ <- run
      _ <- run
      initialized <- ids.get
      recorded <- steps.get
    } yield {
      assertEquals(initialized.size, 2)
      assertEquals(initialized.distinct.size, 2)
      assertEquals(recorded, Vector.empty[Step])
    }
  }

  test("KFH-01.6 a markPublished failure for one claim does not skip the others and is raised after") {
    val claims = List("a", "b", "c", "d").map(claimOf(_))
    for {
      (fake, steps) <- outbox(
        claims,
        published = c => IO.pure(if (c.partitionKey == "b") Left(RepositoryError.Unavailable) else Right(()))
      )
      result <- OperationalEventKafkaRuntime.publishWave(config, fake, Diagnostics.noop, claims)(IO.unit).attempt
      recorded <- steps.get
    } yield {
      assert(result.isLeft)
      assertEquals(keys(recorded) { case Step.Published(key) => key }.toSet, Set("a", "c", "d"))
    }
  }

  test("KFH-01.7 an oversize record in a four-claim wave releases all uncharged and each then retries alone") {
    val claims = List(claimOf("a"), claimOf("b"), claimOf("c"), claimOf("big", attempts = 3))
    for {
      (fake, steps) <- outbox(claims)
      sends <- Ref.of[IO, Vector[List[String]]](Vector.empty)
      singly <- Ref.of[IO, Int](0)
      _ <- publish(
        fake,
        records => {
          val sent = records.toList.map(_.key)
          sends.update(_ :+ sent) *>
            (if (sent.contains("big")) IO.raiseError(new RecordTooLargeException("too large")) else IO.unit)
        },
        retrySingly = singly
      )
      calls <- sends.get
      recorded <- steps.get
    } yield {
      assertEquals(calls.head.toSet, Set("a", "b", "c", "big"))
      assert(calls.tail.forall(_.size == 1), clue(calls))
      assertEquals(keys(recorded) { case Step.Published(key) => key }.toSet, Set("a", "b", "c"))
      assertEquals(keys(recorded) { case Step.Failed(key) => key }, Vector("big"))
      assertEquals(keys(recorded) { case Step.Refunded(key) => key }.toSet, Set("a", "b", "c", "big"))
      val limits = recorded.collect { case Step.Claimed(_, limit) => limit }
      assertEquals(limits.take(2), Vector(4, 1))
    }
  }

  // Scope: this asserts only that the resend is byte-identical (value, key, headers). Duplicate absorption after the
  // resend is covered by the receipt tests (DDC-03.x in OperationalEventKafkaRuntimeSpec) and the analytics Silver
  // dedup tests.
  test("KFH-01.8 a resend after a crash between commit and markPublished carries identical bytes, key and headers") {
    val claim = claimOf("a")
    val first = OperationalEventKafkaRuntime.producerRecord(config.topic, claim)
    val resent = OperationalEventKafkaRuntime.producerRecord(config.topic, claim)
    assertEquals(first.key, resent.key)
    assertEquals(first.value.toList, resent.value.toList)
    assertEquals(first.value.toList, claim.envelopeBytes.toList)
    assertEquals(
      first.headers.toChain.toList.map(h => h.key -> h.value.toList),
      resent.headers.toChain.toList.map(h => h.key -> h.value.toList)
    )
    assertEquals(
      first.headers.toChain.toList
        .find(_.key == OperationalEventHeaders.EventId)
        .map(h => new String(h.value, StandardCharsets.UTF_8)),
      Some(claim.event.eventId.toString)
    )
  }
}
