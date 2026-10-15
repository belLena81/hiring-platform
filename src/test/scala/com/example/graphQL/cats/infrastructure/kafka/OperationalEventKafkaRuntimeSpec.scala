package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.{Deferred, IO, Ref, Resource}
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.example.graphQL.cats.config.{
  KafkaConfig,
  KafkaConsumerConfig,
  KafkaPublisherConfig,
  KafkaSaslSecurityProtocol
}
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.model.JobStatus
import com.example.graphQL.cats.domain.policy.CircuitBreakerPolicy
import com.example.graphQL.cats.service.resilience.CircuitBreaker
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField}
import com.example.graphQL.cats.infrastructure.events.OperationalEventWireCodec
import com.example.graphQL.cats.service.events.*
import fs2.Stream
import munit.CatsEffectSuite
import org.apache.kafka.common.errors.{
  BrokerNotAvailableException,
  DisconnectException,
  InvalidTopicException,
  NetworkException,
  NotLeaderOrFollowerException,
  ProducerFencedException,
  RecordTooLargeException,
  TopicAuthorizationException
}
import org.apache.kafka.common.errors.SerializationException

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*

class OperationalEventKafkaRuntimeSpec extends CatsEffectSuite {
  private val now = Instant.parse("2026-09-20T00:00:00Z")
  private val actor = UserId(UUID.fromString("00000000-0000-0000-0000-000000000201"))
  private val config = KafkaConfig(
    enabled = true,
    bootstrapServers = "127.0.0.1:9092",
    topic = OperationalEventEnvelope.Topic,
    consumerGroup = "test-consumer",
    publisher = KafkaPublisherConfig("test", 10, 30, 1, 3, 100),
    consumer = KafkaConsumerConfig(enabled = true, receiptTtlDays = 8, quarantineTtlDays = 7)
  )

  test("publisher and reader use independent SASL principals") {
    val publisher = KafkaClientSettings.security(Some("publisher"), Some("publish-secret"))
    val reader = KafkaClientSettings.security(Some("analytics_reader"), Some("read-secret"))
    assertEquals(publisher.get("security.protocol"), Some("SASL_SSL"))
    assert(publisher.getOrElse("sasl.jaas.config", "").contains("username=\"publisher\""))
    assert(reader.getOrElse("sasl.jaas.config", "").contains("username=\"analytics_reader\""))
    assert(!publisher.getOrElse("sasl.jaas.config", "").contains("read-secret"))
  }

  test("plaintext SASL transport requires explicit local configuration") {
    val properties = KafkaClientSettings.security(
      Some("publisher"),
      Some("publish-secret"),
      KafkaSaslSecurityProtocol.Plaintext
    )
    assertEquals(properties.get("security.protocol"), Some("SASL_PLAINTEXT"))
  }

  test("Kafka clients without credentials state PLAINTEXT explicitly and carry no SASL properties") {
    assertEquals(KafkaClientSettings.security(None, None), Map("security.protocol" -> "PLAINTEXT"))
  }

  test("outbox failure reasons carry exception class names and never client messages or credentials") {
    val error = new org.apache.kafka.common.KafkaException(
      "Failed to construct kafka producer: sasl.jaas.config username=\"u\" password=\"hunter2\"",
      new IllegalArgumentException("password=\"hunter2\"")
    )
    val reason = OperationalEventKafkaRuntime.sanitized(error)
    assertEquals(reason, "org.apache.kafka.common.KafkaException <- java.lang.IllegalArgumentException")
    assert(!reason.contains("hunter2"))
    assert(OperationalEventKafkaRuntime.sanitized(new RuntimeException("x" * 2000)).length <= 512)
  }

  private def event(
      eventType: OperationalEventType,
      aggregateId: String = "00000000-0000-0000-0000-000000000901",
      occurredAt: Instant = now
  ): OperationalEventEnvelope =
    OperationalEventEnvelope(
      UUID.randomUUID(),
      eventType,
      occurredAt,
      OperationalAggregateType.Job,
      aggregateId,
      actor,
      OperationalEventPayload.JobFact(
        OperationalEventPayload.JobKind.Updated,
        OperationalEventPayload.JobSnapshot(UUID.fromString(aggregateId), List("Scala"), JobStatus.Open)
      )
    )

  private def claim(partitionKey: String, eventId: UUID): ClaimedOperationalEvent = {
    val value = event(OperationalEventType.JOB_UPDATED).copy(eventId = eventId)
    ClaimedOperationalEvent(
      value,
      OperationalEventWireCodec.bytes(value),
      partitionKey,
      s"lease-$partitionKey-$eventId",
      1
    )
  }

  private final case class Fakes(
      receipts: ConsumerReceiptRepository[IO],
      quarantines: EventQuarantineRepository[IO],
      quarantined: Ref[IO, Vector[EventQuarantineRecord]]
  )

  private def fakes: IO[Fakes] =
    for {
      receiptState <- Ref.of[IO, Map[(String, UUID), OperationalEventEnvelope]](Map.empty)
      quarantineState <- Ref.of[IO, Vector[EventQuarantineRecord]](Vector.empty)
    } yield {
      val receipts = new ConsumerReceiptRepository[IO] {
        override def exists(group: String, id: UUID): RepositoryF[IO, Boolean] =
          com.example.graphQL.cats.service.port.RepositoryF
            .lift(IO.raiseError(new AssertionError("receipt deduplication must use the atomic insert")))
        override def record(
            group: String,
            value: OperationalEventEnvelope,
            createdAt: Instant,
            expiresAt: Instant
        ): RepositoryF[IO, Boolean] =
          com.example.graphQL.cats.service.port.RepositoryF.fromFEither(receiptState.modify { values =>
            val key = group -> value.eventId
            if (values.contains(key)) (values, Right(false))
            else (values.updated(key, value), Right(true))
          })
      }
      val quarantines = new EventQuarantineRepository[IO] {
        override def save(record: EventQuarantineRecord): RepositoryF[IO, Unit] =
          com.example.graphQL.cats.service.port.RepositoryF
            .fromFEither(quarantineState.update(_ :+ record).as(Right(())))
      }
      Fakes(receipts, quarantines, quarantineState)
    }

  private def durable(
      config: KafkaConfig,
      receipts: ConsumerReceiptRepository[IO],
      quarantine: EventQuarantineRepository[IO],
      topic: String,
      partition: Int,
      offset: Long,
      bytes: Option[Array[Byte]]
  ): IO[Boolean] =
    OperationalEventKafkaRuntime
      .recordDurably(config, receipts, quarantine, topic, partition, offset, bytes)
      .value
      .map(_.isRight)

  test("tombstones quarantine safely before committing and failures retain the offset") {
    fakes.flatMap { values =>
      val failing = new EventQuarantineRepository[IO] {
        def save(record: EventQuarantineRecord): RepositoryF[IO, Unit] =
          RepositoryF.at[IO].raise(RepositoryError.Unavailable)
      }
      for {
        success <- durable(
          config,
          values.receipts,
          values.quarantines,
          config.topic,
          0,
          1L,
          None
        )
        failure <- durable(
          config,
          values.receipts,
          failing,
          config.topic,
          0,
          2L,
          None
        )
        records <- values.quarantined.get
      } yield {
        assert(success)
        assert(!failure)
        assertEquals(records.size, 1)
        assertEquals(records.head.rawBytes.toList, List.empty[Byte])
        assertEquals(records.head.reason, "null event envelope")
      }
    }
  }

  test("malformed records commit after durable quarantine") {
    fakes.flatMap { values =>
      for {
        malformedCommit <- durable(
          config,
          values.receipts,
          values.quarantines,
          config.topic,
          0,
          1L,
          Some("not-json".getBytes)
        )
        records <- values.quarantined.get
      } yield {
        assert(malformedCommit)
        assertEquals(records.map(_.category), Vector(OperationalEventFailureCategory.MalformedEnvelope))
      }
    }
  }

  test("empty non-null records retain a distinct malformed quarantine reason") {
    fakes.flatMap { values =>
      for {
        accepted <- durable(
          config,
          values.receipts,
          values.quarantines,
          config.topic,
          0,
          3L,
          Some(Array.emptyByteArray)
        )
        records <- values.quarantined.get
      } yield {
        assert(accepted)
        assertEquals(records.map(_.reason), Vector("malformed event envelope"))
        assertEquals(records.head.rawBytes.toList, List.empty[Byte])
      }
    }
  }

  test("malformed records remain uncommitted when quarantine persistence fails") {
    val failedQuarantine = new EventQuarantineRepository[IO] {
      override def save(record: EventQuarantineRecord): RepositoryF[IO, Unit] =
        com.example.graphQL.cats.service.port.RepositoryF.fromFEither(IO.pure(Left(RepositoryError.Unavailable)))
    }
    fakes.flatMap { values =>
      durable(
        config,
        values.receipts,
        failedQuarantine,
        config.topic,
        0,
        2L,
        Some("not-json".getBytes)
      )
        .map(commit => assert(!commit))
    }
  }

  test("an undurable record stops sequential processing before a later offset can commit") {
    for {
      processed <- cats.effect.Ref.of[IO, Vector[Long]](Vector.empty)
      committed <- cats.effect.Ref.of[IO, Vector[Long]](Vector.empty)
      result <- List(10L, 11L).traverse_ { offset =>
        KafkaRuntimeSupport.processRecord("hiring.events", 0, offset)(
          RepositoryF
            .lift(processed.update(_ :+ offset))
            .flatMap(_ => RepositoryF.at[IO].fromEither(Either.cond(offset != 10L, (), RepositoryError.Unavailable)))
        )(committed.update(_ :+ offset))
      }.attempt
      handled <- processed.get
      offsets <- committed.get
    } yield {
      assert(result.isLeft)
      assertEquals(handled, Vector(10L))
      assertEquals(offsets, Vector.empty)
    }
  }

  test("duplicate delivery acknowledges once and accepts events without revision gaps") {
    fakes.flatMap { values =>
      val first = event(OperationalEventType.JOB_CREATED)
      val independent = event(OperationalEventType.JOB_UPDATED)
      for {
        firstCommit <- durable(
          config,
          values.receipts,
          values.quarantines,
          config.topic,
          0,
          4L,
          Some(OperationalEventWireCodec.bytes(first))
        )
        duplicateCommit <- durable(
          config,
          values.receipts,
          values.quarantines,
          config.topic,
          0,
          5L,
          Some(OperationalEventWireCodec.bytes(first))
        )
        independentCommit <- durable(
          config,
          values.receipts,
          values.quarantines,
          config.topic,
          0,
          6L,
          Some(OperationalEventWireCodec.bytes(independent))
        )
        records <- values.quarantined.get
      } yield {
        assert(firstCommit)
        assert(duplicateCommit)
        assert(independentCommit)
        assertEquals(records, Vector.empty)
      }
    }
  }

  test("receipt failures prevent acknowledgment including unacknowledged conflicts") {
    fakes.flatMap { values =>
      List(RepositoryError.Unavailable, RepositoryError.Conflict).traverse_ { error =>
        val receipts = new ConsumerReceiptRepository[IO] {
          def exists(group: String, id: UUID): RepositoryF[IO, Boolean] =
            RepositoryF.lift(IO.raiseError(new AssertionError("Unexpected preflight read")))
          def record(
              group: String,
              value: OperationalEventEnvelope,
              at: Instant,
              until: Instant
          ): RepositoryF[IO, Boolean] =
            RepositoryF.at[IO].raise(error)
        }
        durable(
          config,
          receipts,
          values.quarantines,
          config.topic,
          0,
          7L,
          Some(OperationalEventWireCodec.bytes(event(OperationalEventType.JOB_CREATED)))
        )
          .map(commit => assert(!commit))
      }
    }
  }

  test("publisher bounds active work to four even when many facts share one key") {
    for {
      active <- Ref.of[IO, Int](0)
      maximum <- Ref.of[IO, Int](0)
      entered <- Deferred[IO, Unit]
      release <- Deferred[IO, Unit]
      fiber <- OperationalEventKafkaRuntime
        .publishClaims(List.fill(20)(claim("one-key", UUID.randomUUID()))) { _ =>
          active
            .updateAndGet(_ + 1)
            .flatMap { count =>
              maximum.update(_.max(count)) *> (if (count == 4) entered.complete(()).void else IO.unit) *> release.get
            }
            .guarantee(active.update(_ - 1))
        }
        .start
      _ <- entered.get
      before <- active.get
      _ <- release.complete(())
      _ <- fiber.joinWithNever
      peak <- maximum.get
      remaining <- active.get
    } yield {
      assertEquals(before, 4)
      assertEquals(peak, 4)
      assertEquals(remaining, 0)
    }
  }

  test("publication waves respect the per-poll budget and stop on an empty result") {
    for {
      limits <- Ref.of[IO, Vector[Int]](Vector.empty)
      published <- Ref.of[IO, Int](0)
      _ <- OperationalEventKafkaRuntime.publishWaves(10) { limit =>
        limits.update(_ :+ limit).as(List.fill(limit)(claim("one-key", UUID.randomUUID())))
      }(_ => published.update(_ + 1))
      requests <- limits.get
      count <- published.get
      emptyRequests <- Ref.of[IO, Int](0)
      _ <- OperationalEventKafkaRuntime.publishWaves(10)(_ => emptyRequests.update(_ + 1).as(Nil))(_ => IO.unit)
      emptyCount <- emptyRequests.get
    } yield {
      assertEquals(requests, Vector(4, 4, 2))
      assertEquals(count, 10)
      assertEquals(emptyCount, 1)
    }
  }

  test("publication waves reject a repository result exceeding the requested bound") {
    OperationalEventKafkaRuntime
      .publishWaves(1)(_ => IO.pure(List.fill(2)(claim("key", UUID.randomUUID()))))(_ => IO.unit)
      .attempt
      .map(result => assert(result.isLeft))
  }

  test("publisher overlaps independent partition keys") {
    for {
      arrivals <- Ref.of[IO, Int](0)
      barrier <- Deferred[IO, Unit]
      _ <- OperationalEventKafkaRuntime.publishClaims(
        List(claim("job-1", UUID.randomUUID()), claim("job-2", UUID.randomUUID()))
      ) { value =>
        arrivals.modify(count => (count + 1, count + 1)).flatMap { count =>
          if (count == 2) barrier.complete(()).void else barrier.get
        } *> IO(assert(value.partitionKey == "job-1" || value.partitionKey == "job-2"))
      }
      count <- arrivals.get
    } yield assertEquals(count, 2)
  }

  private val publisherBreakerSettings = CircuitBreakerPolicy.Settings(2, 10.seconds, 100.millis, 1.second, 1)

  private def failingPublication(breaker: CircuitBreaker[IO], error: Throwable, released: Ref[IO, Int]) =
    OperationalEventKafkaRuntime
      .publishClaim(
        config,
        publicationOutbox(IO.pure(Right(())), IO.pure(Right(())), released.update(_ + 1).as(Right(()))),
        Diagnostics.noop,
        claim("key", UUID.randomUUID())
      )(OperationalEventKafkaRuntime.guardedSend(breaker, IO.raiseError(error)))
      .attempt

  test("CBR-05.1 send failures open the breaker and no outbox rows are claimed while it is open") {
    for {
      breaker <- CircuitBreaker.create[IO](publisherBreakerSettings, _ => IO.unit)
      released <- Ref.of[IO, Int](0)
      _ <- failingPublication(breaker, new org.apache.kafka.common.errors.TimeoutException("broker down"), released)
      _ <- failingPublication(breaker, new org.apache.kafka.common.errors.TimeoutException("broker down"), released)
      claims <- Ref.of[IO, Int](0)
      claimed <- OperationalEventKafkaRuntime.claimWithin(breaker, 4)(_ => claims.update(_ + 1).as(Nil))
      attempts <- claims.get
      state <- breaker.current
    } yield {
      assertEquals(claimed, Nil)
      assertEquals(attempts, 0)
      assert(state.isInstanceOf[CircuitBreakerPolicy.State.Open], clue(state))
    }
  }

  test("CBR-05.4 repeated record-too-large failures from one poison event never open the breaker") {
    for {
      breaker <- CircuitBreaker.create[IO](publisherBreakerSettings, _ => IO.unit)
      released <- Ref.of[IO, Int](0)
      results <- List.fill(5)(failingPublication(breaker, new RecordTooLargeException("poison"), released)).sequence
      state <- breaker.current
    } yield {
      assert(results.forall(_.isRight), clue(results))
      assertEquals(state, CircuitBreakerPolicy.initial)
    }
  }

  test("CBR-05.4 only transport and broker-availability errors count against the breaker") {
    val counted = List[Throwable](
      new org.apache.kafka.common.errors.TimeoutException("timeout"),
      new NetworkException("network"),
      new DisconnectException("disconnect"),
      new NotLeaderOrFollowerException("leader moved"),
      new BrokerNotAvailableException("broker gone"),
      new java.util.concurrent.TimeoutException("send"),
      new RuntimeException("wrapped", new NetworkException("cause"))
    )
    val ignored = List[Throwable](
      new RecordTooLargeException("too large"),
      new SerializationException("bad record"),
      new InvalidTopicException("topic"),
      new TopicAuthorizationException("denied"),
      new ProducerFencedException("fenced"),
      new IllegalStateException("bug")
    )
    assert(counted.forall(OperationalEventKafkaRuntime.isBrokerUnavailable), clue(counted))
    assert(!ignored.exists(OperationalEventKafkaRuntime.isBrokerUnavailable), clue(ignored))
  }

  test("CBR-05.2 producer fencing is not counted and still stops the stream") {
    for {
      breaker <- CircuitBreaker.create[IO](publisherBreakerSettings, _ => IO.unit)
      released <- Ref.of[IO, Int](0)
      results <- List
        .fill(4)(failingPublication(breaker, new ProducerFencedException("fenced"), released))
        .sequence
      state <- breaker.current
      retried <- released.get
    } yield {
      assert(results.forall(_.isLeft), clue(results))
      assertEquals(state, CircuitBreakerPolicy.initial)
      assertEquals(retried, 0)
    }
  }

  test("CBR-05.3 after the open period a single probe is claimed and success drains normally") {
    for {
      breaker <- CircuitBreaker.create[IO](publisherBreakerSettings, _ => IO.unit)
      released <- Ref.of[IO, Int](0)
      _ <- failingPublication(breaker, new org.apache.kafka.common.errors.TimeoutException("broker down"), released)
      _ <- failingPublication(breaker, new org.apache.kafka.common.errors.TimeoutException("broker down"), released)
      _ <- IO.sleep(200.millis)
      limits <- Ref.of[IO, List[Int]](Nil)
      probeClaims <- OperationalEventKafkaRuntime.claimWithin(breaker, 4)(limit =>
        limits.update(_ :+ limit).as(List(claim("key", UUID.randomUUID())))
      )
      _ <- OperationalEventKafkaRuntime.publishClaims(probeClaims)(_ =>
        OperationalEventKafkaRuntime.guardedSend(breaker, IO.unit)
      )
      afterRecovery <- OperationalEventKafkaRuntime.claimWithin(breaker, 4)(limit => limits.update(_ :+ limit).as(Nil))
      observed <- limits.get
    } yield {
      assertEquals(observed, List(1, 4))
      assertEquals(afterRecovery, Nil)
    }
  }

  test("CBR-05.1 a send rejected by the open breaker is released for retry without being marked failed") {
    for {
      breaker <- CircuitBreaker.create[IO](publisherBreakerSettings, _ => IO.unit)
      released <- Ref.of[IO, Int](0)
      _ <- failingPublication(breaker, new org.apache.kafka.common.errors.TimeoutException("broker down"), released)
      _ <- failingPublication(breaker, new org.apache.kafka.common.errors.TimeoutException("broker down"), released)
      before <- released.get
      sent <- Ref.of[IO, Boolean](false)
      _ <- OperationalEventKafkaRuntime
        .publishClaim(
          config,
          publicationOutbox(IO.pure(Right(())), IO.pure(Right(())), released.update(_ + 1).as(Right(()))),
          Diagnostics.noop,
          claim("key", UUID.randomUUID())
        )(OperationalEventKafkaRuntime.guardedSend(breaker, sent.set(true)))
      after <- released.get
      didSend <- sent.get
    } yield {
      assertEquals(after - before, 1)
      assert(!didSend)
    }
  }

  private def publicationOutbox(
      renewal: IO[Either[RepositoryError, Unit]],
      published: IO[Either[RepositoryError, Unit]],
      retried: IO[Either[RepositoryError, Unit]] = IO.pure(Right(()))
  ): OperationalEventOutboxRepository[IO] = new OperationalEventOutboxRepository[IO] {
    def claim(
        workerId: String,
        transactionalId: String,
        at: Instant,
        until: Instant,
        limit: Int
    ): RepositoryF[IO, List[ClaimedOperationalEvent]] = RepositoryF.at[IO].raise(RepositoryError.Unavailable)
    def renewLease(id: UUID, token: String, subjects: List[String], until: Instant): RepositoryF[IO, Unit] =
      RepositoryF.fromFEither(renewal)
    def markPublished(id: UUID, token: String, at: Instant, expires: Instant): RepositoryF[IO, Unit] =
      RepositoryF.fromFEither(published)
    def releaseForRetry(id: UUID, token: String, at: Instant, available: Instant): RepositoryF[IO, Unit] =
      RepositoryF.fromFEither(retried)
    def markFailed(id: UUID, token: String, at: Instant, reason: String): RepositoryF[IO, Unit] =
      RepositoryF.fromFEither(retried)
  }

  test("publication renews its guarded lease before sending and then records durable success") {
    for {
      observed <- Ref.of[IO, Vector[String]](Vector.empty)
      outbox = publicationOutbox(
        observed.update(_ :+ "renew").as(Right(())),
        observed.update(_ :+ "published").as(Right(()))
      )
      _ <- OperationalEventKafkaRuntime.publishClaim(config, outbox, Diagnostics.noop, claim("key", UUID.randomUUID()))(
        observed.update(_ :+ "send")
      )
      events <- observed.get
    } yield assertEquals(events, Vector("renew", "send", "published"))
  }

  test("a rejected lease prevents any Kafka send") {
    for {
      sent <- Ref.of[IO, Boolean](false)
      outbox = publicationOutbox(IO.pure(Left(RepositoryError.Conflict)), IO.pure(Right(())))
      result <- OperationalEventKafkaRuntime
        .publishClaim(config, outbox, Diagnostics.noop, claim("key", UUID.randomUUID()))(sent.set(true))
        .attempt
      didSend <- sent.get
    } yield {
      assert(result.isLeft)
      assert(!didSend)
    }
  }

  test("queued publication keeps renewing and a lost lease cancels the uncertain send") {
    for {
      renewals <- Ref.of[IO, Int](0)
      cancelled <- Ref.of[IO, Boolean](false)
      completed <- Ref.of[IO, Int](0)
      outbox = publicationOutbox(
        renewals.updateAndGet(_ + 1).map(count => if (count == 1) Right(()) else Left(RepositoryError.Conflict)),
        completed.update(_ + 1).as(Right(()))
      )
      shortLease = config.copy(publisher = config.publisher.copy(leaseSeconds = 3))
      result <- OperationalEventKafkaRuntime
        .publishClaim(shortLease, outbox, Diagnostics.noop, claim("key", UUID.randomUUID()))(
          IO.never[Unit].onCancel(cancelled.set(true))
        )
        .attempt
        .timeout(5.seconds)
      count <- renewals.get
      wasCancelled <- cancelled.get
      completions <- completed.get
    } yield {
      assert(result.isLeft)
      assertEquals(count, 2)
      assert(wasCancelled)
      assertEquals(completions, 0)
    }
  }

  test("cancelling uncertain publication stops the send without completing or retrying its claim") {
    for {
      entered <- Deferred[IO, Unit]
      cancelled <- Ref.of[IO, Boolean](false)
      completions <- Ref.of[IO, Int](0)
      retries <- Ref.of[IO, Int](0)
      outbox = publicationOutbox(
        IO.pure(Right(())),
        completions.update(_ + 1).as(Right(())),
        retries.update(_ + 1).as(Right(()))
      )
      fiber <- OperationalEventKafkaRuntime
        .publishClaim(config, outbox, Diagnostics.noop, claim("key", UUID.randomUUID()))(
          (entered.complete(()) *> IO.never[Unit]).onCancel(cancelled.set(true))
        )
        .start
      _ <- entered.get
      _ <- fiber.cancel
      wasCancelled <- cancelled.get
      completed <- completions.get
      retried <- retries.get
    } yield {
      assert(wasCancelled)
      assertEquals(completed, 0)
      assertEquals(retried, 0)
    }
  }

  test("failure recording publication after Kafka success never resends in the same attempt") {
    for {
      sends <- Ref.of[IO, Int](0)
      outbox = publicationOutbox(IO.pure(Right(())), IO.pure(Left(RepositoryError.Unavailable)))
      result <- OperationalEventKafkaRuntime
        .publishClaim(config, outbox, Diagnostics.noop, claim("key", UUID.randomUUID()))(sends.update(_ + 1))
        .attempt
      count <- sends.get
    } yield {
      assert(result.isLeft)
      assertEquals(count, 1)
    }
  }

  test("restart delay remains bounded without giving up during a prolonged outage") {
    val policy = KafkaRuntimeSupport.restartPolicy(1.second, 30.seconds)
    (0 to 1000).toList.traverse_ { count =>
      policy.decideNextRetry((), retry.RetryStatus(count, 1.day, Some(30.seconds))).map {
        case retry.PolicyDecision.DelayAndRetry(delay) =>
          assert(delay >= Duration.Zero && delay <= 30.seconds)
        case retry.PolicyDecision.GiveUp => fail("Recovery must remain retryable")
      }
    }
  }

  test("resilient stream retries failed effects until they recover") {
    for {
      attempts <- Ref.of[IO, Int](0)
      stream = KafkaRuntimeSupport.resilientStream(
        Diagnostics.noop,
        fs2.Stream.eval {
          attempts.updateAndGet(_ + 1).flatMap { count =>
            if (count < 3) IO.raiseError[Unit](new IllegalStateException("transient kafka failure"))
            else IO.unit
          }
        },
        1.millis,
        maxDelay = 30.seconds
      )
      _ <- stream.compile.drain
      result <- attempts.get
    } yield assertEquals(result, 3)
  }

  test("producer fencing is detected through wrapped Kafka errors") {
    assert(
      KafkaRuntimeSupport.isProducerFenced(
        new IllegalStateException("send failed", new ProducerFencedException("producer fenced"))
      )
    )
    assert(!KafkaRuntimeSupport.isProducerFenced(new IllegalStateException("ordinary send failure")))
  }

  test("a fenced producer generation is released before the next generation starts") {
    for {
      acquired <- Ref.of[IO, Int](0)
      released <- Ref.of[IO, Int](0)
      transactionalIds <- Ref.of[IO, Vector[String]](Vector.empty)
      secondGeneration <- Deferred[IO, Unit]
      generation = Stream.suspend {
        Stream.eval(UUIDGen[IO].randomUUID.map(id => s"hiring-publisher-$id")).flatMap { transactionalId =>
          Stream.eval(transactionalIds.update(_ :+ transactionalId)) >>
            Stream
              .resource(Resource.make(acquired.update(_ + 1))(_ => released.update(_ + 1)))
              .flatMap { _ =>
                Stream.eval(acquired.get.flatMap {
                  case 1 =>
                    IO.raiseError[Unit](
                      new IllegalStateException("wrapped fence", new ProducerFencedException("fenced"))
                    )
                  case _ => secondGeneration.complete(()).void *> IO.never[Unit]
                })
              }
        }
      }
      supervised = Stream
        .suspend(
          KafkaRuntimeSupport
            .resilientStream(
              Diagnostics.noop,
              generation,
              1.millis,
              KafkaRuntimeSupport.isProducerFenced,
              maxDelay = 30.seconds
            )
            .handleErrorWith {
              case error if KafkaRuntimeSupport.isProducerFenced(error) => Stream.empty
              case error                                                => Stream.raiseError[IO](error)
            }
        )
        .repeat
      fiber <- supervised.compile.drain.start
      _ <- secondGeneration.get
      _ <- fiber.cancel
      observedAcquired <- acquired.get
      observedReleased <- released.get
      observedTransactionalIds <- transactionalIds.get
    } yield {
      assertEquals(observedAcquired, 2)
      assertEquals(observedReleased, 2)
      assertEquals(observedTransactionalIds.size, 2)
      assertEquals(observedTransactionalIds.distinct.size, 2)
    }
  }

  test("resilient stream emits sanitized runtime failure diagnostics") {
    for {
      attempts <- Ref.of[IO, Int](0)
      records <- Ref.of[IO, Vector[(LogEvent, Map[LogField, String])]](Vector.empty)
      diagnostics = new Diagnostics[IO] {
        override def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]): IO[Unit] =
          records.update(_ :+ (event -> fields))
      }
      _ <- KafkaRuntimeSupport
        .resilientStream(
          diagnostics,
          fs2.Stream.eval {
            attempts.updateAndGet(_ + 1).flatMap { count =>
              if (count == 1) IO.raiseError[Unit](new IllegalStateException("transient kafka failure"))
              else IO.unit
            }
          },
          1.millis,
          maxDelay = 30.seconds
        )
        .compile
        .drain
      emitted <- records.get
    } yield {
      assertEquals(emitted.map(_._1), Vector(LogEvent.RuntimeFailed))
      assertEquals(emitted.head._2.get(LogField.ErrorType), Some("java.lang.IllegalStateException"))
    }
  }

  test("resilient stream cancellation interrupts retry backoff") {
    for {
      attempted <- Deferred[IO, Unit]
      fiber <- KafkaRuntimeSupport
        .resilientStream(
          Diagnostics.noop,
          fs2.Stream.eval(
            attempted.complete(()) *> IO.raiseError[Unit](new IllegalStateException("broker unavailable"))
          ),
          1.hour,
          maxDelay = 30.seconds
        )
        .compile
        .drain
        .start
      _ <- attempted.get
      _ <- fiber.cancel
    } yield assert(true)
  }
}
