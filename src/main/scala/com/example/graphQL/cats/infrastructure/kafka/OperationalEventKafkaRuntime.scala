package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.{Clock, Deferred, IO, Ref, Resource}
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.example.graphQL.cats.config.KafkaConfig
import com.example.graphQL.cats.domain.policy.{
  CircuitBreakerPolicy,
  ConsumerReceiptWindow,
  ConsumerReceiptWindowRefusal
}
import org.apache.kafka.common.errors.{
  AuthorizationException,
  BrokerNotAvailableException,
  InvalidTopicException,
  RecordTooLargeException,
  RetriableException,
  SerializationException
}
import com.example.graphQL.cats.service.resilience.{CircuitBreaker, LeaseHeartbeat, LeaseLost}
import com.example.graphQL.cats.service.port.{
  ProducerGeneration,
  ProducerGenerationId,
  ClaimedOperationalEvent,
  ConsumerReceiptRepository,
  EventQuarantineRecord,
  EventQuarantineRepository,
  OperationalEventFailureCategory,
  OperationalEventOutboxRepository,
  RepositoryF
}
import com.example.graphQL.cats.service.{
  BackgroundWorker,
  Diagnostics,
  LogEvent,
  LogField,
  LogFields,
  ReadinessComponent,
  WorkerAlive
}
import com.example.graphQL.cats.service.Diagnostics.*
import com.example.graphQL.cats.infrastructure.events.{OperationalEventHeaders, OperationalEventWireCodec}
import com.example.graphQL.cats.infrastructure.telemetry.KafkaClientMetrics
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.shared.CauseChain
import fs2.Stream
import fs2.kafka.*
import fs2.kafka.producer.MkProducer

import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeoutException
import scala.concurrent.duration.*
import scala.util.control.NoStackTrace

object OperationalEventKafkaRuntime {
  private val PublisherWorker = "operational-event-publisher"
  private val ConsumerWorker = "operational-event-consumer"
  private val PublicationWaveSize = 4
  private val OperationalRequestBytes = 1048576

  /** How often a running consumer re-reads the topic retention (DDC-02.10): one admin describe per 5 minutes,
    * independent of the 20 s heartbeat. A retention raise is detected long before any receipt written now can expire.
    */
  val ReceiptWindowRecheck: FiniteDuration = 5.minutes

  /** Cleanup horizon for Published outbox rows (TTL index `event_outbox_published_retention`), independent of the
    * topic's `retention.ms`: nothing re-sends a Published row (only an InFlight row whose lease expired is sent again),
    * so the two values need not match. Receipt expiry covers re-sends through DDC-02.13.
    */
  val PublishedOutboxRetention: FiniteDuration = 7.days

  def resource(
      config: KafkaConfig,
      outbox: OperationalEventOutboxRepository[IO],
      receipts: ConsumerReceiptRepository[IO],
      quarantine: EventQuarantineRepository[IO],
      diagnostics: Diagnostics[IO],
      clock: Clock[IO] = Clock[IO],
      uuidGen: UUIDGen[IO] = UUIDGen[IO],
      breaker: CircuitBreaker[IO] = CircuitBreaker.disabled[IO],
      alive: ReadinessComponent => IO[WorkerAlive[IO]] = _ => IO.pure(WorkerAlive.none[IO]),
      metrics: KafkaClientMetrics = KafkaClientMetrics.noop
  ): Resource[IO, Unit] =
    if (!config.enabled) Resource.unit
    else
      for {
        publisherAlive <- Resource.eval(alive(ReadinessComponent.KafkaPublisher))
        _ <- publisherResource(config, outbox, diagnostics, clock, uuidGen, breaker, publisherAlive.beat, metrics)
        _ <-
          if (config.consumer.enabled)
            Resource.eval(alive(ReadinessComponent.KafkaConsumer)).flatMap { consumerAlive =>
              consumerResource(config, receipts, quarantine, diagnostics, clock, consumerAlive, metrics = metrics)
            }
          else Resource.unit[IO]
      } yield ()

  private def publisherResource(
      config: KafkaConfig,
      outbox: OperationalEventOutboxRepository[IO],
      diagnostics: Diagnostics[IO],
      clock: Clock[IO],
      uuidGen: UUIDGen[IO],
      breaker: CircuitBreaker[IO],
      batchDone: IO[Unit],
      metrics: KafkaClientMetrics
  ): Resource[IO, Unit] = {
    val settings = KafkaClientSettings.producer(
      config.bootstrapServers,
      config.publisher.saslUsername,
      config.publisher.saslPassword,
      config.saslSecurityProtocol,
      OperationalRequestBytes,
      config.saslMechanism,
      config.publisher.leaseSeconds
    )

    def openProducer(generation: ProducerGeneration): Resource[IO, SendWave] =
      KafkaRuntimeSupport
        .observedGeneration(generation.transactionalId, generation.generationId.value, diagnostics)(
          GuardedTransactionalProducer
            .resource(
              TransactionalProducerSettings(generation.transactionalId, settings),
              MkProducer.mkProducerForSync[IO]
            )
        )
        .map(producer => records => producer.produceWithoutOffsets(records).void)

    Resource.eval(Ref.of[IO, Int](0)).flatMap { retrySingly =>
      BackgroundWorker.resource(PublisherWorker, diagnostics)(
        Stream
          .suspend(
            publisherGeneration(
              config,
              outbox,
              diagnostics,
              clock,
              uuidGen,
              breaker,
              batchDone,
              metrics,
              retrySingly,
              openProducer
            )
          )
          .repeat
          .compile
          .drain
      )
    }
  }

  /** One lifetime of the publisher's producer. The transactional id is the stable `hiring-publisher-<worker id>`, so
    * initializing it fences any earlier lifetime of the same worker; the generation id is drawn fresh on every
    * initialization and reaches the registry only through `claim`, which runs after `openProducer` has succeeded
    * (`initTransactions` included). A fenced or abandoned generation ends here and its successor claims afresh: claims
    * held in memory are never carried over.
    */
  private[kafka] def publisherGeneration(
      config: KafkaConfig,
      outbox: OperationalEventOutboxRepository[IO],
      diagnostics: Diagnostics[IO],
      clock: Clock[IO],
      uuidGen: UUIDGen[IO],
      breaker: CircuitBreaker[IO],
      batchDone: IO[Unit],
      metrics: KafkaClientMetrics,
      retrySingly: Ref[IO, Int],
      openProducer: ProducerGeneration => Resource[IO, SendWave]
  ): Stream[IO, Unit] =
    Stream.eval(uuidGen.randomUUID).flatMap { id =>
      val generation = ProducerGeneration(
        KafkaProducerGenerationRetirement.PublisherPrefix + config.publisher.workerId.value,
        ProducerGenerationId(id)
      )
      Stream
        .resource(openProducer(generation))
        .flatMap { send =>
          KafkaRuntimeSupport.resilientStream(
            diagnostics,
            Stream
              .awakeEvery[IO](config.publisher.pollIntervalMillis.millis)
              .evalMap(_ =>
                publishBatch(config, outbox, diagnostics, generation, send, clock, breaker, metrics, retrySingly) *>
                  batchDone
              ),
            config.publisher.retryDelaySeconds.seconds,
            stopRetrying = error => KafkaRuntimeSupport.isProducerFenced(error) || error.isInstanceOf[WaveAbandoned],
            maxDelay = config.restartMaxDelaySeconds.seconds
          )
        }
        .handleErrorWith {
          // A fenced or abandoned generation is replaced after the retry delay, so two instances sharing a worker id
          // fence each other at that pace instead of spinning.
          case _: ProducerGenerationFenced | _: WaveAbandoned =>
            Stream.eval(diagnostics.emit(LogEvent.RuntimeFailed)) ++
              Stream.sleep_[IO](config.publisher.retryDelaySeconds.seconds)
          case error =>
            Stream.eval(diagnostics.emit(LogEvent.RuntimeFailed, fields = LogFields.failure(error))) ++
              Stream.sleep_[IO](config.publisher.retryDelaySeconds.seconds)
        }
    }

  /** One transactional send of a whole wave of records. */
  private[kafka] type SendWave = ProducerRecords[String, Array[Byte]] => IO[Unit]

  private[kafka] def publishBatch(
      config: KafkaConfig,
      outbox: OperationalEventOutboxRepository[IO],
      diagnostics: Diagnostics[IO],
      generation: ProducerGeneration,
      sendWave: SendWave,
      clock: Clock[IO],
      breaker: CircuitBreaker[IO],
      metrics: KafkaClientMetrics,
      retrySingly: Ref[IO, Int]
  ): IO[Unit] =
    publishWaves(config.publisher.batchSize) { limit =>
      // After a multi-claim wave failed for a reason that may belong to one event, the released claims are retried as
      // waves of one, so only the event that really fails is charged (one counter step per claim taken).
      retrySingly.get.flatMap { pending =>
        claimWithin(breaker, if (pending > 0) 1 else limit) { allowed =>
          clock.realTimeInstant.flatMap { now =>
            requireOutboxSuccess(
              outbox.claim(
                config.publisher.workerId.value,
                generation,
                now,
                config.publisher.leaseSeconds.seconds,
                allowed
              )
            )
          }
        }.flatTap(claims => if (pending > 0) retrySingly.update(n => (n - claims.size).max(0)) else IO.unit)
      }
    } { claims =>
      publishWave(config, outbox, diagnostics, claims, clock, count => retrySingly.update(_ + count))(
        guardedSend(
          breaker,
          metrics.timedSend(sendWave(ProducerRecords(claims.map(producerRecord(config.topic, _)))))
        )
      )
    }

  /** The Kafka key is the aggregate id (job, application or search id), so one aggregate stays in one partition. The
    * headers repeat the envelope's schema version, event type and id; the body stays authoritative. No trace context is
    * attached: the outbox stores none, and the publisher runs outside any request span.
    */
  private[cats] def producerRecord(
      topic: String,
      claim: ClaimedOperationalEvent
  ): ProducerRecord[String, Array[Byte]] =
    ProducerRecord(topic, claim.partitionKey, claim.envelopeBytes)
      .withHeaders(
        Headers.fromIterable(
          OperationalEventHeaders.operational(claim.event).map((key, value) => Header(key, value))
        )
      )

  /** While the breaker is open nothing is claimed, so outbox rows stay untouched until a probe is allowed; probing
    * claims a single event.
    */
  private[kafka] def claimWithin(breaker: CircuitBreaker[IO], limit: Int)(
      claim: Int => IO[List[ClaimedOperationalEvent]]
  ): IO[List[ClaimedOperationalEvent]] =
    breaker.admittedState.flatMap {
      case None                                       => IO.pure(Nil)
      case Some(CircuitBreakerPolicy.State.Closed(_)) => claim(limit)
      case Some(_)                                    => claim(1)
    }

  /** Transport and broker-availability failures only: timeouts, network and disconnect errors, leader changes and other
    * retriable broker errors. Per-record (too large, serialization, invalid topic), authorization and fencing errors
    * say nothing about the broker, and one poison event must not open the breaker for all publication.
    */
  private[kafka] def isBrokerUnavailable(error: Throwable): Boolean = {
    val chain = CauseChain(error).toList
    val perRecordOrAuthorization = chain.exists {
      case _: RecordTooLargeException | _: SerializationException | _: InvalidTopicException |
          _: AuthorizationException =>
        true
      case _ => false
    }
    val transport = chain.exists {
      case _: RetriableException | _: BrokerNotAvailableException | _: TimeoutException | _: java.io.IOException =>
        true
      case _ => false
    }
    !KafkaRuntimeSupport.isProducerFenced(error) && !perRecordOrAuthorization && transport
  }

  /** Send failures count only when they show the broker is unavailable. */
  private[kafka] def guardedSend(breaker: CircuitBreaker[IO], send: IO[Unit]): IO[Unit] =
    breaker
      .protect(send)(CircuitBreaker.classifier[IO, Unit](isBrokerUnavailable))
      .flatMap(_.fold(_ => IO.raiseError(PublisherBreakerOpen), IO.pure))

  /** Claim only work that can start its lease heartbeat immediately. Each claim result is one publication wave. */
  private[kafka] def publishWaves(batchSize: Int)(
      claim: Int => IO[List[ClaimedOperationalEvent]]
  )(publish: List[ClaimedOperationalEvent] => IO[Unit]): IO[Unit] = {
    def loop(remaining: Int): IO[Unit] =
      if (remaining <= 0) IO.unit
      else {
        val limit = remaining.min(PublicationWaveSize)
        claim(limit).flatMap {
          case Nil                           => IO.unit
          case claims if claims.size > limit =>
            IO.raiseError(OutboxOperationFailed("claim exceeded requested publication wave"))
          case claims => publish(claims) *> loop(remaining - claims.size)
        }
      }
    loop(batchSize)
  }

  /** One Kafka transaction for the claims of a wave, under every claim's lease.
    *
    *   - Every lease is renewed once before the send; a lease already lost sends nothing, releases the other claims and
    *     raises.
    *   - While `send` runs, each claim has its own [[LeaseHeartbeat]] guard on the one shared send outcome. A lease
    *     lost meanwhile cancels the send, leaves the lost claim to expire, releases the others and ends the generation
    *     (the cancelled transaction is aborted by the successor's initialization).
    *   - ProducerFenced releases every claim and ends the generation. Releases here never fail the wave on `Conflict`.
    *   - Success marks each claim Published independently; every claim is tried and the first failure is raised after.
    *   - An attempt is charged when the claim is taken; here it is only *judged*: a claim sent alone, or any claim
    *     after a broker-availability failure, becomes Failed once its attempts are exhausted, else it is released after
    *     the retry delay. Any other failure of a multi-claim wave (for example one oversize record) cannot be pinned on
    *     one event, so the claims are released at once, never Failed, and `retrySingly` asks for them to be claimed one
    *     per wave.
    */
  private[kafka] def publishWave(
      config: KafkaConfig,
      outbox: OperationalEventOutboxRepository[IO],
      diagnostics: Diagnostics[IO],
      claims: List[ClaimedOperationalEvent],
      clock: Clock[IO] = Clock[IO],
      retrySingly: Int => IO[Unit] = _ => IO.unit
  )(send: IO[Unit]): IO[Unit] = {
    val lease = config.publisher.leaseSeconds.seconds
    val retryDelay = config.publisher.retryDelaySeconds.toLong

    def renew(claim: ClaimedOperationalEvent): IO[Either[LeaseLost, Unit]] =
      outbox
        .renewLease(claim.event.eventId, claim.leaseToken, claim.subjectIds, lease)
        .value
        .map {
          case Right(_)                       => Either.right[LeaseLost, Unit](())
          case Left(RepositoryError.Conflict) => Left(LeaseLost.Expired)
          case Left(_)                        => Left(LeaseLost.Unavailable)
        }
        .handleError(_ => Left(LeaseLost.Unavailable))

    // Releases never fail the wave: a Conflict means the claim is no longer ours, anything else is logged and the
    // claim simply waits for its lease to expire.
    def releaseAll(targets: List[ClaimedOperationalEvent], delaySeconds: Long): IO[Unit] =
      targets.traverse_ { claim =>
        clock.realTimeInstant
          .flatMap(at =>
            outbox.releaseUncharged(claim.event.eventId, claim.leaseToken, at, at.plusSeconds(delaySeconds)).value
          )
          .flatMap {
            case Right(_) | Left(RepositoryError.Conflict) => IO.unit
            case Left(error)                               =>
              diagnostics.emit(
                LogEvent.RuntimeFailed,
                fields = LogFields.failure(OutboxOperationFailed(error.toString))
              )
          }
          .handleErrorWith(error => diagnostics.emit(LogEvent.RuntimeFailed, fields = LogFields.failure(error)))
      }

    def abandon(lost: Set[UUID], failure: Throwable): IO[Nothing] =
      diagnostics.emit(LogEvent.PublicationWaveAbandoned) *>
        releaseAll(claims.filterNot(claim => lost(claim.event.eventId)), 0L) *>
        IO.raiseError(failure)

    // Every claim is tried; the first failure is raised once all were.
    def settleEach(step: (ClaimedOperationalEvent, Instant) => RepositoryF[IO, Unit]): IO[Unit] =
      clock.realTimeInstant
        .flatMap(at => claims.traverse(claim => requireOutboxSuccess(step(claim, at)).attempt))
        .flatMap(results => results.collectFirst { case Left(error) => error }.fold(IO.unit)(IO.raiseError))

    def settleFailure(error: Throwable): IO[Unit] =
      if (claims.sizeIs == 1 || isBrokerUnavailable(error))
        settleEach { (claim, at) =>
          if (claim.attempts >= config.publisher.maxAttempts)
            outbox.markFailed(claim.event.eventId, claim.leaseToken, at, sanitized(error))
          else outbox.releaseForRetry(claim.event.eventId, claim.leaseToken, at, at.plusSeconds(retryDelay))
        }
      else releaseAll(claims, 0L) *> retrySingly(claims.size)

    val send_ : IO[Either[Throwable, Unit]] =
      for {
        sent <- Deferred[IO, Either[Throwable, Unit]]
        lost <- Ref.of[IO, Set[UUID]](Set.empty)
        lostSignal <- Deferred[IO, Unit]
        heartbeats = claims.parTraverse_ { claim =>
          LeaseHeartbeat
            .guard(lease, renewFirst = false)(renew(claim))(sent.get)
            .flatMap {
              case Left(_)  => lost.update(_ + claim.event.eventId) *> lostSignal.complete(()).void
              case Right(_) => IO.unit
            }
        }
        raced <- (send.attempt.flatMap(sent.complete(_)).void.background, heartbeats.background).tupled
          .use(_ => IO.race(lostSignal.get, sent.get))
        result <- raced match {
          case Left(_) =>
            lost.get.flatMap(ids => abandon(ids, WaveAbandoned("lease lost during Kafka send")))
          case Right(value) => IO.pure(value)
        }
      } yield result

    for {
      initial <- claims.traverse(claim => renew(claim).map(claim.event.eventId -> _))
      lostAtStart = initial.collect { case (id, Left(_)) => id }.toSet
      _ <-
        if (lostAtStart.isEmpty) IO.unit
        else abandon(lostAtStart, OutboxOperationFailed("lease lost before Kafka send"))
      outcome <- send_
      _ <- outcome match {
        case Right(_) =>
          settleEach((claim, at) =>
            outbox.markPublished(
              claim.event.eventId,
              claim.leaseToken,
              at,
              at.plusSeconds(PublishedOutboxRetention.toSeconds)
            )
          )
        case Left(error) if KafkaRuntimeSupport.isProducerFenced(error) =>
          releaseAll(claims, 0L) *> IO.raiseError(ProducerGenerationFenced(error))
        case Left(PublisherBreakerOpen) =>
          // Never sent: not a delivery that can exhaust the event, so it is only released.
          releaseAll(claims, retryDelay)
        case Left(error) =>
          diagnostics.emit(LogEvent.RuntimeFailed, fields = LogFields.failure(error)) *> settleFailure(error)
      }
    } yield ()
  }

  private def requireOutboxSuccess[A](result: RepositoryF[IO, A]): IO[A] =
    result.leftMap(error => OutboxOperationFailed(error.toString)).rethrowT

  private[kafka] def consumerResource(
      config: KafkaConfig,
      receipts: ConsumerReceiptRepository[IO],
      quarantine: EventQuarantineRepository[IO],
      diagnostics: Diagnostics[IO],
      clock: Clock[IO] = Clock[IO],
      alive: WorkerAlive[IO] = WorkerAlive.none[IO],
      topicRetention: KafkaConfig => IO[TopicRetention] = KafkaTopicRetentionReader.read,
      receiptWindowRecheck: FiniteDuration = ReceiptWindowRecheck,
      metrics: KafkaClientMetrics = KafkaClientMetrics.noop
  ): Resource[IO, Unit] = {
    val settings = KafkaClientSettings.committedConsumer(
      config.bootstrapServers,
      config.consumer.saslUsername,
      config.consumer.saslPassword,
      config.saslSecurityProtocol,
      config.consumerGroup,
      config.saslMechanism,
      config.consumer.maxPollRecords,
      config.consumer.maxPollIntervalSeconds
    )

    BackgroundWorker.resource(ConsumerWorker, diagnostics)(
      KafkaRuntimeSupport
        .resilientStream(
          diagnostics,
          // The gate runs on every restart and before any consumer exists, so a refusal commits no offset and records
          // no beat; the restart policy paces the re-reads, so a corrected topic is picked up without a process restart.
          Stream.eval(requireReceiptWindow(config, diagnostics, topicRetention(config))) >> KafkaRuntimeSupport
            .consume(
              settings,
              config.topic,
              config.consumerGroup,
              config.consumer.partitionConcurrency,
              CommitBatching(config.consumer.commitBatchSize, config.consumer.commitBatchIntervalMillis.millis),
              metrics,
              diagnostics,
              alive.every
            ) { message =>
              val record = message.record
              KafkaRuntimeSupport.processRecord(record.topic, record.partition, record.offset)(
                recordDurably(
                  config,
                  receipts,
                  quarantine,
                  record.topic,
                  record.partition,
                  record.offset,
                  Option(record.value),
                  clock,
                  OperationalEventHeaders.fromKafka(record.headers)
                )
              )(message.offset)
            }
            .concurrently(Stream.awakeEvery[IO](alive.every).evalMap(_ => alive.beat))
            // A refusal or unreadable configuration raises here; `concurrently` fails the foreground stream, the consumer
            // closes and the restart re-runs the start gate, which keeps refusing until the topic is corrected.
            .concurrently(
              Stream
                .awakeEvery[IO](receiptWindowRecheck)
                .evalMap(_ => requireReceiptWindow(config, diagnostics, topicRetention(config)))
            ),
          1.second,
          maxDelay = config.restartMaxDelaySeconds.seconds
        )
        .compile
        .drain
    )
  }

  /** Refuses to consume unless the receipt outlives every message the topic can still deliver (DDC-02). */
  private[kafka] def requireReceiptWindow(
      config: KafkaConfig,
      diagnostics: Diagnostics[IO],
      topicRetention: IO[TopicRetention]
  ): IO[Unit] =
    topicRetention.flatMap { retention =>
      val receiptTtl = config.consumer.receiptTtlDays.days
      ConsumerReceiptWindow.check(receiptTtl, retention.retentionMs, retention.segmentMs) match {
        case Right(())     => IO.unit
        case Left(refusal) =>
          diagnostics.emit(
            LogEvent.ConsumerReceiptWindowRefused,
            fields = Map(
              LogField.RetentionMs -> retention.retentionMs.toString,
              LogField.SegmentMs -> retention.segmentMs.toString,
              LogField.ReceiptTtlMs -> receiptTtl.toMillis.toString,
              LogField.WindowRefusal -> refusal.toString
            )
          ) *> IO.raiseError(ConsumerReceiptWindowRefused(refusal))
      }
    }

  /** Makes the record durable (receipt, or quarantine when malformed); a repository failure stays typed.
    *
    * The same event can arrive more than once: a publisher lease does not fence the Kafka send, and an offset commit
    * can be lost after the receipt is stored. Here the receipt is the whole effect, so a duplicate key is a completed
    * delivery. The effect rule and the revisit trigger for a future effect are on [[ConsumerReceiptRepository]].
    */
  private[kafka] def recordDurably(
      config: KafkaConfig,
      receipts: ConsumerReceiptRepository[IO],
      quarantine: EventQuarantineRepository[IO],
      topic: String,
      partition: Int,
      offset: Long,
      bytes: Option[Array[Byte]],
      clock: Clock[IO] = Clock[IO],
      headers: OperationalEventHeaders.RecordHeaders = Nil
  ): RepositoryF[IO, Unit] =
    RepositoryF.lift(clock.realTimeInstant).flatMap { now =>
      def reject(category: OperationalEventFailureCategory, reason: String) =
        quarantine.save(
          EventQuarantineRecord(
            topic,
            partition,
            offset,
            category,
            reason,
            bytes.getOrElse(Array.emptyByteArray),
            now,
            now.plusSeconds(config.consumer.quarantineTtlDays.days.toSeconds)
          )
        )
      // A newer schema version is refused before the body is read: its body may not decode under version 1 rules.
      // Headers are verified, never stored; only the closed reason below reaches the quarantine record.
      if (OperationalEventHeaders.version(headers).exists(_ != OperationalEventHeaders.CurrentSchemaVersion))
        reject(OperationalEventFailureCategory.UnsupportedVersion, UnsupportedVersionReason)
      else
        bytes.toRight("MalformedEnvelope").flatMap(OperationalEventWireCodec.decode) match {
          case Left(_) =>
            reject(
              OperationalEventFailureCategory.MalformedEnvelope,
              bytes.fold("null event envelope")(_ => "malformed event envelope")
            )
          case Right(event) =>
            OperationalEventHeaders.verify(headers, event) match {
              case Left(OperationalEventHeaders.HeaderRefusal.UnsupportedVersion) =>
                reject(OperationalEventFailureCategory.UnsupportedVersion, UnsupportedVersionReason)
              case Left(_) =>
                reject(OperationalEventFailureCategory.MalformedEnvelope, "event headers disagree with the envelope")
              case Right(()) =>
                receipts
                  .record(
                    config.consumerGroup,
                    event,
                    now,
                    now.plusSeconds(config.consumer.receiptTtlDays.days.toSeconds)
                  )
                  .void
            }
        }
    }

  private val UnsupportedVersionReason = "unsupported event schema version"

  /** Client exception messages can embed rejected configuration values, so only class names reach the outbox. */
  private[kafka] def sanitized(error: Throwable): String =
    CauseChain(error)
      .take(5)
      .map(_.getClass.getName)
      .mkString(" <- ")
      .take(512)

  private final case class ConsumerReceiptWindowRefused(refusal: ConsumerReceiptWindowRefusal)
      extends RuntimeException(s"consumer receipt window refused: $refusal")
      with NoStackTrace

  private final case class OutboxOperationFailed(reason: String)
      extends RuntimeException(s"outbox operation failed: $reason")
      with NoStackTrace

  private case object PublisherBreakerOpen
      extends RuntimeException("publisher circuit breaker is open")
      with NoStackTrace

  private final case class ProducerGenerationFenced(cause: Throwable)
      extends RuntimeException("transactional producer generation was fenced", cause)

  /** A send cancelled by a lost lease leaves its transaction open; only a new initialization of the transactional id
    * aborts it, so the generation ends.
    */
  private final case class WaveAbandoned(reason: String)
      extends RuntimeException(s"publication wave abandoned: $reason")
      with NoStackTrace

}
