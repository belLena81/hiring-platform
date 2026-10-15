package com.example.graphQL.cats.service

import cats.{Applicative, Monad, MonadThrow}
import cats.effect.{Clock, Ref}
import cats.syntax.all.*
import com.example.graphQL.cats.service.Diagnostics.*
import java.time.Instant
import scala.concurrent.duration.FiniteDuration

/** A readiness component reported by its stable wire name only; it carries no counts, identifiers or error text. */
enum ReadinessComponent(val wire: String) {
  case KafkaPublisher extends ReadinessComponent("kafka-publisher")
  case KafkaConsumer extends ReadinessComponent("kafka-consumer")
  case InterviewPublication extends ReadinessComponent("interview-publication")
  case InterviewOrchestratorConsumer extends ReadinessComponent("interview-orchestrator-consumer")
  case InterviewWorkerConsumer extends ReadinessComponent("interview-worker-consumer")
  case EmbeddingWorker extends ReadinessComponent("embedding-worker")
  case SearchSessionHandoff extends ReadinessComponent("search-session-handoff")
  case InterviewSubjectCleanup extends ReadinessComponent("interview-subject-cleanup")
  case RecruiterJobClosure extends ReadinessComponent("recruiter-job-closure")
  case OutboxAge extends ReadinessComponent("outbox-age")
  case ClosureWorkAge extends ReadinessComponent("closure-work-age")
}

/** Process-local record of the last successful progress of each long-running worker. A worker is registered when it
  * starts, which begins its staleness grace period; a worker that was never registered (disabled by configuration) is
  * never reported. Time is monotonic, so wall-clock steps cannot make a worker look stale or fresh.
  */
final class WorkerHeartbeats[F[_]: Monad: Clock] private (
    lastProgress: Ref[F, Map[ReadinessComponent, FiniteDuration]]
) {
  def register(component: ReadinessComponent): F[Unit] = beat(component)

  /** The recorder handed to a worker: call it after each successful pass. */
  def beat(component: ReadinessComponent): F[Unit] =
    Clock[F].monotonic.flatMap(now => lastProgress.update(_.updated(component, now)))

  /** Registers the component now and returns its recorder. */
  def alive(component: ReadinessComponent, every: FiniteDuration): F[WorkerAlive[F]] =
    register(component).as(WorkerAlive(beat(component), every))

  /** Registered workers whose last progress is older than `maxAge`, in declaration order. */
  def stale(maxAge: FiniteDuration): F[List[ReadinessComponent]] =
    (Clock[F].monotonic, lastProgress.get).mapN((now, seen) =>
      ReadinessComponent.values.toList.filter(component => seen.get(component).exists(last => now - last > maxAge))
    )
}

object WorkerHeartbeats {
  def create[F[_]: Monad: Clock: Ref.Make]: F[WorkerHeartbeats[F]] =
    Ref.of[F, Map[ReadinessComponent, FiniteDuration]](Map.empty).map(new WorkerHeartbeats[F](_))
}

/** What a worker is given to report progress: `beat` after each successful pass, and `every` as the interval for a
  * worker that has no natural pass of its own (an idle Kafka consumer) to keep reporting while its stream runs.
  */
final case class WorkerAlive[F[_]](beat: F[Unit], every: FiniteDuration)

object WorkerAlive {
  def none[F[_]: Applicative]: WorkerAlive[F] = WorkerAlive(Applicative[F].unit, FiniteDuration(1, "second"))
}

/** Age of the oldest durable work item that is waiting; `None` when nothing is waiting. */
trait PendingWorkAge[F[_]] {
  def oldest(now: Instant): F[Option[FiniteDuration]]
}

final case class ReadinessThresholds(
    workerStaleness: FiniteDuration,
    maxOutboxAge: FiniteDuration,
    maxClosureWorkAge: FiniteDuration
)

/** Collects degraded components. They never decide the readiness status code; an observation that fails is reported as
  * its component being degraded rather than failing the probe. A change of the degraded set is logged once, not on
  * every probe.
  */
final class DegradedComponents[F[_]: MonadThrow: Clock] private (
    heartbeats: WorkerHeartbeats[F],
    thresholds: ReadinessThresholds,
    embeddingHealthy: F[Boolean],
    outboxAge: PendingWorkAge[F],
    closureWorkAge: PendingWorkAge[F],
    diagnostics: Diagnostics[F],
    previous: Ref[F, Set[ReadinessComponent]]
) {
  private def tooOld(
      component: ReadinessComponent,
      source: PendingWorkAge[F],
      limit: FiniteDuration
  ): F[List[ReadinessComponent]] =
    Clock[F].realTimeInstant
      .flatMap(source.oldest)
      .map(age => if (age.exists(_ > limit)) List(component) else Nil)
      .handleError(_ => List(component))

  def current: F[List[ReadinessComponent]] =
    for {
      workers <- heartbeats.stale(thresholds.workerStaleness)
      embedding <- embeddingHealthy
        .map(healthy => if (healthy) Nil else List(ReadinessComponent.EmbeddingWorker))
        .handleError(_ => List(ReadinessComponent.EmbeddingWorker))
      outbox <- tooOld(ReadinessComponent.OutboxAge, outboxAge, thresholds.maxOutboxAge)
      closure <- tooOld(ReadinessComponent.ClosureWorkAge, closureWorkAge, thresholds.maxClosureWorkAge)
      reported = (workers ++ embedding ++ outbox ++ closure).toSet
      degraded = ReadinessComponent.values.toList.filter(reported)
      before <- previous.getAndSet(reported)
      _ <- degraded
        .filterNot(before)
        .traverse_(component =>
          diagnostics.emit(LogEvent.ReadinessDegraded, fields = Map(LogField.Worker -> component.wire))
        )
    } yield degraded
}

object DegradedComponents {
  def create[F[_]: MonadThrow: Clock: Ref.Make](
      heartbeats: WorkerHeartbeats[F],
      thresholds: ReadinessThresholds,
      embeddingHealthy: F[Boolean],
      outboxAge: PendingWorkAge[F],
      closureWorkAge: PendingWorkAge[F],
      diagnostics: Diagnostics[F]
  ): F[DegradedComponents[F]] =
    Ref
      .of[F, Set[ReadinessComponent]](Set.empty)
      .map(
        new DegradedComponents[F](heartbeats, thresholds, embeddingHealthy, outboxAge, closureWorkAge, diagnostics, _)
      )
}
