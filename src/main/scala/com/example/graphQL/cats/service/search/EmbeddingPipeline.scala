package com.example.graphQL.cats.service.search

import com.example.graphQL.cats.domain.policy.RetryBackoff

import cats.effect.std.{Queue, UUIDGen}
import cats.{Applicative, Functor, MonadThrow}
import cats.effect.{Clock, Resource, Temporal}
import cats.effect.implicits.*
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId, parse as parseIdentifier}
import com.example.graphQL.cats.domain.model.{EmbeddingMeta, EntityEmbedding, SearchableText}
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.{BackgroundWorker, Diagnostics, LogEvent, LogFields}
import com.example.graphQL.cats.service.Diagnostics.*
import fs2.Stream
import java.time.Instant
import retry.*
import retry.RetryPolicies.*
import scala.concurrent.duration.*

trait EmbeddingWorkPublisher[F[_]] {

  /** Signals work that was atomically persisted by the mutation transaction. */
  def wake: F[Unit]

  /** A failed wake never fails the committed mutation; the durable work is picked up by the next poll. */
  final def wakeAfterCommit(diagnostics: Diagnostics[F])(using F: MonadThrow[F]): F[Unit] =
    wake.handleErrorWith(error => diagnostics.emit(LogEvent.EmbeddingWakeFailed, fields = LogFields.failure(error)))
}

final class DurableEmbeddingWorkPublisher[F[_]: Functor] private[search] (wakeups: Queue[F, Unit])
    extends EmbeddingWorkPublisher[F] {
  override def wake: F[Unit] = wakeups.tryOffer(()).void
}

/** Bounded durable retries of failed embedding work: attempts before failure, then capped exponential backoff. */
final case class DurableRetrySettings(
    attempts: Int = 1,
    base: FiniteDuration = 1.second,
    cap: FiniteDuration = 5.minutes
)

final class EmbeddingPipeline[F[_]](using F: Temporal[F])(
    wakeups: Queue[F, Unit],
    work: EmbeddingWorkRepository[F],
    users: UserRepository[F],
    jobs: JobRepository[F],
    embeddings: EmbeddingService[F],
    model: String,
    parallelism: Int,
    retryAttempts: Int,
    retryDelay: FiniteDuration,
    leaseDuration: FiniteDuration,
    clock: Clock[F],
    workerId: String,
    diagnostics: Diagnostics[F],
    durableRetry: DurableRetrySettings = new DurableRetrySettings(),
    workerHealth: Boolean => F[Unit] = _ => F.unit
) {
  private val now: F[Instant] = clock.realTimeInstant

  def stream: Stream[F, Unit] =
    Stream
      .fromQueueUnterminated(wakeups)
      .merge(Stream.awakeEvery[F](retryDelay).as(()))
      .parEvalMap(parallelism)(_ => drain)

  private def drain: F[Unit] =
    claimNext.flatMap(_.fold(Applicative[F].unit)(claim => processClaim(claim) *> drain))

  private def claimNext: F[Option[ClaimedEmbeddingWork]] =
    now.flatMap(instant => work.claim(workerId, instant, instant.plusMillis(leaseDuration.toMillis)).value).flatMap {
      case Right(claim) => workerHealth(true).as(claim)
      case Left(_)      => workerHealth(false) *> reportRepositoryFailure.as(None)
    }

  private def reportRepositoryFailure: F[Unit] =
    diagnostics.emit(LogEvent.EmbeddingProcessingFailed)

  /** Runs a repository step, reporting (not raising) its typed failure. */
  private def orReport[A](step: RepositoryF[F, A]): F[Unit] =
    step.leftSemiflatMap(_ => reportRepositoryFailure).value.void

  private enum ProcessingOutcome {
    case Completed, Retry, Conflict, Abandoned

    /** The provider breaker is open: retry later without consuming the in-memory or durable attempt budget. */
    case Deferred
    case Terminal(failure: EmbeddingWorkFailure)
  }

  private val retryPolicy: RetryPolicy[F, ProcessingOutcome] =
    limitRetries[F](math.max(retryAttempts - 1, 0)) join constantDelay[F](retryDelay)

  private def processClaim(claim: ClaimedEmbeddingWork): F[Unit] =
    Temporal[F]
      .race(
        retryingOnFailures(
          process(claim).handleErrorWith(error =>
            diagnostics
              .emit(LogEvent.EmbeddingProcessingFailed, fields = LogFields.failure(error))
              .as(ProcessingOutcome.Retry)
          )
        )(
          policy = retryPolicy,
          valueHandler = (outcome, _) =>
            Applicative[F].pure(outcome match {
              case ProcessingOutcome.Retry => HandlerDecision.Continue
              case _                       => HandlerDecision.Stop
            })
        ),
        leaseHeartbeat(claim)
      )
      .flatMap { result =>
        result.fold(_.fold(identity, identity), _ => ProcessingOutcome.Abandoned) match {
          case ProcessingOutcome.Abandoned => Applicative[F].unit
          case ProcessingOutcome.Conflict  =>
            orReport(
              RepositoryF.lift(conflictOutcome(claim)).flatMap {
                case ProcessingOutcome.Completed         => work.complete(claim)
                case ProcessingOutcome.Terminal(failure) => RepositoryF.lift(now).flatMap(work.fail(claim, failure, _))
                // Revision fencing does not consume the provider failure budget.
                case _ =>
                  RepositoryF
                    .lift(now)
                    .flatMap(at => work.retry(claim, at.plusMillis(durableRetry.base.toMillis), chargeAttempt = false))
              }
            )
          case ProcessingOutcome.Deferred =>
            // An open provider breaker never reached the provider, so no attempt is charged.
            orReport(
              RepositoryF
                .lift(now)
                .flatMap(at => work.retry(claim, at.plusMillis(durableRetry.base.toMillis), chargeAttempt = false))
            )
          case ProcessingOutcome.Retry =>
            orReport(
              RepositoryF.lift(now).flatMap { at =>
                if (claim.attempts + 1 < durableRetry.attempts)
                  work.retry(
                    claim,
                    at.plusMillis(
                      RetryBackoff.exponentialMillis(
                        claim.attempts.toLong + 1L,
                        durableRetry.base.toMillis,
                        durableRetry.cap.toMillis
                      )
                    )
                  )
                else work.fail(claim, EmbeddingWorkFailure.RetryExhausted, at)
              }
            )
          case ProcessingOutcome.Completed         => orReport(work.complete(claim))
          case ProcessingOutcome.Terminal(failure) =>
            orReport(RepositoryF.lift(now).flatMap(work.fail(claim, failure, _)))
        }
      }

  private def invalidWorkKey: F[ProcessingOutcome] =
    Applicative[F].pure(ProcessingOutcome.Terminal(EmbeddingWorkFailure.InvalidWorkKey))

  private def conflictOutcome(claim: ClaimedEmbeddingWork): F[ProcessingOutcome] = {
    def preparedOutcome(source: Option[String], metadata: Option[EmbeddingMeta]): ProcessingOutcome =
      EmbeddingPreparation.prepare(source, metadata, model, SearchableText.DocumentMaxChars) match {
        case EmbeddingPreparation.NoWork           => ProcessingOutcome.Completed
        case EmbeddingPreparation.DocumentTooLarge => ProcessingOutcome.Terminal(EmbeddingWorkFailure.DocumentTooLarge)
        case EmbeddingPreparation.Prepared(_, _)   => ProcessingOutcome.Retry
      }

    val reread = onKey(claim)(id =>
      jobs.findVersioned(id).value.map {
        case Right(Some(observed)) =>
          preparedOutcome(Some(SearchableText.job(observed.value)), observed.value.embedding.map(_.meta))
        case Right(None) => ProcessingOutcome.Completed
        case Left(_)     => ProcessingOutcome.Retry
      }
    )(id =>
      users.findVersioned(id).value.map {
        case Right(Some(observed))
            if observed.value.accountStatus == com.example.graphQL.cats.domain.model.AccountStatus.Active &&
              observed.value.role == com.example.graphQL.cats.domain.model.UserRole.Candidate =>
          preparedOutcome(
            observed.value.candidateProfile.map(SearchableText.candidate),
            observed.value.embedding.map(_.meta)
          )
        case Right(_) => ProcessingOutcome.Completed
        case Left(_)  => ProcessingOutcome.Retry
      }
    )
    reread.handleErrorWith(error =>
      diagnostics
        .emit(LogEvent.EmbeddingProcessingFailed, fields = LogFields.failure(error))
        .as(ProcessingOutcome.Retry)
    )
  }

  private def renewClaim(claim: ClaimedEmbeddingWork): F[Boolean] =
    now.flatMap(at => work.renew(claim, at, at.plusMillis(leaseDuration.toMillis)).value).flatMap {
      case Right(renewed) => Applicative[F].pure(renewed)
      case Left(_)        => reportRepositoryFailure.as(false)
    }

  private def leaseHeartbeat(claim: ClaimedEmbeddingWork): F[Unit] =
    Stream
      .awakeEvery[F]((leaseDuration / 3).max(1.millis))
      .evalMap(_ => renewClaim(claim))
      .takeWhile(identity)
      .compile
      .drain

  private def process(claim: ClaimedEmbeddingWork): F[ProcessingOutcome] =
    onKey(claim)(processJob)(processCandidate)

  /** Runs the step for the claimed entity kind; a key that is not a valid identifier is terminal. */
  private def onKey(claim: ClaimedEmbeddingWork)(job: JobId => F[ProcessingOutcome])(
      candidate: UserId => F[ProcessingOutcome]
  ): F[ProcessingOutcome] =
    claim.key.kind match {
      case EmbeddingWorkKind.Job => parseIdentifier(claim.key.entityId)(JobId.apply).fold(invalidWorkKey)(job)
      case EmbeddingWorkKind.CandidateProfile =>
        parseIdentifier(claim.key.entityId)(UserId.apply).fold(invalidWorkKey)(candidate)
    }

  private def processJob(id: JobId): F[ProcessingOutcome] =
    jobs.findVersioned(id).value.flatMap {
      case Right(Some(observed)) =>
        val job = observed.value
        val prepared = EmbeddingPreparation.prepare(
          Some(SearchableText.job(job)),
          job.embedding.map(_.meta),
          model,
          SearchableText.DocumentMaxChars
        )
        embedPrepared(prepared)(embedding => jobs.updateEmbedding(observed, embedding)).value.map(attemptOutcome)
      case Right(None) => Applicative[F].pure(ProcessingOutcome.Completed)
      case Left(_)     => Applicative[F].pure(ProcessingOutcome.Retry)
    }

  private def processCandidate(id: UserId): F[ProcessingOutcome] =
    users.findVersioned(id).value.flatMap {
      case Right(Some(observed))
          if observed.value.accountStatus == com.example.graphQL.cats.domain.model.AccountStatus.Active &&
            observed.value.role == com.example.graphQL.cats.domain.model.UserRole.Candidate =>
        val user = observed.value
        val prepared = EmbeddingPreparation.prepare(
          user.candidateProfile.map(SearchableText.candidate),
          user.embedding.map(_.meta),
          model,
          SearchableText.DocumentMaxChars
        )
        embedPrepared(prepared)(embedding => users.updateEmbedding(observed, embedding)).value.map(attemptOutcome)
      case Right(Some(_)) => Applicative[F].pure(ProcessingOutcome.Completed)
      case Right(None)    => Applicative[F].pure(ProcessingOutcome.Completed)
      case Left(_)        => Applicative[F].pure(ProcessingOutcome.Retry)
    }

  private def attemptOutcome(result: Either[RepositoryError, ProcessingOutcome]): ProcessingOutcome = result match {
    case Right(outcome)                 => outcome
    case Left(RepositoryError.Conflict) => ProcessingOutcome.Conflict
    case Left(_)                        => ProcessingOutcome.Retry
  }

  private def embedPrepared(prepared: EmbeddingPreparation)(
      persist: EntityEmbedding => RepositoryF[F, Unit]
  ): RepositoryF[F, ProcessingOutcome] = prepared match {
    case EmbeddingPreparation.NoWork           => RepositoryF.pure[F, ProcessingOutcome](ProcessingOutcome.Completed)
    case EmbeddingPreparation.DocumentTooLarge =>
      RepositoryF.pure[F, ProcessingOutcome](ProcessingOutcome.Terminal(EmbeddingWorkFailure.DocumentTooLarge))
    case EmbeddingPreparation.Prepared(text, hash) =>
      RepositoryF
        .lift(
          embeddings
            .embed(EmbeddingInput(text, EmbeddingInputType.Document))
            .map(_.flatMap(EmbeddingVector.validateModel(_, model)))
        )
        .flatMap {
          case Left(EmbeddingError.ProviderUnavailable) =>
            RepositoryF.pure[F, ProcessingOutcome](ProcessingOutcome.Retry)
          case Left(EmbeddingError.CircuitOpen) => RepositoryF.pure[F, ProcessingOutcome](ProcessingOutcome.Deferred)
          case Left(EmbeddingError.InvalidResponse) =>
            RepositoryF.pure[F, ProcessingOutcome](ProcessingOutcome.Terminal(EmbeddingWorkFailure.InvalidResponse))
          case Right(vector) =>
            RepositoryF.lift(now).flatMap { instant =>
              persist(EntityEmbedding(vector.values, EmbeddingMeta(model, hash, instant)))
                .as(ProcessingOutcome.Completed)
            }
        }
  }
}

object EmbeddingPipeline {
  private val WorkerName = "embedding-pipeline"

  def resource[F[_]](using
      F: Temporal[F],
      uuids: UUIDGen[F]
  )(
      work: EmbeddingWorkRepository[F],
      users: UserRepository[F],
      jobs: JobRepository[F],
      embeddings: EmbeddingService[F],
      model: String,
      queueSize: Int,
      parallelism: Int,
      retryAttempts: Int,
      retryDelay: FiniteDuration,
      leaseDuration: FiniteDuration,
      diagnostics: Diagnostics[F],
      durableRetry: DurableRetrySettings = new DurableRetrySettings(attempts = 8),
      workerRestartDelay: FiniteDuration = 1.second,
      workerHealth: Boolean => F[Unit] = _ => F.unit,
      clock: Clock[F] = F
  ): Resource[F, DurableEmbeddingWorkPublisher[F]] =
    Resource.eval(Queue.bounded[F, Unit](queueSize)).flatMap { wakeups =>
      for {
        workerId <- Resource.eval(uuids.randomUUID.map(_.toString))
        publisher = new DurableEmbeddingWorkPublisher(wakeups)
        pipeline = new EmbeddingPipeline(
          wakeups,
          work,
          users,
          jobs,
          embeddings,
          model,
          parallelism,
          retryAttempts,
          retryDelay,
          leaseDuration,
          clock,
          workerId,
          diagnostics,
          durableRetry,
          workerHealth
        )
        _ <- BackgroundWorker.resource(WorkerName, diagnostics)(
          (workerHealth(true) *> pipeline.stream.compile.drain)
            .guarantee(workerHealth(false))
            .handleErrorWith(error =>
              diagnostics.emit(LogEvent.EmbeddingProcessingFailed, fields = LogFields.failure(error))
            )
            .flatMap(_ => Temporal[F].sleep(workerRestartDelay))
            .foreverM
        )
      } yield publisher
    }
}
