package com.example.graphQL.cats.service.search

import com.example.graphQL.cats.domain.policy.RetryBackoff

import cats.effect.std.{Queue, UUIDGen}
import cats.{Applicative, Functor, MonadThrow}
import cats.effect.{Clock, Ref, Resource, Temporal}
import cats.effect.implicits.*
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId, parse as parseIdentifier}
import com.example.graphQL.cats.domain.model.{
  AccountStatus,
  EmbeddingMeta,
  EntityEmbedding,
  SearchableText,
  User,
  UserRole
}
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.resilience.{LeaseHeartbeat, LeaseLost}
import com.example.graphQL.cats.service.{BackgroundWorker, Diagnostics, LogEvent, LogField, LogFields}
import com.example.graphQL.cats.service.Diagnostics.*
import com.example.graphQL.cats.service.RepositoryReporting.orReport
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
    now.flatMap(instant =>
      work
        .claim(workerId, instant, leaseDuration)
        .foldF(
          error => workerHealth(false) *> reportRepositoryFailure(error).as(None),
          claim => workerHealth(true).as(claim)
        )
    )

  private def reportRepositoryFailure(error: RepositoryError): F[Unit] =
    diagnostics.emit(LogEvent.EmbeddingProcessingFailed, fields = LogFields.repositoryFailure(error))

  /** Runs a repository step, reporting (not raising) its typed failure. */
  private def orReport[A](step: RepositoryF[F, A]): F[Unit] =
    step.void.orReport(diagnostics, LogEvent.EmbeddingProcessingFailed)(())

  /** A failed read retries the work; the cause is reported, not dropped. */
  private def retryAfter(error: RepositoryError): F[ProcessingOutcome] =
    reportRepositoryFailure(error).as(ProcessingOutcome.Retry)

  private enum ProcessingOutcome {
    case Completed, Retry, Conflict, Abandoned

    /** The provider breaker is open: retry later without consuming the in-memory or durable attempt budget. */
    case Deferred
    case Terminal(failure: EmbeddingWorkFailure)
  }

  private val retryPolicy: RetryPolicy[F, ProcessingOutcome] =
    limitRetries[F](math.max(retryAttempts - 1, 0)) join constantDelay[F](retryDelay)

  /** A vector the provider already answered within one claim. It is keyed by model and source hash, lives only as long
    * the claim's processing, and is never shared with another claim or entity.
    */
  private final case class HeldVector(model: String, sourceHash: String, values: List[Float])

  private type Held = Ref[F, Option[HeldVector]]

  /** In-claim re-reads after a lost revision that may still reuse the held vector; the in-lease retry budget. */
  private val conflictRereads: Int = math.max(retryAttempts - 1, 0)

  private def processClaim(claim: ClaimedEmbeddingWork): F[Unit] =
    diagnostics
      .emit(LogEvent.EmbeddingLeaseReclaimed, fields = Map(LogField.Count -> claim.attempts.toString))
      .whenA(claim.reclaimedLease) *>
      Ref.of[F, Option[HeldVector]](None).flatMap(held => processWithHeld(claim, held))

  private def processWithHeld(claim: ClaimedEmbeddingWork, held: Held): F[Unit] =
    LeaseHeartbeat
      .guard(leaseDuration, renewFirst = false)(renewClaim(claim))(
        retryingOnFailures(
          process(claim, held).handleErrorWith(error =>
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
        )
      )
      .flatMap { result =>
        result.fold(_ => ProcessingOutcome.Abandoned, _.fold(identity, identity)) match {
          case ProcessingOutcome.Abandoned => Applicative[F].unit
          case ProcessingOutcome.Conflict  =>
            orReport(
              RepositoryF.lift(conflictOutcome(claim, held, conflictRereads)).flatMap {
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

  /** Re-reads the entity after a lost revision. When it still needs the vector this claim already holds, that vector is
    * persisted against the fresh revision within the claim, up to `remaining` more re-reads; otherwise the work is
    * retried by a later claim.
    */
  private def conflictOutcome(claim: ClaimedEmbeddingWork, held: Held, remaining: Int): F[ProcessingOutcome] = {
    def preparedOutcome(source: Option[String], metadata: Option[EmbeddingMeta])(
        persist: EntityEmbedding => RepositoryF[F, Unit]
    ): F[ProcessingOutcome] =
      EmbeddingPreparation.prepare(source, metadata, model, SearchableText.DocumentMaxChars) match {
        case EmbeddingPreparation.NoWork           => ProcessingOutcome.Completed.pure[F]
        case EmbeddingPreparation.DocumentTooLarge =>
          ProcessingOutcome.Terminal(EmbeddingWorkFailure.DocumentTooLarge).pure[F]
        case prepared @ EmbeddingPreparation.Prepared(_, hash) =>
          held.get.flatMap {
            case Some(vector) if vector.model == model && vector.sourceHash == hash =>
              embedPrepared(prepared, held)(persist).foldF(
                {
                  case RepositoryError.Conflict if remaining > 0 =>
                    F.sleep(retryDelay) *> conflictOutcome(claim, held, remaining - 1)
                  case RepositoryError.Conflict => ProcessingOutcome.Retry.pure[F]
                  case other                    => retryAfter(other)
                },
                _.pure[F]
              )
            case _ => ProcessingOutcome.Retry.pure[F]
          }
      }

    val reread = onKey(claim)(id =>
      jobs
        .findVersioned(id)
        .foldF(
          retryAfter,
          {
            case Some(observed) =>
              preparedOutcome(Some(SearchableText.job(observed.value)), observed.value.embedding.map(_.meta))(
                embedding => jobs.updateEmbedding(observed, embedding)
              )
            case None => ProcessingOutcome.Completed.pure[F]
          }
        )
    )(id =>
      users
        .findVersioned(id)
        .foldF(
          retryAfter,
          {
            case Some(observed) if isActiveCandidate(observed.value) =>
              preparedOutcome(
                observed.value.candidateProfile.map(SearchableText.candidate),
                observed.value.embedding.map(_.meta)
              )(embedding => users.updateEmbedding(observed, embedding))
            case _ => ProcessingOutcome.Completed.pure[F]
          }
        )
    )
    reread.handleErrorWith(error =>
      diagnostics
        .emit(LogEvent.EmbeddingProcessingFailed, fields = LogFields.failure(error))
        .as(ProcessingOutcome.Retry)
    )
  }

  /** A reported failure to renew is an unknown lease, handled as a lost one: the work is abandoned to lease recovery.
    */
  private def renewClaim(claim: ClaimedEmbeddingWork): F[Either[LeaseLost, Unit]] =
    work
      .renew(claim, leaseDuration)
      .orReport(diagnostics, LogEvent.EmbeddingProcessingFailed)(false)
      .map(held => Either.cond(held, (), LeaseLost.Expired))

  private def process(claim: ClaimedEmbeddingWork, held: Held): F[ProcessingOutcome] =
    onKey(claim)(processJob(_, held))(processCandidate(_, held))

  /** Runs the step for the claimed entity kind; a key that is not a valid identifier is terminal. */
  private def onKey(claim: ClaimedEmbeddingWork)(job: JobId => F[ProcessingOutcome])(
      candidate: UserId => F[ProcessingOutcome]
  ): F[ProcessingOutcome] =
    claim.key.kind match {
      case EmbeddingWorkKind.Job => parseIdentifier(claim.key.entityId)(JobId.apply).fold(invalidWorkKey)(job)
      case EmbeddingWorkKind.CandidateProfile =>
        parseIdentifier(claim.key.entityId)(UserId.apply).fold(invalidWorkKey)(candidate)
    }

  private def processJob(id: JobId, held: Held): F[ProcessingOutcome] =
    jobs
      .findVersioned(id)
      .foldF(
        retryAfter,
        {
          case Some(observed) =>
            val job = observed.value
            val prepared = EmbeddingPreparation.prepare(
              Some(SearchableText.job(job)),
              job.embedding.map(_.meta),
              model,
              SearchableText.DocumentMaxChars
            )
            embedPrepared(prepared, held)(embedding => jobs.updateEmbedding(observed, embedding)).foldF(
              attemptFailure,
              _.pure[F]
            )
          case None => ProcessingOutcome.Completed.pure[F]
        }
      )

  private def processCandidate(id: UserId, held: Held): F[ProcessingOutcome] =
    users
      .findVersioned(id)
      .foldF(
        retryAfter,
        {
          case Some(observed) if isActiveCandidate(observed.value) =>
            val user = observed.value
            val prepared = EmbeddingPreparation.prepare(
              user.candidateProfile.map(SearchableText.candidate),
              user.embedding.map(_.meta),
              model,
              SearchableText.DocumentMaxChars
            )
            embedPrepared(prepared, held)(embedding => users.updateEmbedding(observed, embedding)).foldF(
              attemptFailure,
              _.pure[F]
            )
          case _ => ProcessingOutcome.Completed.pure[F]
        }
      )

  private def isActiveCandidate(user: User): Boolean =
    user.accountStatus == AccountStatus.Active && user.role == UserRole.Candidate

  /** A guarded write that lost its revision is a conflict; any other failure retries and is reported. */
  private def attemptFailure(error: RepositoryError): F[ProcessingOutcome] =
    error match {
      case RepositoryError.Conflict => ProcessingOutcome.Conflict.pure[F]
      case other                    => retryAfter(other)
    }

  private def embedPrepared(prepared: EmbeddingPreparation, held: Held)(
      persist: EntityEmbedding => RepositoryF[F, Unit]
  ): RepositoryF[F, ProcessingOutcome] = prepared match {
    case EmbeddingPreparation.NoWork           => RepositoryF.pure[F, ProcessingOutcome](ProcessingOutcome.Completed)
    case EmbeddingPreparation.DocumentTooLarge =>
      RepositoryF.pure[F, ProcessingOutcome](ProcessingOutcome.Terminal(EmbeddingWorkFailure.DocumentTooLarge))
    case EmbeddingPreparation.Prepared(text, hash) =>
      def persistVector(values: List[Float]): RepositoryF[F, ProcessingOutcome] =
        RepositoryF.lift(now).flatMap { instant =>
          persist(EntityEmbedding(values, EmbeddingMeta(model, hash, instant))).as(ProcessingOutcome.Completed)
        }
      RepositoryF.lift(held.get).flatMap {
        // The provider already answered for exactly this model and source: pay once, persist as often as needed.
        case Some(vector) if vector.model == model && vector.sourceHash == hash => persistVector(vector.values)
        case _                                                                  =>
          RepositoryF
            .lift(
              embeddings
                .embed(EmbeddingInput(text, EmbeddingInputType.Document))
                .map(_.flatMap(EmbeddingVector.validateModel(_, model)))
            )
            .flatMap {
              case Left(EmbeddingError.ProviderUnavailable) =>
                RepositoryF.pure[F, ProcessingOutcome](ProcessingOutcome.Retry)
              case Left(EmbeddingError.CircuitOpen) =>
                RepositoryF.pure[F, ProcessingOutcome](ProcessingOutcome.Deferred)
              case Left(EmbeddingError.InvalidResponse) =>
                RepositoryF.pure[F, ProcessingOutcome](ProcessingOutcome.Terminal(EmbeddingWorkFailure.InvalidResponse))
              case Right(vector) =>
                RepositoryF.lift(held.set(Some(HeldVector(model, hash, vector.values)))) *> persistVector(vector.values)
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
