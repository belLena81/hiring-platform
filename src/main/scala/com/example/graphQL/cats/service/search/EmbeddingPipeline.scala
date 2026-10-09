package com.example.graphQL.cats.service.search

import cats.effect.std.{Queue, UUIDGen}
import cats.effect.{Clock, IO, Resource}
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

enum EmbeddingWork {
  case JobChanged(id: JobId)
  case CandidateProfileChanged(id: UserId)
}

trait EmbeddingWorkPublisher {

  /** Signals work that was atomically persisted by the mutation transaction. */
  def wake: IO[Unit]
}

final class DurableEmbeddingWorkPublisher private[search] (
    repository: EmbeddingWorkRepository,
    wakeups: Queue[IO, Unit],
    clock: Clock[IO]
) extends EmbeddingWorkPublisher {
  private[search] def offer(work: EmbeddingWork): IO[Unit] =
    clock.realTimeInstant.flatMap(repository.enqueue(DurableEmbeddingWorkPublisher.keyFor(work), _).value).flatMap {
      case Right(())   => wake
      case Left(error) => IO.raiseError(new IllegalStateException(s"Embedding work enqueue failed: $error"))
    }

  override def wake: IO[Unit] = wakeups.tryOffer(()).void
}

object DurableEmbeddingWorkPublisher {
  def keyFor(work: EmbeddingWork): EmbeddingWorkKey = work match {
    case EmbeddingWork.JobChanged(id)              => EmbeddingWorkKey(EmbeddingWorkKind.Job, id.value.toString)
    case EmbeddingWork.CandidateProfileChanged(id) =>
      EmbeddingWorkKey(EmbeddingWorkKind.CandidateProfile, id.value.toString)
  }
}

final class EmbeddingPipeline(
    wakeups: Queue[IO, Unit],
    work: EmbeddingWorkRepository,
    users: UserRepository,
    jobs: JobRepository,
    embeddings: EmbeddingService,
    model: String,
    parallelism: Int,
    retryAttempts: Int,
    retryDelay: FiniteDuration,
    leaseDuration: FiniteDuration,
    clock: Clock[IO],
    workerId: String,
    diagnostics: Diagnostics,
    durableRetryAttempts: Int = 1,
    durableRetryBase: FiniteDuration = 1.second,
    durableRetryCap: FiniteDuration = 5.minutes,
    workerHealth: Boolean => IO[Unit] = _ => IO.unit
) {
  private val now: IO[Instant] = clock.realTimeInstant

  def stream: Stream[IO, Unit] =
    Stream
      .fromQueueUnterminated(wakeups)
      .merge(Stream.awakeEvery[IO](retryDelay).as(()))
      .parEvalMap(parallelism)(_ => drain)

  private def drain: IO[Unit] =
    claimNext.flatMap(_.fold(IO.unit)(claim => processClaim(claim) *> drain))

  private def claimNext: IO[Option[ClaimedEmbeddingWork]] =
    now.flatMap(instant => work.claim(workerId, instant, instant.plusMillis(leaseDuration.toMillis)).value).flatMap {
      case Right(claim) => workerHealth(true).as(claim)
      case Left(_)      => workerHealth(false) *> reportRepositoryFailure.as(None)
    }

  private def reportRepositoryFailure: IO[Unit] =
    diagnostics.emit(LogEvent.EmbeddingProcessingFailed)

  private enum ProcessingOutcome {
    case Completed, Retry, Conflict, Abandoned
    case Terminal(failure: EmbeddingWorkFailure)
  }

  private val retryPolicy: RetryPolicy[IO, ProcessingOutcome] =
    limitRetries[IO](math.max(retryAttempts - 1, 0)) join constantDelay[IO](retryDelay)

  private def processClaim(claim: ClaimedEmbeddingWork): IO[Unit] =
    retryingOnFailures(
      process(claim).handleErrorWith(error =>
        diagnostics
          .emit(LogEvent.EmbeddingProcessingFailed, fields = LogFields.failure(error))
          .as(ProcessingOutcome.Retry)
      )
    )(
      policy = retryPolicy,
      valueHandler = (outcome, _) =>
        IO.pure(outcome match {
          case ProcessingOutcome.Retry => HandlerDecision.Continue
          case _                       => HandlerDecision.Stop
        })
    ).race(leaseHeartbeat(claim)).flatMap { result =>
      result.fold(_.fold(identity, identity), _ => ProcessingOutcome.Abandoned) match {
        case ProcessingOutcome.Abandoned => IO.unit
        case ProcessingOutcome.Conflict  =>
          conflictOutcome(claim)
            .flatMap {
              case ProcessingOutcome.Completed         => work.complete(claim).value
              case ProcessingOutcome.Terminal(failure) => now.flatMap(work.fail(claim, failure, _).value)
              case _                                   =>
                // Revision fencing does not consume the provider failure budget.
                now.flatMap(at =>
                  work.retry(claim, at.plusMillis(durableRetryBase.toMillis), chargeAttempt = false).value
                )
            }
            .flatMap {
              case Right(_) => IO.unit
              case Left(_)  => reportRepositoryFailure
            }
        case ProcessingOutcome.Retry =>
          now
            .flatMap { at =>
              if (claim.attempts + 1 < durableRetryAttempts)
                work
                  .retry(
                    claim,
                    at.plusMillis(
                      EmbeddingRecoveryPolicy.backoffMillis(
                        claim.attempts.toLong + 1L,
                        durableRetryBase.toMillis,
                        durableRetryCap.toMillis
                      )
                    )
                  )
                  .value
              else work.fail(claim, EmbeddingWorkFailure.RetryExhausted, at).value
            }
            .flatMap {
              case Right(_) => IO.unit
              case Left(_)  => reportRepositoryFailure
            }
        case ProcessingOutcome.Completed =>
          work.complete(claim).value.flatMap {
            case Right(_) => IO.unit
            case Left(_)  => reportRepositoryFailure
          }
        case ProcessingOutcome.Terminal(failure) =>
          now.flatMap(work.fail(claim, failure, _).value).flatMap {
            case Right(_) => IO.unit
            case Left(_)  => reportRepositoryFailure
          }
      }
    }

  private def conflictOutcome(claim: ClaimedEmbeddingWork): IO[ProcessingOutcome] = {
    def preparedOutcome(source: Option[String], metadata: Option[EmbeddingMeta]): ProcessingOutcome =
      EmbeddingPreparation.prepare(source, metadata, model, SearchableText.DocumentMaxChars) match {
        case EmbeddingPreparation.NoWork           => ProcessingOutcome.Completed
        case EmbeddingPreparation.DocumentTooLarge => ProcessingOutcome.Terminal(EmbeddingWorkFailure.DocumentTooLarge)
        case EmbeddingPreparation.Prepared(_, _)   => ProcessingOutcome.Retry
      }

    val reread = claim.key.kind match {
      case EmbeddingWorkKind.Job =>
        parseIdentifier(claim.key.entityId)(JobId.apply).fold(
          _ => IO.pure(ProcessingOutcome.Terminal(EmbeddingWorkFailure.InvalidWorkKey)),
          id =>
            jobs.findVersioned(id).value.map {
              case Right(Some(observed)) =>
                preparedOutcome(Some(SearchableText.job(observed.value)), observed.value.embedding.map(_.meta))
              case Right(None) => ProcessingOutcome.Completed
              case Left(_)     => ProcessingOutcome.Retry
            }
        )
      case EmbeddingWorkKind.CandidateProfile =>
        parseIdentifier(claim.key.entityId)(UserId.apply).fold(
          _ => IO.pure(ProcessingOutcome.Terminal(EmbeddingWorkFailure.InvalidWorkKey)),
          id =>
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
    }
    reread.handleErrorWith(error =>
      diagnostics
        .emit(LogEvent.EmbeddingProcessingFailed, fields = LogFields.failure(error))
        .as(ProcessingOutcome.Retry)
    )
  }

  private def renewClaim(claim: ClaimedEmbeddingWork): IO[Boolean] =
    now.flatMap(at => work.renew(claim, at, at.plusMillis(leaseDuration.toMillis)).value).flatMap {
      case Right(renewed) => IO.pure(renewed)
      case Left(_)        => reportRepositoryFailure.as(false)
    }

  private def leaseHeartbeat(claim: ClaimedEmbeddingWork): IO[Unit] =
    Stream
      .awakeEvery[IO]((leaseDuration / 3).max(1.millis))
      .evalMap(_ => renewClaim(claim))
      .takeWhile(identity)
      .compile
      .drain

  private def process(claim: ClaimedEmbeddingWork): IO[ProcessingOutcome] =
    claim.key.kind match {
      case EmbeddingWorkKind.Job =>
        parseIdentifier(claim.key.entityId)(JobId.apply)
          .fold(
            _ => IO.pure(ProcessingOutcome.Terminal(EmbeddingWorkFailure.InvalidWorkKey)),
            processJob
          )
      case EmbeddingWorkKind.CandidateProfile =>
        parseIdentifier(claim.key.entityId)(UserId.apply)
          .fold(
            _ => IO.pure(ProcessingOutcome.Terminal(EmbeddingWorkFailure.InvalidWorkKey)),
            processCandidate
          )
    }

  private def processJob(id: JobId): IO[ProcessingOutcome] =
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
      case Right(None) => IO.pure(ProcessingOutcome.Completed)
      case Left(_)     => IO.pure(ProcessingOutcome.Retry)
    }

  private def processCandidate(id: UserId): IO[ProcessingOutcome] =
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
      case Right(Some(_)) => IO.pure(ProcessingOutcome.Completed)
      case Right(None)    => IO.pure(ProcessingOutcome.Completed)
      case Left(_)        => IO.pure(ProcessingOutcome.Retry)
    }

  private def attemptOutcome(result: Either[RepositoryError, ProcessingOutcome]): ProcessingOutcome = result match {
    case Right(outcome)                 => outcome
    case Left(RepositoryError.Conflict) => ProcessingOutcome.Conflict
    case Left(RepositoryError.AuthorityRevoked) | Left(RepositoryError.Unavailable) |
        Left(RepositoryError.DuplicateApplication) | Left(
          RepositoryError.InvalidStoredData
        ) | Left(RepositoryError.InvalidEvent) | Left(RepositoryError.MissingWriteResult) |
        Left(RepositoryError.MissingStoredResult) =>
      ProcessingOutcome.Retry
  }

  private def embedPrepared(prepared: EmbeddingPreparation)(
      persist: EntityEmbedding => RepositoryIO[Unit]
  ): RepositoryIO[ProcessingOutcome] = prepared match {
    case EmbeddingPreparation.NoWork           => RepositoryIO.fromEither(Right(ProcessingOutcome.Completed))
    case EmbeddingPreparation.DocumentTooLarge =>
      RepositoryIO.fromEither(Right(ProcessingOutcome.Terminal(EmbeddingWorkFailure.DocumentTooLarge)))
    case EmbeddingPreparation.Prepared(text, hash) =>
      RepositoryIO
        .lift(
          embeddings
            .embed(EmbeddingInput(text, EmbeddingInputType.Document))
            .map(_.flatMap(EmbeddingVector.validateModel(_, model)))
        )
        .flatMap {
          case Left(EmbeddingError.ProviderUnavailable) => RepositoryIO.fromEither(Right(ProcessingOutcome.Retry))
          case Left(EmbeddingError.InvalidResponse)     =>
            RepositoryIO.fromEither(Right(ProcessingOutcome.Terminal(EmbeddingWorkFailure.InvalidResponse)))
          case Right(vector) =>
            RepositoryIO.lift(now).flatMap { instant =>
              persist(EntityEmbedding(vector.values, EmbeddingMeta(model, hash, instant)))
                .as(ProcessingOutcome.Completed)
            }
        }
  }
}

object EmbeddingPipeline {
  private val WorkerName = "embedding-pipeline"

  def resource(
      work: EmbeddingWorkRepository,
      users: UserRepository,
      jobs: JobRepository,
      embeddings: EmbeddingService,
      model: String,
      queueSize: Int,
      parallelism: Int,
      retryAttempts: Int,
      retryDelay: FiniteDuration,
      leaseDuration: FiniteDuration,
      workerReady: IO[Boolean] = IO.pure(true),
      diagnostics: Diagnostics,
      durableRetryAttempts: Int = 8,
      durableRetryBase: FiniteDuration = 1.second,
      durableRetryCap: FiniteDuration = 5.minutes,
      workerRestartDelay: FiniteDuration = 1.second,
      workerHealth: Boolean => IO[Unit] = _ => IO.unit,
      clock: Clock[IO] = Clock[IO],
      uuidGen: UUIDGen[IO] = UUIDGen[IO]
  ): Resource[IO, DurableEmbeddingWorkPublisher] =
    Resource.eval(Queue.bounded[IO, Unit](queueSize)).flatMap { wakeups =>
      for {
        workerId <- Resource.eval(uuidGen.randomUUID.map(_.toString))
        publisher = new DurableEmbeddingWorkPublisher(work, wakeups, clock)
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
          durableRetryAttempts,
          durableRetryBase,
          durableRetryCap,
          workerHealth
        )
        _ <- BackgroundWorker.resource(WorkerName, diagnostics)(
          workerReady.flatMap { ready =>
            if (!ready) workerHealth(false)
            else
              (workerHealth(true) *> pipeline.stream.compile.drain)
                .guarantee(workerHealth(false))
                .handleErrorWith(error =>
                  diagnostics.emit(LogEvent.EmbeddingProcessingFailed, fields = LogFields.failure(error))
                )
                .flatMap(_ => IO.sleep(workerRestartDelay))
                .foreverM
          }
        )
      } yield publisher
    }
}
