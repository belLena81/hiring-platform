package com.example.graphQL.cats.service.search

import cats.effect.std.Queue
import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId, parse as parseIdentifier}
import com.example.graphQL.cats.domain.model.{EmbeddingMeta, EntityEmbedding, SearchableText}
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogFields}
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
    now: IO[Instant]
) extends EmbeddingWorkPublisher {
  private[search] def offer(work: EmbeddingWork): IO[Unit] =
    now.flatMap(repository.enqueue(DurableEmbeddingWorkPublisher.keyFor(work), _).value).flatMap {
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
    now: IO[Instant],
    workerId: String,
    diagnostics: Diagnostics
) {
  def stream: Stream[IO, Unit] =
    Stream
      .fromQueueUnterminated(wakeups)
      .merge(Stream.awakeEvery[IO](retryDelay).as(()))
      .parEvalMap(parallelism)(_ => drain)

  private def drain: IO[Unit] =
    claimNext.flatMap(_.fold(IO.unit)(claim => processClaim(claim) *> drain))

  private def claimNext: IO[Option[ClaimedEmbeddingWork]] =
    now.flatMap(instant => work.claim(workerId, instant, instant.plusMillis(leaseDuration.toMillis)).value).flatMap {
      case Right(claim) => IO.pure(claim)
      case Left(_)      => IO.pure(None)
    }

  private enum ProcessingOutcome {
    case Completed, Retry
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
    ).flatMap { result =>
      result.fold(identity, identity) match {
        case ProcessingOutcome.Retry =>
          now.flatMap(work.fail(claim, EmbeddingWorkFailure.RetryExhausted, _).value).void
        case ProcessingOutcome.Completed =>
          work.complete(claim).value.void
        case ProcessingOutcome.Terminal(failure) =>
          now.flatMap(work.fail(claim, failure, _).value).void
      }
    }

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
      case Right(Some(observed)) =>
        val user = observed.value
        val prepared = EmbeddingPreparation.prepare(
          user.candidateProfile.map(SearchableText.candidate),
          user.embedding.map(_.meta),
          model,
          SearchableText.DocumentMaxChars
        )
        embedPrepared(prepared)(embedding => users.updateEmbedding(observed, embedding)).value.map(attemptOutcome)
      case Right(None) => IO.pure(ProcessingOutcome.Completed)
      case Left(_)     => IO.pure(ProcessingOutcome.Retry)
    }

  private def attemptOutcome(result: Either[RepositoryError, ProcessingOutcome]): ProcessingOutcome = result match {
    case Right(outcome)                 => outcome
    case Left(RepositoryError.Conflict) => ProcessingOutcome.Completed
    case Left(RepositoryError.Unavailable) | Left(RepositoryError.DuplicateApplication) | Left(
          RepositoryError.InvalidStoredData
        ) | Left(RepositoryError.MissingWriteResult) | Left(RepositoryError.MissingStoredResult) =>
      ProcessingOutcome.Retry
  }

  private def embedPrepared(prepared: EmbeddingPreparation)(
      persist: EntityEmbedding => RepositoryIO[Unit]
  ): RepositoryIO[ProcessingOutcome] = prepared match {
    case EmbeddingPreparation.NoWork           => RepositoryIO.fromEither(Right(ProcessingOutcome.Completed))
    case EmbeddingPreparation.DocumentTooLarge =>
      RepositoryIO.fromEither(Right(ProcessingOutcome.Terminal(EmbeddingWorkFailure.DocumentTooLarge)))
    case EmbeddingPreparation.Prepared(text, hash) =>
      RepositoryIO.lift(embeddings.embed(EmbeddingInput(text, EmbeddingInputType.Document))).flatMap {
        case Left(_)       => RepositoryIO.fromEither(Right(ProcessingOutcome.Retry))
        case Right(vector) =>
          RepositoryIO.lift(now).flatMap { instant =>
            persist(EntityEmbedding(vector.values, EmbeddingMeta(model, hash, instant)))
              .as(ProcessingOutcome.Completed)
          }
      }
  }
}

object EmbeddingPipeline {
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
      diagnostics: Diagnostics
  ): Resource[IO, DurableEmbeddingWorkPublisher] =
    Resource.eval(Queue.bounded[IO, Unit](queueSize)).flatMap { wakeups =>
      for {
        workerId <- Resource.eval(IO.randomUUID.map(_.toString))
        publisher = new DurableEmbeddingWorkPublisher(work, wakeups, IO.realTimeInstant)
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
          IO.realTimeInstant,
          workerId,
          diagnostics
        )
        _ <- Resource.make(
          workerReady.flatMap(ready => if (ready) pipeline.stream.compile.drain else IO.unit).start
        )(_.cancel)
      } yield publisher
    }
}
