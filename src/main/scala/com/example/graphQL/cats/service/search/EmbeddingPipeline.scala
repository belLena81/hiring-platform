package com.example.graphQL.cats.service.search

import cats.effect.std.Queue
import cats.effect.{IO, Resource}
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.{EmbeddingMeta, EntityEmbedding, SearchableText}
import com.example.graphQL.cats.repository.protocol.*
import com.example.graphQL.cats.repository.protocol.RepositoryError
import com.example.graphQL.cats.shared.crypto.SourceHash
import fs2.Stream
import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*

enum EmbeddingWork {
  case JobChanged(id: JobId)
  case CandidateProfileChanged(id: UserId)
}

trait EmbeddingWorkPublisher[F[_]] {
  /** Signals work that was atomically persisted by the mutation transaction. */
  def wake: F[Unit]
}

object EmbeddingWorkPublisher {
  def noop[F[_]: cats.Applicative]: EmbeddingWorkPublisher[F] =
    new EmbeddingWorkPublisher[F] {
      override def wake: F[Unit] = cats.Applicative[F].unit
    }
}

final class DurableEmbeddingWorkPublisher private[search] (
    repository: EmbeddingWorkRepository[IO],
    wakeups: Queue[IO, Unit],
    now: IO[Instant]
) extends EmbeddingWorkPublisher[IO] {
  private[search] def offer(work: EmbeddingWork): IO[Unit] =
    now.flatMap(repository.enqueue(DurableEmbeddingWorkPublisher.keyFor(work), _)).flatMap {
      case Right(()) => wake
      case Left(error) => IO.raiseError(new IllegalStateException(s"Embedding work enqueue failed: $error"))
    }

  override def wake: IO[Unit] = wakeups.tryOffer(()).void
}

object DurableEmbeddingWorkPublisher {
  def keyFor(work: EmbeddingWork): EmbeddingWorkKey = work match {
    case EmbeddingWork.JobChanged(id) => EmbeddingWorkKey(EmbeddingWorkKind.Job, id.value.toString)
    case EmbeddingWork.CandidateProfileChanged(id) => EmbeddingWorkKey(EmbeddingWorkKind.CandidateProfile, id.value.toString)
  }
}

final class EmbeddingPipeline(
    wakeups: Queue[IO, Unit],
    work: EmbeddingWorkRepository[IO],
    users: UserRepository[IO],
    jobs: JobRepository[IO],
    embeddings: EmbeddingService[IO],
    model: String,
    version: Int,
    parallelism: Int,
    retryAttempts: Int,
    retryDelay: FiniteDuration,
    leaseDuration: FiniteDuration,
    now: IO[Instant],
    workerId: String
) {
  def stream: Stream[IO, Unit] =
    Stream.fromQueueUnterminated(wakeups)
      .merge(Stream.awakeEvery[IO](retryDelay).as(()))
      .parEvalMap(parallelism)(_ => drain)

  private def drain: IO[Unit] =
    claimNext.flatMap(_.fold(IO.unit)(claim => processClaim(claim) *> drain))

  private def claimNext: IO[Option[ClaimedEmbeddingWork]] =
    now.flatMap(instant => work.claim(workerId, instant, instant.plusMillis(leaseDuration.toMillis))).flatMap {
      case Right(claim) => IO.pure(claim)
      case Left(_) => IO.pure(None)
    }

  private enum ProcessingOutcome {
    case Completed, Retry
    case Terminal(failure: EmbeddingWorkFailure)
  }

  private enum EmbeddingOutcome {
    case Embedded(value: EntityEmbedding)
    case Retry
    case Discarded
  }

  private def processClaim(claim: ClaimedEmbeddingWork): IO[Unit] =
    process(claim).handleError(_ => ProcessingOutcome.Retry).flatMap {
      case ProcessingOutcome.Completed => work.complete(claim).void
      case ProcessingOutcome.Terminal(failure) => now.flatMap(work.fail(claim, failure, _)).void
      case ProcessingOutcome.Retry if claim.attempts + 1 >= retryAttempts =>
        now.flatMap(work.fail(claim, EmbeddingWorkFailure.RetryExhausted, _)).void
      case ProcessingOutcome.Retry =>
        now.map(_.plusMillis(retryDelay.toMillis)).flatMap(work.retry(claim, _)).void
    }

  private def process(claim: ClaimedEmbeddingWork): IO[ProcessingOutcome] =
    claim.key.kind match {
      case EmbeddingWorkKind.Job =>
        scala.util.Try(JobId(UUID.fromString(claim.key.entityId))).toEither.fold(
          _ => IO.pure(ProcessingOutcome.Terminal(EmbeddingWorkFailure.InvalidWorkKey)),
          processJob
        )
      case EmbeddingWorkKind.CandidateProfile =>
        scala.util.Try(UserId(UUID.fromString(claim.key.entityId))).toEither.fold(
          _ => IO.pure(ProcessingOutcome.Terminal(EmbeddingWorkFailure.InvalidWorkKey)),
          processCandidate
        )
    }

  private def processJob(id: JobId): IO[ProcessingOutcome] =
    jobs.find(id).flatMap {
      case Right(Some(job)) =>
        val text = SearchableText.job(job)
        val hash = SourceHash.sha256(text)
        if (job.embedding.exists(isCurrent(_, hash))) IO.pure(ProcessingOutcome.Completed)
        else embedDocument(text).flatMap {
          case EmbeddingOutcome.Embedded(embedding) => jobs.updateEmbedding(id, job.version, embedding).map(writeOutcome)
          case EmbeddingOutcome.Retry => IO.pure(ProcessingOutcome.Retry)
          case EmbeddingOutcome.Discarded => IO.pure(ProcessingOutcome.Terminal(EmbeddingWorkFailure.DocumentTooLarge))
        }
      case Right(None) => IO.pure(ProcessingOutcome.Completed)
      case Left(_) => IO.pure(ProcessingOutcome.Retry)
    }

  private def processCandidate(id: UserId): IO[ProcessingOutcome] =
    users.find(id).flatMap {
      case Right(Some(user)) => user.candidateProfile match {
        case Some(profile) =>
          val text = SearchableText.candidate(profile)
          val hash = SourceHash.sha256(text)
          if (user.embedding.exists(isCurrent(_, hash))) IO.pure(ProcessingOutcome.Completed)
          else embedDocument(text).flatMap {
            case EmbeddingOutcome.Embedded(embedding) => users.updateEmbedding(id, user.version, embedding).map(writeOutcome)
            case EmbeddingOutcome.Retry => IO.pure(ProcessingOutcome.Retry)
            case EmbeddingOutcome.Discarded => IO.pure(ProcessingOutcome.Terminal(EmbeddingWorkFailure.DocumentTooLarge))
          }
        case None => IO.pure(ProcessingOutcome.Completed)
      }
      case Right(None) => IO.pure(ProcessingOutcome.Completed)
      case Left(_) => IO.pure(ProcessingOutcome.Retry)
    }

  private def isCurrent(embedding: EntityEmbedding, hash: String): Boolean =
    embedding.meta.sourceHash == hash && embedding.meta.model == model && embedding.meta.version == version

  private def writeOutcome(result: Either[RepositoryError, Unit]): ProcessingOutcome = result match {
    case Right(_) | Left(RepositoryError.Conflict) => ProcessingOutcome.Completed
    case Left(RepositoryError.Unavailable) | Left(RepositoryError.DuplicateApplication) => ProcessingOutcome.Retry
  }

  private def embedDocument(text: String): IO[EmbeddingOutcome] =
    if (text.length > SearchableText.DocumentMaxChars) IO.pure(EmbeddingOutcome.Discarded)
    else embeddings.embed(EmbeddingInput(text, EmbeddingInputType.Document)).flatMap {
      case Left(_) => IO.pure(EmbeddingOutcome.Retry)
      case Right(vector) => now.map { instant =>
        EmbeddingOutcome.Embedded(EntityEmbedding(vector.values, EmbeddingMeta(model, version, SourceHash.sha256(text), instant)))
      }
    }
}

object EmbeddingPipeline {
  def resource(
      work: EmbeddingWorkRepository[IO],
      users: UserRepository[IO],
      jobs: JobRepository[IO],
      embeddings: EmbeddingService[IO],
      model: String,
      version: Int,
      queueSize: Int,
      parallelism: Int,
      retryAttempts: Int,
      retryDelay: FiniteDuration,
      leaseDuration: FiniteDuration
  ): Resource[IO, DurableEmbeddingWorkPublisher] =
    Resource.eval(Queue.bounded[IO, Unit](queueSize)).flatMap { wakeups =>
      for {
        workerId <- Resource.eval(IO.randomUUID.map(_.toString))
        publisher = new DurableEmbeddingWorkPublisher(work, wakeups, IO.realTimeInstant)
        pipeline = new EmbeddingPipeline(wakeups, work, users, jobs, embeddings, model, version, parallelism,
          retryAttempts, retryDelay, leaseDuration, IO.realTimeInstant, workerId)
        _ <- Resource.make(pipeline.stream.compile.drain.start)(_.cancel)
      } yield publisher
    }
}
