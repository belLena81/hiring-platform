package com.example.graphQL.cats.service.search

import cats.effect.std.Queue
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.repository.protocol.*
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.{EmbeddingMeta, EntityEmbedding, SearchableText}
import com.example.graphQL.cats.shared.crypto.SourceHash
import fs2.Stream
import java.time.Instant

enum EmbeddingWork {
  case JobChanged(id: JobId)
  case CandidateProfileChanged(id: UserId)
}

final class EmbeddingWorkQueue private[service] (
    wakeups: Queue[IO, Unit],
    pending: Ref[IO, Set[EmbeddingWork]]
) extends EmbeddingWorkPublisher[IO] {
  def offer(work: EmbeddingWork): IO[Unit] =
    pending.update(_ + work) *> wakeups.tryOffer(()).void

  override def publish(work: EmbeddingWork): IO[Unit] =
    offer(work)
}

trait EmbeddingWorkPublisher[F[_]] {
  def publish(work: EmbeddingWork): F[Unit]
}

object EmbeddingWorkPublisher {
  def noop[F[_]: cats.Applicative]: EmbeddingWorkPublisher[F] =
    _ => cats.Applicative[F].unit
}

object EmbeddingWorkQueue {
  def bounded(capacity: Int): IO[EmbeddingWorkQueue] =
    (Queue.bounded[IO, Unit](capacity), Ref.of[IO, Set[EmbeddingWork]](Set.empty))
      .mapN(new EmbeddingWorkQueue(_, _))
}

final class EmbeddingPipeline(
    wakeups: Queue[IO, Unit],
    pending: Ref[IO, Set[EmbeddingWork]],
    users: UserRepository[IO],
    jobs: JobRepository[IO],
    embeddings: EmbeddingService[IO],
    model: String,
    version: Int,
    parallelism: Int,
    now: IO[Instant]
) {
  def stream: Stream[IO, Unit] =
    Stream.fromQueueUnterminated(wakeups).parEvalMap(parallelism)(_ => drain)

  private def drain: IO[Unit] =
    nextWork.flatMap(_.fold(IO.unit)(work => process(work) *> drain))

  private def nextWork: IO[Option[EmbeddingWork]] =
    pending.modify { work =>
      work.headOption.fold(work -> Option.empty[EmbeddingWork]) { next =>
        (work - next) -> Some(next)
      }
    }

  private def process(work: EmbeddingWork): IO[Unit] =
    work match {
      case EmbeddingWork.JobChanged(id) =>
        jobs.find(id).flatMap {
          case Some(job) =>
            val text = SearchableText.job(job)
            val hash = SourceHash.sha256(text)
            if (job.embedding.exists(isCurrent(_, hash))) IO.unit
            else embedDocument(text).flatMap(_.traverse_(embedding => jobs.updateEmbedding(id, job.version, embedding).void))
          case None => IO.unit
        }
      case EmbeddingWork.CandidateProfileChanged(id) =>
        users.find(id).flatMap {
          case Some(user) =>
            user.candidateProfile match {
              case Some(profile) =>
                val text = SearchableText.candidate(profile)
                val hash = SourceHash.sha256(text)
                if (user.embedding.exists(isCurrent(_, hash))) IO.unit
                else embedDocument(text).flatMap(_.traverse_(embedding => users.updateEmbedding(id, embedding).void))
              case None => IO.unit
            }
          case None => IO.unit
        }
    }

  private def isCurrent(embedding: EntityEmbedding, hash: String): Boolean =
    embedding.meta.sourceHash == hash && embedding.meta.model == model && embedding.meta.version == version

  private def embedDocument(text: String): IO[Option[EntityEmbedding]] =
    if (text.length > SearchableText.DocumentMaxChars) IO.pure(None)
    else embeddings.embed(EmbeddingInput(text, EmbeddingInputType.Document)).flatMap {
      case Left(_) => IO.pure(None)
      case Right(vector) =>
        now.map { instant =>
          Some(EntityEmbedding(vector.values, EmbeddingMeta(model, version, SourceHash.sha256(text), instant)))
        }
    }
}

object EmbeddingPipeline {
  def resource(
      users: UserRepository[IO],
      jobs: JobRepository[IO],
      embeddings: EmbeddingService[IO],
      model: String,
      version: Int,
      queueSize: Int,
      parallelism: Int
  ): Resource[IO, EmbeddingWorkQueue] =
    Resource.eval((Queue.bounded[IO, Unit](queueSize), Ref.of[IO, Set[EmbeddingWork]](Set.empty)).tupled).flatMap {
      case (wakeups, pending) =>
        val publisher = new EmbeddingWorkQueue(wakeups, pending)
        val pipeline = new EmbeddingPipeline(
          wakeups,
          pending,
          users,
          jobs,
          embeddings,
          model,
          version,
          parallelism,
          IO.realTimeInstant
        )
        Resource.make(pipeline.stream.compile.drain.start)(_.cancel).as(publisher)
    }
}
