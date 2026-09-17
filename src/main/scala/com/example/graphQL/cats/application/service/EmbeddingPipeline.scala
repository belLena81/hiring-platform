package com.example.graphQL.cats.application.service

import cats.effect.std.Queue
import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.application.port.*
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.{EmbeddingMeta, EntityEmbedding, SearchableText}
import fs2.Stream
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Instant

enum EmbeddingWork {
  case JobChanged(id: JobId)
  case CandidateProfileChanged(id: UserId)
}

final class EmbeddingWorkQueue private[service] (queue: Queue[IO, EmbeddingWork]) extends EmbeddingWorkPublisher[IO] {
  def offer(work: EmbeddingWork): IO[Unit] = queue.offer(work)
  override def publish(work: EmbeddingWork): IO[Unit] = offer(work)
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
    Queue.bounded[IO, EmbeddingWork](capacity).map(new EmbeddingWorkQueue(_))
}

final class EmbeddingPipeline(
    queue: Queue[IO, EmbeddingWork],
    users: UserRepository[IO],
    jobs: JobRepository[IO],
    embeddings: EmbeddingService[IO],
    model: String,
    version: Int,
    parallelism: Int,
    now: IO[Instant]
) {
  def stream: Stream[IO, Unit] =
    Stream.fromQueueUnterminated(queue).parEvalMap(parallelism)(process)

  private def process(work: EmbeddingWork): IO[Unit] =
    work match {
      case EmbeddingWork.JobChanged(id) =>
        jobs.find(id).flatMap {
          case Some(job) =>
            val text = SearchableText.job(job)
            val hash = SourceHash.sha256(text)
            if (job.embedding.exists(_.meta.sourceHash == hash)) IO.unit
            else embedDocument(text).flatMap(_.traverse_(embedding => jobs.updateEmbedding(id, embedding).void))
          case None => IO.unit
        }
      case EmbeddingWork.CandidateProfileChanged(id) =>
        users.find(id).flatMap {
          case Some(user) =>
            user.profile match {
              case Some(profile) =>
                val text = SearchableText.candidate(profile)
                val hash = SourceHash.sha256(text)
                if (user.embedding.exists(_.meta.sourceHash == hash)) IO.unit
                else embedDocument(text).flatMap(_.traverse_(embedding => users.updateEmbedding(id, embedding).void))
              case None => IO.unit
            }
          case None => IO.unit
        }
    }

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
    Resource.eval(Queue.bounded[IO, EmbeddingWork](queueSize)).flatMap { queue =>
      val pipeline = new EmbeddingPipeline(queue, users, jobs, embeddings, model, version, parallelism, IO.realTimeInstant)
      Resource.make(pipeline.stream.compile.drain.start)(_.cancel).as(new EmbeddingWorkQueue(queue))
    }
}

object SourceHash {
  def sha256(value: String): String = {
    val digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8))
    digest.map(byte => f"${byte & 0xff}%02x").mkString
  }
}
