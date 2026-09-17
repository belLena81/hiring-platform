package com.example.graphQL.cats.service.search

import cats.effect.IO
import cats.effect.Deferred
import cats.effect.Ref
import cats.syntax.all.*
import com.example.graphQL.cats.repository.protocol.*
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.ServiceFixtures.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.shared.crypto.SourceHash
import com.example.graphQL.cats.shared.pagination.JobPageRequest
import com.example.graphQL.cats.shared.search.JobSearchFilter
import java.util.UUID
import munit.CatsEffectSuite
import scala.concurrent.duration.*

final class EmbeddingPipelineSpec extends CatsEffectSuite {
  test("VHS-AC05 VHS-AC06 embedding pipeline updates changed jobs and skips unchanged source hashes") {
    for {
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map.empty)
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> openJob))
      calls <- Ref.of[IO, Int](0)
      users = InMemoryUsers(usersRef)
      jobs = InMemoryJobs(jobsRef)
      embeddings = CountingEmbeddingService(calls)
      _ <- EmbeddingPipeline.resource(
        users,
        jobs,
        embeddings,
        model = "voyage-4-lite",
        version = 1,
        queueSize = 8,
        parallelism = 1
      ).use { queue =>
        for {
          _ <- queue.offer(EmbeddingWork.JobChanged(jobId))
          _ <- waitFor(calls.get.map(_ == 1))
          updated <- eventually(jobs.find(jobId))(
            _.flatMap(_.embedding).exists(_.meta.sourceHash == SourceHash.sha256(SearchableText.job(openJob)))
          )
          _ = assert(updated.flatMap(_.embedding).exists(_.meta.sourceHash == SourceHash.sha256(SearchableText.job(openJob))))
          _ <- queue.offer(EmbeddingWork.JobChanged(jobId))
          _ <- IO.sleep(200.millis)
          finalCalls <- calls.get
        } yield assertEquals(finalCalls, 1)
      }
    } yield ()
  }

  test("VHS-AC06 embedding pipeline regenerates jobs when model or version changes") {
    val currentHash = SourceHash.sha256(SearchableText.job(openJob))
    val staleModel = EntityEmbedding(
      List(0.1f, 0.2f),
      EmbeddingMeta("voyage-previous", 1, currentHash, now)
    )
    val staleVersion = staleModel.copy(meta = staleModel.meta.copy(model = "voyage-4-lite", version = 0))
    List(staleModel, staleVersion).traverse_ { existing =>
      for {
        usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map.empty)
        jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> openJob.copy(embedding = Some(existing))))
        calls <- Ref.of[IO, Int](0)
        users = InMemoryUsers(usersRef)
        jobs = InMemoryJobs(jobsRef)
        embeddings = CountingEmbeddingService(calls)
        _ <- EmbeddingPipeline.resource(
          users,
          jobs,
          embeddings,
          model = "voyage-4-lite",
          version = 1,
          queueSize = 8,
          parallelism = 1
        ).use { queue =>
          for {
            _ <- queue.offer(EmbeddingWork.JobChanged(jobId))
            _ <- waitFor(calls.get.map(_ == 1))
            updated <- jobs.find(jobId)
          } yield {
            assertEquals(updated.flatMap(_.embedding).map(_.meta.model), Some("voyage-4-lite"))
            assertEquals(updated.flatMap(_.embedding).map(_.meta.version), Some(1))
            assertEquals(updated.flatMap(_.embedding).map(_.meta.sourceHash), Some(currentHash))
          }
        }
      } yield ()
    }
  }

  test("VHS-AC05 embedding pipeline ignores stale in-flight job writes after version changes") {
    for {
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map.empty)
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> openJob))
      started <- Deferred[IO, Unit]
      release <- Deferred[IO, Unit]
      writeResult <- Deferred[IO, Either[RepositoryError, Unit]]
      calls <- Ref.of[IO, Int](0)
      users = InMemoryUsers(usersRef)
      jobsDelegate = InMemoryJobs(jobsRef)
      jobs = RecordingEmbeddingWrites(jobsDelegate, writeResult)
      embeddings = BlockingEmbeddingService(calls, started, release)
      _ <- EmbeddingPipeline.resource(
        users,
        jobs,
        embeddings,
        model = "voyage-4-lite",
        version = 1,
        queueSize = 8,
        parallelism = 1
      ).use { queue =>
        for {
          _ <- queue.offer(EmbeddingWork.JobChanged(jobId))
          _ <- started.get
          updated <- jobs.update(openJob.copy(title = "Staff Scala Developer"))
          _ = assert(updated.exists(_.version == 1L))
          _ <- release.complete(()).void
          staleResult <- writeResult.get
          finalJob <- jobs.find(jobId)
        } yield {
          assertEquals(staleResult, Left(RepositoryError.Conflict))
          assertEquals(finalJob.map(_.version), Some(1L))
          assertEquals(finalJob.flatMap(_.embedding), None)
        }
      }
    } yield ()
  }

  test("VHS-AC05 embedding work publishing stays non-blocking when the scheduler is full") {
    val otherJobId = Identifiers.JobId(UUID.fromString("00000000-0000-0000-0000-000000000008"))
    val otherJob = openJob.copy(id = otherJobId, title = "Principal Scala Developer")
    for {
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map.empty)
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> openJob, otherJobId -> otherJob))
      started <- Deferred[IO, Unit]
      release <- Deferred[IO, Unit]
      calls <- Ref.of[IO, Int](0)
      users = InMemoryUsers(usersRef)
      jobs = InMemoryJobs(jobsRef)
      embeddings = BlockingEmbeddingService(calls, started, release)
      _ <- EmbeddingPipeline.resource(
        users,
        jobs,
        embeddings,
        model = "voyage-4-lite",
        version = 1,
        queueSize = 1,
        parallelism = 1
      ).use { queue =>
        for {
          _ <- queue.publish(EmbeddingWork.JobChanged(jobId))
          _ <- started.get
          _ <- queue.publish(EmbeddingWork.JobChanged(jobId))
          saturatedPublish <- queue.publish(EmbeddingWork.JobChanged(otherJobId)).timeout(200.millis).attempt
          _ = assertEquals(saturatedPublish, Right(()))
          _ <- release.complete(()).void
          updated <- eventually(jobs.find(otherJobId))(
            _.flatMap(_.embedding).exists(_.meta.sourceHash == SourceHash.sha256(SearchableText.job(otherJob)))
          )
          finalCalls <- calls.get
        } yield {
          assert(updated.flatMap(_.embedding).exists(_.meta.sourceHash == SourceHash.sha256(SearchableText.job(otherJob))))
          assert(finalCalls >= 2)
        }
      }
    } yield ()
  }

  private def waitFor(done: IO[Boolean], remaining: Int = 20): IO[Unit] =
    done.flatMap {
      case true => IO.unit
      case false if remaining > 0 => IO.sleep(50.millis) *> waitFor(done, remaining - 1)
      case false => IO.raiseError(new AssertionError("condition was not met"))
    }

  private def eventually[A](effect: IO[A])(accepted: A => Boolean, remaining: Int = 20): IO[A] =
    effect.flatMap { value =>
      if (accepted(value)) IO.pure(value)
      else if (remaining > 0) IO.sleep(50.millis) *> eventually(effect)(accepted, remaining - 1)
      else IO.raiseError(new AssertionError("condition was not met"))
    }

  private final case class CountingEmbeddingService(calls: Ref[IO, Int]) extends EmbeddingService[IO] {
    override def embed(input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]] =
      calls.update(_ + 1).as(Right(EmbeddingVector(List(0.1f, 0.2f), "voyage-4-lite", 2)))
  }

  private final case class BlockingEmbeddingService(
      calls: Ref[IO, Int],
      started: Deferred[IO, Unit],
      release: Deferred[IO, Unit]
  ) extends EmbeddingService[IO] {
    override def embed(input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]] =
      calls.update(_ + 1) *> started.complete(()).void *> release.get.as(
        Right(EmbeddingVector(List(0.1f, 0.2f), "voyage-4-lite", 2))
      )
  }

  private final case class RecordingEmbeddingWrites(
      delegate: InMemoryJobs,
      writeResult: Deferred[IO, Either[RepositoryError, Unit]]
  ) extends JobRepository[IO] {
    override def find(id: Identifiers.JobId): IO[Option[Job]] =
      delegate.find(id)

    override def findMany(ids: List[Identifiers.JobId]): IO[List[Job]] =
      delegate.findMany(ids)

    override def findOpen(filter: JobSearchFilter, page: JobPageRequest): IO[List[Job]] =
      delegate.findOpen(filter, page)

    override def findAll(page: JobPageRequest): IO[List[Job]] =
      delegate.findAll(page)

    override def findByRecruiter(recruiterId: Identifiers.UserId, page: JobPageRequest): IO[List[Job]] =
      delegate.findByRecruiter(recruiterId, page)

    override def create(job: Job): IO[Either[RepositoryError, Unit]] =
      delegate.create(job)

    override def update(job: Job): IO[Either[RepositoryError, Job]] =
      delegate.update(job)

    override def updateEmbedding(
        id: Identifiers.JobId,
        observedVersion: Long,
        embedding: EntityEmbedding
    ): IO[Either[RepositoryError, Unit]] =
      delegate.updateEmbedding(id, observedVersion, embedding).flatTap(result => writeResult.complete(result).void)
  }
}
