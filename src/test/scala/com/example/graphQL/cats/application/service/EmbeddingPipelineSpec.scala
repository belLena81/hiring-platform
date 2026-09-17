package com.example.graphQL.cats.application.service

import cats.effect.IO
import cats.effect.Ref
import com.example.graphQL.cats.application.port.*
import com.example.graphQL.cats.application.service.ServiceFixtures.*
import com.example.graphQL.cats.domain.model.*
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
          updated <- jobs.find(jobId)
          _ = assert(updated.flatMap(_.embedding).exists(_.meta.sourceHash == SourceHash.sha256(SearchableText.job(openJob))))
          _ <- queue.offer(EmbeddingWork.JobChanged(jobId))
          _ <- IO.sleep(200.millis)
          finalCalls <- calls.get
        } yield assertEquals(finalCalls, 1)
      }
    } yield ()
  }

  private def waitFor(done: IO[Boolean], remaining: Int = 20): IO[Unit] =
    done.flatMap {
      case true => IO.unit
      case false if remaining > 0 => IO.sleep(50.millis) *> waitFor(done, remaining - 1)
      case false => IO.raiseError(new AssertionError("condition was not met"))
    }

  private final case class CountingEmbeddingService(calls: Ref[IO, Int]) extends EmbeddingService[IO] {
    override def embed(input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]] =
      calls.update(_ + 1).as(Right(EmbeddingVector(List(0.1f, 0.2f), "voyage-4-lite", 2)))
  }
}
