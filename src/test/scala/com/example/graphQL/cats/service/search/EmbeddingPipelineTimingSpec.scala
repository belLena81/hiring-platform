package com.example.graphQL.cats.service.search

import cats.effect.{IO, Ref}
import cats.effect.std.UUIDGen
import cats.effect.testkit.TestControl
import com.example.graphQL.cats.domain.model.{Job, User}
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.service.{Diagnostics, RepositoryError}
import com.example.graphQL.cats.service.ServiceFixtures.*
import com.example.graphQL.cats.service.port.*
import java.time.Instant
import munit.CatsEffectSuite
import scala.concurrent.duration.*

/** EPS-05.2: the embedding worker's restart delay is observed in virtual time, never by sleeping. */
final class EmbeddingPipelineTimingSpec extends CatsEffectSuite {

  private def failingClaims(claimedAt: Ref[IO, Vector[FiniteDuration]]): EmbeddingWorkRepository[IO] =
    new EmbeddingWorkRepository[IO] {
      private def unused[A]: RepositoryF[IO, A] = RepositoryF.at[IO].raise(RepositoryError.Unavailable)
      override def enqueue(key: EmbeddingWorkKey, now: Instant): RepositoryF[IO, Unit] = unused
      override def claim(
          workerId: String,
          now: Instant,
          leaseUntil: Instant
      ): RepositoryF[IO, Option[ClaimedEmbeddingWork]] =
        RepositoryF.lift(
          IO.monotonic.flatMap(at => claimedAt.update(_ :+ at)) *>
            IO.raiseError[Option[ClaimedEmbeddingWork]](new IllegalStateException("claim broke"))
        )
      override def renew(claim: ClaimedEmbeddingWork, now: Instant, leaseUntil: Instant): RepositoryF[IO, Boolean] =
        unused
      override def complete(claim: ClaimedEmbeddingWork): RepositoryF[IO, Unit] = unused
      override def retry(
          claim: ClaimedEmbeddingWork,
          availableAt: Instant,
          chargeAttempt: Boolean
      ): RepositoryF[IO, Unit] =
        unused
      override def fail(
          claim: ClaimedEmbeddingWork,
          failure: EmbeddingWorkFailure,
          now: Instant
      ): RepositoryF[IO, Unit] =
        unused
    }

  test("EPS-05.2 a failed worker waits workerRestartDelay before the stream is restarted") {
    val program = for {
      claimedAt <- Ref.of[IO, Vector[FiniteDuration]](Vector.empty)
      users <- Ref.of[IO, Map[UserId, User]](Map.empty)
      jobs <- Ref.of[IO, Map[JobId, Job]](Map.empty)
      embeddings = new EmbeddingService[IO] {
        override def embed(input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]] = IO.never
      }
      _ <- EmbeddingPipeline
        .resource[IO](
          failingClaims(claimedAt),
          new InMemoryUsers(users),
          new InMemoryJobs(jobs),
          embeddings,
          "model",
          queueSize = 4,
          parallelism = 1,
          retryAttempts = 1,
          retryDelay = 10.seconds,
          leaseDuration = 30.seconds,
          diagnostics = Diagnostics.noop[IO],
          workerRestartDelay = 60.seconds
        )
        .use(_ => IO.sleep(100.seconds))
      times <- claimedAt.get
    } yield times

    // The first poll tick is at retryDelay (10s) and fails; the restart waits 60s (to 70s) and the next tick is at 80s.
    TestControl.executeEmbed(program).map(times => assertEquals(times, Vector(10.seconds, 80.seconds)))
  }
}
