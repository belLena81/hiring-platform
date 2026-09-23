package com.example.graphQL.cats.runtime

import cats.effect.{Deferred, IO, Ref, Resource}
import scala.concurrent.duration.*
import com.example.graphQL.cats.config.VectorSearchConfig
import com.example.graphQL.cats.repository.protocol.{EmbeddingError, EmbeddingInput, EmbeddingService, EmbeddingVector}
import com.example.graphQL.cats.service.{DatabaseProbe, Diagnostics, HealthService, LogEvent, LogField, ProbeResult}
import com.example.graphQL.cats.service.search.EmbeddingWorkPublisher
import munit.CatsEffectSuite

class MongoHiringRuntimeSpec extends CatsEffectSuite {
  private final class Work
  private final case class Users(work: Option[Work])
  private final case class Jobs(work: Option[Work])
  private final class Search

  private val embeddings: EmbeddingService = new EmbeddingService {
    override def embed(_input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]] =
      IO.pure(Left(EmbeddingError.ProviderUnavailable))
  }

  private def vectorSearchConfig(enabled: Boolean, apiKey: Option[String]): VectorSearchConfig =
    VectorSearchConfig(
      enabled = enabled,
      voyageApiKey = apiKey,
      voyageEndpoint = "https://example.test/embeddings",
      voyageModel = "test-model",
      voyageDimension = 1024,
      queueSize = 8,
      parallelism = 1,
      timeoutMillis = 1000,
      retryAttempts = 2,
      retryDelayMillis = 10,
      jobVectorIndex = "job-vector",
      candidateVectorIndex = "candidate-vector",
      jobLexicalIndex = "job-lexical",
      indexReadyTimeoutMillis = 1000,
      indexPollIntervalMillis = 10,
      numCandidates = 20
    )

  test("disabled embedding capability skips embedding factories") {
    for {
      events <- Ref.of[IO, Vector[String]](Vector.empty)
      capability <- EmbeddingCapability
        .resource[Work, Users, Jobs, Search](
          vectorSearchConfig(enabled = false, apiKey = None),
          events.update(_ :+ "work") *> IO(new Work),
          work => events.update(_ :+ "users") *> IO(Users(work)),
          work => events.update(_ :+ "jobs") *> IO(Jobs(work)),
          events.update(_ :+ "search") *> IO(new Search),
          (_, _) => Resource.eval(events.update(_ :+ "provider") *> IO.pure(embeddings)),
          (_, _, _, _) => Resource.eval(events.update(_ :+ "pipeline") *> IO.pure(EmbeddingWorkPublisher.noop))
        )
        .use(IO.pure)
      recorded <- events.get
    } yield {
      assert(capability match {
        case EmbeddingCapability.Disabled(_, _)               => true
        case EmbeddingCapability.Enabled(_, _, _, _, _, _, _) => false
      })
      assertEquals(capability.users.work, None)
      assertEquals(capability.jobs.work, None)
      assertEquals(recorded, Vector("users", "jobs"))
    }
  }

  test("enabled embedding capability validates its API key before invoking factories") {
    for {
      events <- Ref.of[IO, Vector[String]](Vector.empty)
      result <- EmbeddingCapability
        .resource[Work, Users, Jobs, Search](
          vectorSearchConfig(enabled = true, apiKey = None),
          events.update(_ :+ "work") *> IO(new Work),
          work => events.update(_ :+ "users") *> IO(Users(work)),
          work => events.update(_ :+ "jobs") *> IO(Jobs(work)),
          events.update(_ :+ "search") *> IO(new Search),
          (_, _) => Resource.eval(events.update(_ :+ "provider") *> IO.pure(embeddings)),
          (_, _, _, _) => Resource.eval(events.update(_ :+ "pipeline") *> IO.pure(EmbeddingWorkPublisher.noop))
        )
        .use(_ => IO.unit)
        .attempt
      recorded <- events.get
    } yield {
      assert(result.swap.exists(_.isInstanceOf[IllegalArgumentException]))
      assertEquals(recorded, Vector.empty)
    }
  }

  test("enabled embedding capability shares work and releases pipeline before provider") {
    for {
      events <- Ref.of[IO, Vector[String]](Vector.empty)
      observed <- EmbeddingCapability
        .resource[Work, Users, Jobs, Search](
          vectorSearchConfig(enabled = true, apiKey = Some("test-key")),
          events.update(_ :+ "work") *> IO(new Work),
          work => events.update(_ :+ "users") *> IO(Users(work)),
          work => events.update(_ :+ "jobs") *> IO(Jobs(work)),
          events.update(_ :+ "search") *> IO(new Search),
          (_, _) =>
            Resource.make(events.update(_ :+ "provider-acquire").as(embeddings))(_ =>
              events.update(_ :+ "provider-release")
            ),
          (work, users, jobs, _) =>
            Resource
              .make(
                events.update(_ :+ "pipeline-acquire").as((work, users, jobs, EmbeddingWorkPublisher.noop))
              )(_ => events.update(_ :+ "pipeline-release"))
              .map(_._4)
        )
        .use {
          case EmbeddingCapability.Enabled(work, users, jobs, _, _, _, model) =>
            IO.pure((users.work.exists(_ eq work), jobs.work.exists(_ eq work), model))
          case EmbeddingCapability.Disabled(_, _) =>
            IO.raiseError(new AssertionError("expected enabled embedding capability"))
        }
      recorded <- events.get
    } yield {
      assertEquals(observed, (true, true, "test-model"))
      assertEquals(
        recorded,
        Vector(
          "work",
          "users",
          "jobs",
          "search",
          "provider-acquire",
          "pipeline-acquire",
          "pipeline-release",
          "provider-release"
        )
      )
    }
  }

  test("pipeline acquisition failure releases the acquired embedding provider") {
    for {
      events <- Ref.of[IO, Vector[String]](Vector.empty)
      result <- EmbeddingCapability
        .resource[Work, Users, Jobs, Search](
          vectorSearchConfig(enabled = true, apiKey = Some("test-key")),
          IO(new Work),
          work => IO(Users(work)),
          work => IO(Jobs(work)),
          IO(new Search),
          (_, _) =>
            Resource.make(events.update(_ :+ "provider-acquire").as(embeddings))(_ =>
              events.update(_ :+ "provider-release")
            ),
          (_, _, _, _) =>
            Resource.eval(
              events.update(_ :+ "pipeline-acquire") *> IO.raiseError[EmbeddingWorkPublisher](
                new RuntimeException("synthetic pipeline acquisition failure")
              )
            )
        )
        .use(_ => IO.unit)
        .attempt
      recorded <- events.get
    } yield {
      assert(result.isLeft)
      assertEquals(recorded, Vector("provider-acquire", "pipeline-acquire", "provider-release"))
    }
  }

  test("downstream acquisition failure releases pipeline and embedding provider") {
    for {
      events <- Ref.of[IO, Vector[String]](Vector.empty)
      capability = EmbeddingCapability.resource[Work, Users, Jobs, Search](
        vectorSearchConfig(enabled = true, apiKey = Some("test-key")),
        IO(new Work),
        work => IO(Users(work)),
        work => IO(Jobs(work)),
        IO(new Search),
        (_, _) =>
          Resource.make(events.update(_ :+ "provider-acquire").as(embeddings))(_ =>
            events.update(_ :+ "provider-release")
          ),
        (_, _, _, _) =>
          Resource.make(events.update(_ :+ "pipeline-acquire").as(EmbeddingWorkPublisher.noop))(_ =>
            events.update(_ :+ "pipeline-release")
          )
      )
      result <- capability
        .flatMap(_ =>
          Resource.eval(
            IO.raiseError[Unit](
              new RuntimeException("synthetic downstream acquisition failure")
            )
          )
        )
        .use(_ => IO.unit)
        .attempt
      recorded <- events.get
    } yield {
      assert(result.isLeft)
      assertEquals(
        recorded,
        Vector(
          "provider-acquire",
          "pipeline-acquire",
          "pipeline-release",
          "provider-release"
        )
      )
    }
  }

  test("cancelling embedding capability use releases pipeline and provider") {
    for {
      events <- Ref.of[IO, Vector[String]](Vector.empty)
      acquired <- Deferred[IO, Unit]
      use = EmbeddingCapability
        .resource[Work, Users, Jobs, Search](
          vectorSearchConfig(enabled = true, apiKey = Some("test-key")),
          IO(new Work),
          work => IO(Users(work)),
          work => IO(Jobs(work)),
          IO(new Search),
          (_, _) =>
            Resource.make(events.update(_ :+ "provider-acquire").as(embeddings))(_ =>
              events.update(_ :+ "provider-release")
            ),
          (_, _, _, _) =>
            Resource.make(
              events.update(_ :+ "pipeline-acquire") *> acquired.complete(()).as(EmbeddingWorkPublisher.noop)
            )(_ => events.update(_ :+ "pipeline-release"))
        )
        .use(_ => IO.never)
      fiber <- use.start
      _ <- acquired.get
      _ <- fiber.cancel
      recorded <- events.get
    } yield assertEquals(
      recorded,
      Vector(
        "provider-acquire",
        "pipeline-acquire",
        "pipeline-release",
        "provider-release"
      )
    )
  }

  test("readiness observes setup without cancelling its resource-owned operation") {
    for {
      entered <- Deferred[IO, Unit]
      release <- Deferred[IO, Unit]
      setupCalls <- Ref.of[IO, Int](0)
      setup = setupCalls.update(_ + 1) *> entered.complete(()).void *> release.get
      result <- SetupLifecycle.resource(setup).use { lifecycle =>
        val probe = new DatabaseProbe {
          override def check: IO[ProbeResult] = lifecycle.ready.map {
            if (_) ProbeResult.Ready else ProbeResult.Unavailable
          }
        }
        for {
          _ <- entered.get
          before <- new HealthService(probe, Diagnostics.noop).readiness(None)
          _ <- release.complete(()).void
          _ <- lifecycle.await
          after <- new HealthService(probe, Diagnostics.noop).readiness(None)
          calls <- setupCalls.get
        } yield (before, after, calls)
      }
    } yield {
      assertEquals(result, (ProbeResult.Unavailable, ProbeResult.Ready, 1))
    }
  }

  test("setup failure is recorded once and does not trigger readiness retries") {
    for {
      setupCalls <- Ref.of[IO, Int](0)
      setup = setupCalls.update(_ + 1) *> IO.raiseError[Unit](new RuntimeException("synthetic setup failure"))
      result <- SetupLifecycle.resource(setup).use { lifecycle =>
        for {
          first <- lifecycle.await
          second <- lifecycle.await
          ready <- lifecycle.ready
          calls <- setupCalls.get
        } yield (first, second, ready, calls)
      }
    } yield {
      assertEquals(result, (false, false, false, 1))
    }
  }

  test("readiness is immediately unavailable while setup is pending") {
    for {
      entered <- Deferred[IO, Unit]
      release <- Deferred[IO, Unit]
      setup = entered.complete(()).void *> release.get
      ready <- SetupLifecycle.resource(setup).use { lifecycle =>
        entered.get *> lifecycle.ready.timeout(100.millis)
      }
    } yield assertEquals(ready, false)
  }

  test("setup failure is emitted to diagnostics") {
    for {
      events <- Ref.of[IO, List[LogEvent]](Nil)
      diagnostics = new Diagnostics {
        override def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]): IO[Unit] =
          events.update(event :: _)
      }
      _ <- SetupLifecycle
        .resource(IO.raiseError[Unit](new RuntimeException("synthetic setup failure")), diagnostics)
        .use(_.await)
      recorded <- events.get
    } yield assertEquals(recorded, List(LogEvent.MongoSetupFailed))
  }
}
