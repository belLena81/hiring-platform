package com.example.graphQL.cats.service.search

import cats.effect.{IO, Resource}
import cats.effect.Deferred
import cats.effect.Ref
import cats.syntax.all.*
import com.example.graphQL.cats.repository.protocol.*
import com.example.graphQL.cats.repository.protocol.RepositoryError
import com.example.graphQL.cats.service.ServiceFixtures.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.shared.crypto.SourceHash
import com.example.graphQL.cats.shared.events.OperationalEventEnvelope
import com.example.graphQL.cats.shared.pagination.JobPageRequest
import com.example.graphQL.cats.shared.search.JobSearchFilter
import java.util.UUID
import java.time.Instant
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
      _ <- pipelineResource(
        users,
        jobs,
        embeddings,
        model = "voyage-4-lite",
        queueSize = 8,
        parallelism = 1,
        retryAttempts = 3,
        retryDelay = 10.millis
      ).use { queue =>
        for {
          _ <- queue.offer(EmbeddingWork.JobChanged(jobId))
          _ <- waitFor(calls.get.map(_ == 1))
          updated <- eventually(successfulFind(jobs, jobId))(
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

  test("embedding pipeline regenerates jobs when the model changes") {
    val currentHash = SourceHash.sha256(SearchableText.job(openJob))
    val staleModel = EntityEmbedding(
      List(0.1f, 0.2f),
      EmbeddingMeta("voyage-previous", currentHash, now)
    )
    List(staleModel).traverse_ { existing =>
      for {
        usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map.empty)
        jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> openJob.copy(embedding = Some(existing))))
        calls <- Ref.of[IO, Int](0)
        users = InMemoryUsers(usersRef)
        jobs = InMemoryJobs(jobsRef)
        embeddings = CountingEmbeddingService(calls)
        _ <- pipelineResource(
          users,
          jobs,
          embeddings,
          model = "voyage-4-lite",
          queueSize = 8,
          parallelism = 1,
          retryAttempts = 3,
          retryDelay = 10.millis
        ).use { queue =>
          for {
            _ <- queue.offer(EmbeddingWork.JobChanged(jobId))
            _ <- waitFor(calls.get.map(_ == 1))
            updated <- successfulFind(jobs, jobId)
          } yield {
            assertEquals(updated.flatMap(_.embedding).map(_.meta.model), Some("voyage-4-lite"))
            assertEquals(updated.flatMap(_.embedding).map(_.meta.sourceHash), Some(currentHash))
          }
        }
      } yield ()
    }
  }

  test("embedding pipeline retries a transient provider failure with bounded configured attempts") {
    for {
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map.empty)
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> openJob))
      calls <- Ref.of[IO, Int](0)
      users = InMemoryUsers(usersRef)
      jobs = InMemoryJobs(jobsRef)
      embeddings = FailOnceEmbeddingService(calls)
      _ <- pipelineResource(
        users,
        jobs,
        embeddings,
        model = "voyage-4-lite",
        queueSize = 8,
        parallelism = 1,
        retryAttempts = 2,
        retryDelay = 10.millis
      ).use { queue =>
        for {
          _ <- queue.offer(EmbeddingWork.JobChanged(jobId))
          updated <- eventually(successfulFind(jobs, jobId))(_.flatMap(_.embedding).nonEmpty)
          attempts <- calls.get
        } yield {
          assert(updated.flatMap(_.embedding).nonEmpty)
          assertEquals(attempts, 2)
        }
      }
    } yield ()
  }

  test("embedding pipeline keeps durable attempts unchanged during in-lease retries") {
    for {
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map.empty)
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> openJob))
      calls <- Ref.of[IO, Int](0)
      work <- InMemoryEmbeddingWorkRepository.create
      _ <- EmbeddingPipeline.resource(
        work,
        InMemoryUsers(usersRef),
        InMemoryJobs(jobsRef),
        AlwaysFailEmbeddingService(calls),
        "voyage-4-lite",
        8,
        1,
        retryAttempts = 3,
        retryDelay = 10.millis,
        leaseDuration = 1.second
      ).use { publisher =>
        val key = DurableEmbeddingWorkPublisher.keyFor(EmbeddingWork.JobChanged(jobId))
        publisher.offer(EmbeddingWork.JobChanged(jobId)) *>
          eventually(work.snapshot)(_.get(key.value).exists(_.failure.contains(EmbeddingWorkFailure.RetryExhausted))).void
      }
      snapshot <- work.snapshot
      attempts <- calls.get
    } yield {
      val stored = snapshot(DurableEmbeddingWorkPublisher.keyFor(EmbeddingWork.JobChanged(jobId)).value)
      assertEquals(attempts, 3)
      assertEquals(stored.attempts, 0)
      assertEquals(stored.state, "Failed")
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
      _ <- pipelineResource(
        users,
        jobs,
        embeddings,
        model = "voyage-4-lite",
        queueSize = 8,
        parallelism = 1,
        retryAttempts = 3,
        retryDelay = 10.millis
      ).use { queue =>
        for {
          _ <- queue.offer(EmbeddingWork.JobChanged(jobId))
          _ <- started.get
          updated <- jobs.update(openJob.copy(title = "Staff Scala Developer"), now)
          _ = assert(updated.isRight)
          _ <- release.complete(()).void
          staleResult <- writeResult.get
          finalJob <- successfulFind(jobs, jobId)
        } yield {
          assertEquals(staleResult, Left(RepositoryError.Conflict))
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
      _ <- pipelineResource(
        users,
        jobs,
        embeddings,
        model = "voyage-4-lite",
        queueSize = 1,
        parallelism = 1,
        retryAttempts = 3,
        retryDelay = 10.millis
      ).use { queue =>
        for {
          _ <- queue.offer(EmbeddingWork.JobChanged(jobId))
          _ <- started.get
          _ <- queue.offer(EmbeddingWork.JobChanged(jobId))
          saturatedPublish <- queue.offer(EmbeddingWork.JobChanged(otherJobId)).timeout(200.millis).attempt
          _ = assertEquals(saturatedPublish, Right(()))
          _ <- release.complete(()).void
          updated <- eventually(successfulFind(jobs, otherJobId))(
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

  test("durable embedding work preserves an active lease while coalescing newer updates") {
    val work = EmbeddingWorkKey(EmbeddingWorkKind.Job, jobId.value.toString)
    val first = now
    val second = now.plusSeconds(1)
    for {
      repository <- InMemoryEmbeddingWorkRepository.create
      _ <- repository.enqueue(work, first)
      claim <- repository.claim("worker-a", first, first.plusSeconds(30))
      claimed <- IO.fromOption(claim.toOption.flatten)(new AssertionError("expected work claim"))
      _ <- repository.enqueue(work, second)
      completed <- repository.complete(claimed)
      blockedClaim <- repository.claim("worker-b", second, second.plusSeconds(30))
      nextClaim <- repository.claim("worker-b", first.plusSeconds(31), first.plusSeconds(61))
      snapshot <- repository.snapshot
    } yield {
      assertEquals(completed, Left(RepositoryError.Conflict))
      assertEquals(blockedClaim, Right(None))
      assert(nextClaim.toOption.flatten.nonEmpty)
      assertEquals(snapshot(work.value).generation, 2L)
    }
  }

  test("embedding worker records terminal provider failure and continues with later work") {
    val otherJobId = Identifiers.JobId(UUID.fromString("00000000-0000-0000-0000-000000000009"))
    val otherJob = openJob.copy(id = otherJobId, title = "Recovery Scala Developer")
    for {
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map.empty)
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> openJob, otherJobId -> otherJob))
      calls <- Ref.of[IO, Int](0)
      work <- InMemoryEmbeddingWorkRepository.create
      embeddings = new EmbeddingService {
        override def embed(input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]] =
          calls.updateAndGet(_ + 1).map { count =>
            if (count == 1) Left(EmbeddingError.ProviderUnavailable)
            else Right(EmbeddingVector(List(0.1f, 0.2f), "voyage-4-lite", 2))
          }
      }
      _ <- EmbeddingPipeline.resource(work, InMemoryUsers(usersRef), InMemoryJobs(jobsRef), embeddings,
        "voyage-4-lite", 8, 1, retryAttempts = 1, retryDelay = 10.millis, leaseDuration = 1.second).use { publisher =>
          publisher.offer(EmbeddingWork.JobChanged(jobId)) *> publisher.offer(EmbeddingWork.JobChanged(otherJobId)) *>
            eventually(jobsRef.get.map(_.get(otherJobId).flatMap(_.embedding).nonEmpty))(identity).void
        }
      snapshot <- work.snapshot
    } yield {
      assertEquals(snapshot(DurableEmbeddingWorkPublisher.keyFor(EmbeddingWork.JobChanged(jobId)).value).failure,
        Some(EmbeddingWorkFailure.RetryExhausted))
      assert(snapshot.get(DurableEmbeddingWorkPublisher.keyFor(EmbeddingWork.JobChanged(otherJobId)).value).isEmpty)
    }
  }

  test("embedding worker marks malformed durable work keys as terminal failures") {
    val malformedJob = EmbeddingWorkKey(EmbeddingWorkKind.Job, "not-a-uuid")
    val malformedCandidate = EmbeddingWorkKey(EmbeddingWorkKind.CandidateProfile, "also-not-a-uuid")
    for {
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map.empty)
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map.empty)
      work <- InMemoryEmbeddingWorkRepository.create
      _ <- work.enqueue(malformedJob, now)
      _ <- work.enqueue(malformedCandidate, now)
      _ <- EmbeddingPipeline.resource(work, InMemoryUsers(usersRef), InMemoryJobs(jobsRef), CountingEmbeddingService.uncounted,
        "voyage-4-lite", 8, 1, retryAttempts = 1, retryDelay = 10.millis, leaseDuration = 1.second).use { publisher =>
          publisher.wake *> eventually(work.snapshot)(snapshot =>
            List(malformedJob, malformedCandidate).forall(key =>
              snapshot.get(key.value).exists(_.failure.contains(EmbeddingWorkFailure.InvalidWorkKey))
            )
          ).void
        }
    } yield ()
  }

  private def pipelineResource(
      users: UserRepository,
      jobs: JobRepository,
      embeddings: EmbeddingService,
      model: String,
      queueSize: Int,
      parallelism: Int,
      retryAttempts: Int,
      retryDelay: FiniteDuration
  ) =
    Resource.eval(InMemoryEmbeddingWorkRepository.create).flatMap(work =>
      EmbeddingPipeline.resource(work, users, jobs, embeddings, model, queueSize, parallelism, retryAttempts, retryDelay, 1.second))

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

  private def successfulFind(jobs: JobRepository, id: Identifiers.JobId): IO[Option[Job]] =
    jobs.find(id).map(_.toOption.flatten)

  private final case class CountingEmbeddingService(calls: Ref[IO, Int]) extends EmbeddingService {
    override def embed(input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]] =
      calls.update(_ + 1).as(Right(EmbeddingVector(List(0.1f, 0.2f), "voyage-4-lite", 2)))
  }

  private object CountingEmbeddingService {
    val uncounted: EmbeddingService = new EmbeddingService {
      override def embed(input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]] =
        IO.raiseError(new AssertionError("malformed work must not reach the embedding provider"))
    }
  }

  private final case class FailOnceEmbeddingService(calls: Ref[IO, Int]) extends EmbeddingService {
    override def embed(input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]] =
      calls.modify {
        case 0 => 1 -> Left(EmbeddingError.ProviderUnavailable)
        case count => (count + 1) -> Right(EmbeddingVector(List(0.1f, 0.2f), "voyage-4-lite", 2))
      }
  }

  private final case class AlwaysFailEmbeddingService(calls: Ref[IO, Int]) extends EmbeddingService {
    override def embed(input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]] =
      calls.update(_ + 1).as(Left(EmbeddingError.ProviderUnavailable))
  }

  private final case class BlockingEmbeddingService(
      calls: Ref[IO, Int],
      started: Deferred[IO, Unit],
      release: Deferred[IO, Unit]
  ) extends EmbeddingService {
    override def embed(input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]] =
      calls.update(_ + 1) *> started.complete(()).void *> release.get.as(
        Right(EmbeddingVector(List(0.1f, 0.2f), "voyage-4-lite", 2))
      )
  }

  private final case class RecordingEmbeddingWrites(
      delegate: InMemoryJobs,
      writeResult: Deferred[IO, Either[RepositoryError, Unit]]
  ) extends JobRepository {
    override def find(id: Identifiers.JobId): IO[Either[RepositoryError, Option[Job]]] =
      delegate.find(id)

    override def findMany(ids: List[Identifiers.JobId]): IO[Either[RepositoryError, List[Job]]] =
      delegate.findMany(ids)

    override def findOpen(filter: JobSearchFilter, page: JobPageRequest): IO[Either[RepositoryError, List[Job]]] =
      delegate.findOpen(filter, page)

    override def findAll(page: JobPageRequest): IO[Either[RepositoryError, List[Job]]] =
      delegate.findAll(page)

    override def findByRecruiter(recruiterId: Identifiers.UserId, page: JobPageRequest): IO[Either[RepositoryError, List[Job]]] =
      delegate.findByRecruiter(recruiterId, page)

    override def create(job: Job, now: Instant): IO[Either[RepositoryError, Unit]] =
      delegate.create(job, now)

    override def createWithEvents(job: Job, now: Instant, events: List[OperationalEventEnvelope]): IO[Either[RepositoryError, Unit]] =
      delegate.createWithEvents(job, now, events)

    override def update(job: Job, now: Instant): IO[Either[RepositoryError, Job]] =
      delegate.update(job, now)

    override def updateWithEvents(job: Job, now: Instant, events: List[OperationalEventEnvelope]): IO[Either[RepositoryError, Job]] =
      delegate.updateWithEvents(job, now, events)

    override def updateEmbedding(
        id: Identifiers.JobId,
        embedding: EntityEmbedding
    ): IO[Either[RepositoryError, Unit]] =
      delegate.updateEmbedding(id, embedding).flatTap(result => writeResult.complete(result).void)
  }

  private final case class StoredWork(
      key: EmbeddingWorkKey,
      generation: Long,
      attempts: Int,
      state: String,
      availableAt: Instant,
      leaseToken: Option[String],
      leaseUntil: Option[Instant],
      failure: Option[EmbeddingWorkFailure]
  )

  private final class InMemoryEmbeddingWorkRepository(ref: Ref[IO, Map[String, StoredWork]]) extends EmbeddingWorkRepository {
    def snapshot: IO[Map[String, StoredWork]] = ref.get

    override def enqueue(key: EmbeddingWorkKey, now: Instant): IO[Either[RepositoryError, Unit]] =
      ref.update { current =>
        val next = current.get(key.value).fold(StoredWork(key, 1L, 0, "Ready", now, None, None, None)) { existing =>
          if (existing.state == "Processing") existing.copy(generation = existing.generation + 1L, attempts = 0)
          else existing.copy(generation = existing.generation + 1L, state = "Ready", availableAt = now, leaseToken = None, leaseUntil = None, failure = None)
        }
        current.updated(key.value, next)
      }.as(Right(()))

    override def claim(workerId: String, now: Instant, leaseUntil: Instant): IO[Either[RepositoryError, Option[ClaimedEmbeddingWork]]] =
      IO.randomUUID.flatMap { token =>
        ref.modify { current =>
          current.values.toList.sortBy(_.key.value).find(work =>
            ((work.state == "Ready" || work.state == "Retry") && !work.availableAt.isAfter(now)) ||
              (work.state == "Processing" && work.leaseUntil.exists(_.isBefore(now)))
          ).fold(current -> Right(None)) { found =>
            val claimed = found.copy(state = "Processing", leaseToken = Some(token.toString), leaseUntil = Some(leaseUntil))
            current.updated(found.key.value, claimed) -> Right(Some(ClaimedEmbeddingWork(found.key, found.generation, found.attempts, token.toString)))
          }
        }
      }

    override def complete(claim: ClaimedEmbeddingWork): IO[Either[RepositoryError, Unit]] =
      transition(claim)(_ => None)

    override def retry(claim: ClaimedEmbeddingWork, availableAt: Instant): IO[Either[RepositoryError, Unit]] =
      transition(claim)(_.copy(state = "Retry", attempts = claim.attempts + 1, availableAt = availableAt, leaseToken = None, leaseUntil = None).some)

    override def fail(claim: ClaimedEmbeddingWork, failure: EmbeddingWorkFailure, now: Instant): IO[Either[RepositoryError, Unit]] =
      transition(claim)(_.copy(state = "Failed", leaseToken = None, leaseUntil = None, failure = Some(failure)).some)

    private def transition(claim: ClaimedEmbeddingWork)(update: StoredWork => Option[StoredWork]): IO[Either[RepositoryError, Unit]] =
      ref.modify { current =>
        current.get(claim.key.value) match {
          case Some(work) if work.generation == claim.generation && work.state == "Processing" && work.leaseToken.contains(claim.leaseToken) =>
            current.updatedWith(claim.key.value)(_ => update(work)) -> Right(())
          case _ => current -> Left(RepositoryError.Conflict)
        }
      }
  }

  private object InMemoryEmbeddingWorkRepository {
    def create: IO[InMemoryEmbeddingWorkRepository] =
      Ref.of[IO, Map[String, StoredWork]](Map.empty).map(new InMemoryEmbeddingWorkRepository(_))
  }
}
