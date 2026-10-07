package com.example.graphQL.cats.service.search

import cats.effect.{IO, Resource}
import cats.effect.Deferred
import cats.effect.Ref
import cats.effect.std.Queue
import cats.syntax.all.*
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField, LogFields}
import com.example.graphQL.cats.service.ServiceFixtures.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.shared.crypto.SourceHash
import com.example.graphQL.cats.service.events.OperationalEventEnvelope
import com.example.graphQL.cats.domain.pagination.JobPageRequest
import com.example.graphQL.cats.service.search.JobSearchFilter
import java.util.UUID
import java.time.Instant
import munit.CatsEffectSuite
import scala.concurrent.duration.*

final class EmbeddingPipelineSpec extends CatsEffectSuite {
  test("embedding worker waits for database setup before claiming durable work") {
    for {
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map.empty)
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> openJob))
      calls <- Ref.of[IO, Int](0)
      setup <- Deferred[IO, Boolean]
      work <- InMemoryEmbeddingWorkRepository.create
      users = InMemoryUsers(usersRef)
      jobs = InMemoryJobs(jobsRef)
      embeddings = CountingEmbeddingService(calls)
      _ <- EmbeddingPipeline
        .resource(
          work,
          users,
          jobs,
          embeddings,
          "voyage-4-lite",
          queueSize = 8,
          parallelism = 1,
          retryAttempts = 3,
          retryDelay = 10.millis,
          leaseDuration = 1.second,
          workerReady = setup.get,
          diagnostics = com.example.graphQL.cats.service.Diagnostics.noop,
          durableRetryAttempts = 1
        )
        .use { publisher =>
          for {
            _ <- publisher.offer(EmbeddingWork.JobChanged(jobId))
            _ <- IO.sleep(100.millis)
            beforeSetup <- calls.get
            _ <- setup.complete(true)
            _ <- waitFor(calls.get.map(_ == 1))
          } yield assertEquals(beforeSetup, 0)
        }
    } yield ()
  }

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
          _ = assert(
            updated.flatMap(_.embedding).exists(_.meta.sourceHash == SourceHash.sha256(SearchableText.job(openJob)))
          )
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
      _ <- EmbeddingPipeline
        .resource(
          work,
          InMemoryUsers(usersRef),
          InMemoryJobs(jobsRef),
          AlwaysFailEmbeddingService(calls),
          "voyage-4-lite",
          8,
          1,
          retryAttempts = 3,
          retryDelay = 10.millis,
          leaseDuration = 1.second,
          diagnostics = com.example.graphQL.cats.service.Diagnostics.noop,
          durableRetryAttempts = 1
        )
        .use { publisher =>
          val key = DurableEmbeddingWorkPublisher.keyFor(EmbeddingWork.JobChanged(jobId))
          publisher.offer(EmbeddingWork.JobChanged(jobId)) *>
            eventually(work.snapshot)(
              _.get(key.value).exists(_.failure.contains(EmbeddingWorkFailure.RetryExhausted))
            ).void
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

  test("typed provider failures distinguish transient retries from invalid responses") {
    EmbeddingError.values.toList.traverse_ { failure =>
      for {
        usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map.empty)
        jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> openJob))
        calls <- Ref.of[IO, List[FiniteDuration]](Nil)
        work <- InMemoryEmbeddingWorkRepository.create
        completion <- Deferred[IO, Either[RepositoryError, Unit]]
        clock <- Ref.of[IO, Instant](now)
        provider = new EmbeddingService {
          override def embed(input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]] =
            IO.monotonic.flatMap(time => calls.update(_ :+ time)).as(Left(failure))
        }
        key = DurableEmbeddingWorkPublisher.keyFor(EmbeddingWork.JobChanged(jobId))
        _ <- successful(work.enqueue(key, now))
        _ <- controlledWorker(
          ObservableWork(work, completion),
          InMemoryUsers(usersRef),
          InMemoryJobs(jobsRef),
          provider,
          clock,
          retryDelay = 10.millis
        ).use(wakeups => wakeups.offer(()) *> completion.get)
        attempts <- calls.get
        snapshot <- work.snapshot
        finalJobs <- jobsRef.get
      } yield {
        assertEquals(attempts.size, if (failure == EmbeddingError.InvalidResponse) 1 else 3)
        attempts.zip(attempts.drop(1)).foreach { case (before, after) => assert(after - before >= 10.millis) }
        assertEquals(
          snapshot.get(key.value).flatMap(_.failure),
          Some(
            if (failure == EmbeddingError.InvalidResponse) EmbeddingWorkFailure.InvalidResponse
            else EmbeddingWorkFailure.RetryExhausted
          )
        )
        assertEquals(snapshot.get(key.value).map(_.attempts), Some(0))
        assertEquals(finalJobs, Map(jobId -> openJob))
      }
    }
  }

  test("repository fetch and guarded write failures retain required work") {
    RepositoryError.values.toList.traverse_ { failure =>
      List(true, false).traverse_ { failFetch =>
        for {
          usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map.empty)
          jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> openJob))
          calls <- Ref.of[IO, Int](0)
          fetches <- Ref.of[IO, Int](0)
          write <- Deferred[IO, Either[RepositoryError, Unit]]
          completion <- Deferred[IO, Either[RepositoryError, Unit]]
          clock <- Ref.of[IO, Instant](now)
          work <- InMemoryEmbeddingWorkRepository.create
          jobs = RecordingEmbeddingWrites(
            InMemoryJobs(jobsRef),
            write,
            fetchFailure = if (failFetch) Some(failure) else None,
            writeFailure = if (failFetch) None else Some(failure),
            onFind = fetches.update(_ + 1)
          )
          key = DurableEmbeddingWorkPublisher.keyFor(EmbeddingWork.JobChanged(jobId))
          _ <- successful(work.enqueue(key, now))
          _ <- controlledWorker(
            ObservableWork(work, completion),
            InMemoryUsers(usersRef),
            jobs,
            CountingEmbeddingService(calls),
            clock,
            retryDelay = 1.millis
          )
            .use(wakeups => wakeups.offer(()) *> completion.get)
          snapshot <- work.snapshot
          providerCalls <- calls.get
          fetchCount <- fetches.get
          finalJobs <- jobsRef.get
        } yield {
          val conflictRetry = !failFetch && failure == RepositoryError.Conflict
          assertEquals(fetchCount, if (conflictRetry) 2 else 3)
          assertEquals(providerCalls, if (failFetch) 0 else if (conflictRetry) 1 else 3)
          assertEquals(snapshot.get(key.value).map(_.state), Some(if (conflictRetry) "Retry" else "Failed"))
          assertEquals(
            snapshot.get(key.value).flatMap(_.failure),
            if (conflictRetry) None else Some(EmbeddingWorkFailure.RetryExhausted)
          )
          assertEquals(snapshot.get(key.value).map(_.attempts), Some(0))
          assert(snapshot.contains(key.value))
          assertEquals(finalJobs.get(jobId).flatMap(_.embedding), None)
        }
      }
    }
  }

  test("embedding worker reports a durable completion failure without marking the claim complete") {
    for {
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map.empty)
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> openJob))
      calls <- Ref.of[IO, Int](0)
      observed <- Ref.of[IO, List[LogEvent]](Nil)
      clock <- Ref.of[IO, Instant](now)
      work <- InMemoryEmbeddingWorkRepository.create
      key = DurableEmbeddingWorkPublisher.keyFor(EmbeddingWork.JobChanged(jobId))
      _ <- successful(work.enqueue(key, now))
      interruptedWork = new EmbeddingWorkRepository {
        override def enqueue(value: EmbeddingWorkKey, at: Instant): RepositoryIO[Unit] = work.enqueue(value, at)
        override def claim(
            workerId: String,
            at: Instant,
            leaseUntil: Instant
        ): RepositoryIO[Option[ClaimedEmbeddingWork]] =
          work.claim(workerId, at, leaseUntil)
        override def renew(claim: ClaimedEmbeddingWork, now: Instant, leaseUntil: Instant): RepositoryIO[Boolean] =
          work.renew(claim, now, leaseUntil)
        override def complete(claim: ClaimedEmbeddingWork): RepositoryIO[Unit] =
          RepositoryIO.fromEither(Left(RepositoryError.Unavailable))
        override def retry(
            claim: ClaimedEmbeddingWork,
            availableAt: Instant,
            chargeAttempt: Boolean = true
        ): RepositoryIO[Unit] =
          work.retry(claim, availableAt, chargeAttempt)
        override def fail(
            claim: ClaimedEmbeddingWork,
            failure: EmbeddingWorkFailure,
            at: Instant
        ): RepositoryIO[Unit] = work.fail(claim, failure, at)
      }
      diagnostics = new Diagnostics {
        override def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]): IO[Unit] =
          observed.update(_ :+ event)
      }
      _ <- controlledWorker(
        interruptedWork,
        InMemoryUsers(usersRef),
        InMemoryJobs(jobsRef),
        CountingEmbeddingService(calls),
        clock,
        retryDelay = 1.hour,
        diagnostics = diagnostics
      ).use(wakeups => wakeups.offer(()) *> waitFor(observed.get.map(_.contains(LogEvent.EmbeddingProcessingFailed))))
      events <- observed.get
      stored <- work.snapshot
    } yield {
      assertEquals(events, List(LogEvent.EmbeddingProcessingFailed))
      assertEquals(stored.get(key.value).map(_.state), Some("Processing"))
    }
  }

  test("embedding worker reports unexpected processing failure and records retry exhaustion") {
    val provider = new EmbeddingService {
      override def embed(input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]] =
        IO.raiseError(new IllegalStateException("private embedding detail"))
    }
    for {
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map.empty)
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> openJob))
      work <- InMemoryEmbeddingWorkRepository.create
      observed <- Ref.of[IO, List[(LogEvent, Map[LogField, String])]](Nil)
      diagnostics = new Diagnostics {
        override def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]): IO[Unit] =
          observed.update(_ :+ (event -> fields))
      }
      _ <- EmbeddingPipeline
        .resource(
          work,
          InMemoryUsers(usersRef),
          InMemoryJobs(jobsRef),
          provider,
          "voyage-4-lite",
          8,
          1,
          retryAttempts = 1,
          retryDelay = 10.millis,
          leaseDuration = 1.second,
          diagnostics = diagnostics,
          durableRetryAttempts = 1
        )
        .use { publisher =>
          val key = DurableEmbeddingWorkPublisher.keyFor(EmbeddingWork.JobChanged(jobId))
          publisher.offer(EmbeddingWork.JobChanged(jobId)) *>
            eventually(work.snapshot)(
              _.get(key.value).exists(_.failure.contains(EmbeddingWorkFailure.RetryExhausted))
            ).void
        }
      events <- observed.get
      snapshot <- work.snapshot
    } yield {
      val key = DurableEmbeddingWorkPublisher.keyFor(EmbeddingWork.JobChanged(jobId))
      assertEquals(snapshot.get(key.value).flatMap(_.failure), Some(EmbeddingWorkFailure.RetryExhausted))
      assertEquals(events.map(_._1), List(LogEvent.EmbeddingProcessingFailed))
      assertEquals(events.headOption.flatMap(_._2.get(LogField.ErrorType)), Some("java.lang.IllegalStateException"))
      assertEquals(events.headOption.map(_._2.keySet), Some(LogFields.failure(new IllegalStateException()).keySet))
      assert(!events.exists(_._2.values.exists(_.contains("private embedding detail"))))
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
          updated <- jobs
            .updateWithEvents(
              Versioned(openJob, 0L),
              openJob.copy(title = "Staff Scala Developer"),
              now,
              Nil,
              com.example.graphQL.cats.service.port.MutationWriteContext.directWrite
            )
            .value
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

  test("VHS-AC05 embedding pipeline ignores stale in-flight candidate writes after profile changes") {
    val changedProfile = CandidateProfile(Set("Scala", "Kafka"), Some("Updated profile"), None)
    for {
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map(candidateId -> candidate))
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map.empty)
      started <- Deferred[IO, Unit]
      release <- Deferred[IO, Unit]
      calls <- Ref.of[IO, Int](0)
      work <- InMemoryEmbeddingWorkRepository.create
      completion <- Deferred[IO, Either[RepositoryError, Unit]]
      users = InMemoryUsers(usersRef)
      embeddings = BlockingEmbeddingService(calls, started, release)
      _ <- pipelineResource(
        users,
        InMemoryJobs(jobsRef),
        embeddings,
        model = "voyage-4-lite",
        queueSize = 8,
        parallelism = 1,
        retryAttempts = 3,
        retryDelay = 10.millis,
        durableWork = Some(ObservableWork(work, completion))
      ).use { queue =>
        for {
          _ <- queue.offer(EmbeddingWork.CandidateProfileChanged(candidateId))
          _ <- started.get
          _ <- usersRef.update(
            _.updated(candidateId, candidate.copy(profile = Some(UserProfile.Candidate(changedProfile))))
          )
          _ <- release.complete(()).void
          _ <- completion.get
          finalUser <- eventually(users.find(candidateId).value)(
            _.toOption.flatten.exists(_.candidateProfile.contains(changedProfile))
          )
        } yield assertEquals(finalUser.toOption.flatten.flatMap(_.embedding), None)
      }
    } yield ()
  }

  test("job and candidate deletion during provider work cannot restore deleted entities") {
    List(
      EmbeddingWork.JobChanged(jobId) -> false,
      EmbeddingWork.CandidateProfileChanged(candidateId) -> false,
      EmbeddingWork.CandidateProfileChanged(candidateId) -> true
    ).traverse_ { case (changed, tombstone) =>
      for {
        usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map(candidateId -> candidate))
        jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> openJob))
        started <- Deferred[IO, Unit]
        release <- Deferred[IO, Unit]
        completion <- Deferred[IO, Either[RepositoryError, Unit]]
        calls <- Ref.of[IO, Int](0)
        clock <- Ref.of[IO, Instant](now)
        work <- InMemoryEmbeddingWorkRepository.create
        _ <- successful(work.enqueue(DurableEmbeddingWorkPublisher.keyFor(changed), now))
        _ <- controlledWorker(
          ObservableWork(work, completion),
          InMemoryUsers(usersRef),
          InMemoryJobs(jobsRef),
          BlockingEmbeddingService(calls, started, release),
          clock
        ).use { wakeups =>
          for {
            _ <- wakeups.offer(())
            _ <- started.get
            _ <- changed match {
              case EmbeddingWork.JobChanged(id)              => jobsRef.update(_ - id)
              case EmbeddingWork.CandidateProfileChanged(id) =>
                if (tombstone)
                  usersRef.update(
                    _.updated(
                      id,
                      candidate.copy(accountStatus = AccountStatus.Deleted, profile = None, embedding = None)
                    )
                  )
                else usersRef.update(_ - id)
            }
            _ <- release.complete(()).void
            result <- completion.get
            storedUsers <- usersRef.get
            storedJobs <- jobsRef.get
          } yield {
            assertEquals(result, Right(()))
            changed match {
              case EmbeddingWork.JobChanged(id)              => assert(!storedJobs.contains(id))
              case EmbeddingWork.CandidateProfileChanged(id) =>
                if (tombstone) {
                  assertEquals(storedUsers.get(id).map(_.accountStatus), Some(AccountStatus.Deleted))
                  assertEquals(storedUsers.get(id).flatMap(_.embedding), None)
                } else assert(!storedUsers.contains(id))
            }
          }
        }
        remaining <- work.snapshot
      } yield assertEquals(remaining, Map.empty[String, StoredWork])
    }
  }

  test("newer job profile and removed-profile work survive old claim completion and replay") {
    List(
      (EmbeddingWork.JobChanged(jobId), false),
      (EmbeddingWork.CandidateProfileChanged(candidateId), false),
      (EmbeddingWork.CandidateProfileChanged(candidateId), true)
    ).traverse_ { case (changed, removeProfile) =>
      for {
        usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map(candidateId -> candidate))
        jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> openJob))
        started <- Deferred[IO, Unit]
        release <- Deferred[IO, Unit]
        oldCompletion <- Deferred[IO, Either[RepositoryError, Unit]]
        newCompletion <- Deferred[IO, Either[RepositoryError, Unit]]
        calls <- Ref.of[IO, Int](0)
        clock <- Ref.of[IO, Instant](now)
        work <- InMemoryEmbeddingWorkRepository.create
        key = DurableEmbeddingWorkPublisher.keyFor(changed)
        changedProfile = CandidateProfile(Set("Scala", "Kafka"), Some("Updated summary"), None)
        _ <- successful(work.enqueue(key, now))
        _ <- controlledWorker(
          ObservableWork(work, oldCompletion),
          InMemoryUsers(usersRef),
          InMemoryJobs(jobsRef),
          BlockingEmbeddingService(calls, started, release),
          clock
        ).use { wakeups =>
          for {
            _ <- wakeups.offer(())
            _ <- started.get
            _ <- changed match {
              case EmbeddingWork.JobChanged(id) => jobsRef.update(_.updated(id, openJob.copy(title = "Staff Engineer")))
              case EmbeddingWork.CandidateProfileChanged(id) =>
                usersRef.update(
                  _.updated(
                    id,
                    candidate.copy(profile = if (removeProfile) None else Some(UserProfile.Candidate(changedProfile)))
                  )
                )
            }
            _ <- successful(work.enqueue(key, now.plusSeconds(1)))
            _ <- release.complete(()).void
            completion <- oldCompletion.get
          } yield assertEquals(completion, Left(RepositoryError.Conflict))
        }
        retained <- work.snapshot
        staleUsers <- usersRef.get
        staleJobs <- jobsRef.get
        _ <- clock.set(now.plusSeconds(31))
        _ <- controlledWorker(
          ObservableWork(work, newCompletion),
          InMemoryUsers(usersRef),
          InMemoryJobs(jobsRef),
          CountingEmbeddingService(calls),
          clock
        ).use(wakeups => wakeups.offer(()) *> newCompletion.get)
        replayed <- work.snapshot
        storedUsers <- usersRef.get
        storedJobs <- jobsRef.get
        count <- calls.get
      } yield {
        assertEquals(retained.get(key.value).map(_.generation), Some(2L))
        assertEquals(staleUsers.get(candidateId).flatMap(_.embedding), None)
        assertEquals(staleJobs.get(jobId).flatMap(_.embedding), None)
        assertEquals(replayed.get(key.value), None)
        assertEquals(count, if (removeProfile) 1 else 2)
        changed match {
          case EmbeddingWork.JobChanged(id) =>
            assertEquals(
              storedJobs.get(id).flatMap(_.embedding).map(_.meta.sourceHash),
              Some(SourceHash.sha256(SearchableText.job(openJob.copy(title = "Staff Engineer"))))
            )
          case EmbeddingWork.CandidateProfileChanged(id) =>
            assertEquals(
              storedUsers.get(id).flatMap(_.embedding).map(_.meta.sourceHash),
              if (removeProfile) None else Some(SourceHash.sha256(SearchableText.candidate(changedProfile)))
            )
        }
      }
    }
  }

  test("document metadata keeps configured model and injected completion time") {
    val provider = new EmbeddingService {
      override def embed(input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]] = {
        assertEquals(input.inputType, EmbeddingInputType.Document)
        IO.pure(Right(EmbeddingVector(List(0.1f, 0.2f), "provider-reported-model", 2)))
      }
    }
    for {
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map.empty)
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> openJob))
      completion <- Deferred[IO, Either[RepositoryError, Unit]]
      clock <- Ref.of[IO, Instant](now.plusSeconds(5))
      work <- InMemoryEmbeddingWorkRepository.create
      _ <- successful(work.enqueue(DurableEmbeddingWorkPublisher.keyFor(EmbeddingWork.JobChanged(jobId)), now))
      _ <- controlledWorker(
        ObservableWork(work, completion),
        InMemoryUsers(usersRef),
        InMemoryJobs(jobsRef),
        provider,
        clock
      ).use(wakeups => wakeups.offer(()) *> completion.get)
      stored <- jobsRef.get
    } yield assertEquals(
      stored.get(jobId).flatMap(_.embedding).map(_.meta),
      Some(EmbeddingMeta("voyage-4-lite", SourceHash.sha256(SearchableText.job(openJob)), now.plusSeconds(5)))
    )
  }

  test("missing documents absent profiles current sources and oversized sources avoid provider calls") {
    val oversized = openJob.copy(description = "x" * SearchableText.DocumentMaxChars)
    val currentOversized = oversized.copy(embedding =
      Some(
        EntityEmbedding(
          List(0.1f),
          EmbeddingMeta("voyage-4-lite", SourceHash.sha256(SearchableText.job(oversized)), now)
        )
      )
    )
    val currentCandidate = candidate.copy(embedding =
      candidate.candidateProfile.map(profile =>
        EntityEmbedding(
          List(0.1f),
          EmbeddingMeta("voyage-4-lite", SourceHash.sha256(SearchableText.candidate(profile)), now)
        )
      )
    )
    val cases = List(
      (EmbeddingWork.JobChanged(jobId), Option.empty[Job], Option.empty[User], Option.empty[EmbeddingWorkFailure]),
      (EmbeddingWork.CandidateProfileChanged(candidateId), None, None, None),
      (EmbeddingWork.CandidateProfileChanged(candidateId), None, Some(candidate.copy(profile = None)), None),
      (EmbeddingWork.CandidateProfileChanged(candidateId), None, Some(currentCandidate), None),
      (EmbeddingWork.JobChanged(jobId), Some(currentOversized), None, None),
      (EmbeddingWork.JobChanged(jobId), Some(oversized), None, Some(EmbeddingWorkFailure.DocumentTooLarge))
    )
    cases.traverse_ { case (changed, job, user, failure) =>
      for {
        usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](user.map(value => candidateId -> value).toMap)
        jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](job.map(value => jobId -> value).toMap)
        completion <- Deferred[IO, Either[RepositoryError, Unit]]
        clock <- Ref.of[IO, Instant](now)
        work <- InMemoryEmbeddingWorkRepository.create
        _ <- successful(work.enqueue(DurableEmbeddingWorkPublisher.keyFor(changed), now))
        _ <- controlledWorker(
          ObservableWork(work, completion),
          InMemoryUsers(usersRef),
          InMemoryJobs(jobsRef),
          CountingEmbeddingService.uncounted,
          clock
        ).use(wakeups => wakeups.offer(()) *> completion.get)
        snapshot <- work.snapshot
        storedJobs <- jobsRef.get
        storedUsers <- usersRef.get
      } yield {
        assertEquals(snapshot.get(DurableEmbeddingWorkPublisher.keyFor(changed).value).flatMap(_.failure), failure)
        assertEquals(storedJobs, job.map(value => jobId -> value).toMap)
        assertEquals(storedUsers, user.map(value => candidateId -> value).toMap)
      }
    }
  }

  test("cancelled provider calls keep unpersisted claims replayable for both entity types") {
    List(EmbeddingWork.JobChanged(jobId), EmbeddingWork.CandidateProfileChanged(candidateId)).traverse_ { changed =>
      for {
        usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map(candidateId -> candidate))
        jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> openJob))
        started <- Deferred[IO, Unit]
        release <- Deferred[IO, Unit]
        completion <- Deferred[IO, Either[RepositoryError, Unit]]
        calls <- Ref.of[IO, Int](0)
        clock <- Ref.of[IO, Instant](now)
        work <- InMemoryEmbeddingWorkRepository.create
        key = DurableEmbeddingWorkPublisher.keyFor(changed)
        _ <- successful(work.enqueue(key, now))
        _ <- controlledWorker(
          ObservableWork(work, completion),
          InMemoryUsers(usersRef),
          InMemoryJobs(jobsRef),
          BlockingEmbeddingService(calls, started, release),
          clock
        ).use(wakeups => wakeups.offer(()) *> started.get)
        interrupted <- work.snapshot
        unfinished <- completion.tryGet
        beforeUsers <- usersRef.get
        beforeJobs <- jobsRef.get
        _ <- clock.set(now.plusSeconds(31))
        _ <- controlledWorker(
          ObservableWork(work, completion),
          InMemoryUsers(usersRef),
          InMemoryJobs(jobsRef),
          CountingEmbeddingService(calls),
          clock
        ).use(wakeups => wakeups.offer(()) *> completion.get)
        replayed <- work.snapshot
        count <- calls.get
        afterUsers <- usersRef.get
        afterJobs <- jobsRef.get
      } yield {
        assertEquals(interrupted.get(key.value).map(_.state), Some("Processing"))
        assertEquals(unfinished, None)
        assertEquals(beforeUsers.get(candidateId).flatMap(_.embedding), None)
        assertEquals(beforeJobs.get(jobId).flatMap(_.embedding), None)
        assertEquals(replayed.get(key.value), None)
        assertEquals(count, 2)
        changed match {
          case EmbeddingWork.JobChanged(id)              => assert(afterJobs.get(id).flatMap(_.embedding).nonEmpty)
          case EmbeddingWork.CandidateProfileChanged(id) => assert(afterUsers.get(id).flatMap(_.embedding).nonEmpty)
        }
      }
    }
  }

  test("acknowledged embedding writes replay after interruption without another provider call") {
    List(EmbeddingWork.JobChanged(jobId), EmbeddingWork.CandidateProfileChanged(candidateId)).traverse_ { changed =>
      for {
        usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map(candidateId -> candidate))
        jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> openJob))
        beforeCompletion <- Deferred[IO, Unit]
        completion <- Deferred[IO, Either[RepositoryError, Unit]]
        calls <- Ref.of[IO, Int](0)
        clock <- Ref.of[IO, Instant](now)
        work <- InMemoryEmbeddingWorkRepository.create
        key = DurableEmbeddingWorkPublisher.keyFor(changed)
        interruptedWork = ObservableWork(work, completion, beforeCompletion.complete(()).void *> IO.never[Unit])
        _ <- successful(work.enqueue(key, now))
        _ <- controlledWorker(
          interruptedWork,
          InMemoryUsers(usersRef),
          InMemoryJobs(jobsRef),
          CountingEmbeddingService(calls),
          clock
        ).use(wakeups => wakeups.offer(()) *> beforeCompletion.get)
        interrupted <- work.snapshot
        _ <- clock.set(now.plusSeconds(31))
        _ <- controlledWorker(
          ObservableWork(work, completion),
          InMemoryUsers(usersRef),
          InMemoryJobs(jobsRef),
          CountingEmbeddingService.uncounted,
          clock
        ).use(wakeups => wakeups.offer(()) *> completion.get)
        replayed <- work.snapshot
        count <- calls.get
        storedUsers <- usersRef.get
        storedJobs <- jobsRef.get
      } yield {
        assertEquals(interrupted.get(key.value).map(_.state), Some("Processing"))
        assertEquals(replayed.get(key.value), None)
        assertEquals(count, 1)
        val metadata = changed match {
          case EmbeddingWork.JobChanged(id)              => storedJobs.get(id).flatMap(_.embedding).map(_.meta)
          case EmbeddingWork.CandidateProfileChanged(id) => storedUsers.get(id).flatMap(_.embedding).map(_.meta)
        }
        assertEquals(metadata.map(_.updatedAt), Some(now))
      }
    }
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
          assert(
            updated.flatMap(_.embedding).exists(_.meta.sourceHash == SourceHash.sha256(SearchableText.job(otherJob)))
          )
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
      _ <- repository.enqueue(work, first).value
      claim <- repository.claim("worker-a", first, first.plusSeconds(30)).value
      claimed <- IO.fromOption(claim.toOption.flatten)(new AssertionError("expected work claim"))
      _ <- repository.enqueue(work, second).value
      completed <- repository.complete(claimed).value
      blockedClaim <- repository.claim("worker-b", second, second.plusSeconds(30)).value
      nextClaim <- repository.claim("worker-b", first.plusSeconds(31), first.plusSeconds(61)).value
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
      _ <- EmbeddingPipeline
        .resource(
          work,
          InMemoryUsers(usersRef),
          InMemoryJobs(jobsRef),
          embeddings,
          "voyage-4-lite",
          8,
          1,
          retryAttempts = 1,
          retryDelay = 10.millis,
          leaseDuration = 1.second,
          diagnostics = com.example.graphQL.cats.service.Diagnostics.noop,
          durableRetryAttempts = 1
        )
        .use { publisher =>
          publisher.offer(EmbeddingWork.JobChanged(jobId)) *> publisher.offer(EmbeddingWork.JobChanged(otherJobId)) *>
            eventually(jobsRef.get.map(_.get(otherJobId).flatMap(_.embedding).nonEmpty))(identity).void
        }
      snapshot <- work.snapshot
    } yield {
      assertEquals(
        snapshot(DurableEmbeddingWorkPublisher.keyFor(EmbeddingWork.JobChanged(jobId)).value).failure,
        Some(EmbeddingWorkFailure.RetryExhausted)
      )
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
      _ <- work.enqueue(malformedJob, now).value
      _ <- work.enqueue(malformedCandidate, now).value
      _ <- EmbeddingPipeline
        .resource(
          work,
          InMemoryUsers(usersRef),
          InMemoryJobs(jobsRef),
          CountingEmbeddingService.uncounted,
          "voyage-4-lite",
          8,
          1,
          retryAttempts = 1,
          retryDelay = 10.millis,
          leaseDuration = 1.second,
          diagnostics = com.example.graphQL.cats.service.Diagnostics.noop,
          durableRetryAttempts = 1
        )
        .use { publisher =>
          publisher.wake *> eventually(work.snapshot)(snapshot =>
            List(malformedJob, malformedCandidate)
              .forall(key => snapshot.get(key.value).exists(_.failure.contains(EmbeddingWorkFailure.InvalidWorkKey)))
          ).void
        }
    } yield ()
  }

  test("more than eight revision-only conflicts preserve provider failure budget and eventually embed current source") {
    for {
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map(candidateId -> candidate))
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> openJob))
      calls <- Ref.of[IO, Int](0)
      durableAttempts <- Ref.of[IO, List[Int]](Nil)
      work <- InMemoryEmbeddingWorkRepository.create
      key = DurableEmbeddingWorkPublisher.keyFor(EmbeddingWork.JobChanged(jobId))
      provider = new EmbeddingService {
        def embed(input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]] =
          calls.updateAndGet(_ + 1).flatMap { count =>
            work.snapshot.flatMap(snapshot =>
              snapshot.get(key.value).traverse_(stored => durableAttempts.update(_ :+ stored.attempts))
            ) *>
              (if (count <= 9) jobsRef.update(_.updated(jobId, openJob.copy(updatedAt = now.plusSeconds(count.toLong))))
               else IO.unit).as(Right(EmbeddingVector(List(0.1f, 0.2f), "voyage-4-lite", 2)))
          }
      }
      _ <- EmbeddingPipeline
        .resource(
          work,
          InMemoryUsers(usersRef),
          InMemoryJobs(jobsRef),
          provider,
          "voyage-4-lite",
          8,
          1,
          3,
          10.millis,
          1.second,
          diagnostics = Diagnostics.noop,
          durableRetryAttempts = 8,
          durableRetryBase = 10.millis,
          durableRetryCap = 20.millis
        )
        .use { publisher =>
          publisher.offer(EmbeddingWork.JobChanged(jobId)) *>
            eventually(jobsRef.get)(_.get(jobId).flatMap(_.embedding).nonEmpty).void *>
            eventually(work.snapshot)(_.isEmpty).void
        }
      count <- calls.get
      observedAttempts <- durableAttempts.get
      remaining <- work.snapshot
      stored <- jobsRef.get
    } yield {
      assertEquals(count, 10)
      assertEquals(observedAttempts, List.fill(10)(0))
      assertEquals(remaining, Map.empty[String, StoredWork])
      assertEquals(stored(jobId).embedding.map(_.meta.sourceHash), Some(SourceHash.sha256(SearchableText.job(openJob))))
    }
  }

  test("transient provider exhaustion is retried durably and converges without another mutation") {
    for {
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map.empty)
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> openJob))
      calls <- Ref.of[IO, Int](0)
      work <- InMemoryEmbeddingWorkRepository.create
      provider = new EmbeddingService {
        def embed(input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]] = calls
          .updateAndGet(_ + 1)
          .map(count =>
            if (count <= 3) Left(EmbeddingError.ProviderUnavailable)
            else Right(EmbeddingVector(List(0.1f, 0.2f), "voyage-4-lite", 2))
          )
      }
      _ <- EmbeddingPipeline
        .resource(
          work,
          InMemoryUsers(usersRef),
          InMemoryJobs(jobsRef),
          provider,
          "voyage-4-lite",
          8,
          1,
          3,
          10.millis,
          1.second,
          diagnostics = Diagnostics.noop,
          durableRetryAttempts = 2,
          durableRetryBase = 10.millis,
          durableRetryCap = 20.millis
        )
        .use { publisher =>
          publisher.offer(EmbeddingWork.JobChanged(jobId)) *>
            eventually(jobsRef.get)(_.get(jobId).flatMap(_.embedding).nonEmpty).void *>
            eventually(work.snapshot)(_.isEmpty).void
        }
      count <- calls.get
    } yield assertEquals(count, 4)
  }

  test("provider work renews its lease while active and abandoned claims remain recoverable") {
    for {
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map.empty)
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> openJob))
      started <- Deferred[IO, Unit]
      release <- Deferred[IO, Unit]
      calls <- Ref.of[IO, Int](0)
      work <- InMemoryEmbeddingWorkRepository.create
      _ <- EmbeddingPipeline
        .resource(
          work,
          InMemoryUsers(usersRef),
          InMemoryJobs(jobsRef),
          BlockingEmbeddingService(calls, started, release),
          "voyage-4-lite",
          8,
          1,
          1,
          10.millis,
          150.millis,
          diagnostics = Diagnostics.noop
        )
        .use { publisher =>
          for {
            _ <- publisher.offer(EmbeddingWork.JobChanged(jobId))
            _ <- started.get
            _ <- IO.sleep(300.millis)
            at <- IO.realTimeInstant
            competing <- successful(work.claim("other", at, at.plusSeconds(1)))
            _ <- release.complete(()).void
            _ <- eventually(work.snapshot)(_.isEmpty)
          } yield assertEquals(competing, None)
        }
    } yield ()
  }

  test("unexpected claim and completion failures are observed and restart an owned worker") {
    List(true, false).traverse_ { failClaim =>
      for {
        usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map.empty)
        jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> openJob))
        work <- InMemoryEmbeddingWorkRepository.create
        first <- Ref.of[IO, Boolean](true)
        health <- Ref.of[IO, List[Boolean]](Nil)
        observed <- Ref.of[IO, List[LogEvent]](Nil)
        adapter = new EmbeddingWorkRepository {
          def enqueue(key: EmbeddingWorkKey, at: Instant) = work.enqueue(key, at)
          def claim(worker: String, at: Instant, until: Instant) =
            RepositoryIO.fromIOEither((if (failClaim) first.getAndSet(false) else IO.pure(false)).flatMap {
              case true  => IO.raiseError(new IllegalStateException("unexpected claim failure"))
              case false => work.claim(worker, at, until).value
            })
          def renew(claim: ClaimedEmbeddingWork, at: Instant, until: Instant) = work.renew(claim, at, until)
          def complete(claim: ClaimedEmbeddingWork) =
            RepositoryIO.fromIOEither((if (!failClaim) first.getAndSet(false) else IO.pure(false)).flatMap {
              case true  => IO.raiseError(new IllegalStateException("unexpected completion failure"))
              case false => work.complete(claim).value
            })
          def retry(claim: ClaimedEmbeddingWork, at: Instant, chargeAttempt: Boolean = true) =
            work.retry(claim, at, chargeAttempt)
          def fail(claim: ClaimedEmbeddingWork, failure: EmbeddingWorkFailure, at: Instant) =
            work.fail(claim, failure, at)
        }
        diagnostics = new Diagnostics {
          def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]) =
            observed.update(_ :+ event)
        }
        _ <- EmbeddingPipeline
          .resource(
            adapter,
            InMemoryUsers(usersRef),
            InMemoryJobs(jobsRef),
            new EmbeddingService {
              def embed(input: EmbeddingInput) = IO.pure(Right(EmbeddingVector(List(0.1f, 0.2f), "voyage-4-lite", 2)))
            },
            "voyage-4-lite",
            8,
            1,
            1,
            10.millis,
            100.millis,
            diagnostics = diagnostics,
            workerRestartDelay = 10.millis,
            workerHealth = value => health.update(_ :+ value)
          )
          .use { publisher =>
            publisher.offer(EmbeddingWork.JobChanged(jobId)) *>
              eventually(jobsRef.get)(_.get(jobId).flatMap(_.embedding).nonEmpty).void *>
              eventually(work.snapshot)(_.isEmpty).void
          }
        changes <- health.get
        events <- observed.get
      } yield {
        assert(changes.contains(false))
        assert(changes.dropWhile(_ != false).contains(true))
        assert(events.contains(LogEvent.EmbeddingProcessingFailed))
      }
    }
  }

  test("embedding backoff saturates without overflow") {
    assertEquals(EmbeddingRecoveryPolicy.backoffMillis(1L, 1000L, 300000L), 1000L)
    assertEquals(EmbeddingRecoveryPolicy.backoffMillis(2L, 1000L, 300000L), 2000L)
    assertEquals(EmbeddingRecoveryPolicy.backoffMillis(Long.MaxValue, 1000L, 300000L), 300000L)
  }

  private def pipelineResource(
      users: UserRepository,
      jobs: JobRepository,
      embeddings: EmbeddingService,
      model: String,
      queueSize: Int,
      parallelism: Int,
      retryAttempts: Int,
      retryDelay: FiniteDuration,
      durableWork: Option[EmbeddingWorkRepository] = None
  ) =
    Resource
      .eval(InMemoryEmbeddingWorkRepository.create)
      .flatMap(work =>
        EmbeddingPipeline
          .resource(
            durableWork.getOrElse(work),
            users,
            jobs,
            embeddings,
            model,
            queueSize,
            parallelism,
            retryAttempts,
            retryDelay,
            1.second,
            diagnostics = com.example.graphQL.cats.service.Diagnostics.noop,
            durableRetryAttempts = 1
          )
      )

  private def successful[A](result: RepositoryIO[A]): IO[A] =
    result.value.flatMap(_.fold(error => IO.raiseError(new AssertionError(s"Repository failure: $error")), IO.pure))

  private def controlledWorker(
      work: EmbeddingWorkRepository,
      users: UserRepository,
      jobs: JobRepository,
      provider: EmbeddingService,
      clock: Ref[IO, Instant],
      retryDelay: FiniteDuration = 1.hour,
      diagnostics: Diagnostics = Diagnostics.noop
  ): Resource[IO, Queue[IO, Unit]] =
    Resource.eval(Queue.bounded[IO, Unit](8)).flatMap { wakeups =>
      val worker = new EmbeddingPipeline(
        wakeups,
        work,
        users,
        jobs,
        provider,
        "voyage-4-lite",
        1,
        3,
        retryDelay,
        30.seconds,
        clock.get,
        "fixture-worker",
        diagnostics
      )
      Resource.make(worker.stream.compile.drain.start)(_.cancel).as(wakeups)
    }

  private final case class ObservableWork(
      delegate: EmbeddingWorkRepository,
      completion: Deferred[IO, Either[RepositoryError, Unit]],
      beforeCompletion: IO[Unit] = IO.unit
  ) extends EmbeddingWorkRepository {
    override def enqueue(key: EmbeddingWorkKey, now: Instant): RepositoryIO[Unit] = delegate.enqueue(key, now)
    override def claim(
        workerId: String,
        now: Instant,
        leaseUntil: Instant
    ): RepositoryIO[Option[ClaimedEmbeddingWork]] =
      delegate.claim(workerId, now, leaseUntil)
    override def renew(claim: ClaimedEmbeddingWork, now: Instant, leaseUntil: Instant): RepositoryIO[Boolean] =
      delegate.renew(claim, now, leaseUntil)
    override def complete(claim: ClaimedEmbeddingWork): RepositoryIO[Unit] = RepositoryIO.fromIOEither(
      beforeCompletion *> delegate.complete(claim).value.flatTap(result => completion.complete(result).void)
    )
    override def retry(
        claim: ClaimedEmbeddingWork,
        availableAt: Instant,
        chargeAttempt: Boolean = true
    ): RepositoryIO[Unit] =
      RepositoryIO.fromIOEither(
        delegate.retry(claim, availableAt, chargeAttempt).value.flatTap(result => completion.complete(result).void)
      )
    override def fail(claim: ClaimedEmbeddingWork, failure: EmbeddingWorkFailure, now: Instant): RepositoryIO[Unit] =
      RepositoryIO.fromIOEither(
        delegate.fail(claim, failure, now).value.flatTap(result => completion.complete(result).void)
      )
  }

  private def waitFor(done: IO[Boolean], remaining: Int = 20): IO[Unit] =
    done.flatMap {
      case true                   => IO.unit
      case false if remaining > 0 => IO.sleep(50.millis) *> waitFor(done, remaining - 1)
      case false                  => IO.raiseError(new AssertionError("condition was not met"))
    }

  private def eventually[A](effect: IO[A])(accepted: A => Boolean, remaining: Int = 20): IO[A] =
    effect.flatMap { value =>
      if (accepted(value)) IO.pure(value)
      else if (remaining > 0) IO.sleep(50.millis) *> eventually(effect)(accepted, remaining - 1)
      else IO.raiseError(new AssertionError("condition was not met"))
    }

  private def successfulFind(jobs: JobRepository, id: Identifiers.JobId): IO[Option[Job]] =
    jobs.find(id).value.map(_.toOption.flatten)

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
        case 0     => 1 -> Left(EmbeddingError.ProviderUnavailable)
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
      writeResult: Deferred[IO, Either[RepositoryError, Unit]],
      fetchFailure: Option[RepositoryError] = None,
      writeFailure: Option[RepositoryError] = None,
      onFind: IO[Unit] = IO.unit
  ) extends JobRepository {
    override def relatedJobs(
        scope: com.example.graphQL.cats.service.read.HiringReadScope,
        keys: List[com.example.graphQL.cats.service.read.JobRelationKey]
    ) = delegate.relatedJobs(scope, keys)

    override def find(id: Identifiers.JobId): RepositoryIO[Option[Job]] = delegate.find(id)

    override def findVersioned(
        id: Identifiers.JobId
    ): RepositoryIO[Option[Versioned[Job]]] = RepositoryIO.lift(onFind) *>
      fetchFailure.fold(delegate.findVersioned(id))(error => RepositoryIO.fromEither(Left(error)))

    override def findMany(ids: List[Identifiers.JobId]): RepositoryIO[List[Job]] = delegate.findMany(ids)

    override def findOpen(filter: JobSearchFilter, page: JobPageRequest): RepositoryIO[List[Job]] =
      delegate.findOpen(filter, page)

    override def nearbyJobs(
        scope: com.example.graphQL.cats.service.read.HiringReadScope,
        query: NearbyJobsQuery,
        limit: Int
    ) = delegate.nearbyJobs(scope, query, limit)
    override def jobDiscoveryFacets(
        scope: com.example.graphQL.cats.service.read.HiringReadScope,
        query: JobFacetQuery
    ) = delegate.jobDiscoveryFacets(scope, query)

    override def findAll(page: JobPageRequest): RepositoryIO[List[Job]] = delegate.findAll(page)

    override def findByRecruiter(
        recruiterId: Identifiers.UserId,
        page: JobPageRequest
    ): RepositoryIO[List[Job]] = delegate.findByRecruiter(recruiterId, page)

    override def createWithEvents(
        job: Job,
        now: Instant,
        events: List[OperationalEventEnvelope],
        context: MutationWriteContext
    ): RepositoryIO[Unit] = delegate.createWithEvents(job, now, events, context)

    override def updateWithEvents(
        expected: Versioned[Job],
        replacement: Job,
        now: Instant,
        events: List[OperationalEventEnvelope],
        context: MutationWriteContext
    ): RepositoryIO[Versioned[Job]] = delegate.updateWithEvents(expected, replacement, now, events, context)

    override def updateEmbedding(
        id: Identifiers.JobId,
        embedding: EntityEmbedding
    ): RepositoryIO[Unit] = com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(
      writeFailure
        .fold(delegate.updateEmbedding(id, embedding))(error => RepositoryIO.fromEither(Left(error)))
        .value
        .flatTap(result => writeResult.complete(result).void)
    )

    override def updateEmbedding(
        observed: Versioned[Job],
        embedding: EntityEmbedding
    ): RepositoryIO[Unit] = com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(
      writeFailure
        .fold(delegate.updateEmbedding(observed, embedding))(error => RepositoryIO.fromEither(Left(error)))
        .value
        .flatTap(result => writeResult.complete(result).void)
    )
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

  private final class InMemoryEmbeddingWorkRepository(ref: Ref[IO, Map[String, StoredWork]])
      extends EmbeddingWorkRepository {
    def snapshot: IO[Map[String, StoredWork]] = ref.get

    override def enqueue(key: EmbeddingWorkKey, now: Instant): RepositoryIO[Unit] =
      com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(
        ref
          .update { current =>
            val next = current.get(key.value).fold(StoredWork(key, 1L, 0, "Ready", now, None, None, None)) { existing =>
              if (existing.state == "Processing") existing.copy(generation = existing.generation + 1L, attempts = 0)
              else
                existing.copy(
                  generation = existing.generation + 1L,
                  state = "Ready",
                  availableAt = now,
                  leaseToken = None,
                  leaseUntil = None,
                  failure = None
                )
            }
            current.updated(key.value, next)
          }
          .as(Right(()))
      )

    override def claim(
        workerId: String,
        now: Instant,
        leaseUntil: Instant
    ): RepositoryIO[Option[ClaimedEmbeddingWork]] =
      com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(IO.randomUUID.flatMap { token =>
        ref.modify { current =>
          current.values.toList
            .sortBy(_.key.value)
            .find(work =>
              ((work.state == "Ready" || work.state == "Retry") && !work.availableAt.isAfter(now)) ||
                (work.state == "Processing" && work.leaseUntil.exists(_.isBefore(now)))
            )
            .fold(current -> Right(None)) { found =>
              val claimed =
                found.copy(state = "Processing", leaseToken = Some(token.toString), leaseUntil = Some(leaseUntil))
              current.updated(found.key.value, claimed) -> Right(
                Some(ClaimedEmbeddingWork(found.key, found.generation, found.attempts, token.toString))
              )
            }
        }
      })

    override def renew(claim: ClaimedEmbeddingWork, now: Instant, leaseUntil: Instant): RepositoryIO[Boolean] =
      RepositoryIO.fromIOEither(ref.modify { current =>
        current.get(claim.key.value) match {
          case Some(found)
              if found.generation == claim.generation && found.state == "Processing" &&
                found.leaseToken.contains(claim.leaseToken) && found.leaseUntil.exists(_.isAfter(now)) =>
            current.updated(claim.key.value, found.copy(leaseUntil = Some(leaseUntil))) -> Right(true)
          case _ => current -> Right(false)
        }
      })

    override def complete(claim: ClaimedEmbeddingWork): RepositoryIO[Unit] =
      com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(transition(claim)(_ => None))

    override def retry(
        claim: ClaimedEmbeddingWork,
        availableAt: Instant,
        chargeAttempt: Boolean = true
    ): RepositoryIO[Unit] =
      com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(
        transition(claim)(
          _.copy(
            state = "Retry",
            attempts = claim.attempts + (if (chargeAttempt) 1 else 0),
            availableAt = availableAt,
            leaseToken = None,
            leaseUntil = None
          ).some
        )
      )

    override def fail(
        claim: ClaimedEmbeddingWork,
        failure: EmbeddingWorkFailure,
        now: Instant
    ): RepositoryIO[Unit] = com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(
      transition(claim)(_.copy(state = "Failed", leaseToken = None, leaseUntil = None, failure = Some(failure)).some)
    )

    private def transition(
        claim: ClaimedEmbeddingWork
    )(update: StoredWork => Option[StoredWork]): IO[Either[RepositoryError, Unit]] =
      ref.modify { current =>
        current.get(claim.key.value) match {
          case Some(work)
              if work.generation == claim.generation && work.state == "Processing" && work.leaseToken.contains(
                claim.leaseToken
              ) =>
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
