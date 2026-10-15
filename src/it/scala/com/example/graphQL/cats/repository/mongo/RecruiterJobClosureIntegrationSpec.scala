package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.AccountValueFixtures.email
import com.example.graphQL.cats.repository.mongo.MongoRepositoryTestSupport.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.*
import org.bson.Document
import com.example.graphQL.cats.service.{Diagnostics, RepositoryError}
import com.example.graphQL.cats.service.events.OperationalEventType
import com.example.graphQL.cats.service.auth.{RecruiterJobClosureSchedule, RecruiterJobClosureWorker}
import com.example.graphQL.cats.service.port.*
import com.mongodb.client.model.Filters
import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*

/** A deleted recruiter's open jobs are closed in bounded transactions: the deletion closes at most one page and a
  * durable, resumable follow-up closes the rest with exactly one `JOB_CLOSED` event per job.
  */
final class RecruiterJobClosureIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 10.minutes

  private val created = Instant.parse("2026-10-05T12:00:00Z")
  private val deletedAt = created.plusSeconds(60)

  private def pageSize(value: Int): JobClosurePageSize =
    JobClosurePageSize.from(value).getOrElse(fail(s"invalid page size $value"))

  private def recruiter(): User =
    User(
      UserId(UUID.randomUUID()),
      Some(email(s"${UUID.randomUUID()}@example.com")),
      "Closure recruiter",
      UserRole.Recruiter,
      Some(UserProfile.Recruiter(RecruiterProfile("Closure organization", None))),
      created
    )

  private def openJob(owner: UserId, index: Int): Job =
    Job(
      JobId(UUID.randomUUID()),
      owner,
      s"Closure job $index",
      "Closure description",
      List("Closure requirement"),
      Set("Scala"),
      Location("Cyprus", Some("Nicosia"), remote = false),
      JobStatus.Open,
      created.plusSeconds(index.toLong),
      created.plusSeconds(index.toLong)
    )

  private final class Harness(
      val fixture: MongoAccessEvaluationSupport.Fixture,
      size: JobClosurePageSize,
      retry: JobClosureRetry
  ) {
    val users = MongoUserRepository.transactional(
      fixture.database,
      fixture.client,
      MongoEmbeddingWorkEnqueuer.disabled,
      Diagnostics.noop[IO],
      jobClosurePageSize = size
    )
    val closure = MongoRecruiterJobClosure.transactional(
      fixture.database,
      fixture.client,
      Diagnostics.noop[IO],
      size,
      retry = retry
    )

    /** A closure whose every transaction does all its work and then fails before commit, like a crash mid-page. */
    val crashing = MongoRecruiterJobClosure.withRunner(
      fixture.database,
      new MongoTransactionRunner {
        private val real =
          MongoTransactionRunner.sessions(fixture.client, RepositoryError.Conflict, diagnostics = Diagnostics.noop[IO])
        override def run[A](operation: Option[mongo4cats.client.ClientSession[IO]] => RepositoryF[IO, A]) =
          real.run(session =>
            operation(session) <* RepositoryF.lift[IO, Unit](IO.raiseError(new RuntimeException("crash")))
          )
      },
      Diagnostics.noop[IO],
      size,
      retry
    )

    def seed(owner: User, jobs: Int): IO[List[Job]] = {
      val values = List.tabulate(jobs)(i => openJob(owner.id, i))
      MongoRepositoryTestSupport.insertOne(
        fixture.database,
        MongoCollections.Users,
        MongoHiringCodecs.user(owner)
      ) *> values
        .grouped(500)
        .toList
        .traverse_(batch =>
          Mongo4catsCollections
            .documents(fixture.database, MongoCollections.Jobs)
            .flatMap(_.insertMany(batch.map(MongoHiringCodecs.job(_))))
        )
        .as(values)
    }

    def delete(owner: User): IO[Either[RepositoryError, Unit]] =
      users.deleteAccount(owner.id, deletedAt, s"deleted-${owner.id.value}", MutationWriteContext.directWrite).value

    def jobsWith(owner: User, status: JobStatus): IO[Long] =
      MongoRepositoryTestSupport.count(
        fixture.database,
        MongoCollections.Jobs,
        Filters.and(Filters.eq("recruiterId", owner.id.value.toString), Filters.eq("status", status.toString))
      )

    def closedEvents(maximum: Int): IO[List[String]] =
      MongoRepositoryTestSupport
        .collectWithin(
          fixture.database
            .getCollection(MongoCollections.EventOutbox)
            .find(Filters.eq("eventType", OperationalEventType.JOB_CLOSED.toString)),
          maximum
        )
        .map(_.map(_.getString("aggregateId")))

    def pendingWork(owner: User): IO[Long] =
      MongoRepositoryTestSupport.count(
        fixture.database,
        MongoCollections.RecruiterJobClosureWork,
        Filters.eq("_id", owner.id.value.toString)
      )
  }

  private val quickRetry = JobClosureRetry.from(20.millis, 20.millis).get

  private def harness[A](size: JobClosurePageSize, retry: JobClosureRetry = quickRetry)(
      body: Harness => IO[A]
  ): IO[A] =
    mongoResource.use { fixture =>
      MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO]) *> body(new Harness(fixture, size, retry))
    }

  test("ARR-08.1 deleting a recruiter with 2500 open jobs closes one page, then the follow-up closes the rest") {
    harness(JobClosurePageSize.Default) { h =>
      val owner = recruiter()
      for {
        jobs <- h.seed(owner, 2500)
        deletion <- h.delete(owner)
        openAfterDeletion <- h.jobsWith(owner, JobStatus.Open)
        closedAfterDeletion <- h.jobsWith(owner, JobStatus.Closed)
        eventsAfterDeletion <- h.closedEvents(3000)
        pendingAfterDeletion <- h.pendingWork(owner)
        second <- h.closure.closeNextPage.value
        third <- h.closure.closeNextPage.value
        idle <- h.closure.closeNextPage.value
        openAtEnd <- h.jobsWith(owner, JobStatus.Open)
        events <- h.closedEvents(3000)
        pendingAtEnd <- h.pendingWork(owner)
      } yield {
        assertEquals(deletion, Right(()))
        assertEquals((openAfterDeletion, closedAfterDeletion), (1500L, 1000L))
        assertEquals(eventsAfterDeletion.size, 1000)
        assertEquals(pendingAfterDeletion, 1L)
        assertEquals(second, Right(RecruiterJobClosureProgress.PageClosed(1000)))
        assertEquals(third, Right(RecruiterJobClosureProgress.Completed(500)))
        assertEquals(idle, Right(RecruiterJobClosureProgress.Idle))
        assertEquals(openAtEnd, 0L)
        assertEquals(pendingAtEnd, 0L)
        assertEquals(events.size, 2500)
        assertEquals(events.toSet, jobs.map(_.id.value.toString).toSet)
      }
    }
  }

  test("ARR-08.1 a recruiter whose jobs fit one page leaves no follow-up work") {
    harness(pageSize(10)) { h =>
      val owner = recruiter()
      for {
        _ <- h.seed(owner, 10)
        deletion <- h.delete(owner)
        pending <- h.pendingWork(owner)
        open <- h.jobsWith(owner, JobStatus.Open)
        idle <- h.closure.closeNextPage.value
      } yield {
        assertEquals(deletion, Right(()))
        assertEquals((pending, open), (0L, 0L))
        assertEquals(idle, Right(RecruiterJobClosureProgress.Idle))
      }
    }
  }

  test("ARR-08.2 a crash after the first follow-up page resumes without a duplicate JOB_CLOSED event") {
    harness(pageSize(10)) { h =>
      val owner = recruiter()
      for {
        jobs <- h.seed(owner, 35)
        _ <- h.delete(owner)
        first <- h.closure.closeNextPage.value
        // The next page does all its work and crashes before commit: nothing of it may be visible.
        crashed <- h.crashing.closeNextPage.value.attempt
        _ <- IO.sleep(100.millis) // the failed row's short retry delay
        openAfterCrash <- h.jobsWith(owner, JobStatus.Open)
        eventsAfterCrash <- h.closedEvents(100)
        pendingAfterCrash <- h.pendingWork(owner)
        // Restart: a fresh instance continues from the stored state.
        resumed <- List.fill(3)(()).traverse(_ => h.closure.closeNextPage.value)
        openAtEnd <- h.jobsWith(owner, JobStatus.Open)
        events <- h.closedEvents(100)
        pendingAtEnd <- h.pendingWork(owner)
      } yield {
        assertEquals(first, Right(RecruiterJobClosureProgress.PageClosed(10)))
        assert(crashed.isLeft || crashed.exists(_.isLeft), clue(crashed))
        assertEquals((openAfterCrash, eventsAfterCrash.size, pendingAfterCrash), (15L, 20, 1L))
        assertEquals(
          resumed,
          List(
            Right(RecruiterJobClosureProgress.PageClosed(10)),
            Right(RecruiterJobClosureProgress.Completed(5)),
            Right(RecruiterJobClosureProgress.Idle)
          ).map(identity)
        )
        assertEquals((openAtEnd, pendingAtEnd), (0L, 0L))
        assertEquals(events.size, 35)
        assertEquals(events.toSet, jobs.map(_.id.value.toString).toSet)
      }
    }
  }

  test("ARR-08.2 the background worker finishes the closure and reports no failure") {
    harness(pageSize(10)) { h =>
      val owner = recruiter()
      val schedule = RecruiterJobClosureSchedule.from(50.millis, 50.millis, 200.millis).get
      for {
        jobs <- h.seed(owner, 45)
        _ <- h.delete(owner)
        _ <- new RecruiterJobClosureWorker[IO](h.closure, Diagnostics.noop[IO], schedule).resource.use { _ =>
          fs2.Stream
            .repeatEval(h.pendingWork(owner))
            .metered(50.millis)
            .takeWhile(_ > 0L)
            .compile
            .drain
            .timeout(60.seconds)
        }
        open <- h.jobsWith(owner, JobStatus.Open)
        events <- h.closedEvents(100)
      } yield {
        assertEquals(open, 0L)
        assertEquals(events.toSet, jobs.map(_.id.value.toString).toSet)
        assertEquals(events.size, 45)
      }
    }
  }

  test("ARR-08.1 a candidate cannot apply to a deleted recruiter's job that is still waiting for closure") {
    harness(pageSize(1)) { h =>
      val owner = recruiter()
      val candidate = User(
        UserId(UUID.randomUUID()),
        None,
        "Closure candidate",
        UserRole.Candidate,
        Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None))),
        created
      )
      val applications =
        MongoApplicationRepository.transactional(h.fixture.database, h.fixture.client, Diagnostics.noop[IO])
      for {
        jobs <- h.seed(owner, 3)
        _ <- MongoRepositoryTestSupport.insertOne(
          h.fixture.database,
          MongoCollections.Users,
          MongoHiringCodecs.user(candidate)
        )
        _ <- h.delete(owner)
        stillOpen <- MongoRepositoryTestSupport
          .collectWithin(
            h.fixture.database
              .getCollection(MongoCollections.Jobs)
              .find(Filters.and(Filters.eq("recruiterId", owner.id.value.toString), Filters.eq("status", "Open"))),
            10
          )
          .map(_.map(_.getString("_id")))
        target = JobId(UUID.fromString(stillOpen.head))
        application = Application.create(ApplicationId(UUID.randomUUID()), candidate.id, target, deletedAt)
        event = ApplicationEvent(
          ApplicationEventId(UUID.randomUUID()),
          application.id,
          None,
          ApplicationStatus.Created,
          candidate.id,
          deletedAt,
          None,
          None,
          Some(candidate.role)
        )
        attempt <- applications
          .createForOpenJobWithEvents(
            JobSubmissionSnapshot(target, JobStatus.Open, 0L),
            application,
            event,
            Nil,
            ApplicationWriteActor(candidate.id, candidate.role),
            MutationWriteContext.directWrite
          )
          .value
        rows <- MongoRepositoryTestSupport.count(h.fixture.database, MongoCollections.Applications)
      } yield {
        assertEquals(jobs.size, 3)
        assertEquals(stillOpen.size, 2)
        assertEquals(attempt, Left(RepositoryError.Conflict))
        assertEquals(rows, 0L)
      }
    }
  }

  test("ARR-08.1 deletion is reported pending while closure work exists and complete only after the follow-up") {
    harness(pageSize(10)) { h =>
      val owner = recruiter()
      val erasures =
        MongoAnalyticsErasureRequestRepository.transactional(h.fixture.database, h.fixture.client, Diagnostics.noop[IO])
      for {
        _ <- h.seed(owner, 25)
        receipt <- erasures.enqueue(owner.id, deletedAt, MutationWriteContext.directWrite).value
        receiptId = receipt.fold(error => fail(s"enqueue failed: $error"), identity)
        _ <- h.delete(owner)
        _ <- erasures.markComplete(owner.id, deletedAt.plusSeconds(1)).value
        pendingFirstPage <- erasures.statusForSubject(owner.id, receiptId).value
        _ <- h.closure.closeNextPage.value
        pendingSecondPage <- erasures.statusForSubject(owner.id, receiptId).value
        _ <- h.closure.closeNextPage.value
        complete <- erasures.statusForSubject(owner.id, receiptId).value
      } yield {
        assertEquals(pendingFirstPage, Right(AccountDeletionStatus.Pending))
        assertEquals(pendingSecondPage, Right(AccountDeletionStatus.Pending))
        assertEquals(complete, Right(AccountDeletionStatus.Complete))
      }
    }
  }

  private def explain(h: Harness, collection: String, filter: Document, sort: Option[Document], limit: Int) = {
    val find = new Document("find", collection).append("filter", filter).append("limit", limit)
    sort.foreach(find.append("sort", _))
    h.fixture.database
      .runCommand(new Document("explain", find).append("verbosity", "executionStats"))
      .map(_.toJson)
  }

  private def stat(json: String, field: String): Long =
    s""""$field"\\s*:\\s*(?:\\{\\s*"\\$$numberLong"\\s*:\\s*"(\\d+)"\\s*\\}|(\\d+))""".r
      .findFirstMatchIn(json)
      .map(m => Option(m.group(1)).getOrElse(m.group(2)).toLong)
      .getOrElse(fail(s"no $field in explain: $json"))

  test("ARR-08.1 explain: the follow-up page and the submit-gate reads use indexes and stay within the page bound") {
    harness(JobClosurePageSize.Default) { h =>
      val owner = recruiter()
      val filter = new Document("recruiterId", owner.id.value.toString).append("status", "Open")
      val order = new Document("createdAt", -1).append("_id", -1)
      for {
        jobs <- h.seed(owner, 2500)
        page <- explain(h, MongoCollections.Jobs, filter, Some(order), 1000)
        remain <- explain(h, MongoCollections.Jobs, filter, None, 1)
        jobGate <- explain(h, MongoCollections.Jobs, new Document("_id", jobs.head.id.value.toString), None, 1)
        userGate <- explain(
          h,
          MongoCollections.Users,
          new Document("_id", owner.id.value.toString).append("accountStatus", "Active"),
          None,
          1
        )
      } yield {
        List("page" -> page, "remaining" -> remain, "jobGate" -> jobGate, "userGate" -> userGate).foreach {
          case (name, json) =>
            println(
              s"ARR-08.1 explain $name: keysExamined=${stat(json, "totalKeysExamined")} " +
                s"docsExamined=${stat(json, "totalDocsExamined")} returned=${stat(json, "nReturned")} " +
                s"ixscan=${json.contains("IXSCAN")} idhack=${json.contains("IDHACK")} collscan=${json.contains("COLLSCAN")}"
            )
            assert(!json.contains("COLLSCAN"), clue(name))
        }
        assert(page.contains(MongoIndexNames.JobsRecruiterStatusCreated), clue(page))
        val stages = """"stage"\s*:\s*"([A-Z_]+)"""".r.findAllMatchIn(page).map(_.group(1)).toList
        println(s"ARR-08.1 explain page stages=$stages")
        // The winning plan is first; a rejected plan may sort, but the winner walks the index in order.
        assertEquals(stages.take(3), List("LIMIT", "FETCH", "IXSCAN"))
        assert(stat(page, "totalDocsExamined") <= 1000L)
        assert(remain.contains(MongoIndexNames.JobsRecruiterStatusCreated), clue(remain))
        assert(stat(remain, "totalDocsExamined") <= 1L)
        assert(stat(jobGate, "totalDocsExamined") <= 1L)
        assert(stat(userGate, "totalDocsExamined") <= 1L)
      }
    }
  }

  private def insertPending(h: Harness, owner: UserId, requestedAt: Instant, attempts: Int = 0): IO[Unit] =
    MongoRepositoryTestSupport.insertOne(
      h.fixture.database,
      MongoCollections.RecruiterJobClosureWork,
      new Document("_id", owner.value.toString)
        .append("requestedAt", java.util.Date.from(requestedAt))
        .append("notBefore", java.util.Date.from(requestedAt))
        .append("attempts", Int.box(attempts))
    )

  test("ARR-08.1 a successful page resets the attempt count and the retry time of the remaining row") {
    harness(pageSize(10)) { h =>
      val owner = recruiter()
      for {
        _ <- h.seed(owner, 25)
        _ <- insertPending(h, owner.id, created, attempts = 3)
        page <- h.closure.closeNextPage.value
        row <- MongoRepositoryTestSupport.findOne(
          h.fixture.database,
          MongoCollections.RecruiterJobClosureWork,
          Filters.eq("_id", owner.id.value.toString)
        )
      } yield {
        assertEquals(page, Right(RecruiterJobClosureProgress.PageClosed(10)))
        assertEquals(row.map(_.getInteger("attempts").intValue()), Some(0))
        assert(row.exists(_.getDate("notBefore").toInstant.isAfter(created)), clue(row))
      }
    }
  }

  test("ARR-08.1 a poison closure row does not block later recruiters and is retried with backoff, never dropped") {
    harness(pageSize(10), JobClosureRetry.from(300.millis, 600.millis).get) { h =>
      val poisoned = recruiter()
      val healthy = recruiter()
      for {
        jobs <- h.seed(healthy, 25)
        _ <- h.delete(healthy)
        // The poisoned recruiter's closure is older than the healthy one, so it is the head; its job is malformed.
        _ <- insertPending(h, poisoned.id, created)
        _ <- h.fixture.database
          .getCollection(MongoCollections.Jobs)
          .insertOne(
            new Document("_id", UUID.randomUUID().toString)
              .append("recruiterId", poisoned.id.value.toString)
              .append("status", "Open")
              .append("createdAt", java.util.Date.from(created)),
            new com.mongodb.client.model.InsertOneOptions().bypassDocumentValidation(true)
          )
        first <- h.closure.closeNextPage.value
        afterFirst <- MongoRepositoryTestSupport.findOne(
          h.fixture.database,
          MongoCollections.RecruiterJobClosureWork,
          Filters.eq("_id", poisoned.id.value.toString)
        )
        second <- h.closure.closeNextPage.value
        third <- h.closure.closeNextPage.value
        idle <- h.closure.closeNextPage.value
        healthyPending <- h.pendingWork(healthy)
        _ <- IO.sleep(700.millis)
        retried <- h.closure.closeNextPage.value
        afterRetry <- MongoRepositoryTestSupport.findOne(
          h.fixture.database,
          MongoCollections.RecruiterJobClosureWork,
          Filters.eq("_id", poisoned.id.value.toString)
        )
        events <- h.closedEvents(100)
        healthyOpen <- h.jobsWith(healthy, JobStatus.Open)
      } yield {
        assertEquals(first, Left(RepositoryError.InvalidStoredData))
        assertEquals(afterFirst.map(_.getInteger("attempts").intValue()), Some(1))
        assert(afterFirst.exists(_.getDate("notBefore").toInstant.isAfter(created)), clue(afterFirst))
        assertEquals(second, Right(RecruiterJobClosureProgress.PageClosed(10)))
        assertEquals(third, Right(RecruiterJobClosureProgress.Completed(5)))
        assertEquals(idle, Right(RecruiterJobClosureProgress.Idle))
        assertEquals((healthyPending, healthyOpen), (0L, 0L))
        assertEquals(events.toSet, jobs.map(_.id.value.toString).toSet)
        assertEquals(retried, Left(RepositoryError.InvalidStoredData))
        assertEquals(afterRetry.map(_.getInteger("attempts").intValue()), Some(2))
      }
    }
  }

  test("ARR-08.1 an idle closure queue opens no transaction") {
    harness(pageSize(10)) { h =>
      for {
        runs <- cats.effect.Ref.of[IO, Int](0)
        counting = MongoRecruiterJobClosure.withRunner(
          h.fixture.database,
          new MongoTransactionRunner {
            override def run[A](operation: Option[mongo4cats.client.ClientSession[IO]] => RepositoryF[IO, A]) =
              RepositoryF.lift[IO, Unit](runs.update(_ + 1)) *> operation(None)
          },
          Diagnostics.noop[IO],
          pageSize(10),
          quickRetry
        )
        results <- List.fill(5)(()).traverse(_ => counting.closeNextPage.value)
        opened <- runs.get
      } yield {
        assertEquals(results.distinct, List(Right(RecruiterJobClosureProgress.Idle)))
        assertEquals(opened, 0)
      }
    }
  }

  test("ARR-08.1 an application accepted in the submission window ends on a job the follow-up closes") {
    harness(pageSize(1)) { h =>
      val owner = recruiter()
      for {
        jobs <- h.seed(owner, 3)
        _ <- h.delete(owner)
        open <- h.fixture.database
          .getCollection(MongoCollections.Jobs)
          .find(Filters.and(Filters.eq("recruiterId", owner.id.value.toString), Filters.eq("status", "Open")))
          .first
        target = JobId(UUID.fromString(open.map(_.getString("_id")).getOrElse(fail("no open job"))))
        // A submission that read the recruiter as active before the deletion committed lands on a job beyond the
        // first page: the application exists, and the follow-up then closes the job under it.
        application = Application.create(ApplicationId(UUID.randomUUID()), UserId(UUID.randomUUID()), target, created)
        _ <- MongoRepositoryTestSupport.insertOne(
          h.fixture.database,
          MongoCollections.Applications,
          MongoHiringCodecs.application(application)
        )
        _ <- List.fill(3)(()).traverse(_ => h.closure.closeNextPage.value)
        stored <- h.fixture.database
          .getCollection(MongoCollections.Jobs)
          .find(Filters.eq("_id", target.value.toString))
          .first
        applications <- MongoRepositoryTestSupport.count(h.fixture.database, MongoCollections.Applications)
        openAtEnd <- h.jobsWith(owner, JobStatus.Open)
      } yield {
        assertEquals(jobs.size, 3)
        assertEquals(stored.map(_.getString("status")), Some("Closed"))
        assertEquals((applications, openAtEnd), (1L, 0L))
      }
    }
  }

  test("ARR-07.2 the age of the oldest pending closure is reported until the follow-up finishes") {
    harness(pageSize(10)) { h =>
      val owner = recruiter()
      val later = deletedAt.plusSeconds(90)
      for {
        none <- h.closure.oldestPendingAge(later)
        _ <- h.seed(owner, 25)
        _ <- h.delete(owner)
        pending <- h.closure.oldestPendingAge(later)
        explained <- MongoAccessEvaluationSupport.command(
          h.fixture.database,
          new Document(
            "explain",
            new Document("find", MongoCollections.RecruiterJobClosureWork)
              .append("filter", new Document("requestedAt", new Document("$exists", true)))
              .append("sort", new Document("requestedAt", 1))
              .append("projection", new Document("requestedAt", 1))
              .append("limit", 1)
              .append("maxTimeMS", MongoPendingWorkAge.MaxTime.toMillis.toInt)
          ).append("verbosity", "executionStats")
        )
        _ <- List.fill(3)(h.closure.closeNextPage.value).sequence_
        drained <- h.closure.oldestPendingAge(later)
      } yield {
        val stats = explained.get("executionStats", classOf[Document])
        println(
          s"ARR-07.2 explain closure-oldest-pending plan=${explained.toJson.contains("COLLSCAN")} " +
            s"keysExamined=${stats.get("totalKeysExamined")} docsExamined=${stats.get("totalDocsExamined")}"
        )
        assertEquals(none, None)
        assertEquals(pending, Some(90.seconds))
        assertEquals(drained, None)
      }
    }
  }
}
