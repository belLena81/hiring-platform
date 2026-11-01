package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, UserId}
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.events.OperationalEventPayload.SearchKind
import com.example.graphQL.cats.service.events.{
  OperationalAggregateType,
  OperationalEventEnvelope,
  OperationalEventPayload,
  OperationalEventType,
  SearchFilterSnapshot,
  SearchSession
}
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.{Diagnostics, RepositoryError}
import mongo4cats.database.MongoDatabase

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*

/** DDR-01: lease expiry is decided by the database clock, for the search-session work, event outbox and interview
  * command queues (the embedding work queue is covered in `MongoEmbeddingWorkIntegrationSpec`).
  *
  *   - DDR-01.1 a stored expiry is database time at the claim plus the requested duration;
  *   - DDR-01.2 a caller clock skewed by +/- 10 minutes can neither reclaim a live lease nor displace its holder;
  *   - DDR-01.3 an expired lease is reclaimed exactly once under concurrent claimers;
  *   - DDR-01.4 renewal after expiry reports loss and the displaced holder's writes are rejected.
  */
final class LeaseExpiryDatabaseClockIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes

  private val skew = 10.minutes
  private val claimers = 12

  private def success[A](result: RepositoryF[IO, A]): IO[A] =
    result.value.flatMap(_.fold(error => IO.raiseError(new AssertionError(s"Repository failure: $error")), IO.pure))

  private def assertWithin(expiry: Instant, before: Instant, after: Instant, duration: FiniteDuration)(using
      munit.Location
  ): Unit = {
    assert(!expiry.isBefore(before.plusMillis(duration.toMillis)), clue(expiry -> before))
    assert(!expiry.isAfter(after.plusMillis(duration.toMillis)), clue(expiry -> after))
  }

  // ------------------------------------------------------------------ search-session work

  private def searchSession(at: Instant): SearchSession =
    SearchSession(
      UUID.randomUUID(),
      UserId(UUID.randomUUID()),
      SearchKind.Jobs,
      None,
      SearchFilterSnapshot.Unfiltered,
      None,
      Nil,
      at,
      at.plusSeconds(3600)
    )

  private def searchWork(fixture: MongoAccessEvaluationSupport.Fixture, wall: Instant) = {
    val repository =
      MongoSearchSessionWorkRepository.transactional(fixture.database, fixture.client, Diagnostics.noop[IO])
    val session = searchSession(wall)
    (MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO]) *>
      success(repository.enqueue(PendingSearchSessionWork(session), wall.minusSeconds(1)))).as(repository -> session)
  }

  test("DDR-01.1 a search-session lease ends at database time plus the requested duration") {
    mongoResource.use { fixture =>
      for {
        wall <- IO.realTimeInstant
        (repository, session) <- searchWork(fixture, wall)
        window <- LeaseTestSupport.withinDatabaseWindow(fixture.database)(
          success(repository.claim("worker", wall, 45.seconds))
        )
        (claim, before, after) = window
        expiry <- LeaseTestSupport.storedExpiry(
          fixture.database,
          MongoCollections.SearchSessionWork,
          session.id.toString
        )
      } yield {
        assert(claim.nonEmpty)
        assertWithin(expiry, before, after, 45.seconds)
      }
    }
  }

  test("DDR-01.2 a search-session claimer with a clock skewed by 10 minutes cannot reclaim a live lease") {
    mongoResource.use { fixture =>
      for {
        wall <- IO.realTimeInstant
        (repository, session) <- searchWork(fixture, wall)
        holder <- success(repository.claim("holder", wall, 30.seconds))
        ahead <- success(repository.claim("fast-clock", wall.plusMillis(skew.toMillis), 30.seconds))
        behind <- success(repository.claim("slow-clock", wall.minusMillis(skew.toMillis), 30.seconds))
        stored <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          MongoCollections.SearchSessionWork,
          com.mongodb.client.model.Filters.eq(MongoFields.Id, session.id.toString)
        )
      } yield {
        assertEquals(ahead, None)
        assertEquals(behind, None)
        assertEquals(stored.map(_.getString(MongoFields.LeaseToken)), holder.map(_.leaseToken))
        assertEquals(stored.map(_.getString(MongoFields.LeaseOwner)), Some("holder"))
      }
    }
  }

  test("DDR-01.3 DDR-01.4 an expired search-session lease is reclaimed once and the displaced holder is fenced") {
    mongoResource.use { fixture =>
      for {
        wall <- IO.realTimeInstant
        (repository, session) <- searchWork(fixture, wall)
        stale <- success(repository.claim("stale", wall, 30.seconds))
          .flatMap(IO.fromOption(_)(new AssertionError("claim")))
        _ <- LeaseTestSupport.expire(fixture.database, MongoCollections.SearchSessionWork, session.id.toString)
        results <- (1 to claimers).toList.parTraverse(i => success(repository.claim(s"claimer-$i", wall, 30.seconds)))
        staleComplete <- repository.complete(stale, wall).value
        staleRetry <- repository.retry(stale, wall).value
        staleFail <- repository.fail(stale, SearchSessionWorkFailure.RetryExhausted, wall).value
      } yield {
        assertEquals(results.flatten.size, 1)
        assertNotEquals(results.flatten.head.leaseToken, stale.leaseToken)
        List(staleComplete, staleRetry, staleFail).foreach(result =>
          assertEquals(result, Left(RepositoryError.Conflict))
        )
      }
    }
  }

  // ------------------------------------------------------------------ event outbox

  private def outboxEvent(wall: Instant): OperationalEventEnvelope = {
    val job = UUID.randomUUID()
    OperationalEventEnvelope(
      UUID.randomUUID(),
      OperationalEventType.JOB_CREATED,
      wall.minusSeconds(5),
      OperationalAggregateType.Job,
      job.toString,
      UserId(UUID.randomUUID()),
      OperationalEventPayload.JobFact(
        OperationalEventPayload.JobKind.Created,
        OperationalEventPayload.JobSnapshot(job, List("Scala"), JobStatus.Open)
      )
    )
  }

  private def outboxWith(fixture: MongoAccessEvaluationSupport.Fixture, wall: Instant) = {
    val value = outboxEvent(wall)
    val record = MongoHiringCodecs.outboxRecord(value, value.occurredAt).fold(message => fail(message), identity)
    (MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO]) *>
      MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.EventOutbox, record)).as(
      MongoOperationalEventOutboxRepository.transactional(
        fixture.database,
        fixture.client,
        Diagnostics.noop[IO]
      ) -> value
    )
  }

  private def outboxClaim(
      repository: MongoOperationalEventOutboxRepository,
      worker: String,
      at: Instant,
      duration: FiniteDuration
  ): IO[List[ClaimedOperationalEvent]] =
    success(repository.claim(worker, s"hiring-publisher-$worker", at, duration, 1))

  test("DDR-01.1 an outbox event lease and its subject lease end at database time plus the requested duration") {
    mongoResource.use { fixture =>
      for {
        wall <- IO.realTimeInstant
        (repository, value) <- outboxWith(fixture, wall)
        window <- LeaseTestSupport.withinDatabaseWindow(fixture.database)(
          outboxClaim(repository, "w", wall, 50.seconds)
        )
        (claimed, before, after) = window
        eventExpiry <- LeaseTestSupport.storedExpiry(
          fixture.database,
          MongoCollections.EventOutbox,
          value.eventId.toString
        )
        subject = claimed.head.subjectIds.head
        subjectExpiry <- LeaseTestSupport.storedExpiry(fixture.database, MongoCollections.OutboxSubjectFences, subject)
        renewWindow <- LeaseTestSupport.withinDatabaseWindow(fixture.database)(
          success(repository.renewLease(value.eventId, claimed.head.leaseToken, claimed.head.subjectIds, 80.seconds))
        )
        (_, renewBefore, renewAfter) = renewWindow
        renewedEvent <- LeaseTestSupport.storedExpiry(
          fixture.database,
          MongoCollections.EventOutbox,
          value.eventId.toString
        )
        renewedSubject <- LeaseTestSupport.storedExpiry(fixture.database, MongoCollections.OutboxSubjectFences, subject)
      } yield {
        assertEquals(claimed.size, 1)
        assertWithin(eventExpiry, before, after, 50.seconds)
        assertWithin(subjectExpiry, before, after, 50.seconds)
        assertWithin(renewedEvent, renewBefore, renewAfter, 80.seconds)
        assertWithin(renewedSubject, renewBefore, renewAfter, 80.seconds)
      }
    }
  }

  test(
    "DDR-01.2 an outbox claimer with a clock skewed by 10 minutes cannot reclaim a live lease or displace the holder"
  ) {
    mongoResource.use { fixture =>
      for {
        wall <- IO.realTimeInstant
        (repository, value) <- outboxWith(fixture, wall)
        holder <- outboxClaim(repository, "holder", wall, 30.seconds)
        ahead <- outboxClaim(repository, "fast-clock", wall.plusMillis(skew.toMillis), 30.seconds)
        behind <- outboxClaim(repository, "slow-clock", wall.minusMillis(skew.toMillis), 30.seconds)
        renewed <- repository
          .renewLease(value.eventId, holder.head.leaseToken, holder.head.subjectIds, 30.seconds)
          .value
        stored <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          MongoCollections.EventOutbox,
          com.mongodb.client.model.Filters.eq(MongoFields.Id, value.eventId.toString)
        )
      } yield {
        assertEquals(holder.size, 1)
        assertEquals(ahead, Nil)
        assertEquals(behind, Nil)
        assertEquals(renewed, Right(()))
        assertEquals(stored.map(_.getString(MongoFields.LeaseToken)), Some(holder.head.leaseToken))
        assertEquals(stored.map(_.getInteger(MongoFields.Attempts).intValue), Some(1))
      }
    }
  }

  test(
    "DDR-01.3 DDR-01.4 an expired outbox lease is reclaimed once, its renewal reports loss and the holder is fenced"
  ) {
    mongoResource.use { fixture =>
      for {
        wall <- IO.realTimeInstant
        (repository, value) <- outboxWith(fixture, wall)
        stale <- outboxClaim(repository, "stale", wall, 30.seconds)
        _ <- LeaseTestSupport.expire(fixture.database, MongoCollections.EventOutbox, value.eventId.toString)
        _ <- LeaseTestSupport.expire(fixture.database, MongoCollections.OutboxSubjectFences, stale.head.subjectIds.head)
        lostBeforeTakeover <- repository
          .renewLease(value.eventId, stale.head.leaseToken, stale.head.subjectIds, 30.seconds)
          .value
        results <- (1 to claimers).toList.parTraverse(i => outboxClaim(repository, s"claimer-$i", wall, 30.seconds))
        lostAfterTakeover <- repository
          .renewLease(value.eventId, stale.head.leaseToken, stale.head.subjectIds, 30.seconds)
          .value
        late <- repository.markPublished(value.eventId, stale.head.leaseToken, wall, wall.plusSeconds(3600)).value
        stored <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          MongoCollections.EventOutbox,
          com.mongodb.client.model.Filters.eq(MongoFields.Id, value.eventId.toString)
        )
      } yield {
        assertEquals(lostBeforeTakeover, Left(RepositoryError.Conflict))
        assertEquals(results.flatten.size, 1)
        assertEquals(lostAfterTakeover, Left(RepositoryError.Conflict))
        assertEquals(late, Left(RepositoryError.Conflict))
        assertEquals(stored.map(_.getString(MongoFields.State)), Some("InFlight"))
        assertEquals(stored.map(_.getString(MongoFields.LeaseToken)), Some(results.flatten.head.leaseToken))
      }
    }
  }

  // ------------------------------------------------------------------ interview commands

  private def interviewCommand(fixture: MongoAccessEvaluationSupport.Fixture, wall: Instant) = {
    val repository = MongoInterviewWorkflowRepository.live(fixture.database, fixture.client, Diagnostics.noop[IO])
    val value = InterviewWorkflow
      .create(
        InterviewWorkflowId(UUID.randomUUID()),
        ApplicationId(UUID.randomUUID()),
        UserId(UUID.randomUUID()),
        UserId(UUID.randomUUID()),
        InterviewInterval(wall.plusSeconds(172800), wall.plusSeconds(176400)),
        wall.plusSeconds(3600),
        UUID.randomUUID(),
        ApplicationStatus.Accepted,
        UserId(UUID.randomUUID()),
        UserRole.Recruiter
      )
      .map(workflow => workflow.copy(initiatedBy = workflow.recruiterId))
      .fold(error => fail(s"Invalid fixture: $error"), identity)
    for {
      _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
      _ <- InterviewSchedulingFixtures.seed(fixture.database, List(value), wall)
      _ <- success(
        repository.create(value, InterviewWorkflow.initialCommand(value), wall, MutationWriteContext.directWrite)
      )
    } yield repository -> value
  }

  private def commandId(claim: ClaimedInterviewWorkflowCommand): String =
    MongoInterviewWorkflowCodec.commandId(claim.record.workflowId, claim.record.stepId)

  private def storedClaimExpiry(database: MongoDatabase[IO], claim: ClaimedInterviewWorkflowCommand): IO[Instant] =
    LeaseTestSupport.storedExpiry(database, MongoCollections.InterviewWorkflowCommands, commandId(claim), "claimUntil")

  test("DDR-01.1 an interview publication and execution lease end at database time plus the requested duration") {
    mongoResource.use { fixture =>
      for {
        wall <- IO.realTimeInstant
        (repository, _) <- interviewCommand(fixture, wall)
        window <- LeaseTestSupport.withinDatabaseWindow(fixture.database)(
          success(repository.claimDueCommands("publisher", wall, 40.seconds, 1))
        )
        (claims, before, after) = window
        claim <- IO.fromOption(claims.headOption)(new AssertionError("publication claim"))
        publicationExpiry <- storedClaimExpiry(fixture.database, claim)
        renewWindow <- LeaseTestSupport.withinDatabaseWindow(fixture.database)(
          success(repository.renewPublication(claim, 70.seconds))
        )
        (renewed, renewBefore, renewAfter) = renewWindow
        renewedExpiry <- storedClaimExpiry(fixture.database, claim)
        _ <- success(repository.markPublished(claim, wall))
        executionWindow <- LeaseTestSupport.withinDatabaseWindow(fixture.database)(
          success(repository.claimExecution(claim.record, "executor", wall, 55.seconds, 5))
        )
        (outcome, executionBefore, executionAfter) = executionWindow
        executing <- outcome match {
          case InterviewExecutionClaimOutcome.Acquired(value) => IO.pure(value)
          case other => IO.raiseError(new AssertionError(s"Missing execution claim: $other"))
        }
        executionExpiry <- storedClaimExpiry(fixture.database, executing)
      } yield {
        assertWithin(publicationExpiry, before, after, 40.seconds)
        assert(renewed)
        assertWithin(renewedExpiry, renewBefore, renewAfter, 70.seconds)
        assertWithin(executionExpiry, executionBefore, executionAfter, 55.seconds)
      }
    }
  }

  test("DDR-01.2 an interview claimer with a clock skewed by 10 minutes cannot reclaim a live lease or displace it") {
    mongoResource.use { fixture =>
      for {
        wall <- IO.realTimeInstant
        (repository, _) <- interviewCommand(fixture, wall)
        holder <- success(repository.claimDueCommands("holder", wall, 30.seconds, 1)).flatMap(claims =>
          IO.fromOption(claims.headOption)(new AssertionError("publication claim"))
        )
        ahead <- success(repository.claimDueCommands("fast-clock", wall.plusMillis(skew.toMillis), 30.seconds, 1))
        behind <- success(repository.claimDueCommands("slow-clock", wall.minusMillis(skew.toMillis), 30.seconds, 1))
        stillHeld <- success(repository.renewPublication(holder, 30.seconds))
      } yield {
        assertEquals(ahead, Nil)
        assertEquals(behind, Nil)
        assert(stillHeld)
      }
    }
  }

  test("DDR-01.2 a skewed execution claimer never reclaims a live execution lease") {
    mongoResource.use { fixture =>
      for {
        wall <- IO.realTimeInstant
        (repository, _) <- interviewCommand(fixture, wall)
        publication <- success(repository.claimDueCommands("publisher", wall, 30.seconds, 1)).flatMap(claims =>
          IO.fromOption(claims.headOption)(new AssertionError("publication claim"))
        )
        _ <- success(repository.markPublished(publication, wall))
        first <- success(repository.claimExecution(publication.record, "executor", wall, 30.seconds, 5))
        // The caller's clock claims the execution lease expired; the database clock says it is live.
        ahead <- success(
          repository.claimExecution(publication.record, "fast-clock", wall.plusMillis(skew.toMillis), 30.seconds, 5)
        )
        behind <- success(
          repository.claimExecution(publication.record, "slow-clock", wall.minusMillis(skew.toMillis), 30.seconds, 5)
        )
      } yield {
        assert(first.isInstanceOf[InterviewExecutionClaimOutcome.Acquired])
        assertEquals(ahead, InterviewExecutionClaimOutcome.Busy)
        assertEquals(behind, InterviewExecutionClaimOutcome.Busy)
      }
    }
  }

  test(
    "DDR-01.3 DDR-01.4 an expired interview lease is reclaimed once, its renewal reports loss and the holder is fenced"
  ) {
    mongoResource.use { fixture =>
      for {
        wall <- IO.realTimeInstant
        (repository, _) <- interviewCommand(fixture, wall)
        stale <- success(repository.claimDueCommands("stale", wall, 30.seconds, 1)).flatMap(claims =>
          IO.fromOption(claims.headOption)(new AssertionError("publication claim"))
        )
        _ <- LeaseTestSupport.expire(
          fixture.database,
          MongoCollections.InterviewWorkflowCommands,
          commandId(stale),
          "claimUntil"
        )
        lostBeforeTakeover <- success(repository.renewPublication(stale, 30.seconds))
        results <- (1 to claimers).toList.parTraverse(i =>
          success(repository.claimDueCommands(s"claimer-$i", wall, 30.seconds, 1))
        )
        lostAfterTakeover <- success(repository.renewPublication(stale, 30.seconds))
        late <- repository.markPublished(stale, wall).value
      } yield {
        assert(!lostBeforeTakeover)
        assertEquals(results.flatten.size, 1)
        assertNotEquals(results.flatten.head.fencingToken, stale.fencingToken)
        assert(!lostAfterTakeover)
        assertEquals(late, Left(RepositoryError.Conflict))
      }
    }
  }

  test("DDR-01.4 an Executing lease expired on the database clock refuses the effect fence for a stale caller clock") {
    mongoResource.use { fixture =>
      for {
        wall <- IO.realTimeInstant
        (repository, value) <- interviewCommand(fixture, wall)
        calendar = LedgerInterviewCalendarProvider.durable(repository)
        publication <- success(repository.claimDueCommands("publisher", wall, 30.seconds, 1)).flatMap(claims =>
          IO.fromOption(claims.headOption)(new AssertionError("publication claim"))
        )
        _ <- success(repository.markPublished(publication, wall))
        executing <- success(repository.claimExecution(publication.record, "executor", wall, 30.seconds, 5)).flatMap {
          case InterviewExecutionClaimOutcome.Acquired(claim) => IO.pure(claim)
          case other => IO.raiseError(new AssertionError(s"Missing execution claim: $other"))
        }
        _ <- LeaseTestSupport.expire(
          fixture.database,
          MongoCollections.InterviewWorkflowCommands,
          commandId(executing),
          "claimUntil"
        )
        key = InterviewWorkflow.reservationKey(value.id, 0)
        // The caller clock is an hour behind, so by it the lease would still be live.
        refused <- calendar
          .reserve(
            value.id,
            key,
            value.candidateId,
            value.recruiterId,
            value.interval,
            wall.minusSeconds(3600),
            Some(executing)
          )
          .value
        effect <- calendar.lookup(value.id, key).value
      } yield {
        assert(refused.isLeft, clue(refused))
        assertEquals(effect, Right(None))
      }
    }
  }

  test("DDR-01.3 a subject lease held by another live claim is a typed Conflict at the lease write itself") {
    mongoResource.use { fixture =>
      for {
        wall <- IO.realTimeInstant
        (repository, value) <- outboxWith(fixture, wall)
        holder <- outboxClaim(repository, "holder", wall, 30.seconds)
        subject = holder.head.subjectIds.head
        // A second event of the same subject goes straight to the lease write, bypassing the held-subject pre-read.
        other = outboxEvent(wall).copy(actorId = UserId(UUID.fromString(subject)))
        intruder = holder.head.copy(event = other, leaseToken = "intruder-token")
        runner = MongoTransactionRunner.sessions(
          fixture.client,
          RepositoryError.Conflict,
          diagnostics = Diagnostics.noop[IO]
        )
        offered <- runner
          .run(session =>
            repository
              .leaseSubjects(session, List(intruder), "hiring-publisher-intruder", "intruder-token", wall, 30.seconds)
          )
          .value
        fence <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          MongoCollections.OutboxSubjectFences,
          com.mongodb.client.model.Filters.eq(MongoFields.Id, subject)
        )
      } yield {
        assertEquals(offered, Left(RepositoryError.Conflict))
        assertEquals(fence.map(_.getString(MongoFields.LeaseToken)), Some(holder.head.leaseToken))
        assertEquals(value.eventId, holder.head.event.eventId)
      }
    }
  }
}
