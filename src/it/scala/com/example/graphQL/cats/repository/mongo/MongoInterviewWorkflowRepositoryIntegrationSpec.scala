package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.{ApplicationEvent, ApplicationStatus, UserRole, User}
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, UserId}
import com.example.graphQL.cats.domain.policy.ApplicationLifecycle
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.{ActorContext, UseCaseError}
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.service.application.InterviewSchedulingService
import com.example.graphQL.cats.service.events.OperationalEvents
import com.example.graphQL.cats.service.port.*
import munit.CatsEffectSuite
import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*

final class MongoInterviewWorkflowRepositoryIntegrationSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes

  private val now = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS)
  private val startsAt = now.plusSeconds(172800)
  private val endsAt = startsAt.plusSeconds(3600)

  private def success[A](value: RepositoryIO[A]): IO[A] =
    value.value.flatMap(_.fold(error => IO.raiseError(new AssertionError(s"Repository failure: $error")), IO.pure))

  private def executionClaim(outcome: InterviewExecutionClaimOutcome): IO[ClaimedInterviewWorkflowCommand] =
    outcome match {
      case InterviewExecutionClaimOutcome.Acquired(claim) => IO.pure(claim)
      case other => IO.raiseError(new AssertionError(s"Expected execution claim, got $other"))
    }

  private def providerSuccess[A](value: InterviewProviderIO[A]): IO[A] =
    value.value.flatMap(_.fold(error => IO.raiseError(new AssertionError(s"Provider failure: $error")), IO.pure))

  private def seedSubjects(database: mongo4cats.database.MongoDatabase[IO], values: InterviewWorkflow*): IO[Unit] =
    InterviewSchedulingFixtures.seed(database, values.toList, now)

  private def workflow(
      id: UUID,
      application: UUID,
      candidate: UserId,
      recruiter: UserId,
      interval: InterviewInterval = InterviewInterval(startsAt, endsAt)
  ): InterviewWorkflow =
    InterviewWorkflow
      .create(
        InterviewWorkflowId(id),
        ApplicationId(application),
        candidate,
        recruiter,
        interval,
        now.plusSeconds(300),
        UUID.randomUUID(),
        ApplicationStatus.Accepted
      )
      .fold(error => fail(s"Invalid test workflow: $error"), identity)

  private def reserveRequest(value: InterviewWorkflow, at: Instant = now): InterviewCalendarReservation = {
    val reservationKey = InterviewWorkflow.initialCommand(value) match {
      case InterviewWorkflowCommand.ReserveCalendarSlot(key) => key
      case _                                                 => fail("Expected a reservation command")
    }
    InterviewCalendarReservation(
      value.id,
      reservationKey,
      value.candidateId,
      value.recruiterId,
      value.interval,
      at,
      None
    )
  }

  test("workflow create and advance are atomic, replayable, and guarded by revision and inbox identity") {
    MongoAccessEvaluationSupport.resource.use { fixture =>
      val workflowRepository = new MongoInterviewWorkflowRepository(
        fixture.database,
        MongoTransactionRunner.sessions(fixture.client, RepositoryError.Conflict, diagnostics = Diagnostics.noop),
        Diagnostics.noop
      )
      val candidate = UserId(UUID.randomUUID())
      val recruiter = UserId(UUID.randomUUID())
      val value = workflow(UUID.randomUUID(), UUID.randomUUID(), candidate, recruiter)
      val replayValue = value.copy(id = InterviewWorkflowId(UUID.randomUUID()))
      val requestKey = UUID.randomUUID()
      val fingerprint = MutationReceiptFingerprint.fromCanonicalInput("schedule:application:interval")
      val next = InterviewWorkflow
        .decide(value, 0L, InterviewWorkflowEvent.ReservationConfirmed)
        .fold(error => fail(s"Unexpected policy error: $error"), identity)
      val createdAt = now
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- seedSubjects(fixture.database, value)
        created <- success(
          workflowRepository.create(
            value,
            InterviewWorkflow.initialCommand(value),
            requestKey,
            fingerprint,
            createdAt
          )
        )
        replay <- success(
          workflowRepository.create(
            replayValue,
            InterviewWorkflow.initialCommand(replayValue),
            requestKey,
            fingerprint,
            createdAt.plusSeconds(1)
          )
        )
        wrongFingerprint <- workflowRepository
          .create(
            value,
            InterviewWorkflow.initialCommand(value),
            requestKey,
            MutationReceiptFingerprint.fromCanonicalInput("different request"),
            createdAt
          )
          .value
        visible <- success(
          workflowRepository.findForActor(value.id, InterviewWorkflowAccess(candidate, UserRole.Candidate))
        )
        forbidden <- success(
          workflowRepository.findForActor(
            value.id,
            InterviewWorkflowAccess(UserId(UUID.randomUUID()), UserRole.Candidate)
          )
        )
        advanced <- success(
          workflowRepository.advance(
            next.workflow,
            0L,
            InterviewAdvanceCause.ResultReceipt("result-reservation-1"),
            next.commands,
            now.plusSeconds(2)
          )
        )
        duplicateInbox <- success(
          workflowRepository.advance(
            next.workflow,
            0L,
            InterviewAdvanceCause.ResultReceipt("result-reservation-1"),
            next.commands,
            now.plusSeconds(3)
          )
        )
        stale <- success(
          workflowRepository.advance(
            next.workflow,
            0L,
            InterviewAdvanceCause.ResultReceipt("different-message"),
            Nil,
            now.plusSeconds(4)
          )
        )
        _ = assertEquals(created, InterviewWorkflowAdvanceResult.Applied)
        _ = assert(replay match {
          case InterviewWorkflowAdvanceResult.Duplicate(existing) => existing.id == value.id
          case _                                                  => false
        })
        _ = assertEquals(wrongFingerprint, Left(RepositoryError.Conflict))
        _ = assertEquals(visible.map(_.id), Some(value.id))
        _ = assertEquals(forbidden, None)
        _ = assertEquals(advanced, InterviewWorkflowAdvanceResult.Applied)
        _ = assert(duplicateInbox match {
          case InterviewWorkflowAdvanceResult.Duplicate(existing) => existing.revision == 1L
          case _                                                  => false
        })
        _ = assertEquals(stale, InterviewWorkflowAdvanceResult.StaleRevision)
        workflowRecord <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          MongoCollections.InterviewWorkflows,
          com.mongodb.client.model.Filters.eq(MongoFields.Id, value.id.value.toString)
        )
        ledgerRecord <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          MongoCollections.HiringMigrationLedger,
          com.mongodb.client.model.Filters.eq(MongoFields.Id, "007_interview_workflow_storage")
        )
      } yield {
        assertEquals(workflowRecord.map(_.getLong("revision").longValue()), Some(1L))
        assertEquals(ledgerRecord.map(_.getString(MongoFields.State)), Some("Complete"))
      }
    }
  }

  test("fake calendar reservations serialize overlaps, allow adjacency, and release idempotently") {
    MongoAccessEvaluationSupport.resource.use { fixture =>
      val workflowRepository = new MongoInterviewWorkflowRepository(
        fixture.database,
        MongoTransactionRunner.sessions(fixture.client, RepositoryError.Conflict, diagnostics = Diagnostics.noop),
        Diagnostics.noop
      )
      val calendar = FakeInterviewCalendarProvider.durable(workflowRepository)
      val candidate = UserId(UUID.randomUUID())
      val first = workflow(UUID.randomUUID(), UUID.randomUUID(), candidate, UserId(UUID.randomUUID()))
      val second = workflow(UUID.randomUUID(), UUID.randomUUID(), candidate, UserId(UUID.randomUUID()))
      val adjacent = workflow(
        UUID.randomUUID(),
        UUID.randomUUID(),
        candidate,
        UserId(UUID.randomUUID()),
        InterviewInterval(endsAt, endsAt.plusSeconds(3600))
      )
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- seedSubjects(fixture.database, first, second, adjacent)
        firstCreate <- success(
          workflowRepository.create(
            first,
            InterviewWorkflow.initialCommand(first),
            UUID.randomUUID(),
            MutationReceiptFingerprint.fromCanonicalInput("first"),
            now
          )
        )
        secondCreate <- success(
          workflowRepository.create(
            second,
            InterviewWorkflow.initialCommand(second),
            UUID.randomUUID(),
            MutationReceiptFingerprint.fromCanonicalInput("second"),
            now
          )
        )
        adjacentCreate <- success(
          workflowRepository.create(
            adjacent,
            InterviewWorkflow.initialCommand(adjacent),
            UUID.randomUUID(),
            MutationReceiptFingerprint.fromCanonicalInput("adjacent"),
            now
          )
        )
        outcomes <- IO.both(
          calendar
            .reserve(first.id, reserveRequest(first).idempotencyKey, candidate, first.recruiterId, first.interval, now)
            .value,
          calendar
            .reserve(
              second.id,
              reserveRequest(second).idempotencyKey,
              candidate,
              second.recruiterId,
              second.interval,
              now
            )
            .value
        )
        _ = assertEquals(
          List(firstCreate, secondCreate, adjacentCreate),
          List.fill(3)(InterviewWorkflowAdvanceResult.Applied)
        )
        _ = assertEquals(List(outcomes._1, outcomes._2).count(_.isRight), 1)
        _ = assertEquals(List(outcomes._1, outcomes._2).count(_ == Left(InterviewProviderError.Conflict)), 1)
        winner = if (outcomes._1.isRight) first else second
        loser = if (outcomes._1.isLeft) first else second
        winnerReservation <- providerSuccess(calendar.lookup(winner.id))
          .flatMap(IO.fromOption(_)(new AssertionError("Missing reservation")))
        _ = assertEquals(winnerReservation.releasedAt, None)
        _ <- Mongo4catsCollections
          .documents(fixture.database, MongoCollections.InterviewWorkflows)
          .flatMap(
            _.updateOne(
              MongoFilter.eq(MongoFields.Id, winner.id.value.toString).bson,
              MongoUpdate.set("phase", InterviewWorkflowPhase.CompensationPending.toString).bson
            )
          )
        released <- providerSuccess(calendar.release(s"${winner.id.value}:release", now.plusSeconds(1)))
        releasedAgain <- providerSuccess(calendar.release(s"${winner.id.value}:release", now.plusSeconds(2)))
        adjacentReservation <- providerSuccess(
          calendar.reserve(
            adjacent.id,
            reserveRequest(adjacent).idempotencyKey,
            candidate,
            adjacent.recruiterId,
            adjacent.interval,
            now
          )
        )
        retryLoser <- providerSuccess(
          calendar.reserve(
            loser.id,
            reserveRequest(loser).idempotencyKey,
            candidate,
            loser.recruiterId,
            loser.interval,
            now.plusSeconds(3)
          )
        )
      } yield {
        assertEquals(released, ())
        assertEquals(releasedAgain, ())
        assertEquals(adjacentReservation.workflowId, adjacent.id)
        assertEquals(retryLoser.workflowId, loser.id)
      }
    }
  }

  test("expired command claims cannot publish and notification receipts are idempotent") {
    MongoAccessEvaluationSupport.resource.use { fixture =>
      val workflowRepository = new MongoInterviewWorkflowRepository(
        fixture.database,
        MongoTransactionRunner.sessions(fixture.client, RepositoryError.Conflict, diagnostics = Diagnostics.noop),
        Diagnostics.noop
      )
      val notification = FakeInterviewNotificationProvider.durable(workflowRepository)
      val value = workflow(UUID.randomUUID(), UUID.randomUUID(), UserId(UUID.randomUUID()), UserId(UUID.randomUUID()))
      val claimAt = now.plusSeconds(10)
      val leaseUntil = claimAt.plusSeconds(30)
      val receipt = InterviewNotificationReceipt(
        value.id,
        value.candidateId,
        InterviewParticipant.Candidate,
        s"${value.id.value}:notify:Candidate",
        now.plusSeconds(5)
      )
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- seedSubjects(fixture.database, value)
        _ <- success(
          workflowRepository.create(
            value,
            InterviewWorkflow.initialCommand(value),
            UUID.randomUUID(),
            MutationReceiptFingerprint.fromCanonicalInput("claim"),
            now
          )
        )
        initial <- success(workflowRepository.claimDueCommands("worker-a", claimAt, leaseUntil, 1))
          .flatMap(claims => IO.fromOption(claims.headOption)(new AssertionError("Expected command claim")))
        expired <- workflowRepository.markPublished(initial, leaseUntil).value
        current <- success(workflowRepository.claimDueCommands("worker-b", leaseUntil, leaseUntil.plusSeconds(30), 1))
          .flatMap(claims => IO.fromOption(claims.headOption)(new AssertionError("Expected reclaimed command")))
        published <- workflowRepository.markPublished(current, leaseUntil.plusSeconds(1)).value
        _ <- providerSuccess(
          FakeInterviewCalendarProvider
            .durable(workflowRepository)
            .reserve(
              value.id,
              reserveRequest(value).idempotencyKey,
              value.candidateId,
              value.recruiterId,
              value.interval,
              now
            )
        )
        statusPending = InterviewWorkflow
          .decide(value, 0L, InterviewWorkflowEvent.ReservationConfirmed)
          .fold(error => fail(s"Invalid policy transition: $error"), identity)
        _ <- success(
          workflowRepository.advance(
            statusPending.workflow,
            0L,
            InterviewAdvanceCause.ResultReceipt("reserved"),
            statusPending.commands,
            now
          )
        )
        _ <- success(workflowRepository.commitHiring(statusPending.workflow, now))
        delivered <- providerSuccess(
          notification.notify(
            value.id,
            value.candidateId,
            InterviewParticipant.Candidate,
            receipt.idempotencyKey,
            receipt.deliveredAt
          )
        )
        duplicate <- providerSuccess(
          notification.notify(
            value.id,
            value.candidateId,
            InterviewParticipant.Candidate,
            receipt.idempotencyKey,
            receipt.deliveredAt.plusSeconds(1)
          )
        )
        lookedUp <- providerSuccess(notification.lookup(receipt.idempotencyKey))
          .flatMap(IO.fromOption(_)(new AssertionError("Missing notification receipt")))
      } yield {
        assertEquals(expired, Left(RepositoryError.Conflict))
        assertNotEquals(initial.fencingToken, current.fencingToken)
        assertEquals(published, Right(()))
        assertEquals(delivered, receipt)
        assertEquals(duplicate, receipt)
        assertEquals(lookedUp, receipt)
      }
    }
  }

  test("scheduling derives participants, scopes reads, rejects forbidden requests and replays durable requests") {
    MongoAccessEvaluationSupport.resource.use { fixture =>
      val repository = MongoInterviewWorkflowRepository.live(fixture.database, fixture.client, Diagnostics.noop)
      val users = MongoUserRepository.transactional(
        fixture.database,
        fixture.client,
        MongoEmbeddingWorkEnqueuer.disabled,
        Diagnostics.noop
      )
      val jobs = MongoJobRepository.transactional(
        fixture.database,
        fixture.client,
        MongoEmbeddingWorkEnqueuer.disabled,
        Diagnostics.noop
      )
      val applications = MongoApplicationRepository.transactional(fixture.database, fixture.client, Diagnostics.noop)
      val service = new InterviewSchedulingService(users, jobs, applications, repository, 5.minutes)
      val own = workflow(UUID.randomUUID(), UUID.randomUUID(), UserId(UUID.randomUUID()), UserId(UUID.randomUUID()))
      val other = workflow(UUID.randomUUID(), UUID.randomUUID(), UserId(UUID.randomUUID()), UserId(UUID.randomUUID()))
      val key = UUID.randomUUID()
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- seedSubjects(fixture.database, own, other)
        candidateForbidden <- service
          .schedule(ActorContext(own.candidateId, UserRole.Candidate), own.applicationId, startsAt, endsAt, key)
          .value
        recruiterForbidden <- service
          .schedule(ActorContext(other.recruiterId, UserRole.Recruiter), own.applicationId, startsAt, endsAt, key)
          .value
        scheduled <- service
          .schedule(ActorContext(own.recruiterId, UserRole.Recruiter), own.applicationId, startsAt, endsAt, key)
          .value
          .flatMap(_.fold(error => IO.raiseError(new AssertionError(s"Scheduling failed: $error")), IO.pure))
        candidateView <- service.inspect(ActorContext(own.candidateId, UserRole.Candidate), scheduled.id).value
        strangerView <- service.inspect(ActorContext(other.candidateId, UserRole.Candidate), scheduled.id).value
        recruiterRepair <- service
          .repair(ActorContext(own.recruiterId, UserRole.Recruiter), scheduled.id, 0L, UUID.randomUUID())
          .value
        _ <- Mongo4catsCollections
          .documents(fixture.database, MongoCollections.Applications)
          .flatMap(
            _.updateOne(
              MongoFilter.eq(MongoFields.Id, own.applicationId.value.toString).bson,
              MongoUpdate.set(MongoFields.Status, ApplicationStatus.Interview.toString).bson
            )
          )
        replay <- service
          .schedule(ActorContext(own.recruiterId, UserRole.Recruiter), own.applicationId, startsAt, endsAt, key)
          .value
        mismatch <- service
          .schedule(
            ActorContext(own.recruiterId, UserRole.Recruiter),
            own.applicationId,
            startsAt,
            endsAt.plusSeconds(1),
            key
          )
          .value
        _ <- success(repository.quarantine("invalid-record-one", now))
        _ <- success(repository.quarantine("invalid-record-two", now))
      } yield {
        assertEquals(candidateForbidden, Left(UseCaseError.Domain(DomainError.Forbidden)))
        assertEquals(recruiterForbidden, Left(UseCaseError.Domain(DomainError.Forbidden)))
        assertEquals(scheduled.candidateId, own.candidateId)
        assertEquals(scheduled.recruiterId, own.recruiterId)
        assertEquals(scheduled.initiatedBy, own.recruiterId)
        assertEquals(candidateView.map(_.id), Right(scheduled.id))
        assertEquals(strangerView, Left(UseCaseError.Domain(DomainError.NotFound("interviewWorkflow"))))
        assertEquals(recruiterRepair, Left(UseCaseError.Domain(DomainError.Forbidden)))
        assertEquals(replay.map(_.id), Right(scheduled.id))
        assertEquals(mismatch, Left(UseCaseError.Repository(RepositoryError.Conflict)))
      }
    }
  }

  test("Admin interview commit matches the lifecycle state, history and outbox and replays once") {
    MongoAccessEvaluationSupport.resource.use { fixture =>
      val repository = MongoInterviewWorkflowRepository.live(fixture.database, fixture.client, Diagnostics.noop)
      val applications = MongoApplicationRepository.transactional(fixture.database, fixture.client, Diagnostics.noop)
      val target = workflow(UUID.randomUUID(), UUID.randomUUID(), UserId(UUID.randomUUID()), UserId(UUID.randomUUID()))
      val admin = User(
        UserId(UUID.randomUUID()),
        None,
        "Scheduling audit admin",
        UserRole.Admin,
        None,
        now,
        adminSingleton = true
      )
      val initiated = target.copy(initiatedBy = admin.id)
      val eventId =
        UUID.nameUUIDFromBytes(s"${initiated.id.value}:status".getBytes(java.nio.charset.StandardCharsets.UTF_8))
      val calendar = FakeInterviewCalendarProvider.durable(repository)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- seedSubjects(fixture.database, target)
        _ <- Mongo4catsCollections
          .documents(fixture.database, MongoCollections.Users)
          .flatMap(_.insertOne(MongoHiringCodecs.user(admin)))
        _ <- success(
          repository.create(
            initiated,
            InterviewWorkflow.initialCommand(initiated),
            initiated.idempotencyKey,
            MutationReceiptFingerprint.fromCanonicalInput("admin-initiated-workflow"),
            now
          )
        )
        _ <- providerSuccess(
          calendar.reserve(
            initiated.id,
            s"${initiated.id.value}:reserve",
            initiated.candidateId,
            initiated.recruiterId,
            initiated.interval,
            now
          )
        )
        pending = InterviewWorkflow
          .decide(initiated, initiated.revision, InterviewWorkflowEvent.ReservationConfirmed)
          .fold(error => fail(s"Transition: $error"), identity)
        _ <- success(
          repository.advance(
            pending.workflow,
            initiated.revision,
            InterviewAdvanceCause.ResultReceipt("reserved"),
            pending.commands,
            now
          )
        )
        original <- success(applications.find(initiated.applicationId)).flatMap(value =>
          IO.fromOption(value)(new AssertionError("Missing accepted application"))
        )
        expected <- IO.fromEither(
          ApplicationLifecycle
            .changeStatus(ApplicationStatus.Interview, admin.id, now, None, None)
            .run(original)
            .leftMap(error => new AssertionError(s"Unexpected lifecycle rejection: $error"))
        )
        (expectedApplication, expectedChange) = expected
        expectedEvent = ApplicationEvent(
          ApplicationEventId(eventId),
          initiated.applicationId,
          Some(expectedChange.previousStatus),
          expectedChange.newStatus,
          expectedChange.actorId,
          expectedChange.occurredAt,
          expectedChange.feedback,
          expectedChange.reason
        )
        _ <- success(repository.commitHiring(pending.workflow, now))
        _ <- success(repository.commitHiring(pending.workflow, now.plusSeconds(1)))
        saved <- success(applications.find(initiated.applicationId))
        history <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          MongoCollections.ApplicationEvents,
          MongoFilter.eq(MongoFields.Id, eventId.toString).bson
        )
        outbox <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          MongoCollections.EventOutbox,
          MongoFilter.eq(MongoFields.Id, eventId.toString).bson
        )
        historyCount <- MongoRepositoryTestSupport.count(fixture.database, MongoCollections.ApplicationEvents)
        outboxCount <- MongoRepositoryTestSupport.count(fixture.database, MongoCollections.EventOutbox)
        receiptCount <- MongoRepositoryTestSupport.count(
          fixture.database,
          MongoCollections.InterviewWorkflowInbox,
          MongoFilter.eq(MongoFields.Id, s"${initiated.id.value}:hiring").bson
        )
        notificationCount <- MongoRepositoryTestSupport.count(
          fixture.database,
          MongoCollections.InterviewWorkflowCommands,
          MongoFilter
            .and(
              MongoFilter.eq("workflowId", initiated.id.value.toString),
              MongoFilter.eq("command.kind", "notify")
            )
            .bson
        )
        current <- success(repository.findForAdmin(initiated.id))
        expectedWorkflow = InterviewWorkflow
          .decide(pending.workflow, pending.workflow.revision, InterviewWorkflowEvent.StatusCommitted)
          .fold(error => fail(s"Transition: $error"), identity)
      } yield {
        assertEquals(saved, Some(expectedApplication))
        assertEquals(history.flatMap(MongoHiringCodecs.readEvent(_).toOption), Some(expectedEvent))
        assertEquals(
          outbox.flatMap(MongoHiringCodecs.readOperationalEvent(_).toOption),
          Some(OperationalEvents.statusChanged(eventId, expectedApplication, expectedEvent))
        )
        assertEquals(historyCount, 1L)
        assertEquals(outboxCount, 1L)
        assertEquals(receiptCount, 1L)
        assertEquals(notificationCount, 2L)
        assertEquals(current, Some(expectedWorkflow.workflow))
      }
    }
  }

  test("repair fences an in-flight notification result from its superseded live claim") {
    MongoAccessEvaluationSupport.resource.use { fixture =>
      val repository = MongoInterviewWorkflowRepository.live(fixture.database, fixture.client, Diagnostics.noop)
      val value = workflow(UUID.randomUUID(), UUID.randomUUID(), UserId(UUID.randomUUID()), UserId(UUID.randomUUID()))
      val calendar = FakeInterviewCalendarProvider.durable(repository)
      val notifications = FakeInterviewNotificationProvider.durable(repository)
      val worker = new com.example.graphQL.cats.service.application.InterviewWorkflowWorker(
        repository,
        calendar,
        notifications,
        com.example.graphQL.cats.service.application.InterviewWorkerSettings(
          "repair-race-worker",
          100.millis,
          60.seconds,
          10.seconds,
          5,
          1.second,
          30.seconds
        ),
        Diagnostics.noop
      )
      def stableId(identity: String): UUID =
        UUID.nameUUIDFromBytes(identity.getBytes(java.nio.charset.StandardCharsets.UTF_8))
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- seedSubjects(fixture.database, value)
        _ <- success(
          repository.create(
            value,
            InterviewWorkflow.initialCommand(value),
            value.idempotencyKey,
            MutationReceiptFingerprint.fromCanonicalInput("repair-completion-race"),
            now
          )
        )
        claimedAt <- IO.realTimeInstant
        reservePublished <- success(
          repository.claimDueCommands("repair-race-publisher", claimedAt, claimedAt.plusSeconds(60), 1)
        ).flatMap(value => IO.fromOption(value.headOption)(new AssertionError("Missing reservation publication")))
        _ <- success(repository.markPublished(reservePublished, claimedAt))
        reserveExecution <- success(
          repository.claimExecution(
            reservePublished.record,
            "repair-race-worker",
            claimedAt,
            claimedAt.plusSeconds(60),
            5
          )
        ).flatMap(executionClaim)
        _ <- providerSuccess(
          calendar.reserve(
            value.id,
            s"${value.id.value}:reserve",
            value.candidateId,
            value.recruiterId,
            value.interval,
            claimedAt,
            Some(reserveExecution)
          )
        )
        _ <- success(repository.recordResult(reserveExecution, InterviewCommandResult.Succeeded, claimedAt))
        reservationResult = InterviewMessage(
          stableId(s"${stableId(reserveExecution.record.stepId)}:result"),
          value.id.value,
          reserveExecution.record.stepId,
          InterviewStep.Reserve,
          reserveExecution.record.revision,
          stableId(reserveExecution.record.stepId),
          value.preCommitDeadline,
          Some(InterviewResult.Succeeded),
          reserveExecution.record.occurredAt
        )
        _ <- worker.receiveResult(reservationResult).map(assert(_))
        statusPending <- success(repository.findForAdmin(value.id)).flatMap(value =>
          IO.fromOption(value)(new AssertionError("Missing status-commit workflow"))
        )
        _ <- success(repository.commitHiring(statusPending, claimedAt))
        notificationPublished <- success(
          repository.claimDueCommands("repair-race-publisher", claimedAt, claimedAt.plusSeconds(60), 16)
        ).flatMap(commands =>
          IO.fromOption(commands.find(_.record.command match {
            case InterviewWorkflowCommand.Notify(InterviewParticipant.Candidate, _) => true
            case _                                                                  => false
          }))(new AssertionError("Missing candidate notification publication"))
        )
        _ <- success(repository.markPublished(notificationPublished, claimedAt))
        notificationExecution <- success(
          repository.claimExecution(
            notificationPublished.record,
            "repair-race-worker",
            claimedAt,
            claimedAt.plusSeconds(60),
            5
          )
        ).flatMap(executionClaim)
        _ <- providerSuccess(
          notifications.notify(
            value.id,
            value.candidateId,
            InterviewParticipant.Candidate,
            s"${value.id.value}:notify:Candidate",
            claimedAt,
            Some(notificationExecution)
          )
        )
        notifying <- success(repository.findForAdmin(value.id)).flatMap(value =>
          IO.fromOption(value)(new AssertionError("Missing notification workflow"))
        )
        failed = InterviewWorkflow
          .decide(notifying, notifying.revision, InterviewWorkflowEvent.RetryExhausted("notification"))
          .fold(error => fail(s"Transition: $error"), identity)
        _ <- success(
          repository.advance(
            failed.workflow,
            notifying.revision,
            InterviewAdvanceCause.ResultReceipt("exhausted"),
            failed.commands,
            claimedAt
          )
        )
        repaired <- success(
          repository.repair(failed.workflow, failed.workflow.revision, UUID.randomUUID(), claimedAt, value.recruiterId)
        )
        lateResult <- repository.recordResult(notificationExecution, InterviewCommandResult.Succeeded, claimedAt).value
        command <- success(repository.findCommand(value.id, notificationExecution.record.stepId))
        repairedStored <- success(repository.findForAdmin(value.id))
      } yield {
        assertEquals(lateResult, Left(RepositoryError.Conflict))
        assertEquals(command.map(_.state), Some(InterviewWorkflowCommandState.Superseded))
        assertEquals(repaired.phase, InterviewWorkflowPhase.NotificationsPending)
        assertEquals(repaired.notified, Set.empty)
        assertEquals(repairedStored.map(_.phase), Some(InterviewWorkflowPhase.NotificationsPending))
        assertEquals(repairedStored.map(_.notified), Some(Set.empty))
      }
    }
  }

  test("expired execution tokens cannot reserve and stale release cannot remove a committed reservation") {
    MongoAccessEvaluationSupport.resource.use { fixture =>
      val repository = MongoInterviewWorkflowRepository.live(fixture.database, fixture.client, Diagnostics.noop)
      val calendar = FakeInterviewCalendarProvider.durable(repository)
      val value = workflow(UUID.randomUUID(), UUID.randomUUID(), UserId(UUID.randomUUID()), UserId(UUID.randomUUID()))
      val key = s"${value.id.value}:reserve"
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- seedSubjects(fixture.database, value)
        _ <- success(
          repository.create(
            value,
            InterviewWorkflow.initialCommand(value),
            value.idempotencyKey,
            MutationReceiptFingerprint.fromCanonicalInput("stale-effect"),
            now
          )
        )
        fresh <- IO.realTimeInstant
        publishedClaim <- success(repository.claimDueCommands("publisher", fresh, fresh.plusSeconds(60), 1))
          .flatMap(value => IO.fromOption(value.headOption)(new AssertionError("Missing publication claim")))
        _ <- success(repository.markPublished(publishedClaim, fresh))
        command = publishedClaim.record
        first <- success(repository.claimExecution(command, "worker-old", fresh, fresh.plusMillis(300), 5))
          .flatMap(executionClaim)
        _ <- providerSuccess(
          calendar.reserve(value.id, key, value.candidateId, value.recruiterId, value.interval, fresh, Some(first))
        )
        currentAt = fresh.plusSeconds(1)
        recovered <- success(repository.claimExecution(command, "worker-new", currentAt, currentAt.plusSeconds(60), 5))
        staleReserve <- calendar
          .reserve(value.id, key, value.candidateId, value.recruiterId, value.interval, currentAt, Some(first))
          .value
        _ = assertEquals(recovered, InterviewExecutionClaimOutcome.ReconciliationQueued)
        statusPending = InterviewWorkflow
          .decide(value, 0L, InterviewWorkflowEvent.ReservationConfirmed)
          .fold(error => fail(s"Invalid transition: $error"), identity)
        _ <- success(
          repository.advance(
            statusPending.workflow,
            0L,
            InterviewAdvanceCause.ResultReceipt("reservation-result"),
            statusPending.commands,
            currentAt
          )
        )
        unknownCommit <- success(repository.commitHiring(statusPending.workflow, currentAt)) *>
          IO.raiseError[Unit](new IllegalStateException("simulated response loss after durable hiring commit")).attempt
        receipt <- success(repository.hasHiringReceipt(value.id))
        duplicateCommit <- repository.commitHiring(statusPending.workflow, currentAt).value
        staleRelease <- calendar.release(s"${value.id.value}:release", currentAt, Some(first)).value
        reserveAfterCommit <- calendar
          .reserve(value.id, key, value.candidateId, value.recruiterId, value.interval, currentAt)
          .value
        kept <- providerSuccess(calendar.lookup(value.id))
        staleResult <- repository.recordResult(first, InterviewCommandResult.Succeeded, currentAt).value
      } yield {
        assert(staleReserve.isLeft)
        assert(receipt)
        assert(unknownCommit.isLeft)
        assertEquals(duplicateCommit, Right(()))
        assert(staleRelease.isLeft)
        assert(reserveAfterCommit.isLeft)
        assertEquals(kept.flatMap(_.releasedAt), None)
        assertEquals(staleResult, Left(RepositoryError.Conflict))
      }
    }
  }

  test("Admin repair reconciles a partial notification receipt and completion retains deduplication evidence") {
    MongoAccessEvaluationSupport.resource.use { fixture =>
      val repository = MongoInterviewWorkflowRepository.live(fixture.database, fixture.client, Diagnostics.noop)
      val users = MongoUserRepository.transactional(
        fixture.database,
        fixture.client,
        MongoEmbeddingWorkEnqueuer.disabled,
        Diagnostics.noop
      )
      val jobs = MongoJobRepository.transactional(
        fixture.database,
        fixture.client,
        MongoEmbeddingWorkEnqueuer.disabled,
        Diagnostics.noop
      )
      val applications = MongoApplicationRepository.transactional(fixture.database, fixture.client, Diagnostics.noop)
      val service = new InterviewSchedulingService(users, jobs, applications, repository, 5.minutes)
      val value = workflow(UUID.randomUUID(), UUID.randomUUID(), UserId(UUID.randomUUID()), UserId(UUID.randomUUID()))
      val admin = User(
        UserId(UUID.randomUUID()),
        None,
        "Scheduling repair admin",
        UserRole.Admin,
        None,
        now,
        adminSingleton = true
      )
      val calendar = FakeInterviewCalendarProvider.durable(repository)
      val notifications = FakeInterviewNotificationProvider.durable(repository)
      val repairKey = UUID.randomUUID()
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- seedSubjects(fixture.database, value)
        _ <- Mongo4catsCollections
          .documents(fixture.database, MongoCollections.Users)
          .flatMap(_.insertOne(MongoHiringCodecs.user(admin)))
        _ <- success(
          repository.create(
            value,
            InterviewWorkflow.initialCommand(value),
            value.idempotencyKey,
            MutationReceiptFingerprint.fromCanonicalInput("partial-notification"),
            now
          )
        )
        _ <- providerSuccess(
          calendar.reserve(
            value.id,
            s"${value.id.value}:reserve",
            value.candidateId,
            value.recruiterId,
            value.interval,
            now
          )
        )
        pending = InterviewWorkflow
          .decide(value, 0L, InterviewWorkflowEvent.ReservationConfirmed)
          .fold(error => fail(s"Transition: $error"), identity)
        _ <- success(
          repository.advance(
            pending.workflow,
            0L,
            InterviewAdvanceCause.ResultReceipt("reserved"),
            pending.commands,
            now
          )
        )
        _ <- success(repository.commitHiring(pending.workflow, now))
        notifying <- success(repository.findForAdmin(value.id)).flatMap(value =>
          IO.fromOption(value)(new AssertionError("Missing workflow"))
        )
        delivered <- providerSuccess(
          notifications.notify(
            value.id,
            value.candidateId,
            InterviewParticipant.Candidate,
            s"${value.id.value}:notify:Candidate",
            now
          )
        )
        failed = InterviewWorkflow
          .decide(notifying, notifying.revision, InterviewWorkflowEvent.RetryExhausted("notification"))
          .fold(error => fail(s"Transition: $error"), identity)
        _ <- success(
          repository.advance(
            failed.workflow,
            notifying.revision,
            InterviewAdvanceCause.ResultReceipt("exhausted"),
            failed.commands,
            now
          )
        )
        repaired <- service
          .repair(ActorContext(admin.id, UserRole.Admin), value.id, failed.workflow.revision, repairKey)
          .value
          .flatMap(_.fold(error => IO.raiseError(new AssertionError(s"Repair: $error")), IO.pure))
        replay <- service
          .repair(ActorContext(admin.id, UserRole.Admin), value.id, failed.workflow.revision, repairKey)
          .value
        stale <- service
          .repair(ActorContext(admin.id, UserRole.Admin), value.id, failed.workflow.revision - 1L, UUID.randomUUID())
          .value
        found <- providerSuccess(notifications.lookup(delivered.idempotencyKey))
        candidateConfirmed = InterviewWorkflow
          .decide(
            repaired,
            repaired.revision,
            InterviewWorkflowEvent.NotificationLookupFound(InterviewParticipant.Candidate)
          )
          .fold(error => fail(s"Transition: $error"), identity)
        _ <- success(
          repository.advance(
            candidateConfirmed.workflow,
            repaired.revision,
            InterviewAdvanceCause.ResultReceipt("candidate-lookup"),
            candidateConfirmed.commands,
            now
          )
        )
        _ <- providerSuccess(
          notifications.notify(
            value.id,
            value.recruiterId,
            InterviewParticipant.Recruiter,
            s"${value.id.value}:notify:Recruiter",
            now
          )
        )
        completed = InterviewWorkflow
          .decide(
            candidateConfirmed.workflow,
            candidateConfirmed.workflow.revision,
            InterviewWorkflowEvent.NotificationDelivered(InterviewParticipant.Recruiter)
          )
          .fold(error => fail(s"Transition: $error"), identity)
        _ <- success(
          repository.advance(
            completed.workflow,
            candidateConfirmed.workflow.revision,
            InterviewAdvanceCause.ResultReceipt("recruiter-result"),
            completed.commands,
            now
          )
        )
        stored <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          MongoCollections.InterviewWorkflows,
          MongoFilter.eq(MongoFields.Id, value.id.value.toString).bson
        )
        reservation <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          MongoCollections.FakeInterviewCalendarReservations,
          MongoFilter.eq(MongoFields.Id, value.id.value.toString).bson
        )
        repairAudit <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          MongoCollections.InterviewWorkflowInbox,
          MongoFilter.eq(MongoFields.Id, s"${value.id.value}:repair:$repairKey").bson
        )
      } yield {
        assertEquals(found, Some(delivered))
        assertEquals(repaired.phase, InterviewWorkflowPhase.NotificationsPending)
        assertEquals(replay.map(_.id), Right(repaired.id))
        assertEquals(stale, Left(UseCaseError.Repository(RepositoryError.Conflict)))
        assertEquals(completed.workflow.phase, InterviewWorkflowPhase.Completed)
        assert(
          stored
            .flatMap(doc => Option(doc.getDate(MongoFields.RetentionExpiresAt)))
            .exists(_.toInstant.isAfter(now.plusSeconds(604800)))
        )
        assert(
          reservation
            .flatMap(doc => Option(doc.getDate(MongoFields.RetentionExpiresAt)))
            .exists(_.toInstant.isAfter(endsAt))
        )
        assertEquals(repairAudit.map(_.getString("auditActorId")), Some(admin.id.value.toString))
      }
    }
  }
  test("obsolete hiring results terminalize without repair or reservation release") {
    MongoAccessEvaluationSupport.resource.use { fixture =>
      val repository = MongoInterviewWorkflowRepository.live(fixture.database, fixture.client, Diagnostics.noop)
      val calendar = FakeInterviewCalendarProvider.durable(repository)
      val value = workflow(UUID.randomUUID(), UUID.randomUUID(), UserId(UUID.randomUUID()), UserId(UUID.randomUUID()))
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- seedSubjects(fixture.database, value)
        _ <- success(
          repository.create(
            value,
            InterviewWorkflow.initialCommand(value),
            value.idempotencyKey,
            MutationReceiptFingerprint.fromCanonicalInput("obsolete-hiring-result"),
            now
          )
        )
        _ <- providerSuccess(
          calendar.reserve(
            value.id,
            s"${value.id.value}:reserve",
            value.candidateId,
            value.recruiterId,
            value.interval,
            now
          )
        )
        pending = InterviewWorkflow
          .decide(value, 0L, InterviewWorkflowEvent.ReservationConfirmed)
          .toOption
          .getOrElse(fail("policy"))
        _ <- success(
          repository.advance(
            pending.workflow,
            0L,
            InterviewAdvanceCause.ResultReceipt("reserved"),
            pending.commands,
            now
          )
        )
        due <- success(repository.claimDueCommands("publisher", now, now.plusSeconds(3600), 16))
        commit <- IO.fromOption(
          due.find(_.record.command.isInstanceOf[InterviewWorkflowCommand.CommitAcceptedToInterview])
        )(new AssertionError("Missing hiring command"))
        _ <- success(repository.markPublished(commit, now))
        executing <- success(repository.claimExecution(commit.record, "worker", now, now.plusSeconds(3600), 5))
          .flatMap(executionClaim)
        _ <- success(repository.commitHiring(pending.workflow, now, Some(executing)))
        _ <- success(repository.recordResult(executing, InterviewCommandResult.Succeeded, now))
        result <- success(repository.claimDueCommands("publisher", now, now.plusSeconds(3600), 16)).flatMap(claims =>
          IO.fromOption(claims.find(_.record.stepId == commit.record.stepId))(new AssertionError("Missing result"))
        )
        resolution <- success(repository.requireRepair(result, now, "publication_exhausted"))
        after <- success(repository.findForAdmin(value.id))
        command <- success(repository.findCommand(value.id, commit.record.stepId))
        reservation <- providerSuccess(calendar.lookup(value.id))
      } yield {
        assertEquals(resolution, InterviewPublicationResolution.Superseded)
        assertEquals(after.map(_.phase), Some(InterviewWorkflowPhase.NotificationsPending))
        assertEquals(command.map(_.state), Some(InterviewWorkflowCommandState.Superseded))
        assertEquals(command.flatMap(_.result), Some(InterviewCommandResult.Succeeded))
        assertEquals(reservation.flatMap(_.releasedAt), None)
      }
    }
  }

  test("failed terminal persistence does not permit another send after publication budget exhaustion") {
    MongoAccessEvaluationSupport.resource.use { fixture =>
      val repository = MongoInterviewWorkflowRepository.live(fixture.database, fixture.client, Diagnostics.noop)
      val value = workflow(UUID.randomUUID(), UUID.randomUUID(), UserId(UUID.randomUUID()), UserId(UUID.randomUUID()))
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- seedSubjects(fixture.database, value)
        _ <- success(
          repository.create(
            value,
            InterviewWorkflow.initialCommand(value),
            value.idempotencyKey,
            MutationReceiptFingerprint.fromCanonicalInput("publication-budget"),
            now
          )
        )
        clock <- Ref.of[IO, Instant](now)
        sends <- Ref.of[IO, Int](0)
        transport = new InterviewTransport {
          private val generation = InterviewPublisherGeneration(InterviewPublisherRole.Orchestrator, UUID.randomUUID())
          override def generationFor(message: InterviewMessage): InterviewPublisherGeneration = generation
          override def publish(message: InterviewMessage): IO[Unit] =
            sends.update(_ + 1) *> clock.update(_.plusSeconds(61)) *>
              IO.raiseError(new java.io.IOException("simulated transport failure"))
        }
        worker = new com.example.graphQL.cats.service.application.InterviewWorkflowWorker(
          repository,
          FakeInterviewCalendarProvider.durable(repository),
          FakeInterviewNotificationProvider.durable(repository),
          com.example.graphQL.cats.service.application
            .InterviewWorkerSettings("budget", 1.second, 60.seconds, 10.seconds, 1, 1.second, 30.seconds),
          Diagnostics.noop,
          currentTime = clock.get
        )
        _ <- worker.publishDue(transport)
        before <- success(repository.findForAdmin(value.id))
        _ <- worker.publishDue(transport)
        after <- success(repository.findForAdmin(value.id))
        count <- sends.get
      } yield {
        assertEquals(before.map(_.phase), Some(InterviewWorkflowPhase.ReservationPending))
        assertEquals(count, 1)
        assertEquals(after.map(_.phase), Some(InterviewWorkflowPhase.RepairRequired))
      }
    }
  }

  test("concurrent execution claims charge one durable slot and expired claims queue reconciliation") {
    MongoAccessEvaluationSupport.resource.use { fixture =>
      val repository = MongoInterviewWorkflowRepository.live(fixture.database, fixture.client, Diagnostics.noop)
      val value = workflow(UUID.randomUUID(), UUID.randomUUID(), UserId(UUID.randomUUID()), UserId(UUID.randomUUID()))
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- seedSubjects(fixture.database, value)
        _ <- success(
          repository.create(
            value,
            InterviewWorkflow.initialCommand(value),
            value.idempotencyKey,
            MutationReceiptFingerprint.fromCanonicalInput("execution-budget"),
            now
          )
        )
        claimed <- success(repository.claimDueCommands("publisher", now, now.plusSeconds(60), 1))
          .flatMap(values => IO.fromOption(values.headOption)(new AssertionError("Missing command")))
        _ <- success(repository.markPublished(claimed, now))
        outcomes <- (
          repository.claimExecution(claimed.record, "a", now, now.plusSeconds(60), 1).value,
          repository.claimExecution(claimed.record, "b", now, now.plusSeconds(60), 1).value
        ).parTupled
        count <- success(repository.attemptCount(value.id, claimed.record.command))
        expired <- success(
          repository.claimExecution(claimed.record, "recovery", now.plusSeconds(61), now.plusSeconds(120), 1)
        )
        stored <- success(repository.findCommand(value.id, claimed.record.stepId))
        after <- success(repository.attemptCount(value.id, claimed.record.command))
      } yield {
        assertEquals(
          List(outcomes._1, outcomes._2).count(_.exists {
            case InterviewExecutionClaimOutcome.Acquired(_) => true
            case _                                          => false
          }),
          1
        )
        assertEquals(count, 1L)
        assertEquals(after, 1L)
        assertEquals(expired, InterviewExecutionClaimOutcome.ReconciliationQueued)
        assertEquals(stored.flatMap(_.result), Some(InterviewCommandResult.OutcomeUnknown))
      }
    }
  }

  test("migration resumes concurrently, preserves logical slots and converts unknown executions") {
    MongoAccessEvaluationSupport.resource.use { fixture =>
      val id = UUID.randomUUID().toString
      val queued = new org.bson.Document("_id", s"$id:$id:0:0")
        .append("workflowId", id)
        .append("stepId", s"$id:0:0")
        .append("revision", Long.box(0L))
        .append("availableAt", java.util.Date.from(now))
        .append("occurredAt", java.util.Date.from(now))
        .append("attempts", Int.box(3))
        .append("commandState", "Published")
        .append("command", new org.bson.Document("kind", "reserveCalendar").append("idempotencyKey", s"$id:reserve"))
      val uncertain = new org.bson.Document("_id", s"$id:$id:0:1")
        .append("workflowId", id)
        .append("stepId", s"$id:0:1")
        .append("revision", Long.box(0L))
        .append("availableAt", java.util.Date.from(now))
        .append("occurredAt", java.util.Date.from(now))
        .append("attempts", Int.box(2))
        .append("commandState", "Executing")
        .append("claimOwner", "old")
        .append("claimToken", UUID.randomUUID().toString)
        .append("claimUntil", java.util.Date.from(now))
        .append(
          "command",
          new org.bson.Document("kind", "lookupNotification")
            .append("idempotencyKey", s"$id:notify:Candidate")
        )
      for {
        collection <- Mongo4catsCollections.documents(fixture.database, MongoCollections.InterviewWorkflowCommands)
        _ <- collection.insertMany(List(queued, uncertain))
        _ <- (
          MongoHiringSetup.initialize(fixture.database, Diagnostics.noop),
          MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        ).parTupled
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        saved <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          MongoCollections.InterviewWorkflowCommands,
          MongoFilter.eq(MongoFields.Id, s"$id:$id:0:0").bson
        )
        recovered <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          MongoCollections.InterviewWorkflowCommands,
          MongoFilter.eq(MongoFields.Id, s"$id:$id:0:1").bson
        )
      } yield {
        assertEquals(saved.map(_.getInteger("executionAttempts").intValue()), Some(1))
        assertEquals(saved.map(_.getInteger("attempts").intValue()), Some(3))
        assertEquals(saved.map(_.getString("commandState")), Some("Published"))
        assertEquals(recovered.map(_.getInteger("executionAttempts").intValue()), Some(1))
        assertEquals(recovered.map(_.getString("result")), Some("OutcomeUnknown"))
        assertEquals(recovered.map(_.getString("commandState")), Some("ResultPending"))
        assertEquals(
          recovered.map(_.get("command", classOf[org.bson.Document]).getString("participant")),
          Some("Candidate")
        )
        assert(recovered.forall(!_.containsKey("claimToken")))
      }
    }
  }

  test("completed attempt migration validates subsequently malformed records at startup") {
    MongoAccessEvaluationSupport.resource.use { fixture =>
      val repository = MongoInterviewWorkflowRepository.live(fixture.database, fixture.client, Diagnostics.noop)
      val value = workflow(UUID.randomUUID(), UUID.randomUUID(), UserId(UUID.randomUUID()), UserId(UUID.randomUUID()))
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- seedSubjects(fixture.database, value)
        _ <- success(
          repository.create(
            value,
            InterviewWorkflow.initialCommand(value),
            value.idempotencyKey,
            MutationReceiptFingerprint.fromCanonicalInput("malformed-startup"),
            now
          )
        )
        collection <- Mongo4catsCollections.documents(fixture.database, MongoCollections.InterviewWorkflowCommands)
        _ <- collection.updateOne(
          com.mongodb.client.model.Filters.eq("workflowId", value.id.value.toString),
          com.mongodb.client.model.Updates.set("executionAttempts", -1)
        )
        rejected <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop).attempt
        stored <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          MongoCollections.InterviewWorkflowCommands,
          com.mongodb.client.model.Filters.eq("workflowId", value.id.value.toString)
        )
      } yield {
        assert(rejected.isLeft, "Complete ledger must not bypass current stored-contract validation")
        assertEquals(stored.map(_.getInteger("executionAttempts").intValue()), Some(-1))
      }
    }
  }

  test("completed attempt migration rejects mixed BSON identities beyond a full string batch and wrong ledger type") {
    MongoAccessEvaluationSupport.resource.use { fixture =>
      val id = UUID.randomUUID().toString
      val rows = (0 until 501).toList.map { index =>
        new org.bson.Document("_id", s"$id:$id:0:$index")
          .append("workflowId", id)
          .append("stepId", s"$id:0:$index")
          .append("revision", Long.box(0L))
          .append("availableAt", java.util.Date.from(now))
          .append("occurredAt", java.util.Date.from(now))
          .append("attempts", Int.box(0))
          .append("executionAttempts", Int.box(0))
          .append("commandState", "Published")
          .append("command", new org.bson.Document("kind", "reserveCalendar").append("idempotencyKey", s"$id:reserve"))
      }
      val malformedId = new org.bson.types.ObjectId()
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        collection <- Mongo4catsCollections.documents(fixture.database, MongoCollections.InterviewWorkflowCommands)
        _ <- collection.insertMany(rows :+ new org.bson.Document("_id", malformedId))
        rejected <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop).attempt
        _ = assert(rejected.isLeft, "non-string identity after 500 string rows must not evade validation")
        _ <- collection.deleteOne(com.mongodb.client.model.Filters.eq("_id", malformedId))
        valid <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop).attempt
        _ = assert(valid.isRight)
        ledger <- Mongo4catsCollections.documents(fixture.database, MongoCollections.HiringMigrationLedger)
        _ <- ledger.updateOne(
          com.mongodb.client.model.Filters.eq("_id", "010_interview_workflow_attempts"),
          com.mongodb.client.model.Updates.set("version", Int.box(1))
        )
        wrongType <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop).attempt
      } yield assert(wrongType.isLeft, "numeric equality must not accept an unsupported BSON ledger shape")
    }
  }

  test("necessary result exhaustion retains its receipt and enters repair; a prepaid final slot executes once") {
    MongoAccessEvaluationSupport.resource.use { fixture =>
      val repository = MongoInterviewWorkflowRepository.live(fixture.database, fixture.client, Diagnostics.noop)
      val value = workflow(UUID.randomUUID(), UUID.randomUUID(), UserId(UUID.randomUUID()), UserId(UUID.randomUUID()))
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- seedSubjects(fixture.database, value)
        _ <- success(
          repository.create(
            value,
            InterviewWorkflow.initialCommand(value),
            value.idempotencyKey,
            MutationReceiptFingerprint.fromCanonicalInput("prepaid-required-result"),
            now
          )
        )
        publication <- success(repository.claimDueCommands("prepaid", now, now.plusSeconds(600), 1)).map(_.head)
        _ <- success(repository.markPublished(publication, now))
        collection <- Mongo4catsCollections.documents(fixture.database, MongoCollections.InterviewWorkflowCommands)
        _ <- collection.updateOne(
          com.mongodb.client.model.Filters.eq("workflowId", value.id.value.toString),
          com.mongodb.client.model.Updates.set("executionAttempts", Int.box(1))
        )
        charged <- success(repository.findCommand(value.id, publication.record.stepId))
          .map(_.getOrElse(fail("missing command")))
        outcome <- success(repository.claimExecution(charged, "prepaid-execute", now, now.plusSeconds(600), 1))
        claim <- executionClaim(outcome)
        count <- success(repository.attemptCount(value.id, charged.command))
        _ = assertEquals(count, 1L, "a conservatively charged queued command reuses its only slot")
        _ <- success(repository.recordResult(claim, InterviewCommandResult.OutcomeUnknown, now))
        resultClaim <- success(repository.claimDueCommands("required-result", now, now.plusSeconds(600), 1)).map(_.head)
        repaired <- success(repository.requireRepair(resultClaim, now, "PublicationBudgetExhausted"))
        state <- success(repository.findForAdmin(value.id))
        result <- success(repository.findCommand(value.id, resultClaim.record.stepId))
      } yield {
        assertEquals(repaired, InterviewPublicationResolution.Repaired)
        assertEquals(state.map(_.phase), Some(InterviewWorkflowPhase.RepairRequired))
        assertEquals(result.flatMap(_.result), Some(InterviewCommandResult.OutcomeUnknown))
        assertEquals(result.map(_.state), Some(InterviewWorkflowCommandState.RepairRequired))
        assertEquals(result.map(_.executionAttempts), Some(1))
      }
    }
  }

  test("concurrent distinct intents cannot overdraw their shared capability budget") {
    MongoAccessEvaluationSupport.resource.use { fixture =>
      val repository = MongoInterviewWorkflowRepository.live(fixture.database, fixture.client, Diagnostics.noop)
      val value = workflow(UUID.randomUUID(), UUID.randomUUID(), UserId(UUID.randomUUID()), UserId(UUID.randomUUID()))
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- seedSubjects(fixture.database, value)
        _ <- success(
          repository.create(
            value,
            InterviewWorkflow.initialCommand(value),
            value.idempotencyKey,
            MutationReceiptFingerprint.fromCanonicalInput("concurrent-distinct-intents"),
            now
          )
        )
        publication <- success(repository.claimDueCommands("distinct", now, now.plusSeconds(600), 1)).map(_.head)
        _ <- success(repository.markPublished(publication, now))
        existing <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          MongoCollections.InterviewWorkflowCommands,
          com.mongodb.client.model.Filters.eq("workflowId", value.id.value.toString)
        )
        document = new org.bson.Document(existing.getOrElse(fail("missing intent")))
        duplicateStep = s"${value.id.value}:0:duplicate"
        _ = document.put("_id", s"${value.id.value}:$duplicateStep")
        _ = document.put("stepId", duplicateStep)
        collection <- Mongo4catsCollections.documents(fixture.database, MongoCollections.InterviewWorkflowCommands)
        _ <- collection.insertOne(document)
        duplicate <- success(repository.findCommand(value.id, duplicateStep))
          .map(_.getOrElse(fail("missing duplicate intent")))
        outcomes <- (
          repository.claimExecution(publication.record, "first-intent", now, now.plusSeconds(600), 1).value,
          repository.claimExecution(duplicate, "second-intent", now, now.plusSeconds(600), 1).value
        ).parTupled
        count <- success(repository.attemptCount(value.id, duplicate.command))
      } yield {
        assertEquals(
          List(outcomes._1, outcomes._2).count(_.exists {
            case InterviewExecutionClaimOutcome.Acquired(_) => true
            case _                                          => false
          }),
          1
        )
        assertEquals(count, 1L)
        assert(List(outcomes._1, outcomes._2).contains(Right(InterviewExecutionClaimOutcome.RepairRequired)))
      }
    }
  }

}
