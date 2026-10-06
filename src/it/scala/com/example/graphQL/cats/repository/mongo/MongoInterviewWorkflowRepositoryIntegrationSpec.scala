package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import com.example.graphQL.cats.domain.model.{ApplicationStatus, UserRole, User}
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, UserId}
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.{ActorContext, UseCaseError}
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.service.application.InterviewSchedulingService
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
          workflowRepository.advance(next._1, 0L, "result-reservation-1", next._2, now.plusSeconds(2))
        )
        duplicateInbox <- success(
          workflowRepository.advance(next._1, 0L, "result-reservation-1", next._2, now.plusSeconds(3))
        )
        stale <- success(
          workflowRepository.advance(next._1, 0L, "different-message", Nil, now.plusSeconds(4))
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
        _ <- success(workflowRepository.advance(statusPending._1, 0L, "reserved", statusPending._2, now))
        _ <- success(workflowRepository.commitHiring(statusPending._1, now))
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

  test("Admin initiated interview scheduling attributes the hiring history and outbox to the Admin") {
    MongoAccessEvaluationSupport.resource.use { fixture =>
      val repository = MongoInterviewWorkflowRepository.live(fixture.database, fixture.client, Diagnostics.noop)
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
        _ <- success(repository.advance(pending._1, initiated.revision, "reserved", pending._2, now))
        _ <- success(repository.commitHiring(pending._1, now))
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
      } yield {
        assertEquals(history.map(_.getString(MongoFields.ActorId)), Some(admin.id.value.toString))
        assertEquals(outbox.map(_.getString(MongoFields.ActorId)), Some(admin.id.value.toString))
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
        )
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
          repository.claimExecution(reservePublished.record, "repair-race-worker", claimedAt, claimedAt.plusSeconds(60))
        ).flatMap(value => IO.fromOption(value)(new AssertionError("Missing reservation execution")))
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
            claimedAt.plusSeconds(60)
          )
        ).flatMap(value => IO.fromOption(value)(new AssertionError("Missing notification execution")))
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
        _ <- success(repository.advance(failed._1, notifying.revision, "exhausted", failed._2, claimedAt))
        repaired <- success(
          repository.repair(failed._1, failed._1.revision, UUID.randomUUID(), claimedAt, value.recruiterId)
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
        first <- success(repository.claimExecution(command, "worker-old", fresh, fresh.plusMillis(300)))
          .flatMap(value => IO.fromOption(value)(new AssertionError("Missing execution claim")))
        _ <- IO.sleep(400.millis)
        currentAt <- IO.realTimeInstant
        second <- success(repository.claimExecution(command, "worker-new", currentAt, currentAt.plusSeconds(60)))
          .flatMap(value => IO.fromOption(value)(new AssertionError("Missing reclaimed execution")))
        staleReserve <- calendar
          .reserve(value.id, key, value.candidateId, value.recruiterId, value.interval, currentAt, Some(first))
          .value
        _ <- providerSuccess(
          calendar.reserve(value.id, key, value.candidateId, value.recruiterId, value.interval, currentAt, Some(second))
        )
        statusPending = InterviewWorkflow
          .decide(value, 0L, InterviewWorkflowEvent.ReservationConfirmed)
          .fold(error => fail(s"Invalid transition: $error"), identity)
        _ <- success(repository.advance(statusPending._1, 0L, "reservation-result", statusPending._2, currentAt))
        unknownCommit <- success(repository.commitHiring(statusPending._1, currentAt)) *>
          IO.raiseError[Unit](new IllegalStateException("simulated response loss after durable hiring commit")).attempt
        receipt <- success(repository.hasHiringReceipt(value.id))
        duplicateCommit <- repository.commitHiring(statusPending._1, currentAt).value
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
        _ <- success(repository.advance(pending._1, 0L, "reserved", pending._2, now))
        _ <- success(repository.commitHiring(pending._1, now))
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
        _ <- success(repository.advance(failed._1, notifying.revision, "exhausted", failed._2, now))
        repaired <- service
          .repair(ActorContext(admin.id, UserRole.Admin), value.id, failed._1.revision, repairKey)
          .value
          .flatMap(_.fold(error => IO.raiseError(new AssertionError(s"Repair: $error")), IO.pure))
        replay <- service.repair(ActorContext(admin.id, UserRole.Admin), value.id, failed._1.revision, repairKey).value
        stale <- service
          .repair(ActorContext(admin.id, UserRole.Admin), value.id, failed._1.revision - 1L, UUID.randomUUID())
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
          repository.advance(candidateConfirmed._1, repaired.revision, "candidate-lookup", candidateConfirmed._2, now)
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
            candidateConfirmed._1,
            candidateConfirmed._1.revision,
            InterviewWorkflowEvent.NotificationDelivered(InterviewParticipant.Recruiter)
          )
          .fold(error => fail(s"Transition: $error"), identity)
        _ <- success(
          repository.advance(completed._1, candidateConfirmed._1.revision, "recruiter-result", completed._2, now)
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
        assertEquals(completed._1.phase, InterviewWorkflowPhase.Completed)
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
}
