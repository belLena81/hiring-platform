package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Ref}
import cats.data.EitherT
import com.example.graphQL.cats.domain.model.{ApplicationStatus, User, UserRole}
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, UserId}
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.application.{InterviewWorkflowWorker, InterviewWorkerSettings}
import com.example.graphQL.cats.service.port.*
import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*

/** Actual Mongo transactions and durable provider receipts; broker restarts are covered by the worker workload. */
final class InterviewWorkflowRecoveryIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 4.minutes
  private val settings =
    InterviewWorkerSettings("recovery-worker", 10.millis, 60.seconds, 10.seconds, 5, 1.second, 30.seconds)

  private def success[A](operation: RepositoryIO[A]): IO[A] = operation.value.flatMap {
    case Right(value) => IO.pure(value)
    case Left(error)  => IO.raiseError(new AssertionError(s"Repository failure: $error"))
  }

  private def workflow(now: Instant): InterviewWorkflow =
    InterviewWorkflow
      .create(
        InterviewWorkflowId(UUID.randomUUID()),
        ApplicationId(UUID.randomUUID()),
        UserId(UUID.randomUUID()),
        UserId(UUID.randomUUID()),
        InterviewInterval(
          now.plusSeconds(3600).truncatedTo(java.time.temporal.ChronoUnit.MILLIS),
          now.plusSeconds(7200).truncatedTo(java.time.temporal.ChronoUnit.MILLIS)
        ),
        now.plusSeconds(300),
        UUID.randomUUID(),
        ApplicationStatus.Accepted
      )
      .fold(error => fail(s"Invalid fixture: $error"), identity)

  private def drive(
      repository: MongoInterviewWorkflowRepository,
      worker: InterviewWorkflowWorker,
      id: InterviewWorkflowId,
      phases: Set[InterviewWorkflowPhase],
      remaining: Int = 1000
  ): IO[InterviewWorkflow] =
    for {
      outgoing <- Ref.of[IO, Vector[InterviewMessage]](Vector.empty)
      transport = new InterviewTransport {
        private val commandGeneration =
          InterviewPublisherGeneration(InterviewPublisherRole.Orchestrator, UUID.randomUUID())
        private val resultGeneration = InterviewPublisherGeneration(InterviewPublisherRole.Worker, UUID.randomUUID())
        override def generationFor(message: InterviewMessage): InterviewPublisherGeneration =
          if (message.result.nonEmpty) resultGeneration else commandGeneration
        override def publish(message: InterviewMessage): IO[Unit] = outgoing.update(_ :+ message)
      }
      _ <- worker.publishDue(transport)
      messages <- outgoing.get
      _ <- messages.foldLeft(IO.unit)((done, message) =>
        done *> (if (message.result.isEmpty) worker.receiveCommand(message)
                 else worker.receiveResult(message)).flatMap(durable =>
          if (durable) IO.unit else IO.raiseError(new AssertionError(s"Undurable ${message.step} record"))
        )
      )
      state <- success(repository.findForAdmin(id)).flatMap(value =>
        IO.fromOption(value)(new AssertionError("Missing workflow"))
      )
      result <-
        if (phases.contains(state.phase)) IO.pure(state)
        else if (remaining <= 0) IO.raiseError(new AssertionError(s"Workflow stuck: ${state.phase}"))
        else IO.sleep(50.millis) *> drive(repository, worker, id, phases, remaining - 1)
    } yield result

  test("a reserved slot surviving a provider crash is released after the precommit deadline") {
    mongoResource.use { fixture =>
      val repository = MongoInterviewWorkflowRepository.live(fixture.database, fixture.client, Diagnostics.noop)
      val calendar = FakeInterviewCalendarProvider.durable(repository)
      val notification = FakeInterviewNotificationProvider.durable(repository)
      val worker = new InterviewWorkflowWorker(repository, calendar, notification, settings, Diagnostics.noop)
      for {
        now <- IO.realTimeInstant
        original = workflow(now)
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- InterviewSchedulingFixtures.seed(fixture.database, List(original), now)
        acceptedAt <- IO.realTimeInstant
        value = original.copy(preCommitDeadline = acceptedAt.plusSeconds(10))
        _ <- success(
          repository.create(
            value,
            InterviewWorkflow.initialCommand(value),
            value.idempotencyKey,
            MutationReceiptFingerprint.fromCanonicalInput("deadline-crash"),
            acceptedAt
          )
        )
        reserved <- calendar
          .reserve(
            value.id,
            s"${value.id.value}:reserve",
            value.candidateId,
            value.recruiterId,
            value.interval,
            acceptedAt
          )
          .value
        _ = assert(reserved.isRight, s"Expected durable reservation, got $reserved")
        _ <- IO.realTimeInstant.flatMap(current =>
          IO.sleep(java.time.Duration.between(current, value.preCommitDeadline).toMillis.max(0L).millis + 200.millis)
        )
        repair <- drive(repository, worker, value.id, Set(InterviewWorkflowPhase.RepairRequired))
        finalReservation <- calendar.lookup(value.id).value
        committed <- success(repository.hasHiringReceipt(value.id))
        application <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          MongoCollections.Applications,
          MongoFilter.eq(MongoFields.Id, value.applicationId.value.toString).bson
        )
      } yield {
        assertEquals(repair.phase, InterviewWorkflowPhase.RepairRequired)
        assert(finalReservation.toOption.flatten.flatMap(_.releasedAt).nonEmpty)
        assert(!committed)
        assertEquals(application.map(_.getString(MongoFields.Status)), Some(ApplicationStatus.Accepted.toString))
      }
    }
  }

  test("Admin repair reconciles a released reservation without reopening it or extending its deadline") {
    mongoResource.use { fixture =>
      val repository = MongoInterviewWorkflowRepository.live(fixture.database, fixture.client, Diagnostics.noop)
      val calendar = FakeInterviewCalendarProvider.durable(repository)
      for {
        now <- IO.realTimeInstant.map(_.truncatedTo(java.time.temporal.ChronoUnit.MILLIS))
        value = workflow(now)
        afterDeadline = value.preCommitDeadline.plusSeconds(1)
        clock <- Ref.of[IO, Instant](afterDeadline)
        worker = new InterviewWorkflowWorker(
          repository,
          calendar,
          FakeInterviewNotificationProvider.durable(repository),
          settings,
          Diagnostics.noop,
          currentTime = clock.getAndUpdate(_.plusSeconds(1))
        )
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- InterviewSchedulingFixtures.seed(fixture.database, List(value), now)
        _ <- success(
          repository.create(
            value,
            InterviewWorkflow.initialCommand(value),
            value.idempotencyKey,
            MutationReceiptFingerprint.fromCanonicalInput("released-reservation-repair"),
            now
          )
        )
        reservation <- calendar
          .reserve(value.id, s"${value.id.value}:reserve", value.candidateId, value.recruiterId, value.interval, now)
          .value
        _ = assert(reservation.isRight)
        compensated <- drive(repository, worker, value.id, Set(InterviewWorkflowPhase.RepairRequired), remaining = 50)
        released <- calendar.lookup(value.id).value
        _ = assert(released.toOption.flatten.flatMap(_.releasedAt).nonEmpty)
        repaired <- success(
          repository.repair(
            compensated,
            compensated.revision,
            UUID.randomUUID(),
            afterDeadline,
            UserId(UUID.randomUUID())
          )
        )
        finalState <- drive(repository, worker, value.id, Set(InterviewWorkflowPhase.RepairRequired), remaining = 50)
        retained <- calendar.lookup(value.id).value
        committed <- success(repository.hasHiringReceipt(value.id))
        reservationCount <- MongoRepositoryTestSupport.count(
          fixture.database,
          MongoCollections.FakeInterviewCalendarReservations
        )
        historyCount <- MongoRepositoryTestSupport.count(fixture.database, MongoCollections.ApplicationEvents)
        outboxCount <- MongoRepositoryTestSupport.count(fixture.database, MongoCollections.EventOutbox)
        application <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          MongoCollections.Applications,
          MongoFilter.eq(MongoFields.Id, value.applicationId.value.toString).bson
        )
      } yield {
        assertEquals(repaired.phase, InterviewWorkflowPhase.ReservationPending)
        assertEquals(repaired.preCommitDeadline, value.preCommitDeadline)
        assertEquals(finalState.preCommitDeadline, value.preCommitDeadline)
        assertEquals(finalState.phase, InterviewWorkflowPhase.RepairRequired)
        assert(finalState.revision > repaired.revision)
        assertEquals(retained, released)
        assertEquals(reservationCount, 1L)
        assert(!committed)
        assertEquals(historyCount, 0L)
        assertEquals(outboxCount, 0L)
        assertEquals(application.map(_.getString(MongoFields.Status)), Some(ApplicationStatus.Accepted.toString))
      }
    }
  }

  test("compensation exhausts five durable attempts and retains uncertain reservation for visible repair") {
    mongoResource.use { fixture =>
      val repository = MongoInterviewWorkflowRepository.live(fixture.database, fixture.client, Diagnostics.noop)
      val calendar = FakeInterviewCalendarProvider.durable(repository)
      val failedRelease = new InterviewCalendarProvider {
        override def reserve(
            id: InterviewWorkflowId,
            key: String,
            candidate: UserId,
            recruiter: UserId,
            interval: InterviewInterval,
            at: Instant,
            execution: Option[ClaimedInterviewWorkflowCommand]
        ): InterviewProviderIO[InterviewCalendarReservation] =
          calendar.reserve(id, key, candidate, recruiter, interval, at, execution)
        override def lookup(id: InterviewWorkflowId): InterviewProviderIO[Option[InterviewCalendarReservation]] =
          calendar.lookup(id)
        override def release(
            key: String,
            at: Instant,
            execution: Option[ClaimedInterviewWorkflowCommand]
        ): InterviewProviderIO[Unit] =
          EitherT.leftT[IO, Unit](InterviewProviderError.Unavailable)
      }
      val worker = new InterviewWorkflowWorker(
        repository,
        failedRelease,
        FakeInterviewNotificationProvider.durable(repository),
        settings,
        Diagnostics.noop
      )
      for {
        now <- IO.realTimeInstant
        value = workflow(now)
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- InterviewSchedulingFixtures.seed(fixture.database, List(value), now)
        _ <- success(
          repository.create(
            value,
            InterviewWorkflow.initialCommand(value),
            value.idempotencyKey,
            MutationReceiptFingerprint.fromCanonicalInput("compensation"),
            now
          )
        )
        initialReservation <- calendar
          .reserve(value.id, s"${value.id.value}:reserve", value.candidateId, value.recruiterId, value.interval, now)
          .value
        _ = assert(initialReservation.isRight, s"Expected durable reservation, got $initialReservation")
        _ <- Mongo4catsCollections
          .documents(fixture.database, MongoCollections.Applications)
          .flatMap(
            _.updateOne(
              MongoFilter.eq(MongoFields.Id, value.applicationId.value.toString).bson,
              MongoUpdate.set(MongoFields.Status, ApplicationStatus.Created.toString).bson
            )
          )
        repair <- drive(repository, worker, value.id, Set(InterviewWorkflowPhase.RepairRequired))
        attempts <- success(
          repository.attemptCount(value.id, InterviewWorkflowCommand.ReleaseCalendarSlot(s"${value.id.value}:release"))
        )
        reservation <- calendar.lookup(value.id).value
        committed <- success(repository.hasHiringReceipt(value.id))
      } yield {
        assertEquals(repair.phase, InterviewWorkflowPhase.RepairRequired)
        assertEquals(attempts, 5L)
        assert(reservation.toOption.flatten.nonEmpty)
        assertEquals(reservation.toOption.flatten.flatMap(_.releasedAt), None)
        assert(!committed)
      }
    }
  }

  test("notification exhaustion retains committed hiring and Admin repair resets the bounded attempt generation") {
    mongoResource.use { fixture =>
      val repository = MongoInterviewWorkflowRepository.live(fixture.database, fixture.client, Diagnostics.noop)
      val calendar = FakeInterviewCalendarProvider.durable(repository)
      val notification = FakeInterviewNotificationProvider.durable(repository)
      val failedNotification = new InterviewNotificationProvider {
        override def notify(
            id: InterviewWorkflowId,
            recipient: UserId,
            participant: InterviewParticipant,
            key: String,
            at: Instant,
            execution: Option[ClaimedInterviewWorkflowCommand]
        ): InterviewProviderIO[InterviewNotificationReceipt] =
          if (participant == InterviewParticipant.Candidate)
            EitherT.leftT[IO, InterviewNotificationReceipt](InterviewProviderError.Unavailable)
          else notification.notify(id, recipient, participant, key, at, execution)
        override def lookup(key: String): InterviewProviderIO[Option[InterviewNotificationReceipt]] =
          if (key.endsWith(":Candidate"))
            EitherT.leftT[IO, Option[InterviewNotificationReceipt]](InterviewProviderError.Unavailable)
          else notification.lookup(key)
      }
      val failingWorker =
        new InterviewWorkflowWorker(repository, calendar, failedNotification, settings, Diagnostics.noop)
      val recoveredWorker = new InterviewWorkflowWorker(repository, calendar, notification, settings, Diagnostics.noop)
      for {
        now <- IO.realTimeInstant
        value = workflow(now)
        admin = User(
          UserId(UUID.randomUUID()),
          None,
          "Recovery admin",
          UserRole.Admin,
          None,
          now,
          adminSingleton = true
        )
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- InterviewSchedulingFixtures.seed(fixture.database, List(value), now)
        _ <- Mongo4catsCollections
          .documents(fixture.database, MongoCollections.Users)
          .flatMap(_.insertOne(MongoHiringCodecs.user(admin)))
        _ <- success(
          repository.create(
            value,
            InterviewWorkflow.initialCommand(value),
            value.idempotencyKey,
            MutationReceiptFingerprint.fromCanonicalInput("notification-exhaustion"),
            now
          )
        )
        repair <- drive(repository, failingWorker, value.id, Set(InterviewWorkflowPhase.RepairRequired))
        candidateKey = s"${value.id.value}:notify:Candidate"
        attempts <- success(
          repository.attemptCount(
            value.id,
            InterviewWorkflowCommand.LookupNotificationReceipt(InterviewParticipant.Candidate, candidateKey)
          )
        )
        committed <- success(repository.hasHiringReceipt(value.id))
        kept <- calendar.lookup(value.id).value
        storedRepair <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          MongoCollections.InterviewWorkflows,
          MongoFilter.eq(MongoFields.Id, value.id.value.toString).bson
        )
        _ <- success(repository.repair(repair, repair.revision, UUID.randomUUID(), now, admin.id))
        completed <- drive(repository, recoveredWorker, value.id, Set(InterviewWorkflowPhase.Completed))
        newAttempts <- success(
          repository.attemptCount(
            value.id,
            InterviewWorkflowCommand.LookupNotificationReceipt(InterviewParticipant.Candidate, candidateKey)
          )
        )
        candidateReceipt <- notification.lookup(candidateKey).value
      } yield {
        assertEquals(attempts, 5L)
        assert(committed)
        assertEquals(repair.notified, Set(InterviewParticipant.Recruiter))
        assertEquals(kept.toOption.flatten.flatMap(_.releasedAt), None)
        assert(storedRepair.exists(!_.containsKey(MongoFields.RetentionExpiresAt)))
        assertEquals(completed.phase, InterviewWorkflowPhase.Completed)
        assertEquals(newAttempts, 2L)
        assert(candidateReceipt.toOption.flatten.nonEmpty)
      }
    }
  }

  test("actual account deletion fences original command and result replay before and after workflow purge") {
    mongoResource.use { fixture =>
      val repository = MongoInterviewWorkflowRepository.live(fixture.database, fixture.client, Diagnostics.noop)
      val calendar = FakeInterviewCalendarProvider.durable(repository)
      val worker = new InterviewWorkflowWorker(
        repository,
        calendar,
        FakeInterviewNotificationProvider.durable(repository),
        settings,
        Diagnostics.noop
      )
      val users = new MongoUserRepository(
        fixture.database,
        MongoTransactionRunner.sessions(fixture.client, RepositoryError.Conflict, diagnostics = Diagnostics.noop),
        MongoEmbeddingWorkEnqueuer.disabled,
        Diagnostics.noop
      )
      val cleanup = new MongoInterviewSubjectCleanup(fixture.database)
      for {
        now <- IO.realTimeInstant
        value = workflow(now)
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- InterviewSchedulingFixtures.seed(fixture.database, List(value), now)
        _ <- success(
          repository.create(
            value,
            InterviewWorkflow.initialCommand(value),
            value.idempotencyKey,
            MutationReceiptFingerprint.fromCanonicalInput("deletion-replay"),
            now
          )
        )
        outgoing <- Ref.of[IO, Vector[InterviewMessage]](Vector.empty)
        transport = new InterviewTransport {
          private val commandGeneration =
            InterviewPublisherGeneration(InterviewPublisherRole.Orchestrator, UUID.randomUUID())
          private val resultGeneration = InterviewPublisherGeneration(InterviewPublisherRole.Worker, UUID.randomUUID())
          override def generationFor(message: InterviewMessage): InterviewPublisherGeneration =
            if (message.result.nonEmpty) resultGeneration else commandGeneration
          override def publish(message: InterviewMessage): IO[Unit] = outgoing.update(_ :+ message)
        }
        _ <- worker.publishDue(transport)
        command <- outgoing.get.map(_.head)
        reserved <- worker.receiveCommand(command)
        _ = assert(reserved)
        _ <- outgoing.set(Vector.empty)
        _ <- worker.publishDue(transport)
        result <- outgoing.get.map(_.head)
        deletionAt <- IO.realTimeInstant
        _ <- success(
          users.deleteAccount(value.candidateId, deletionAt, "deleted-account", MutationWriteContext.directWrite)
        )
        fencedEffect <- calendar
          .reserve(
            value.id,
            s"${value.id.value}:reserve",
            value.candidateId,
            value.recruiterId,
            value.interval,
            deletionAt
          )
          .value
        _ = assert(fencedEffect.isLeft)
        barriers = Vector(
          com.example.graphQL.cats.service.port.InterviewRetentionBarrier("hiring.interview-commands", 0, 1L),
          com.example.graphQL.cats.service.port.InterviewRetentionBarrier("hiring.interview-results", 0, 1L)
        )
        cleaner = new com.example.graphQL.cats.service.application.InterviewSubjectCleanupWorker(
          cleanup,
          new InterviewPublisherFencer {
            override def fence(ids: Vector[String]): RepositoryIO[Unit] = RepositoryIO.fromEither(Right(()))
          },
          IO.pure(barriers),
          _ => IO.pure(false),
          Diagnostics.noop
        )
        _ <- success(cleaner.runOnce(None))
        _ <- success(cleaner.runOnce(None))
        _ <- success(cleaner.runOnce(None))
        commandAcknowledged <- worker.receiveCommand(command)
        resultAcknowledged <- worker.receiveResult(result)
        pending <- cleanup.complete(value.candidateId)
        workflowCount <- MongoRepositoryTestSupport.count(fixture.database, MongoCollections.InterviewWorkflows)
        reservationCount <- MongoRepositoryTestSupport.count(
          fixture.database,
          MongoCollections.FakeInterviewCalendarReservations
        )
        notificationCount <- MongoRepositoryTestSupport.count(
          fixture.database,
          MongoCollections.FakeInterviewNotificationReceipts
        )
        commandCount <- MongoRepositoryTestSupport.count(fixture.database, MongoCollections.InterviewWorkflowCommands)
      } yield {
        assert(commandAcknowledged && resultAcknowledged)
        assert(!pending)
        assertEquals(workflowCount, 0L)
        assertEquals(reservationCount, 0L)
        assertEquals(notificationCount, 0L)
        assertEquals(commandCount, 0L)
      }
    }
  }

}
