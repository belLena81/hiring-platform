package com.example.graphQL.cats.service.application

import cats.effect.{IO, Ref}
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField}
import com.example.graphQL.cats.service.port.*
import java.time.Instant
import java.util.UUID
import munit.CatsEffectSuite
import scala.concurrent.duration.*

final class InterviewWorkflowWorkerSpec extends CatsEffectSuite {
  private def unexpected[A]: RepositoryIO[A] =
    RepositoryIO.lift(IO.raiseError(new AssertionError("Unexpected repository operation")))

  private class RepositoryStub extends InterviewWorkflowRepository {
    override def claimDueCommands(workerId: String, now: Instant, leaseUntil: Instant, limit: Int) =
      RepositoryIO.fromEither[List[ClaimedInterviewWorkflowCommand]](Left(RepositoryError.Unavailable))
    override def claimExecution(
        command: InterviewWorkflowCommandRecord,
        workerId: String,
        now: Instant,
        leaseUntil: Instant,
        maxAttempts: Int
    ) =
      unexpected[InterviewExecutionClaimOutcome]
    override def authorizePublication(
        claim: ClaimedInterviewWorkflowCommand,
        generation: InterviewPublisherGeneration,
        now: Instant
    ) = unexpected[Boolean]
    override def attemptCount(workflowId: InterviewWorkflowId, command: InterviewWorkflowCommand) = unexpected[Long]
    override def quarantine(identity: String, now: Instant) = unexpected[Unit]
    override def recordResult(claim: ClaimedInterviewWorkflowCommand, result: InterviewCommandResult, now: Instant) =
      unexpected[Unit]
    override def findRequest(recruiterId: UserId, requestKey: UUID, fingerprint: MutationReceiptFingerprint) =
      unexpected[Option[InterviewWorkflow]]
    override def findCommand(workflowId: InterviewWorkflowId, stepId: String) =
      unexpected[Option[InterviewWorkflowCommandRecord]]
    override def repair(
        workflow: InterviewWorkflow,
        expectedRevision: Long,
        requestKey: UUID,
        now: Instant,
        actorId: UserId
    ) = unexpected[InterviewWorkflow]
    override def commitHiring(
        workflow: InterviewWorkflow,
        now: Instant,
        execution: Option[ClaimedInterviewWorkflowCommand]
    ) = unexpected[Unit]
    override def hasHiringReceipt(workflowId: InterviewWorkflowId) = unexpected[Boolean]
    override def create(
        workflow: InterviewWorkflow,
        initialCommand: InterviewWorkflowCommand,
        requestKey: UUID,
        fingerprint: MutationReceiptFingerprint,
        createdAt: Instant
    ) = unexpected[InterviewWorkflowAdvanceResult]
    override def findForActor(workflowId: InterviewWorkflowId, access: InterviewWorkflowAccess) =
      unexpected[Option[InterviewWorkflow]]
    override def findForAdmin(workflowId: InterviewWorkflowId) = unexpected[Option[InterviewWorkflow]]
    override def advance(
        workflow: InterviewWorkflow,
        expectedRevision: Long,
        cause: InterviewAdvanceCause,
        commands: List[InterviewWorkflowCommand],
        occurredAt: Instant,
        availableAt: Option[Instant]
    ) = unexpected[InterviewWorkflowAdvanceResult]
    override def markPublished(claim: ClaimedInterviewWorkflowCommand, publishedAt: Instant) = unexpected[Unit]
    override def retry(
        claim: ClaimedInterviewWorkflowCommand,
        now: Instant,
        availableAt: Instant,
        failureCode: String
    ) = unexpected[Unit]
    override def requireRepair(claim: ClaimedInterviewWorkflowCommand, now: Instant, failureCode: String) =
      unexpected[InterviewPublicationResolution]
  }

  private val repository = new RepositoryStub

  private def unexpectedProvider[A]: InterviewProviderIO[A] =
    cats.data.EitherT.liftF(IO.raiseError[A](new AssertionError("Unexpected provider operation")))

  private val calendar = new InterviewCalendarProvider {
    override def reserve(
        workflowId: InterviewWorkflowId,
        idempotencyKey: String,
        candidateId: UserId,
        recruiterId: UserId,
        interval: InterviewInterval,
        now: Instant,
        execution: Option[ClaimedInterviewWorkflowCommand]
    ) = unexpectedProvider[InterviewCalendarReservation]
    override def lookup(workflowId: InterviewWorkflowId) = unexpectedProvider[Option[InterviewCalendarReservation]]
    override def release(idempotencyKey: String, now: Instant, execution: Option[ClaimedInterviewWorkflowCommand]) =
      unexpectedProvider[Unit]
  }
  private val notifications = new InterviewNotificationProvider {
    override def notify(
        workflowId: InterviewWorkflowId,
        recipientId: UserId,
        participant: InterviewParticipant,
        idempotencyKey: String,
        now: Instant,
        execution: Option[ClaimedInterviewWorkflowCommand]
    ) = unexpectedProvider[InterviewNotificationReceipt]
    override def lookup(idempotencyKey: String) = unexpectedProvider[Option[InterviewNotificationReceipt]]
  }
  private val transport = new InterviewTransport {
    override def generationFor(message: InterviewMessage) =
      throw new AssertionError("No publication should be prepared after a failed claim")
    override def publish(message: InterviewMessage) =
      IO.raiseError(new AssertionError("No publication should follow a failed claim"))
  }

  test(
    "a failed publication claim emits the supplied sanitized diagnostic and performs no provider or transport effects"
  ) {
    for {
      events <- Ref.of[IO, Vector[(LogEvent, Map[LogField, String])]](Vector.empty)
      diagnostics = new Diagnostics {
        override def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]) =
          events.update(_ :+ (event, fields))
      }
      worker = new InterviewWorkflowWorker(
        repository,
        calendar,
        notifications,
        InterviewWorkerSettings("interview-worker", 1.second, 60.seconds, 10.seconds, 5, 1.second, 30.seconds),
        diagnostics
      )
      _ <- worker.publishDue(transport)
      observed <- events.get
    } yield assertEquals(
      observed,
      Vector((LogEvent.MongoRepositoryFailed, Map(LogField.SpanName -> "interviewWorkflow.claimPublication")))
    )
  }

  private val observedAt = Instant.parse("2026-10-06T12:00:00Z")
  private val workflow = InterviewWorkflow(
    InterviewWorkflowId(new UUID(0L, 1L)),
    com.example.graphQL.cats.domain.model.Identifiers.ApplicationId(new UUID(0L, 2L)),
    UserId(new UUID(0L, 3L)),
    UserId(new UUID(0L, 4L)),
    InterviewInterval(observedAt.plusSeconds(600), observedAt.plusSeconds(1200)),
    observedAt.plusSeconds(300),
    new UUID(0L, 5L),
    0L,
    InterviewWorkflowPhase.ReservationPending,
    Set.empty,
    UserId(new UUID(0L, 4L))
  )
  private val command = InterviewWorkflowCommandRecord(
    workflow.id,
    "reservation-step",
    0L,
    InterviewWorkflow.initialCommand(workflow),
    InterviewWorkflowCommandState.Published,
    1,
    observedAt,
    observedAt
  )
  private def stableId(value: String): UUID =
    UUID.nameUUIDFromBytes(value.getBytes(java.nio.charset.StandardCharsets.UTF_8))
  private def messageFor(
      record: InterviewWorkflowCommandRecord,
      current: InterviewWorkflow,
      step: InterviewStep,
      result: Option[InterviewResult]
  ): InterviewMessage = {
    val commandId = stableId(record.stepId)
    InterviewMessage(
      if (result.isDefined) stableId(s"$commandId:result") else commandId,
      current.id.value,
      record.stepId,
      step,
      record.revision,
      if (result.isDefined) commandId else current.id.value,
      current.preCommitDeadline,
      result,
      record.occurredAt
    )
  }
  private val message = messageFor(command, workflow, InterviewStep.Reserve, None)
  private final case class DeliveryObservation(
      acknowledged: Boolean,
      quarantined: Vector[String],
      claims: Int,
      advanced: Vector[InterviewWorkflow]
  )
  private def observeDelivery(
      delivery: InterviewMessage,
      resultDelivery: Boolean = false,
      stored: Option[InterviewWorkflowCommandRecord] = Some(command),
      current: Option[InterviewWorkflow] = Some(workflow)
  ): IO[DeliveryObservation] =
    for {
      quarantined <- Ref.of[IO, Vector[String]](Vector.empty)
      claims <- Ref.of[IO, Int](0)
      advanced <- Ref.of[IO, Vector[InterviewWorkflow]](Vector.empty)
      store = new RepositoryStub {
        override def findForAdmin(id: InterviewWorkflowId) = RepositoryIO.fromEither(Right(current.filter(_.id == id)))
        override def findCommand(id: InterviewWorkflowId, step: String) =
          RepositoryIO.fromEither(Right(stored.filter(record => record.workflowId == id && record.stepId == step)))
        override def quarantine(identity: String, now: Instant) = RepositoryIO.lift(quarantined.update(_ :+ identity))
        override def claimExecution(
            record: InterviewWorkflowCommandRecord,
            workerId: String,
            now: Instant,
            leaseUntil: Instant,
            maxAttempts: Int
        ) = RepositoryIO.lift(claims.update(_ + 1).as(InterviewExecutionClaimOutcome.AlreadyHandled))
        override def attemptCount(id: InterviewWorkflowId, requested: InterviewWorkflowCommand) =
          RepositoryIO.fromEither(Right(0L))
        override def advance(
            next: InterviewWorkflow,
            expectedRevision: Long,
            cause: InterviewAdvanceCause,
            commands: List[InterviewWorkflowCommand],
            occurredAt: Instant,
            availableAt: Option[Instant]
        ) = RepositoryIO.lift(advanced.update(_ :+ next).as(InterviewWorkflowAdvanceResult.Applied))
      }
      worker = new InterviewWorkflowWorker(
        store,
        calendar,
        notifications,
        InterviewWorkerSettings("interview-worker", 1.second, 60.seconds, 10.seconds, 5, 1.second, 30.seconds),
        Diagnostics.noop,
        IO.pure(observedAt)
      )
      acknowledged <- if (resultDelivery) worker.receiveResult(delivery) else worker.receiveCommand(delivery)
      quarantines <- quarantined.get
      claimCount <- claims.get
      advances <- advanced.get
    } yield DeliveryObservation(acknowledged, quarantines, claimCount, advances)

  test("a matching timely command reaches execution claiming without quarantine") {
    observeDelivery(message).map(result =>
      assertEquals(result, DeliveryObservation(true, Vector.empty, 1, Vector.empty))
    )
  }

  test("a timely command with a stored result acknowledges before identity validation") {
    observeDelivery(
      message.copy(messageId = new UUID(0L, 99L)),
      stored = Some(command.copy(result = Some(InterviewCommandResult.Succeeded)))
    )
      .map(result => assertEquals(result, DeliveryObservation(true, Vector.empty, 0, Vector.empty)))
  }

  test("future commands quarantine before the stored-result acknowledgment shortcut") {
    observeDelivery(
      message.copy(occurredAt = observedAt.plusSeconds(1)),
      stored = Some(command.copy(result = Some(InterviewCommandResult.Succeeded)))
    )
      .map { result =>
        assert(result.acknowledged)
        assertEquals(result.claims, 0)
        assert(result.quarantined.exists(_.endsWith(":future")))
      }
  }

  test("an expired matching command requires repair even when its stored result exists") {
    val expired = command.copy(
      occurredAt = observedAt.minusSeconds(8.days.toSeconds),
      result = Some(InterviewCommandResult.Succeeded)
    )
    observeDelivery(messageFor(expired, workflow, InterviewStep.Reserve, None), stored = Some(expired)).map { result =>
      assert(result.acknowledged)
      assertEquals(result.claims, 0)
      assertEquals(result.quarantined, Vector.empty)
      assertEquals(result.advanced.map(_.phase), Vector(InterviewWorkflowPhase.RepairRequired))
    }
  }

  test("a timely inconsistent command quarantines without execution") {
    observeDelivery(message.copy(causationId = new UUID(0L, 99L))).map { result =>
      assert(result.quarantined.exists(_.endsWith(":inconsistent")))
      assertEquals(result.claims, 0)
    }
  }

  test("a matching stored reservation result advances the pure workflow") {
    val completed = command.copy(result = Some(InterviewCommandResult.Succeeded))
    observeDelivery(
      messageFor(completed, workflow, InterviewStep.Reserve, Some(InterviewResult.Succeeded)),
      true,
      Some(completed)
    ).map { result =>
      assert(result.acknowledged)
      assertEquals(result.quarantined, Vector.empty)
      assertEquals(result.advanced.map(_.phase), Vector(InterviewWorkflowPhase.StatusCommitPending))
    }
  }

  test("a result differing from the stored receipt is quarantined without advancement") {
    val completed = command.copy(result = Some(InterviewCommandResult.Succeeded))
    observeDelivery(
      messageFor(completed, workflow, InterviewStep.Reserve, Some(InterviewResult.Rejected)),
      true,
      Some(completed)
    ).map { result =>
      assert(result.quarantined.exists(_.endsWith(":inconsistent")))
      assertEquals(result.advanced, Vector.empty)
    }
  }

  test("an outstanding notification result from an earlier revision can advance independently") {
    val notifying = workflow.copy(revision = 3L, phase = InterviewWorkflowPhase.NotificationsPending)
    val notification = command.copy(
      revision = 2L,
      command = InterviewWorkflowCommand.Notify(InterviewParticipant.Candidate, "candidate-notification"),
      result = Some(InterviewCommandResult.Succeeded)
    )
    observeDelivery(
      messageFor(notification, notifying, InterviewStep.NotifyCandidate, Some(InterviewResult.Succeeded)),
      true,
      Some(notification),
      Some(notifying)
    ).map { result =>
      assertEquals(result.quarantined, Vector.empty)
      assertEquals(result.advanced.map(_.notified), Vector(Set(InterviewParticipant.Candidate)))
    }
  }

  test("future notification revisions do not advance the workflow") {
    val notifying = workflow.copy(revision = 3L, phase = InterviewWorkflowPhase.NotificationsPending)
    val notification = command.copy(
      revision = 4L,
      command = InterviewWorkflowCommand.Notify(InterviewParticipant.Candidate, "candidate-notification"),
      result = Some(InterviewCommandResult.Succeeded)
    )
    observeDelivery(
      messageFor(notification, notifying, InterviewStep.NotifyCandidate, Some(InterviewResult.Succeeded)),
      true,
      Some(notification),
      Some(notifying)
    ).map { result =>
      assert(result.quarantined.exists(_.endsWith(":inconsistent")))
      assertEquals(result.advanced, Vector.empty)
    }
  }

  test("missing workflow and missing result command retain distinct quarantine reasons") {
    val result = messageFor(command, workflow, InterviewStep.Reserve, Some(InterviewResult.Succeeded))
    for {
      absentWorkflow <- observeDelivery(result, true, current = None)
      absentCommand <- observeDelivery(result, true, stored = None)
    } yield {
      assert(absentWorkflow.quarantined.exists(_.endsWith(":unknown")))
      assert(absentCommand.quarantined.exists(_.endsWith(":inconsistent")))
    }
  }
}
