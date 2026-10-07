package com.example.graphQL.cats.service.application

import cats.effect.{Deferred, IO, Ref}
import cats.syntax.foldable.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField}
import com.example.graphQL.cats.service.port.*
import java.time.Instant
import java.util.UUID
import munit.CatsEffectSuite
import scala.concurrent.duration.*

final class InterviewWorkflowWorkerSpec extends CatsEffectSuite {
  test("full publication passes continue immediately while idle and deferred passes wait") {
    for {
      calls <- Ref.of[IO, Int](0)
      third <- Deferred[IO, Unit]
      publish = calls.getAndUpdate(_ + 1).flatMap {
        case 0 | 1 => IO.pure(InterviewPublicationPass.Full)
        case _     => third.complete(()).void.as(InterviewPublicationPass.Idle)
      }
      fiber <- InterviewWorkflowWorker.publicationLoop(publish, 1.day).compile.drain.start
      _ <- third.get.timeout(2.seconds).guarantee(fiber.cancel)
      count <- calls.get
    } yield assertEquals(count, 3)
  }

  test("idle and deferred passes sleep instead of repeatedly claiming") {
    List(InterviewPublicationPass.Idle, InterviewPublicationPass.Deferred).traverse_ { outcome =>
      for {
        calls <- Ref.of[IO, Int](0)
        entered <- Deferred[IO, Unit]
        fiber <- InterviewWorkflowWorker
          .publicationLoop(
            calls.update(_ + 1) *> entered.complete(()).void.as(outcome),
            1.day
          )
          .compile
          .drain
          .start
        _ <- entered.get *> IO.sleep(30.millis)
        _ <- fiber.cancel
        count <- calls.get
      } yield assertEquals(count, 1)
    }
  }

  test("a continuously full publication loop remains cancellable") {
    for {
      entered <- Deferred[IO, Unit]
      fiber <- InterviewWorkflowWorker
        .publicationLoop(
          entered.complete(()).void.as(InterviewPublicationPass.Full),
          1.day
        )
        .compile
        .drain
        .start
      _ <- entered.get
      _ <- fiber.cancel.timeout(2.seconds)
    } yield ()
  }
  private val repository = new TestInterviewWorkflowRepository

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

  test("a bounded successful publication pass reports full, then idle after remaining work drains") {
    for {
      remaining <- Ref.of[IO, Int](3)
      sent <- Ref.of[IO, Int](0)
      record = command.copy(state = InterviewWorkflowCommandState.Claimed, publicationAttempts = 0)
      claim = ClaimedInterviewWorkflowCommand(record, "worker", new UUID(0L, 77L), observedAt.plusSeconds(60))
      store = new TestInterviewWorkflowRepository {
        override def claimDueCommands(workerId: String, now: Instant, leaseUntil: Instant, limit: Int) =
          RepositoryIO.lift(remaining.modify(n => (math.max(0, n - 1), if (n > 0) List(claim) else Nil)))
        override def findForAdmin(id: InterviewWorkflowId) = RepositoryIO.fromEither(Right(Some(workflow)))
        override def authorizePublication(
            value: ClaimedInterviewWorkflowCommand,
            generation: InterviewPublisherGeneration,
            now: Instant
        ) = RepositoryIO.fromEither(Right(true))
        override def renewPublication(value: ClaimedInterviewWorkflowCommand, now: Instant, leaseUntil: Instant) =
          RepositoryIO.fromEither(Right(true))
        override def markPublished(value: ClaimedInterviewWorkflowCommand, now: Instant) =
          RepositoryIO.fromEither(Right(()))
      }
      sender = new InterviewTransport {
        def generationFor(value: InterviewMessage) =
          InterviewPublisherGeneration(InterviewPublisherRole.Orchestrator, new UUID(0L, 78L))
        def publish(value: InterviewMessage) = sent.update(_ + 1)
      }
      worker = new InterviewWorkflowWorker(
        store,
        calendar,
        notifications,
        InterviewWorkerSettings(
          "worker",
          1.second,
          60.seconds,
          10.seconds,
          5,
          1.second,
          30.seconds,
          publicationBatchSize = 2
        ),
        Diagnostics.noop,
        IO.pure(observedAt)
      )
      first <- worker.publishDue(sender)
      second <- worker.publishDue(sender)
      count <- sent.get
    } yield {
      assertEquals(first, InterviewPublicationPass.Full)
      assertEquals(second, InterviewPublicationPass.Idle)
      assertEquals(count, 3)
    }
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
      store = new TestInterviewWorkflowRepository {
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

  test("publication claims one command and renews its lease while sending") {
    for {
      renewed <- Deferred[IO, Unit]
      renewCount <- Ref.of[IO, Int](0)
      marked <- Ref.of[IO, Boolean](false)
      requested <- Ref.of[IO, Vector[Int]](Vector.empty)
      record = command.copy(state = InterviewWorkflowCommandState.Claimed, publicationAttempts = 0)
      claim = ClaimedInterviewWorkflowCommand(record, "worker", new UUID(0L, 70L), observedAt.plusSeconds(1))
      store = new TestInterviewWorkflowRepository {
        override def claimDueCommands(workerId: String, now: Instant, leaseUntil: Instant, limit: Int) =
          RepositoryIO.lift(requested.modify(values => (values :+ limit, if (values.isEmpty) List(claim) else Nil)))
        override def findForAdmin(id: InterviewWorkflowId) = RepositoryIO.fromEither(Right(Some(workflow)))
        override def authorizePublication(
            value: ClaimedInterviewWorkflowCommand,
            generation: InterviewPublisherGeneration,
            now: Instant
        ) =
          RepositoryIO.fromEither(Right(true))
        override def renewPublication(value: ClaimedInterviewWorkflowCommand, now: Instant, leaseUntil: Instant) =
          RepositoryIO.lift(renewCount.getAndUpdate(_ + 1).flatMap { count =>
            if (count == 0) IO.pure(true) else renewed.complete(()).as(true)
          })
        override def markPublished(value: ClaimedInterviewWorkflowCommand, now: Instant) =
          RepositoryIO.lift(marked.set(true))
      }
      sender = new InterviewTransport {
        def generationFor(value: InterviewMessage) =
          InterviewPublisherGeneration(InterviewPublisherRole.Orchestrator, new UUID(0L, 71L))
        def publish(value: InterviewMessage) = renewed.get
      }
      worker = new InterviewWorkflowWorker(
        store,
        calendar,
        notifications,
        InterviewWorkerSettings("worker", 1.second, 3.millis, 1.second, 5, 1.second, 30.seconds),
        Diagnostics.noop,
        IO.pure(observedAt)
      )
      _ <- worker.publishDue(sender)
      limits <- requested.get
      completed <- marked.get
    } yield {
      assertEquals(limits, Vector(1, 1))
      assert(completed)
    }
  }

  test("lost publication lease cancels the send and stops the sweep") {
    for {
      cancelled <- Deferred[IO, Unit]
      retries <- Ref.of[IO, Int](0)
      renewals <- Ref.of[IO, Int](0)
      record = command.copy(state = InterviewWorkflowCommandState.Claimed, publicationAttempts = 0)
      claim = ClaimedInterviewWorkflowCommand(record, "worker", new UUID(0L, 72L), observedAt.plusSeconds(1))
      store = new TestInterviewWorkflowRepository {
        override def claimDueCommands(workerId: String, now: Instant, leaseUntil: Instant, limit: Int) =
          RepositoryIO.fromEither(Right(List(claim)))
        override def findForAdmin(id: InterviewWorkflowId) = RepositoryIO.fromEither(Right(Some(workflow)))
        override def authorizePublication(
            value: ClaimedInterviewWorkflowCommand,
            generation: InterviewPublisherGeneration,
            now: Instant
        ) = RepositoryIO.fromEither(Right(true))
        override def renewPublication(value: ClaimedInterviewWorkflowCommand, now: Instant, leaseUntil: Instant) =
          RepositoryIO.lift(renewals.getAndUpdate(_ + 1).map(_ == 0))
        override def retry(
            value: ClaimedInterviewWorkflowCommand,
            now: Instant,
            availableAt: Instant,
            failureCode: String
        ) = RepositoryIO.lift(retries.update(_ + 1))
      }
      sender = new InterviewTransport {
        def generationFor(value: InterviewMessage) =
          InterviewPublisherGeneration(InterviewPublisherRole.Orchestrator, new UUID(0L, 73L))
        def publish(value: InterviewMessage) = IO.never[Unit].onCancel(cancelled.complete(()).void)
      }
      worker = new InterviewWorkflowWorker(
        store,
        calendar,
        notifications,
        InterviewWorkerSettings("worker", 1.second, 3.millis, 1.second, 5, 1.second, 30.seconds),
        Diagnostics.noop,
        IO.pure(observedAt)
      )
      _ <- worker.publishDue(sender)
      _ <- cancelled.get
      retried <- retries.get
    } yield assertEquals(retried, 0)
  }

  test("initial lease renewal failure prevents transport invocation after authorization") {
    List[Either[RepositoryError, Boolean]](Right(false), Left(RepositoryError.Unavailable)).traverse_ { renewal =>
      for {
        sent <- Ref.of[IO, Int](0)
        retried <- Ref.of[IO, Int](0)
        failures <- Ref.of[IO, Vector[String]](Vector.empty)
        record = command.copy(state = InterviewWorkflowCommandState.Claimed, publicationAttempts = 0)
        claim = ClaimedInterviewWorkflowCommand(record, "worker", new UUID(0L, 80L), observedAt.plusSeconds(60))
        store = new TestInterviewWorkflowRepository {
          override def claimDueCommands(workerId: String, now: Instant, leaseUntil: Instant, limit: Int) =
            RepositoryIO.fromEither(Right(List(claim)))
          override def findForAdmin(id: InterviewWorkflowId) = RepositoryIO.fromEither(Right(Some(workflow)))
          override def authorizePublication(
              value: ClaimedInterviewWorkflowCommand,
              generation: InterviewPublisherGeneration,
              now: Instant
          ) = RepositoryIO.fromEither(Right(true))
          override def renewPublication(value: ClaimedInterviewWorkflowCommand, now: Instant, leaseUntil: Instant) =
            RepositoryIO.fromEither(renewal)
          override def retry(
              value: ClaimedInterviewWorkflowCommand,
              now: Instant,
              availableAt: Instant,
              failureCode: String
          ) = RepositoryIO.lift(retried.update(_ + 1) *> failures.update(_ :+ failureCode))
        }
        sender = new InterviewTransport {
          def generationFor(value: InterviewMessage) =
            InterviewPublisherGeneration(InterviewPublisherRole.Orchestrator, new UUID(0L, 81L))
          def publish(value: InterviewMessage) = sent.update(_ + 1)
        }
        worker = new InterviewWorkflowWorker(
          store,
          calendar,
          notifications,
          InterviewWorkerSettings("worker", 1.second, 60.seconds, 1.second, 5, 1.second, 30.seconds),
          Diagnostics.noop,
          IO.pure(observedAt)
        )
        _ <- worker.publishDue(sender)
        count <- sent.get
        retries <- retried.get
        codes <- failures.get
      } yield {
        assertEquals(count, 0)
        assertEquals(retries, if (renewal.isLeft) 1 else 0)
        assertEquals(codes, if (renewal.isLeft) Vector("publication_coordination_unavailable") else Vector.empty)
      }
    }
  }

  test("slow publication preparation refreshes the lease before sending") {
    for {
      clock <- Ref.of[IO, Instant](observedAt)
      expiry <- Ref.of[IO, Instant](observedAt.plusMillis(60))
      claims <- Ref.of[IO, Int](0)
      record = command.copy(state = InterviewWorkflowCommandState.Claimed, publicationAttempts = 0)
      claim = ClaimedInterviewWorkflowCommand(record, "worker", new UUID(0L, 82L), observedAt.plusMillis(60))
      store = new TestInterviewWorkflowRepository {
        override def claimDueCommands(workerId: String, now: Instant, leaseUntil: Instant, limit: Int) =
          RepositoryIO.lift(claims.getAndUpdate(_ + 1).map(n => if (n == 0) List(claim) else Nil))
        override def findForAdmin(id: InterviewWorkflowId) =
          RepositoryIO.lift(clock.set(observedAt.plusMillis(59)).as(Some(workflow)))
        override def authorizePublication(
            value: ClaimedInterviewWorkflowCommand,
            generation: InterviewPublisherGeneration,
            now: Instant
        ) = RepositoryIO.fromEither(Right(now.isBefore(claim.leaseUntil)))
        override def renewPublication(value: ClaimedInterviewWorkflowCommand, now: Instant, leaseUntil: Instant) =
          RepositoryIO.lift(expiry.set(leaseUntil).as(now.isBefore(claim.leaseUntil)))
        override def markPublished(value: ClaimedInterviewWorkflowCommand, now: Instant) =
          RepositoryIO.fromEither(Right(()))
      }
      sender = new InterviewTransport {
        def generationFor(value: InterviewMessage) =
          InterviewPublisherGeneration(InterviewPublisherRole.Orchestrator, new UUID(0L, 83L))
        def publish(value: InterviewMessage) = clock.set(observedAt.plusMillis(70)) *> expiry.get.flatMap(until =>
          IO(assert(until.isAfter(observedAt.plusMillis(70))))
        )
      }
      worker = new InterviewWorkflowWorker(
        store,
        calendar,
        notifications,
        InterviewWorkerSettings("worker", 1.second, 60.millis, 1.second, 5, 1.second, 30.seconds),
        Diagnostics.noop,
        clock.get
      )
      _ <- worker.publishDue(sender)
      renewedUntil <- expiry.get
    } yield assertEquals(renewedUntil, observedAt.plusMillis(119))
  }

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

  test("near-future commands defer without acknowledgment or quarantine") {
    observeDelivery(message.copy(occurredAt = observedAt.plusSeconds(1))).map { result =>
      assert(!result.acknowledged)
      assertEquals(result.claims, 0)
      assertEquals(result.quarantined, Vector.empty)
    }
  }

  test("future commands quarantine before the stored-result acknowledgment shortcut") {
    observeDelivery(
      message.copy(occurredAt = observedAt.plusSeconds(6)),
      stored = Some(command.copy(result = Some(InterviewCommandResult.Succeeded)))
    )
      .map { result =>
        assert(result.acknowledged)
        assertEquals(result.claims, 0)
        assert(result.quarantined.exists(_.endsWith(":future")))
        assertEquals(result.advanced, Vector.empty)
      }
  }

  test("matching far-future commands require visible repair before acknowledgment") {
    val future = command.copy(occurredAt = observedAt.plusSeconds(6))
    observeDelivery(messageFor(future, workflow, InterviewStep.Reserve, None), stored = Some(future)).map { result =>
      assert(result.acknowledged)
      assertEquals(result.claims, 0)
      assertEquals(result.advanced.map(_.phase), Vector(InterviewWorkflowPhase.RepairRequired))
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
