package com.example.graphQL.cats.repository.mongo

import cats.data.EitherT
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, UserId}
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.application.{InterviewWorkerSettings, InterviewWorkflowWorker}
import com.example.graphQL.cats.service.port.*
import com.mongodb.client.model.{Filters, Updates}
import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*

/** Counters and one-shot faults around the durable ledgers, so a test can crash or stall a provider exactly where the
  * failure table says the system must recover.
  */
private[mongo] final case class InterviewProviderFaults(
    cancelCalls: Ref[IO, List[String]],
    confirmedCancels: Ref[IO, List[String]],
    holdCalls: Ref[IO, List[String]],
    deliveries: Ref[IO, List[String]],
    // Calls that fail as `Unavailable` before reaching the ledger.
    cancelUnavailable: Ref[IO, Int],
    holdUnavailable: Ref[IO, Int],
    notifyUnavailable: Ref[IO, Int],
    // The next cancel / delivery reaches the ledger and then the process "dies" before the worker can record it.
    crashAfterCancel: Ref[IO, Boolean],
    crashAfterDelivery: Ref[IO, Boolean],
    // The next delivery dies before the ledger records anything (the provider never saw it).
    crashBeforeDelivery: Ref[IO, Boolean],
    // The cancel waits here until released, to observe the state while the provider has not yet confirmed.
    cancelGate: Ref[IO, Option[cats.effect.Deferred[IO, Unit]]],
    // After every provider call the number of live holds, to prove nobody is ever without a held slot.
    liveHolds: Ref[IO, Vector[Int]]
)

private[mongo] object InterviewProviderFaults {
  def create: IO[InterviewProviderFaults] =
    (
      Ref.of[IO, List[String]](Nil),
      Ref.of[IO, List[String]](Nil),
      Ref.of[IO, List[String]](Nil),
      Ref.of[IO, List[String]](Nil),
      Ref.of[IO, Int](0),
      Ref.of[IO, Int](0),
      Ref.of[IO, Int](0),
      Ref.of[IO, Boolean](false),
      Ref.of[IO, Boolean](false),
      Ref.of[IO, Boolean](false),
      Ref.of[IO, Option[cats.effect.Deferred[IO, Unit]]](None),
      Ref.of[IO, Vector[Int]](Vector.empty)
    ).mapN(InterviewProviderFaults.apply)
}

/** A process that died between two durable writes. */
private[mongo] final class SimulatedCrash extends RuntimeException("simulated worker crash")

/** A Completed, booked interview over the real replica set: users, job, an `Interview` application, the workflow and
  * its live scheduling hold. Workers run against the same repository and ledgers; an in-process pump stands in for the
  * broker (commands and results are delivered, and undeliverable ones are redelivered like uncommitted offsets).
  */
private[mongo] final case class InterviewLifecycleScenario(
    fixture: MongoAccessEvaluationSupport.Fixture,
    repository: MongoInterviewWorkflowRepository,
    workflow: InterviewWorkflow,
    faults: InterviewProviderFaults,
    inFlight: Ref[IO, Vector[InterviewMessage]]
) {
  val id: InterviewWorkflowId = workflow.id

  def collection(name: String) = Mongo4catsCollections.documents(fixture.database, name)
  def count(name: String, filter: org.bson.conversions.Bson = new org.bson.BsonDocument()): IO[Long] =
    MongoRepositoryTestSupport.count(fixture.database, name, filter)

  def current: IO[InterviewWorkflow] =
    repository
      .findForAdmin(id)
      .value
      .flatMap(_.fold(error => IO.raiseError(new AssertionError(s"$error")), IO.pure))
      .flatMap(found => IO.fromOption(found)(new AssertionError("workflow missing")))

  def applicationStatus: IO[Option[String]] =
    MongoRepositoryTestSupport
      .findOne(
        fixture.database,
        MongoCollections.Applications,
        Filters.eq(MongoFields.Id, workflow.applicationId.value.toString)
      )
      .map(_.map(_.getString(MongoFields.Status)))

  def setApplicationStatus(status: ApplicationStatus): IO[Unit] =
    collection(MongoCollections.Applications).flatMap(
      _.updateOne(
        Filters.eq(MongoFields.Id, workflow.applicationId.value.toString),
        Updates.set(MongoFields.Status, status.toString)
      ).void
    )

  /** The reservation of a generation, if the ledger has one. */
  def reservation(generation: Int): IO[Option[InterviewCalendarReservation]] =
    repository
      .find(id, InterviewWorkflow.reservationKey(id, generation))
      .value
      .flatMap(_.fold(error => IO.raiseError(new AssertionError(s"$error")), IO.pure))

  def isHeld(generation: Int): IO[Boolean] = reservation(generation).map(_.exists(_.releasedAt.isEmpty))
  def isReleased(generation: Int): IO[Boolean] = reservation(generation).map(_.exists(_.releasedAt.nonEmpty))

  def receipts: IO[List[InterviewNotificationReceipt]] =
    collection(MongoCollections.InterviewNotificationReceipts)
      .flatMap(
        _.find(Filters.eq("workflowId", id.value.toString)).stream.compile.toList
      )
      .flatMap(
        _.traverse(document =>
          IO.fromEither(
            (
              InterviewNotificationKind.values.find(_.toString == document.getString("kind")),
              InterviewParticipant.values.find(_.toString == document.getString("participant"))
            ).tupled
              .map { case (kind, participant) =>
                InterviewNotificationReceipt(
                  id,
                  UserId(UUID.fromString(document.getString("recipientId"))),
                  participant,
                  kind,
                  document.getString(MongoFields.Id),
                  document.getDate("deliveredAt").toInstant
                )
              }
              .toRight(new AssertionError("undecodable receipt"))
          )
        )
      )

  def commandRows: IO[List[InterviewWorkflowCommandRecord]] =
    collection(MongoCollections.InterviewWorkflowCommands)
      .flatMap(_.find(Filters.eq("workflowId", id.value.toString)).stream.compile.toList)
      .flatMap(
        _.traverse(document =>
          IO.fromEither(MongoInterviewWorkflowCommandCodec.decode(document).leftMap(e => new AssertionError(s"$e")))
        )
      )

  def actor(role: UserRole, user: UserId, key: UUID = UUID.randomUUID(), input: String = "action") =
    InterviewLifecycleOrigin.Actor(
      InterviewWorkflowAccess(user, role),
      key,
      MutationReceiptFingerprint.fromCanonicalInput(input)
    )

  def apply(
      event: InterviewLifecycleEvent,
      origin: InterviewLifecycleOrigin,
      expected: Long,
      at: Instant
  ): IO[Either[com.example.graphQL.cats.service.RepositoryError, InterviewLifecycleOutcome]] =
    repository.applyLifecycle(id, expected, event, origin, at, None).value

  def applied(
      event: InterviewLifecycleEvent,
      origin: InterviewLifecycleOrigin,
      expected: Long,
      at: Instant
  ): IO[InterviewWorkflow] =
    apply(event, origin, expected, at).flatMap {
      case Right(InterviewLifecycleOutcome.Applied(next)) => IO.pure(next)
      case other => IO.raiseError(new AssertionError(s"expected an applied transition, got $other"))
    }

  val calendarLedger: InterviewCalendarProvider = LedgerInterviewCalendarProvider.durable(repository)
  val notificationLedger: InterviewNotificationProvider = LedgerInterviewNotificationProvider.durable(repository)

  private def snapshotHolds: IO[Unit] =
    List(0, 1, 2, 3).traverse(isHeld).flatMap(held => faults.liveHolds.update(_ :+ held.count(identity)))

  /** The ledger calendar, wrapped with the scenario's faults and counters. */
  val calendar: InterviewCalendarProvider = new InterviewCalendarProvider {
    private def unavailable(counter: Ref[IO, Int]): InterviewProviderIO[Unit] =
      EitherT(counter.modify(left => if (left > 0) (left - 1, true) else (0, false)).map { fail =>
        if (fail) Left(InterviewProviderError.Unavailable) else Right(())
      })

    override def reserve(
        workflowId: InterviewWorkflowId,
        idempotencyKey: String,
        candidateId: UserId,
        recruiterId: UserId,
        interval: InterviewInterval,
        now: Instant,
        execution: Option[ClaimedInterviewWorkflowCommand]
    ): InterviewProviderIO[InterviewCalendarReservation] =
      EitherT.liftF[IO, InterviewProviderError, Unit](faults.holdCalls.update(_ :+ idempotencyKey)) *>
        unavailable(faults.holdUnavailable) *>
        calendarLedger.reserve(workflowId, idempotencyKey, candidateId, recruiterId, interval, now, execution)

    override def lookup(
        workflowId: InterviewWorkflowId,
        idempotencyKey: String
    ): InterviewProviderIO[Option[InterviewCalendarReservation]] = calendarLedger.lookup(workflowId, idempotencyKey)

    override def cancel(
        workflowId: InterviewWorkflowId,
        idempotencyKey: String,
        now: Instant,
        execution: Option[ClaimedInterviewWorkflowCommand]
    ): InterviewProviderIO[InterviewCalendarCancellation] =
      for {
        _ <- EitherT.liftF[IO, InterviewProviderError, Unit](faults.cancelCalls.update(_ :+ idempotencyKey))
        _ <- unavailable(faults.cancelUnavailable)
        gate <- EitherT.liftF[IO, InterviewProviderError, Option[cats.effect.Deferred[IO, Unit]]](
          faults.cancelGate.get
        )
        _ <- EitherT.liftF[IO, InterviewProviderError, Unit](gate.traverse_(_.get))
        outcome <- calendarLedger.cancel(workflowId, idempotencyKey, now, execution)
        _ <- EitherT.liftF[IO, InterviewProviderError, Unit](outcome match {
          case InterviewCalendarCancellation.Cancelled(_) => faults.confirmedCancels.update(_ :+ idempotencyKey)
          case _                                          => IO.unit
        })
        _ <- EitherT.liftF[IO, InterviewProviderError, Unit](snapshotHolds)
        _ <- EitherT.liftF[IO, InterviewProviderError, Unit](
          faults.crashAfterCancel.getAndSet(false).flatMap(crash => IO.raiseWhen(crash)(new SimulatedCrash))
        )
      } yield outcome

    override def release(
        idempotencyKey: String,
        now: Instant,
        execution: Option[ClaimedInterviewWorkflowCommand]
    ): InterviewProviderIO[Unit] = calendarLedger.release(idempotencyKey, now, execution)
  }

  /** The ledger receipt store, wrapped with the scenario's faults and counters. */
  val notifications: InterviewNotificationProvider = new InterviewNotificationProvider {
    override def notify(
        workflowId: InterviewWorkflowId,
        recipientId: UserId,
        participant: InterviewParticipant,
        kind: InterviewNotificationKind,
        idempotencyKey: String,
        now: Instant,
        execution: Option[ClaimedInterviewWorkflowCommand]
    ): InterviewProviderIO[InterviewNotificationReceipt] =
      for {
        _ <- EitherT.liftF[IO, InterviewProviderError, Unit](faults.deliveries.update(_ :+ idempotencyKey))
        failing <- EitherT.liftF[IO, InterviewProviderError, Boolean](
          faults.notifyUnavailable.modify(left => if (left > 0) (left - 1, true) else (0, false))
        )
        _ <- EitherT.cond[IO](!failing, (), InterviewProviderError.Unavailable)
        _ <- EitherT.liftF[IO, InterviewProviderError, Unit](
          faults.crashBeforeDelivery.getAndSet(false).flatMap(crash => IO.raiseWhen(crash)(new SimulatedCrash))
        )
        receipt <- notificationLedger.notify(
          workflowId,
          recipientId,
          participant,
          kind,
          idempotencyKey,
          now,
          execution
        )
        _ <- EitherT.liftF[IO, InterviewProviderError, Unit](
          faults.crashAfterDelivery.getAndSet(false).flatMap(crash => IO.raiseWhen(crash)(new SimulatedCrash))
        )
      } yield receipt

    override def lookup(idempotencyKey: String): InterviewProviderIO[Option[InterviewNotificationReceipt]] =
      notificationLedger.lookup(idempotencyKey)
  }

  def settings(
      workerId: String = "lifecycle-worker",
      claimLease: FiniteDuration = 60.seconds,
      maxAttempts: Int = 5
  ): InterviewWorkerSettings =
    InterviewWorkerSettings(workerId, 10.millis, claimLease, 10.seconds, maxAttempts, 10.millis, 50.millis)

  def worker(
      workerId: String = "lifecycle-worker",
      claimLease: FiniteDuration = 60.seconds,
      maxAttempts: Int = 5,
      clock: IO[Instant] = IO.realTimeInstant
  ): InterviewWorkflowWorker =
    new InterviewWorkflowWorker(
      repository,
      calendar,
      notifications,
      settings(workerId, claimLease, maxAttempts),
      Diagnostics.noop,
      clock
    )

  private val commandGeneration = InterviewPublisherGeneration(InterviewPublisherRole.Orchestrator, UUID.randomUUID())
  private val resultGeneration = InterviewPublisherGeneration(InterviewPublisherRole.Worker, UUID.randomUUID())

  private def transport: InterviewTransport = new InterviewTransport {
    override def generationFor(message: InterviewMessage): InterviewPublisherGeneration =
      if (message.result.nonEmpty) resultGeneration else commandGeneration
    override def publish(message: InterviewMessage): IO[Unit] = inFlight.update(_ :+ message)
  }

  /** One broker round: publish what is due, then deliver every pending message. A message whose handler crashes or
    * reports it is not durable stays pending, like an uncommitted offset. Returns the crashes observed.
    */
  def round(worker: InterviewWorkflowWorker): IO[List[Throwable]] =
    for {
      _ <- worker.publishDue(transport)
      pending <- inFlight.getAndSet(Vector.empty)
      outcomes <- pending.toList.traverse { message =>
        (if (message.result.isEmpty) worker.receiveCommand(message) else worker.receiveResult(message)).attempt
          .map(message -> _)
      }
      _ <- inFlight.update(kept =>
        outcomes.collect {
          case (message, Left(_))      => message
          case (message, Right(false)) => message
        }.toVector ++ kept
      )
      _ <- snapshotHolds
    } yield outcomes.collect { case (_, Left(error)) => error }

  /** Pumps rounds until the workflow satisfies `done`. The crashes of earlier rounds are returned for assertions. */
  def drive(
      workers: List[InterviewWorkflowWorker],
      done: InterviewWorkflow => Boolean,
      rounds: Int = 400,
      pause: FiniteDuration = 25.millis
  ): IO[(InterviewWorkflow, List[Throwable])] = {
    def loop(remaining: Int, crashes: List[Throwable]): IO[(InterviewWorkflow, List[Throwable])] =
      for {
        found <- workers.parTraverse(round)
        state <- current
        all = crashes ++ found.flatten
        result <-
          if (done(state)) IO.pure((state, all))
          else if (remaining <= 0)
            commandRows.flatMap(rows =>
              IO.raiseError(
                new AssertionError(
                  s"workflow stuck in ${state.phase} at revision ${state.revision}; crashes: ${all.map(_.toString).distinct.take(3)}; commands: " +
                    rows
                      .map(r =>
                        s"${r.stepId.takeRight(5)} ${r.command.getClass.getSimpleName} ${r.state} ${r.result} rev=${r.revision} at=${r.availableAt}"
                      )
                      .mkString("; ")
                )
              )
            )
          else IO.sleep(pause) *> loop(remaining - 1, all)
      } yield result
    loop(rounds, Nil)
  }

  /** Pumps rounds until `condition` holds; unlike `drive` it does not look at the workflow phase. */
  def pumpUntil(
      workers: List[InterviewWorkflowWorker],
      condition: IO[Boolean],
      rounds: Int = 400,
      pause: FiniteDuration = 25.millis
  ): IO[Unit] = {
    def loop(remaining: Int): IO[Unit] =
      workers.parTraverse(round) *> condition.flatMap { done =>
        if (done) IO.unit
        else if (remaining <= 0) IO.raiseError(new AssertionError("the awaited condition never held"))
        else IO.sleep(pause) *> loop(remaining - 1)
      }
    loop(rounds)
  }

  /** Lets whatever the workflow's current phase still has to do finish, then a few more rounds so informational
    * notifications are delivered. Returns the settled workflow.
    */
  def settle(workers: List[InterviewWorkflowWorker]): IO[InterviewWorkflow] =
    current.flatMap { observed =>
      val finished: InterviewWorkflow => Boolean = observed.phase match {
        case InterviewWorkflowPhase.CancelPending | InterviewWorkflowPhase.CancelNotificationsPending =>
          _.phase == InterviewWorkflowPhase.Cancelled
        case InterviewWorkflowPhase.RescheduleHoldPending | InterviewWorkflowPhase.RescheduleSwapPending |
            InterviewWorkflowPhase.RescheduleCancelOldPending | InterviewWorkflowPhase.RescheduleCompensationPending |
            InterviewWorkflowPhase.RescheduleNotificationsPending =>
          _.phase == InterviewWorkflowPhase.Completed
        case _ => _ => true
      }
      drive(workers, finished).flatMap(_ => workers.parTraverse(round).replicateA_(6)) *> current
    }

  def settled(state: InterviewWorkflow): Boolean =
    Set(InterviewWorkflowPhase.Completed, InterviewWorkflowPhase.Cancelled, InterviewWorkflowPhase.RepairRequired)(
      state.phase
    )
}

private[mongo] object InterviewLifecycleScenario {
  val now: Instant = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS)
  val interval: InterviewInterval = InterviewInterval(now.plusSeconds(172800), now.plusSeconds(176400))
  val replacement: InterviewInterval = InterviewInterval(now.plusSeconds(259200), now.plusSeconds(262800))

  private def success[A](operation: RepositoryIO[A]): IO[A] =
    operation.value.flatMap(_.fold(error => IO.raiseError(new AssertionError(s"$error")), IO.pure))

  /** Seeds a booked interview (`Completed`, application `Interview`, scheduling hold live) and runs `use` against it.
    */
  def arranged[A](
      mongo: Resource[IO, MongoAccessEvaluationSupport.Fixture],
      held: InterviewInterval = interval
  )(use: InterviewLifecycleScenario => IO[A]): IO[A] =
    mongo.use { fixture =>
      val repository = MongoInterviewWorkflowRepository.live(fixture.database, fixture.client, Diagnostics.noop)
      for {
        at <- IO.realTimeInstant.map(_.truncatedTo(java.time.temporal.ChronoUnit.MILLIS))
        base <- IO.fromEither(
          InterviewWorkflow
            .create(
              InterviewWorkflowId(UUID.randomUUID()),
              ApplicationId(UUID.randomUUID()),
              UserId(UUID.randomUUID()),
              UserId(UUID.randomUUID()),
              held,
              at.plusSeconds(300),
              UUID.randomUUID(),
              ApplicationStatus.Accepted
            )
            .leftMap(error => new AssertionError(s"invalid workflow $error"))
        )
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- InterviewSchedulingFixtures.seed(fixture.database, List(base), at)
        _ <- success(
          repository.create(
            base,
            InterviewWorkflow.initialCommand(base),
            UUID.randomUUID(),
            MutationReceiptFingerprint.fromCanonicalInput("arrangement"),
            at
          )
        )
        faults <- InterviewProviderFaults.create
        inFlight <- Ref.of[IO, Vector[InterviewMessage]](Vector.empty)
        scenario = InterviewLifecycleScenario(fixture, repository, base, faults, inFlight)
        _ <- repository
          .reserveIfAvailable(
            InterviewCalendarReservation(
              base.id,
              InterviewWorkflow.reservationKey(base.id, 0),
              base.candidateId,
              base.recruiterId,
              held,
              at,
              None
            )
          )
          .value
          .flatMap(_.fold(error => IO.raiseError(new AssertionError(s"$error")), _ => IO.unit))
        workflows <- scenario.collection(MongoCollections.InterviewWorkflows)
        _ <- workflows.updateOne(
          Filters.eq(MongoFields.Id, base.id.value.toString),
          Updates.combine(
            Updates.set("phase", InterviewWorkflowPhase.Completed.toString),
            Updates.set("notified", java.util.List.of("Candidate", "Recruiter"))
          )
        )
        // The creation command (reserve) is history here: the booking is complete.
        commands <- scenario.collection(MongoCollections.InterviewWorkflowCommands)
        _ <- commands.updateMany(
          Filters.eq("workflowId", base.id.value.toString),
          Updates.set("commandState", InterviewWorkflowCommandState.Superseded.toString)
        )
        _ <- scenario.setApplicationStatus(ApplicationStatus.Interview)
        result <- use(scenario)
      } yield result
    }
}
