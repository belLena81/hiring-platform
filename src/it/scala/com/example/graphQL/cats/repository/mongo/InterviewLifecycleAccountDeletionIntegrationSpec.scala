package com.example.graphQL.cats.repository.mongo

import cats.data.EitherT
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.{Diagnostics, RepositoryError}
import com.example.graphQL.cats.service.application.{InterviewMessagePolicy, InterviewSubjectCleanupWorker}
import com.example.graphQL.cats.service.port.*
import com.mongodb.client.model.Filters
import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*

/** DHW-30 on the real replica set: deleting a participant while an interview is being cancelled, a reschedule is
  * proposed or a replacement is being booked. Account deletion fences the subject first; cleanup then confirms every
  * live provider hold is cancelled before it purges the workflow, its commands, receipts and reservations. Nothing is
  * sent to anyone, and nothing a late result, expiry, accept or redelivered message does can bring the data back.
  */
final class InterviewLifecycleAccountDeletionIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 8.minutes

  import InterviewLifecycleScenario.{now, replacement}

  private val barriers = Vector(
    InterviewRetentionBarrier("hiring.interview-commands", 0, 4L),
    InterviewRetentionBarrier("hiring.interview-results", 0, 5L)
  )
  private val fencer = new InterviewPublisherFencer {
    override def fence(ids: Vector[String]): RepositoryIO[Unit] = RepositoryIO.fromEither(Right(()))
  }

  private def scenario[A](use: InterviewLifecycleScenario => IO[A]): IO[A] =
    InterviewLifecycleScenario.arranged(mongoResource)(use)

  private def success[A](operation: RepositoryIO[A]): IO[A] =
    operation.value.flatMap(_.fold(error => IO.raiseError(new AssertionError(s"$error")), IO.pure))

  /** One cancel request the cleanup made of the provider and what the ledger showed at that moment. */
  private final case class HoldCall(cancelKey: String, rowPresent: Boolean, releasedBefore: Boolean)

  /** The provider as the cleanup sees it: records every cancel, and answers as configured. */
  private final class RecordingHolds(
      s: InterviewLifecycleScenario,
      val calls: Ref[IO, Vector[HoldCall]],
      answer: InterviewLiveHold => IO[Either[InterviewProviderError, InterviewCalendarCancellation]]
  ) extends InterviewHoldCanceller {
    override def cancel(hold: InterviewLiveHold, at: Instant): InterviewProviderIO[InterviewCalendarCancellation] =
      EitherT(for {
        row <- MongoRepositoryTestSupport.findOne(
          s.fixture.database,
          MongoCollections.InterviewCalendarReservations,
          Filters.and(
            Filters.eq("workflowId", hold.workflowId.value.toString),
            Filters.eq("reserveKey", hold.reserveKey)
          )
        )
        _ <- calls.update(_ :+ HoldCall(hold.cancelKey, row.nonEmpty, row.exists(_.containsKey("releasedAt"))))
        result <- answer(hold)
      } yield result)
    def keys: IO[Vector[String]] = calls.get.map(_.map(_.cancelKey))
  }

  private def holds(
      s: InterviewLifecycleScenario,
      answer: InterviewLiveHold => IO[Either[InterviewProviderError, InterviewCalendarCancellation]] = _ =>
        IO.realTimeInstant.map(at => Right(InterviewCalendarCancellation.Cancelled(at)))
  ): IO[RecordingHolds] = Ref.of[IO, Vector[HoldCall]](Vector.empty).map(new RecordingHolds(s, _, answer))

  private def cleanupOf(s: InterviewLifecycleScenario, canceller: InterviewHoldCanceller) =
    new MongoInterviewSubjectCleanup(s.fixture.database, holds = canceller)

  private def sweep(cleanup: MongoInterviewSubjectCleanup): IO[Option[RepositoryError]] =
    new InterviewSubjectCleanupWorker(cleanup, fencer, IO.pure(barriers), _ => IO.pure(true), Diagnostics.noop)
      .runOnce(None)
      .value
      .map(_.fold(Some(_), _.firstFailure))

  /** Sweeps until the subject's cleanup is complete; any failure on the way is a failed test. */
  private def finish(cleanup: MongoInterviewSubjectCleanup, subject: UserId): IO[Unit] = {
    def loop(remaining: Int): IO[Unit] =
      sweep(cleanup).flatMap {
        case Some(error) => IO.raiseError(new AssertionError(s"cleanup failed: $error"))
        case None        =>
          cleanup.complete(subject).flatMap { done =>
            if (done) IO.unit
            else if (remaining <= 0) IO.raiseError(new AssertionError("cleanup never completed"))
            else loop(remaining - 1)
          }
      }
    loop(10)
  }

  private def usersOf(s: InterviewLifecycleScenario) =
    new MongoUserRepository(
      s.fixture.database,
      MongoTransactionRunner.sessions(s.fixture.client, RepositoryError.Conflict, diagnostics = Diagnostics.noop),
      MongoEmbeddingWorkEnqueuer.disabled,
      Diagnostics.noop
    )

  private def delete(s: InterviewLifecycleScenario, subject: UserId): IO[Unit] = {
    def attempt(remaining: Int): IO[Unit] =
      IO.realTimeInstant.flatMap(at =>
        usersOf(s).deleteAccount(subject, at, "deleted-account", MutationWriteContext.directWrite).value.flatMap {
          case Right(_)                 => IO.unit
          case Left(_) if remaining > 0 => IO.sleep(20.millis) *> attempt(remaining - 1)
          case Left(error)              => IO.raiseError(new AssertionError(s"deletion failed: $error"))
        }
      )
    attempt(5)
  }

  /** Everything the interview workflow left behind: workflow, request receipts, commands, inbox, holds, receipts. */
  private def remaining(s: InterviewLifecycleScenario): IO[Map[String, Long]] = {
    val id = s.id.value.toString
    List(
      "workflow and request receipts" -> (
        MongoCollections.InterviewWorkflows,
        Filters.or(Filters.eq(MongoFields.Id, id), Filters.eq("requestWorkflowId", id))
      ),
      "commands" -> (MongoCollections.InterviewWorkflowCommands, Filters.eq("workflowId", id)),
      "inbox" -> (MongoCollections.InterviewWorkflowInbox, Filters.eq("workflowId", id)),
      "reservations" -> (MongoCollections.InterviewCalendarReservations, Filters.eq("workflowId", id)),
      "notification receipts" -> (MongoCollections.InterviewNotificationReceipts, Filters.eq("workflowId", id))
    ).traverse { case (label, (collection, filter)) => s.count(collection, filter).map(label -> _) }.map(_.toMap)
  }

  private val nothingLeft: Map[String, Long] = Map(
    "workflow and request receipts" -> 0L,
    "commands" -> 0L,
    "inbox" -> 0L,
    "reservations" -> 0L,
    "notification receipts" -> 0L
  )

  /** What providers have been asked to do so far. */
  private def activity(s: InterviewLifecycleScenario): IO[(List[String], List[String], List[String])] =
    (s.faults.cancelCalls.get, s.faults.holdCalls.get, s.faults.deliveries.get).tupled

  /** Workers keep running after the deletion: with no work admitted they must do nothing at all. */
  private def idle(s: InterviewLifecycleScenario): IO[Unit] =
    s.round(s.worker("after-deletion")).replicateA_(4)

  private def applicationRecords(s: InterviewLifecycleScenario): IO[(Long, Long)] =
    (
      s.count(MongoCollections.ApplicationEvents, Filters.eq("applicationId", s.workflow.applicationId.value.toString)),
      s.count(MongoCollections.EventOutbox)
    ).tupled

  private def cancelBy(
      s: InterviewLifecycleScenario,
      role: UserRole,
      user: UserId,
      expected: Long = 0L
  ): IO[InterviewWorkflow] = {
    val initiator = role match {
      case UserRole.Candidate => InterviewCancellationInitiator.Candidate
      case UserRole.Recruiter => InterviewCancellationInitiator.Recruiter
      case UserRole.Admin     => InterviewCancellationInitiator.Admin
    }
    s.applied(
      InterviewLifecycleEvent.Cancel(initiator, now),
      s.actor(role, user, input = s"cancel-$role"),
      expected,
      now
    )
  }

  private def propose(s: InterviewLifecycleScenario, expected: Long = 0L): IO[InterviewWorkflow] =
    s.applied(
      InterviewLifecycleEvent
        .Propose(replacement.startsAt, replacement.endsAt, s.workflow.recruiterId, now, InterviewProposalTtl.Default),
      s.actor(UserRole.Recruiter, s.workflow.recruiterId, input = s"propose-$expected"),
      expected,
      now
    )

  private def accept(s: InterviewLifecycleScenario, expected: Long): IO[InterviewWorkflow] =
    s.applied(
      InterviewLifecycleEvent.AcceptProposal(now),
      s.actor(UserRole.Candidate, s.workflow.candidateId, input = s"accept-$expected"),
      expected,
      now
    )

  private val subjects: List[(String, InterviewLifecycleScenario => UserId)] =
    List("candidate" -> (_.workflow.candidateId), "recruiter" -> (_.workflow.recruiterId))

  private def captureTransport(sink: Ref[IO, Vector[InterviewMessage]]): InterviewTransport = new InterviewTransport {
    private val commands = InterviewPublisherGeneration(InterviewPublisherRole.Orchestrator, UUID.randomUUID())
    private val results = InterviewPublisherGeneration(InterviewPublisherRole.Worker, UUID.randomUUID())
    override def generationFor(message: InterviewMessage): InterviewPublisherGeneration =
      if (message.result.nonEmpty) results else commands
    override def publish(message: InterviewMessage): IO[Unit] = sink.update(_ :+ message)
  }

  private def expiryMessage(workflow: InterviewWorkflow, row: InterviewWorkflowCommandRecord): InterviewMessage =
    InterviewMessage(
      InterviewMessagePolicy.stableId(row.stepId),
      workflow.id.value,
      row.stepId,
      InterviewStep.ExpireProposal,
      row.revision,
      workflow.id.value,
      workflow.preCommitDeadline,
      None,
      row.occurredAt
    )

  subjects.foreach { case (label, pick) =>
    test(
      s"deleting the $label during a pending provider cancel confirms the cancel, frees the slot, notifies nobody and keeps the rejection record"
    ) {
      scenario { s =>
        val subject = pick(s)
        for {
          recorder <- holds(s)
          cancelling <- cancelBy(s, UserRole.Candidate, s.workflow.candidateId)
          _ <- MongoWorkflowIntegrityAudit.audit(s.fixture.database)
          _ <- delete(s, subject)
          // The deleted account is fenced out: the cancel is not executed and nobody is told while data is still there.
          _ <- idle(s)
          fencedActivity <- activity(s)
          stillThere <- remaining(s)
          records <- applicationRecords(s)
          _ <- finish(cleanupOf(s, recorder), subject)
          gone <- remaining(s)
          calls <- recorder.calls.get
          _ <- idle(s)
          after <- activity(s)
          recordsAfter <- applicationRecords(s)
          status <- s.applicationStatus
          lock <- s.count(
            MongoCollections.InterviewCalendarParticipantLocks,
            Filters.eq(MongoFields.Id, subject.value.toString)
          )
          reservationsOfRecruiter <- s.count(
            MongoCollections.InterviewCalendarReservations,
            Filters.eq("participants", s.workflow.recruiterId.value.toString)
          )
          _ <- MongoWorkflowIntegrityAudit.audit(s.fixture.database)
        } yield {
          assertEquals(cancelling.phase, InterviewWorkflowPhase.CancelPending)
          assertEquals(
            fencedActivity,
            (Nil, Nil, Nil),
            "a fenced subject triggers no provider call and no notification"
          )
          assert(stillThere("reservations") > 0 && stillThere("commands") > 0)
          assertEquals(
            calls,
            Vector(HoldCall(InterviewWorkflow.cancellationKey(s.id, 0), rowPresent = true, releasedBefore = false)),
            "the cleanup cancelled the live hold, with the local evidence still present, before purging it"
          )
          assertEquals(gone, nothingLeft)
          assertEquals(lock, 0L)
          assertEquals(reservationsOfRecruiter, 0L, "the slot is free for the other participant")
          assertEquals(after, (Nil, Nil, Nil), "no notification or provider effect is created after the purge")
          assertEquals(status, Some("Rejected"))
          assertEquals(recordsAfter, records, "the committed rejection history and outbox follow the existing rules")
        }
      }
    }
  }

  test("an Admin-driven cancel leaves no Admin-keyed receipt behind and the Admin account is untouched") {
    scenario { s =>
      val admin = UserId(UUID.randomUUID())
      for {
        _ <- MongoRepositoryTestSupport.insertOne(
          s.fixture.database,
          MongoCollections.Users,
          MongoHiringCodecs.user(
            User(admin, None, s"Admin-${admin.value}", UserRole.Admin, None, now, adminSingleton = true)
          )
        )
        _ <- cancelBy(s, UserRole.Admin, admin)
        receiptsBefore <- s.count(
          MongoCollections.InterviewWorkflows,
          Filters.regex(MongoFields.Id, s"^request:${admin.value}:")
        )
        _ <- delete(s, s.workflow.candidateId)
        // The default cleanup treats the platform ledger as the provider.
        _ <- finish(new MongoInterviewSubjectCleanup(s.fixture.database), s.workflow.candidateId)
        gone <- remaining(s)
        receiptsAfter <- s.count(
          MongoCollections.InterviewWorkflows,
          Filters.regex(MongoFields.Id, s"^request:${admin.value}:")
        )
        adminStatus <- MongoRepositoryTestSupport
          .findOne(s.fixture.database, MongoCollections.Users, Filters.eq(MongoFields.Id, admin.value.toString))
          .map(_.map(_.getString(MongoFields.AccountStatus)))
        history <- MongoRepositoryTestSupport.findOne(
          s.fixture.database,
          MongoCollections.ApplicationEvents,
          Filters.eq("applicationId", s.workflow.applicationId.value.toString)
        )
        _ <- idle(s)
        after <- activity(s)
      } yield {
        assertEquals(receiptsBefore, 1L)
        assertEquals(receiptsAfter, 0L)
        assertEquals(gone, nothingLeft)
        assertEquals(adminStatus, Some("Active"))
        assertEquals(history.map(_.getString("actorId")), Some(admin.value.toString), "the committed history stays")
        assertEquals(after, (Nil, Nil, Nil))
      }
    }
  }

  subjects.foreach { case (label, pick) =>
    test(s"deleting the $label with an open proposal clears it, denies accept and expiry and sends nothing") {
      scenario { s =>
        val subject = pick(s)
        for {
          recorder <- holds(s)
          open <- propose(s)
          due = open.proposal.map(_.expiresAt).getOrElse(fail("no proposal"))
          expiry <- s.commandRows.flatMap(rows =>
            IO.fromOption(rows.find(_.command.isInstanceOf[InterviewLifecycleCommand.ExpireProposal]))(
              new AssertionError("no expiry row")
            )
          )
          message = expiryMessage(open, expiry)
          _ <- MongoWorkflowIntegrityAudit.audit(s.fixture.database)
          _ <- delete(s, subject)
          lateWorker = s.worker("after-expiry", clock = IO.pure(due.plusSeconds(1)))
          deniedAccept <- s.apply(
            InterviewLifecycleEvent.AcceptProposal(now),
            s.actor(UserRole.Candidate, s.workflow.candidateId, input = "accept-after-deletion"),
            open.revision,
            now
          )
          deniedExpiry <- lateWorker.receiveCommand(message)
          _ <- s.round(lateWorker).replicateA_(3)
          unchanged <- s.current
          _ <- finish(cleanupOf(s, recorder), subject)
          gone <- remaining(s)
          redelivered <- lateWorker.receiveCommand(message)
          lateAccept <- s.apply(
            InterviewLifecycleEvent.AcceptProposal(now),
            s.actor(UserRole.Candidate, s.workflow.candidateId, input = "accept-after-purge"),
            open.revision,
            now
          )
          _ <- s.round(lateWorker).replicateA_(3)
          after <- activity(s)
          again <- remaining(s)
          calls <- recorder.keys
          _ <- MongoWorkflowIntegrityAudit.audit(s.fixture.database)
        } yield {
          assertEquals(open.phase, InterviewWorkflowPhase.ProposalPending)
          assert(!deniedAccept.exists(_.isInstanceOf[InterviewLifecycleOutcome.Applied]), clue(deniedAccept))
          assert(!deniedExpiry, "an expiry for a fenced subject is not acknowledged as applied")
          assertEquals(unchanged.revision, open.revision)
          assertEquals(unchanged.proposal, open.proposal)
          assertEquals(gone, nothingLeft)
          assert(redelivered, "a redelivered expiry for a purged workflow is settled without effect")
          assert(!lateAccept.exists(_.isInstanceOf[InterviewLifecycleOutcome.Applied]), clue(lateAccept))
          assertEquals(again, nothingLeft)
          assertEquals(after, (Nil, Nil, Nil))
          assertEquals(calls, Vector(InterviewWorkflow.cancellationKey(s.id, 0)), "only the booked slot was held")
        }
      }
    }
  }

  test("deleting a participant during a reschedule that holds both slots cancels both holds before the purge") {
    scenario { s =>
      for {
        recorder <- holds(s)
        proposed <- propose(s)
        _ <- accept(s, proposed.revision)
        // The swap is refused (hired meanwhile) and the compensation cannot be confirmed: both holds stay live.
        _ <- s.setApplicationStatus(ApplicationStatus.Hired)
        _ <- s.faults.cancelUnavailable.set(1000)
        end <- s.drive(List(s.worker(maxAttempts = 3)), _.phase == InterviewWorkflowPhase.RepairRequired)
        bothHeld <- (s.isHeld(0), s.isHeld(1)).tupled
        _ <- delete(s, s.workflow.candidateId)
        before <- activity(s)
        _ <- finish(cleanupOf(s, recorder), s.workflow.candidateId)
        gone <- remaining(s)
        calls <- recorder.calls.get
        _ <- idle(s)
        after <- activity(s)
      } yield {
        assertEquals(end._1.pendingInterval, Some(replacement))
        assertEquals(bothHeld, (true, true))
        assertEquals(
          calls.map(_.cancelKey).toSet,
          Set(InterviewWorkflow.cancellationKey(s.id, 0), InterviewWorkflow.cancellationKey(s.id, 1))
        )
        assert(calls.forall(call => call.rowPresent && !call.releasedBefore), clue(calls))
        assertEquals(gone, nothingLeft)
        assertEquals(after, before, "no provider effect or notification after the deletion")
      }
    }
  }

  test("deleting the recruiter after a compensated attempt purges the retired generation and the one awaiting a hold") {
    scenario { s =>
      for {
        recorder <- holds(s)
        first <- propose(s)
        _ <- accept(s, first.revision)
        _ <- s.setApplicationStatus(ApplicationStatus.Hired)
        compensated <- s.drive(List(s.worker()), _.phase == InterviewWorkflowPhase.Completed)
        _ <- s.setApplicationStatus(ApplicationStatus.Interview)
        second <- propose(s, expected = compensated._1.revision)
        accepted <- accept(s, second.revision)
        retired <- s.isReleased(1)
        _ <- MongoWorkflowIntegrityAudit.audit(s.fixture.database)
        _ <- delete(s, s.workflow.recruiterId)
        _ <- finish(cleanupOf(s, recorder), s.workflow.recruiterId)
        gone <- remaining(s)
        calls <- recorder.keys
        _ <- idle(s)
        _ <- MongoWorkflowIntegrityAudit.audit(s.fixture.database)
      } yield {
        assertEquals(compensated._1.skippedGenerations, 1)
        assertEquals(accepted.phase, InterviewWorkflowPhase.RescheduleHoldPending)
        assert(retired, "the compensated hold was cancelled earlier and stays on record until the purge")
        assertEquals(
          calls,
          Vector(InterviewWorkflow.cancellationKey(s.id, 0)),
          "only the booked slot is live: the retired hold is already cancelled and none was taken for the new attempt"
        )
        assertEquals(gone, nothingLeft)
      }
    }
  }

  test("pending informational notifications and an exhausted one are cleared without being sent") {
    scenario { s =>
      for {
        recorder <- holds(s)
        proposed <- propose(s)
        withdrawn <- s.applied(
          InterviewLifecycleEvent.WithdrawProposal(now),
          s.actor(UserRole.Recruiter, s.workflow.recruiterId, input = "withdraw"),
          proposed.revision,
          now
        )
        cancelling <- cancelBy(s, UserRole.Recruiter, s.workflow.recruiterId, withdrawn.revision)
        failed <- s.applied(
          InterviewLifecycleEvent.RetryExhausted("provider"),
          InterviewLifecycleOrigin.Internal(InterviewAdvanceCause.ResultReceipt(s"exhausted-${UUID.randomUUID()}")),
          cancelling.revision,
          now
        )
        _ <- s.faults.notifyUnavailable.set(1000)
        worker = s.worker(maxAttempts = 3)
        _ <- s.pumpUntil(List(worker), success(s.repository.findNotificationRepairs(s.id)).map(_.size == 2))
        _ <- MongoWorkflowIntegrityAudit.audit(s.fixture.database)
        // A fresh informational command is still pending when the account goes away.
        _ <- s.faults.notifyUnavailable.set(0)
        _ <- success(s.repository.repairNotifications(s.id, UUID.randomUUID(), now, UserId(UUID.randomUUID())))
        pendingRows <- s.commandRows.map(_.count(_.state == InterviewWorkflowCommandState.Pending))
        _ <- delete(s, s.workflow.candidateId)
        sentBefore <- s.faults.deliveries.get
        _ <- idle(s)
        sentFenced <- s.faults.deliveries.get
        _ <- finish(cleanupOf(s, recorder), s.workflow.candidateId)
        gone <- remaining(s)
        repairs <- success(s.repository.findNotificationRepairs(s.id))
        _ <- idle(s)
        sentAfter <- s.faults.deliveries.get
        receipts <- s.receipts
        _ <- MongoWorkflowIntegrityAudit.audit(s.fixture.database)
      } yield {
        assertEquals(failed.phase, InterviewWorkflowPhase.RepairRequired)
        assert(pendingRows >= 2, "informational notifications were pending at deletion")
        assertEquals(sentFenced, sentBefore, "a fenced subject gets nothing while the data is still there")
        assertEquals(sentAfter, sentBefore, "and nothing after the purge")
        assertEquals(gone, nothingLeft)
        assertEquals(repairs, Nil)
        assertEquals(receipts, Nil)
      }
    }
  }

  test("a result and a command redelivered after the deletion are settled without any effect") {
    scenario { s =>
      for {
        recorder <- holds(s)
        sink <- Ref.of[IO, Vector[InterviewMessage]](Vector.empty)
        transport = captureTransport(sink)
        worker = s.worker()
        _ <- cancelBy(s, UserRole.Candidate, s.workflow.candidateId)
        _ <- worker.publishDue(transport)
        command <- sink.getAndSet(Vector.empty).map(_.head)
        executed <- worker.receiveCommand(command)
        _ <- worker.publishDue(transport)
        result <- sink.getAndSet(Vector.empty).map(_.head)
        confirmed <- s.faults.confirmedCancels.get
        _ <- delete(s, s.workflow.candidateId)
        beforePurge <- worker.receiveResult(result)
        stillPending <- s.current
        _ <- finish(cleanupOf(s, recorder), s.workflow.candidateId)
        gone <- remaining(s)
        commandAgain <- worker.receiveCommand(command)
        resultAgain <- worker.receiveResult(result)
        again <- remaining(s)
        calls <- recorder.calls.get
        providerCancels <- s.faults.confirmedCancels.get
        sent <- s.faults.deliveries.get
      } yield {
        assert(executed)
        assertEquals(confirmed, List(InterviewWorkflow.cancellationKey(s.id, 0)))
        assert(!beforePurge, "a result for a fenced subject is not applied")
        assertEquals(stillPending.phase, InterviewWorkflowPhase.CancelPending)
        assertEquals(gone, nothingLeft)
        assert(commandAgain && resultAgain, "redelivered messages for a purged workflow are acknowledged")
        assertEquals(again, nothingLeft, "and recreate nothing")
        assertEquals(
          calls,
          Vector.empty[HoldCall],
          "the provider already cancelled the slot: nothing is cancelled twice"
        )
        assertEquals(providerCancels, confirmed)
        assertEquals(sent, Nil)
      }
    }
  }

  test("a cancel command redelivered after the deletion is refused first and then settled without a provider call") {
    scenario { s =>
      for {
        recorder <- holds(s)
        sink <- Ref.of[IO, Vector[InterviewMessage]](Vector.empty)
        worker = s.worker()
        _ <- cancelBy(s, UserRole.Recruiter, s.workflow.recruiterId)
        _ <- worker.publishDue(captureTransport(sink))
        command <- sink.get.map(_.head)
        _ <- delete(s, s.workflow.recruiterId)
        refused <- worker.receiveCommand(command)
        calledBefore <- activity(s)
        _ <- finish(cleanupOf(s, recorder), s.workflow.recruiterId)
        settled <- worker.receiveCommand(command)
        gone <- remaining(s)
        calledAfter <- activity(s)
        calls <- recorder.keys
      } yield {
        assert(!refused)
        assertEquals(calledBefore, (Nil, Nil, Nil), "the fenced claim never reached the provider")
        assert(settled)
        assertEquals(gone, nothingLeft)
        assertEquals(calledAfter, (Nil, Nil, Nil))
        assertEquals(calls, Vector(InterviewWorkflow.cancellationKey(s.id, 0)))
      }
    }
  }

  test("a failed or unknown provider answer stops the purge, keeps all evidence and a restarted cleanup completes") {
    scenario { s =>
      for {
        proposed <- propose(s)
        _ <- accept(s, proposed.revision)
        _ <- s.setApplicationStatus(ApplicationStatus.Hired)
        _ <- s.faults.cancelUnavailable.set(1000)
        _ <- s.drive(List(s.worker(maxAttempts = 3)), _.phase == InterviewWorkflowPhase.RepairRequired)
        _ <- delete(s, s.workflow.candidateId)
        firstKey = InterviewWorkflow.cancellationKey(s.id, 0)
        secondKey = InterviewWorkflow.cancellationKey(s.id, 1)
        unavailableOnce <- Ref.of[IO, Boolean](true)
        // Holds are visited in identity order; the replacement's reserve key sorts after the workflow id.
        flaky <- holds(
          s,
          hold =>
            if (hold.cancelKey == secondKey)
              unavailableOnce.getAndSet(false).flatMap { fail =>
                IO.realTimeInstant.map(at =>
                  if (fail) Left(InterviewProviderError.Unavailable)
                  else Right(InterviewCalendarCancellation.Cancelled(at))
                )
              }
            else IO.realTimeInstant.map(at => Right(InterviewCalendarCancellation.Cancelled(at)))
        )
        cleanup = cleanupOf(s, flaky)
        failure <- sweep(cleanup) // fences producers
        failurePurge <- sweep(cleanup)
        afterFailure <- remaining(s)
        state <- cleanup.find(s.workflow.candidateId).value
        // A restart: a new cleanup instance and a provider that knows nothing about the reservation.
        unknown <- holds(s, _ => IO.pure(Right(InterviewCalendarCancellation.UnknownReservation)))
        unknownFailure <- sweep(cleanupOf(s, unknown))
        afterUnknown <- remaining(s)
        restarted = cleanupOf(s, flaky)
        _ <- finish(restarted, s.workflow.candidateId)
        gone <- remaining(s)
        calls <- flaky.keys
      } yield {
        assertEquals(failure, None)
        assertEquals(failurePurge, Some(RepositoryError.Unavailable))
        assert(
          afterFailure("reservations") > 0 && afterFailure("workflow and request receipts") > 0,
          clue(afterFailure)
        )
        assertEquals(
          state.toOption.flatten.map(_.state),
          Some(InterviewCleanupState.ProducersFenced),
          "the cleanup did not advance past the purge"
        )
        assertEquals(
          unknownFailure,
          Some(RepositoryError.Conflict),
          "an unknown reservation is not a confirmed release"
        )
        assert(afterUnknown("reservations") > 0 && afterUnknown("commands") > 0)
        assertEquals(gone, nothingLeft)
        assert(calls.contains(firstKey) && calls.count(_ == secondKey) == 2, clue(calls))
      }
    }
  }

  List("accept", "cancel").foreach { race =>
    test(s"a deletion racing a $race leaves no data, no live hold and no notification once cleanup finishes") {
      (1 to 3).toList.traverse_ { _ =>
        scenario { s =>
          for {
            recorder <- holds(s)
            open <- if (race == "accept") propose(s) else IO.pure(s.workflow)
            outcome <-
              (
                delete(s, s.workflow.candidateId),
                if (race == "accept")
                  s.apply(
                    InterviewLifecycleEvent.AcceptProposal(now),
                    s.actor(UserRole.Candidate, s.workflow.candidateId, input = "accept-race"),
                    open.revision,
                    now
                  )
                else
                  s.apply(
                    InterviewLifecycleEvent.Cancel(InterviewCancellationInitiator.Recruiter, now),
                    s.actor(UserRole.Recruiter, s.workflow.recruiterId, input = "cancel-race"),
                    open.revision,
                    now
                  )
              ).parTupled.map(_._2)
            // Whoever won, running workers meanwhile and cleaning up afterwards converges to nothing.
            _ <- s.round(s.worker("race-worker")).replicateA_(2)
            _ <- finish(cleanupOf(s, recorder), s.workflow.candidateId)
            gone <- remaining(s)
            calls <- recorder.calls.get
            before <- activity(s)
            _ <- idle(s)
            after <- activity(s)
            deletedCandidate <- MongoRepositoryTestSupport
              .findOne(
                s.fixture.database,
                MongoCollections.Users,
                Filters.eq(MongoFields.Id, s.workflow.candidateId.value.toString)
              )
              .map(_.map(_.getString(MongoFields.AccountStatus)))
          } yield {
            assertEquals(deletedCandidate, Some("Deleted"))
            assertEquals(gone, nothingLeft, clue(outcome))
            assert(calls.forall(call => call.rowPresent && !call.releasedBefore), clue(calls))
            assertEquals(after, before, "nothing is created after the purge")
          }
        }
      }
    }
  }

  test("the live-hold lookup of a purge is served by the reservation identity index, not by a collection scan") {
    mongoResource.use { fixture =>
      val subject = UserId(UUID.randomUUID())
      val workflowId = UUID.randomUUID()
      val at = Instant.now()
      def reservation(owner: UUID, participant: UserId) =
        new org.bson.Document(MongoFields.Id, owner.toString)
          .append("workflowId", owner.toString)
          .append("reserveKey", s"$owner:reserve")
          .append("releaseKey", s"$owner:release")
          .append("candidateId", participant.value.toString)
          .append("recruiterId", UUID.randomUUID().toString)
          .append("participants", java.util.List.of(participant.value.toString))
          .append("startsAt", java.util.Date.from(at.plusSeconds(3600)))
          .append("endsAt", java.util.Date.from(at.plusSeconds(7200)))
          .append("reservedAt", java.util.Date.from(at))
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.InterviewWorkflows,
          new org.bson.Document(MongoFields.Id, workflowId.toString)
            .append("candidateId", subject.value.toString)
            .append("recruiterId", UUID.randomUUID().toString)
        )
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.InterviewCalendarReservations,
          reservation(workflowId, subject)
        )
        _ <- (1 to 200).toList.traverse_(_ =>
          MongoRepositoryTestSupport.insertOne(
            fixture.database,
            MongoCollections.InterviewCalendarReservations,
            reservation(UUID.randomUUID(), UserId(UUID.randomUUID()))
          )
        )
        _ <- fixture.commands.clear
        cleanup = new MongoInterviewSubjectCleanup(fixture.database)
        _ <- success(cleanup.purge(subject))
        reads <- fixture.commands.snapshot.map(
          _.filter(command =>
            command.getFirstKey == "find" &&
              command.getString("find").getValue == MongoCollections.InterviewCalendarReservations &&
              command.toJson.contains("releasedAt")
          )
        )
        plans <- reads.traverse { observed =>
          val command = org.bson.Document.parse(observed.toJson)
          List("$db", "lsid", "readConcern").foreach(command.remove)
          MongoAccessEvaluationSupport.command(
            fixture.database,
            new org.bson.Document("explain", command).append("verbosity", "executionStats")
          )
        }
        left <- MongoRepositoryTestSupport.count(
          fixture.database,
          MongoCollections.InterviewCalendarReservations,
          Filters.eq("workflowId", workflowId.toString)
        )
      } yield {
        assert(reads.nonEmpty, "the purge looked up live holds")
        plans.foreach { result =>
          val json = io.circe.parser.parse(result.toJson).toOption.getOrElse(fail("Invalid explain JSON"))
          val stats = json.hcursor.downField("executionStats")
          val examined = stats.get[Long]("totalDocsExamined").toOption.getOrElse(fail("Missing documents examined"))
          val keys = stats.get[Long]("totalKeysExamined").toOption.getOrElse(fail("Missing keys examined"))
          println(s"Live-hold lookup: documentsExamined=$examined keysExamined=$keys of 201 reservations")
          assert(examined <= 2L && keys <= 2L, s"examined $examined documents and $keys keys")
          assert(!json.findAllByKey("stage").flatMap(_.asString).contains("COLLSCAN"))
        }
        assertEquals(left, 0L)
      }
    }
  }
}
