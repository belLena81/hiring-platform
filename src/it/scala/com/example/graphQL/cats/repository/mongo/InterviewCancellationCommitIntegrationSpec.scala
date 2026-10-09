package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, UserId}
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.port.*
import com.mongodb.client.model.{Filters, Updates}
import java.time.Instant
import java.util.{Date, UUID}
import scala.concurrent.duration.*

/** Real replica-set evidence for the cancellation transaction and the persistence of lifecycle state and intents. */
final class InterviewCancellationCommitIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes

  private val now = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS)
  private val interval = InterviewInterval(now.plusSeconds(172800), now.plusSeconds(176400))
  private val replacement = InterviewInterval(now.plusSeconds(259200), now.plusSeconds(262800))

  private final case class Arrangement(
      fixture: MongoAccessEvaluationSupport.Fixture,
      repository: MongoInterviewWorkflowRepository,
      workflow: InterviewWorkflow
  ) {
    def collection(name: String) = Mongo4catsCollections.documents(fixture.database, name)
    def count(name: String): IO[Long] = MongoRepositoryTestSupport.count(fixture.database, name)
    def stored(name: String, id: String) =
      MongoRepositoryTestSupport.findOne(fixture.database, name, Filters.eq(MongoFields.Id, id))
    def application: IO[Option[org.bson.Document]] =
      stored(MongoCollections.Applications, workflow.applicationId.value.toString)
    def workflowDocument: IO[org.bson.Document] =
      stored(MongoCollections.InterviewWorkflows, workflow.id.value.toString).flatMap(found =>
        IO.fromOption(found)(new AssertionError("workflow missing"))
      )
    def current: IO[InterviewWorkflow] =
      repository
        .findForAdmin(workflow.id)
        .value
        .flatMap(_.fold(error => IO.raiseError(new AssertionError(s"$error")), IO.pure))
        .flatMap(found => IO.fromOption(found)(new AssertionError("workflow missing")))
  }

  private def arranged[A](held: InterviewInterval = interval)(use: Arrangement => IO[A]): IO[A] =
    mongoResource.use { fixture =>
      val repository = MongoInterviewWorkflowRepository.live(fixture.database, fixture.client, Diagnostics.noop)
      val base = InterviewWorkflow
        .create(
          InterviewWorkflowId(UUID.randomUUID()),
          ApplicationId(UUID.randomUUID()),
          UserId(UUID.randomUUID()),
          UserId(UUID.randomUUID()),
          held,
          Instant.now().plusSeconds(300),
          UUID.randomUUID(),
          ApplicationStatus.Accepted
        )
        .fold(error => fail(s"invalid workflow $error"), identity)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- InterviewSchedulingFixtures.seed(fixture.database, List(base), now)
        _ <- repository
          .create(
            base,
            InterviewWorkflow.initialCommand(base),
            UUID.randomUUID(),
            MutationReceiptFingerprint.fromCanonicalInput("arrangement"),
            now
          )
          .value
          .flatMap(_.fold(error => IO.raiseError(new AssertionError(s"$error")), _ => IO.unit))
        arrangement = Arrangement(fixture, repository, base)
        // The scheduling hold the workflow carries, so its reservation takes part in every retention assertion.
        _ <- repository
          .reserveIfAvailable(
            InterviewCalendarReservation(
              base.id,
              InterviewWorkflow.reservationKey(base.id, 0),
              base.candidateId,
              base.recruiterId,
              held,
              now,
              None
            )
          )
          .value
          .flatMap(_.fold(error => IO.raiseError(new AssertionError(s"$error")), _ => IO.unit))
        workflows <- arrangement.collection(MongoCollections.InterviewWorkflows)
        _ <- workflows.updateOne(
          Filters.eq(MongoFields.Id, base.id.value.toString),
          Updates.combine(
            Updates.set("phase", InterviewWorkflowPhase.Completed.toString),
            Updates.set("notified", List("Candidate", "Recruiter").asJavaList)
          )
        )
        applications <- arrangement.collection(MongoCollections.Applications)
        _ <- applications.updateOne(
          Filters.eq(MongoFields.Id, base.applicationId.value.toString),
          Updates.set(MongoFields.Status, ApplicationStatus.Interview.toString)
        )
        result <- use(arrangement)
      } yield result
    }

  extension (values: List[String]) private def asJavaList = scala.jdk.CollectionConverters.SeqHasAsJava(values).asJava

  private def actor(role: UserRole, id: UserId, key: UUID = UUID.randomUUID(), input: String = "cancel") =
    InterviewLifecycleOrigin.Actor(
      InterviewWorkflowAccess(id, role),
      key,
      MutationReceiptFingerprint.fromCanonicalInput(input)
    )

  private def apply(
      a: Arrangement,
      event: InterviewLifecycleEvent,
      origin: InterviewLifecycleOrigin,
      expected: Long = 0L
  ): IO[Either[com.example.graphQL.cats.service.RepositoryError, InterviewLifecycleOutcome]] =
    a.repository.applyLifecycle(a.workflow.id, expected, event, origin, now, None).value

  private def applied(outcome: Either[?, InterviewLifecycleOutcome]): InterviewWorkflow = outcome match {
    case Right(InterviewLifecycleOutcome.Applied(workflow)) => workflow
    case other                                              => fail(s"expected an applied transition, got $other")
  }

  private def untouched(a: Arrangement, history: Long = 0L, outbox: Long = 0L): IO[Unit] =
    for {
      workflow <- a.current
      application <- a.application
      events <- a.count(MongoCollections.ApplicationEvents)
      events2 <- a.count(MongoCollections.EventOutbox)
      lifecycleCommands <- a.count(MongoCollections.InterviewWorkflowCommands)
    } yield {
      assertEquals(workflow.phase, InterviewWorkflowPhase.Completed)
      assertEquals(workflow.revision, 0L)
      assertEquals(application.map(_.getString(MongoFields.Status)), Some("Interview"))
      assertEquals(events, history)
      assertEquals(events2, outbox)
      assertEquals(lifecycleCommands, 1L) // only the scheduling command created with the workflow
    }

  test(
    "candidate cancellation rejects the application, appends generated history and records the provider intent atomically"
  ) {
    arranged() { a =>
      val origin = actor(UserRole.Candidate, a.workflow.candidateId)
      for {
        result <- apply(a, InterviewLifecycleEvent.Cancel(InterviewCancellationInitiator.Candidate, now), origin)
        next = applied(result)
        stored <- a.current
        application <- a.application
        history <- MongoRepositoryTestSupport.findOne(
          a.fixture.database,
          MongoCollections.ApplicationEvents,
          Filters.eq("applicationId", a.workflow.applicationId.value.toString)
        )
        outbox <- a.count(MongoCollections.EventOutbox)
        step <- a.repository.findCommand(a.workflow.id, s"${a.workflow.id.value}:1:0").value
        replay <- apply(a, InterviewLifecycleEvent.Cancel(InterviewCancellationInitiator.Candidate, now), origin)
        historyAfter <- a.count(MongoCollections.ApplicationEvents)
      } yield {
        assertEquals(stored, next)
        assertEquals(stored.phase, InterviewWorkflowPhase.CancelPending)
        assertEquals(stored.revision, 1L)
        assertEquals(application.map(_.getString(MongoFields.Status)), Some("Rejected"))
        assertEquals(history.map(_.getString("feedback")), Some("Interview cancelled by the candidate."))
        assertEquals(history.map(_.getString("actorId")), Some(a.workflow.candidateId.value.toString))
        assertEquals(history.map(_.getString("previousStatus")), Some("Interview"))
        assertEquals(outbox, 1L)
        assertEquals(
          step.toOption.flatten.map(_.command),
          Some(InterviewLifecycleCommand.CancelCalendarSlot(InterviewWorkflow.cancellationKey(a.workflow.id, 0)))
        )
        assertEquals(step.toOption.flatten.map(_.state), Some(InterviewWorkflowCommandState.Pending))
        assertEquals(replay, Right(InterviewLifecycleOutcome.Duplicate(stored)))
        assertEquals(historyAfter, 1L)
      }
    }
  }

  test("recruiter cancellation records the recruiter text and rejects initiator and role mismatches without writes") {
    arranged() { a =>
      for {
        mismatch <- apply(
          a,
          InterviewLifecycleEvent.Cancel(InterviewCancellationInitiator.Admin, now),
          actor(UserRole.Recruiter, a.workflow.recruiterId)
        )
        _ <- untouched(a)
        ok <- apply(
          a,
          InterviewLifecycleEvent.Cancel(InterviewCancellationInitiator.Recruiter, now),
          actor(UserRole.Recruiter, a.workflow.recruiterId)
        )
        history <- MongoRepositoryTestSupport.findOne(
          a.fixture.database,
          MongoCollections.ApplicationEvents,
          Filters.eq("applicationId", a.workflow.applicationId.value.toString)
        )
      } yield {
        assertEquals(mismatch, Left(com.example.graphQL.cats.service.RepositoryError.InvalidEvent))
        applied(ok)
        assertEquals(history.map(_.getString("feedback")), Some("Interview cancelled by the recruiter."))
      }
    }
  }

  test("a non-participant, a stale revision, a started interview and a non-Interview application write nothing") {
    arranged() { a =>
      val cancel = InterviewLifecycleEvent.Cancel(InterviewCancellationInitiator.Candidate, now)
      for {
        stranger <- apply(a, cancel, actor(UserRole.Candidate, UserId(UUID.randomUUID())))
        otherRecruiter <- apply(
          a,
          InterviewLifecycleEvent.Cancel(InterviewCancellationInitiator.Recruiter, now),
          actor(UserRole.Recruiter, UserId(UUID.randomUUID()))
        )
        stale <- apply(a, cancel, actor(UserRole.Candidate, a.workflow.candidateId), expected = 7L)
        started <- apply(
          a,
          InterviewLifecycleEvent.Cancel(InterviewCancellationInitiator.Candidate, interval.startsAt),
          actor(UserRole.Candidate, a.workflow.candidateId)
        )
        _ <- untouched(a)
        applications <- a.collection(MongoCollections.Applications)
        _ <- applications.updateOne(
          Filters.eq(MongoFields.Id, a.workflow.applicationId.value.toString),
          Updates.set(MongoFields.Status, ApplicationStatus.Hired.toString)
        )
        hired <- apply(a, cancel, actor(UserRole.Candidate, a.workflow.candidateId))
        events <- a.count(MongoCollections.ApplicationEvents)
        workflow <- a.current
      } yield {
        assertEquals(stranger, Right(InterviewLifecycleOutcome.NotVisible))
        assertEquals(otherRecruiter, Right(InterviewLifecycleOutcome.NotVisible))
        assertEquals(stale, Right(InterviewLifecycleOutcome.Rejected(InterviewWorkflowError.StaleRevision)))
        assertEquals(started, Right(InterviewLifecycleOutcome.Rejected(InterviewWorkflowError.InterviewAlreadyStarted)))
        assertEquals(hired, Right(InterviewLifecycleOutcome.ApplicationNotInterview(ApplicationStatus.Hired)))
        assertEquals(events, 0L)
        assertEquals(workflow.phase, InterviewWorkflowPhase.Completed)
      }
    }
  }

  test("a failure after the application and workflow writes rolls the whole cancellation back") {
    arranged() { a =>
      val eventId = UUID.nameUUIDFromBytes(s"${a.workflow.id.value}:cancel:status".getBytes("UTF-8"))
      val collision = ApplicationEvent(
        ApplicationEventId(eventId),
        a.workflow.applicationId,
        Some(ApplicationStatus.Accepted),
        ApplicationStatus.Interview,
        a.workflow.recruiterId,
        now,
        None,
        None
      )
      for {
        _ <- MongoRepositoryTestSupport.insertOne(
          a.fixture.database,
          MongoCollections.ApplicationEvents,
          MongoHiringCodecs.event(collision)
        )
        failed <- apply(
          a,
          InterviewLifecycleEvent.Cancel(InterviewCancellationInitiator.Candidate, now),
          actor(UserRole.Candidate, a.workflow.candidateId)
        )
        _ <- untouched(a, history = 1L)
        receipts <- a.count(MongoCollections.InterviewWorkflows)
      } yield {
        assert(failed.isLeft)
        assertEquals(receipts, 2L) // the workflow and its creation receipt: no cancellation receipt was kept
      }
    }
  }

  test(
    "a proposal persists its state, expiry intent and notification, leaves retention and is cleared by cancellation"
  ) {
    arranged() { a =>
      val proposer = actor(UserRole.Recruiter, a.workflow.recruiterId, input = "propose")
      val propose = InterviewLifecycleEvent.Propose(
        replacement.startsAt,
        replacement.endsAt,
        a.workflow.recruiterId,
        now,
        InterviewProposalTtl.Default
      )
      for {
        workflows <- a.collection(MongoCollections.InterviewWorkflows)
        _ <- workflows.updateOne(
          Filters.eq(MongoFields.Id, a.workflow.id.value.toString),
          Updates.set(MongoFields.RetentionExpiresAt, Date.from(now.plusSeconds(10)))
        )
        proposed <- apply(a, propose, proposer)
        next = applied(proposed)
        stored <- a.current
        document <- a.workflowDocument
        expiry <- a.repository.findCommand(a.workflow.id, s"${a.workflow.id.value}:1:0").value
        inform <- a.repository.findCommand(a.workflow.id, s"${a.workflow.id.value}:1:1").value
        claimed <- a.repository.claimDueCommands("worker", now.plusSeconds(60), now.plusSeconds(120), 5).value
        cancelled <- apply(
          a,
          InterviewLifecycleEvent.Cancel(InterviewCancellationInitiator.Recruiter, now),
          actor(UserRole.Recruiter, a.workflow.recruiterId, input = "cancel"),
          expected = 1L
        )
        afterCancel <- a.workflowDocument
        afterCancelWorkflow <- a.current
      } yield {
        assertEquals(stored, next)
        assertEquals(stored.phase, InterviewWorkflowPhase.ProposalPending)
        assertEquals(stored.proposal.map(_.interval), Some(replacement))
        assert(!document.containsKey(MongoFields.RetentionExpiresAt))
        val dueAt = stored.proposal.map(_.expiresAt)
        assertEquals(expiry.toOption.flatten.map(_.command), dueAt.map(InterviewLifecycleCommand.ExpireProposal.apply))
        assertEquals(expiry.toOption.flatten.map(_.availableAt), dueAt)
        assert(inform.toOption.flatten.exists(_.command.isInstanceOf[InterviewLifecycleCommand.Notify]))
        // The proposal's expiry is not due yet, so only its notification is claimable (one executor claims both kinds).
        assertEquals(
          claimed.map(_.map(_.record.stepId).filter(_.startsWith(s"${a.workflow.id.value}:1:"))),
          Right(List(s"${a.workflow.id.value}:1:1"))
        )
        assertEquals(afterCancelWorkflow.phase, InterviewWorkflowPhase.CancelPending)
        assertEquals(afterCancelWorkflow.proposal, None)
        assert(!afterCancel.containsKey("proposal"))
        applied(cancelled)
      }
    }
  }

  test("a reused request key with different input is a conflict and writes nothing") {
    arranged() { a =>
      val key = UUID.randomUUID()
      for {
        first <- apply(
          a,
          InterviewLifecycleEvent.RequestReschedule(now),
          actor(UserRole.Candidate, a.workflow.candidateId, key, "request")
        )
        reused <- apply(
          a,
          InterviewLifecycleEvent.Cancel(InterviewCancellationInitiator.Candidate, now),
          actor(UserRole.Candidate, a.workflow.candidateId, key, "cancel")
        )
        application <- a.application
        stored <- a.current
      } yield {
        applied(first)
        assertEquals(reused, Left(com.example.graphQL.cats.service.RepositoryError.Conflict))
        assertEquals(application.map(_.getString(MongoFields.Status)), Some("Interview"))
        assertEquals(stored.rescheduleRequestedAt, Some(now))
      }
    }
  }

  private def expiryOf(a: Arrangement, collection: String, id: String): IO[Option[Instant]] =
    a.stored(collection, id)
      .map(_.flatMap(document => Option(document.getDate(MongoFields.RetentionExpiresAt))).map(_.toInstant))

  private val retention = 8.days.toMillis
  private def datedExpiry(held: InterviewInterval = interval) = held.endsAt.plusMillis(retention)

  private def propose(a: Arrangement, expected: Long, key: String) =
    apply(
      a,
      InterviewLifecycleEvent
        .Propose(replacement.startsAt, replacement.endsAt, a.workflow.recruiterId, now, InterviewProposalTtl.Default),
      actor(UserRole.Recruiter, a.workflow.recruiterId, input = key),
      expected
    )

  test("evidence of a settled workflow outlives the interview and every return to Completed keeps the dated expiry") {
    arranged() { a =>
      val candidate = a.workflow.candidateId
      val recruiter = a.workflow.recruiterId
      val id = a.workflow.id.value.toString
      for {
        // decline
        _ <- propose(a, 0L, "p1").map(applied)
        _ <- apply(
          a,
          InterviewLifecycleEvent.DeclineProposal(now),
          actor(UserRole.Candidate, candidate, input = "d"),
          1L
        ).map(applied)
        afterDecline <- expiryOf(a, MongoCollections.InterviewWorkflows, id)
        // withdraw
        _ <- propose(a, 2L, "p2").map(applied)
        _ <- apply(
          a,
          InterviewLifecycleEvent.WithdrawProposal(now),
          actor(UserRole.Recruiter, recruiter, input = "w"),
          3L
        ).map(applied)
        afterWithdraw <- expiryOf(a, MongoCollections.InterviewWorkflows, id)
        // expiry
        proposed <- propose(a, 4L, "p3").map(applied)
        dueAt = proposed.proposal.map(_.expiresAt).getOrElse(now)
        _ <- apply(
          a,
          InterviewLifecycleEvent.ProposalExpired(dueAt),
          InterviewLifecycleOrigin.Internal(InterviewAdvanceCause.ResultReceipt("expiry-1")),
          5L
        ).map(applied)
        afterExpiry <- expiryOf(a, MongoCollections.InterviewWorkflows, id)
        commandExpiry <- expiryOf(a, MongoCollections.InterviewWorkflowCommands, s"$id:$id:6:0")
        inboxExpiry <- expiryOf(a, MongoCollections.InterviewWorkflowInbox, s"$id:expiry-1")
        reservationExpiry <- expiryOf(a, MongoCollections.InterviewCalendarReservations, id)
      } yield {
        val earliestPlainRetention = now.plusMillis(retention)
        List(afterDecline, afterWithdraw, afterExpiry, commandExpiry, inboxExpiry).foreach { value =>
          assertEquals(value, Some(datedExpiry()))
          // Beyond now + eight days but before the interview ends the evidence is still stored.
          assert(value.exists(_.isAfter(earliestPlainRetention)), clue(value))
        }
        assertEquals(reservationExpiry, Some(datedExpiry()))
      }
    }
  }

  test("an interview 30 days out keeps every piece of evidence until eight days after it ends") {
    val farOut = InterviewInterval(now.plusSeconds(30L * 86400L), now.plusSeconds(30L * 86400L + 3600L))
    arranged(farOut) { a =>
      val id = a.workflow.id.value.toString
      for {
        _ <- propose(a, 0L, "p1").map(applied)
        _ <- apply(
          a,
          InterviewLifecycleEvent.DeclineProposal(now),
          actor(UserRole.Candidate, a.workflow.candidateId, input = "d"),
          1L
        ).map(applied)
        workflowExpiry <- expiryOf(a, MongoCollections.InterviewWorkflows, id)
        commandExpiry <- expiryOf(a, MongoCollections.InterviewWorkflowCommands, s"$id:$id:2:0")
        reservationExpiry <- expiryOf(a, MongoCollections.InterviewCalendarReservations, id)
      } yield {
        val expected = Some(datedExpiry(farOut))
        assertEquals(workflowExpiry, expected)
        assertEquals(commandExpiry, expected)
        assertEquals(reservationExpiry, expected)
        // Far beyond the plain eight-day window: a 30-day-out interview stays cancellable until it has happened.
        assert(workflowExpiry.exists(_.isAfter(now.plusSeconds(30L * 86400L))), clue(workflowExpiry))
      }
    }
  }

  test("request and dismiss on a settled workflow give their receipts and intents the dated expiry") {
    arranged() { a =>
      val candidate = a.workflow.candidateId
      val recruiter = a.workflow.recruiterId
      val id = a.workflow.id.value.toString
      val requestKey = UUID.randomUUID()
      val dismissKey = UUID.randomUUID()
      for {
        _ <- propose(a, 0L, "p1").map(applied)
        _ <- apply(
          a,
          InterviewLifecycleEvent.DeclineProposal(now),
          actor(UserRole.Candidate, candidate, input = "d"),
          1L
        ).map(applied)
        requested <- apply(
          a,
          InterviewLifecycleEvent.RequestReschedule(now),
          actor(UserRole.Candidate, candidate, requestKey, "r"),
          2L
        ).map(applied)
        requestReceipt <- expiryOf(a, MongoCollections.InterviewWorkflows, s"request:${candidate.value}:$requestKey")
        notifyIntent <- expiryOf(a, MongoCollections.InterviewWorkflowCommands, s"$id:$id:3:0")
        _ <- apply(
          a,
          InterviewLifecycleEvent.DismissRescheduleRequest,
          actor(UserRole.Recruiter, recruiter, dismissKey, "x"),
          3L
        ).map(applied)
        dismissReceipt <- expiryOf(a, MongoCollections.InterviewWorkflows, s"request:${recruiter.value}:$dismissKey")
        // A repeated request is a no-op decision: it answers with the stored workflow and stores nothing at all.
        noopKey = UUID.randomUUID()
        _ <- apply(
          a,
          InterviewLifecycleEvent.RequestReschedule(now),
          actor(UserRole.Candidate, candidate, UUID.randomUUID(), "r2"),
          4L
        ).map(applied)
        receiptsBefore <- a.count(MongoCollections.InterviewWorkflows)
        noop <- apply(
          a,
          InterviewLifecycleEvent.RequestReschedule(now),
          actor(UserRole.Candidate, candidate, noopKey, "r3"),
          5L
        )
        noopReceipt <- a.stored(MongoCollections.InterviewWorkflows, s"request:${candidate.value}:$noopKey")
        receiptsAfter <- a.count(MongoCollections.InterviewWorkflows)
        reused <- apply(
          a,
          InterviewLifecycleEvent.DismissRescheduleRequest,
          actor(UserRole.Candidate, candidate, noopKey, "other"),
          5L
        )
      } yield {
        assertEquals(requested.rescheduleRequestedAt, Some(now))
        assertEquals(requestReceipt, Some(datedExpiry()))
        assertEquals(notifyIntent, Some(datedExpiry()))
        assertEquals(dismissReceipt, Some(datedExpiry()))
        assertEquals(noopReceipt, None)
        assertEquals(receiptsAfter, receiptsBefore)
        assert(noop.exists(_.isInstanceOf[InterviewLifecycleOutcome.Duplicate]), clue(noop))
        // A candidate may not dismiss at all, whatever the key was used for before.
        assertEquals(reused, Left(com.example.graphQL.cats.service.RepositoryError.InvalidEvent))
      }
    }
  }

  private def seedAdmin(a: Arrangement): IO[UserId] = {
    val id = UserId(UUID.randomUUID())
    MongoRepositoryTestSupport
      .insertOne(
        a.fixture.database,
        MongoCollections.Users,
        MongoHiringCodecs.user(User(id, None, s"Admin-${id.value}", UserRole.Admin, None, now, adminSingleton = true))
      )
      .as(id)
  }

  test(
    "an Admin cancels without a participant predicate, records the administrator text and is fenced as an active user"
  ) {
    arranged() { a =>
      for {
        admin <- seedAdmin(a)
        versionBefore <- a.stored(MongoCollections.Users, admin.value.toString).map(_.map(_.get(MongoFields.Version)))
        result <- apply(
          a,
          InterviewLifecycleEvent.Cancel(InterviewCancellationInitiator.Admin, now),
          actor(UserRole.Admin, admin)
        )
        versionAfter <- a.stored(MongoCollections.Users, admin.value.toString).map(_.map(_.get(MongoFields.Version)))
        history <- MongoRepositoryTestSupport.findOne(
          a.fixture.database,
          MongoCollections.ApplicationEvents,
          Filters.eq("applicationId", a.workflow.applicationId.value.toString)
        )
      } yield {
        applied(result)
        assertEquals(history.map(_.getString("feedback")), Some("Interview cancelled by an administrator."))
        assertEquals(history.map(_.getString("actorId")), Some(admin.value.toString))
        assertNotEquals(versionAfter, versionBefore)
      }
    }
  }

  test("an Admin that is not an active user is fenced out and nothing is written") {
    arranged() { a =>
      for {
        result <- apply(
          a,
          InterviewLifecycleEvent.Cancel(InterviewCancellationInitiator.Admin, now),
          actor(UserRole.Admin, UserId(UUID.randomUUID()))
        )
        _ <- untouched(a)
      } yield assert(result.isLeft, clue(result))
    }
  }

  test("a recruiter who no longer owns the job conflicts and nothing is written") {
    arranged() { a =>
      for {
        jobs <- a.collection(MongoCollections.Jobs)
        _ <- jobs.updateOne(
          Filters.eq(MongoFields.RecruiterId, a.workflow.recruiterId.value.toString),
          Updates.set(MongoFields.RecruiterId, UUID.randomUUID().toString)
        )
        result <- apply(
          a,
          InterviewLifecycleEvent.Cancel(InterviewCancellationInitiator.Recruiter, now),
          actor(UserRole.Recruiter, a.workflow.recruiterId)
        )
        _ <- untouched(a)
      } yield assertEquals(result, Left(com.example.graphQL.cats.service.RepositoryError.Conflict))
    }
  }

  test("a failure after the outbox event and request receipt were written rolls everything back") {
    arranged() { a =>
      for {
        commands <- a.collection(MongoCollections.InterviewWorkflowCommands)
        _ <- commands.insertOne(
          new org.bson.Document(MongoFields.Id, s"${a.workflow.id.value}:${a.workflow.id.value}:1:0")
            .append("workflowId", a.workflow.id.value.toString)
            .append("stepId", s"${a.workflow.id.value}:1:0")
            .append("revision", Long.box(1L))
            .append("attempts", Int.box(0))
            .append("executionAttempts", Int.box(0))
            .append("commandState", "Pending")
            .append("availableAt", Date.from(now))
            .append("occurredAt", Date.from(now))
            .append("command", new org.bson.Document("kind", "requireRepair").append("reason", "occupied"))
        )
        failed <- apply(
          a,
          InterviewLifecycleEvent.Cancel(InterviewCancellationInitiator.Candidate, now),
          actor(UserRole.Candidate, a.workflow.candidateId)
        )
        workflow <- a.current
        application <- a.application
        history <- a.count(MongoCollections.ApplicationEvents)
        outbox <- a.count(MongoCollections.EventOutbox)
        receipts <- a.count(MongoCollections.InterviewWorkflows)
      } yield {
        assert(failed.isLeft, clue(failed))
        assertEquals(workflow.phase, InterviewWorkflowPhase.Completed)
        assertEquals(application.map(_.getString(MongoFields.Status)), Some("Interview"))
        assertEquals((history, outbox, receipts), (0L, 0L, 2L))
      }
    }
  }
}
