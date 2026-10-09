package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.domain.InterviewLifecycleFixtures.*
import com.example.graphQL.cats.domain.workflow.*
import munit.FunSuite
import org.bson.Document
import java.util.Date

final class MongoInterviewLifecycleCodecSpec extends FunSuite {
  private val codec = MongoInterviewWorkflowCommandCodec
  private val at = Date.from(now)

  private def envelope(step: String, command: Document, availableAt: Date = at): Document =
    new Document("_id", s"${workflowId.value}:$step")
      .append("workflowId", workflowId.value.toString)
      .append("stepId", step)
      .append("revision", Long.box(4L))
      .append("commandState", "Pending")
      .append("attempts", Int.box(0))
      .append("executionAttempts", Int.box(0))
      .append("availableAt", availableAt)
      .append("occurredAt", at)
      .append("command", command)

  private val samples: List[InterviewLifecycleCommand] = List(
    InterviewLifecycleCommand.CancelCalendarSlot(InterviewWorkflow.cancellationKey(workflowId, 0)),
    InterviewLifecycleCommand.LookupCalendarCancellation(InterviewWorkflow.cancellationKey(workflowId, 2)),
    InterviewLifecycleCommand.HoldReplacementSlot(InterviewWorkflow.reservationKey(workflowId, 1), replacement),
    InterviewLifecycleCommand.LookupReplacementHold(InterviewWorkflow.reservationKey(workflowId, 1)),
    InterviewLifecycleCommand.CommitRescheduledInterval(replacement, 1),
    InterviewLifecycleCommand.LookupRescheduleCommitReceipt(workflowId, 1),
    InterviewLifecycleCommand.ExpireProposal(now),
    InterviewLifecycleCommand.Notify(
      InterviewNotificationKind.RescheduleProposed,
      InterviewParticipant.Candidate,
      s"${workflowId.value}:rescheduleProposed:r4:notify:Candidate"
    ),
    InterviewLifecycleCommand.LookupNotificationReceipt(
      InterviewNotificationKind.Cancelled,
      InterviewParticipant.Recruiter,
      s"${workflowId.value}:cancelled:notify:Recruiter"
    ),
    InterviewLifecycleCommand.RequireRepair("retry exhausted: cancel")
  )

  test("every lifecycle intent round-trips through its stored shape and is recognised as a lifecycle row") {
    samples.zipWithIndex.foreach { case (command, index) =>
      val document = envelope(s"4:$index", codec.encodeLifecycle(command))
      val expected: InterviewCommand = command match {
        // The repair marker has one stored shape for both families and is administrative in each.
        case InterviewLifecycleCommand.RequireRepair(reason) => InterviewWorkflowCommand.RequireRepair(reason)
        case other                                           => other
      }
      assertEquals(codec.decode(document).map(_.command), Right(expected))
      assert(codec.decodeStored(document).isRight, clue(command))
    }
  }

  test("scheduling and lifecycle rows decode through one entry point so one executor claims both") {
    val scheduling = InterviewWorkflowCommand.LookupCalendarReservation(workflowId)
    val row = envelope(
      "4:0",
      new Document("kind", "lookupCalendar").append("workflowId", workflowId.value.toString)
    )
    assertEquals(codec.decode(row).map(_.command), Right(scheduling))
    assertEquals(
      codec.decode(envelope("4:1", codec.encodeLifecycle(samples.head))).map(_.command),
      Right(samples.head)
    )
  }

  test("keys that address another workflow, generation or participant are invalid stored data") {
    val foreign = InterviewWorkflowId(java.util.UUID.randomUUID())
    val invalid = List(
      new Document("kind", "cancelCalendar").append("idempotencyKey", InterviewWorkflow.cancellationKey(foreign, 0)),
      new Document("kind", "cancelCalendar").append("idempotencyKey", InterviewWorkflow.reservationKey(workflowId, 1)),
      codec
        .encodeLifecycle(
          InterviewLifecycleCommand.HoldReplacementSlot(InterviewWorkflow.reservationKey(workflowId, 0), replacement)
        ),
      codec.encodeLifecycle(InterviewLifecycleCommand.CommitRescheduledInterval(replacement, 0)),
      codec.encodeLifecycle(
        InterviewLifecycleCommand.Notify(
          InterviewNotificationKind.Cancelled,
          InterviewParticipant.Candidate,
          s"${workflowId.value}:cancelled:notify:Recruiter"
        )
      ),
      codec.encodeLifecycle(
        InterviewLifecycleCommand.Notify(
          InterviewNotificationKind.Cancelled,
          InterviewParticipant.Candidate,
          s"${workflowId.value}:rescheduled:notify:Candidate"
        )
      ),
      new Document("kind", "unknownIntent")
    )
    invalid.foreach(body => assert(codec.decodeStored(envelope("4:0", body)).isLeft, clue(body)))
    // The expiry intent must be due exactly when it says it expires.
    assert(
      codec
        .decode(envelope("4:0", codec.encodeLifecycle(InterviewLifecycleCommand.ExpireProposal(now)), new Date(0L)))
        .isLeft
    )
  }

  test("workflow lifecycle fields decode their defaults and reject combinations the phase cannot hold") {
    import InterviewWorkflowPhase as Phase
    val codecs = MongoInterviewWorkflowLifecycleCodec
    assertEquals(
      codecs.decode(new Document(), Phase.Completed),
      Right(codecs.LifecycleFields(0, None, None, None, None))
    )
    val proposal = proposalPending().proposal
    val withProposal = new Document()
    codecs.insertFields(proposalPending()).foreach { case (field, value) => withProposal.append(field, value) }
    assertEquals(codecs.decode(withProposal, Phase.ProposalPending).map(_.proposal), Right(proposal))
    assert(codecs.decode(withProposal, Phase.Completed).isLeft)
    assert(codecs.decode(new Document(), Phase.ProposalPending).isLeft)
    assert(codecs.decode(new Document(), Phase.RescheduleSwapPending).isLeft)
    assert(codecs.decode(new Document(), Phase.Cancelled).isLeft)
    assert(codecs.decode(new Document("rescheduleRequestedAt", at), Phase.CancelPending).isLeft)
    assert(codecs.decode(new Document("generation", Int.box(-1)), Phase.Completed).isLeft)
    assert(codecs.decode(new Document("generation", Long.box(1L)), Phase.Completed).isLeft)
  }

  test("skipped generations default to zero, must be non-negative integers and fit under the live generation") {
    import InterviewWorkflowPhase as Phase
    val codecs = MongoInterviewWorkflowLifecycleCodec
    assertEquals(codecs.decode(new Document(), Phase.Completed).map(_.skippedGenerations), Right(0))
    assertEquals(
      codecs.decode(new Document("skippedGenerations", Int.box(2)), Phase.Completed).map(_.skippedGenerations),
      Right(2)
    )
    assert(codecs.decode(new Document("skippedGenerations", Int.box(-1)), Phase.Completed).isLeft)
    assert(codecs.decode(new Document("skippedGenerations", Long.box(1L)), Phase.Completed).isLeft)
    assert(codecs.decode(new Document("skippedGenerations", "1"), Phase.Completed).isLeft)
    def retiring(generation: Int, skipped: Int) =
      new Document("generation", Int.box(generation)).append("skippedGenerations", Int.box(skipped))
    assert(codecs.decode(retiring(1, 0), Phase.RescheduleCancelOldPending).isRight)
    assert(codecs.decode(retiring(3, 2), Phase.RescheduleCancelOldPending).isRight)
    assert(codecs.decode(retiring(2, 2), Phase.RescheduleCancelOldPending).isLeft)
    assert(codecs.decode(retiring(0, 0), Phase.RescheduleCancelOldPending).isLeft)
  }

  test("the ledger collections carry business names and the original names exist only in the rename migration") {
    val renamed = MongoInterviewLedgerCollectionMigrations.Renames
    assertEquals(
      renamed.map(_._2),
      List("interview_calendar_reservations", "interview_calendar_participant_locks", "interview_notification_receipts")
    )
    assert(renamed.forall { case (legacy, current) => legacy.startsWith("fake_") && !current.contains("fake") })
    assert(MongoHiringMigrations.ownedCollections.forall(!_.contains("fake")))
    assert(MongoHiringIndexSetup.allSpecs.forall(spec => !spec.options.getName.contains("fake")))
  }
}
