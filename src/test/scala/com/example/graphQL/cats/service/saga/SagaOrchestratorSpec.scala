package com.example.graphQL.cats.service.saga

import cats.Id
import cats.data.EitherT
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.domain.workflow.characterization.InterviewCharacterizationFixtures.*
import com.example.graphQL.cats.service.port.RepositoryF
import com.example.graphQL.cats.domain.saga.{SagaError, SagaInput, StepOutcome}
import java.time.Instant

/** SAG-05: the orchestrator sequences effects only. It runs here in `Id`, so it needs nothing beyond `Monad`; there are
  * no timers or sleeps to control, and backoff instants are plain inputs.
  */
class SagaOrchestratorSpec extends munit.FunSuite {

  /** An in-memory store with the contract of the interview repository: revision CAS and receipt uniqueness. */
  private final class MemoryStore(var current: InterviewWorkflow)
      extends SagaStore[Id, InterviewWorkflow, String, InterviewWorkflowCommand] {
    var receipts: Map[String, InterviewWorkflow] = Map.empty
    var writes: Vector[(InterviewWorkflow, List[InterviewWorkflowCommand], Option[Instant])] = Vector.empty

    def advance(
        next: InterviewWorkflow,
        expectedRevision: Long,
        cause: String,
        commands: List[InterviewWorkflowCommand],
        occurredAt: Instant,
        availableAt: Option[Instant]
    ): RepositoryF[Id, SagaAdvanceResult[InterviewWorkflow]] =
      EitherT.pure[Id, com.example.graphQL.cats.service.RepositoryError](
        receipts.get(cause) match {
          case Some(existing)                               => SagaAdvanceResult.Duplicate(existing)
          case None if current.revision != expectedRevision => SagaAdvanceResult.StaleRevision
          case None                                         =>
            current = next
            receipts = receipts.updated(cause, next)
            writes = writes :+ ((next, commands, availableAt))
            SagaAdvanceResult.Applied
        }
      )
  }

  private val start = workflow(InterviewWorkflowPhase.ReservationPending)
  private val confirmed =
    SagaInput.Outcome(InterviewSchedulingSaga.ReserveCalendarSlotStep, StepOutcome.Confirmed, None)
  private val retryAt = Some(Instant.parse("2026-10-01T12:01:00Z"))

  private def orchestrator(store: MemoryStore) =
    new SagaOrchestrator(InterviewSchedulingSaga.definition, store)

  private def settle(
      o: SagaOrchestrator[
        Id,
        InterviewWorkflow,
        InterviewWorkflowPhase,
        InterviewWorkflowEvent,
        InterviewWorkflowCommand,
        InterviewFailureCode,
        InterviewRepairReason,
        InterviewWorkflowError,
        String
      ],
      w: InterviewWorkflow,
      input: SagaInput[InterviewWorkflowEvent, InterviewFailureCode],
      cause: String
  ) =
    o.settle(w, input, cause, now, retryAt).value.toOption.get

  test("SAG-05.1 a repeated message receipt returns Duplicate with the stored workflow and writes nothing") {
    val store = new MemoryStore(start)
    val o = orchestrator(store)
    assertEquals(settle(o, start, confirmed, "m1"), SagaSettlement.Applied)
    val stored = store.current
    assertEquals(settle(o, start, confirmed, "m1"), SagaSettlement.Duplicate(stored))
    assertEquals(store.writes.size, 1)
  }

  test("SAG-05.2 two writers from the same revision: one Applied, the other StaleRevision") {
    val store = new MemoryStore(start)
    val o = orchestrator(store)
    assertEquals(settle(o, start, confirmed, "m1"), SagaSettlement.Applied)
    assertEquals(settle(o, start, confirmed, "m2"), SagaSettlement.StaleRevision)
    assertEquals(store.writes.size, 1)
    assertEquals(store.current.revision, start.revision + 1L)
  }

  test("SAG-05.3 an input the pure decision refuses is Refused and nothing is written") {
    val store = new MemoryStore(start)
    val o = orchestrator(store)
    val wrongStep =
      SagaInput.Outcome(InterviewSchedulingSaga.CommitAcceptedToInterviewStep, StepOutcome.Confirmed, None)
    assertEquals(settle(o, start, wrongStep, "m1"), SagaSettlement.Refused(SagaError.InvalidTransition))
    assertEquals(store.writes, Vector.empty)
    assertEquals(store.current, start)
  }

  test("SAG-05.4 the decision, emitted commands and the retry instant reach the store unchanged") {
    val store = new MemoryStore(start)
    settle(orchestrator(store), start, confirmed, "m1")
    val (next, commands, availableAt) = store.writes.head
    assertEquals(next.phase, InterviewWorkflowPhase.StatusCommitPending)
    assertEquals(
      commands,
      List(
        InterviewWorkflowCommand.CommitAcceptedToInterview(
          com.example.graphQL.cats.domain.model.ApplicationStatus.Accepted
        )
      )
    )
    assertEquals(availableAt, retryAt)
  }

  test("SAG-05.5 the orchestrator runs in an effect with only a Monad (Id), so it needs no Temporal capability") {
    val store = new MemoryStore(start)
    assertEquals(settle(orchestrator(store), start, confirmed, "m1"), SagaSettlement.Applied)
  }
}
