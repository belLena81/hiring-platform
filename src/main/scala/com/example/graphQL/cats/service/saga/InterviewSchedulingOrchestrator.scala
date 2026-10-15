package com.example.graphQL.cats.service.saga

import cats.Monad
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.port.{InterviewWorkflowAdvanceResult, InterviewWorkflowRepository, RepositoryF}
import java.time.Instant

/** The scheduling saga's store: the existing interview repository already commits state, inbox receipt and command
  * intents atomically with a revision compare-and-set, so this only translates its result. Nothing is stored
  * differently.
  */
final class InterviewSchedulingSagaStore[F[_]: Monad](repository: InterviewWorkflowRepository[F])
    extends SagaStore[F, InterviewWorkflow, InterviewAdvanceCause, InterviewWorkflowCommand] {

  def advance(
      next: InterviewWorkflow,
      expectedRevision: Long,
      cause: InterviewAdvanceCause,
      commands: List[InterviewWorkflowCommand],
      occurredAt: Instant,
      availableAt: Option[Instant]
  ): RepositoryF[F, SagaAdvanceResult[InterviewWorkflow]] =
    repository.advance(next, expectedRevision, cause, commands, occurredAt, availableAt).map {
      case InterviewWorkflowAdvanceResult.Applied             => SagaAdvanceResult.Applied
      case InterviewWorkflowAdvanceResult.Duplicate(existing) => SagaAdvanceResult.Duplicate(existing)
      case InterviewWorkflowAdvanceResult.StaleRevision       => SagaAdvanceResult.StaleRevision
    }
}

object InterviewSchedulingOrchestrator {
  type Orchestrator[F[_]] = SagaOrchestrator[
    F,
    InterviewWorkflow,
    InterviewWorkflowPhase,
    InterviewWorkflowEvent,
    InterviewWorkflowCommand,
    InterviewFailureCode,
    InterviewRepairReason,
    InterviewWorkflowError,
    InterviewAdvanceCause
  ]

  def apply[F[_]: Monad](repository: InterviewWorkflowRepository[F]): Orchestrator[F] =
    new SagaOrchestrator(InterviewSchedulingSaga.definition, new InterviewSchedulingSagaStore(repository))
}
