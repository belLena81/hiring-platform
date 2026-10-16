package com.example.graphQL.cats.service.saga

import cats.Monad
import cats.data.EitherT
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.port.RepositoryF
import com.example.graphQL.cats.domain.saga.{SagaDefinition, SagaEngine, SagaError, SagaInput}
import java.time.Instant

/** Result of the durable write of one saga step. */
enum SagaAdvanceResult[+I] {
  case Applied

  /** The cause (message receipt) was already applied; the stored instance is returned unchanged. */
  case Duplicate(existing: I)

  /** Another writer advanced the instance first. */
  case StaleRevision
}

/** The durable side of a saga: state, inbox receipt (`cause`) and emitted commands commit in one transaction, with
  * revision compare-and-set, receipt and step uniqueness and fencing enforced by the store.
  */
trait SagaStore[F[_], I, Cause, C] {
  def advance(
      next: I,
      expectedRevision: Long,
      cause: Cause,
      commands: List[C],
      occurredAt: Instant,
      availableAt: Option[Instant]
  ): RepositoryF[F, SagaAdvanceResult[I]]
}

/** What happened to one input. `Refused` means the pure decision rejected it and nothing was written. */
enum SagaSettlement[+I, +X] {
  case Applied
  case Duplicate(existing: I)
  case StaleRevision
  case Refused(error: SagaError[X])
}

/** Sequences effects only: decide purely with the engine, then hand the decision to the store. No business decision
  * lives here, and it needs nothing beyond `Monad`: time and backoff instants are inputs.
  */
final class SagaOrchestrator[F[_]: Monad, I, P, E, C, Fail, R, X, Cause](
    definition: SagaDefinition[I, P, E, C, Fail, R, X],
    store: SagaStore[F, I, Cause, C]
) {

  def settle(
      instance: I,
      input: SagaInput[E, Fail],
      cause: Cause,
      occurredAt: Instant,
      availableAt: Option[Instant]
  ): RepositoryF[F, SagaSettlement[I, X]] = {
    val expectedRevision = definition.access.revision(instance)
    SagaEngine.decide(definition, instance, expectedRevision, input) match {
      case Left(error) =>
        EitherT.pure[F, RepositoryError](SagaSettlement.Refused(error))
      case Right(decision) =>
        store.advance(decision.instance, expectedRevision, cause, decision.commands, occurredAt, availableAt).map {
          case SagaAdvanceResult.Applied             => SagaSettlement.Applied
          case SagaAdvanceResult.Duplicate(existing) => SagaSettlement.Duplicate(existing)
          case SagaAdvanceResult.StaleRevision       => SagaSettlement.StaleRevision
        }
    }
  }
}
