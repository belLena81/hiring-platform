package com.example.graphQL.cats.domain.saga

/** Where a saga instance stands, derived from its phase by the definition; it is never stored as a new field. */
enum SagaStatus {
  case Running, Compensating, Completed, RepairRequired
}

/** Reference to a retry budget owned by existing typed configuration; the catalogue never holds the number. */
final case class RetryBudget(name: String)

/** Typed failure of a pure saga decision. `Definition` carries a workflow-specific business error. */
enum SagaError[+X] {
  case StaleRevision
  case InvalidTransition
  case Definition(error: X)
}

/** The single revision compare-and-set rule: equal revisions, and `Long.MaxValue` could not be incremented. */
object SagaRevision {
  def matches(current: Long, expected: Long): Boolean = current == expected && expected != Long.MaxValue
}

/** Generic outcome of a step. A definition maps its existing events to these without renaming them. */
enum StepOutcome[+F] {
  case Confirmed
  case Rejected
  case OutcomeUnknown
  case LookupFound
  case LookupAbsent
  case RetryExhausted(failure: F)
}

/** What undoes a step. Every step must declare one; there is no default. */
enum StepUndo[I, P, C] {
  case Compensate(step: SagaStep[I, P, C])
  case Irreversible(reason: String)
}

/** A catalogue entry: the effect run in `phase`, its stable idempotency key, how to look it up, and its undo. Two steps
  * are the same step when their names are equal: function fields have no useful equality, and the catalogue requires
  * unique names.
  */
final case class SagaStep[I, P, C](
    name: String,
    phase: P,
    key: I => String,
    start: I => List[C],
    lookup: I => List[C],
    undo: StepUndo[I, P, C],
    retryBudget: RetryBudget
) {
  override def equals(other: Any): Boolean = other match {
    case that: SagaStep[?, ?, ?] => that.name == name
    case _                       => false
  }

  override def hashCode: Int = name.hashCode
}

/** Forward-only ordered phases: only the immediate next phase is a valid move and nothing is undone. */
final case class ForwardOnlyPhases[P](ordered: Vector[P]) {
  def next(phase: P): Option[P] = {
    val index = ordered.indexOf(phase)
    if (index >= 0) ordered.lift(index + 1) else None
  }

  def advance(from: P, to: P): Either[SagaError[Nothing], P] =
    Either.cond(next(from).contains(to), to, SagaError.InvalidTransition)
}
