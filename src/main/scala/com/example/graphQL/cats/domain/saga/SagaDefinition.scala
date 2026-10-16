package com.example.graphQL.cats.domain.saga

/** Lookup behaviour of a step when its outcome is unknown. */
enum LookupSupport {

  /** The step has no generic outcome other than confirmation or rejection. */
  case Unsupported

  /** Unknown outcome moves to `lookup`; nothing else (for example a release is simply re-issued). */
  case UnknownOnly

  /** Unknown -> lookup, lookup found = confirmed, lookup absent -> `start` again with the same key. */
  case Full
}

/** The result of interpreting one input, before the engine applies revision and phase. */
enum SagaMove[I, P, C, R] {

  /** Move to `phase`, emit `commands`, after `update` changed the workflow payload (never the revision). */
  case Advance(phase: P, commands: List[C], update: I => I)

  /** Enter the repair phase with `reason`; the engine records the origin phase when the definition asks for it. */
  case Repair(reason: R)

  /** Idempotent no-op: no revision bump, no command. */
  case Unchanged()
}

object SagaMove {
  def advance[I, P, C, R](phase: P, commands: List[C]): SagaMove[I, P, C, R] =
    SagaMove.Advance(phase, commands, identity)
}

/** A definition-specific interpretation of an instance and the optional payload of the input. It may fail, so every
  * precondition (for example a missing pending interval) is decided here, before the engine applies anything.
  */
type SagaRoute[I, P, C, R, E, X] = (I, Option[E]) => Either[SagaError[X], SagaMove[I, P, C, R]]

/** What a rejected step does. */
enum Rejection[I, P, C, R, E, X] {
  case Unsupported()

  /** Nothing to compensate: straight to repair with this reason. */
  case ToRepair(reason: R)

  /** Run the compensation declared by the step's `StepUndo.Compensate`. */
  case Compensate()

  /** Definition-specific route (counted as an escape hatch). */
  case Custom(route: SagaRoute[I, P, C, R, E, X])
}

/** A non-terminal phase driven by one step. `onConfirmed` is absent when confirmations are per-participant triggers. */
final case class PhaseNode[I, P, C, R, E, X](
    step: SagaStep[I, P, C],
    lookup: LookupSupport,
    onConfirmed: Option[SagaRoute[I, P, C, R, E, X]],
    onRejected: Rejection[I, P, C, R, E, X]
)

/** How a saga instance exposes its generic fields. The engine is the only writer of revision and repair origin. */
trait SagaInstanceAccess[I, P] {
  def revision(instance: I): Long
  def phase(instance: I): P
  def repairOrigin(instance: I): Option[P]
  def withRevisionAndPhase(instance: I, revision: Long, phase: P): I
  def withRepairOrigin(instance: I, origin: Option[P]): I
}

/** Repair vocabulary of a definition. */
final case class RepairPolicy[C, F, R](
    recordsOrigin: Boolean,
    requireRepair: R => C,
    exhausted: F => R,
    compensationFailed: F => R
)

/** Everything the engine accepts. An outcome names the step it belongs to, so a reply for another phase's step is
  * refused, and may carry a definition payload (for example a confirmation time). `Trigger` carries workflow-specific
  * events such as actor-initiated ones.
  */
enum SagaInput[+E, +F] {
  case Outcome(step: String, outcome: StepOutcome[F], payload: Option[E])
  case Resume
  case Trigger(event: E)
}

final case class SagaDecision[I, C](instance: I, commands: List[C])

/** A workflow type as a pure value: phases, step catalogue graph, repair vocabulary and definition-specific triggers.
  */
final case class SagaDefinition[I, P, E, C, F, R, X](
    name: String,
    access: SagaInstanceAccess[I, P],
    phases: List[P],
    terminal: Set[P],
    eventDriven: Set[P],
    repairPhase: P,
    nodes: Map[P, PhaseNode[I, P, C, R, E, X]],
    repair: RepairPolicy[C, F, R],
    triggers: (I, E) => Either[SagaError[X], SagaMove[I, P, C, R]]
) {

  /** Every phase step and every compensation target, duplicates kept so uniqueness of names can be checked. */
  def allSteps: List[SagaStep[I, P, C]] =
    nodes.values.toList.flatMap { node =>
      node.step :: (node.step.undo match {
        case StepUndo.Compensate(target) => List(target)
        case StepUndo.Irreversible(_)    => Nil
      })
    }

  /** The catalogue: steps by name, once. */
  def steps: List[SagaStep[I, P, C]] = allSteps.distinct

  def isCompensationPhase(phase: P): Boolean =
    nodes.values.exists(_.step.undo match {
      case StepUndo.Compensate(target) => target.phase == phase
      case StepUndo.Irreversible(_)    => false
    })

  def status(phase: P): SagaStatus =
    if (phase == repairPhase) SagaStatus.RepairRequired
    else if (terminal.contains(phase)) SagaStatus.Completed
    else if (isCompensationPhase(phase)) SagaStatus.Compensating
    else SagaStatus.Running
}
