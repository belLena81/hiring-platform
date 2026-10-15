package com.example.hiring.saga

/** The generic, pure decision rules, written once: revision compare-and-set, step identity, unknown outcome and lookup,
  * retry exhaustion, rejection and compensation, and Admin repair by lookup. No clock, IDs, I/O or logging.
  */
object SagaEngine {

  def decide[I, P, E, C, F, R, X](
      definition: SagaDefinition[I, P, E, C, F, R, X],
      instance: I,
      expectedRevision: Long,
      input: SagaInput[E, F]
  ): Either[SagaError[X], SagaDecision[I, C]] =
    if (!SagaRevision.matches(definition.access.revision(instance), expectedRevision)) Left(SagaError.StaleRevision)
    else interpret(definition, instance, input).map(move => applyMove(definition, instance, move))

  private def interpret[I, P, E, C, F, R, X](
      definition: SagaDefinition[I, P, E, C, F, R, X],
      instance: I,
      input: SagaInput[E, F]
  ): Either[SagaError[X], SagaMove[I, P, C, R]] = {
    val phase = definition.access.phase(instance)
    input match {
      case SagaInput.Trigger(event)                      => definition.triggers(instance, event)
      case SagaInput.Resume                              => resume(definition, instance, phase)
      case SagaInput.Outcome(stepName, outcome, payload) =>
        definition.nodes
          .get(phase)
          .filter(_.step.name == stepName)
          .toRight(SagaError.InvalidTransition)
          .flatMap(node => outcomeMove(definition, instance, node, outcome, payload))
    }
  }

  private def outcomeMove[I, P, E, C, F, R, X](
      definition: SagaDefinition[I, P, E, C, F, R, X],
      instance: I,
      node: PhaseNode[I, P, C, R, E, X],
      outcome: StepOutcome[F],
      payload: Option[E]
  ): Either[SagaError[X], SagaMove[I, P, C, R]] = {
    val step = node.step
    def confirmed: Either[SagaError[X], SagaMove[I, P, C, R]] =
      node.onConfirmed.toRight(SagaError.InvalidTransition).flatMap(route => route(instance, payload))
    def whenLookup(allowed: Boolean)(move: => SagaMove[I, P, C, R]): Either[SagaError[X], SagaMove[I, P, C, R]] =
      Either.cond(allowed, move, SagaError.InvalidTransition)
    outcome match {
      case StepOutcome.Confirmed   => confirmed
      case StepOutcome.LookupFound =>
        if (node.lookup == LookupSupport.Full) confirmed else Left(SagaError.InvalidTransition)
      case StepOutcome.OutcomeUnknown =>
        whenLookup(node.lookup != LookupSupport.Unsupported)(SagaMove.advance(step.phase, step.lookup(instance)))
      case StepOutcome.LookupAbsent =>
        whenLookup(node.lookup == LookupSupport.Full)(SagaMove.advance(step.phase, step.start(instance)))
      case StepOutcome.RetryExhausted(failure) =>
        val reason =
          if (definition.isCompensationPhase(step.phase)) definition.repair.compensationFailed(failure)
          else definition.repair.exhausted(failure)
        Right(SagaMove.Repair(reason))
      case StepOutcome.Rejected =>
        node.onRejected match {
          case Rejection.Unsupported() => Left(SagaError.InvalidTransition)
          case Rejection.ToRepair(why) => Right(SagaMove.Repair(why))
          case Rejection.Custom(route) => route(instance, payload)
          case Rejection.Compensate()  =>
            step.undo match {
              case StepUndo.Compensate(target) => Right(SagaMove.advance(target.phase, target.start(instance)))
              case StepUndo.Irreversible(_)    => Left(SagaError.InvalidTransition)
            }
        }
    }
  }

  /** Admin repair: resume the recorded origin by lookup, never by repeating the effect. */
  private def resume[I, P, E, C, F, R, X](
      definition: SagaDefinition[I, P, E, C, F, R, X],
      instance: I,
      phase: P
  ): Either[SagaError[X], SagaMove[I, P, C, R]] =
    for {
      _ <- Either.cond(phase == definition.repairPhase, (), SagaError.InvalidTransition)
      origin <- definition.access.repairOrigin(instance).toRight(SagaError.InvalidTransition)
      node <- definition.nodes.get(origin).toRight(SagaError.InvalidTransition)
    } yield SagaMove.advance(origin, node.step.lookup(instance))

  private def applyMove[I, P, E, C, F, R, X](
      definition: SagaDefinition[I, P, E, C, F, R, X],
      instance: I,
      move: SagaMove[I, P, C, R]
  ): SagaDecision[I, C] = {
    val access = definition.access
    val revision = access.revision(instance) + 1L // SagaRevision.matches excludes Long.MaxValue
    move match {
      case SagaMove.Unchanged()                      => SagaDecision(instance, Nil)
      case SagaMove.Advance(phase, commands, update) =>
        val moved = access.withRevisionAndPhase(update(instance), revision, phase)
        // Any advance out of the repair phase (a resume, or a trigger the definition allows) closes the repair.
        val leavesRepair = access.phase(instance) == definition.repairPhase && phase != definition.repairPhase
        SagaDecision(if (leavesRepair) access.withRepairOrigin(moved, None) else moved, commands)
      case SagaMove.Repair(reason) =>
        val moved = access.withRevisionAndPhase(instance, revision, definition.repairPhase)
        val recorded =
          if (definition.repair.recordsOrigin) access.withRepairOrigin(moved, Some(access.phase(instance))) else moved
        SagaDecision(recorded, List(definition.repair.requireRepair(reason)))
    }
  }
}
