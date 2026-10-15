package com.example.hiring.saga

/** Pure well-formedness rules of a saga definition, evaluated against a sample instance for the keys. */
object SagaDefinitionChecks {

  /** Human-readable problems; an empty list means the definition is well formed. */
  def problems[I, P, E, C, F, R, X](
      definition: SagaDefinition[I, P, E, C, F, R, X],
      sample: I
  ): List[String] = {
    val nodeProblems = definition.nodes.toList.flatMap { case (phase, node) =>
      val step = node.step
      val rejection = node.onRejected match {
        case Rejection.Compensate() =>
          step.undo match {
            case StepUndo.Compensate(_)   => Nil
            case StepUndo.Irreversible(_) => List(s"$phase: irreversible step ${step.name} has a compensation edge")
          }
        case _ => Nil
      }
      val undo = step.undo match {
        case StepUndo.Compensate(target) =>
          val hasNode = Option.when(!definition.nodes.contains(target.phase))(
            s"$phase: compensation ${target.name} has no node in phase ${target.phase}"
          )
          val ownStep = Option.when(definition.nodes.get(target.phase).exists(_.step != target))(
            s"$phase: compensation ${target.name} is not the step of phase ${target.phase}"
          )
          val declared = Option.when(!definition.phases.contains(target.phase))(
            s"$phase: compensation ${target.name} runs in undeclared phase ${target.phase}"
          )
          val key =
            Option.when(target.key(sample) == step.key(sample))(s"$phase: ${target.name} reuses key of ${step.name}")
          val budget = Option.when(target.retryBudget.name.isEmpty)(s"$phase: ${target.name} has no retry budget")
          val cascade = target.undo match {
            case StepUndo.Compensate(_)   => List(s"$phase: compensation ${target.name} must itself be irreversible")
            case StepUndo.Irreversible(_) => Nil
          }
          hasNode.toList ++ ownStep ++ declared ++ key ++ budget ++ cascade
        case StepUndo.Irreversible(reason) =>
          Option.when(reason.trim.isEmpty)(s"$phase: irreversible step ${step.name} has no reason").toList
      }
      val self = List(
        Option.when(step.phase != phase)(s"$phase: step ${step.name} is bound to phase ${step.phase}"),
        Option.when(step.retryBudget.name.isEmpty)(s"$phase: step ${step.name} has no retry budget"),
        Option.when(definition.terminal.contains(phase) || phase == definition.repairPhase)(
          s"$phase: terminal or repair phase has a step"
        ),
        Option.when(node.onConfirmed.isEmpty && node.lookup != LookupSupport.Unsupported)(
          s"$phase: lookup outcomes without a confirmation route"
        )
      ).flatten
      self ++ rejection ++ undo
    }
    val phaseProblems = definition.phases
      .filterNot { phase =>
        definition.terminal.contains(phase) || phase == definition.repairPhase ||
        definition.eventDriven.contains(phase) || definition.nodes.contains(phase)
      }
      .map(phase => s"$phase: non-terminal phase has no step and is not event driven")
    val unknownPhases =
      (definition.nodes.keySet ++ definition.terminal ++ definition.eventDriven + definition.repairPhase)
        .filterNot(definition.phases.contains)
        .toList
        .map(phase => s"$phase: not a declared phase")
    // Steps are equal by name, so the same name must always denote the same phase and key.
    val duplicateNames = definition.allSteps
      .groupBy(_.name)
      .collect {
        case (name, group) if group.map(step => (step.phase, step.key(sample))).distinct.size > 1 =>
          s"step name $name is used by different steps"
      }
      .toList
    val catalogue = definition.steps
    val keys = catalogue.map(_.key(sample))
    val duplicateKeys = Option.when(keys.distinct.size != keys.size)("catalogue steps share an idempotency key").toList
    nodeProblems ++ phaseProblems ++ unknownPhases ++ duplicateNames ++ duplicateKeys
  }
}
