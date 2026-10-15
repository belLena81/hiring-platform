package com.example.hiring.saga

import com.example.hiring.saga.BookingSagaFixture.*

/** Compile-time enum case names, so a new generic outcome cannot be left out of the grid. */
object SagaTestLabels {
  import scala.compiletime.constValueTuple
  import scala.deriving.Mirror

  inline def labels[E](using m: Mirror.SumOf[E]): Vector[String] =
    constValueTuple[m.MirroredElemLabels].toList.map(_.toString).toVector
}

/** SAG-02: the generic engine rules, exercised on a small booking definition that is not a hiring workflow. */
class SagaEngineSpec extends munit.FunSuite {
  import Phase.*

  private type Input = SagaInput[Event, String]

  /** The step that runs in each fixture phase (phases without a node get a name that matches nothing). */
  private def stepNameIn(phase: Phase): String = standard.nodes.get(phase).fold("none")(_.step.name)

  private def outcome(phase: Phase, o: StepOutcome[String], payload: Option[Event] = None): Input =
    SagaInput.Outcome(stepNameIn(phase), o, payload)

  private def decide(b: Booking, input: Input, expected: Option[Long] = None, d: Def = standard) =
    SagaEngine.decide(d, b, expected.getOrElse(b.revision), input)

  private def at(b: Booking, o: StepOutcome[String], payload: Option[Event] = None) =
    decide(b, outcome(b.phase, o, payload))

  private val reserved = booking(Reserve)

  test("SAG-02.1 a stale revision is StaleRevision and Long.MaxValue is stale even when equal") {
    val confirmed = outcome(Reserve, StepOutcome.Confirmed)
    assertEquals(decide(reserved, confirmed, Some(4L)), Left(SagaError.StaleRevision))
    assertEquals(decide(reserved, confirmed, Some(6L)), Left(SagaError.StaleRevision))
    assertEquals(decide(booking(Reserve, revision = Long.MaxValue), confirmed), Left(SagaError.StaleRevision))
    assert(
      SagaRevision.matches(5L, 5L) && !SagaRevision.matches(5L, 4L) && !SagaRevision.matches(
        Long.MaxValue,
        Long.MaxValue
      )
    )
  }

  test("SAG-02.1 the revision is checked before the input is interpreted") {
    assertEquals(
      decide(booking(Done), outcome(Done, StepOutcome.Confirmed), Some(1L)),
      Left(SagaError.StaleRevision)
    )
  }

  test("SAG-02.1 resume at Long.MaxValue is stale") {
    val repaired = booking(Repair, revision = Long.MaxValue, origin = Some(Commit))
    assertEquals(decide(repaired, SagaInput.Resume), Left(SagaError.StaleRevision))
  }

  test("SAG-02.2 a successful decision bumps the revision by exactly one, also at Long.MaxValue - 1") {
    assertEquals(at(reserved, StepOutcome.Confirmed).toOption.get.instance.revision, 6L)
    val last = booking(Reserve, revision = Long.MaxValue - 1L)
    assertEquals(at(last, StepOutcome.Confirmed).toOption.get.instance.revision, Long.MaxValue)
  }

  test("SAG-02.2 confirmation advances the phase and emits the next step's command") {
    val ok = at(reserved, StepOutcome.Confirmed).toOption.get
    assertEquals(ok.instance.phase, Commit)
    assertEquals(ok.commands, List(Command.Commit("b1:commit")))
  }

  test("SAG-02.2 an outcome for another phase's step is InvalidTransition") {
    val commitReply: Input = SagaInput.Outcome("commit", StepOutcome.Confirmed, None)
    assertEquals(decide(reserved, commitReply), Left(SagaError.InvalidTransition))
    for (o <- allOutcomes)
      assertEquals(
        decide(booking(Commit), SagaInput.Outcome("reserve", o, None)),
        Left(SagaError.InvalidTransition),
        o.toString
      )
    assertEquals(
      decide(reserved, SagaInput.Outcome("", StepOutcome.Confirmed, None)),
      Left(SagaError.InvalidTransition)
    )
  }

  test("SAG-02.2 a confirmation payload reaches the route, and a route may refuse it with a definition error") {
    val committed = booking(Commit, payload = 10)
    val ok = at(committed, StepOutcome.Confirmed, Some(Event.Poke(5))).toOption.get
    assertEquals((ok.instance.phase, ok.instance.payload), (Wait, 15))
    assertEquals(at(committed, StepOutcome.Confirmed).toOption.get.instance.payload, 11)
    assertEquals(
      at(committed, StepOutcome.Confirmed, Some(Event.Poke(-1))),
      Left(SagaError.Definition("negative payload"))
    )
    assertEquals(
      at(committed, StepOutcome.LookupFound, Some(Event.Poke(-1))),
      Left(SagaError.Definition("negative payload"))
    )
  }

  test("SAG-02.3 unknown outcome emits the lookup for the same key and stays in the phase") {
    val ok = at(reserved, StepOutcome.OutcomeUnknown).toOption.get
    assertEquals(ok.instance.phase, Reserve)
    assertEquals(ok.commands, List(Command.LookupReserve("b1:reserve")))
  }

  test("SAG-02.3 a step with unknown-only lookup re-issues its own command and accepts no other lookup outcome") {
    val inRelease = booking(Release)
    assertEquals(at(inRelease, StepOutcome.OutcomeUnknown).toOption.get.commands, List(Command.Release("b1:release")))
    assertEquals(at(inRelease, StepOutcome.LookupAbsent), Left(SagaError.InvalidTransition))
    assertEquals(at(inRelease, StepOutcome.LookupFound), Left(SagaError.InvalidTransition))
  }

  test("SAG-02.4 lookup absent re-issues the original command with the same key") {
    val ok = at(reserved, StepOutcome.LookupAbsent).toOption.get
    assertEquals(ok.instance.phase, Reserve)
    assertEquals(ok.commands, List(Command.Reserve("b1:reserve")))
  }

  test("SAG-02.4 lookup found is a confirmation") {
    assertEquals(at(reserved, StepOutcome.LookupFound), at(reserved, StepOutcome.Confirmed))
  }

  test("SAG-02.5 a rejected step with a compensation runs the declared compensation under its own key") {
    val ok = at(booking(Commit), StepOutcome.Rejected).toOption.get
    assertEquals(ok.instance.phase, Release)
    assertEquals(ok.commands, List(Command.Release("b1:release")))
    assertEquals(standard.status(Release), SagaStatus.Compensating)
  }

  test("SAG-02.5 a rejected step with nothing to compensate goes to repair without a compensation") {
    val ok = at(reserved, StepOutcome.Rejected).toOption.get
    assertEquals(ok.instance.phase, Repair)
    assertEquals(ok.commands, List(Command.RequireRepair(Reason.Rejected)))
  }

  test("SAG-02.5 a rejection in a compensation node follows that node's route, and is invalid when unsupported") {
    assertEquals(at(booking(Release), StepOutcome.Rejected), Left(SagaError.InvalidTransition))
    val repairing =
      definition(nodes.updated(Release, nodes(Release).copy(onRejected = Rejection.ToRepair(Reason.Rejected))))
    val ok = decide(booking(Release), outcome(Release, StepOutcome.Rejected), d = repairing).toOption.get
    assertEquals((ok.instance.phase, ok.commands), (Repair, List(Command.RequireRepair(Reason.Rejected))))
  }

  test("SAG-02.5 a custom rejection route may fail or move") {
    val custom = definition(
      nodes.updated(
        Reserve,
        nodes(Reserve).copy(onRejected =
          Rejection.Custom((b, _) =>
            if (b.payload > 0) Right(SagaMove.advance(Wait, Nil)) else Left(SagaError.Definition("no payload"))
          )
        )
      )
    )
    val reject = outcome(Reserve, StepOutcome.Rejected)
    assertEquals(decide(reserved, reject, d = custom), Left(SagaError.Definition("no payload")))
    assertEquals(decide(booking(Reserve, payload = 1), reject, d = custom).toOption.get.instance.phase, Wait)
  }

  test("SAG-02.5 retry exhaustion goes to repair for compensable and irreversible steps alike") {
    for (phase <- List(Reserve, Commit)) {
      val ok = at(booking(phase), StepOutcome.RetryExhausted("provider")).toOption.get
      assertEquals(ok.instance.phase, Repair, phase.toString)
      assertEquals(ok.commands, List(Command.RequireRepair(Reason.Exhausted("provider"))), phase.toString)
    }
  }

  test("SAG-02.6 exhaustion of a compensation goes to repair with the compensation-failed reason") {
    val ok = at(booking(Release), StepOutcome.RetryExhausted("provider")).toOption.get
    assertEquals(ok.instance.phase, Repair)
    assertEquals(ok.commands, List(Command.RequireRepair(Reason.CompensationFailed("provider"))))
  }

  test("SAG-02.6 a completed compensation ends in repair, not in a retry loop") {
    val ok = at(booking(Release), StepOutcome.Confirmed).toOption.get
    assertEquals((ok.instance.phase, ok.commands), (Repair, List(Command.RequireRepair(Reason.Compensated))))
  }

  test("SAG-02.7 repair records its origin and an Admin resume looks the origin up and clears it") {
    val repaired = at(booking(Commit), StepOutcome.RetryExhausted("provider")).toOption.get.instance
    assertEquals(repaired.origin, Some(Commit))
    val resumed = decide(repaired, SagaInput.Resume).toOption.get
    assertEquals(resumed.instance.phase, Commit)
    assertEquals(resumed.instance.origin, None)
    assertEquals(resumed.instance.revision, repaired.revision + 1L)
    assertEquals(resumed.commands, List(Command.LookupCommit("b1:commit")))
  }

  test("SAG-02.7 resume of a compensation origin looks the compensation up and reopens nothing else") {
    val repaired = booking(Repair, origin = Some(Release))
    val resumed = decide(repaired, SagaInput.Resume).toOption.get
    assertEquals((resumed.instance.phase, resumed.instance.origin), (Release, None))
    assertEquals(resumed.commands, List(Command.Release("b1:release")))
  }

  test("SAG-02.7 resume never repeats the effect and refuses a workflow that is not in repair or has no origin") {
    val resumed = decide(booking(Repair, origin = Some(Reserve)), SagaInput.Resume).toOption.get
    assertEquals(resumed.commands, List(Command.LookupReserve("b1:reserve")))
    assertEquals(decide(booking(Repair), SagaInput.Resume), Left(SagaError.InvalidTransition))
    assertEquals(decide(booking(Commit), SagaInput.Resume), Left(SagaError.InvalidTransition))
    assertEquals(decide(booking(Repair, origin = Some(Done)), SagaInput.Resume), Left(SagaError.InvalidTransition))
  }

  test("SAG-02.7 a definition that records no origin leaves it empty") {
    val ok = decide(reserved, outcome(Reserve, StepOutcome.RetryExhausted("x")), d = definition(recordsOrigin = false))
    assertEquals(ok.toOption.get.instance.origin, None)
  }

  test("SAG-02.8 a pair outside the graph is InvalidTransition") {
    for (phase <- List(Wait, Done, Repair); o <- allOutcomes)
      assertEquals(at(booking(phase), o), Left(SagaError.InvalidTransition), s"$phase $o")
  }

  test("SAG-02.8 triggers are interpreted by the definition under the same revision rule") {
    val waiting = booking(Wait)
    val ok = decide(waiting, SagaInput.Trigger(Event.Poke(7))).toOption.get
    assertEquals((ok.instance.phase, ok.instance.payload, ok.instance.revision), (Done, 7, 6L))
    assertEquals(decide(waiting, SagaInput.Trigger(Event.Poke(-1))), Left(SagaError.Definition("negative")))
    assertEquals(decide(waiting, SagaInput.Trigger(Event.Poke(1)), Some(4L)), Left(SagaError.StaleRevision))
    assertEquals(decide(reserved, SagaInput.Trigger(Event.Ignore)), Left(SagaError.InvalidTransition))
  }

  test("SAG-02.8 an idempotent trigger leaves the instance and the revision unchanged") {
    val waiting = booking(Wait)
    assertEquals(decide(waiting, SagaInput.Trigger(Event.Ignore)), Right(SagaDecision(waiting, Nil)))
  }

  test("SAG-02.8 a trigger that leaves the repair phase closes the repair and clears its origin") {
    val repaired = booking(Repair, origin = Some(Commit))
    val ok = decide(repaired, SagaInput.Trigger(Event.Reopen)).toOption.get
    assertEquals((ok.instance.phase, ok.instance.origin, ok.instance.revision), (Wait, None, 6L))
  }

  test("SAG-02.9 every success bumps the revision exactly once and every failure is typed, over the whole grid") {
    for { b <- allInstances; input <- allInputs; expected <- expectedRevisions(b) } {
      val clue = s"${b.phase} $input expected=$expected"
      decide(b, input, Some(expected)) match {
        case Right(decision) =>
          assertEquals(expected, b.revision, clue)
          assert(
            decision.instance == b && decision.commands.isEmpty || decision.instance.revision == b.revision + 1L,
            clue
          )
        case Left(error) =>
          if (expected != b.revision || b.revision == Long.MaxValue) assertEquals(error, SagaError.StaleRevision, clue)
      }
    }
  }

  test("SAG-02.9 a rejected input leaves the instance unchanged and a retry gives the same answer") {
    for { b <- allInstances; input <- allInputs } {
      val before = b.copy()
      val first = decide(b, input)
      if (first.isLeft) {
        assertEquals(b, before, s"${b.phase} $input")
        assertEquals(decide(b, input), first, s"${b.phase} $input")
      }
    }
  }

  test("SAG-02.9 replaying a decision from the instance it produced is stale, never applied twice") {
    for { b <- allInstances; input <- allInputs; decision <- decide(b, input).toOption if decision.instance != b } {
      assertEquals(decide(decision.instance, input, Some(b.revision)), Left(SagaError.StaleRevision))
    }
  }

  test("SAG-02.9 the grid covers every generic outcome case") {
    val kinds = allOutcomes.map(_.ordinal).toSet
    assertEquals(kinds, SagaTestLabels.labels[StepOutcome[String]].indices.toSet)
    assertEquals(allInstances.map(_.phase).toSet, Phase.values.toSet)
  }

  test("SAG-02 forward-only phases accept only the immediate next phase") {
    val phases = ForwardOnlyPhases(Vector("Requested", "Fenced", "Purged", "Reported"))
    assertEquals(phases.advance("Requested", "Fenced"), Right("Fenced"))
    assertEquals(phases.advance("Requested", "Purged"), Left(SagaError.InvalidTransition))
    assertEquals(phases.advance("Purged", "Fenced"), Left(SagaError.InvalidTransition))
    assertEquals(phases.advance("Reported", "Reported"), Left(SagaError.InvalidTransition))
    assertEquals(phases.advance("Unknown", "Fenced"), Left(SagaError.InvalidTransition))
    assertEquals(phases.next("Reported"), None)
  }

  private val allOutcomes: List[StepOutcome[String]] =
    List(
      StepOutcome.Confirmed,
      StepOutcome.Rejected,
      StepOutcome.OutcomeUnknown,
      StepOutcome.LookupFound,
      StepOutcome.LookupAbsent,
      StepOutcome.RetryExhausted("provider")
    )

  private def allInstances: List[Booking] =
    Phase.values.toList.flatMap(phase =>
      List(booking(phase), booking(phase, origin = Some(Commit)), booking(phase, revision = Long.MaxValue - 1L))
    ) :+ booking(Reserve, revision = Long.MaxValue)

  private def allInputs: List[Input] =
    (for { name <- List("reserve", "commit", "release", "other"); o <- allOutcomes } yield SagaInput.Outcome(
      name,
      o,
      None
    )) ++
      List(
        SagaInput.Resume,
        SagaInput.Trigger(Event.Poke(3)),
        SagaInput.Trigger(Event.Ignore),
        SagaInput.Trigger(Event.Reopen)
      )

  private def expectedRevisions(b: Booking): List[Long] = List(b.revision, b.revision - 1L, Long.MaxValue)
}
