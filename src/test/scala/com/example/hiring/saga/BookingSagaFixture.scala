package com.example.hiring.saga

/** A small booking workflow used only to exercise the engine: reserve (irreversible), commit (compensated by release),
  * a trigger-driven wait, and repair. It is not a hiring definition.
  */
object BookingSagaFixture {
  enum Phase {
    case Reserve, Commit, Release, Wait, Done, Repair
  }

  enum Reason {
    case Rejected, Compensated
    case Exhausted(failure: String)
    case CompensationFailed(failure: String)
  }

  enum Command {
    case Reserve(key: String)
    case LookupReserve(key: String)
    case Commit(key: String)
    case LookupCommit(key: String)
    case Release(key: String)
    case RequireRepair(reason: Reason)
  }

  enum Event {
    case Poke(value: Int)
    case Ignore
    case Reopen
  }

  final case class Booking(id: String, revision: Long, phase: Phase, origin: Option[Phase], payload: Int)

  type Def = SagaDefinition[Booking, Phase, Event, Command, String, Reason, String]
  type Step = SagaStep[Booking, Phase, Command]
  type Node = PhaseNode[Booking, Phase, Command, Reason, Event, String]
  type Move = SagaMove[Booking, Phase, Command, Reason]

  def booking(phase: Phase, revision: Long = 5L, origin: Option[Phase] = None, payload: Int = 0): Booking =
    Booking("b1", revision, phase, origin, payload)

  object access extends SagaInstanceAccess[Booking, Phase] {
    def revision(instance: Booking): Long = instance.revision
    def phase(instance: Booking): Phase = instance.phase
    def repairOrigin(instance: Booking): Option[Phase] = instance.origin
    def withRevisionAndPhase(instance: Booking, revision: Long, phase: Phase): Booking =
      instance.copy(revision = revision, phase = phase)
    def withRepairOrigin(instance: Booking, origin: Option[Phase]): Booking = instance.copy(origin = origin)
  }

  val release: Step = SagaStep(
    "release",
    Phase.Release,
    b => s"${b.id}:release",
    b => List(Command.Release(s"${b.id}:release")),
    b => List(Command.Release(s"${b.id}:release")),
    StepUndo.Irreversible("a released slot cannot be taken back"),
    RetryBudget("booking.release")
  )

  val reserve: Step = SagaStep(
    "reserve",
    Phase.Reserve,
    b => s"${b.id}:reserve",
    b => List(Command.Reserve(s"${b.id}:reserve")),
    b => List(Command.LookupReserve(s"${b.id}:reserve")),
    StepUndo.Irreversible("nothing was reserved when the reservation failed"),
    RetryBudget("booking.reserve")
  )

  val commit: Step = SagaStep(
    "commit",
    Phase.Commit,
    b => s"${b.id}:commit",
    b => List(Command.Commit(s"${b.id}:commit")),
    b => List(Command.LookupCommit(s"${b.id}:commit")),
    StepUndo.Compensate(release),
    RetryBudget("booking.commit")
  )

  /** Confirmation payload: the commit adds it to the booking payload; a negative one is refused by the route. */
  private def confirmedAmount(payload: Option[Event]): Either[SagaError[String], Int] = payload match {
    case Some(Event.Poke(n)) if n < 0 => Left(SagaError.Definition("negative payload"))
    case Some(Event.Poke(n))          => Right(n)
    case _                            => Right(1)
  }

  val nodes: Map[Phase, Node] = Map(
    Phase.Reserve -> PhaseNode(
      reserve,
      LookupSupport.Full,
      Some((b, _) => Right(SagaMove.advance(Phase.Commit, commit.start(b)))),
      Rejection.ToRepair(Reason.Rejected)
    ),
    Phase.Commit -> PhaseNode(
      commit,
      LookupSupport.Full,
      Some((b, payload) =>
        confirmedAmount(payload).map(amount => SagaMove.Advance(Phase.Wait, Nil, _.copy(payload = b.payload + amount)))
      ),
      Rejection.Compensate()
    ),
    Phase.Release -> PhaseNode(
      release,
      LookupSupport.UnknownOnly,
      Some((_, _) => Right(SagaMove.Repair(Reason.Compensated))),
      Rejection.Unsupported()
    )
  )

  def definition(
      overrideNodes: Map[Phase, Node] = nodes,
      eventDriven: Set[Phase] = Set(Phase.Wait),
      phases: List[Phase] = Phase.values.toList,
      recordsOrigin: Boolean = true
  ): Def =
    SagaDefinition(
      name = "booking",
      access = access,
      phases = phases,
      terminal = Set(Phase.Done),
      eventDriven = eventDriven,
      repairPhase = Phase.Repair,
      nodes = overrideNodes,
      repair = RepairPolicy(recordsOrigin, Command.RequireRepair(_), Reason.Exhausted(_), Reason.CompensationFailed(_)),
      triggers = (b, event) =>
        (b.phase, event) match {
          case (Phase.Wait, Event.Poke(n)) if n < 0 => Left(SagaError.Definition("negative"))
          case (Phase.Wait, Event.Poke(n))          => Right(SagaMove.Advance(Phase.Done, Nil, _.copy(payload = n)))
          case (Phase.Wait, Event.Ignore)           => Right(SagaMove.Unchanged())
          case (Phase.Repair, Event.Reopen)         => Right(SagaMove.advance(Phase.Wait, Nil))
          case _                                    => Left(SagaError.InvalidTransition)
        }
    )

  val standard: Def = definition()
}
