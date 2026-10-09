package com.example.graphQL.cats.repository.mongo

import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.RepositoryError
import org.bson.Document
import com.example.graphQL.cats.repository.mongo.MongoDocumentFields.Repository.{instant, optionalInstant}
import java.time.Instant
import java.util.{Date, UUID}

/** The lifecycle fields of a stored interview workflow: reservation generation, replacement hold, cancellation time,
  * the single open proposal and the advisory reschedule request. Absent optional fields decode to `None` and an absent
  * generation to 0; this is the single active shape, not a compatibility layer.
  */
private[mongo] object MongoInterviewWorkflowLifecycleCodec {
  val GenerationField = "generation"
  val PendingStartField = "pendingStartsAt"
  val PendingEndField = "pendingEndsAt"
  val CancelledAtField = "cancelledAt"
  val ProposalField = "proposal"
  val RescheduleRequestedAtField = "rescheduleRequestedAt"
  val RepairOriginField = "repairOrigin"
  val SkippedGenerationsField = "skippedGenerations"
  private val StartField = "startsAt"
  private val EndField = "endsAt"
  private val ProposedByField = "proposedBy"
  private val ExpiresAtField = "expiresAt"

  /** Fields written when a workflow document is first inserted; only present values are included. */
  def insertFields(workflow: InterviewWorkflow): List[(String, AnyRef)] =
    List(GenerationField -> Int.box(workflow.generation)) ++
      workflow.pendingInterval.toList.flatMap(interval =>
        List(PendingStartField -> interval.startsAt.toDate, PendingEndField -> interval.endsAt.toDate)
      ) ++
      workflow.cancelledAt.map(value => CancelledAtField -> value.toDate) ++
      workflow.proposal.map(value => ProposalField -> proposalDocument(value)) ++
      workflow.rescheduleRequestedAt.map(value => RescheduleRequestedAtField -> value.toDate) ++
      workflow.repairOrigin.map(value => RepairOriginField -> value.toString) ++
      Option.when(workflow.skippedGenerations != 0)(SkippedGenerationsField -> Int.box(workflow.skippedGenerations))

  /** Total update of every mutable lifecycle field: absent optional values are unset so a transition never leaks one.
    */
  def stateUpdate(workflow: InterviewWorkflow): MongoUpdate = {
    def optional(field: String, value: Option[AnyRef]): MongoUpdate =
      value.fold(MongoUpdate.unset(field))(MongoUpdate.set(field, _))
    MongoUpdate.combine(
      MongoUpdate.set(StartField, workflow.interval.startsAt.toDate),
      MongoUpdate.set(EndField, workflow.interval.endsAt.toDate),
      MongoUpdate.set(GenerationField, Int.box(workflow.generation)),
      optional(PendingStartField, workflow.pendingInterval.map(value => value.startsAt.toDate)),
      optional(PendingEndField, workflow.pendingInterval.map(value => value.endsAt.toDate)),
      optional(CancelledAtField, workflow.cancelledAt.map(Date.from)),
      optional(ProposalField, workflow.proposal.map(proposalDocument)),
      optional(RescheduleRequestedAtField, workflow.rescheduleRequestedAt.map(Date.from)),
      optional(RepairOriginField, workflow.repairOrigin.map(_.toString)),
      MongoUpdate.set(SkippedGenerationsField, Int.box(workflow.skippedGenerations))
    )
  }

  final case class LifecycleFields(
      generation: Int,
      pendingInterval: Option[InterviewInterval],
      cancelledAt: Option[Instant],
      proposal: Option[InterviewRescheduleProposal],
      rescheduleRequestedAt: Option[Instant],
      repairOrigin: Option[InterviewWorkflowPhase] = None,
      skippedGenerations: Int = 0
  )

  /** Decodes and checks the fields against the workflow phase; an inconsistent combination is invalid stored data. */
  def decode(document: Document, phase: InterviewWorkflowPhase): Either[RepositoryError, LifecycleFields] = {
    import InterviewWorkflowPhase as Phase
    for {
      generation <- Option(document.get(GenerationField)) match {
        case None                                                  => Right(0)
        case Some(value: java.lang.Integer) if value.intValue >= 0 => Right(value.intValue)
        case _                                                     => Left(RepositoryError.InvalidStoredData)
      }
      pending <- optionalInterval(document, PendingStartField, PendingEndField)
      cancelledAt <- optionalInstant(document, CancelledAtField)
      requested <- optionalInstant(document, RescheduleRequestedAtField)
      proposal <- Option(document.get(ProposalField)) match {
        case None                  => Right(None)
        case Some(value: Document) => decodeProposal(value).map(Some(_))
        case _                     => Left(RepositoryError.InvalidStoredData)
      }
      origin <- Option(document.get(RepairOriginField)) match {
        case None               => Right(None)
        case Some(name: String) =>
          InterviewWorkflowPhase.values
            .find(value => value.toString == name && repairable(value))
            .map(Some(_))
            .toRight(RepositoryError.InvalidStoredData)
        case _ => Left(RepositoryError.InvalidStoredData)
      }
      _ <- Either.cond(origin.isEmpty || phase == Phase.RepairRequired, (), RepositoryError.InvalidStoredData)
      skipped <- Option(document.get(SkippedGenerationsField)) match {
        case None                                                  => Right(0)
        case Some(value: java.lang.Integer) if value.intValue >= 0 => Right(value.intValue)
        case _                                                     => Left(RepositoryError.InvalidStoredData)
      }
      holding = Set(Phase.RescheduleHoldPending, Phase.RescheduleSwapPending, Phase.RescheduleCompensationPending)
      cancelling = Set(Phase.CancelNotificationsPending, Phase.Cancelled)
      _ <- Either.cond(proposal.nonEmpty == (phase == Phase.ProposalPending), (), RepositoryError.InvalidStoredData)
      _ <- Either.cond(
        pending.nonEmpty || !holding(phase),
        (),
        RepositoryError.InvalidStoredData
      )
      _ <- Either.cond(
        pending.isEmpty || holding(phase) || phase == Phase.RepairRequired,
        (),
        RepositoryError.InvalidStoredData
      )
      _ <- Either.cond(
        cancelledAt.nonEmpty || !cancelling(phase),
        (),
        RepositoryError.InvalidStoredData
      )
      _ <- Either.cond(
        cancelledAt.isEmpty || cancelling(phase) || phase == Phase.RepairRequired,
        (),
        RepositoryError.InvalidStoredData
      )
      _ <- Either.cond(requested.isEmpty || phase == Phase.Completed, (), RepositoryError.InvalidStoredData)
      // The swap made the replacement generation live: one past the retired one plus every compensated attempt.
      _ <- Either.cond(
        phase != Phase.RescheduleCancelOldPending || generation >= 1 + skipped,
        (),
        RepositoryError.InvalidStoredData
      )
    } yield LifecycleFields(generation, pending, cancelledAt, proposal, requested, origin, skipped)
  }

  /** The phases whose retry exhaustion Admin repair can resume by lookup. */
  private def repairable(phase: InterviewWorkflowPhase): Boolean = {
    import InterviewWorkflowPhase as Phase
    Set(
      Phase.CancelPending,
      Phase.CancelNotificationsPending,
      Phase.RescheduleHoldPending,
      Phase.RescheduleSwapPending,
      Phase.RescheduleCancelOldPending,
      Phase.RescheduleCompensationPending,
      Phase.RescheduleNotificationsPending
    )(phase)
  }

  private def proposalDocument(value: InterviewRescheduleProposal): Document =
    new Document(StartField, value.interval.startsAt.toDate)
      .append(EndField, value.interval.endsAt.toDate)
      .append(ProposedByField, value.proposedBy.value.toString)
      .append(ExpiresAtField, value.expiresAt.toDate)

  private def decodeProposal(document: Document): Either[RepositoryError, InterviewRescheduleProposal] =
    for {
      starts <- instant(document, StartField)
      ends <- instant(document, EndField)
      _ <- Either.cond(ends.isAfter(starts), (), RepositoryError.InvalidStoredData)
      proposer <- Option(document.get(ProposedByField))
        .collect { case value: String => value }
        .flatMap(value => Either.catchNonFatal(UUID.fromString(value)).toOption)
        .map(UserId.apply)
        .toRight(RepositoryError.InvalidStoredData)
      expiresAt <- instant(document, ExpiresAtField)
    } yield InterviewRescheduleProposal(InterviewInterval(starts, ends), proposer, expiresAt)

  private def optionalInterval(
      document: Document,
      startField: String,
      endField: String
  ): Either[RepositoryError, Option[InterviewInterval]] =
    (optionalInstant(document, startField), optionalInstant(document, endField)).tupled.flatMap {
      case (None, None)               => Right(None)
      case (Some(starts), Some(ends)) =>
        Either.cond(ends.isAfter(starts), Some(InterviewInterval(starts, ends)), RepositoryError.InvalidStoredData)
      case _ => Left(RepositoryError.InvalidStoredData)
    }
}
