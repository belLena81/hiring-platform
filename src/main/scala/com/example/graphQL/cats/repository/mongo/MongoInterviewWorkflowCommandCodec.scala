package com.example.graphQL.cats.repository.mongo

import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.ApplicationStatus
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.port.*
import org.bson.Document
import com.example.graphQL.cats.repository.mongo.MongoDocumentFields.Repository.{instant, int32, int64}
import java.time.Instant
import java.util.UUID

/** Canonical current stored command contract shared by transactional reads and startup verification. */
private[mongo] object MongoInterviewWorkflowCommandCodec {
  private val WorkflowIdField = "workflowId"
  private val RevisionField = "revision"
  private val StepIdField = "stepId"
  private val CommandField = "command"
  private val CommandStateField = "commandState"
  private val IdempotencyKeyField = "idempotencyKey"
  private val StartField = "startsAt"
  private val EndField = "endsAt"

  /** Stored command kinds introduced for cancellation and rescheduling. */
  object LifecycleKinds {
    val CancelCalendar = "cancelCalendar"
    val LookupCancellation = "lookupCancellation"
    val HoldReplacement = "holdReplacement"
    val LookupReplacementHold = "lookupReplacementHold"
    val CommitReschedule = "commitReschedule"
    val LookupRescheduleCommit = "lookupRescheduleCommit"
    val ExpireProposal = "expireProposal"
    val NotifyKind = "notifyKind"
    val LookupNotificationKind = "lookupNotificationKind"
    val all: Set[String] = Set(
      CancelCalendar,
      LookupCancellation,
      HoldReplacement,
      LookupReplacementHold,
      CommitReschedule,
      LookupRescheduleCommit,
      ExpireProposal,
      NotifyKind,
      LookupNotificationKind
    )
  }

  private final case class Envelope(
      workflowId: InterviewWorkflowId,
      step: String,
      revision: Long,
      command: Document,
      state: InterviewWorkflowCommandState,
      attempts: Int,
      executionAttempts: Int,
      availableAt: Instant,
      occurredAt: Instant,
      result: Option[InterviewCommandResult]
  )

  /** Any stored row: scheduling and lifecycle intents share one record, claim path and executor. */
  def decode(document: Document): Either[RepositoryError, InterviewWorkflowCommandRecord] =
    envelope(document).flatMap { e =>
      string(e.command, "kind").flatMap { kind =>
        val command: Either[RepositoryError, InterviewCommand] =
          if (LifecycleKinds.all(kind)) decodeLifecycleCommand(e.command, e.workflowId, e.availableAt)
          else decodeCommand(e.command, e.workflowId)
        command.map { value =>
          InterviewWorkflowCommandRecord(
            e.workflowId,
            e.step,
            e.revision,
            value,
            e.state,
            e.attempts,
            e.availableAt,
            e.occurredAt,
            e.result,
            e.executionAttempts
          )
        }
      }
    }

  /** Audits and cutovers validate every stored row through the same entry point as the claim path. */
  def decodeStored(document: Document): Either[RepositoryError, InterviewWorkflowCommandRecord] = decode(document)

  private def envelope(document: Document): Either[RepositoryError, Envelope] =
    for {
      workflowId <- string(document, WorkflowIdField).flatMap(uuid).map(InterviewWorkflowId.apply)
      step <- string(document, StepIdField)
      revision <- int64(document, RevisionField)
      id <- string(document, MongoFields.Id)
      _ <- Either.cond(id == s"${workflowId.value}:$step", (), RepositoryError.InvalidStoredData)
      commandDoc <- Option(document.get(CommandField))
        .collect { case value: Document => value }
        .toRight(RepositoryError.InvalidStoredData)
      stateName <- string(document, CommandStateField)
      state <- InterviewWorkflowCommandState.values
        .find(_.toString == stateName)
        .toRight(RepositoryError.InvalidStoredData)
      attempts <- int32(document, MongoFields.Attempts)
      executionAttempts <- int32(document, "executionAttempts")
      _ <- Either.cond(revision >= 0L && attempts >= 0 && executionAttempts >= 0, (), RepositoryError.InvalidStoredData)
      availableAt <- instant(document, MongoFields.AvailableAt)
      occurredAt <- instant(document, MongoFields.OccurredAt)
      result <- Option(document.get("result")) match {
        case None               => Right(None)
        case Some(name: String) =>
          InterviewCommandResult.values.find(_.toString == name).map(Some(_)).toRight(RepositoryError.InvalidStoredData)
        case _ => Left(RepositoryError.InvalidStoredData)
      }
      _ <- Either.cond(
        !Set(InterviewWorkflowCommandState.ResultPending, InterviewWorkflowCommandState.ResultPublished)(
          state
        ) || result.nonEmpty,
        (),
        RepositoryError.InvalidStoredData
      )
      _ <-
        if (Set(InterviewWorkflowCommandState.Claimed, InterviewWorkflowCommandState.Executing)(state)) for {
          _ <- string(document, "claimOwner")
          _ <- string(document, "claimToken").flatMap(uuid)
          _ <- instant(document, "claimUntil")
        } yield ()
        else Right(())
    } yield Envelope(
      workflowId,
      step,
      revision,
      commandDoc,
      state,
      attempts,
      executionAttempts,
      availableAt,
      occurredAt,
      result
    )

  def encodeLifecycle(command: InterviewLifecycleCommand): Document = {
    import InterviewLifecycleCommand as C
    import LifecycleKinds as K
    def interval(value: InterviewInterval): Document =
      new Document(StartField, value.startsAt.toDate).append(EndField, value.endsAt.toDate)
    command match {
      case C.CancelCalendarSlot(key)       => new Document("kind", K.CancelCalendar).append(IdempotencyKeyField, key)
      case C.LookupCalendarCancellation(k) => new Document("kind", K.LookupCancellation).append(IdempotencyKeyField, k)
      case C.HoldReplacementSlot(key, in)  =>
        new Document("kind", K.HoldReplacement).append(IdempotencyKeyField, key).append("interval", interval(in))
      case C.LookupReplacementHold(key) =>
        new Document("kind", K.LookupReplacementHold).append(IdempotencyKeyField, key)
      case C.CommitRescheduledInterval(in, generation) =>
        new Document("kind", K.CommitReschedule).append("interval", interval(in)).append("generation", generation)
      case C.LookupRescheduleCommitReceipt(id, generation) =>
        new Document("kind", K.LookupRescheduleCommit)
          .append(WorkflowIdField, id.value.toString)
          .append("generation", generation)
      case C.ExpireProposal(at) => new Document("kind", K.ExpireProposal).append(MongoFields.AvailableAt, at.toDate)
      case C.Notify(kind, participant, key) =>
        new Document("kind", K.NotifyKind)
          .append("notificationKind", kind.toString)
          .append("participant", participant.toString)
          .append(IdempotencyKeyField, key)
      case C.LookupNotificationReceipt(kind, participant, key) =>
        new Document("kind", K.LookupNotificationKind)
          .append("notificationKind", kind.toString)
          .append("participant", participant.toString)
          .append(IdempotencyKeyField, key)
      case C.RequireRepair(reason) => new Document("kind", "requireRepair").append("reason", reason)
    }
  }

  private def decodeLifecycleCommand(
      document: Document,
      workflowId: InterviewWorkflowId,
      availableAt: Instant
  ): Either[RepositoryError, InterviewLifecycleCommand] = {
    import InterviewLifecycleCommand as C
    import LifecycleKinds as K
    def keyed(valid: String => Boolean): Either[RepositoryError, String] =
      string(document, IdempotencyKeyField).flatMap(key =>
        Either.cond(valid(key), key, RepositoryError.InvalidStoredData)
      )
    def cancelKey(key: String) = InterviewCalendarKeys.cancellationGeneration(workflowId, key).isDefined
    def holdKey(key: String) = InterviewCalendarKeys.reservationGeneration(workflowId, key).exists(_ > 0)
    def interval(field: String): Either[RepositoryError, InterviewInterval] =
      Option(document.get(field))
        .collect { case value: Document => value }
        .toRight(RepositoryError.InvalidStoredData)
        .flatMap { value =>
          for {
            starts <- instant(value, StartField)
            ends <- instant(value, EndField)
            _ <- Either.cond(ends.isAfter(starts), (), RepositoryError.InvalidStoredData)
          } yield InterviewInterval(starts, ends)
        }
    def participant: Either[RepositoryError, InterviewParticipant] =
      string(document, "participant").flatMap(name =>
        InterviewParticipant.values.find(_.toString == name).toRight(RepositoryError.InvalidStoredData)
      )
    def kindAndKey: Either[RepositoryError, (InterviewNotificationKind, InterviewParticipant, String)] =
      for {
        kindName <- string(document, "notificationKind")
        kind <- InterviewNotificationKind.values.find(_.toString == kindName).toRight(RepositoryError.InvalidStoredData)
        who <- participant
        key <- keyed(value =>
          value.startsWith(s"${workflowId.value}:${kind.keyName}:") && value.endsWith(s":notify:$who")
        )
      } yield (kind, who, key)
    string(document, "kind").flatMap {
      case K.CancelCalendar        => keyed(cancelKey).map(C.CancelCalendarSlot.apply)
      case K.LookupCancellation    => keyed(cancelKey).map(C.LookupCalendarCancellation.apply)
      case K.HoldReplacement       => (keyed(holdKey), interval("interval")).mapN(C.HoldReplacementSlot.apply)
      case K.LookupReplacementHold => keyed(holdKey).map(C.LookupReplacementHold.apply)
      case K.CommitReschedule      =>
        (interval("interval"), int32(document, "generation").filterOrElse(_ > 0, RepositoryError.InvalidStoredData))
          .mapN(C.CommitRescheduledInterval.apply)
      case K.LookupRescheduleCommit =>
        (
          string(document, WorkflowIdField)
            .flatMap(uuid)
            .map(InterviewWorkflowId.apply)
            .filterOrElse(_ == workflowId, RepositoryError.InvalidStoredData),
          int32(document, "generation").filterOrElse(_ > 0, RepositoryError.InvalidStoredData)
        ).mapN(C.LookupRescheduleCommitReceipt.apply)
      case K.ExpireProposal =>
        instant(document, MongoFields.AvailableAt)
          .filterOrElse(_ == availableAt, RepositoryError.InvalidStoredData)
          .map(C.ExpireProposal.apply)
      case K.NotifyKind             => kindAndKey.map((kind, who, key) => C.Notify(kind, who, key))
      case K.LookupNotificationKind => kindAndKey.map((kind, who, key) => C.LookupNotificationReceipt(kind, who, key))
      case "requireRepair"          => string(document, "reason").map(C.RequireRepair.apply)
      case _                        => Left(RepositoryError.InvalidStoredData)
    }
  }

  private def decodeCommand(
      document: Document,
      workflowId: InterviewWorkflowId
  ): Either[RepositoryError, InterviewWorkflowCommand] =
    decodeCommandShape(document).flatMap(command => validateKeys(command, workflowId).as(command))

  private def validateKeys(command: InterviewWorkflowCommand, workflowId: InterviewWorkflowId) = command match {
    case InterviewWorkflowCommand.Notify(participant, key) =>
      Either.cond(key == s"${workflowId.value}:notify:$participant", (), RepositoryError.InvalidStoredData)
    case InterviewWorkflowCommand.LookupNotificationReceipt(participant, key) =>
      Either.cond(key == s"${workflowId.value}:notify:$participant", (), RepositoryError.InvalidStoredData)
    case InterviewWorkflowCommand.ReserveCalendarSlot(key) =>
      Either.cond(key == s"${workflowId.value}:reserve", (), RepositoryError.InvalidStoredData)
    case InterviewWorkflowCommand.ReleaseCalendarSlot(key) =>
      Either.cond(key == s"${workflowId.value}:release", (), RepositoryError.InvalidStoredData)
    case InterviewWorkflowCommand.LookupCalendarReservation(id) =>
      Either.cond(id == workflowId, (), RepositoryError.InvalidStoredData)
    case InterviewWorkflowCommand.LookupStatusCommitReceipt(id) =>
      Either.cond(id == workflowId, (), RepositoryError.InvalidStoredData)
    case InterviewWorkflowCommand.CommitAcceptedToInterview(status) =>
      Either.cond(status == ApplicationStatus.Accepted, (), RepositoryError.InvalidStoredData)
    case InterviewWorkflowCommand.RequireRepair(_) => Right(())
  }

  private def decodeCommandShape(document: Document): Either[RepositoryError, InterviewWorkflowCommand] =
    string(document, "kind").flatMap {
      case "reserveCalendar" =>
        string(document, IdempotencyKeyField).map(InterviewWorkflowCommand.ReserveCalendarSlot.apply)
      case "lookupCalendar" =>
        string(document, WorkflowIdField)
          .flatMap(uuid)
          .map(id => InterviewWorkflowCommand.LookupCalendarReservation(InterviewWorkflowId(id)))
      case "commitInterview" =>
        string(document, "expectedStatus")
          .flatMap(enumStatus)
          .map(InterviewWorkflowCommand.CommitAcceptedToInterview.apply)
      case "lookupStatusCommit" =>
        string(document, WorkflowIdField)
          .flatMap(uuid)
          .map(id => InterviewWorkflowCommand.LookupStatusCommitReceipt(InterviewWorkflowId(id)))
      case "releaseCalendar" =>
        string(document, IdempotencyKeyField).map(InterviewWorkflowCommand.ReleaseCalendarSlot.apply)
      case "notify" =>
        for {
          participantName <- string(document, "participant")
          participant <- InterviewParticipant.values
            .find(_.toString == participantName)
            .toRight(RepositoryError.InvalidStoredData)
          key <- string(document, IdempotencyKeyField)
        } yield InterviewWorkflowCommand.Notify(participant, key)
      case "lookupNotification" =>
        for {
          name <- string(document, "participant")
          participant <- InterviewParticipant.values.find(_.toString == name).toRight(RepositoryError.InvalidStoredData)
          key <- string(document, IdempotencyKeyField)
        } yield InterviewWorkflowCommand.LookupNotificationReceipt(participant, key)
      case "requireRepair" => string(document, "reason").map(InterviewWorkflowCommand.RequireRepair.apply)
      case _               => Left(RepositoryError.InvalidStoredData)
    }

  private def string(document: Document, field: String): Either[RepositoryError, String] =
    Option(document.get(field))
      .collect { case value: String if value.nonEmpty && value.length <= 256 => value }
      .toRight(RepositoryError.InvalidStoredData)
  private def uuid(value: String): Either[RepositoryError, UUID] =
    Either.catchNonFatal(UUID.fromString(value)).leftMap(_ => RepositoryError.InvalidStoredData)
  private def enumStatus(value: String): Either[RepositoryError, ApplicationStatus] =
    ApplicationStatus.values.find(_.toString == value).toRight(RepositoryError.InvalidStoredData)
}
