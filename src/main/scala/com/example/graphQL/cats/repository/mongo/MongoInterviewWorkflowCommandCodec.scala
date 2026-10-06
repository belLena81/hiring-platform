package com.example.graphQL.cats.repository.mongo

import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.ApplicationStatus
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.port.*
import org.bson.Document
import java.time.Instant
import java.util.{Date, UUID}

/** Canonical current stored command contract shared by transactional reads and startup verification. */
private[mongo] object MongoInterviewWorkflowCommandCodec {
  private val WorkflowIdField = "workflowId"
  private val RevisionField = "revision"
  private val StepIdField = "stepId"
  private val CommandField = "command"
  private val CommandStateField = "commandState"
  private val IdempotencyKeyField = "idempotencyKey"
  def decode(document: Document): Either[RepositoryError, InterviewWorkflowCommandRecord] =
    for {
      workflowId <- string(document, WorkflowIdField).flatMap(uuid).map(InterviewWorkflowId.apply)
      step <- string(document, StepIdField)
      revision <- long(document, RevisionField)
      id <- string(document, MongoFields.Id)
      _ <- Either.cond(id == s"${workflowId.value}:$step", (), RepositoryError.InvalidStoredData)
      commandDoc <- Option(document.get(CommandField))
        .collect { case value: Document => value }
        .toRight(RepositoryError.InvalidStoredData)
      command <- decodeCommand(commandDoc)
      _ <- command match {
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
      stateName <- string(document, CommandStateField)
      state <- InterviewWorkflowCommandState.values
        .find(_.toString == stateName)
        .toRight(RepositoryError.InvalidStoredData)
      attempts <- integer(document, MongoFields.Attempts)
      executionAttempts <- integer(document, "executionAttempts")
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
    } yield InterviewWorkflowCommandRecord(
      workflowId,
      step,
      revision,
      command,
      state,
      attempts,
      availableAt,
      occurredAt,
      result,
      executionAttempts
    )

  private def decodeCommand(document: Document): Either[RepositoryError, InterviewWorkflowCommand] =
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
  private def long(document: Document, field: String): Either[RepositoryError, Long] =
    Option(document.get(field))
      .collect { case value: java.lang.Long => value.longValue }
      .toRight(RepositoryError.InvalidStoredData)
  private def integer(document: Document, field: String): Either[RepositoryError, Int] =
    Option(document.get(field))
      .collect { case value: java.lang.Integer => value.intValue }
      .toRight(RepositoryError.InvalidStoredData)
  private def instant(document: Document, field: String): Either[RepositoryError, Instant] =
    Option(document.get(field))
      .collect { case value: Date => value.toInstant }
      .toRight(RepositoryError.InvalidStoredData)
  private def enumStatus(value: String): Either[RepositoryError, ApplicationStatus] =
    ApplicationStatus.values.find(_.toString == value).toRight(RepositoryError.InvalidStoredData)
}
