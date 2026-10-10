package com.example.graphQL.cats.repository.mongo

import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.repository.mongo.MongoDocumentFields.Repository.*
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.RepositoryError
import org.bson.Document

import java.time.Instant
import java.util.UUID
import scala.jdk.CollectionConverters.*

/** Stored shapes of the interview workflow documents: workflow, request receipt, inbox receipt and command row. */
private[mongo] object MongoInterviewWorkflowCodec {
  import MongoInterviewFields.*

  def workflowDocument(value: InterviewWorkflow): Document = {
    val base = new Document(MongoFields.Id, value.id.value.toString)
      .append(DocumentTypeField, "workflow")
      .append("applicationId", value.applicationId.value.toString)
      .append("initiatedBy", value.initiatedBy.value.toString)
      .append("candidateId", value.candidateId.value.toString)
      .append("recruiterId", value.recruiterId.value.toString)
      .append(
        MongoFields.SubjectIds,
        List(value.candidateId.value.toString, value.recruiterId.value.toString).distinct.asJava
      )
      .append(StartField, value.interval.startsAt.toDate)
      .append(EndField, value.interval.endsAt.toDate)
      .append("preCommitDeadline", value.preCommitDeadline.toDate)
      .append(IdempotencyKeyField, value.idempotencyKey.toString)
      .append(RevisionField, value.revision)
      .append("phase", value.phase.toString)
      .append("notified", value.notified.toList.map(_.toString).sorted.asJava)
    MongoInterviewWorkflowLifecycleCodec.insertFields(value).foldLeft(base) { case (document, (field, fieldValue)) =>
      document.append(field, fieldValue)
    }
  }

  def requestReceiptDocument(
      receiptId: String,
      workflowId: InterviewWorkflowId,
      fingerprint: MutationReceiptFingerprint,
      createdAt: Instant
  ): Document =
    new Document(MongoFields.Id, receiptId)
      .append(DocumentTypeField, "requestReceipt")
      .append(RequestWorkflowField, workflowId.value.toString)
      .append(RequestFingerprintField, fingerprint.value)
      .append(MongoFields.CreatedAt, createdAt.toDate)

  def requestReceiptId(actorId: UserId, requestKey: UUID): String =
    s"request:${actorId.value}:$requestKey"

  def decodeReceipt(document: Document): Either[RepositoryError, (InterviewWorkflowId, String)] =
    for {
      workflow <- uuid(document, RequestWorkflowField).map(InterviewWorkflowId.apply)
      fingerprint <- string(document, RequestFingerprintField)
    } yield workflow -> fingerprint

  def inboxDocument(
      id: String,
      workflowId: InterviewWorkflowId,
      messageId: String,
      revision: Long,
      at: Instant
  ): Document =
    new Document(MongoFields.Id, id)
      .append(DocumentTypeField, "inboxReceipt")
      .append(WorkflowIdField, workflowId.value.toString)
      .append(MessageIdField, messageId)
      .append(RevisionField, revision)
      .append(MongoFields.OccurredAt, at.toDate)

  def commandDocument(record: InterviewWorkflowCommandRecord): Document =
    commandEnvelope(
      record.workflowId,
      record.stepId,
      record.revision,
      record.command match {
        case scheduling: InterviewWorkflowCommand => MongoInterviewWorkflowCommandCodec.encodeCommand(scheduling)
        case lifecycle: InterviewLifecycleCommand => MongoInterviewWorkflowCommandCodec.encodeLifecycle(lifecycle)
      },
      record.state,
      record.publicationAttempts,
      record.executionAttempts,
      record.availableAt,
      record.occurredAt
    )

  def commandEnvelope(
      workflowId: InterviewWorkflowId,
      stepId: String,
      revision: Long,
      command: Document,
      state: InterviewWorkflowCommandState,
      publicationAttempts: Int,
      executionAttempts: Int,
      availableAt: Instant,
      occurredAt: Instant
  ): Document =
    new Document(MongoFields.Id, commandId(workflowId, stepId))
      .append(DocumentTypeField, "command")
      .append(WorkflowIdField, workflowId.value.toString)
      .append(StepIdField, stepId)
      .append(RevisionField, revision)
      .append(CommandField, command)
      .append(CommandStateField, state.toString)
      .append(MongoFields.Attempts, publicationAttempts)
      .append("executionAttempts", executionAttempts)
      .append(MongoFields.AvailableAt, availableAt.toDate)
      .append(MongoFields.OccurredAt, occurredAt.toDate)

  def commandId(workflowId: InterviewWorkflowId, stepId: String): String =
    s"${workflowId.value}:$stepId"

  def stepId(workflowId: InterviewWorkflowId, revision: Long, ordinal: Int): String =
    s"${workflowId.value}:$revision:$ordinal"

  def decodeWorkflow(document: Document): Either[RepositoryError, InterviewWorkflow] =
    for {
      id <- uuid(document, MongoFields.Id).map(InterviewWorkflowId.apply)
      initiator <- uuid(document, "initiatedBy").map(UserId.apply)
      application <- uuid(document, "applicationId")
        .map(com.example.graphQL.cats.domain.model.Identifiers.ApplicationId.apply)
      candidate <- uuid(document, "candidateId").map(UserId.apply)
      recruiter <- uuid(document, "recruiterId").map(UserId.apply)
      starts <- instant(document, StartField)
      ends <- instant(document, EndField)
      _ <- Either.cond(ends.isAfter(starts), (), RepositoryError.InvalidStoredData)
      deadline <- instant(document, "preCommitDeadline")
      requestKey <- uuid(document, IdempotencyKeyField)
      revision <- int64(document, RevisionField)
      phaseName <- string(document, "phase")
      phase <- named(InterviewWorkflowPhase.values)(phaseName)
      notified <- stringList(document, "notified").flatMap(names =>
        names
          .traverse(name => named(InterviewParticipant.values)(name))
          .map(_.toSet)
      )
      lifecycle <- MongoInterviewWorkflowLifecycleCodec.decode(document, phase)
      _ <- Either.cond(
        revision >= 0L && (!Set(InterviewWorkflowPhase.Completed, InterviewWorkflowPhase.Cancelled)(phase) ||
          notified == InterviewParticipant.values.toSet),
        (),
        RepositoryError.InvalidStoredData
      )
    } yield InterviewWorkflow(
      id,
      application,
      candidate,
      recruiter,
      InterviewInterval(starts, ends),
      deadline,
      requestKey,
      revision,
      phase,
      notified,
      initiator,
      lifecycle.generation,
      lifecycle.pendingInterval,
      lifecycle.cancelledAt,
      lifecycle.proposal,
      lifecycle.rescheduleRequestedAt,
      lifecycle.repairOrigin,
      lifecycle.skippedGenerations
    )

  /** A reservation is addressed by workflow and reserve key, never by workflow alone. */
  def reservationIdentity(workflowId: InterviewWorkflowId, reserveKey: String): MongoFilter =
    MongoFilter.and(
      MongoFilter.eq(WorkflowIdField, workflowId.value.toString),
      MongoFilter.eq("reserveKey", reserveKey)
    )

  def decodeReservation(document: Document): Either[RepositoryError, InterviewCalendarReservation] =
    for {
      workflowId <- uuid(document, WorkflowIdField).map(InterviewWorkflowId.apply)
      reserveKey <- string(document, "reserveKey")
      candidate <- uuid(document, "candidateId").map(UserId.apply)
      recruiter <- uuid(document, "recruiterId").map(UserId.apply)
      starts <- instant(document, StartField)
      ends <- instant(document, EndField)
      reserved <- instant(document, "reservedAt")
      released <- optionalInstant(document, ReleasedAtField)
    } yield InterviewCalendarReservation(
      workflowId,
      reserveKey,
      candidate,
      recruiter,
      InterviewInterval(starts, ends),
      reserved,
      released
    )
}
