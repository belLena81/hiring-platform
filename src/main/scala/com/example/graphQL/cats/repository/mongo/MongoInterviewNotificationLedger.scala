package com.example.graphQL.cats.repository.mongo

import cats.data.EitherT
import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.repository.mongo.MongoDocumentFields.Repository.*
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.RepositoryError
import org.bson.Document

import java.util.UUID
import scala.jdk.CollectionConverters.*

/** Durable notification receipts: each delivery is recorded once, fenced against the workflow phase. */
final class MongoInterviewNotificationLedger(store: MongoInterviewWorkflowStore)
    extends InterviewNotificationReceiptLedger {
  import MongoInterviewFields.*
  import store.*

  override def deliverOnce(
      receipt: InterviewNotificationReceipt,
      execution: Option[ClaimedInterviewWorkflowCommand] = None
  ): InterviewProviderIO[InterviewNotificationReceipt] = {
    val result = MongoRepositorySupport.repositoryGuard(diagnostics, "interviewNotification.deliverOnce") {
      transactionRunner.run { session =>
        fenceExecution(session, receipt.workflowId, execution) *>
          loadWorkflow(receipt.workflowId, session).orConflict.flatMap(workflow =>
            for {
              _ <- fenceParticipants(session, workflow)
              _ <- RepositoryIO.fromEither(
                Either.cond(
                  InterviewNotificationFence.permits(receipt.kind, workflow.phase),
                  (),
                  RepositoryError.Conflict
                )
              )
              _ <- bumpProviderFence(session, workflow.id, workflow.phase)
              receiptExists <- RepositoryIO.lift(
                MongoSessionOperations.findById(inbox, session, s"${workflow.id.value}:hiring")
              )
              // Only the scheduling notification requires the hiring commit; later kinds follow other durable events.
              _ <- RepositoryIO.fromEither(
                Either.cond(
                  receipt.kind != InterviewNotificationKind.Scheduled || receiptExists.nonEmpty,
                  (),
                  RepositoryError.Conflict
                )
              )
            } yield ()
          ) *>
          RepositoryIO
            .lift(
              MongoSessionOperations.findById(notificationReceipts, session, receipt.idempotencyKey)
            )
            .flatMap {
              case Some(document) =>
                RepositoryIO.fromEither(decodeNotification(document).flatMap { existing =>
                  Either.cond(sameNotification(existing, receipt), existing, RepositoryError.Conflict)
                })
              case None =>
                RepositoryIO
                  .lift(MongoSessionOperations.insertOne(notificationReceipts, session, notificationDocument(receipt)))
                  .as(receipt)
            }
      }
    }
    mapProvider(result)
  }

  override def find(idempotencyKey: String): InterviewProviderIO[Option[InterviewNotificationReceipt]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "interviewNotification.find") {
        RepositoryIO
          .lift(
            MongoSessionOperations.findById(notificationReceipts, None, idempotencyKey)
          )
          .flatMap(_.traverse(document => RepositoryIO.fromEither(decodeNotification(document))))
      }
      .leftMap(_ => InterviewProviderError.Unavailable)

  private def notificationDocument(value: InterviewNotificationReceipt): Document =
    new Document(MongoFields.Id, value.idempotencyKey)
      .append(WorkflowIdField, value.workflowId.value.toString)
      .append("recipientId", value.recipientId.value.toString)
      .append(MongoFields.SubjectIds, List(value.recipientId.value.toString).asJava)
      .append("participant", value.participant.toString)
      .append("kind", value.kind.toString)
      .append("deliveredAt", value.deliveredAt.toDate)

  private def decodeNotification(document: Document): Either[RepositoryError, InterviewNotificationReceipt] =
    for {
      workflowId <- uuid(document, WorkflowIdField).map(InterviewWorkflowId.apply)
      recipient <- uuid(document, "recipientId").map(UserId.apply)
      participantName <- string(document, "participant")
      participant <- named(InterviewParticipant.values)(participantName)
      kind <- Option(document.get("kind")) match {
        // Receipts written before message kinds existed are scheduling notifications.
        case None               => Right(InterviewNotificationKind.Scheduled)
        case Some(name: String) =>
          named(InterviewNotificationKind.values)(name)
        case _ => Left(RepositoryError.InvalidStoredData)
      }
      key <- string(document, MongoFields.Id)
      delivered <- instant(document, "deliveredAt")
    } yield InterviewNotificationReceipt(workflowId, recipient, participant, kind, key, delivered)

  private def sameNotification(left: InterviewNotificationReceipt, right: InterviewNotificationReceipt): Boolean =
    left.workflowId == right.workflowId && left.recipientId == right.recipientId &&
      left.participant == right.participant && left.kind == right.kind && left.idempotencyKey == right.idempotencyKey
}
