package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.domain.workflow.InterviewWorkflowId

/** Stored field names and collection sets of the interview workflow, declared once for every reader and writer. */
private[mongo] object MongoInterviewFields {
  val WorkflowIdField = "workflowId"
  val RevisionField = "revision"
  val DocumentTypeField = "documentType"

  /** The stored `documentType` values: `interview_workflows` holds workflows, the inbox holds inbox receipts and
    * quarantine markers, and the command queue holds commands.
    */
  val WorkflowType = "workflow"

  /** Document types that earlier binaries stored and that migrations 029 and 030 removed; kept for the validators of
    * the earlier generation, migration 022 and the two removal steps.
    */
  val RetiredHiringReceiptType = "hiringReceipt"
  val RetiredRequestReceiptType = "requestReceipt"
  val InboxReceiptType = "inboxReceipt"
  val QuarantineType = "quarantine"
  val CommandType = "command"

  val MessageIdField = "messageId"
  val StepIdField = "stepId"
  val CommandField = "command"
  val CommandStateField = "commandState"
  val FailureCodeField = "failureCode"
  val ReleasedAtField = "releasedAt"
  val StartField = "startsAt"
  val EndField = "endsAt"
  val IdempotencyKeyField = "idempotencyKey"

  /** The command queue stores its lease under claim-prefixed names. */
  val Lease: MongoLeaseQueue.LeaseFields = MongoLeaseQueue.LeaseFields("claimOwner", "claimToken", "claimUntil")

  /** Every collection that keeps evidence of one workflow; account erasure and retention both walk these. */
  val EvidenceCollections: List[String] = List(
    MongoCollections.InterviewWorkflows,
    MongoCollections.InterviewWorkflowCommands,
    MongoCollections.InterviewWorkflowInbox,
    MongoCollections.InterviewCalendarReservations,
    MongoCollections.InterviewNotificationReceipts
  )

  /** Evidence plus the participant locks, which belong to no single workflow. */
  val StoredCollections: List[String] = EvidenceCollections :+ MongoCollections.InterviewCalendarParticipantLocks

  /** The evidence collections with an index-served filter for one workflow: the workflow by `_id`, the rest by their
    * indexed `workflowId`. No branch is unindexed, so retention never scans a collection.
    */
  def retentionTargets(id: InterviewWorkflowId): List[(String, MongoFilter)] = {
    val identity = id.value.toString
    EvidenceCollections.map {
      case MongoCollections.InterviewWorkflows =>
        MongoCollections.InterviewWorkflows -> MongoFilter.eq(MongoFields.Id, identity)
      case name => name -> MongoFilter.eq(WorkflowIdField, identity)
    }
  }
}
