package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.domain.workflow.InterviewWorkflowId

/** Stored field names and collection sets of the interview workflow, declared once for every reader and writer. */
private[mongo] object MongoInterviewFields {
  val WorkflowIdField = "workflowId"
  val RevisionField = "revision"
  val DocumentTypeField = "documentType"
  val RequestFingerprintField = "requestFingerprint"
  val RequestWorkflowField = "requestWorkflowId"
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

  /** The evidence collections with an index-served filter for one workflow: the workflow and its request receipts share
    * a collection (`_id` and the indexed `requestWorkflowId`), the rest carry an indexed `workflowId`. No branch is
    * unindexed, so retention never scans a collection.
    */
  def retentionTargets(id: InterviewWorkflowId): List[(String, MongoFilter)] = {
    val identity = id.value.toString
    EvidenceCollections.map {
      case MongoCollections.InterviewWorkflows =>
        MongoCollections.InterviewWorkflows ->
          MongoFilter.or(MongoFilter.eq(MongoFields.Id, identity), MongoFilter.eq(RequestWorkflowField, identity))
      case name => name -> MongoFilter.eq(WorkflowIdField, identity)
    }
  }
}
