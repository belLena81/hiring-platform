package com.example.graphQL.cats.repository.mongo

import org.bson.Document

import scala.jdk.CollectionConverters.*

/** Strict `$jsonSchema` validators of the operational collections that migration `025` installs, one definition per
  * collection. Every rule comes from the stored shape the collection's writers produce:
  *   - closed shapes (`additionalProperties: false`): applications, application events and the receipts, which have one
  *     writer and no evolving fields;
  *   - open shapes (required fields and types only): embedding work, search sessions and work, and the interview
  *     collections, whose lifecycle fields grow with the workflow;
  *   - polymorphic interview collections carry `documentType` with one `oneOf` branch per stored type.
  *
  * Enumerations are stored by Scala name. The closed domain enumerations are written here as literals so a new
  * enumeration case is a deliberate validator change; `MongoOperationalValidatorsSpec` compares them with the domain.
  */
private[mongo] object MongoOperationalValidators {
  import MongoValidatorSchemas.*

  val ApplicationStatuses: List[String] = List("Created", "Accepted", "Declined", "Interview", "Hired", "Rejected")
  val UserRoles: List[String] = List("Candidate", "Recruiter", "Admin")
  val EmbeddingWorkKinds: List[String] = List("Job", "CandidateProfile")
  val EmbeddingWorkStates: List[String] = List("Ready", "Retry", "Processing", "Failed")
  val EmbeddingWorkFailures: List[String] =
    List("RetryExhausted", "DocumentTooLarge", "InvalidWorkKey", "InvalidResponse")
  val SearchSessionWorkStates: List[String] = List("Ready", "Processing", "Retry", "Failed")
  val SearchSessionWorkFailures: List[String] = List("RetryExhausted", "InvalidWork")
  val MutationReceiptStates: List[String] = List("InProgress", "Completed")

  private def text: Document = new Document("bsonType", "string")
  private def array(items: Document): Document = new Document("bsonType", "array").append("items", items)
  private def int: Document = new Document("bsonType", "int")
  private def document: Document = new Document("bsonType", "object")
  private def binary: Document = new Document("bsonType", "binData")
  private def double: Document = new Document("bsonType", "double")

  private def properties(entries: (String, Document)*): Document =
    entries.foldLeft(new Document()) { case (all, (name, schema)) => all.append(name, schema) }

  private def validator(required: List[String], entries: Document, closed: Boolean): Document = {
    val schema = objectSchema(required, entries)
    new Document("$jsonSchema", if (closed) schema.append("additionalProperties", false) else schema)
  }

  /** One `oneOf` branch per stored `documentType`; a document with another or no type matches none. */
  private def polymorphic(branches: List[(String, List[String], Document)]): Document = {
    val tagged = branches.map { case (tag, required, entries) =>
      objectSchema(
        MongoFields.DocumentType :: required,
        new Document(entries).append(MongoFields.DocumentType, enumSchema(tag))
      )
    }
    new Document(
      "$jsonSchema",
      new Document("bsonType", "object")
        .append("required", List(MongoFields.DocumentType).asJava)
        .append("oneOf", tagged.asJava)
    )
  }

  private def leaseFields: List[(String, Document)] = List(
    MongoFields.LeaseOwner -> text,
    MongoFields.LeaseToken -> text,
    MongoFields.LeaseUntil -> date
  )

  def applicationsValidator: Document = validator(
    List(
      MongoFields.Id,
      MongoFields.CandidateId,
      MongoFields.JobId,
      MongoFields.Status,
      MongoFields.CreatedAt,
      MongoFields.UpdatedAt
    ),
    properties(
      MongoFields.Id -> stringUuid,
      MongoFields.CandidateId -> stringUuid,
      MongoFields.JobId -> stringUuid,
      MongoFields.Status -> enumSchema(ApplicationStatuses*),
      MongoFields.CreatedAt -> date,
      MongoFields.UpdatedAt -> date
    ),
    closed = true
  )

  /** An event always records the role its actor acted in: the history is evidence of who was authorized. */
  def applicationEventsValidator: Document = applicationEventsSchema(withActorRole = true)

  /** The definition migration 025 installed, which still allowed an event without the actor's role. */
  def applicationEventsValidatorThrough025: Document = applicationEventsSchema(withActorRole = false)

  private def applicationEventsSchema(withActorRole: Boolean): Document = validator(
    List(
      MongoFields.Id,
      MongoFields.ApplicationId,
      MongoFields.NewStatus,
      MongoFields.ActorId,
      MongoFields.OccurredAt
    ) ++ Option.when(withActorRole)(MongoFields.ActorRole).toList,
    properties(
      MongoFields.Id -> stringUuid,
      MongoFields.ApplicationId -> stringUuid,
      MongoFields.PreviousStatus -> enumSchema(ApplicationStatuses*),
      MongoFields.NewStatus -> enumSchema(ApplicationStatuses*),
      MongoFields.ActorId -> stringUuid,
      MongoFields.ActorRole -> enumSchema(UserRoles*),
      MongoFields.OccurredAt -> date,
      MongoFields.Feedback -> text,
      MongoFields.Reason -> text
    ),
    closed = true
  )

  /** The work row's kind and entity are the parts of its `_id` (`Kind:entityId`) and are not stored again. */
  def embeddingWorkValidator: Document = embeddingWorkSchema(withKeyFields = false)

  /** The definition migration 025 installed, which still required the two redundant key fields. */
  def embeddingWorkValidatorThrough025: Document = embeddingWorkSchema(withKeyFields = true)

  private def embeddingWorkSchema(withKeyFields: Boolean): Document = validator(
    List(MongoFields.Id) ++ Option
      .when(withKeyFields)(List(MongoFields.Kind, MongoFields.WorkEntityId))
      .toList
      .flatten ++ List(
      MongoFields.State,
      MongoFields.Generation,
      MongoFields.Attempts,
      MongoFields.AvailableAt,
      MongoFields.CreatedAt,
      MongoFields.UpdatedAt
    ),
    properties(
      (List(MongoFields.Id -> shortText) ++
        Option
          .when(withKeyFields)(
            List(
              MongoFields.Kind -> enumSchema(EmbeddingWorkKinds*),
              MongoFields.WorkEntityId -> shortText
            )
          )
          .toList
          .flatten ++ List(
          MongoFields.State -> enumSchema(EmbeddingWorkStates*),
          MongoFields.Generation -> nonNegativeLong,
          MongoFields.Attempts -> nonNegativeInt,
          MongoFields.AvailableAt -> date,
          MongoFields.CreatedAt -> date,
          MongoFields.UpdatedAt -> date,
          MongoFields.Failure -> enumSchema(EmbeddingWorkFailures*),
          MongoFields.FinishedAt -> date
        ) ++ leaseFields)*
    ),
    closed = false
  )

  private def sessionProperties: List[(String, Document)] = List(
    MongoFields.Id -> stringUuid,
    MongoFields.ActorId -> stringUuid,
    MongoFields.SearchKind -> text,
    MongoFields.Query -> text,
    MongoFields.Filter -> text,
    MongoFields.Model -> text,
    MongoFields.Results -> array(
      objectSchema(
        List(MongoFields.ResultId, MongoFields.Rank, MongoFields.Score),
        properties(MongoFields.ResultId -> text, MongoFields.Rank -> int, MongoFields.Score -> double)
      )
    ),
    MongoFields.OccurredAt -> date,
    MongoFields.ExpiresAt -> date
  )

  private val SessionRequired: List[String] = List(
    MongoFields.Id,
    MongoFields.ActorId,
    MongoFields.SearchKind,
    MongoFields.Filter,
    MongoFields.Results,
    MongoFields.OccurredAt,
    MongoFields.ExpiresAt
  )

  def searchSessionsValidator: Document = validator(SessionRequired, properties(sessionProperties*), closed = false)

  /** The work row holds the session only; its `SearchPerformed` event is derived when the work completes. */
  def searchSessionWorkValidator: Document = searchSessionWorkSchema(withEvent = false)

  /** The definition migration 025 installed, which also required a stored copy of the event. */
  def searchSessionWorkValidatorThrough025: Document = searchSessionWorkSchema(withEvent = true)

  private def searchSessionWorkSchema(withEvent: Boolean): Document = {
    val queryFreeSession = objectSchema(SessionRequired, properties(sessionProperties*))
      .append("not", new Document("required", List(MongoFields.Query).asJava))
    val event = objectSchema(
      List(MongoFields.Id, MongoFields.Payload, MongoFields.EnvelopeBytes),
      properties(
        MongoFields.Id -> stringUuid,
        MongoFields.Payload -> text,
        MongoFields.EnvelopeBytes -> binary
      )
    )
    validator(
      List(MongoFields.Id, MongoFields.ActorId, MongoFields.Session) ++ Option.when(withEvent)(MongoFields.Event) ++
        List(
          MongoFields.State,
          MongoFields.Attempts,
          MongoFields.AvailableAt,
          MongoFields.CreatedAt,
          MongoFields.UpdatedAt
        ),
      properties(
        (List(
          MongoFields.Id -> stringUuid,
          MongoFields.ActorId -> stringUuid,
          MongoFields.Session -> queryFreeSession
        ) ++ Option.when(withEvent)(MongoFields.Event -> event).toList ++ List(
          MongoFields.State -> enumSchema(SearchSessionWorkStates*),
          MongoFields.Attempts -> nonNegativeInt,
          MongoFields.AvailableAt -> date,
          MongoFields.CreatedAt -> date,
          MongoFields.UpdatedAt -> date,
          MongoFields.Failure -> enumSchema(SearchSessionWorkFailures*),
          MongoFields.FinishedAt -> date,
          MongoFields.RetentionExpiresAt -> date
        ) ++ leaseFields)*
      ),
      closed = false
    )
  }

  def mutationReceiptsValidator: Document = validator(
    List(
      MongoFields.Id,
      MongoFields.Operation,
      MongoFields.ActorScope,
      MongoFields.IdempotencyKey,
      MongoFields.Fingerprint,
      MongoFields.State,
      MongoFields.CreatedAt,
      MongoFields.ExpiresAt
    ),
    properties(
      MongoFields.Id -> stringUuid,
      MongoFields.Operation -> text,
      MongoFields.ActorScope -> text,
      MongoFields.IdempotencyKey -> stringUuid,
      MongoFields.Fingerprint -> text,
      MongoFields.State -> enumSchema(MutationReceiptStates*),
      MongoFields.CreatedAt -> date,
      MongoFields.CompletedAt -> date,
      MongoFields.ExpiresAt -> date,
      MongoFields.Entity -> document.append(
        "properties",
        properties(MongoFields.Type -> text, MongoFields.EntityId -> text)
      )
    ),
    closed = true
  )

  def consumerReceiptsValidator: Document = {
    val required = List(
      MongoFields.Id,
      MongoFields.ConsumerGroup,
      MongoFields.EventId,
      MongoFields.AggregateType,
      MongoFields.AggregateId,
      MongoFields.CreatedAt,
      MongoFields.ExpiresAt
    )
    validator(
      required,
      properties(
        MongoFields.Id -> text,
        MongoFields.ConsumerGroup -> text,
        MongoFields.EventId -> stringUuid,
        MongoFields.AggregateType -> text,
        MongoFields.AggregateId -> text,
        MongoFields.CreatedAt -> date,
        MongoFields.ExpiresAt -> date
      ),
      closed = true
    )
  }

  /** Workflows only, each recording the role of its initiator (`032`). */
  def interviewWorkflowsValidator: Document = interviewWorkflowsSchema(withRequestReceipts = false, roleRequired = true)

  /** The definition `028` installed: workflows only (interview request idempotency is a `mutation_receipts` row), the
    * initiator's role still optional.
    */
  def interviewWorkflowsValidatorThrough028: Document =
    interviewWorkflowsSchema(withRequestReceipts = false, roleRequired = false)

  /** The definition migration 025 installed, which also accepted the request receipt document type. */
  def interviewWorkflowsValidatorThrough025: Document =
    interviewWorkflowsSchema(withRequestReceipts = true, roleRequired = false)

  private def interviewWorkflowsSchema(withRequestReceipts: Boolean, roleRequired: Boolean): Document = polymorphic(
    List(
      (
        MongoInterviewFields.WorkflowType,
        List(
          MongoFields.Id,
          MongoFields.ApplicationId,
          "initiatedBy",
          MongoFields.CandidateId,
          MongoFields.RecruiterId,
          MongoInterviewFields.StartField,
          MongoInterviewFields.EndField,
          "preCommitDeadline",
          MongoInterviewFields.IdempotencyKeyField,
          MongoInterviewFields.RevisionField,
          "phase",
          "notified"
        ) ++ Option.when(roleRequired)("initiatedByRole").toList,
        properties(
          MongoFields.Id -> stringUuid,
          MongoFields.ApplicationId -> stringUuid,
          "initiatedBy" -> stringUuid,
          "initiatedByRole" -> enumSchema(UserRoles*),
          MongoFields.CandidateId -> stringUuid,
          MongoFields.RecruiterId -> stringUuid,
          MongoInterviewFields.StartField -> date,
          MongoInterviewFields.EndField -> date,
          "preCommitDeadline" -> date,
          MongoInterviewFields.IdempotencyKeyField -> text,
          MongoInterviewFields.RevisionField -> nonNegativeLong,
          "phase" -> text,
          "notified" -> array(text),
          MongoInterviewWorkflowLifecycleCodec.GenerationField -> nonNegativeInt,
          MongoInterviewWorkflowLifecycleCodec.PendingStartField -> date,
          MongoInterviewWorkflowLifecycleCodec.PendingEndField -> date,
          MongoInterviewWorkflowLifecycleCodec.CancelledAtField -> date,
          MongoInterviewWorkflowLifecycleCodec.ProposalField -> document,
          MongoInterviewWorkflowLifecycleCodec.RescheduleRequestedAtField -> date,
          MongoInterviewWorkflowLifecycleCodec.RepairOriginField -> text,
          MongoInterviewWorkflowLifecycleCodec.SkippedGenerationsField -> nonNegativeInt,
          "attemptEpochRevision" -> nonNegativeLong,
          MongoFields.RetentionExpiresAt -> date
        )
      )
    ) ++ Option
      .when(withRequestReceipts)(
        (
          MongoInterviewFields.RetiredRequestReceiptType,
          List(MongoFields.Id, "requestWorkflowId", "requestFingerprint", MongoFields.CreatedAt),
          properties(
            MongoFields.Id -> text,
            "requestWorkflowId" -> stringUuid,
            "requestFingerprint" -> text,
            MongoFields.CreatedAt -> date,
            MongoFields.RetentionExpiresAt -> date
          )
        )
      )
      .toList
  )

  /** Inbox receipts and quarantine markers: the hiring commit is evidenced by its `application_events` row. */
  def interviewWorkflowInboxValidator: Document = interviewWorkflowInboxSchema(withHiringReceipts = false)

  /** The definition migration 025 installed, which also accepted the hiring receipt document type. */
  def interviewWorkflowInboxValidatorThrough025: Document = interviewWorkflowInboxSchema(withHiringReceipts = true)

  private def interviewWorkflowInboxSchema(withHiringReceipts: Boolean): Document = polymorphic(
    List(
      (
        MongoInterviewFields.InboxReceiptType,
        List(
          MongoFields.Id,
          MongoInterviewFields.WorkflowIdField,
          MongoInterviewFields.MessageIdField,
          MongoInterviewFields.RevisionField,
          MongoFields.OccurredAt
        ),
        properties(
          MongoFields.Id -> text,
          MongoInterviewFields.WorkflowIdField -> stringUuid,
          MongoInterviewFields.MessageIdField -> text,
          MongoInterviewFields.RevisionField -> nonNegativeLong,
          MongoFields.OccurredAt -> date,
          "repaired" -> nonNegativeInt,
          "auditActorId" -> stringUuid,
          MongoFields.RetentionExpiresAt -> date
        )
      ),
      (
        MongoInterviewFields.QuarantineType,
        List(MongoFields.Id, "failureCode", MongoFields.OccurredAt, MongoFields.RetentionExpiresAt),
        properties(
          MongoFields.Id -> text,
          "failureCode" -> text,
          MongoFields.OccurredAt -> date,
          MongoFields.RetentionExpiresAt -> date
        )
      )
    ) ++ Option
      .when(withHiringReceipts)(
        (
          MongoInterviewFields.RetiredHiringReceiptType,
          List(MongoFields.Id, MongoInterviewFields.WorkflowIdField, MongoFields.SubjectIds, MongoFields.OccurredAt),
          properties(
            MongoFields.Id -> text,
            MongoInterviewFields.WorkflowIdField -> stringUuid,
            MongoFields.SubjectIds -> array(stringUuid).append("minItems", 1).append("maxItems", 2),
            MongoFields.OccurredAt -> date,
            MongoFields.RetentionExpiresAt -> date
          )
        )
      )
      .toList
  )

  def interviewCalendarReservationsValidator: Document = validator(
    List(
      MongoFields.Id,
      MongoInterviewFields.WorkflowIdField,
      "reserveKey",
      "releaseKey",
      MongoFields.CandidateId,
      MongoFields.RecruiterId,
      "participants",
      MongoInterviewFields.StartField,
      MongoInterviewFields.EndField,
      "reservedAt"
    ),
    properties(
      MongoFields.Id -> text,
      MongoInterviewFields.WorkflowIdField -> stringUuid,
      "reserveKey" -> text,
      "releaseKey" -> text,
      MongoFields.CandidateId -> stringUuid,
      MongoFields.RecruiterId -> stringUuid,
      "participants" -> array(stringUuid).append("minItems", 1).append("maxItems", 2),
      MongoInterviewFields.StartField -> date,
      MongoInterviewFields.EndField -> date,
      "reservedAt" -> date,
      MongoInterviewFields.ReleasedAtField -> date,
      MongoFields.RetentionExpiresAt -> date
    ),
    closed = false
  )

  def interviewNotificationReceiptsValidator: Document = validator(
    List(
      MongoFields.Id,
      MongoInterviewFields.WorkflowIdField,
      "recipientId",
      "participant",
      MongoFields.Kind,
      "deliveredAt"
    ),
    properties(
      MongoFields.Id -> text,
      MongoInterviewFields.WorkflowIdField -> stringUuid,
      "recipientId" -> stringUuid,
      "participant" -> enumSchema("Candidate", "Recruiter"),
      MongoFields.Kind -> text,
      "deliveredAt" -> date,
      MongoFields.RetentionExpiresAt -> date
    ),
    closed = true
  )

  /** The current definitions, in installation order; the users definition is `MongoHiringValidators.userValidator`. */
  def all: List[(String, Document)] = List(
    MongoCollections.Users -> MongoHiringValidators.userValidator,
    MongoCollections.Applications -> applicationsValidator,
    MongoCollections.ApplicationEvents -> applicationEventsValidator,
    MongoCollections.EmbeddingWork -> embeddingWorkValidator,
    MongoCollections.SearchSessions -> searchSessionsValidator,
    MongoCollections.SearchSessionWork -> searchSessionWorkValidator,
    MongoCollections.MutationReceipts -> mutationReceiptsValidator,
    MongoCollections.ConsumerReceipts -> consumerReceiptsValidator,
    MongoCollections.InterviewWorkflows -> interviewWorkflowsValidator,
    MongoCollections.InterviewWorkflowInbox -> interviewWorkflowInboxValidator,
    MongoCollections.InterviewCalendarReservations -> interviewCalendarReservationsValidator,
    MongoCollections.InterviewNotificationReceipts -> interviewNotificationReceiptsValidator
  )

  /** The definitions migration 025 installed. Later migrations replaced five of them (application events, embedding
    * work, search session work, interview workflows and the inbox); 025 installs these and its completed proof accepts
    * either generation.
    */
  /** Definitions that `all` replaced after 028 and 029 to 031 had run: a database that completed those steps but not
    * `032` still holds them, and the proof of 025 accepts them.
    */
  def supersededByLaterSteps: Map[String, Document] =
    Map(MongoCollections.InterviewWorkflows -> interviewWorkflowsValidatorThrough028)

  def installedBy025: List[(String, Document)] = all.map {
    case (MongoCollections.ApplicationEvents, _) =>
      MongoCollections.ApplicationEvents -> applicationEventsValidatorThrough025
    case (MongoCollections.EmbeddingWork, _)     => MongoCollections.EmbeddingWork -> embeddingWorkValidatorThrough025
    case (MongoCollections.SearchSessionWork, _) =>
      MongoCollections.SearchSessionWork -> searchSessionWorkValidatorThrough025
    case (MongoCollections.InterviewWorkflows, _) =>
      MongoCollections.InterviewWorkflows -> interviewWorkflowsValidatorThrough025
    case (MongoCollections.InterviewWorkflowInbox, _) =>
      MongoCollections.InterviewWorkflowInbox -> interviewWorkflowInboxValidatorThrough025
    case entry => entry
  }
}
