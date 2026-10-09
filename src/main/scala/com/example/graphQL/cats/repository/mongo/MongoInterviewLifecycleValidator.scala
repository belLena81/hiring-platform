package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.domain.workflow.{InterviewNotificationKind, InterviewParticipant}
import org.bson.Document
import scala.jdk.CollectionConverters.*

/** Command-validator additions for cancellation and rescheduling intents: closed kinds, field shapes and the identity
  * rules that bind every key to its own workflow. The base command validator composes these when it is extended.
  */
private[mongo] object MongoInterviewLifecycleValidator {
  import MongoInterviewWorkflowCommandCodec.LifecycleKinds as K
  import MongoValidatorSchemas.{date, enumSchema, objectSchema, shortText as text, stringUuid}

  private val participants = enumSchema(InterviewParticipant.values.toList.map(_.toString)*)
  private val interval = objectSchema(
    List("startsAt", "endsAt"),
    new Document("startsAt", date).append("endsAt", date)
  )

  private def kind(name: String, fields: (String, Document)*): Document =
    objectSchema(
      MongoFields.Kind :: fields.toList.map(_._1),
      fields.foldLeft(new Document(MongoFields.Kind, enumSchema(name))) { case (result, (field, schema)) =>
        result.append(field, schema)
      }
    )

  val commandKinds: List[Document] = {
    val key = MongoFields.IdempotencyKey -> text
    val notificationKind = "notificationKind" -> enumSchema(InterviewNotificationKind.values.toList.map(_.toString)*)
    List(
      kind(K.CancelCalendar, key),
      kind(K.LookupCancellation, key),
      kind(K.HoldReplacement, key, "interval" -> interval),
      kind(K.LookupReplacementHold, key),
      kind(
        K.CommitReschedule,
        "interval" -> interval,
        "generation" -> new Document("bsonType", "int").append("minimum", 1)
      ),
      kind(K.LookupRescheduleCommit, MongoFields.WorkflowId -> stringUuid),
      kind(K.ExpireProposal, MongoFields.AvailableAt -> date),
      kind(K.NotifyKind, notificationKind, "participant" -> participants, key),
      kind(K.LookupNotificationKind, notificationKind, "participant" -> participants, key)
    )
  }

  private def safeString(path: String) =
    new Document(
      "$convert",
      new Document("input", path).append("to", "string").append("onError", "").append("onNull", "")
    )
  private def concat(parts: Any*) = new Document("$concat", parts.toList.asJava)
  private def eq(left: Any, right: Any) = new Document("$eq", List(left, right).asJava)
  private def matches(input: String, pattern: Document) =
    new Document("$regexMatch", new Document("input", safeString(input)).append("regex", pattern))
  private def branch(names: List[String], expression: Document) =
    new Document("case", new Document("$in", List("$command.kind", names.asJava).asJava)).append("then", expression)

  private val workflow = safeString("$workflowId")

  /** A bare `$` would be read as a field path; the regex end anchor must be a literal. */
  private val endAnchor = new Document("$literal", "$")

  /** Mirrors `InterviewNotificationKind.keyName`: the kind name with a lower-case first letter. */
  private def keyName(kind: Document) = concat(
    new Document("$toLower", new Document("$substrCP", List(kind, 0, 1).asJava)),
    new Document("$substrCP", List(kind, 1, 64).asJava)
  )

  /** Keys must start with the owning workflow id and carry the generation or participant they address. */
  val ownershipBranches: List[Document] = List(
    branch(
      List(K.CancelCalendar, K.LookupCancellation),
      matches("$command.idempotencyKey", concat("^", workflow, ":cancel:g[0-9]+", endAnchor))
    ),
    branch(
      List(K.HoldReplacement, K.LookupReplacementHold),
      matches("$command.idempotencyKey", concat("^", workflow, ":reserve:g[1-9][0-9]*", endAnchor))
    ),
    branch(List(K.LookupRescheduleCommit), eq(safeString("$command.workflowId"), workflow)),
    branch(List(K.ExpireProposal), eq("$command.availableAt", "$availableAt")),
    branch(
      List(K.NotifyKind, K.LookupNotificationKind),
      matches(
        "$command.idempotencyKey",
        concat(
          "^",
          workflow,
          ":",
          keyName(safeString("$command.notificationKind")),
          "(:[rg][0-9]+)?:notify:",
          safeString("$command.participant"),
          endAnchor
        )
      )
    )
  )
}
