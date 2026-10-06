package com.example.graphQL.cats.repository.mongo

import com.mongodb.MongoClientSettings
import org.bson.{BsonDocument, Document}
import com.mongodb.client.model.Updates
import munit.FunSuite
import java.util.{UUID, Date}

final class MongoInterviewWorkflowMigrationsSpec extends FunSuite {
  private val workflow = UUID.fromString("00000000-0000-0000-0000-000000000001").toString
  private def row(kind: String, state: String = "Pending"): Document =
    new Document("_id", s"$workflow:$workflow:0:0")
      .append("workflowId", workflow)
      .append("stepId", s"$workflow:0:0")
      .append("revision", Long.box(0L))
      .append("availableAt", new Date(0L))
      .append("occurredAt", new Date(0L))
      .append("claimOwner", "worker")
      .append("claimToken", workflow)
      .append("claimUntil", new Date(1000L))
      .append("attempts", Int.box(2))
      .append("commandState", state)
      .append("command", new Document("kind", kind).append("idempotencyKey", s"$workflow:reserve"))

  private def updates(value: Document): BsonDocument =
    MongoInterviewWorkflowMigrations
      .commandUpdates(value)
      .fold(
        reason => fail(reason),
        changes =>
          Updates.combine(changes*).toBsonDocument(classOf[Document], MongoClientSettings.getDefaultCodecRegistry)
      )

  test("queued commands preserve their previously charged logical slot") {
    val migrated = updates(row("reserveCalendar"))
    assertEquals(migrated.getDocument("$set").getInt32("executionAttempts").getValue, 1)
    assert(!migrated.getDocument("$set").containsKey("commandState"))
  }

  test("unknown old execution queues reconciliation while preserving the charged slot") {
    val migrated = updates(row("reserveCalendar", "Executing"))
    assertEquals(migrated.getDocument("$set").getString("result").getValue, "OutcomeUnknown")
    assertEquals(migrated.getDocument("$set").getString("commandState").getValue, "ResultPending")
    assert(migrated.getDocument("$unset").containsKey("claimToken"))
  }

  test("notification lookup participant is derived only from the complete canonical key") {
    val value = row("lookupNotification")
    value.get("command", classOf[Document]).append("idempotencyKey", s"$workflow:notify:Candidate")
    assertEquals(updates(value).getDocument("$set").getString("command.participant").getValue, "Candidate")
    value.get("command", classOf[Document]).put("idempotencyKey", "unrelated:Candidate")
    assert(MongoInterviewWorkflowMigrations.commandUpdates(value).isLeft)
  }

  test("unsupported retry, malformed counters and incomplete converted payloads fail closed") {
    assert(MongoInterviewWorkflowMigrations.commandUpdates(row("retry")).isLeft)
    val malformed = row("reserveCalendar").append("attempts", Int.box(-1))
    assert(MongoInterviewWorkflowMigrations.commandUpdates(malformed).isLeft)
    val incomplete = row("lookupNotification").append("executionAttempts", Int.box(1))
    incomplete.get("command", classOf[Document]).append("idempotencyKey", s"$workflow:notify:Candidate")
    assert(MongoInterviewWorkflowMigrations.commandUpdates(incomplete).isLeft)
  }

  test("already converted execution counters cannot be reset on restart") {
    val value = row("reserveCalendar", "Executing").append("executionAttempts", Int.box(3))
    assertEquals(MongoInterviewWorkflowMigrations.commandUpdates(value), Right(Nil))
  }
}
