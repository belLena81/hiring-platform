package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.mongodb.MongoCommandException
import com.mongodb.client.model.{Filters, Sorts, Updates, UpdateOptions}
import fs2.interop.reactivestreams.*
import org.bson.Document
import scala.jdk.CollectionConverters.*

/** A new cutover establishes an audited baseline and store-enforced command semantics before avoiding old scans. */
private[mongo] object MongoWorkflowIntegrityMigrations {
  val MigrationId = "013_hiring_workflow_integrity"
  private val BatchSize = 500
  private val Uuid = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"

  def commandValidator: Document = {
    def text = new Document("bsonType", "string").append("minLength", 1).append("maxLength", 256)
    def enumSchema(values: String*) = new Document("enum", values.toList.asJava)
    def stringUuid = text.append("pattern", Uuid)
    def objectSchema(required: List[String], properties: Document) = new Document("bsonType", "object")
      .append("required", required.asJava)
      .append("properties", properties)
    def kind(name: String, fields: (String, Document)*) = objectSchema(
      "kind" :: fields.toList.map(_._1),
      fields.foldLeft(new Document("kind", enumSchema(name))) { case (result, (field, schema)) =>
        result.append(field, schema)
      }
    )
    val commands = List(
      kind("reserveCalendar", "idempotencyKey" -> text),
      kind("releaseCalendar", "idempotencyKey" -> text),
      kind("lookupCalendar", "workflowId" -> stringUuid),
      kind("lookupStatusCommit", "workflowId" -> stringUuid),
      kind("commitInterview", "expectedStatus" -> enumSchema("Accepted")),
      kind("notify", "participant" -> enumSchema("Candidate", "Recruiter"), "idempotencyKey" -> text),
      kind("lookupNotification", "participant" -> enumSchema("Candidate", "Recruiter"), "idempotencyKey" -> text),
      kind("requireRepair", "reason" -> text)
    )
    val states = List(
      "Pending",
      "Claimed",
      "Published",
      "Executing",
      "ResultPending",
      "ResultPublished",
      "RepairRequired",
      "Superseded"
    )
    def stateCase(names: List[String], required: List[String]) = {
      val shape = new Document("properties", new Document("commandState", enumSchema(names*)))
      if (required.nonEmpty) shape.append("required", required.asJava) else shape
    }
    val schema = objectSchema(
      List(
        "_id",
        "workflowId",
        "stepId",
        "revision",
        "command",
        "commandState",
        "attempts",
        "executionAttempts",
        "availableAt",
        "occurredAt"
      ),
      new Document("_id", text)
        .append("workflowId", stringUuid)
        .append("stepId", text)
        .append("revision", new Document("bsonType", "long").append("minimum", 0L))
        .append("attempts", new Document("bsonType", "int").append("minimum", 0))
        .append("executionAttempts", new Document("bsonType", "int").append("minimum", 0))
        .append("availableAt", new Document("bsonType", "date"))
        .append("occurredAt", new Document("bsonType", "date"))
        .append("command", new Document("oneOf", commands.asJava))
        .append("commandState", enumSchema(states*))
        .append("result", enumSchema("Succeeded", "Rejected", "OutcomeUnknown", "Found", "Absent"))
    )
      .append(
        "allOf",
        List(
          new Document(
            "oneOf",
            List(
              stateCase(List("Claimed", "Executing"), List("claimOwner", "claimToken", "claimUntil"))
                .append(
                  "properties",
                  new Document("commandState", enumSchema("Claimed", "Executing"))
                    .append("claimOwner", text)
                    .append("claimToken", stringUuid)
                    .append("claimUntil", new Document("bsonType", "date"))
                ),
              stateCase(states.filterNot(Set("Claimed", "Executing")), Nil)
            ).asJava
          ),
          new Document(
            "oneOf",
            List(
              stateCase(List("ResultPending", "ResultPublished"), List("result")),
              stateCase(states.filterNot(Set("ResultPending", "ResultPublished")), Nil)
            ).asJava
          )
        ).asJava
      )
    def safeString(path: String) = new Document(
      "$convert",
      new Document("input", path)
        .append("to", "string")
        .append("onError", "")
        .append("onNull", "")
    )
    def concat(parts: Any*) = new Document("$concat", parts.toList.asJava)
    def eq(left: Any, right: Any) = new Document("$eq", List(left, right).asJava)
    def branch(names: List[String], expression: Document) =
      new Document("case", new Document("$in", List("$command.kind", names.asJava).asJava))
        .append("then", expression)
    val ownership = new Document(
      "$switch",
      new Document(
        "branches",
        List(
          branch(
            List("reserveCalendar"),
            eq(safeString("$command.idempotencyKey"), concat(safeString("$workflowId"), ":reserve"))
          ),
          branch(
            List("releaseCalendar"),
            eq(safeString("$command.idempotencyKey"), concat(safeString("$workflowId"), ":release"))
          ),
          branch(
            List("lookupCalendar", "lookupStatusCommit"),
            eq(safeString("$command.workflowId"), safeString("$workflowId"))
          ),
          branch(
            List("notify", "lookupNotification"),
            eq(
              safeString("$command.idempotencyKey"),
              concat(safeString("$workflowId"), ":notify:", safeString("$command.participant"))
            )
          )
        ).asJava
      ).append("default", true)
    )
    new Document(
      "$and",
      List(
        new Document("$jsonSchema", schema),
        new Document(
          "$expr",
          new Document(
            "$and",
            List(
              eq(safeString("$_id"), concat(safeString("$workflowId"), ":", safeString("$stepId"))),
              ownership
            ).asJava
          )
        )
      ).asJava
    )
  }

  private def currentDefinitions(database: MongoHiringSetup.SetupDatabase): IO[Boolean] = {
    val names = List(MongoCollections.InterviewWorkflowCommands, MongoCollections.EventOutbox)
    val command =
      new Document("listCollections", 1).append("filter", new Document("name", new Document("$in", names.asJava)))
    IO.delay(database.underlying.underlying.runCommand(command, classOf[Document]))
      .flatMap(_.toStreamBuffered[IO](1).compile.lastOrError)
      .map { result =>
        val entries = Option(result.get("cursor", classOf[Document])).toList
          .flatMap(cursor => Option(cursor.getList("firstBatch", classOf[Document])).toList.flatMap(_.asScala))
        names.forall { name =>
          entries.find(_.getString("name") == name).exists { entry =>
            Option(entry.get("options", classOf[Document])).exists { options =>
              options.getString("validationLevel") == "strict" && options.getString("validationAction") == "error" &&
              Option(options.get("validator", classOf[Document])).contains(
                if (name == MongoCollections.InterviewWorkflowCommands) commandValidator
                else MongoHiringValidators.outboxValidator
              )
            }
          }
        }
      }
  }

  private def verifyCoveredLedgers(database: MongoHiringSetup.SetupDatabase): IO[Unit] =
    List("003_event_outbox_subject_references", "010_interview_workflow_attempts").traverse_ { id =>
      database.getCollection(MongoCollections.HiringMigrationLedger).find(Filters.eq("_id", id)).first.flatMap {
        case Some(row)
            if Option(row.get("version"))
              .collect { case value: java.lang.Long if value.longValue() == 1L => () }
              .contains(()) &&
              Option(row.get("state")).contains("Complete") =>
          IO.unit
        case _ => IO.raiseError(new IllegalStateException("Unsupported covered hiring migration proof"))
      }
    }

  def trusted(database: MongoHiringSetup.SetupDatabase): IO[Boolean] =
    database.getCollection(MongoCollections.HiringMigrationLedger).find(Filters.eq("_id", MigrationId)).first.flatMap {
      case None => IO.pure(false)
      case Some(row)
          if Option(row.get("version"))
            .collect { case value: java.lang.Long if value.longValue() == 1L => () }
            .contains(()) && row.getString("state") == "Running" =>
        IO.pure(false)
      case Some(row)
          if Option(row.get("version"))
            .collect { case value: java.lang.Long if value.longValue() == 1L => () }
            .contains(()) && row.getString("state") == "Complete" =>
        currentDefinitions(database).flatMap {
          case true  => verifyCoveredLedgers(database).as(true)
          case false =>
            IO.raiseError(
              new IllegalStateException("Hiring integrity validator changed; explicit audit and repair required")
            )
        }
      case Some(_) => IO.raiseError(new IllegalStateException("Unsupported hiring integrity migration state"))
    }

  def initialize(database: MongoHiringSetup.SetupDatabase): IO[Unit] = trusted(database).flatMap {
    case true  => IO.unit
    case false =>
      val ledger = database.getCollection(MongoCollections.HiringMigrationLedger)
      val identity = Filters.eq("_id", MigrationId)
      def scan(after: Option[String]): IO[Unit] = {
        val collection = database.getCollection(MongoCollections.InterviewWorkflowCommands)
        collection
          .find(after.fold(Filters.empty())(id => Filters.gt("_id", id)))
          .sort(Sorts.ascending("_id"))
          .limit(BatchSize)
          .boundedStream(32)
          .compile
          .toList
          .flatMap { rows =>
            rows.traverse_(row =>
              IO.fromEither(
                MongoInterviewWorkflowCommandCodec
                  .decode(row)
                  .left
                  .map(_ => new IllegalStateException("Hiring integrity cutover rejected a command"))
              ).void
            ) *>
              (if (rows.size == BatchSize) scan(rows.lastOption.map(_.getString("_id"))) else IO.unit)
          }
      }
      def install(name: String, validator: Document): IO[Unit] =
        database.createCollection(name).recoverWith {
          case error: MongoCommandException if error.getErrorCode == 48 => IO.unit
        } *> database.runCommand(
          new Document("collMod", name)
            .append("validator", validator)
            .append("validationLevel", "strict")
            .append("validationAction", "error")
        )
      ledger
        .updateOne(
          identity,
          Updates.combine(
            Updates.setOnInsert("_id", MigrationId),
            Updates.set("version", Long.box(1L)),
            Updates.set("state", "Running")
          ),
          new UpdateOptions().upsert(true)
        )
        .void *>
        scan(None) *> install(MongoCollections.InterviewWorkflowCommands, commandValidator) *>
        install(MongoCollections.EventOutbox, MongoHiringValidators.outboxValidator) *>
        verifyStored(database) *> verifyCoveredLedgers(database) *>
        ledger.updateOne(identity, Updates.set("state", "Complete")).void
  }

  /** Strict validators are not retroactive: the cutover checks all existing rows once. */
  private def verifyStored(database: MongoHiringSetup.SetupDatabase): IO[Unit] =
    List(
      MongoCollections.InterviewWorkflowCommands -> commandValidator,
      MongoCollections.EventOutbox -> MongoHiringValidators.outboxValidator
    ).traverse_ { case (name, validator) =>
      database.getCollection(name).find(new Document("$nor", List(validator).asJava)).limit(1).first.flatMap {
        case None    => IO.unit
        case Some(_) => IO.raiseError(new IllegalStateException("Hiring integrity cutover found invalid stored data"))
      }
    }
}
