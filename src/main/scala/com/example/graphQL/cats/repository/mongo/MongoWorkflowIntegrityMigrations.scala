package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.mongodb.client.model.{Filters, Sorts}
import org.bson.Document
import scala.jdk.CollectionConverters.*

/** A new cutover establishes an audited baseline and store-enforced command semantics before avoiding old scans. */
private[mongo] object MongoWorkflowIntegrityMigrations {
  private val Id: MigrationId = MigrationIds.HiringWorkflowIntegrity

  /** Ledger literal kept as `String` for existing callers; the typed identity is
    * `MigrationIds.HiringWorkflowIntegrity`.
    */
  val MigrationId: String = Id.value
  private val BatchSize = 500
  private val CoveredProofs = List(MigrationIds.EventOutboxSubjectReferences, MigrationIds.InterviewWorkflowAttempts)

  def commandValidator: Document = {
    import MongoValidatorSchemas.{enumSchema, objectSchema, shortText as text, stringUuid}
    def kind(name: String, fields: (String, Document)*) = objectSchema(
      MongoFields.Kind :: fields.toList.map(_._1),
      fields.foldLeft(new Document(MongoFields.Kind, enumSchema(name))) { case (result, (field, schema)) =>
        result.append(field, schema)
      }
    )
    val commands = List(
      kind("reserveCalendar", MongoFields.IdempotencyKey -> text),
      kind("releaseCalendar", MongoFields.IdempotencyKey -> text),
      kind("lookupCalendar", MongoFields.WorkflowId -> stringUuid),
      kind("lookupStatusCommit", MongoFields.WorkflowId -> stringUuid),
      kind("commitInterview", "expectedStatus" -> enumSchema("Accepted")),
      kind("notify", "participant" -> enumSchema("Candidate", "Recruiter"), MongoFields.IdempotencyKey -> text),
      kind(
        "lookupNotification",
        "participant" -> enumSchema("Candidate", "Recruiter"),
        MongoFields.IdempotencyKey -> text
      ),
      kind("requireRepair", MongoFields.Reason -> text)
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
        MongoFields.Id,
        MongoFields.WorkflowId,
        "stepId",
        MongoFields.Revision,
        "command",
        "commandState",
        MongoFields.Attempts,
        "executionAttempts",
        MongoFields.AvailableAt,
        MongoFields.OccurredAt
      ),
      new Document(MongoFields.Id, text)
        .append(MongoFields.WorkflowId, stringUuid)
        .append("stepId", text)
        .append(MongoFields.Revision, new Document("bsonType", "long").append("minimum", 0L))
        .append(MongoFields.Attempts, new Document("bsonType", "int").append("minimum", 0))
        .append("executionAttempts", new Document("bsonType", "int").append("minimum", 0))
        .append(MongoFields.AvailableAt, new Document("bsonType", "date"))
        .append(MongoFields.OccurredAt, new Document("bsonType", "date"))
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

  private val validators: List[(String, Document)] = List(
    MongoCollections.InterviewWorkflowCommands -> commandValidator,
    MongoCollections.EventOutbox -> MongoHiringValidators.outboxValidator
  )

  /** Exact installed validators plus the covered 003/010 proofs are required before their old scans are skipped. */
  private def verifyCompleted(database: MongoHiringSetup.SetupDatabase): IO[CompletedProof] =
    MongoHiringValidators.assertStrictValidators(database, validators) *>
      MongoMigrationLedger.requireComplete(database, CoveredProofs).as(CompletedProof.Trusted)

  private def cutover(run: MigrationRun): IO[Unit] = {
    val commands = run.database.getCollection(MongoCollections.InterviewWorkflowCommands)
    def scan(after: Option[String]): IO[Unit] =
      commands
        .find(after.fold(Filters.empty())(id => Filters.gt(MongoFields.Id, id)))
        .sort(Sorts.ascending(MongoFields.Id))
        .limit(BatchSize)
        .boundedStream(32)
        .compile
        .toList
        .flatMap { rows =>
          rows.traverse_(row =>
            IO.fromEither(
              MongoInterviewWorkflowCommandCodec
                .decode(row)
                .leftMap(_ => MigrationError.StepFailed(run.id, "cutover rejected a command"))
            ).void
          ) *>
            (if (rows.size == BatchSize) scan(rows.lastOption.map(_.getString(MongoFields.Id))) else IO.unit)
        }
    scan(None) *>
      validators.traverse_ { case (name, validator) => MongoHiringValidators.install(run.database, name, validator) } *>
      verifyStored(run) *> MongoMigrationLedger.requireComplete(run.database, CoveredProofs)
  }

  /** Strict validators are not retroactive: the cutover checks all existing rows once. */
  private def verifyStored(run: MigrationRun): IO[Unit] =
    validators.traverse_ { case (name, validator) =>
      run.database.getCollection(name).find(new Document("$nor", List(validator).asJava)).limit(1).first.flatMap {
        case None    => IO.unit
        case Some(_) => run.fail(s"cutover found invalid stored data in $name")
      }
    }

  val step: MongoMigrationStep = MongoMigrationStep(Id, cutover, verifyCompleted)

  def trusted(database: MongoHiringSetup.SetupDatabase): IO[Boolean] = MongoMigrationRunner.trusted(database, step)

  def initialize(database: MongoHiringSetup.SetupDatabase): IO[Unit] = MongoMigrationRunner.run(database, step)
}
