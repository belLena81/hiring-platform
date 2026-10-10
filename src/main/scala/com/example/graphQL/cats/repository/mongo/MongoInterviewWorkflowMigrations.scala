package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.workflow.InterviewParticipant
import com.mongodb.client.model.{CountOptions, Filters, Indexes, UpdateOptions, Updates}
import org.bson.Document
import org.bson.conversions.Bson
import java.util.{UUID, Date}

/** Maintenance cutover preserves identities, receipts and conservatively charged execution budgets. */
private[mongo] object MongoInterviewWorkflowMigrations {
  import MongoBatchScan.BatchSize
  private val ExecutableKinds = Set(
    "reserveCalendar",
    "lookupCalendar",
    "commitInterview",
    "lookupStatusCommit",
    "releaseCalendar",
    "notify",
    "lookupNotification"
  )

  /** Converts each stored command once (guarded per row against concurrent initializers), then re-verifies all rows. */
  val step: MongoMigrationStep = {
    def commands(database: MongoHiringSetup.SetupDatabase) =
      database.getCollection(MongoCollections.InterviewWorkflowCommands)

    def requireStringIdentities(run: MigrationRun): IO[Unit] =
      commands(run.database)
        .count(Filters.not(Filters.`type`(MongoFields.Id, org.bson.BsonType.STRING)), new CountOptions())
        .flatMap(count => IO.whenA(count != 0L)(run.fail("found invalid command identity type")))

    def scan(run: MigrationRun, convert: Boolean): IO[Unit] = {
      val collection = commands(run.database)
      MongoBatchScan.byId(BatchSize, Option.empty[String])(after =>
        collection
          .find(after.fold[Bson](new Document())(id => Filters.gt(MongoFields.Id, id)))
          .sort(Indexes.ascending(MongoFields.Id))
          .limit(BatchSize)
          .boundedStream(32)
          .compile
          .toList
      )(row => IO.delay(row.getString(MongoFields.Id)))(
        _.traverse_ { row =>
          IO.delay(commandUpdates(row)).flatMap {
            case Left(reason)                                  => run.fail(s"rejected a command: $reason")
            case Right(updates) if convert && updates.nonEmpty =>
              // The field guard excludes rows already converted by another initializer or new strict writer.
              collection
                .updateOne(
                  Filters.and(
                    Filters.eq(MongoFields.Id, row.getString(MongoFields.Id)),
                    Filters.exists("executionAttempts", false)
                  ),
                  Updates.combine(updates*),
                  new UpdateOptions()
                )
                .void
            case Right(updates) if updates.nonEmpty => run.fail("verification found an unconverted command")
            case Right(_)                           =>
              IO.fromEither(
                MongoInterviewWorkflowCommandCodec
                  .decode(row)
                  .leftMap(_ => MigrationError.StepFailed(run.id, "verification rejected the current command contract"))
              ).void
          }
        }
      )
    }

    MongoMigrationStep(
      MigrationIds.InterviewWorkflowAttempts,
      run => requireStringIdentities(run) *> scan(run, convert = true) *> scan(run, convert = false),
      database => {
        val run = MigrationRun(database, MigrationIds.InterviewWorkflowAttempts, None)
        requireStringIdentities(run) *> scan(run, convert = false).as(CompletedProof.Trusted)
      }
    )
  }

  /** Driver document construction remains inside the migration's effect evaluation. */
  private[mongo] def commandUpdates(row: Document): Either[String, List[Bson]] = {
    def text(document: Document, field: String): Either[String, String] = Option(document.get(field))
      .collect {
        case value: String if value.nonEmpty && value.length <= 256 => value
      }
      .toRight(s"invalid $field")
    def counter(field: String): Either[String, Int] = Option(row.get(field))
      .collect {
        case value: java.lang.Integer if value.intValue >= 0 => value.intValue
      }
      .toRight(s"invalid $field")
    for {
      _ <- text(row, MongoFields.Id)
      workflow <- text(row, "workflowId")
      _ <- Either.catchNonFatal(UUID.fromString(workflow)).leftMap(_ => "invalid workflowId")
      _ <- counter(MongoFields.Attempts)
      step <- text(row, "stepId")
      _ <- Either.cond(row.getString("_id") == s"$workflow:$step", (), "inconsistent command identity")
      _ <- Option(row.get("revision"))
        .collect { case value: java.lang.Long if value.longValue >= 0L => () }
        .toRight("invalid revision")
      _ <- Option(row.get("availableAt")).collect { case _: Date => () }.toRight("invalid availableAt")
      _ <- Option(row.get("occurredAt")).collect { case _: Date => () }.toRight("invalid occurredAt")
      command <- Option(row.get("command")).collect { case value: Document => value }.toRight("invalid command")
      kind <- text(command, "kind")
      _ <- Either.cond(
        ExecutableKinds(kind) || kind == "requireRepair",
        (),
        if (kind == "retry") "unsupported retry intent requires explicit repair" else "unsupported command kind"
      )
      _ <- kind match {
        case "reserveCalendar" =>
          text(command, "idempotencyKey").flatMap(key =>
            Either.cond(key == s"$workflow:reserve", (), "invalid reservation key")
          )
        case "releaseCalendar" =>
          text(command, "idempotencyKey").flatMap(key =>
            Either.cond(key == s"$workflow:release", (), "invalid release key")
          )
        case "lookupCalendar" | "lookupStatusCommit" =>
          text(command, "workflowId").flatMap(id => Either.cond(id == workflow, (), "inconsistent lookup workflow"))
        case "commitInterview" =>
          text(command, "expectedStatus").flatMap(status =>
            Either.cond(status == "Accepted", (), "invalid expected hiring status")
          )
        case "requireRepair" => text(command, "reason").void
        case _               => Right(())
      }
      state <- text(row, "commandState")
      _ <- Either.cond(
        Set(
          "Pending",
          "Claimed",
          "Published",
          "Executing",
          "ResultPending",
          "ResultPublished",
          "RepairRequired",
          "Superseded"
        )(state),
        (),
        "invalid commandState"
      )
      _ <-
        if (row.containsKey("result"))
          text(row, "result").flatMap(result =>
            Either.cond(Set("Succeeded", "Rejected", "OutcomeUnknown", "Found", "Absent")(result), (), "invalid result")
          )
        else Right(())
      _ <- Either.cond(
        !Set("ResultPending", "ResultPublished")(state) || row.containsKey("result"),
        (),
        "missing result"
      )
      _ <-
        if (state == "Executing" && kind != "requireRepair") for {
          _ <- text(row, "claimOwner")
          token <- text(row, "claimToken")
          _ <- Either.catchNonFatal(UUID.fromString(token)).leftMap(_ => "invalid execution token")
          _ <- Option(row.get("claimUntil")).collect { case _: Date => () }.toRight("invalid execution lease")
        } yield ()
        else Right(())
      participant <-
        if (kind == "lookupNotification" || kind == "notify") for {
          key <- text(command, "idempotencyKey")
          participant <- InterviewParticipant.values
            .find(value => key == s"$workflow:notify:$value")
            .toRight("invalid notification key")
          _ <-
            if (command.containsKey("participant"))
              text(command, "participant")
                .flatMap(name => Either.cond(name == participant.toString, (), "inconsistent notification participant"))
            else Right(())
        } yield Some(participant)
        else Right(None)
      existing <- if (row.containsKey("executionAttempts")) counter("executionAttempts").map(Some(_)) else Right(None)
      _ <- Either.cond(
        existing.isEmpty || participant.isEmpty || command.containsKey("participant"),
        (),
        "missing notification participant"
      )
    } yield
      if (existing.nonEmpty) Nil
      else {
        val charged = if (kind == "requireRepair") 0 else 1
        val base = List(Updates.set("executionAttempts", charged)) ++ participant.toList.map(value =>
          Updates.set("command.participant", value.toString)
        )
        val marker =
          if (kind == "requireRepair" && state != "Superseded")
            List(
              Updates.set("commandState", "RepairRequired"),
              Updates.unset("claimOwner"),
              Updates.unset("claimToken"),
              Updates.unset("claimUntil")
            )
          else Nil
        val uncertain =
          if (state == "Executing" && !row.containsKey("result") && kind != "requireRepair")
            List(
              Updates.set("result", "OutcomeUnknown"),
              Updates.set("commandState", "ResultPending"),
              Updates.unset("claimOwner"),
              Updates.unset("claimToken"),
              Updates.unset("claimUntil")
            )
          else Nil
        base ++ marker ++ uncertain
      }
  }
}
