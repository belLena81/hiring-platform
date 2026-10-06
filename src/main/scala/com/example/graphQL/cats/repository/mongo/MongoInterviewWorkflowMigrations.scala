package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.workflow.InterviewParticipant
import com.mongodb.MongoWriteException
import com.mongodb.client.model.{CountOptions, Filters, Indexes, UpdateOptions, Updates}
import org.bson.Document
import org.bson.conversions.Bson
import java.util.{UUID, Date}

/** Maintenance cutover preserves identities, receipts and conservatively charged execution budgets. */
private[mongo] object MongoInterviewWorkflowMigrations {
  private val MigrationId = "010_interview_workflow_attempts"
  private val BatchSize = 500
  private val ExecutableKinds = Set(
    "reserveCalendar",
    "lookupCalendar",
    "commitInterview",
    "lookupStatusCommit",
    "releaseCalendar",
    "notify",
    "lookupNotification"
  )

  def initialize(database: MongoHiringSetup.SetupDatabase): IO[Unit] = {
    val ledger = database.getCollection(MongoCollections.HiringMigrationLedger)
    val collection = database.getCollection(MongoCollections.InterviewWorkflowCommands)
    val identity = Filters.eq(MongoFields.Id, MigrationId)

    def scan(after: Option[String], convert: Boolean): IO[Unit] = {
      val filter = after.fold[Bson](new Document())(id => Filters.gt(MongoFields.Id, id))
      collection
        .find(filter)
        .sort(Indexes.ascending(MongoFields.Id))
        .limit(BatchSize)
        .boundedStream(32)
        .compile
        .toList
        .flatMap { batch =>
          batch.traverse_ { row =>
            IO.delay(commandUpdates(row)).flatMap {
              case Left(reason) =>
                IO.raiseError(new IllegalStateException(s"Interview workflow migration rejected a command: $reason"))
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
              case Right(updates) if !convert && updates.nonEmpty =>
                IO.raiseError(
                  new IllegalStateException("Interview workflow migration verification found an unconverted command")
                )
              case Right(_) =>
                IO.fromEither(
                  MongoInterviewWorkflowCommandCodec
                    .decode(row)
                    .leftMap(_ =>
                      new IllegalStateException(
                        "Interview workflow migration verification rejected the current command contract"
                      )
                    )
                ).void
            }
          } *> (if (batch.size == BatchSize) scan(batch.lastOption.map(_.getString(MongoFields.Id)), convert)
                else IO.unit)
        }
    }

    def run: IO[Unit] =
      ledger
        .updateOne(
          identity,
          Updates.combine(
            Updates.setOnInsert(MongoFields.Id, MigrationId),
            Updates.set(MongoFields.Version, 1L),
            Updates.set(MongoFields.State, "Running")
          ),
          new UpdateOptions().upsert(true)
        )
        .void
        .handleErrorWith {
          case error: MongoWriteException if error.getError.getCode == 11000 => IO.unit
          case error                                                         => IO.raiseError(error)
        } *> scan(None, true) *> scan(None, false) *>
        ledger
          .updateOne(
            identity,
            Updates.combine(Updates.set(MongoFields.Version, 1L), Updates.set(MongoFields.State, "Complete")),
            new UpdateOptions()
          )
          .void

    collection
      .count(Filters.not(Filters.`type`(MongoFields.Id, org.bson.BsonType.STRING)), new CountOptions())
      .flatMap {
        case 0L => IO.unit
        case _  =>
          IO.raiseError(new IllegalStateException("Interview workflow migration found invalid command identity type"))
      } *> ledger.find(identity).first.flatMap {
      case Some(row)
          if !Option(row.get(MongoFields.Version))
            .collect {
              case value: java.lang.Long if value.longValue == 1L => ()
            }
            .contains(()) =>
        IO.raiseError(new IllegalStateException("Unsupported interview workflow attempt migration version"))
      case Some(row) if Option(row.get(MongoFields.State)).contains("Complete") => scan(None, false)
      case Some(row) if Option(row.get(MongoFields.State)).contains("Running")  => run
      case Some(_) => IO.raiseError(new IllegalStateException("Invalid interview workflow attempt migration state"))
      case None    => run
    }
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
