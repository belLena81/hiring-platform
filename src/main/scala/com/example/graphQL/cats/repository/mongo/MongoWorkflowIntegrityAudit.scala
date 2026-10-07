package com.example.graphQL.cats.repository.mongo

import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.all.*
import com.example.graphQL.cats.config.AppConfig
import com.example.graphQL.cats.domain.workflow.InterviewTopicPair
import com.mongodb.client.model.{Filters, Sorts, Updates, UpdateOptions}
import org.bson.{BsonValue, Document}
import scala.jdk.CollectionConverters.*

/** Explicit bounded, resumable semantic audit. Invalid rows retain the previous checkpoint for repair and rerun. */
object MongoWorkflowIntegrityAudit extends IOApp {
  private val BatchSize = 500
  private val AuditId = "audit_hiring_workflow_integrity"

  private[mongo] def audit(
      database: mongo4cats.database.MongoDatabase[IO],
      topics: InterviewTopicPair = InterviewTopicPair.Default
  ): IO[Unit] = {
    val ledger = Mongo4catsCollections.documents(database, MongoCollections.HiringMigrationLedger)
    val collections = List(
      MongoCollections.InterviewWorkflowCommands,
      MongoCollections.EventOutbox,
      MongoCollections.InterviewSubjectCleanup
    )
    def scan(collectionName: String, after: Option[BsonValue]): IO[Unit] = {
      val collection = Mongo4catsCollections.documents(database, collectionName)
      collection
        .flatMap(
          _.find(MongoInterviewCleanupSweepCodec.afterFilter(after))
            .sort(Sorts.ascending("_id"))
            .limit(BatchSize)
            .boundedStream(32)
            .compile
            .toList
        )
        .flatMap { rows =>
          val validate =
            if (collectionName == MongoCollections.InterviewWorkflowCommands)
              rows.traverse_(row =>
                IO.fromEither(
                  MongoInterviewWorkflowCommandCodec
                    .decode(row)
                    .left
                    .map(_ => new IllegalStateException("Integrity audit rejected a command"))
                ).void
              )
            else if (collectionName == MongoCollections.InterviewSubjectCleanup)
              rows.traverse_(row =>
                IO.fromEither(
                  MongoInterviewCleanupCodec
                    .decodeCurrent(row, topics)
                    .leftMap(_ => new IllegalStateException("Integrity audit rejected cleanup evidence"))
                ).void
              )
            else {
              val invalid = new Document("$nor", List(MongoHiringValidators.outboxValidator).asJava)
              collection
                .flatMap(_.find(Filters.and(Filters.in("_id", rows.map(_.get("_id"))*), invalid)).limit(1).first)
                .flatMap {
                  case None    => IO.unit
                  case Some(_) => IO.raiseError(new IllegalStateException("Integrity audit rejected an outbox row"))
                }
            }
          validate *> rows.lastOption.traverse_(row =>
            ledger
              .flatMap(
                _.updateOne(
                  Filters.eq("_id", AuditId),
                  Updates
                    .combine(Updates.set("collection", collectionName), Updates.set("lastId", row.get("_id"))),
                  new UpdateOptions().upsert(true)
                )
              )
              .void
          ) *>
            (if (rows.size == BatchSize)
               IO.fromEither(
                 rows.lastOption
                   .traverse(MongoInterviewCleanupSweepCodec.identity)
                   .leftMap(_ => new IllegalStateException("Invalid integrity audit identity"))
               ).flatMap(scan(collectionName, _))
             else IO.unit)
        }
    }
    for {
      previous <- ledger.flatMap(_.find(Filters.eq("_id", AuditId)).first)
      resume = previous.filter(row => row.getString("state") != "Complete")
      selected = resume.flatMap(row => Option(row.getString("collection")))
      _ <- IO.raiseUnless(selected.forall(collections.contains))(
        new IllegalStateException("Unsupported audit checkpoint")
      )
      _ <- ledger
        .flatMap(
          _.updateOne(
            Filters.eq("_id", AuditId),
            Updates.combine(Updates.set("state", "Running"), Updates.setOnInsert("_id", AuditId)),
            new UpdateOptions().upsert(true)
          )
        )
        .void
      _ <- collections.dropWhile(name => selected.exists(_ != name)).traverse_ { name =>
        val checkpoint = Option.when(selected.contains(name))(resume.flatMap(row => Option(row.get("lastId")))).flatten
        // Persist collection switches before reading; a restart must not reuse the previous collection's cursor.
        val setCollection =
          if (selected.contains(name)) IO.unit
          else
            ledger
              .flatMap(
                _.updateOne(
                  Filters.eq("_id", AuditId),
                  Updates.combine(Updates.set("collection", name), Updates.unset("lastId"))
                )
              )
              .void
        setCollection *> IO
          .fromEither(
            checkpoint
              .traverse(value => MongoInterviewCleanupSweepCodec.identity(new Document("_id", value)))
              .leftMap(_ => new IllegalStateException("Invalid integrity audit checkpoint"))
          )
          .flatMap(scan(name, _))
      }
      _ <- ledger
        .flatMap(
          _.updateOne(
            Filters.eq("_id", AuditId),
            Updates.combine(Updates.set("state", "Complete"), Updates.unset("lastId"), Updates.unset("collection"))
          )
        )
        .void
    } yield ()
  }

  def run(args: List[String]): IO[ExitCode] =
    if (args.nonEmpty) IO.println("Usage: MongoWorkflowIntegrityAudit (configured local MongoDB)").as(ExitCode.Error)
    else
      AppConfig.load.flatMap {
        case Left(_)       => IO.println("Invalid application configuration").as(ExitCode.Error)
        case Right(config) =>
          MongoDatabaseProbe
            .clientResource(config.mongoUri)
            .use { client =>
              client.getDatabase(config.mongoDatabase).flatMap(audit(_, config.kafka.interview.topics))
            }
            .attempt
            .flatMap {
              case Right(_) => IO.println("Hiring workflow integrity audit completed").as(ExitCode.Success)
              case Left(_)  =>
                IO.println("Integrity audit failed; checkpoint retained for repair and rerun").as(ExitCode.Error)
            }
      }
}
