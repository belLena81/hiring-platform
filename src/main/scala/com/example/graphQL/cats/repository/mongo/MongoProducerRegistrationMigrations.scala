package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import fs2.interop.reactivestreams.*
import com.mongodb.MongoCommandException
import com.mongodb.client.model.{Filters, Sorts, Updates, UpdateOptions}
import org.bson.Document
import java.util.Date
import scala.jdk.CollectionConverters.*

/** Maintenance cutover: all publishers and erasure workers must be stopped. Rows are replay-safe checkpoints. */
private[mongo] object MongoProducerRegistrationMigrations {
  val MigrationId = "012_attributable_producer_registrations"
  private val BatchSize = 500

  def initialize(database: MongoHiringSetup.SetupDatabase): IO[Unit] = {
    val registrations = database.getCollection(MongoProducerRegistrations.Collection)
    val ledger = database.getCollection(MongoCollections.HiringMigrationLedger)
    val marker = Filters.eq("_id", MigrationId)
    def register(subject: String, ids: Vector[String], kind: String, now: Date): IO[Unit] =
      ids.traverse_ { id =>
        registrations
          .updateOne(
            Filters.eq("_id", s"$subject:$id"),
            Updates.combine(
              Updates.setOnInsert("subjectId", subject),
              Updates.setOnInsert("transactionalId", id),
              Updates.setOnInsert("kind", kind),
              Updates.setOnInsert("state", "Active"),
              Updates.setOnInsert("registeredAt", now)
            ),
            new UpdateOptions().upsert(true)
          )
          .flatMap(result =>
            if (result.wasAcknowledged()) IO.unit
            else IO.raiseError(new IllegalStateException("Producer registration migration write failed"))
          )
      }
    def ids(row: Document, field: String): IO[Vector[String]] =
      Option(row.get(field)) match {
        case None                            => IO.pure(Vector.empty)
        case Some(values: java.util.List[?]) =>
          IO.fromEither(
            values.asScala.toVector
              .traverse {
                case value: String if value.nonEmpty => Right(value)
                case _ => Left(new IllegalStateException("Invalid producer registration attribution"))
              }
              .map(_.distinct)
          )
        case _ => IO.raiseError(new IllegalStateException("Invalid producer registration attribution array"))
      }
    def migrate(
        collection: String,
        fields: Vector[(String, String)],
        extra: Document => List[org.bson.conversions.Bson]
    ): IO[Unit] = {
      val source = database.getCollection(collection)
      val legacy =
        if (collection == MongoCollections.InterviewSubjectCleanup) Filters.exists("producerRegistry", false)
        else Filters.or(fields.map { case (field, _) => Filters.exists(field, true) }.asJava)
      def batch: IO[Unit] = source.find(legacy).sort(Sorts.ascending("_id")).limit(BatchSize).all.flatMap { rows =>
        rows.toList.traverse_ { row =>
          val subject = Option(row.get("_id")).collect { case value: String => value }
          subject.fold(IO.raiseError[Unit](new IllegalStateException("Invalid producer subject identity"))) { id =>
            for {
              now <- IO.realTimeInstant.map(Date.from)
              _ <- fields.traverse_ { case (field, kind) =>
                ids(row, field).flatMap { values =>
                  val checked =
                    if (kind == "Interview")
                      com.example.graphQL.cats.domain.workflow.InterviewSubjectCleanup
                        .validateProducerIds(values)
                        .leftMap(_ => new IllegalStateException("Invalid interview producer attribution"))
                    else Right(values)
                  IO.fromEither(checked).flatMap(register(id, _, kind, now))
                }
              }
              result <- source.updateOne(
                Filters.eq("_id", id),
                Updates.combine((fields.map { case (field, _) =>
                  if (collection == MongoCollections.InterviewSubjectCleanup)
                    Updates.set(field, Vector.empty[String].asJava)
                  else Updates.unset(field)
                }.toList ++ extra(row)).asJava)
              )
              _ <-
                if (result.wasAcknowledged()) IO.unit
                else IO.raiseError(new IllegalStateException("Producer attribution cutover write failed"))
            } yield ()
          }
        } *> (if (rows.isEmpty) IO.unit else batch)
      }
      batch
    }
    def validation(collection: String, forbidden: Vector[String]): IO[Unit] =
      database.createCollection(collection).handleErrorWith {
        case error: MongoCommandException if error.getErrorCode == 48 => IO.unit
        case error                                                    => IO.raiseError(error)
      } *> database.runCommand(
        new Document("collMod", collection)
          .append(
            "validator",
            new Document("$and", forbidden.map(field => new Document(field, new Document("$exists", false))).asJava)
          )
          .append("validationLevel", "strict")
          .append("validationAction", "error")
      )

    def registrationValidator: Document = {
      val properties = new Document("_id", new Document("bsonType", "string"))
        .append("subjectId", new Document("bsonType", "string"))
        .append("transactionalId", new Document("bsonType", "string"))
        .append("kind", new Document("enum", Vector("Operational", "Interview").asJava))
        .append("state", new Document("enum", Vector("Active", "Fenced").asJava))
        .append("registeredAt", new Document("bsonType", "date"))
      val schema = new Document("bsonType", "object")
        .append("required", Vector("_id", "subjectId", "transactionalId", "kind", "state", "registeredAt").asJava)
        .append("properties", properties)
      val active = new Document("state", "Active").append("expiresAt", new Document("$exists", false))
      val fenced = new Document("state", "Fenced")
        .append("fencedAt", new Document("$type", "date"))
        .append("expiresAt", new Document("$type", "date"))
      new Document(
        "$and",
        Vector(new Document("$jsonSchema", schema), new Document("$or", Vector(active, fenced).asJava)).asJava
      )
    }
    def registryValidation: IO[Unit] = database.runCommand(
      new Document("collMod", MongoProducerRegistrations.Collection)
        .append("validator", registrationValidator)
        .append("validationLevel", "strict")
        .append("validationAction", "error")
    )
    def verifyDefinitions: IO[Unit] = {
      val names = Vector(MongoProducerRegistrations.Collection, MongoCollections.OutboxSubjectFences)
      val command =
        new Document("listCollections", 1).append("filter", new Document("name", new Document("$in", names.asJava)))
      IO.delay(database.underlying.underlying.runCommand(command, classOf[Document]))
        .flatMap(_.toStreamBuffered[IO](1).compile.lastOrError)
        .flatMap { result =>
          val entries = Option(result.get("cursor", classOf[Document])).toVector
            .flatMap(cursor => Option(cursor.getList("firstBatch", classOf[Document])).toVector.flatMap(_.asScala))
          val current = names.forall { name =>
            entries.find(_.getString("name") == name).exists { entry =>
              val expected =
                if (name == MongoProducerRegistrations.Collection) registrationValidator
                else
                  new Document(
                    "$and",
                    Vector("transactionalIds", "interviewTransactionalIds")
                      .map(field => new Document(field, new Document("$exists", false)))
                      .asJava
                  )
              Option(entry.get("options", classOf[Document])).exists(options =>
                options.getString("validationLevel") == "strict" &&
                  options.getString("validationAction") == "error" && Option(
                    options.get("validator", classOf[Document])
                  ).contains(expected)
              )
            }
          }
          if (current) IO.unit
          else
            IO.raiseError(
              new IllegalStateException("Producer registry validator changed; explicit maintenance repair required")
            )
        }
    }

    def verify(after: Option[String]): IO[Unit] =
      registrations
        .find(after.fold(Filters.empty())(id => Filters.gt("_id", id)))
        .sort(Sorts.ascending("_id"))
        .limit(BatchSize)
        .all
        .flatMap { rows =>
          rows.toList.traverse_ { row =>
            val strings = Vector("_id", "subjectId", "transactionalId", "kind", "state")
              .forall(field => Option(row.get(field)).exists { case value: String => value.nonEmpty; case _ => false })
            val active = Option(row.get("state")).contains("Active") && !row.containsKey("expiresAt")
            val fenced =
              Option(row.get("state")).contains("Fenced") && Option(row.get("fencedAt")).exists(_.isInstanceOf[Date]) &&
                Option(row.get("expiresAt")).exists(_.isInstanceOf[Date])
            if (
              strings && Option(row.get("registeredAt")).exists(_.isInstanceOf[Date]) &&
              Set("Operational", "Interview").contains(row.getString("kind")) && (active || fenced)
            ) IO.unit
            else IO.raiseError(new IllegalStateException("Invalid producer registry evidence"))
          } *> rows.lastOption.fold(IO.unit)(row => verify(Some(row.getString("_id"))))
        }

    ledger.find(marker).first.flatMap {
      case Some(row)
          if !Option(row.get("version"))
            .collect { case value: java.lang.Long if value.longValue() == 1L => () }
            .contains(()) =>
        IO.raiseError(new IllegalStateException("Unsupported producer registration migration version"))
      case Some(row) if row.getString("state") == "Complete" =>
        verifyDefinitions
      case Some(row) if row.getString("state") != "Running" =>
        IO.raiseError(new IllegalStateException("Invalid producer registration migration state"))
      case _ =>
        database.createCollection(MongoProducerRegistrations.Collection).handleErrorWith {
          case error: MongoCommandException if error.getErrorCode == 48 => IO.unit
          case error                                                    => IO.raiseError(error)
        } *>
          ledger
            .updateOne(
              marker,
              Updates.combine(Updates.set("version", Long.box(1L)), Updates.set("state", "Running")),
              new UpdateOptions().upsert(true)
            )
            .void *>
          validation(MongoCollections.OutboxSubjectFences, Vector("transactionalIds", "interviewTransactionalIds")) *>
          migrate(
            MongoCollections.OutboxSubjectFences,
            Vector("transactionalIds" -> "Operational", "interviewTransactionalIds" -> "Interview"),
            _ => Nil
          ) *>
          migrate(
            MongoCollections.AnalyticsErasureRequests,
            Vector("transactionalIds" -> "Operational"),
            row => {
              val common = List(Updates.set("producerRegistry", true))
              if (row.getString("state") == "Complete") common
              else
                common ++ List(
                  Updates.set("state", "Pending"),
                  Updates.set("phase", "Requested"),
                  Updates.unset("leaseToken"),
                  Updates.unset("leaseUntil"),
                  Updates.unset("resumeAfter")
                )
            }
          ) *>
          // Existing empty cleanup arrays are bounded; populated snapshots are first copied then reset for fresh proof.
          migrate(
            MongoCollections.InterviewSubjectCleanup,
            Vector("interviewTransactionalIds" -> "Interview"),
            _ =>
              List(
                Updates.set("producerRegistry", true),
                Updates.set("state", "Pending"),
                Updates.inc("revision", Long.box(1L)),
                Updates.unset("barriers"),
                Updates.unset("completedAt")
              )
          ) *>
          database
            .getCollection(MongoCollections.InterviewSubjectCleanup)
            .updateMany(
              Filters.exists("interviewTransactionalIds", false),
              Updates.set("interviewTransactionalIds", Vector.empty[String].asJava)
            )
            .void *>
          database
            .getCollection(MongoCollections.AnalyticsErasureRequests)
            .updateMany(Filters.exists("producerRegistry", false), Updates.set("producerRegistry", true))
            .void *>
          verify(None) *> registryValidation *>
          ledger
            .updateOne(marker, Updates.combine(Updates.set("state", "Complete")), new UpdateOptions().upsert(true))
            .void
    }
  }
}
