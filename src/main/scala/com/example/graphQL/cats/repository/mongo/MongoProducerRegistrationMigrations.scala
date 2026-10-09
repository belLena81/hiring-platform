package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.mongodb.client.model.{Filters, Sorts, UpdateOptions, Updates}
import org.bson.Document
import java.util.Date
import scala.jdk.CollectionConverters.*

/** Maintenance cutover: all publishers and erasure workers must be stopped. Rows are replay-safe checkpoints. */
private[mongo] object MongoProducerRegistrationMigrations {
  private val Id: MigrationId = MigrationIds.AttributableProducerRegistrations

  /** Ledger literal kept as `String` for existing callers; the typed identity is
    * `MigrationIds.AttributableProducerRegistrations`.
    */
  val MigrationId: String = Id.value
  private val BatchSize = 500
  private val LegacyAttributionFields = Vector("transactionalIds", MongoFields.InterviewTransactionalIds)

  def registrationValidator: Document = {
    val properties = new Document(MongoFields.Id, new Document("bsonType", "string"))
      .append("subjectId", new Document("bsonType", "string"))
      .append("transactionalId", new Document("bsonType", "string"))
      .append(MongoFields.Kind, new Document("enum", Vector("Operational", "Interview").asJava))
      .append(MongoFields.State, new Document("enum", Vector("Active", "Fenced").asJava))
      .append("registeredAt", new Document("bsonType", "date"))
    val schema = new Document("bsonType", "object")
      .append(
        "required",
        Vector(
          MongoFields.Id,
          "subjectId",
          "transactionalId",
          MongoFields.Kind,
          MongoFields.State,
          "registeredAt"
        ).asJava
      )
      .append("properties", properties)
    val active = new Document(MongoFields.State, "Active").append(MongoFields.ExpiresAt, new Document("$exists", false))
    val fenced = new Document(MongoFields.State, "Fenced")
      .append("fencedAt", new Document("$type", "date"))
      .append(MongoFields.ExpiresAt, new Document("$type", "date"))
    new Document(
      "$and",
      Vector(new Document("$jsonSchema", schema), new Document("$or", Vector(active, fenced).asJava)).asJava
    )
  }

  /** The fence store forbids the retired attribution arrays after their transfer. */
  def fenceValidator: Document =
    new Document(
      "$and",
      LegacyAttributionFields.map(field => new Document(field, new Document("$exists", false))).asJava
    )

  private val validators: List[(String, Document)] = List(
    MongoProducerRegistrations.Collection -> registrationValidator,
    MongoCollections.OutboxSubjectFences -> fenceValidator
  )

  private def verifyDefinitions(database: MongoHiringSetup.SetupDatabase): IO[CompletedProof] =
    MongoHiringValidators.assertStrictValidators(database, validators).as(CompletedProof.Trusted)

  private def cutover(run: MigrationRun): IO[Unit] = {
    val database = run.database
    val registrations = database.getCollection(MongoProducerRegistrations.Collection)
    def register(subject: String, ids: Vector[String], kind: String, now: Date): IO[Unit] =
      ids.traverse_ { id =>
        registrations
          .updateOne(
            Filters.eq(MongoFields.Id, s"$subject:$id"),
            Updates.combine(
              Updates.setOnInsert("subjectId", subject),
              Updates.setOnInsert("transactionalId", id),
              Updates.setOnInsert(MongoFields.Kind, kind),
              Updates.setOnInsert(MongoFields.State, "Active"),
              Updates.setOnInsert("registeredAt", now)
            ),
            new UpdateOptions().upsert(true)
          )
          .flatMap(result => IO.raiseUnless(result.wasAcknowledged())(MigrationError.UnacknowledgedWrite(run.id)))
      }
    def ids(row: Document, field: String): IO[Vector[String]] =
      Option(row.get(field)) match {
        case None                            => IO.pure(Vector.empty)
        case Some(values: java.util.List[?]) =>
          IO.fromEither(
            values.asScala.toVector
              .traverse {
                case value: String if value.nonEmpty => Right(value)
                case _ => Left(MigrationError.StepFailed(run.id, "invalid producer registration attribution"))
              }
              .map(_.distinct)
          )
        case _ => run.fail("invalid producer registration attribution array")
      }
    def migrate(
        collection: String,
        fields: Vector[(String, String)],
        extra: Document => List[org.bson.conversions.Bson]
    ): IO[Unit] = {
      val source = database.getCollection(collection)
      val legacy =
        if (collection == MongoCollections.InterviewSubjectCleanup)
          Filters.exists(MongoFields.ProducerRegistry, false)
        else Filters.or(fields.map { case (field, _) => Filters.exists(field, true) }.asJava)
      def batch: IO[Unit] =
        source.find(legacy).sort(Sorts.ascending(MongoFields.Id)).limit(BatchSize).all.flatMap { rows =>
          rows.toList.traverse_ { row =>
            val subject = Option(row.get(MongoFields.Id)).collect { case value: String => value }
            subject.fold(run.fail[Unit]("invalid producer subject identity")) { id =>
              for {
                now <- IO.realTimeInstant.map(Date.from)
                _ <- fields.traverse_ { case (field, kind) =>
                  ids(row, field).flatMap { values =>
                    val checked =
                      if (kind == "Interview")
                        com.example.graphQL.cats.domain.workflow.InterviewSubjectCleanup
                          .validateProducerIds(values)
                          .leftMap(_ => MigrationError.StepFailed(run.id, "invalid interview producer attribution"))
                      else Right(values)
                    IO.fromEither(checked).flatMap(register(id, _, kind, now))
                  }
                }
                result <- source.updateOne(
                  Filters.eq(MongoFields.Id, id),
                  Updates.combine((fields.map { case (field, _) =>
                    if (collection == MongoCollections.InterviewSubjectCleanup)
                      Updates.set(field, Vector.empty[String].asJava)
                    else Updates.unset(field)
                  }.toList ++ extra(row)).asJava)
                )
                _ <- IO.raiseUnless(result.wasAcknowledged())(MigrationError.UnacknowledgedWrite(run.id))
              } yield ()
            }
          } *> (if (rows.isEmpty) IO.unit else batch)
        }
      batch
    }

    def verify(after: Option[String]): IO[Unit] =
      registrations
        .find(after.fold(Filters.empty())(id => Filters.gt(MongoFields.Id, id)))
        .sort(Sorts.ascending(MongoFields.Id))
        .limit(BatchSize)
        .all
        .flatMap { rows =>
          rows.toList.traverse_ { row =>
            val strings = Vector(MongoFields.Id, "subjectId", "transactionalId", MongoFields.Kind, MongoFields.State)
              .forall(field => Option(row.get(field)).exists { case value: String => value.nonEmpty; case _ => false })
            val active =
              Option(row.get(MongoFields.State)).contains("Active") && !row.containsKey(MongoFields.ExpiresAt)
            val fenced =
              Option(row.get(MongoFields.State)).contains("Fenced") &&
                Option(row.get("fencedAt")).exists(_.isInstanceOf[Date]) &&
                Option(row.get(MongoFields.ExpiresAt)).exists(_.isInstanceOf[Date])
            if (
              strings && Option(row.get("registeredAt")).exists(_.isInstanceOf[Date]) &&
              Set("Operational", "Interview").contains(row.getString(MongoFields.Kind)) && (active || fenced)
            ) IO.unit
            else run.fail("invalid producer registry evidence")
          } *> rows.lastOption.fold(IO.unit)(row => verify(Some(row.getString(MongoFields.Id))))
        }

    database.ensureCollection(MongoProducerRegistrations.Collection) *>
      MongoHiringValidators.install(database, MongoCollections.OutboxSubjectFences, fenceValidator) *>
      migrate(
        MongoCollections.OutboxSubjectFences,
        Vector("transactionalIds" -> "Operational", MongoFields.InterviewTransactionalIds -> "Interview"),
        _ => Nil
      ) *>
      migrate(
        MongoCollections.AnalyticsErasureRequests,
        Vector("transactionalIds" -> "Operational"),
        row => {
          val common = List(Updates.set(MongoFields.ProducerRegistry, true))
          if (row.getString(MongoFields.State) == "Complete") common
          else
            common ++ List(
              Updates.set(MongoFields.State, "Pending"),
              Updates.set("phase", "Requested"),
              Updates.unset(MongoFields.LeaseToken),
              Updates.unset(MongoFields.LeaseUntil),
              Updates.unset("resumeAfter")
            )
        }
      ) *>
      // Existing empty cleanup arrays are bounded; populated snapshots are first copied then reset for fresh proof.
      migrate(
        MongoCollections.InterviewSubjectCleanup,
        Vector(MongoFields.InterviewTransactionalIds -> "Interview"),
        _ =>
          List(
            Updates.set(MongoFields.ProducerRegistry, true),
            Updates.set(MongoFields.State, "Pending"),
            Updates.inc(MongoFields.Revision, Long.box(1L)),
            Updates.unset("barriers"),
            Updates.unset(MongoFields.CompletedAt)
          )
      ) *>
      database
        .getCollection(MongoCollections.InterviewSubjectCleanup)
        .updateMany(
          Filters.exists(MongoFields.InterviewTransactionalIds, false),
          Updates.set(MongoFields.InterviewTransactionalIds, Vector.empty[String].asJava)
        )
        .void *>
      database
        .getCollection(MongoCollections.AnalyticsErasureRequests)
        .updateMany(
          Filters.exists(MongoFields.ProducerRegistry, false),
          Updates.set(MongoFields.ProducerRegistry, true)
        )
        .void *>
      verify(None) *>
      database
        .runCommand(
          MongoHiringValidators.strictValidation(MongoProducerRegistrations.Collection, registrationValidator)
        )
        .void
  }

  val step: MongoMigrationStep = MongoMigrationStep(Id, cutover, verifyDefinitions)

  def initialize(database: MongoHiringSetup.SetupDatabase): IO[Unit] = MongoMigrationRunner.run(database, step)
}
