package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.workflow.InterviewSubjectCleanup
import com.mongodb.MongoWriteException
import com.mongodb.client.model.{Filters, Sorts, Updates, UpdateOptions, CountOptions}
import org.bson.Document
import java.util.{Date, UUID}
import scala.jdk.CollectionConverters.*

/** Offline forward repair: old nontransactional publishers must be stopped and their credentials revoked first. Legacy
  * completion/barriers are re-proved; each atomic row update is its own restart checkpoint.
  */
private[mongo] object MongoInterviewCleanupMigrations {
  val MigrationId = "011_interview_publication_fencing"
  private val BatchSize = 128
  private val ProducerIdsField = "interviewTransactionalIds"

  def initialize(database: MongoHiringSetup.SetupDatabase): IO[Unit] = {
    val queue = database.getCollection(MongoCollections.InterviewSubjectCleanup)
    val fences = database.getCollection(MongoCollections.OutboxSubjectFences)
    val ledger = database.getCollection(MongoCollections.HiringMigrationLedger)
    val identity = Filters.eq(MongoFields.Id, MigrationId)
    val legacy = Filters.or(Filters.exists("revision", false), Filters.exists(ProducerIdsField, false))

    def invalid(detail: String): IO[Nothing] = IO.raiseError(new IllegalStateException(detail))

    def snapshot(document: Option[Document]): IO[Vector[String]] = {
      val parsed = document match {
        case None                                                => Right(Vector.empty[String])
        case Some(value) if !value.containsKey(ProducerIdsField) => Right(Vector.empty[String])
        case Some(value)                                         =>
          Option(value.get(ProducerIdsField)) match {
            case Some(values: java.util.List[?]) =>
              values.asScala.toVector.traverse {
                case id: String => Right(id)
                case _          => Left("Invalid interview producer snapshot")
              }
            case _ => Left("Invalid interview producer snapshot")
          }
      }
      IO.fromEither(
        parsed
          .flatMap(values =>
            InterviewSubjectCleanup.validateProducerIds(values).leftMap(_ => "Invalid interview producer identity")
          )
          .leftMap(message => new IllegalStateException(message))
      )
    }

    def migrate(row: Document): IO[Unit] = {
      val subject = Option(row.get(MongoFields.Id)).collect { case value: String => value }
      val requested = Option(row.get("requestedAt")).collect { case value: Date => value }
      val knownLegacyState = Option(row.get("state"))
        .collect { case value: String => value }
        .exists(Set("Pending", "Purged", "Cleaned", "Complete"))
      (subject, requested) match {
        case (Some(id), Some(_))
            if knownLegacyState && scala.util.Try(UUID.fromString(id)).toOption.exists(_.toString == id) =>
          for {
            fence <- fences.find(Filters.eq(MongoFields.Id, id)).first
            ids <- snapshot(fence)
            update <- queue.updateOne(
              Filters.and(Filters.eq(MongoFields.Id, id), legacy),
              Updates.combine(
                Updates.set("revision", Long.box(0L)),
                Updates.set("state", "Pending"),
                Updates.set(ProducerIdsField, ids.asJava),
                Updates.unset("barriers"),
                Updates.unset("completedAt"),
                Updates.unset("purgedAt")
              )
            )
            _ <-
              if (update.wasAcknowledged() && update.getMatchedCount <= 1L) IO.unit
              else invalid("Interview cleanup migration write was not acknowledged")
          } yield ()
        case _ => invalid("Interview cleanup migration found invalid legacy state, subject or request time")
      }
    }

    def batch: IO[Unit] =
      queue.find(legacy).sort(Sorts.ascending(MongoFields.Id)).limit(BatchSize).all.flatMap { rows =>
        if (rows.isEmpty) IO.unit else rows.toList.traverse_(migrate) *> batch
      }

    def verifyRows(after: Option[String]): IO[Unit] = {
      val predicate = after.fold(Filters.empty())(id => Filters.gt(MongoFields.Id, id))
      queue.find(predicate).sort(Sorts.ascending(MongoFields.Id)).limit(BatchSize).all.flatMap { rows =>
        rows.toVector
          .traverse(row =>
            IO.fromEither(
              MongoInterviewCleanupCodec
                .decode(row)
                .leftMap(_ => new IllegalStateException("Interview cleanup migration found invalid typed evidence"))
            )
          )
          .flatMap { decoded =>
            decoded.lastOption.fold(IO.unit)(last => verifyRows(Some(last.subjectId.value.toString)))
          }
      }
    }

    def verify: IO[Unit] =
      queue.count(legacy, new CountOptions()).flatMap { remaining =>
        if (remaining == 0L) IO.unit else invalid("Interview cleanup migration left legacy evidence")
      } *> queue
        .count(Filters.not(Filters.`type`(MongoFields.Id, org.bson.BsonType.STRING)), new CountOptions())
        .flatMap {
          case 0L => verifyRows(None)
          case _  => invalid("Interview cleanup migration found invalid subject identity type")
        }

    def run: IO[Unit] =
      ledger
        .updateOne(
          identity,
          Updates.combine(
            Updates.setOnInsert(MongoFields.Id, MigrationId),
            Updates.set(MongoFields.Version, Long.box(1L)),
            Updates.set(MongoFields.State, "Running")
          ),
          new UpdateOptions().upsert(true)
        )
        .void
        .handleErrorWith {
          case error: MongoWriteException if error.getError.getCode == 11000 => IO.unit
          case error                                                         => IO.raiseError(error)
        } *> batch *> verify *> ledger.updateOne(identity, Updates.set(MongoFields.State, "Complete")).void

    ledger.find(identity).first.flatMap {
      case Some(row) if !Option(row.get(MongoFields.Version)).collect {
            case value: java.lang.Long if value.longValue() == 1L => ()
          }.isDefined =>
        invalid("Unsupported interview cleanup migration version")
      case Some(row) if Option(row.get(MongoFields.State)).contains("Complete") => verify
      case Some(row) if Option(row.get(MongoFields.State)).contains("Running")  => run
      case Some(_) => invalid("Invalid interview cleanup migration state")
      case None    => run
    }
  }
}
