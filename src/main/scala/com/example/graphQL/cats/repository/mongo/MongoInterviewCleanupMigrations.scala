package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.workflow.InterviewSubjectCleanup
import com.mongodb.client.model.{CountOptions, Filters, Sorts, Updates}
import org.bson.Document
import java.util.{Date, UUID}
import scala.jdk.CollectionConverters.*

/** Offline forward repair: old nontransactional publishers must be stopped and their credentials revoked first. Legacy
  * completion/barriers are re-proved; each atomic row update is its own restart checkpoint.
  */
private[mongo] object MongoInterviewCleanupMigrations {
  private val Id: MigrationId = MigrationIds.InterviewPublicationFencing

  /** Ledger literal kept as `String` for existing callers; the typed identity is
    * `MigrationIds.InterviewPublicationFencing`.
    */
  val MigrationId: String = Id.value
  private val BatchSize = 128
  private val ProducerIdsField = MongoFields.InterviewTransactionalIds
  private val legacy = Filters.or(Filters.exists(MongoFields.Revision, false), Filters.exists(ProducerIdsField, false))

  private def snapshot(run: MigrationRun, document: Option[Document]): IO[Vector[String]] = {
    val parsed = document match {
      case None                                                => Right(Vector.empty[String])
      case Some(value) if !value.containsKey(ProducerIdsField) => Right(Vector.empty[String])
      case Some(value)                                         =>
        Option(value.get(ProducerIdsField)) match {
          case Some(values: java.util.List[?]) =>
            values.asScala.toVector.traverse {
              case id: String => Right(id)
              case _          => Left("invalid interview producer snapshot")
            }
          case _ => Left("invalid interview producer snapshot")
        }
    }
    IO.fromEither(
      parsed
        .flatMap(values =>
          InterviewSubjectCleanup.validateProducerIds(values).leftMap(_ => "invalid interview producer identity")
        )
        .leftMap(MigrationError.StepFailed(run.id, _))
    )
  }

  private def migrate(run: MigrationRun, row: Document): IO[Unit] = {
    val queue = run.database.getCollection(MongoCollections.InterviewSubjectCleanup)
    val fences = run.database.getCollection(MongoCollections.OutboxSubjectFences)
    val subject = Option(row.get(MongoFields.Id)).collect { case value: String => value }
    val requested = Option(row.get(MongoFields.RequestedAt)).collect { case value: Date => value }
    val knownLegacyState = Option(row.get(MongoFields.State))
      .collect { case value: String => value }
      .exists(Set("Pending", "Purged", "Cleaned", "Complete"))
    (subject, requested) match {
      case (Some(id), Some(_))
          if knownLegacyState && scala.util.Try(UUID.fromString(id)).toOption.exists(_.toString == id) =>
        for {
          fence <- fences.find(Filters.eq(MongoFields.Id, id)).first
          ids <- snapshot(run, fence)
          update <- queue.updateOne(
            Filters.and(Filters.eq(MongoFields.Id, id), legacy),
            Updates.combine(
              Updates.set(MongoFields.Revision, Long.box(0L)),
              Updates.set(MongoFields.State, "Pending"),
              Updates.set(ProducerIdsField, ids.asJava),
              Updates.unset("barriers"),
              Updates.unset(MongoFields.CompletedAt),
              Updates.unset("purgedAt")
            )
          )
          _ <-
            if (update.wasAcknowledged() && update.getMatchedCount <= 1L) IO.unit
            else IO.raiseError(MigrationError.UnacknowledgedWrite(run.id))
        } yield ()
      case _ => run.fail("found invalid legacy state, subject or request time")
    }
  }

  private def repair(run: MigrationRun): IO[Unit] = {
    val queue = run.database.getCollection(MongoCollections.InterviewSubjectCleanup)
    def batch: IO[Unit] =
      queue.find(legacy).sort(Sorts.ascending(MongoFields.Id)).limit(BatchSize).all.flatMap { rows =>
        if (rows.isEmpty) IO.unit else rows.toList.traverse_(migrate(run, _)) *> batch
      }
    batch *> verify(run)
  }

  private def verifyRows(run: MigrationRun, after: Option[String]): IO[Unit] = {
    val queue = run.database.getCollection(MongoCollections.InterviewSubjectCleanup)
    val predicate = after.fold(Filters.empty())(id => Filters.gt(MongoFields.Id, id))
    queue.find(predicate).sort(Sorts.ascending(MongoFields.Id)).limit(BatchSize).all.flatMap { rows =>
      rows.toVector
        .traverse(row =>
          IO.fromEither(
            MongoInterviewCleanupCodec
              .decode(row)
              .leftMap(_ => MigrationError.StepFailed(run.id, "found invalid typed cleanup evidence"))
          )
        )
        .flatMap { decoded =>
          decoded.lastOption.fold(IO.unit)(last => verifyRows(run, Some(last.subjectId.value.toString)))
        }
    }
  }

  /** Verified on every start while the 015 audited proof is not yet trusted. */
  private def verify(run: MigrationRun): IO[Unit] = {
    val queue = run.database.getCollection(MongoCollections.InterviewSubjectCleanup)
    queue.count(legacy, new CountOptions()).flatMap { remaining =>
      if (remaining == 0L) IO.unit else run.fail("left legacy cleanup evidence")
    } *> queue
      .count(Filters.not(Filters.`type`(MongoFields.Id, org.bson.BsonType.STRING)), new CountOptions())
      .flatMap {
        case 0L => verifyRows(run, None)
        case _  => run.fail("found invalid subject identity type")
      }
  }

  val step: MongoMigrationStep = MongoMigrationStep(
    Id,
    repair,
    database => verify(MigrationRun(database, Id, None)).as(CompletedProof.Trusted)
  )

  def initialize(database: MongoHiringSetup.SetupDatabase): IO[Unit] = MongoMigrationRunner.run(database, step)
}
