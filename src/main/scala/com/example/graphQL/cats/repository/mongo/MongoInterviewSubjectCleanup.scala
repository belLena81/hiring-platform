package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.port.RepositoryIO
import com.example.graphQL.cats.service.{RepositoryError, Diagnostics, LogEvent, LogFields}
import com.example.graphQL.cats.service.Diagnostics.*
import mongo4cats.client.ClientSession
import mongo4cats.database.MongoDatabase
import org.bson.Document
import com.mongodb.client.model.{Sorts, UpdateOptions}
import java.time.Instant
import java.util.Date
import scala.jdk.CollectionConverters.*
import scala.concurrent.duration.*

/** Persistent deletion work. A public receipt cannot complete until both workflow topics have physically expired. */
final class MongoInterviewSubjectCleanup(database: MongoDatabase[IO]) {
  private val queue = Mongo4catsCollections.documents(database, MongoCollections.InterviewSubjectCleanup)
  private val workflows = Mongo4catsCollections.documents(database, MongoCollections.InterviewWorkflows)

  def enqueue(subject: UserId, now: Instant, session: Option[ClientSession[IO]]): RepositoryIO[Unit] =
    RepositoryIO
      .lift(
        List(
          MongoCollections.InterviewWorkflows,
          MongoCollections.InterviewWorkflowCommands,
          MongoCollections.InterviewWorkflowInbox,
          MongoCollections.FakeInterviewCalendarReservations,
          MongoCollections.FakeInterviewNotificationReceipts
        ).traverse(name =>
          MongoSessionOperations
            .findOne(Mongo4catsCollections.documents(database, name), session, subjectFilter(subject.value.toString))
        ).map(_.exists(_.nonEmpty))
      )
      .flatMap {
        case false =>
          RepositoryIO
            .lift(
              MongoSessionOperations.findOne(
                Mongo4catsCollections.documents(database, MongoCollections.FakeInterviewCalendarParticipantLocks),
                session,
                MongoFilter.eq("_id", subject.value.toString)
              )
            )
            .map(_.nonEmpty)
        case true => RepositoryIO.fromEither(Right(true))
      }
      .flatMap {
        case false => RepositoryIO.fromEither(Right(()))
        case true  =>
          RepositoryIO
            .lift(
              MongoSessionOperations.updateOne(
                queue,
                session,
                MongoFilter.eq(MongoFields.Id, subject.value.toString),
                MongoUpdate.combine(
                  MongoUpdate.setOnInsert(MongoFields.Id, subject.value.toString),
                  MongoUpdate.setOnInsert("state", "Pending"),
                  MongoUpdate.setOnInsert("requestedAt", Date.from(now))
                ),
                new UpdateOptions().upsert(true)
              )
            )
            .flatMap {
              case Some(value) if value.wasAcknowledged() => RepositoryIO.fromEither(Right(()))
              case _ => RepositoryIO.fromEither(Left(RepositoryError.MissingWriteResult))
            }
      }

  def complete(subject: UserId): IO[Boolean] =
    MongoSessionOperations
      .findOne(queue, None, MongoFilter.eq(MongoFields.Id, subject.value.toString))
      .map(_.forall(_.getString("state") == "Complete"))

  /** Callback capture occurs after claims, provider calls and Kafka delivery have drained. */
  def runOnce(drain: FiniteDuration, capture: IO[List[Document]], passed: List[Document] => IO[Boolean]): IO[Unit] =
    for {
      now <- IO.realTimeInstant
      pending <- queue.flatMap(
        _.find(MongoFilter.ne("state", "Complete").bson).sort(Sorts.ascending("requestedAt")).limit(32).all
      )
      _ <- pending.toList.traverse_ { row =>
        val subject = row.getString("_id")
        val filter = MongoFilter.eq("_id", subject)
        val drained = !now.isBefore(row.getDate("requestedAt").toInstant.plusMillis(drain.toMillis))
        if (!drained) IO.unit
        else if (row.getString("state") == "Pending")
          purge(subject) *> IO.realTimeInstant.flatMap(purgedAt =>
            MongoSessionOperations
              .updateOne(
                queue,
                None,
                filter,
                MongoUpdate
                  .combine(MongoUpdate.set("state", "Purged"), MongoUpdate.set("purgedAt", Date.from(purgedAt)))
              )
              .void
          )
        else if (row.getString("state") == "Purged") {
          val effectsDrained = !now.isBefore(row.getDate("purgedAt").toInstant.plusMillis(drain.toMillis))
          if (!effectsDrained) IO.unit
          else
            purge(subject) *> capture.flatMap { barriers =>
              MongoSessionOperations
                .updateOne(
                  queue,
                  None,
                  filter,
                  MongoUpdate.combine(MongoUpdate.set("state", "Cleaned"), MongoUpdate.set("barriers", barriers.asJava))
                )
                .void
            }
        } else {
          val barriers = Option(row.getList("barriers", classOf[Document])).fold(List.empty[Document])(_.asScala.toList)
          if (barriers.isEmpty) IO.raiseError(new IllegalStateException("interview retention barrier missing"))
          else
            passed(barriers).flatMap {
              case false => IO.unit
              case true  =>
                purge(subject) *> MongoSessionOperations
                  .updateOne(
                    queue,
                    None,
                    filter,
                    MongoUpdate.combine(
                      MongoUpdate.set("state", "Complete"),
                      MongoUpdate.unset("barriers"),
                      MongoUpdate.set("completedAt", Date.from(now))
                    )
                  )
                  .void
            }
        }
      }
    } yield ()

  private def subjectFilter(subject: String): MongoFilter = MongoFilter.or(
    MongoFilter.eq("initiatedBy", subject),
    MongoFilter.eq("candidateId", subject),
    MongoFilter.eq("recruiterId", subject),
    MongoFilter.eq("recipientId", subject),
    MongoFilter.eq("subjectIds", subject),
    MongoFilter.eq("participants", subject)
  )

  private def purge(subject: String): IO[Unit] = {
    val subjectPredicate = subjectFilter(subject)
    def batch: IO[Unit] = List(
      MongoCollections.InterviewWorkflows,
      MongoCollections.FakeInterviewCalendarReservations,
      MongoCollections.FakeInterviewNotificationReceipts
    ).traverse(name =>
      Mongo4catsCollections
        .documents(database, name)
        .flatMap(_.find(subjectPredicate.bson).limit(128).all)
        .map(_.toList)
    ).map(_.flatten)
      .flatMap { found =>
        val ids = found.toList.flatMap(row => Option(row.getString("workflowId")).orElse(Option(row.getString("_id"))))
        if (ids.isEmpty) IO.unit
        else {
          val byWorkflow = MongoFilter.or(MongoFilter.in("workflowId", ids), subjectPredicate)
          List(
            MongoCollections.InterviewWorkflowCommands,
            MongoCollections.InterviewWorkflowInbox,
            MongoCollections.FakeInterviewCalendarReservations,
            MongoCollections.FakeInterviewNotificationReceipts
          )
            .traverse_(name =>
              Mongo4catsCollections.documents(database, name).flatMap(_.deleteMany(byWorkflow.bson)).void
            ) *>
            workflows
              .flatMap(
                _.deleteMany(
                  MongoFilter
                    .or(MongoFilter.in("_id", ids), MongoFilter.in("requestWorkflowId", ids), subjectPredicate)
                    .bson
                )
              )
              .void *> batch
        }
      }
    batch *> List(
      MongoCollections.InterviewWorkflowCommands,
      MongoCollections.InterviewWorkflowInbox,
      MongoCollections.FakeInterviewCalendarReservations,
      MongoCollections.FakeInterviewNotificationReceipts
    )
      .traverse_(name =>
        Mongo4catsCollections.documents(database, name).flatMap(_.deleteMany(subjectPredicate.bson)).void
      ) *>
      Mongo4catsCollections
        .documents(database, MongoCollections.FakeInterviewCalendarParticipantLocks)
        .flatMap(_.deleteMany(MongoFilter.eq("_id", subject).bson))
        .void
  }

  def resource(
      drain: FiniteDuration,
      capture: IO[List[Document]],
      passed: List[Document] => IO[Boolean],
      diagnostics: Diagnostics
  ): Resource[IO, Unit] =
    Resource
      .make(
        fs2.Stream
          .repeatEval(
            runOnce(drain, capture, passed).handleErrorWith(error =>
              diagnostics.emit(LogEvent.RuntimeFailed, fields = LogFields.failure(error))
            )
          )
          .metered(1.second)
          .compile
          .drain
          .start
      )(_.cancel)
      .void
}
