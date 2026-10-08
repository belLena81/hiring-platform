package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.workflow.{
  InterviewSubjectCleanup,
  InterviewCleanupState,
  InterviewRetentionBarrier,
  InterviewTopicPair
}
import com.example.graphQL.cats.service.port.{
  InterviewSubjectCleanupRepository,
  InterviewCleanupUpdate,
  InterviewCleanupCursor,
  InterviewCleanupPage,
  RepositoryIO
}
import com.example.graphQL.cats.service.{RepositoryError, Diagnostics}
import mongo4cats.client.ClientSession
import mongo4cats.database.MongoDatabase
import org.bson.Document
import com.mongodb.client.model.{Sorts, UpdateOptions}
import java.time.Instant
import java.util.{Date, UUID}
import scala.jdk.CollectionConverters.*

/** Durable deletion evidence. Broker fencing precedes barrier capture in the application worker. */
final class MongoInterviewSubjectCleanup(
    database: MongoDatabase[IO],
    diagnostics: Diagnostics = Diagnostics.noop,
    topics: InterviewTopicPair = InterviewTopicPair.Default
) extends InterviewSubjectCleanupRepository {
  private val queue = Mongo4catsCollections.documents(database, MongoCollections.InterviewSubjectCleanup)
  private val workflows = Mongo4catsCollections.documents(database, MongoCollections.InterviewWorkflows)
  private val RelatedCollections = List(
    MongoCollections.InterviewWorkflows,
    MongoCollections.InterviewWorkflowCommands,
    MongoCollections.InterviewWorkflowInbox,
    MongoCollections.FakeInterviewCalendarReservations,
    MongoCollections.FakeInterviewNotificationReceipts
  )
  private val ProducerIdsField = "interviewTransactionalIds"

  /** Called within account deletion after its permanent subject fence is marked deleted. */
  def enqueue(subject: UserId, now: Instant, session: Option[ClientSession[IO]]): RepositoryIO[Unit] =
    for {
      ids <- MongoProducerRegistrations.batch(database, subject.value.toString, "Interview")
      attributable <- hasAttributableData(subject, session)
      _ <-
        if (!attributable && ids.isEmpty) RepositoryIO.fromEither(Right(()))
        else
          RepositoryIO
            .lift(
              MongoSessionOperations.updateOne(
                queue,
                session,
                MongoFilter.eq(MongoFields.Id, subject.value.toString),
                MongoUpdate.combine(
                  MongoUpdate.setOnInsert("state", "Pending"),
                  MongoUpdate.setOnInsert("producerRegistry", true),
                  MongoUpdate.setOnInsert("revision", Long.box(0L)),
                  MongoUpdate.setOnInsert("requestedAt", Date.from(now)),
                  MongoUpdate.setOnInsert(ProducerIdsField, Vector.empty[String].asJava)
                ),
                new UpdateOptions().upsert(true)
              )
            )
            .subflatMap {
              case Some(value) if value.wasAcknowledged() => Right(())
              case _                                      => Left(RepositoryError.MissingWriteResult)
            }
    } yield ()

  override def producerBatch(subject: UserId): RepositoryIO[Vector[String]] = guard("interviewCleanup.producerBatch") {
    MongoProducerRegistrations.batch(database, subject.value.toString, "Interview")
  }

  override def markProducersFenced(subject: UserId, ids: Vector[String], now: Instant): RepositoryIO[Unit] =
    guard("interviewCleanup.markProducersFenced") {
      MongoProducerRegistrations.markFenced(database, subject.value.toString, "Interview", ids, now)
    }

  private def hasAttributableData(subject: UserId, session: Option[ClientSession[IO]]): RepositoryIO[Boolean] =
    RepositoryIO
      .lift(
        RelatedCollections
          .traverse(name =>
            MongoSessionOperations.findOne(
              Mongo4catsCollections.documents(database, name),
              session,
              subjectFilter(subject.value.toString)
            )
          )
          .map(_.exists(_.nonEmpty))
      )
      .flatMap {
        case true  => RepositoryIO.fromEither(Right(true))
        case false =>
          RepositoryIO
            .lift(
              MongoSessionOperations.findOne(
                Mongo4catsCollections.documents(database, MongoCollections.FakeInterviewCalendarParticipantLocks),
                session,
                MongoFilter.eq(MongoFields.Id, subject.value.toString)
              )
            )
            .map(_.nonEmpty)
      }

  override def find(subject: UserId): RepositoryIO[Option[InterviewSubjectCleanup]] = guard("interviewCleanup.find") {
    RepositoryIO
      .lift(MongoSessionOperations.findOne(queue, None, MongoFilter.eq(MongoFields.Id, subject.value.toString)))
      .flatMap(_.traverse(row => RepositoryIO.fromEither(MongoInterviewCleanupCodec.decodeCurrent(row, topics))))
  }

  /** Malformed evidence cannot certify public deletion completion. */
  def complete(subject: UserId): IO[Boolean] = find(subject).value.map {
    case Right(None)        => true
    case Right(Some(value)) =>
      value.state match {
        case InterviewCleanupState.Complete(_) => true
        case _                                 => false
      }
    case Left(_) => false
  }

  override def pendingPage(
      cursor: Option[InterviewCleanupCursor],
      observedAt: Instant
  ): RepositoryIO[InterviewCleanupPage] = guard("interviewCleanup.pending") {
    import MongoInterviewCleanupSweepCodec.*
    val sweep = cursor match {
      case Some(value) => RepositoryIO.fromEither(decode(value).map(Some(_)))
      case None        =>
        RepositoryIO
          .lift(
            queue.flatMap(
              _.find(eligible(observedAt)).sort(Sorts.descending(MongoFields.Id)).hint(ActiveIndex).limit(1).first
            )
          )
          .flatMap(_.traverse(row => RepositoryIO.fromEither(identity(row).map(id => Sweep(None, id, observedAt)))))
    }
    sweep.flatMap {
      case None          => RepositoryIO.fromEither(Right(InterviewCleanupPage(Vector.empty, None)))
      case Some(current) =>
        RepositoryIO
          .lift(
            queue.flatMap(
              _.find(pageFilter(current)).sort(Sorts.ascending(MongoFields.Id)).hint(ActiveIndex).limit(PageSize).all
            )
          )
          .flatMap { rows =>
            val selected = rows.toVector
            val next =
              if (selected.size < PageSize) Right(None)
              else
                selected.lastOption
                  .traverse(row =>
                    identity(row).map { id =>
                      Option.unless(id == current.throughId)(encode(current, id))
                    }
                  )
                  .map(_.flatten)
            RepositoryIO.fromEither(
              next.map(token =>
                InterviewCleanupPage(selected.map(MongoInterviewCleanupCodec.decodeCurrent(_, topics)), token)
              )
            )
          }
    }
  }

  override def transition(
      expected: InterviewSubjectCleanup,
      next: InterviewSubjectCleanup
  ): RepositoryIO[InterviewCleanupUpdate] =
    guard("interviewCleanup.transition") {
      val immutableInputs = expected.subjectId == next.subjectId && expected.requestedAt == next.requestedAt &&
        expected.transactionalIds == next.transactionalIds && expected.revision < Long.MaxValue &&
        next.revision == expected.revision + 1L
      val forward = (expected.state, next.state) match {
        case (InterviewCleanupState.Pending, InterviewCleanupState.ProducersFenced)          => true
        case (InterviewCleanupState.ProducersFenced, InterviewCleanupState.MongoPurged)      => true
        case (InterviewCleanupState.MongoPurged, InterviewCleanupState.AwaitingRetention(_)) => true
        case (InterviewCleanupState.AwaitingRetention(_), InterviewCleanupState.Complete(_)) => true
        case _                                                                               => false
      }
      for {
        _ <- RepositoryIO.fromEither(
          InterviewSubjectCleanup.validate(expected, topics).leftMap(_ => RepositoryError.InvalidStoredData)
        )
        _ <- RepositoryIO.fromEither(
          InterviewSubjectCleanup.validate(next, topics).leftMap(_ => RepositoryError.InvalidStoredData)
        )
        _ <- RepositoryIO.fromEither(Either.cond(immutableInputs && forward, (), RepositoryError.InvalidStoredData))
        result <- RepositoryIO
          .lift(
            MongoSessionOperations.updateOne(
              queue,
              None,
              MongoFilter.and(
                MongoFilter.eq(MongoFields.Id, expected.subjectId.value.toString),
                MongoFilter.eq("revision", Long.box(expected.revision)),
                MongoFilter.eq("state", stateName(expected.state))
              ),
              stateUpdate(next)
            )
          )
          .subflatMap {
            case Some(value) if !value.wasAcknowledged()    => Left(RepositoryError.MissingWriteResult)
            case Some(value) if value.getMatchedCount == 1L => Right(InterviewCleanupUpdate.Applied)
            case Some(_)                                    => Right(InterviewCleanupUpdate.StaleRevision)
            case None                                       => Left(RepositoryError.MissingWriteResult)
          }
      } yield result
    }

  private def stateUpdate(value: InterviewSubjectCleanup): MongoUpdate = {
    val common =
      List(MongoUpdate.set("revision", Long.box(value.revision)), MongoUpdate.set("state", stateName(value.state)))
    val detail = value.state match {
      case InterviewCleanupState.AwaitingRetention(barriers) =>
        List(
          MongoUpdate.set(
            "barriers",
            barriers
              .map(barrier =>
                new Document("topic", barrier.topic)
                  .append("partition", Int.box(barrier.partition))
                  .append("endOffset", Long.box(barrier.endOffset))
              )
              .asJava
          )
        )
      case InterviewCleanupState.Complete(at) =>
        List(MongoUpdate.unset("barriers"), MongoUpdate.set("completedAt", Date.from(at)))
      case _ => List.empty
    }
    MongoUpdate.combine((common ++ detail)*)
  }

  private def stateName(value: InterviewCleanupState): String = value match {
    case InterviewCleanupState.Pending              => "Pending"
    case InterviewCleanupState.ProducersFenced      => "ProducersFenced"
    case InterviewCleanupState.MongoPurged          => "MongoPurged"
    case InterviewCleanupState.AwaitingRetention(_) => "AwaitingRetention"
    case InterviewCleanupState.Complete(_)          => "Complete"
  }

  private def guard[A](operation: String)(effect: RepositoryIO[A]): RepositoryIO[A] =
    MongoRepositorySupport.repositoryGuard(diagnostics, operation)(effect)(_ => Left(RepositoryError.Unavailable))

  override def absent(subject: UserId): RepositoryIO[Boolean] = guard("interviewCleanup.absent") {
    hasAttributableData(subject, None).map(value => !value)
  }

  override def purge(subject: UserId): RepositoryIO[Unit] = guard("interviewCleanup.purge") {
    RepositoryIO.lift(purgeData(subject.value.toString))
  }

  private def subjectFilter(subject: String): MongoFilter = MongoFilter.or(
    MongoFilter.eq("initiatedBy", subject),
    MongoFilter.eq("candidateId", subject),
    MongoFilter.eq("recruiterId", subject),
    MongoFilter.eq("recipientId", subject),
    MongoFilter.eq("subjectIds", subject),
    MongoFilter.eq("participants", subject),
    // Request receipts retain their initiating subject in this existing intrinsic identity after workflow TTL.
    MongoFilter.and(
      MongoFilter.gte(MongoFields.Id, s"request:$subject:"),
      MongoFilter.lt(MongoFields.Id, s"request:$subject;")
    )
  )

  private def purgeData(subject: String): IO[Unit] = {
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
        val ids = found.toList
          .flatMap(row =>
            Option(row.getString("workflowId"))
              .orElse(Option(row.getString("requestWorkflowId")))
              .orElse(Option(row.getString("_id")))
          )
          .distinct
        if (ids.isEmpty) IO.unit
        else {
          // Retain unselected attribution until its linked children have been purged.
          val byWorkflow = MongoFilter.in("workflowId", ids)
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
                    .or(MongoFilter.in("_id", ids), MongoFilter.in("requestWorkflowId", ids))
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

}

private[mongo] object MongoInterviewCleanupCodec {
  private val ProducerIdsField = "interviewTransactionalIds"

  def decodeCurrent(
      row: Document,
      topics: InterviewTopicPair = InterviewTopicPair.Default
  ): Either[RepositoryError, InterviewSubjectCleanup] =
    Either
      .cond(Option(row.get("producerRegistry")).contains(java.lang.Boolean.TRUE), (), RepositoryError.InvalidStoredData)
      .flatMap(_ => decode(row, topics))

  def decode(
      row: Document,
      topics: InterviewTopicPair = InterviewTopicPair.Default
  ): Either[RepositoryError, InterviewSubjectCleanup] = for {
    id <- string(row, MongoFields.Id).flatMap(value =>
      Either
        .catchNonFatal(UUID.fromString(value))
        .leftMap(_ => RepositoryError.InvalidStoredData)
        .flatMap(id => Either.cond(id.toString == value, id, RepositoryError.InvalidStoredData))
    )
    revision <- long(row, "revision")
    requested <- instant(row, "requestedAt")
    ids <- stringVector(row, ProducerIdsField)
    name <- string(row, "state")
    state <- name match {
      case "Pending"           => Right(InterviewCleanupState.Pending)
      case "ProducersFenced"   => Right(InterviewCleanupState.ProducersFenced)
      case "MongoPurged"       => Right(InterviewCleanupState.MongoPurged)
      case "AwaitingRetention" => barriers(row).map(InterviewCleanupState.AwaitingRetention.apply)
      case "Complete"          => instant(row, "completedAt").map(InterviewCleanupState.Complete.apply)
      case _                   => Left(RepositoryError.InvalidStoredData)
    }
    value <- InterviewSubjectCleanup
      .validate(InterviewSubjectCleanup(UserId(id), revision, requested, ids, state), topics)
      .leftMap(_ => RepositoryError.InvalidStoredData)
  } yield value

  private def barriers(row: Document): Either[RepositoryError, Vector[InterviewRetentionBarrier]] =
    Option(row.get("barriers"))
      .collect { case values: java.util.List[?] => values.asScala.toVector }
      .toRight(RepositoryError.InvalidStoredData)
      .flatMap(_.traverse {
        case document: Document =>
          for {
            topic <- string(document, "topic")
            partition <- Option(document.get("partition"))
              .collect { case number: java.lang.Integer => number.intValue() }
              .toRight(RepositoryError.InvalidStoredData)
            offset <- long(document, "endOffset")
          } yield InterviewRetentionBarrier(topic, partition, offset)
        case _ => Left(RepositoryError.InvalidStoredData)
      })

  private def string(row: Document, field: String): Either[RepositoryError, String] =
    Option(row.get(field)).collect { case value: String => value }.toRight(RepositoryError.InvalidStoredData)

  private def long(row: Document, field: String): Either[RepositoryError, Long] =
    Option(row.get(field))
      .collect { case value: java.lang.Long => value.longValue() }
      .toRight(RepositoryError.InvalidStoredData)

  private def instant(row: Document, field: String): Either[RepositoryError, Instant] =
    Option(row.get(field)).collect { case value: Date => value.toInstant }.toRight(RepositoryError.InvalidStoredData)

  def stringVector(row: Document, field: String): Either[RepositoryError, Vector[String]] =
    Option(row.get(field))
      .collect { case values: java.util.List[?] => values.asScala.toVector }
      .toRight(RepositoryError.InvalidStoredData)
      .flatMap(_.traverse {
        case value: String => Right(value)
        case _             => Left(RepositoryError.InvalidStoredData)
      })

}
