package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.AccountDeletionStatus
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.{
  AnalyticsFunnelDay,
  AnalyticsReportSnapshot,
  AnalyticsSkillPostingDay,
  AnalyticsTimeToHire
}
import com.example.graphQL.cats.service.port.{
  AnalyticsErasureRequestRepository,
  AnalyticsReportRepository,
  AnalyticsReportRunReservation,
  AnalyticsRunId,
  AnalyticsRangeFingerprint,
  AnalyticsReportSnapshotPublisher,
  MutationWriteContext,
  RepositoryError,
  RepositoryIO
}
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.shared.Parsing
import com.mongodb.client.model.{ReplaceOptions, UpdateOptions}
import mongo4cats.client.{ClientSession, MongoClient}
import mongo4cats.database.MongoDatabase
import mongo4cats.collection.MongoCollection
import org.bson.Document

import java.util.Date
import java.time.Instant
import scala.jdk.CollectionConverters.*

private[mongo] object MongoAnalyticsRepositoryOperations {
  type Documents = MongoCollection[IO, Document]

  def findOne(
      collection: IO[Documents],
      session: Option[ClientSession[IO]],
      filter: MongoFilter
  ): RepositoryIO[Option[Document]] =
    RepositoryIO.lift(MongoSessionOperations.findOne(collection, session, filter))

  def insertOne(
      collection: IO[Documents],
      session: Option[ClientSession[IO]],
      document: Document
  ): RepositoryIO[Option[com.mongodb.client.result.InsertOneResult]] =
    RepositoryIO.lift(MongoSessionOperations.insertOne(collection, session, document))

  def updateOne(
      collection: IO[Documents],
      session: Option[ClientSession[IO]],
      filter: MongoFilter,
      update: MongoUpdate,
      options: UpdateOptions = new UpdateOptions
  ): RepositoryIO[Option[com.mongodb.client.result.UpdateResult]] =
    RepositoryIO.lift(MongoSessionOperations.updateOne(collection, session, filter, update, options))

  def replaceOne(
      collection: IO[Documents],
      session: Option[ClientSession[IO]],
      filter: MongoFilter,
      replacement: Document,
      options: ReplaceOptions
  ): RepositoryIO[Option[com.mongodb.client.result.UpdateResult]] =
    RepositoryIO.lift(MongoSessionOperations.replaceOne(collection, session, filter, replacement, options))
}

/** Stores one idempotent erasure request per subject in the same Mongo transaction as account deletion. */
final class MongoAnalyticsErasureRequestRepository(
    database: MongoDatabase[IO],
    transactionRunner: MongoTransactionRunner,
    diagnostics: Diagnostics,
    topics: com.example.graphQL.cats.domain.workflow.InterviewTopicPair =
      com.example.graphQL.cats.domain.workflow.InterviewTopicPair.Default
) extends AnalyticsErasureRequestRepository {
  private val collection = Mongo4catsCollections.documents(database, MongoCollections.AnalyticsErasureRequests)
  private val subjectFences = Mongo4catsCollections.documents(database, MongoCollections.OutboxSubjectFences)
  private val outbox = Mongo4catsCollections.documents(database, MongoCollections.EventOutbox)
  private val users = Mongo4catsCollections.documents(database, MongoCollections.Users)
  private val completions = Mongo4catsCollections.documents(database, MongoCollections.AnalyticsErasureCompletions)
  private val workerHeartbeats = Mongo4catsCollections.documents(database, MongoCollections.AnalyticsWorkerHeartbeats)

  override def workerReady(now: Instant): RepositoryIO[Unit] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "analyticsErasure.workerReady")(
        MongoAnalyticsRepositoryOperations
          .findOne(
            workerHeartbeats,
            None,
            MongoFilter.and(
              MongoFilter.eq(MongoFields.Id, "analytics-erasure"),
              MongoFilter.eq(MongoFields.State, "Ready"),
              MongoFilter.gt(MongoFields.LeaseUntil, Date.from(now))
            )
          )
          .subflatMap(_.fold[Either[RepositoryError, Unit]](Left(RepositoryError.Unavailable))(_ => Right(())))
      )(_ => Left(RepositoryError.Unavailable))

  override def enqueue(
      userId: UserId,
      now: Instant,
      context: MutationWriteContext
  ): RepositoryIO[String] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "analyticsErasure.enqueue")(
        RepositoryIO.lift(UUIDGen[IO].randomUUID.map(_.toString)).flatMap { freshReceiptId =>
          MongoMutationWriteContext.run(context, transactionRunner, transactionRequired = true) { session =>
            val requestId = userId.value.toString
            val completion = session.fold(
              MongoAnalyticsRepositoryOperations.findOne(completions, None, MongoFilter.eq(MongoFields.Id, requestId))
            )(active =>
              MongoAnalyticsRepositoryOperations
                .findOne(completions, Some(active), MongoFilter.eq(MongoFields.Id, requestId))
            )
            completion
              .flatMap {
                case Some(document) =>
                  Option(document.getString(MongoFields.ReceiptId)) match {
                    case Some(value) => RepositoryIO.fromEither(Right(value))
                    case None        =>
                      val update = MongoUpdate.set(MongoFields.ReceiptId, freshReceiptId)
                      val result = session.fold(
                        MongoAnalyticsRepositoryOperations.updateOne(
                          completions,
                          None,
                          MongoFilter.eq(MongoFields.Id, requestId),
                          update,
                          new UpdateOptions
                        )
                      )(active =>
                        MongoAnalyticsRepositoryOperations.updateOne(
                          completions,
                          Some(active),
                          MongoFilter.eq(MongoFields.Id, requestId),
                          update,
                          new UpdateOptions
                        )
                      )
                      result.subflatMap(
                        _.fold[Either[RepositoryError, String]](Left(RepositoryError.MissingWriteResult))(_ =>
                          Right(freshReceiptId)
                        )
                      )
                  }
                case None =>
                  val existing = session.fold(
                    MongoAnalyticsRepositoryOperations
                      .findOne(collection, None, MongoFilter.eq(MongoFields.Id, requestId))
                  )(active =>
                    MongoAnalyticsRepositoryOperations
                      .findOne(collection, Some(active), MongoFilter.eq(MongoFields.Id, requestId))
                  )
                  existing.flatMap {
                    case Some(document) =>
                      Option(document.getString(MongoFields.ReceiptId)) match {
                        case Some(value) => RepositoryIO.fromEither(Right(value))
                        case None        =>
                          val update = MongoUpdate.set(MongoFields.ReceiptId, freshReceiptId)
                          val result = session.fold(
                            MongoAnalyticsRepositoryOperations.updateOne(
                              collection,
                              None,
                              MongoFilter.eq(MongoFields.Id, requestId),
                              update,
                              new UpdateOptions
                            )
                          )(active =>
                            MongoAnalyticsRepositoryOperations.updateOne(
                              collection,
                              Some(active),
                              MongoFilter.eq(MongoFields.Id, requestId),
                              update,
                              new UpdateOptions
                            )
                          )
                          result.subflatMap(
                            _.fold[Either[RepositoryError, String]](Left(RepositoryError.MissingWriteResult))(_ =>
                              Right(freshReceiptId)
                            )
                          )
                      }
                    case None =>
                      val fence = session.fold(
                        MongoAnalyticsRepositoryOperations
                          .findOne(subjectFences, None, MongoFilter.eq(MongoFields.Id, requestId))
                      )(active =>
                        MongoAnalyticsRepositoryOperations
                          .findOne(subjectFences, Some(active), MongoFilter.eq(MongoFields.Id, requestId))
                      )
                      fence.flatMap { _ =>
                        val request = new Document(MongoFields.Id, requestId)
                          .append(MongoFields.RequestedAt, Date.from(now))
                          .append(MongoFields.ReceiptId, freshReceiptId)
                          .append(MongoFields.State, "Pending")
                          .append(MongoFields.FencingVersion, 1)
                          .append("producerRegistry", true)
                        val insert = session.fold(
                          MongoAnalyticsRepositoryOperations.insertOne(collection, None, request)
                        )(active => MongoAnalyticsRepositoryOperations.insertOne(collection, Some(active), request))
                        insert.flatMap {
                          case Some(_) =>
                            val reportControl =
                              Mongo4catsCollections.documents(database, MongoCollections.AnalyticsReportControl)
                            val hide = MongoUpdate.combine(
                              MongoUpdate.inc(MongoFields.Generation, 1L),
                              MongoUpdate.set(MongoFields.State, AnalyticsReportSnapshotDocument.Hidden),
                              MongoUpdate.set(MongoFields.HiddenAt, Date.from(now))
                            )
                            val hideResult = session.fold(
                              MongoAnalyticsRepositoryOperations.updateOne(
                                reportControl,
                                None,
                                MongoFilter.eq(MongoFields.Id, AnalyticsReportSnapshotDocument.ControlId),
                                hide,
                                new UpdateOptions
                              )
                            )(active =>
                              MongoAnalyticsRepositoryOperations.updateOne(
                                reportControl,
                                Some(active),
                                MongoFilter.eq(MongoFields.Id, AnalyticsReportSnapshotDocument.ControlId),
                                hide,
                                new UpdateOptions
                              )
                            )
                            hideResult.subflatMap {
                              case Some(result) if result.getMatchedCount == 1L => Right(freshReceiptId)
                              case Some(_)                                      => Left(RepositoryError.Conflict)
                              case None => Left(RepositoryError.MissingWriteResult)
                            }
                          case None => RepositoryIO.fromEither(Left(RepositoryError.MissingWriteResult))
                        }
                      }
                  }
              }
          }
        }
      )(_ => Left(RepositoryError.Unavailable))

  override def statusForSubject(
      userId: UserId,
      receiptId: String
  ): RepositoryIO[AccountDeletionStatus] =
    if (Parsing.parseUuid(receiptId).isLeft) RepositoryIO.fromEither(Right(AccountDeletionStatus.NotFound))
    else
      MongoRepositorySupport
        .repositoryGuard(diagnostics, "analyticsErasure.statusForSubject")({
          val requestFilter = MongoFilter.and(
            MongoFilter.eq(MongoFields.Id, userId.value.toString),
            MongoFilter.eq(MongoFields.ReceiptId, receiptId)
          )
          val completionFilter = MongoFilter.and(
            MongoFilter.eq(MongoFields.Id, userId.value.toString),
            MongoFilter.eq(MongoFields.ReceiptId, receiptId)
          )
          MongoAnalyticsRepositoryOperations
            .findOne(collection, None, requestFilter)
            .flatMap {
              case Some(document) if document.getString(MongoFields.State) == "Complete" =>
                RepositoryIO.fromEither(Right(AccountDeletionStatus.Complete))
              case Some(document) if Set("Pending", "Processing").contains(document.getString(MongoFields.State)) =>
                RepositoryIO.fromEither(Right(AccountDeletionStatus.Pending))
              case Some(_) => RepositoryIO.fromEither(Left(RepositoryError.InvalidStoredData))
              case None    =>
                MongoAnalyticsRepositoryOperations
                  .findOne(completions, None, completionFilter)
                  .subflatMap(
                    _.fold[Either[RepositoryError, AccountDeletionStatus]](Right(AccountDeletionStatus.NotFound))(_ =>
                      Right(AccountDeletionStatus.Complete)
                    )
                  )
            }
        })(_ => Left(RepositoryError.Unavailable))
        .flatMap {
          case AccountDeletionStatus.Complete =>
            RepositoryIO
              .lift(new MongoInterviewSubjectCleanup(database, diagnostics, topics).complete(userId))
              .map(if (_) AccountDeletionStatus.Complete else AccountDeletionStatus.Pending)
          case other => RepositoryIO.fromEither(Right(other))
        }

  override def purgeSubjectOutbox(userId: UserId): RepositoryIO[Unit] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "analyticsErasure.purgeSubjectOutbox")(
        RepositoryIO
          .lift(
            outbox
              .flatMap(_.deleteMany(MongoFilter.in(MongoFields.SubjectIds, List(userId.value.toString)).bson))
          )
          .void
      )(_ => Left(RepositoryError.Unavailable))

  override def markComplete(userId: UserId, now: Instant): RepositoryIO[Unit] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "analyticsErasure.markComplete")(transactionRunner.run { session =>
        val requestId = userId.value.toString
        val user = session.fold(
          MongoAnalyticsRepositoryOperations.findOne(users, None, MongoFilter.eq(MongoFields.Id, requestId))
        )(active =>
          MongoAnalyticsRepositoryOperations.findOne(users, Some(active), MongoFilter.eq(MongoFields.Id, requestId))
        )
        user
          .flatMap {
            case Some(document) if document.getString(MongoFields.AccountStatus) == "Deleted" =>
              val filter = MongoFilter.and(
                MongoFilter.eq(MongoFields.Id, requestId),
                MongoFilter.in(MongoFields.State, List("Pending", "Processing"))
              )
              val update = MongoUpdate.combine(
                MongoUpdate.set(MongoFields.State, "Complete"),
                MongoUpdate.set(MongoFields.CompletedAt, Date.from(now)),
                MongoUpdate.set(MongoFields.ExpiresAt, Date.from(now.plusSeconds(31L * 24L * 60L * 60L)))
              )
              val requests = session.fold(
                MongoAnalyticsRepositoryOperations.updateOne(collection, None, filter, update, new UpdateOptions)
              )(active =>
                MongoAnalyticsRepositoryOperations
                  .updateOne(collection, Some(active), filter, update, new UpdateOptions)
              )
              requests.flatMap {
                case Some(result) if result.getMatchedCount == 1L => persistCompletion(session, requestId, now)
                case Some(_)                                      =>
                  val completed = session.fold(
                    MongoAnalyticsRepositoryOperations.findOne(
                      collection,
                      None,
                      MongoFilter
                        .and(MongoFilter.eq(MongoFields.Id, requestId), MongoFilter.eq(MongoFields.State, "Complete"))
                    )
                  )(active =>
                    MongoAnalyticsRepositoryOperations.findOne(
                      collection,
                      Some(active),
                      MongoFilter
                        .and(MongoFilter.eq(MongoFields.Id, requestId), MongoFilter.eq(MongoFields.State, "Complete"))
                    )
                  )
                  completed.flatMap(
                    _.fold(RepositoryIO.fromEither(Left(RepositoryError.Conflict)))(_ =>
                      persistCompletion(session, requestId, now)
                    )
                  )
                case None => RepositoryIO.fromEither(Left(RepositoryError.MissingWriteResult))
              }
            case _ => RepositoryIO.fromEither(Left(RepositoryError.Conflict))
          }
      })(_ => Left(RepositoryError.Unavailable))

  private def persistCompletion(
      session: Option[ClientSession[IO]],
      userId: String,
      now: Instant
  ): RepositoryIO[Unit] = {
    val request = session.fold(
      MongoAnalyticsRepositoryOperations.findOne(collection, None, MongoFilter.eq(MongoFields.Id, userId))
    )(active =>
      MongoAnalyticsRepositoryOperations.findOne(collection, Some(active), MongoFilter.eq(MongoFields.Id, userId))
    )
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "analyticsErasure.persistCompletion")(
        request
          .flatMap { stored =>
            val updates = List(
              Some(MongoUpdate.setOnInsert(MongoFields.Id, userId)),
              Some(MongoUpdate.setOnInsert(MongoFields.CompletedAt, Date.from(now))),
              stored
                .flatMap(value => Option(value.getString(MongoFields.ReceiptId)))
                .map(MongoUpdate.setOnInsert(MongoFields.ReceiptId, _))
            ).flatten
            val filter = MongoFilter.eq(MongoFields.Id, userId)
            val update = MongoUpdate.combine(updates*)
            val result = session.fold(
              MongoAnalyticsRepositoryOperations
                .updateOne(completions, None, filter, update, new UpdateOptions().upsert(true))
            )(active =>
              MongoAnalyticsRepositoryOperations
                .updateOne(completions, Some(active), filter, update, new UpdateOptions().upsert(true))
            )
            result
              .subflatMap {
                case Some(_) => Right(())
                case _       => Left(RepositoryError.MissingWriteResult)
              }
          }
      )(_ => Left(RepositoryError.Unavailable))
  }
}

object MongoAnalyticsErasureRequestRepository {
  def transactional(
      database: MongoDatabase[IO],
      client: MongoClient[IO],
      diagnostics: Diagnostics,
      topics: com.example.graphQL.cats.domain.workflow.InterviewTopicPair =
        com.example.graphQL.cats.domain.workflow.InterviewTopicPair.Default
  ): MongoAnalyticsErasureRequestRepository =
    new MongoAnalyticsErasureRequestRepository(
      database,
      MongoTransactionRunner.sessions(client, RepositoryError.Conflict, diagnostics = diagnostics),
      diagnostics,
      topics
    )
}

/** Publishes complete analytics snapshots atomically and exposes the newest valid one to the API. */
final class MongoAnalyticsReportRepository(
    database: MongoDatabase[IO],
    transactionRunner: MongoTransactionRunner,
    diagnostics: Diagnostics
) extends AnalyticsReportRepository,
      AnalyticsReportSnapshotPublisher {
  private val collection = Mongo4catsCollections.documents(database, MongoCollections.AnalyticsReportSnapshots)
  private val control = Mongo4catsCollections.documents(database, MongoCollections.AnalyticsReportControl)
  private val reservations = Mongo4catsCollections.documents(database, MongoCollections.AnalyticsReportRuns)

  override def latest: RepositoryIO[Option[AnalyticsReportSnapshot]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "analyticsReport.latest")(transactionRunner.run { session =>
        findOne(session, control, MongoFilter.eq(MongoFields.Id, AnalyticsReportSnapshotDocument.ControlId)).flatMap {
          case Some(document) =>
            MongoHiringPersistenceCodecs
              .decodeAnalyticsReportControl(document)
              .toOption
              .filter(_.state == AnalyticsReportSnapshotDocument.Published) match {
              case Some(state) if state.generation >= 0L && state.lastPublishedRevision >= 0L =>
                findOne(
                  session,
                  collection,
                  MongoFilter.and(
                    MongoFilter.eq(MongoFields.Id, AnalyticsReportSnapshotDocument.CurrentId),
                    MongoFilter.eq(MongoFields.State, AnalyticsReportSnapshotDocument.Published),
                    MongoFilter.eq(MongoFields.Generation, state.generation),
                    MongoFilter.eq(MongoFields.Revision, state.lastPublishedRevision),
                    MongoFilter.gt(MongoFields.ExpiresAt, new Date())
                  )
                ).subflatMap {
                  case None           => Right(None)
                  case Some(snapshot) =>
                    AnalyticsReportSnapshotDocument
                      .read(snapshot)
                      .toRight(RepositoryError.InvalidStoredData)
                      .map(Some(_))
                }
              case _ => RepositoryIO.fromEither(Right(None))
            }
          case _ => RepositoryIO.fromEither(Right(None))
        }
      })(_ => Left(RepositoryError.Unavailable))

  override def reserve(
      runId: AnalyticsRunId,
      rangeFingerprint: AnalyticsRangeFingerprint,
      now: Instant,
      reservationExpiresAt: Instant
  ): RepositoryIO[AnalyticsReportRunReservation] =
    if (!reservationExpiresAt.isAfter(now)) RepositoryIO.fromEither(Left(RepositoryError.Conflict))
    else {
      val result = transactionRunner.run { session =>
        findOne(session, reservations, MongoFilter.eq(MongoFields.Id, runId.value)).flatMap {
          case Some(existing) =>
            readReservation(existing) match {
              case Some(reservation) if reservation.rangeFingerprint == rangeFingerprint =>
                findOne(session, control, MongoFilter.eq(MongoFields.Id, AnalyticsReportSnapshotDocument.ControlId))
                  .flatMap {
                    case Some(state)
                        if existing.getString(MongoFields.State) == "Reserved" &&
                          (Option(state.get(MongoFields.Generation, classOf[java.lang.Long]))
                            .exists(_.longValue() > reservation.generation) ||
                            Option(state.get(MongoFields.LastPublishedRevision, classOf[java.lang.Long]))
                              .exists(_.longValue() >= reservation.revision)) =>
                      updateOne(
                        session,
                        control,
                        MongoFilter.eq(MongoFields.Id, AnalyticsReportSnapshotDocument.ControlId),
                        MongoUpdate.inc(MongoFields.NextRevision, 1L)
                      ).flatMap {
                        case Some(incremented) if incremented.getMatchedCount == 1L =>
                          findOne(
                            session,
                            control,
                            MongoFilter.eq(MongoFields.Id, AnalyticsReportSnapshotDocument.ControlId)
                          )
                            .flatMap {
                              case Some(updatedState) =>
                                val generation =
                                  Option(updatedState.get(MongoFields.Generation, classOf[java.lang.Long]))
                                    .fold(-1L)(_.longValue())
                                val revision =
                                  Option(updatedState.get(MongoFields.NextRevision, classOf[java.lang.Long]))
                                    .fold(-1L)(_.longValue())
                                val refreshed = reservation.copy(generation = generation, revision = revision)
                                if (generation < reservation.generation || revision <= reservation.revision)
                                  RepositoryIO.fromEither(Left(RepositoryError.InvalidStoredData))
                                else
                                  updateOne(
                                    session,
                                    reservations,
                                    MongoFilter.and(
                                      MongoFilter.eq(MongoFields.Id, runId.value),
                                      MongoFilter.eq(MongoFields.State, "Reserved")
                                    ),
                                    MongoUpdate.combine(
                                      MongoUpdate.set(MongoFields.Generation, generation),
                                      MongoUpdate.set(MongoFields.Revision, revision),
                                      MongoUpdate.set(MongoFields.CreatedAt, Date.from(now)),
                                      MongoUpdate.set(MongoFields.ExpiresAt, Date.from(reservationExpiresAt))
                                    )
                                  ).subflatMap {
                                    case Some(result) if result.getMatchedCount == 1L => Right(refreshed)
                                    case Some(_)                                      => Left(RepositoryError.Conflict)
                                    case None => Left(RepositoryError.MissingWriteResult)
                                  }
                              case None => RepositoryIO.fromEither(Left(RepositoryError.InvalidStoredData))
                            }
                        case Some(_) => RepositoryIO.fromEither(Left(RepositoryError.Conflict))
                        case None    => RepositoryIO.fromEither(Left(RepositoryError.MissingWriteResult))
                      }
                    case Some(_) => RepositoryIO.fromEither(Right(reservation))
                    case None    => RepositoryIO.fromEither(Left(RepositoryError.InvalidStoredData))
                  }
              case Some(_) => RepositoryIO.fromEither(Left(RepositoryError.Conflict))
              case None    => RepositoryIO.fromEither(Left(RepositoryError.InvalidStoredData))
            }
          case None =>
            val increment = MongoUpdate.combine(
              MongoUpdate.setOnInsert(MongoFields.Id, AnalyticsReportSnapshotDocument.ControlId),
              MongoUpdate.inc(MongoFields.NextRevision, 1L)
            )
            updateOne(
              session,
              control,
              MongoFilter.eq(MongoFields.Id, AnalyticsReportSnapshotDocument.ControlId),
              increment
            )
              .flatMap {
                case Some(result) if result.getMatchedCount == 1L =>
                  findOne(session, control, MongoFilter.eq(MongoFields.Id, AnalyticsReportSnapshotDocument.ControlId))
                    .flatMap {
                      case Some(state) =>
                        val generation = Option(state.get(MongoFields.Generation, classOf[java.lang.Long]))
                          .fold(-1L)(_.longValue())
                        val revision = Option(state.get(MongoFields.NextRevision, classOf[java.lang.Long]))
                          .fold(-1L)(_.longValue())
                        if (generation < 0L || revision < 1L)
                          RepositoryIO.fromEither(Left(RepositoryError.InvalidStoredData))
                        else {
                          val reservation = AnalyticsReportRunReservation(runId, rangeFingerprint, generation, revision)
                          val document = MongoHiringPersistenceCodecs.analyticsReportRun(
                            MongoHiringPersistenceCodecs.StoredAnalyticsReportRun(
                              runId.value,
                              rangeFingerprint.value,
                              generation,
                              revision,
                              Some("Reserved"),
                              Some(Date.from(now)),
                              Some(Date.from(reservationExpiresAt))
                            )
                          )
                          insertOne(session, reservations, document).as(reservation)
                        }
                      case None => RepositoryIO.fromEither(Left(RepositoryError.InvalidStoredData))
                    }
                case Some(_) => RepositoryIO.fromEither(Left(RepositoryError.Conflict))
                case None    => RepositoryIO.fromEither(Left(RepositoryError.MissingWriteResult))
              }
        }
      }
      MongoRepositorySupport
        .repositoryGuard(diagnostics, "analyticsReport.reserve")(result.recoverWith { case RepositoryError.Conflict =>
          findOne(None, reservations, MongoFilter.eq(MongoFields.Id, runId.value)).subflatMap { existing =>
            existing match {
              case Some(document) if readReservation(document).isEmpty => Left(RepositoryError.InvalidStoredData)
              case Some(document)                                      =>
                readReservation(document).filter(_.rangeFingerprint == rangeFingerprint) match {
                  case Some(reservation) => Right(reservation)
                  case None              => Left(RepositoryError.Conflict)
                }
              case None => Left(RepositoryError.Conflict)
            }
          }
        })(_ => Left(RepositoryError.Unavailable))

    }

  override def publish(
      reservation: AnalyticsReportRunReservation,
      snapshot: AnalyticsReportSnapshot,
      expiresAt: Instant
  ): RepositoryIO[Unit] =
    if (!expiresAt.isAfter(snapshot.asOf))
      RepositoryIO.fromEither(Left(RepositoryError.Conflict))
    else
      MongoRepositorySupport
        .repositoryGuard(diagnostics, "analyticsReport.publish")(transactionRunner.run { session =>
          for {
            storedRun <- findOne(session, reservations, MongoFilter.eq(MongoFields.Id, reservation.runId.value))
            controlState <- findOne(
              session,
              control,
              MongoFilter.eq(MongoFields.Id, AnalyticsReportSnapshotDocument.ControlId)
            )
            result <- (storedRun.flatMap(readReservation), controlState) match {
              case (Some(stored), Some(state)) if stored == reservation =>
                val currentGeneration = Option(state.get(MongoFields.Generation, classOf[java.lang.Long]))
                  .fold(-1L)(_.longValue())
                val lastRevision = Option(state.get(MongoFields.LastPublishedRevision, classOf[java.lang.Long]))
                  .fold(0L)(_.longValue())
                val currentState = state.getString(MongoFields.State)
                val stateAllowsPublish =
                  currentState == AnalyticsReportSnapshotDocument.Published ||
                    currentState == AnalyticsReportSnapshotDocument.Unpublished
                if (currentGeneration != reservation.generation || !stateAllowsPublish)
                  RepositoryIO.fromEither(Left(RepositoryError.Conflict))
                else if (reservation.revision <= lastRevision) {
                  val sameRunAlreadyPublished = lastRevision == reservation.revision &&
                    state.getString(MongoFields.LastRunId) == reservation.runId.value
                  if (!sameRunAlreadyPublished) RepositoryIO.fromEither(Left(RepositoryError.Conflict))
                  else
                    findOne(
                      session,
                      collection,
                      MongoFilter.eq(MongoFields.Id, AnalyticsReportSnapshotDocument.CurrentId)
                    ).flatMap {
                      case Some(existing)
                          if Option(existing.get(MongoFields.Generation, classOf[java.lang.Long]))
                            .exists(_.longValue() == reservation.generation) &&
                            Option(existing.get(MongoFields.Revision, classOf[java.lang.Long]))
                              .exists(_.longValue() == reservation.revision) &&
                            existing.getString(MongoFields.RunId) == reservation.runId.value &&
                            Option(existing.getDate(MongoFields.ExpiresAt)).exists(_.after(new Date())) =>
                        RepositoryIO.fromEither(Right(()))
                      case Some(existing)
                          if Option(existing.getDate(MongoFields.ExpiresAt)).exists(_.after(new Date())) =>
                        RepositoryIO.fromEither(Left(RepositoryError.Conflict))
                      case _ =>
                        val restored = AnalyticsReportSnapshotDocument
                          .write(snapshot, expiresAt)
                          .append(MongoFields.Generation, reservation.generation)
                          .append(MongoFields.Revision, reservation.revision)
                          .append(MongoFields.RunId, reservation.runId.value)
                        replaceOne(
                          session,
                          collection,
                          MongoFilter.eq(MongoFields.Id, AnalyticsReportSnapshotDocument.CurrentId),
                          restored,
                          new ReplaceOptions().upsert(true)
                        ).subflatMap(
                          _.fold[Either[RepositoryError, Unit]](Left(RepositoryError.MissingWriteResult))(_ =>
                            Right(())
                          )
                        )
                    }
                } else {
                  val updateControl = MongoUpdate.combine(
                    MongoUpdate.set(MongoFields.State, AnalyticsReportSnapshotDocument.Published),
                    MongoUpdate.set(MongoFields.LastPublishedRevision, reservation.revision),
                    MongoUpdate.set(MongoFields.LastRunId, reservation.runId.value),
                    MongoUpdate.unset(MongoFields.HiddenAt)
                  )
                  val controlFilter = MongoFilter.and(
                    MongoFilter.eq(MongoFields.Id, AnalyticsReportSnapshotDocument.ControlId),
                    MongoFilter.eq(MongoFields.Generation, reservation.generation),
                    MongoFilter.lt(MongoFields.LastPublishedRevision, reservation.revision),
                    MongoFilter.in(
                      MongoFields.State,
                      List(AnalyticsReportSnapshotDocument.Published, AnalyticsReportSnapshotDocument.Unpublished)
                    )
                  )
                  updateOne(session, control, controlFilter, updateControl).flatMap {
                    case Some(updateResult) if updateResult.getMatchedCount == 1L =>
                      val snapshotDocument = AnalyticsReportSnapshotDocument
                        .write(snapshot, expiresAt)
                        .append(MongoFields.Generation, reservation.generation)
                        .append(MongoFields.Revision, reservation.revision)
                        .append(MongoFields.RunId, reservation.runId.value)
                      replaceOne(
                        session,
                        collection,
                        MongoFilter.eq(MongoFields.Id, AnalyticsReportSnapshotDocument.CurrentId),
                        snapshotDocument,
                        new ReplaceOptions().upsert(true)
                      ).flatMap {
                        case Some(_) =>
                          updateOne(
                            session,
                            reservations,
                            MongoFilter.and(
                              MongoFilter.eq(MongoFields.Id, reservation.runId.value),
                              MongoFilter.eq(MongoFields.Generation, reservation.generation),
                              MongoFilter.eq(MongoFields.Revision, reservation.revision)
                            ),
                            MongoUpdate.set(MongoFields.State, "Published")
                          ).subflatMap {
                            case Some(_) => Right(())
                            case None    => Left(RepositoryError.MissingWriteResult)
                          }
                        case None => RepositoryIO.fromEither(Left(RepositoryError.MissingWriteResult))
                      }
                    case Some(_) => RepositoryIO.fromEither(Left(RepositoryError.Conflict))
                    case None    => RepositoryIO.fromEither(Left(RepositoryError.MissingWriteResult))
                  }
                }
              case (Some(_), Some(_))              => RepositoryIO.fromEither(Left(RepositoryError.Conflict))
              case (Some(_), None)                 => RepositoryIO.fromEither(Left(RepositoryError.InvalidStoredData))
              case (None, _) if storedRun.nonEmpty => RepositoryIO.fromEither(Left(RepositoryError.InvalidStoredData))
              case (None, _)                       => RepositoryIO.fromEither(Left(RepositoryError.Conflict))
            }
          } yield result
        })(_ => Left(RepositoryError.Unavailable))

  private def findOne(
      session: Option[ClientSession[IO]],
      collection: IO[MongoCollection[IO, Document]],
      filter: MongoFilter
  ): RepositoryIO[Option[Document]] = MongoAnalyticsRepositoryOperations.findOne(collection, session, filter)

  private def updateOne(
      session: Option[ClientSession[IO]],
      collection: IO[MongoCollection[IO, Document]],
      filter: MongoFilter,
      update: MongoUpdate
  ): RepositoryIO[Option[com.mongodb.client.result.UpdateResult]] =
    MongoAnalyticsRepositoryOperations.updateOne(collection, session, filter, update, new UpdateOptions)

  private def replaceOne(
      session: Option[ClientSession[IO]],
      collection: IO[MongoCollection[IO, Document]],
      filter: MongoFilter,
      replacement: Document,
      options: ReplaceOptions
  ): RepositoryIO[Option[com.mongodb.client.result.UpdateResult]] =
    MongoAnalyticsRepositoryOperations.replaceOne(collection, session, filter, replacement, options)

  private def insertOne(
      session: Option[ClientSession[IO]],
      collection: IO[MongoCollection[IO, Document]],
      document: Document
  ): RepositoryIO[Unit] =
    MongoAnalyticsRepositoryOperations
      .insertOne(collection, session, document)
      .subflatMap(_.fold[Either[RepositoryError, Unit]](Left(RepositoryError.MissingWriteResult))(_ => Right(())))

  private def readReservation(document: Document): Option[AnalyticsReportRunReservation] =
    for {
      stored <- MongoHiringPersistenceCodecs.decodeAnalyticsReportRun(document).toOption
      runId <- AnalyticsRunId.from(stored._id)
      fingerprint <- AnalyticsRangeFingerprint.from(stored.rangeFingerprint)
    } yield AnalyticsReportRunReservation(runId, fingerprint, stored.generation, stored.revision)

}

object MongoAnalyticsReportRepository {
  def transactional(
      database: MongoDatabase[IO],
      client: MongoClient[IO],
      diagnostics: Diagnostics
  ): MongoAnalyticsReportRepository =
    new MongoAnalyticsReportRepository(
      database,
      MongoTransactionRunner.sessions(client, RepositoryError.Conflict, diagnostics = diagnostics),
      diagnostics
    )
}

private[mongo] object AnalyticsReportSnapshotDocument {
  val CurrentId = "current"
  val ControlId = "analytics-report"
  val Hidden = "Hidden"
  val Unpublished = "Unpublished"
  val Published = "Published"

  def write(snapshot: AnalyticsReportSnapshot, expiresAt: Instant): Document = {
    val document = new Document(MongoFields.Id, CurrentId)
      .append(MongoFields.State, "Published")
      .append(MongoFields.AsOf, Date.from(snapshot.asOf))
      .append(MongoFields.ExpiresAt, Date.from(expiresAt))
      .append(MongoFields.Funnel, snapshot.funnel.map(writeFunnel).asJava)
      .append(MongoFields.SkillPostingActivity, snapshot.skillPostingActivity.map(writeSkill).asJava)
    snapshot.timeToHire.fold(document)(value => document.append(MongoFields.TimeToHire, writeTimeToHire(value)))
  }

  def read(document: Document): Option[AnalyticsReportSnapshot] =
    for {
      asOf <- Option(document.getDate(MongoFields.AsOf)).map(_.toInstant)
      funnel <- documents(document, MongoFields.Funnel).flatMap(_.traverse(readFunnel))
      skills <- documents(document, MongoFields.SkillPostingActivity).flatMap(_.traverse(readSkill))
    } yield AnalyticsReportSnapshot(
      asOf,
      funnel,
      Option(document.get(MongoFields.TimeToHire, classOf[Document])).flatMap(readTimeToHire),
      skills
    )

  private def writeFunnel(value: AnalyticsFunnelDay): Document =
    new Document(MongoFields.Day, Date.from(value.day))
      .append(MongoFields.Created, value.created)
      .append(MongoFields.Accepted, value.accepted)
      .append(MongoFields.Declined, value.declined)
      .append(MongoFields.Interview, value.interview)
      .append(MongoFields.Hired, value.hired)
      .append(MongoFields.Rejected, value.rejected)

  private def writeTimeToHire(value: AnalyticsTimeToHire): Document =
    new Document(MongoFields.P50Hours, value.p50Hours)
      .append(MongoFields.P75Hours, value.p75Hours)
      .append(MongoFields.P90Hours, value.p90Hours)
      .append(MongoFields.P95Hours, value.p95Hours)
      .append(MongoFields.EligibleCount, value.eligibleCount)
      .append(MongoFields.ExcludedCount, value.excludedCount)

  private def writeSkill(value: AnalyticsSkillPostingDay): Document =
    new Document(MongoFields.Day, Date.from(value.day))
      .append(MongoFields.Skill, value.skill)
      .append(MongoFields.Postings, value.postings)

  private def readFunnel(document: Document): Option[AnalyticsFunnelDay] =
    for {
      day <- Option(document.getDate(MongoFields.Day)).map(_.toInstant)
      created <- count(document, MongoFields.Created)
      accepted <- count(document, MongoFields.Accepted)
      declined <- count(document, MongoFields.Declined)
      interview <- count(document, MongoFields.Interview)
      hired <- count(document, MongoFields.Hired)
      rejected <- count(document, MongoFields.Rejected)
    } yield AnalyticsFunnelDay(day, created, accepted, declined, interview, hired, rejected)

  private def readTimeToHire(document: Document): Option[AnalyticsTimeToHire] =
    for {
      p50 <- decimal(document, MongoFields.P50Hours)
      p75 <- decimal(document, MongoFields.P75Hours)
      p90 <- decimal(document, MongoFields.P90Hours)
      p95 <- decimal(document, MongoFields.P95Hours)
      eligible <- count(document, MongoFields.EligibleCount)
      excluded <- count(document, MongoFields.ExcludedCount)
    } yield AnalyticsTimeToHire(p50, p75, p90, p95, eligible, excluded)

  private def readSkill(document: Document): Option[AnalyticsSkillPostingDay] =
    for {
      day <- Option(document.getDate(MongoFields.Day)).map(_.toInstant)
      skill <- Option(document.getString(MongoFields.Skill))
      postings <- count(document, MongoFields.Postings)
    } yield AnalyticsSkillPostingDay(day, skill, postings)

  private def documents(document: Document, field: String): Option[List[Document]] =
    Option(document.get(field, classOf[java.util.List[?]])).map(_.toArray.toList.collect { case value: Document =>
      value
    })

  private def count(document: Document, field: String): Option[Long] =
    Option(document.get(field)).collect { case value: Number => value.longValue }

  private def decimal(document: Document, field: String): Option[Double] =
    Option(document.get(field)).collect { case value: Number => value.doubleValue }
}
