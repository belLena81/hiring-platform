package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.AccountDeletionStatus
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.repository.protocol.{
  AnalyticsErasureRequestRepository,
  AnalyticsFunnelDay,
  AnalyticsReportRepository,
  AnalyticsReportRunReservation,
  AnalyticsReportSnapshot,
  AnalyticsReportSnapshotPublisher,
  AnalyticsSkillPostingDay,
  AnalyticsTimeToHire,
  MutationWriteContext,
  RepositoryError
}
import com.example.graphQL.cats.service.Diagnostics
import com.mongodb.client.model.{Filters, ReplaceOptions, UpdateOptions, Updates}
import com.mongodb.reactivestreams.client.{MongoClient, MongoDatabase}
import org.bson.Document

import java.util.{Date, UUID}
import java.time.Instant
import scala.jdk.CollectionConverters.*

/** Stores one idempotent erasure request per subject in the same Mongo transaction as account deletion. */
final class MongoAnalyticsErasureRequestRepository(
    database: MongoDatabase,
    transactionRunner: MongoTransactionRunner = MongoTransactionRunner.noTransaction,
    diagnostics: Diagnostics = Diagnostics.noop
) extends AnalyticsErasureRequestRepository {
  private val collection = database.getCollection("analytics_erasure_requests")
  private val subjectFences = database.getCollection("outbox_subject_fences")
  private val outbox = database.getCollection("event_outbox")
  private val users = database.getCollection("users")
  private val completions = database.getCollection("analytics_erasure_completions")
  private val workerHeartbeats = database.getCollection("analytics_worker_heartbeats")

  override def workerReady(now: Instant): IO[Either[RepositoryError, Unit]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "analyticsErasure.workerReady")(
        PublisherBridge
          .first(
            workerHeartbeats.find(
              Filters.and(
                Filters.eq("_id", "analytics-erasure"),
                Filters.eq("state", "Ready"),
                Filters.gt("leaseUntil", Date.from(now))
              )
            )
          )
          .map(_.fold[Either[RepositoryError, Unit]](Left(RepositoryError.Unavailable))(_ => Right(())))
      )(_ => Left(RepositoryError.Unavailable))
      .value

  override def enqueue(
      userId: UserId,
      now: Instant,
      context: MutationWriteContext
  ): IO[Either[RepositoryError, String]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "analyticsErasure.enqueue")(IO.delay(UUID.randomUUID().toString).flatMap {
        freshReceiptId =>
          MongoMutationWriteContext.run(context, transactionRunner, transactionRequired = true) { session =>
            val requestId = userId.value.toString
            val completion = session.fold(
              PublisherBridge.first(completions.find(Filters.eq("_id", requestId)))
            )(active => PublisherBridge.first(completions.find(active, Filters.eq("_id", requestId))))
            completion
              .flatMap {
                case Some(document) =>
                  Option(document.getString("receiptId")) match {
                    case Some(value) => IO.pure(Right(value))
                    case None        =>
                      val update = Updates.set("receiptId", freshReceiptId)
                      val result = session.fold(
                        PublisherBridge.first(completions.updateOne(Filters.eq("_id", requestId), update))
                      )(active =>
                        PublisherBridge.first(completions.updateOne(active, Filters.eq("_id", requestId), update))
                      )
                      result.map(
                        _.fold[Either[RepositoryError, String]](Left(RepositoryError.MissingWriteResult))(_ =>
                          Right(freshReceiptId)
                        )
                      )
                  }
                case None =>
                  val existing = session.fold(
                    PublisherBridge.first(collection.find(Filters.eq("_id", requestId)))
                  )(active => PublisherBridge.first(collection.find(active, Filters.eq("_id", requestId))))
                  existing.flatMap {
                    case Some(document) =>
                      Option(document.getString("receiptId")) match {
                        case Some(value) => IO.pure(Right(value))
                        case None        =>
                          val update = Updates.set("receiptId", freshReceiptId)
                          val result = session.fold(
                            PublisherBridge.first(collection.updateOne(Filters.eq("_id", requestId), update))
                          )(active =>
                            PublisherBridge.first(collection.updateOne(active, Filters.eq("_id", requestId), update))
                          )
                          result.map(
                            _.fold[Either[RepositoryError, String]](Left(RepositoryError.MissingWriteResult))(_ =>
                              Right(freshReceiptId)
                            )
                          )
                      }
                    case None =>
                      val fence = session.fold(
                        PublisherBridge.first(subjectFences.find(Filters.eq("_id", requestId)))
                      )(active => PublisherBridge.first(subjectFences.find(active, Filters.eq("_id", requestId))))
                      fence.flatMap { currentFence =>
                        val transactionalIds = currentFence
                          .flatMap(value => Option(value.getList("transactionalIds", classOf[String])))
                          .fold(List.empty[String])(_.asScala.toList.distinct.sorted)
                        val request = new Document("_id", requestId)
                          .append("requestedAt", Date.from(now))
                          .append("receiptId", freshReceiptId)
                          .append("state", "Pending")
                          .append("fencingVersion", 1)
                          .append("transactionalIds", transactionalIds.asJava)
                        val insert = session.fold(
                          PublisherBridge.first(collection.insertOne(request))
                        )(active => PublisherBridge.first(collection.insertOne(active, request)))
                        insert.flatMap {
                          case Some(_) =>
                            val reportControl = database.getCollection("analytics_report_control")
                            val hide = Updates.combine(
                              Updates.inc("generation", 1L),
                              Updates.set("state", AnalyticsReportSnapshotDocument.Hidden),
                              Updates.set("hiddenAt", Date.from(now))
                            )
                            val hideResult = session.fold(
                              PublisherBridge.first(
                                reportControl
                                  .updateOne(Filters.eq("_id", AnalyticsReportSnapshotDocument.ControlId), hide)
                              )
                            )(active =>
                              PublisherBridge.first(
                                reportControl
                                  .updateOne(active, Filters.eq("_id", AnalyticsReportSnapshotDocument.ControlId), hide)
                              )
                            )
                            hideResult.map {
                              case Some(result) if result.getMatchedCount == 1L => Right(freshReceiptId)
                              case Some(_)                                      => Left(RepositoryError.Conflict)
                              case None => Left(RepositoryError.MissingWriteResult)
                            }
                          case None => IO.pure(Left(RepositoryError.MissingWriteResult))
                        }
                      }
                  }
              }
          }
      })(_ => Left(RepositoryError.Unavailable))
      .value

  override def statusForSubject(
      userId: UserId,
      receiptId: String
  ): IO[Either[RepositoryError, AccountDeletionStatus]] =
    if (scala.util.Try(UUID.fromString(receiptId)).isFailure) IO.pure(Right(AccountDeletionStatus.NotFound))
    else
      MongoRepositorySupport
        .repositoryGuard(diagnostics, "analyticsErasure.statusForSubject")({
          val requestFilter = Filters.and(
            Filters.eq("_id", userId.value.toString),
            Filters.eq("receiptId", receiptId)
          )
          val completionFilter = Filters.and(
            Filters.eq("_id", userId.value.toString),
            Filters.eq("receiptId", receiptId)
          )
          PublisherBridge
            .first(collection.find(requestFilter))
            .flatMap {
              case Some(document) if document.getString("state") == "Complete" =>
                IO.pure(Right(AccountDeletionStatus.Complete))
              case Some(document) if Set("Pending", "Processing").contains(document.getString("state")) =>
                IO.pure(Right(AccountDeletionStatus.Pending))
              case Some(_) => IO.pure(Left(RepositoryError.InvalidStoredData))
              case None    =>
                PublisherBridge
                  .first(completions.find(completionFilter))
                  .map(
                    _.fold[Either[RepositoryError, AccountDeletionStatus]](Right(AccountDeletionStatus.NotFound))(_ =>
                      Right(AccountDeletionStatus.Complete)
                    )
                  )
            }
        })(_ => Left(RepositoryError.Unavailable))
        .value

  override def purgeSubjectOutbox(userId: UserId): IO[Either[RepositoryError, Unit]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "analyticsErasure.purgeSubjectOutbox")(
        PublisherBridge
          .first(outbox.deleteMany(Filters.in("subjectIds", userId.value.toString)))
          .map(_.fold[Either[RepositoryError, Unit]](Left(RepositoryError.MissingWriteResult))(_ => Right(())))
      )(_ => Left(RepositoryError.Unavailable))
      .value

  override def markComplete(userId: UserId, now: Instant): IO[Either[RepositoryError, Unit]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "analyticsErasure.markComplete")(transactionRunner.run { session =>
        val requestId = userId.value.toString
        val user = session.fold(
          PublisherBridge.first(users.find(Filters.eq("_id", requestId)))
        )(active => PublisherBridge.first(users.find(active, Filters.eq("_id", requestId))))
        user
          .flatMap {
            case Some(document) if document.getString("accountStatus") == "Deleted" =>
              val filter = Filters.and(Filters.eq("_id", requestId), Filters.in("state", "Pending", "Processing"))
              val update = Updates.combine(
                Updates.set("state", "Complete"),
                Updates.set("completedAt", Date.from(now)),
                Updates.set("expiresAt", Date.from(now.plusSeconds(31L * 24L * 60L * 60L)))
              )
              val requests = session.fold(
                PublisherBridge.first(collection.updateOne(filter, update))
              )(active => PublisherBridge.first(collection.updateOne(active, filter, update)))
              requests.flatMap {
                case Some(result) if result.getMatchedCount == 1L => persistCompletion(session, requestId, now)
                case Some(_)                                      =>
                  val completed = session.fold(
                    PublisherBridge.first(
                      collection.find(Filters.and(Filters.eq("_id", requestId), Filters.eq("state", "Complete")))
                    )
                  )(active =>
                    PublisherBridge.first(
                      collection
                        .find(active, Filters.and(Filters.eq("_id", requestId), Filters.eq("state", "Complete")))
                    )
                  )
                  completed.flatMap(
                    _.fold(IO.pure(Left(RepositoryError.Conflict)))(_ => persistCompletion(session, requestId, now))
                  )
                case None => IO.pure(Left(RepositoryError.MissingWriteResult))
              }
            case _ => IO.pure(Left(RepositoryError.Conflict))
          }
      })(_ => Left(RepositoryError.Unavailable))
      .value

  private def persistCompletion(
      session: Option[com.mongodb.reactivestreams.client.ClientSession],
      userId: String,
      now: Instant
  ): IO[Either[RepositoryError, Unit]] = {
    val request = session.fold(
      PublisherBridge.first(collection.find(Filters.eq("_id", userId)))
    )(active => PublisherBridge.first(collection.find(active, Filters.eq("_id", userId))))
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "analyticsErasure.persistCompletion")(
        request
          .flatMap { stored =>
            val updates = List(
              Some(Updates.setOnInsert("_id", userId)),
              Some(Updates.setOnInsert("completedAt", Date.from(now))),
              stored.flatMap(value => Option(value.getString("receiptId"))).map(Updates.setOnInsert("receiptId", _))
            ).flatten
            val filter = Filters.eq("_id", userId)
            val update = Updates.combine(updates.asJava)
            val result = session.fold(
              PublisherBridge.first(completions.updateOne(filter, update, new UpdateOptions().upsert(true)))
            )(active =>
              PublisherBridge.first(completions.updateOne(active, filter, update, new UpdateOptions().upsert(true)))
            )
            result
              .map {
                case Some(_) => Right(())
                case _       => Left(RepositoryError.MissingWriteResult)
              }
          }
      )(_ => Left(RepositoryError.Unavailable))
      .value
  }
}

object MongoAnalyticsErasureRequestRepository {
  def transactional(
      database: MongoDatabase,
      client: MongoClient,
      diagnostics: Diagnostics = Diagnostics.noop
  ): MongoAnalyticsErasureRequestRepository =
    new MongoAnalyticsErasureRequestRepository(
      database,
      MongoTransactionRunner.sessions(client, RepositoryError.Conflict, diagnostics = diagnostics),
      diagnostics
    )
}

/** Publishes complete analytics snapshots atomically and exposes the newest valid one to the API. */
final class MongoAnalyticsReportRepository(
    database: MongoDatabase,
    transactionRunner: MongoTransactionRunner = MongoTransactionRunner.noTransaction,
    diagnostics: Diagnostics = Diagnostics.noop
) extends AnalyticsReportRepository,
      AnalyticsReportSnapshotPublisher {
  private val collection = database.getCollection("analytics_report_snapshots")
  private val control = database.getCollection("analytics_report_control")
  private val reservations = database.getCollection("analytics_report_runs")

  override def latest: IO[Either[RepositoryError, Option[AnalyticsReportSnapshot]]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "analyticsReport.latest")(transactionRunner.run { session =>
        findOne(session, control, Filters.eq("_id", AnalyticsReportSnapshotDocument.ControlId)).flatMap {
          case Some(document) if document.getString("state") == AnalyticsReportSnapshotDocument.Published =>
            val generation = Option(document.get("generation", classOf[java.lang.Long])).fold(-1L)(_.longValue())
            val revision = Option(document.get("lastPublishedRevision", classOf[java.lang.Long]))
              .fold(-1L)(_.longValue())
            if (generation < 0L || revision < 0L) IO.pure(Right(None))
            else
              findOne(
                session,
                collection,
                Filters.and(
                  Filters.eq("_id", AnalyticsReportSnapshotDocument.CurrentId),
                  Filters.eq("state", AnalyticsReportSnapshotDocument.Published),
                  Filters.eq("generation", generation),
                  Filters.eq("revision", revision),
                  Filters.gt("expiresAt", new Date())
                )
              ).map {
                case None           => Right(None)
                case Some(snapshot) =>
                  AnalyticsReportSnapshotDocument.read(snapshot).toRight(RepositoryError.InvalidStoredData).map(Some(_))
              }
          case _ => IO.pure(Right(None))
        }
      })(_ => Left(RepositoryError.Unavailable))
      .value

  override def reserve(
      runId: String,
      rangeFingerprint: String,
      now: Instant,
      reservationExpiresAt: Instant
  ): IO[Either[RepositoryError, AnalyticsReportRunReservation]] =
    if (
      runId == null || runId.trim.isEmpty || rangeFingerprint == null || rangeFingerprint.trim.isEmpty ||
      !reservationExpiresAt.isAfter(now)
    ) IO.pure(Left(RepositoryError.Conflict))
    else {
      val result = transactionRunner.run { session =>
        findOne(session, reservations, Filters.eq("_id", runId)).flatMap {
          case Some(existing) =>
            readReservation(existing) match {
              case Some(reservation) if reservation.rangeFingerprint == rangeFingerprint =>
                findOne(session, control, Filters.eq("_id", AnalyticsReportSnapshotDocument.ControlId)).flatMap {
                  case Some(state)
                      if existing.getString("state") == "Reserved" &&
                        (Option(state.get("generation", classOf[java.lang.Long]))
                          .exists(_.longValue() > reservation.generation) ||
                          Option(state.get("lastPublishedRevision", classOf[java.lang.Long]))
                            .exists(_.longValue() >= reservation.revision)) =>
                    updateOne(
                      session,
                      control,
                      Filters.eq("_id", AnalyticsReportSnapshotDocument.ControlId),
                      Updates.inc("nextRevision", 1L)
                    ).flatMap {
                      case Some(incremented) if incremented.getMatchedCount == 1L =>
                        findOne(session, control, Filters.eq("_id", AnalyticsReportSnapshotDocument.ControlId))
                          .flatMap {
                            case Some(updatedState) =>
                              val generation = Option(updatedState.get("generation", classOf[java.lang.Long]))
                                .fold(-1L)(_.longValue())
                              val revision = Option(updatedState.get("nextRevision", classOf[java.lang.Long]))
                                .fold(-1L)(_.longValue())
                              val refreshed = reservation.copy(generation = generation, revision = revision)
                              if (generation < reservation.generation || revision <= reservation.revision)
                                IO.pure(Left(RepositoryError.InvalidStoredData))
                              else
                                updateOne(
                                  session,
                                  reservations,
                                  Filters.and(Filters.eq("_id", runId), Filters.eq("state", "Reserved")),
                                  Updates.combine(
                                    Updates.set("generation", generation),
                                    Updates.set("revision", revision),
                                    Updates.set("createdAt", Date.from(now)),
                                    Updates.set("expiresAt", Date.from(reservationExpiresAt))
                                  )
                                ).map {
                                  case Some(result) if result.getMatchedCount == 1L => Right(refreshed)
                                  case Some(_)                                      => Left(RepositoryError.Conflict)
                                  case None => Left(RepositoryError.MissingWriteResult)
                                }
                            case None => IO.pure(Left(RepositoryError.InvalidStoredData))
                          }
                      case Some(_) => IO.pure(Left(RepositoryError.Conflict))
                      case None    => IO.pure(Left(RepositoryError.MissingWriteResult))
                    }
                  case Some(_) => IO.pure(Right(reservation))
                  case None    => IO.pure(Left(RepositoryError.InvalidStoredData))
                }
              case Some(_) => IO.pure(Left(RepositoryError.Conflict))
              case None    => IO.pure(Left(RepositoryError.InvalidStoredData))
            }
          case None =>
            val increment = Updates.combine(
              Updates.setOnInsert("_id", AnalyticsReportSnapshotDocument.ControlId),
              Updates.inc("nextRevision", 1L)
            )
            updateOne(session, control, Filters.eq("_id", AnalyticsReportSnapshotDocument.ControlId), increment)
              .flatMap {
                case Some(result) if result.getMatchedCount == 1L =>
                  findOne(session, control, Filters.eq("_id", AnalyticsReportSnapshotDocument.ControlId)).flatMap {
                    case Some(state) =>
                      val generation = Option(state.get("generation", classOf[java.lang.Long]))
                        .fold(-1L)(_.longValue())
                      val revision = Option(state.get("nextRevision", classOf[java.lang.Long]))
                        .fold(-1L)(_.longValue())
                      if (generation < 0L || revision < 1L) IO.pure(Left(RepositoryError.InvalidStoredData))
                      else {
                        val reservation = AnalyticsReportRunReservation(runId, rangeFingerprint, generation, revision)
                        val document = new Document("_id", runId)
                          .append("rangeFingerprint", rangeFingerprint)
                          .append("generation", generation)
                          .append("revision", revision)
                          .append("state", "Reserved")
                          .append("createdAt", Date.from(now))
                          .append("expiresAt", Date.from(reservationExpiresAt))
                        insertOne(session, reservations, document).map(_.map(_ => reservation))
                      }
                    case None => IO.pure(Left(RepositoryError.InvalidStoredData))
                  }
                case Some(_) => IO.pure(Left(RepositoryError.Conflict))
                case None    => IO.pure(Left(RepositoryError.MissingWriteResult))
              }
        }
      }
      MongoRepositorySupport
        .repositoryGuard(diagnostics, "analyticsReport.reserve")(result.flatMap {
          case Left(RepositoryError.Conflict) =>
            findOne(None, reservations, Filters.eq("_id", runId)).map { existing =>
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
          case other => IO.pure(other)
        })(_ => Left(RepositoryError.Unavailable))
        .value
    }

  override def publish(
      reservation: AnalyticsReportRunReservation,
      snapshot: AnalyticsReportSnapshot,
      expiresAt: Instant
  ): IO[Either[RepositoryError, Unit]] =
    if (!expiresAt.isAfter(snapshot.asOf) || reservation.runId.trim.isEmpty)
      IO.pure(Left(RepositoryError.Conflict))
    else
      MongoRepositorySupport
        .repositoryGuard(diagnostics, "analyticsReport.publish")(transactionRunner.run { session =>
          for {
            storedRun <- findOne(session, reservations, Filters.eq("_id", reservation.runId))
            controlState <- findOne(session, control, Filters.eq("_id", AnalyticsReportSnapshotDocument.ControlId))
            result <- (storedRun.flatMap(readReservation), controlState) match {
              case (Some(stored), Some(state)) if stored == reservation =>
                val currentGeneration = Option(state.get("generation", classOf[java.lang.Long]))
                  .fold(-1L)(_.longValue())
                val lastRevision = Option(state.get("lastPublishedRevision", classOf[java.lang.Long]))
                  .fold(0L)(_.longValue())
                val currentState = state.getString("state")
                val stateAllowsPublish =
                  currentState == AnalyticsReportSnapshotDocument.Published ||
                    currentState == AnalyticsReportSnapshotDocument.Unpublished
                if (currentGeneration != reservation.generation || !stateAllowsPublish)
                  IO.pure(Left(RepositoryError.Conflict))
                else if (reservation.revision <= lastRevision) {
                  val sameRunAlreadyPublished = lastRevision == reservation.revision &&
                    state.getString("lastRunId") == reservation.runId
                  if (!sameRunAlreadyPublished) IO.pure(Left(RepositoryError.Conflict))
                  else
                    findOne(
                      session,
                      collection,
                      Filters.eq("_id", AnalyticsReportSnapshotDocument.CurrentId)
                    ).flatMap {
                      case Some(existing)
                          if Option(existing.get("generation", classOf[java.lang.Long]))
                            .exists(_.longValue() == reservation.generation) &&
                            Option(existing.get("revision", classOf[java.lang.Long]))
                              .exists(_.longValue() == reservation.revision) &&
                            existing.getString("runId") == reservation.runId &&
                            Option(existing.getDate("expiresAt")).exists(_.after(new Date())) =>
                        IO.pure(Right(()))
                      case Some(existing) if Option(existing.getDate("expiresAt")).exists(_.after(new Date())) =>
                        IO.pure(Left(RepositoryError.Conflict))
                      case _ =>
                        val restored = AnalyticsReportSnapshotDocument
                          .write(snapshot, expiresAt)
                          .append("generation", reservation.generation)
                          .append("revision", reservation.revision)
                          .append("runId", reservation.runId)
                        replaceOne(
                          session,
                          collection,
                          Filters.eq("_id", AnalyticsReportSnapshotDocument.CurrentId),
                          restored,
                          new ReplaceOptions().upsert(true)
                        ).map(
                          _.fold[Either[RepositoryError, Unit]](Left(RepositoryError.MissingWriteResult))(_ =>
                            Right(())
                          )
                        )
                    }
                } else {
                  val updateControl = Updates.combine(
                    Updates.set("state", AnalyticsReportSnapshotDocument.Published),
                    Updates.set("lastPublishedRevision", reservation.revision),
                    Updates.set("lastRunId", reservation.runId),
                    Updates.unset("hiddenAt")
                  )
                  val controlFilter = Filters.and(
                    Filters.eq("_id", AnalyticsReportSnapshotDocument.ControlId),
                    Filters.eq("generation", reservation.generation),
                    Filters.lt("lastPublishedRevision", reservation.revision),
                    Filters
                      .in(
                        "state",
                        AnalyticsReportSnapshotDocument.Published,
                        AnalyticsReportSnapshotDocument.Unpublished
                      )
                  )
                  updateOne(session, control, controlFilter, updateControl).flatMap {
                    case Some(updateResult) if updateResult.getMatchedCount == 1L =>
                      val snapshotDocument = AnalyticsReportSnapshotDocument
                        .write(snapshot, expiresAt)
                        .append("generation", reservation.generation)
                        .append("revision", reservation.revision)
                        .append("runId", reservation.runId)
                      replaceOne(
                        session,
                        collection,
                        Filters.eq("_id", AnalyticsReportSnapshotDocument.CurrentId),
                        snapshotDocument,
                        new ReplaceOptions().upsert(true)
                      ).flatMap {
                        case Some(_) =>
                          updateOne(
                            session,
                            reservations,
                            Filters.and(
                              Filters.eq("_id", reservation.runId),
                              Filters.eq("generation", reservation.generation),
                              Filters.eq("revision", reservation.revision)
                            ),
                            Updates.set("state", "Published")
                          ).map {
                            case Some(_) => Right(())
                            case None    => Left(RepositoryError.MissingWriteResult)
                          }
                        case None => IO.pure(Left(RepositoryError.MissingWriteResult))
                      }
                    case Some(_) => IO.pure(Left(RepositoryError.Conflict))
                    case None    => IO.pure(Left(RepositoryError.MissingWriteResult))
                  }
                }
              case (Some(_), Some(_))              => IO.pure(Left(RepositoryError.Conflict))
              case (Some(_), None)                 => IO.pure(Left(RepositoryError.InvalidStoredData))
              case (None, _) if storedRun.nonEmpty => IO.pure(Left(RepositoryError.InvalidStoredData))
              case (None, _)                       => IO.pure(Left(RepositoryError.Conflict))
            }
          } yield result
        })(_ => Left(RepositoryError.Unavailable))
        .value

  private def findOne(
      session: Option[com.mongodb.reactivestreams.client.ClientSession],
      collection: com.mongodb.reactivestreams.client.MongoCollection[Document],
      filter: org.bson.conversions.Bson
  ): IO[Option[Document]] =
    session.fold(PublisherBridge.first(collection.find(filter)))(active =>
      PublisherBridge.first(collection.find(active, filter))
    )

  private def updateOne(
      session: Option[com.mongodb.reactivestreams.client.ClientSession],
      collection: com.mongodb.reactivestreams.client.MongoCollection[Document],
      filter: org.bson.conversions.Bson,
      update: org.bson.conversions.Bson
  ): IO[Option[com.mongodb.client.result.UpdateResult]] =
    session.fold(PublisherBridge.first(collection.updateOne(filter, update)))(active =>
      PublisherBridge.first(collection.updateOne(active, filter, update))
    )

  private def replaceOne(
      session: Option[com.mongodb.reactivestreams.client.ClientSession],
      collection: com.mongodb.reactivestreams.client.MongoCollection[Document],
      filter: org.bson.conversions.Bson,
      replacement: Document,
      options: ReplaceOptions
  ): IO[Option[com.mongodb.client.result.UpdateResult]] =
    session.fold(PublisherBridge.first(collection.replaceOne(filter, replacement, options)))(active =>
      PublisherBridge.first(collection.replaceOne(active, filter, replacement, options))
    )

  private def insertOne(
      session: Option[com.mongodb.reactivestreams.client.ClientSession],
      collection: com.mongodb.reactivestreams.client.MongoCollection[Document],
      document: Document
  ): IO[Either[RepositoryError, Unit]] =
    session
      .fold(PublisherBridge.first(collection.insertOne(document)))(active =>
        PublisherBridge.first(collection.insertOne(active, document))
      )
      .map(_.fold[Either[RepositoryError, Unit]](Left(RepositoryError.MissingWriteResult))(_ => Right(())))

  private def readReservation(document: Document): Option[AnalyticsReportRunReservation] =
    for {
      runId <- Option(document.getString("_id"))
      fingerprint <- Option(document.getString("rangeFingerprint"))
      generation <- Option(document.get("generation", classOf[java.lang.Long])).map(_.longValue())
      revision <- Option(document.get("revision", classOf[java.lang.Long])).map(_.longValue())
    } yield AnalyticsReportRunReservation(runId, fingerprint, generation, revision)

}

object MongoAnalyticsReportRepository {
  def transactional(
      database: MongoDatabase,
      client: MongoClient,
      diagnostics: Diagnostics = Diagnostics.noop
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
    val document = new Document("_id", CurrentId)
      .append("state", "Published")
      .append("asOf", Date.from(snapshot.asOf))
      .append("expiresAt", Date.from(expiresAt))
      .append("funnel", snapshot.funnel.map(writeFunnel).asJava)
      .append("skillPostingActivity", snapshot.skillPostingActivity.map(writeSkill).asJava)
    snapshot.timeToHire.fold(document)(value => document.append("timeToHire", writeTimeToHire(value)))
  }

  def read(document: Document): Option[AnalyticsReportSnapshot] =
    for {
      asOf <- Option(document.getDate("asOf")).map(_.toInstant)
      funnel <- documents(document, "funnel").flatMap(_.traverse(readFunnel))
      skills <- documents(document, "skillPostingActivity").flatMap(_.traverse(readSkill))
    } yield AnalyticsReportSnapshot(
      asOf,
      funnel,
      Option(document.get("timeToHire", classOf[Document])).flatMap(readTimeToHire),
      skills
    )

  private def writeFunnel(value: AnalyticsFunnelDay): Document =
    new Document("day", Date.from(value.day))
      .append("created", value.created)
      .append("accepted", value.accepted)
      .append("declined", value.declined)
      .append("interview", value.interview)
      .append("hired", value.hired)
      .append("rejected", value.rejected)

  private def writeTimeToHire(value: AnalyticsTimeToHire): Document =
    new Document("p50Hours", value.p50Hours)
      .append("p75Hours", value.p75Hours)
      .append("p90Hours", value.p90Hours)
      .append("p95Hours", value.p95Hours)
      .append("eligibleCount", value.eligibleCount)
      .append("excludedCount", value.excludedCount)

  private def writeSkill(value: AnalyticsSkillPostingDay): Document =
    new Document("day", Date.from(value.day)).append("skill", value.skill).append("postings", value.postings)

  private def readFunnel(document: Document): Option[AnalyticsFunnelDay] =
    for {
      day <- Option(document.getDate("day")).map(_.toInstant)
      created <- count(document, "created")
      accepted <- count(document, "accepted")
      declined <- count(document, "declined")
      interview <- count(document, "interview")
      hired <- count(document, "hired")
      rejected <- count(document, "rejected")
    } yield AnalyticsFunnelDay(day, created, accepted, declined, interview, hired, rejected)

  private def readTimeToHire(document: Document): Option[AnalyticsTimeToHire] =
    for {
      p50 <- decimal(document, "p50Hours")
      p75 <- decimal(document, "p75Hours")
      p90 <- decimal(document, "p90Hours")
      p95 <- decimal(document, "p95Hours")
      eligible <- count(document, "eligibleCount")
      excluded <- count(document, "excludedCount")
    } yield AnalyticsTimeToHire(p50, p75, p90, p95, eligible, excluded)

  private def readSkill(document: Document): Option[AnalyticsSkillPostingDay] =
    for {
      day <- Option(document.getDate("day")).map(_.toInstant)
      skill <- Option(document.getString("skill"))
      postings <- count(document, "postings")
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
