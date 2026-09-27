package com.example.hiring.analytics.mongo

import com.example.hiring.analytics.batch.{AnalyticsReportOutput, AnalyticsReportPublisher, AnalyticsReportReservation}
import com.example.hiring.analytics.erasure.{ErasureClaim, ErasurePhase}

import com.example.hiring.analytics.*
import com.example.hiring.analytics.mongo.MongoAnalyticsReportRecords.*

import cats.effect.IO
import cats.syntax.all.*
import com.mongodb.client.{ClientSession, MongoClient, MongoCollection, MongoDatabase}
import com.mongodb.client.model.{Filters, ReplaceOptions, Updates}
import org.bson.Document
import org.bson.conversions.Bson

import java.time.Instant
import java.util.Date
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** Sync Mongo calls are isolated on the blocking pool. Mongo owns revision allocation and the publication CAS. */
final class MongoAnalyticsReportPublisher(client: MongoClient, database: MongoDatabase)
    extends AnalyticsReportPublisher {
  private val control = database.getCollection(AnalyticsCollections.ReportControl)
  private val reservations = database.getCollection(AnalyticsCollections.ReportRuns)
  private val snapshots = database.getCollection(AnalyticsCollections.ReportSnapshots)
  private val reportCodecs = MongoAnalyticsReportRecords.registry(database.getCodecRegistry)
  private val typedControl = database
    .getCollection(AnalyticsCollections.ReportControl, classOf[DecodedControl])
    .withCodecRegistry(reportCodecs)
  private val typedReservations = database
    .getCollection(AnalyticsCollections.ReportRuns, classOf[DecodedRun])
    .withCodecRegistry(reportCodecs)
  private val typedSnapshots = database
    .getCollection(AnalyticsCollections.ReportSnapshots, classOf[DecodedSnapshot])
    .withCodecRegistry(reportCodecs)

  override def reserve(runId: String, rangeFingerprint: String, now: Instant): IO[AnalyticsReportReservation] =
    if (runId == null || runId.trim.isEmpty || rangeFingerprint == null || rangeFingerprint.trim.isEmpty)
      IO.raiseError(AnalyticsError.InvalidConfiguration("report reservation identity is empty"))
    else
      IO.blocking {
        Option(typedReservations.find(Filters.eq(AnalyticsCollections.Fields.Id, runId)).first())
      }.flatMap {
        case Some(previous) =>
          IO.fromEither(previous.value.map(record => (record.reservation, record.state)))
            .flatMap { case (existing, state) =>
              if (existing.rangeFingerprint != rangeFingerprint)
                IO.raiseError[AnalyticsReportReservation](AnalyticsError.RunIdRangeConflict(runId))
              else if (state != "Reserved") IO.pure(existing)
              else
                MongoSession.resource(client).use { session =>
                  IO.blocking(
                    transaction(session) {
                      for {
                        current <- Option(
                          typedControl
                            .find(session, Filters.eq(AnalyticsCollections.Fields.Id, "analytics-report"))
                            .first()
                        )
                          .toRight(AnalyticsError.InvalidConfiguration("analytics report control is unavailable"))
                        decoded <- current.value
                        generation = decoded.generation
                        lastPublishedRevision = decoded.lastPublishedRevision
                        result <-
                          if (generation <= existing.generation && lastPublishedRevision < existing.revision)
                            Right(existing)
                          else
                            for {
                              _ <- Either.cond(
                                casUpdate(
                                  session,
                                  control,
                                  Filters.eq(AnalyticsCollections.Fields.Id, "analytics-report"),
                                  Updates.inc(AnalyticsCollections.Fields.NextRevision, 1L)
                                ),
                                (),
                                AnalyticsError.InvalidConfiguration("analytics report control is unavailable")
                              )
                              updated <- Option(
                                typedControl
                                  .find(session, Filters.eq(AnalyticsCollections.Fields.Id, "analytics-report"))
                                  .first()
                              )
                                .toRight(AnalyticsError.InvalidConfiguration("analytics report control is unavailable"))
                              updatedControl <- updated.value
                              updatedGeneration = updatedControl.generation
                              updatedRevision <- updatedControl.nextRevision
                                .toRight(AnalyticsError.InvalidConfiguration("analytics report control is unavailable"))
                              refreshed = existing.copy(generation = updatedGeneration, revision = updatedRevision)
                              replaced = casUpdate(
                                session,
                                reservations,
                                Filters.and(
                                  Filters.eq(AnalyticsCollections.Fields.Id, runId),
                                  Filters.eq(AnalyticsCollections.Fields.State, "Reserved")
                                ),
                                Updates.combine(
                                  Updates.set(AnalyticsCollections.Fields.Generation, refreshed.generation),
                                  Updates.set(AnalyticsCollections.Fields.Revision, refreshed.revision),
                                  Updates.set(AnalyticsCollections.Fields.CreatedAt, Date.from(now)),
                                  Updates.set(
                                    AnalyticsCollections.Fields.ExpiresAt,
                                    Date.from(now.plusSeconds(90L * 24L * 60L * 60L))
                                  )
                                )
                              )
                              _ <- Either.cond(replaced, (), AnalyticsError.RunIdRangeConflict(runId))
                            } yield refreshed
                      } yield result
                    }
                  ).flatMap(IO.fromEither)
                }
            }
        case None =>
          MongoSession.resource(client).use { session =>
            IO.blocking(
              transaction(session) {
                for {
                  _ <- Option(
                    typedControl.find(session, Filters.eq(AnalyticsCollections.Fields.Id, "analytics-report")).first()
                  )
                    .toRight(AnalyticsError.InvalidConfiguration("analytics report control is not initialized"))
                  changed = casUpdate(
                    session,
                    control,
                    Filters.eq(AnalyticsCollections.Fields.Id, "analytics-report"),
                    Updates.inc(AnalyticsCollections.Fields.NextRevision, 1L)
                  )
                  _ <- Either
                    .cond(changed, (), AnalyticsError.InvalidConfiguration("analytics report control is unavailable"))
                  updated <- Option(
                    typedControl.find(session, Filters.eq(AnalyticsCollections.Fields.Id, "analytics-report")).first()
                  )
                    .toRight(AnalyticsError.InvalidConfiguration("analytics report control is unavailable"))
                  updatedControl <- updated.value
                  generation = updatedControl.generation
                  revision <- updatedControl.nextRevision
                    .toRight(AnalyticsError.InvalidConfiguration("analytics report control is unavailable"))
                } yield {
                  val value = AnalyticsReportReservation(runId, rangeFingerprint, generation, revision)
                  reservations.insertOne(
                    session,
                    new Document(AnalyticsCollections.Fields.Id, runId)
                      .append(AnalyticsCollections.Fields.RangeFingerprint, rangeFingerprint)
                      .append(AnalyticsCollections.Fields.Generation, value.generation)
                      .append(AnalyticsCollections.Fields.Revision, value.revision)
                      .append(AnalyticsCollections.Fields.State, "Reserved")
                      .append(AnalyticsCollections.Fields.CreatedAt, Date.from(now))
                      .append(AnalyticsCollections.Fields.ExpiresAt, Date.from(now.plusSeconds(90L * 24L * 60L * 60L)))
                  )
                  value
                }
              }
            ).flatMap(IO.fromEither)
          }
      }.adaptError {
        case error: AnalyticsError => error
        case NonFatal(cause)       => AnalyticsError.MongoConnectionFailure(cause)
      }

  override def publish(
      reservation: AnalyticsReportReservation,
      report: AnalyticsReportOutput,
      expiresAt: Instant
  ): IO[Unit] =
    if (!expiresAt.isAfter(report.asOf))
      IO.raiseError(AnalyticsError.InvalidConfiguration("report expiry must follow publication time"))
    else
      MongoSession
        .resource(client)
        .use { session =>
          IO.blocking(
            transaction(session) {
              for {
                reserved <- Option(
                  typedReservations.find(session, Filters.eq(AnalyticsCollections.Fields.Id, reservation.runId)).first()
                )
                  .toRight(AnalyticsError.RunIdRangeConflict(reservation.runId))
                decoded <- reserved.value
                _ <- Either
                  .cond(decoded.reservation == reservation, (), AnalyticsError.RunIdRangeConflict(reservation.runId))
                state <- Option(
                  typedControl.find(session, Filters.eq(AnalyticsCollections.Fields.Id, "analytics-report")).first()
                )
                  .toRight(AnalyticsError.RunIdRangeConflict(reservation.runId))
                decodedState <- state.value
                generation = decodedState.generation
                stateName = decodedState.state
                _ <- Either.cond(
                  generation == reservation.generation &&
                    Set("Published", "Unpublished").contains(stateName),
                  (),
                  AnalyticsError.RunIdRangeConflict(reservation.runId)
                )
                lastRevision = decodedState.lastPublishedRevision
                lastRunId = decodedState.lastRunId.getOrElse("")
                current = Option(
                  typedSnapshots.find(session, Filters.eq(AnalyticsCollections.Fields.Id, "current")).first()
                )
                snapshot <- current.traverse(_.value)
                alreadyPublished = lastRevision == reservation.revision && lastRunId == reservation.runId
                result <-
                  if (
                    alreadyPublished && snapshot
                      .exists(value => value.matches(reservation) && value.expiresAt.exists(_.after(new Date())))
                  )
                    Right(())
                  else if (alreadyPublished && snapshot.exists(_.expiresAt.exists(_.after(new Date()))))
                    Left(AnalyticsError.RunIdRangeConflict(reservation.runId))
                  else if (alreadyPublished) {
                    snapshots.replaceOne(
                      session,
                      Filters.eq(AnalyticsCollections.Fields.Id, "current"),
                      reportDocument(reservation, report, expiresAt),
                      new ReplaceOptions().upsert(true)
                    )
                    Right(())
                  } else if (reservation.revision <= lastRevision)
                    Left(AnalyticsError.RunIdRangeConflict(reservation.runId))
                  else {
                    val cas = casUpdate(
                      session,
                      control,
                      Filters.and(
                        Filters.eq(AnalyticsCollections.Fields.Id, "analytics-report"),
                        Filters.eq(AnalyticsCollections.Fields.Generation, reservation.generation),
                        Filters.lt(AnalyticsCollections.Fields.LastPublishedRevision, reservation.revision),
                        Filters.in(AnalyticsCollections.Fields.State, Set("Published", "Unpublished").asJava)
                      ),
                      Updates.combine(
                        Updates.set(AnalyticsCollections.Fields.State, "Published"),
                        Updates.set(AnalyticsCollections.Fields.LastPublishedRevision, reservation.revision),
                        Updates.set(AnalyticsCollections.Fields.LastRunId, reservation.runId),
                        Updates.unset(AnalyticsCollections.Fields.HiddenAt)
                      )
                    )
                    if (!cas) Left(AnalyticsError.RunIdRangeConflict(reservation.runId))
                    else {
                      snapshots.replaceOne(
                        session,
                        Filters.eq(AnalyticsCollections.Fields.Id, "current"),
                        reportDocument(reservation, report, expiresAt),
                        new ReplaceOptions().upsert(true)
                      )
                      reservations.updateOne(
                        session,
                        Filters.eq(AnalyticsCollections.Fields.Id, reservation.runId),
                        Updates.set(AnalyticsCollections.Fields.State, "Published")
                      )
                      Right(())
                    }
                  }
              } yield result
            }
          ).flatMap(IO.fromEither)
        }
        .adaptError {
          case error: AnalyticsError => error
          case NonFatal(cause)       => AnalyticsError.MongoConnectionFailure(cause)
        }

  /** Erasure reveal and durable completion are one Mongo transaction guarded by the current worker lease. */
  override def publishErasure(
      reservation: AnalyticsReportReservation,
      report: AnalyticsReportOutput,
      expiresAt: Instant,
      claim: ErasureClaim,
      completedAt: Instant
  ): IO[Unit] =
    if (!expiresAt.isAfter(report.asOf) || !expiresAt.isAfter(completedAt))
      IO.raiseError(AnalyticsError.InvalidConfiguration("erasure snapshot expiry must follow publication time"))
    else
      MongoSession
        .resource(client)
        .use { session =>
          IO.blocking(
            transaction(session) {
              val requestFilter = Filters.and(
                Filters.eq(AnalyticsCollections.Fields.Id, claim.requestId),
                Filters.eq(AnalyticsCollections.Fields.State, "Processing"),
                Filters.eq(AnalyticsCollections.Fields.LeaseToken, claim.leaseToken),
                Filters.gt(AnalyticsCollections.Fields.LeaseUntil, Date.from(completedAt)),
                Filters.eq(AnalyticsCollections.Fields.Phase, ErasurePhase.ReadyToPublish.persistedName)
              )
              val nonReadyOther = Filters.and(
                Filters.ne(AnalyticsCollections.Fields.Id, claim.requestId),
                Filters.in(AnalyticsCollections.Fields.State, "Pending", "Processing"),
                Filters.ne(AnalyticsCollections.Fields.Phase, ErasurePhase.ReadyToPublish.persistedName)
              )
              for {
                request <- Option(
                  database
                    .getCollection(AnalyticsCollections.ErasureRequests)
                    .find(session, requestFilter)
                    .first()
                ).toRight(AnalyticsError.ErasureNotReady)
                user <- Option(
                  database
                    .getCollection(AnalyticsCollections.Users)
                    .find(session, Filters.eq(AnalyticsCollections.Fields.Id, claim.requestId))
                    .first()
                )
                  .toRight(AnalyticsError.ErasureNotReady)
                accountStatus <- requiredString(user, AnalyticsCollections.Fields.AccountStatus)
                _ <- Either.cond(accountStatus == "Deleted", (), AnalyticsError.ErasureNotReady)
                _ <- Option(
                  database
                    .getCollection(AnalyticsCollections.OutboxSubjectFences)
                    .find(
                      session,
                      Filters.and(
                        Filters.eq(AnalyticsCollections.Fields.Id, claim.requestId),
                        Filters.eq(AnalyticsCollections.Fields.Deleted, true)
                      )
                    )
                    .first()
                )
                  .toRight(AnalyticsError.ErasureNotReady)
                  .map(_ => ())
                _ <- Either.cond(
                  database
                    .getCollection(AnalyticsCollections.ErasureRequests)
                    .countDocuments(session, nonReadyOther) == 0L,
                  (),
                  AnalyticsError.ErasureNotReady
                )
                reserved <- Option(
                  typedReservations.find(session, Filters.eq(AnalyticsCollections.Fields.Id, reservation.runId)).first()
                )
                  .toRight(AnalyticsError.RunIdRangeConflict(reservation.runId))
                decoded <- reserved.value
                reservedState = decoded.state
                _ <- Either.cond(
                  decoded.reservation == reservation && reservedState == "Reserved",
                  (),
                  AnalyticsError.RunIdRangeConflict(reservation.runId)
                )
                state <- Option(
                  typedControl.find(session, Filters.eq(AnalyticsCollections.Fields.Id, "analytics-report")).first()
                )
                  .toRight(AnalyticsError.RunIdRangeConflict(reservation.runId))
                decodedState <- state.value
                generation = decodedState.generation
                stateName = decodedState.state
                _ <- Either.cond(
                  generation == reservation.generation &&
                    Set("Hidden", "Published").contains(stateName),
                  (),
                  AnalyticsError.RunIdRangeConflict(reservation.runId)
                )
                lastRevision = decodedState.lastPublishedRevision
                _ <- Either
                  .cond(reservation.revision > lastRevision, (), AnalyticsError.RunIdRangeConflict(reservation.runId))
                cas = casUpdate(
                  session,
                  control,
                  Filters.and(
                    Filters.eq(AnalyticsCollections.Fields.Id, "analytics-report"),
                    Filters.eq(AnalyticsCollections.Fields.Generation, reservation.generation),
                    Filters.in(AnalyticsCollections.Fields.State, Set("Hidden", "Published").asJava),
                    Filters.lt(AnalyticsCollections.Fields.LastPublishedRevision, reservation.revision)
                  ),
                  Updates.combine(
                    Updates.set(AnalyticsCollections.Fields.State, "Published"),
                    Updates.set(AnalyticsCollections.Fields.LastPublishedRevision, reservation.revision),
                    Updates.set(AnalyticsCollections.Fields.LastRunId, reservation.runId),
                    Updates.unset(AnalyticsCollections.Fields.HiddenAt)
                  )
                )
                _ <- Either.cond(cas, (), AnalyticsError.RunIdRangeConflict(reservation.runId))
                _ = snapshots.replaceOne(
                  session,
                  Filters.eq(AnalyticsCollections.Fields.Id, "current"),
                  reportDocument(reservation, report, expiresAt),
                  new ReplaceOptions().upsert(true)
                )
                _ = reservations.updateOne(
                  session,
                  Filters.and(
                    Filters.eq(AnalyticsCollections.Fields.Id, reservation.runId),
                    Filters.eq(AnalyticsCollections.Fields.State, "Reserved")
                  ),
                  Updates.set(AnalyticsCollections.Fields.State, "Published")
                )
                completed = casUpdate(
                  session,
                  database.getCollection(AnalyticsCollections.ErasureRequests),
                  requestFilter,
                  Updates.combine(
                    Updates.set(AnalyticsCollections.Fields.State, "Complete"),
                    Updates.set(AnalyticsCollections.Fields.Phase, ErasurePhase.ReportPublished.persistedName),
                    Updates.set(AnalyticsCollections.Fields.Progress, 0),
                    Updates
                      .set(
                        AnalyticsCollections.Fields.ProgressKey,
                        ErasurePhase.ReportPublished.ordinal.toLong * ErasurePhase.ProgressPerPhase
                      ),
                    Updates.set(AnalyticsCollections.Fields.CompletedAt, Date.from(completedAt)),
                    Updates.set(
                      AnalyticsCollections.Fields.ExpiresAt,
                      Date.from(completedAt.plusSeconds(AnalyticsRetention.DeletionMarkerDays.toLong * 86400L))
                    ),
                    Updates.unset(AnalyticsCollections.Fields.LeaseToken),
                    Updates.unset(AnalyticsCollections.Fields.LeaseUntil)
                  )
                )
                _ <- Either.cond(completed, (), AnalyticsError.ErasureNotReady)
              } yield {
                val completion = database.getCollection(AnalyticsCollections.ErasureCompletions)
                completion.updateOne(
                  session,
                  Filters.eq(AnalyticsCollections.Fields.Id, claim.requestId),
                  Option(request.get(AnalyticsCollections.Fields.ReceiptId)).collect { case value: String =>
                    value
                  } match {
                    case Some(receiptId) =>
                      Updates.combine(
                        Updates.setOnInsert(AnalyticsCollections.Fields.Id, claim.requestId),
                        Updates.setOnInsert(AnalyticsCollections.Fields.CompletedAt, Date.from(completedAt)),
                        Updates.setOnInsert(AnalyticsCollections.Fields.ReceiptId, receiptId)
                      )
                    case None =>
                      Updates.combine(
                        Updates.setOnInsert(AnalyticsCollections.Fields.Id, claim.requestId),
                        Updates.setOnInsert(AnalyticsCollections.Fields.CompletedAt, Date.from(completedAt))
                      )
                  },
                  new com.mongodb.client.model.UpdateOptions().upsert(true)
                )
                ()
              }
            }
          ).flatMap(IO.fromEither)
        }
        .adaptError {
          case error: AnalyticsError => error
          case NonFatal(cause)       => AnalyticsError.MongoConnectionFailure(cause)
        }

  private def requiredString(document: Document, field: String): Either[AnalyticsError, String] =
    Option(document.get(field))
      .collect { case value: String => value }
      .toRight(AnalyticsError.InvalidConfiguration("analytics report record is malformed"))

  /** Let the driver retry labeled transaction and commit errors; abort a typed rejection before callback return. */
  private def transaction[A](session: ClientSession)(work: => Either[AnalyticsError, A]): Either[AnalyticsError, A] =
    session.withTransaction(() => {
      val result = work
      if (result.isLeft) session.abortTransaction()
      result
    })

  /** Runs the matched-count CAS inside the caller-owned transaction session. A false result is interpreted by the
    * caller so each operation retains its existing typed conflict failure.
    */
  private def casUpdate(
      session: ClientSession,
      collection: MongoCollection[Document],
      filter: Bson,
      update: Bson
  ): Boolean = collection.updateOne(session, filter, update).getMatchedCount == 1L

  private def reportDocument(
      reservation: AnalyticsReportReservation,
      report: AnalyticsReportOutput,
      expiresAt: Instant
  ): Document = {
    val document = new Document(AnalyticsCollections.Fields.Id, "current")
      .append(AnalyticsCollections.Fields.State, "Published")
      .append(AnalyticsCollections.Fields.Generation, reservation.generation)
      .append(AnalyticsCollections.Fields.Revision, reservation.revision)
      .append(AnalyticsCollections.Fields.RunId, reservation.runId)
      .append(AnalyticsCollections.Fields.AsOf, Date.from(report.asOf))
      .append(AnalyticsCollections.Fields.ExpiresAt, Date.from(expiresAt))
      .append(
        "funnel",
        report.funnel
          .map(day =>
            new Document("day", Date.from(day.day))
              .append("created", day.created)
              .append("accepted", day.accepted)
              .append("declined", day.declined)
              .append("interview", day.interview)
              .append("hired", day.hired)
              .append("rejected", day.rejected)
          )
          .asJava
      )
      .append(
        "skillPostingActivity",
        report.skillPostingActivity
          .map(row =>
            new Document("day", Date.from(row.day))
              .append("skill", row.skill)
              .append("postings", row.postings)
          )
          .asJava
      )
    report.timeToHire.fold(document) { value =>
      document.append(
        "timeToHire",
        new Document("p50Hours", value.p50Hours)
          .append("p75Hours", value.p75Hours)
          .append("p90Hours", value.p90Hours)
          .append("p95Hours", value.p95Hours)
          .append("eligibleCount", value.eligibleCount)
          .append("excludedCount", value.excludedCount)
      )
    }
  }
}
