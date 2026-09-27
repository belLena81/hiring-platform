package com.example.hiring.analytics.mongo

import com.example.hiring.analytics.batch.{AnalyticsReportOutput, AnalyticsReportPublisher, AnalyticsReportReservation}
import com.example.hiring.analytics.erasure.{ErasureClaim, ErasurePhase}

import com.example.hiring.analytics.*

import cats.effect.IO
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

  override def reserve(runId: String, rangeFingerprint: String, now: Instant): IO[AnalyticsReportReservation] =
    IO.blocking {
      require(runId != null && runId.trim.nonEmpty && rangeFingerprint != null && rangeFingerprint.trim.nonEmpty)
      Option(reservations.find(Filters.eq(AnalyticsCollections.Fields.Id, runId)).first())
    }.flatMap {
      case Some(previous) =>
        if (previous.getString("rangeFingerprint") != rangeFingerprint)
          IO.raiseError[AnalyticsReportReservation](AnalyticsError.RunIdRangeConflict(runId))
        else {
          val existing = decodeReservation(previous)
          if (previous.getString(AnalyticsCollections.Fields.State) != "Reserved") IO.pure(existing)
          else
            MongoSession.resource(client).use { session =>
              IO.blocking(session.withTransaction(() => {
                val current =
                  control.find(session, Filters.eq(AnalyticsCollections.Fields.Id, "analytics-report")).first()
                if (current == null)
                  throw AnalyticsError.InvalidConfiguration("analytics report control is unavailable")
                val generation = current.getLong("generation")
                val lastPublishedRevision = current.getLong("lastPublishedRevision")
                if (generation <= existing.generation && lastPublishedRevision < existing.revision) existing
                else {
                  val changed = casUpdate(
                    session,
                    control,
                    Filters.eq(AnalyticsCollections.Fields.Id, "analytics-report"),
                    Updates.inc("nextRevision", 1L)
                  )
                  if (!changed)
                    throw AnalyticsError.InvalidConfiguration("analytics report control is unavailable")
                  val updated =
                    control.find(session, Filters.eq(AnalyticsCollections.Fields.Id, "analytics-report")).first()
                  val refreshed = existing.copy(
                    generation = updated.getLong("generation"),
                    revision = updated.getLong("nextRevision")
                  )
                  val replaced = casUpdate(
                    session,
                    reservations,
                    Filters.and(
                      Filters.eq(AnalyticsCollections.Fields.Id, runId),
                      Filters.eq(AnalyticsCollections.Fields.State, "Reserved")
                    ),
                    Updates.combine(
                      Updates.set("generation", refreshed.generation),
                      Updates.set("revision", refreshed.revision),
                      Updates.set(AnalyticsCollections.Fields.CreatedAt, Date.from(now)),
                      Updates
                        .set(AnalyticsCollections.Fields.ExpiresAt, Date.from(now.plusSeconds(90L * 24L * 60L * 60L)))
                    )
                  )
                  if (!replaced)
                    throw AnalyticsError.RunIdRangeConflict(runId)
                  refreshed
                }
              }))
            }
        }
      case None =>
        MongoSession.resource(client).use { session =>
          IO.blocking(session.withTransaction(() => {
            val state = control.find(session, Filters.eq(AnalyticsCollections.Fields.Id, "analytics-report")).first()
            if (state == null) throw AnalyticsError.InvalidConfiguration("analytics report control is not initialized")
            val changed = casUpdate(
              session,
              control,
              Filters.eq(AnalyticsCollections.Fields.Id, "analytics-report"),
              Updates.inc("nextRevision", 1L)
            )
            if (!changed)
              throw AnalyticsError.InvalidConfiguration("analytics report control is unavailable")
            val updated = control.find(session, Filters.eq(AnalyticsCollections.Fields.Id, "analytics-report")).first()
            val value = AnalyticsReportReservation(
              runId,
              rangeFingerprint,
              updated.getLong("generation"),
              updated.getLong("nextRevision")
            )
            reservations.insertOne(
              session,
              new Document(AnalyticsCollections.Fields.Id, runId)
                .append("rangeFingerprint", rangeFingerprint)
                .append("generation", value.generation)
                .append("revision", value.revision)
                .append(AnalyticsCollections.Fields.State, "Reserved")
                .append(AnalyticsCollections.Fields.CreatedAt, Date.from(now))
                .append(AnalyticsCollections.Fields.ExpiresAt, Date.from(now.plusSeconds(90L * 24L * 60L * 60L)))
            )
            value
          }))
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
          IO.blocking(session.withTransaction(() => {
            val reserved =
              reservations.find(session, Filters.eq(AnalyticsCollections.Fields.Id, reservation.runId)).first()
            if (reserved == null || decodeReservation(reserved) != reservation)
              throw AnalyticsError.RunIdRangeConflict(reservation.runId)
            val state = control.find(session, Filters.eq(AnalyticsCollections.Fields.Id, "analytics-report")).first()
            if (
              state == null || state.getLong("generation") != reservation.generation ||
              !Set("Published", "Unpublished").contains(state.getString(AnalyticsCollections.Fields.State))
            )
              throw AnalyticsError.RunIdRangeConflict(reservation.runId)
            val lastRevision = state.getLong("lastPublishedRevision")
            val alreadyPublished =
              lastRevision == reservation.revision && state.getString("lastRunId") == reservation.runId
            val current = snapshots.find(session, Filters.eq(AnalyticsCollections.Fields.Id, "current")).first()
            val currentUntil =
              Option(current).flatMap(value => Option(value.getDate(AnalyticsCollections.Fields.ExpiresAt)))
            val currentMatches = Option(current).exists(value =>
              value.getLong("generation") == reservation.generation && value
                .getLong("revision") == reservation.revision &&
                value.getString("runId") == reservation.runId
            )
            if (alreadyPublished && currentMatches && currentUntil.exists(_.after(new Date()))) ()
            else if (alreadyPublished && current != null && currentUntil.exists(_.after(new Date())))
              throw AnalyticsError.RunIdRangeConflict(reservation.runId)
            else if (alreadyPublished) {
              snapshots.replaceOne(
                session,
                Filters.eq(AnalyticsCollections.Fields.Id, "current"),
                reportDocument(reservation, report, expiresAt),
                new ReplaceOptions().upsert(true)
              )
              ()
            } else if (reservation.revision <= lastRevision)
              throw AnalyticsError.RunIdRangeConflict(reservation.runId)
            else {
              val cas = casUpdate(
                session,
                control,
                Filters.and(
                  Filters.eq(AnalyticsCollections.Fields.Id, "analytics-report"),
                  Filters.eq("generation", reservation.generation),
                  Filters.lt("lastPublishedRevision", reservation.revision),
                  Filters.in(AnalyticsCollections.Fields.State, Set("Published", "Unpublished").asJava)
                ),
                Updates.combine(
                  Updates.set(AnalyticsCollections.Fields.State, "Published"),
                  Updates.set("lastPublishedRevision", reservation.revision),
                  Updates.set("lastRunId", reservation.runId),
                  Updates.unset("hiddenAt")
                )
              )
              if (!cas) throw AnalyticsError.RunIdRangeConflict(reservation.runId)
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
              ()
            }
          }))
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
          IO.blocking(session.withTransaction(() => {
            val requestFilter = Filters.and(
              Filters.eq(AnalyticsCollections.Fields.Id, claim.requestId),
              Filters.eq(AnalyticsCollections.Fields.State, "Processing"),
              Filters.eq(AnalyticsCollections.Fields.LeaseToken, claim.leaseToken),
              Filters.gt(AnalyticsCollections.Fields.LeaseUntil, Date.from(completedAt)),
              Filters.eq(AnalyticsCollections.Fields.Phase, ErasurePhase.ReadyToPublish.persistedName)
            )
            val request =
              database.getCollection(AnalyticsCollections.ErasureRequests).find(session, requestFilter).first()
            val user = database
              .getCollection(AnalyticsCollections.Users)
              .find(session, Filters.eq(AnalyticsCollections.Fields.Id, claim.requestId))
              .first()
            val fence = database
              .getCollection(AnalyticsCollections.OutboxSubjectFences)
              .find(
                session,
                Filters.and(Filters.eq(AnalyticsCollections.Fields.Id, claim.requestId), Filters.eq("deleted", true))
              )
              .first()
            if (
              request == null || user == null || user
                .getString(AnalyticsCollections.Fields.AccountStatus) != "Deleted" || fence == null
            )
              throw AnalyticsError.ErasureNotReady
            val nonReadyOther = Filters.and(
              Filters.ne(AnalyticsCollections.Fields.Id, claim.requestId),
              Filters.in(AnalyticsCollections.Fields.State, "Pending", "Processing"),
              Filters.ne(AnalyticsCollections.Fields.Phase, ErasurePhase.ReadyToPublish.persistedName)
            )
            if (
              database.getCollection(AnalyticsCollections.ErasureRequests).countDocuments(session, nonReadyOther) != 0L
            )
              throw AnalyticsError.ErasureNotReady

            val reserved =
              reservations.find(session, Filters.eq(AnalyticsCollections.Fields.Id, reservation.runId)).first()
            if (
              reserved == null || decodeReservation(reserved) != reservation ||
              reserved.getString(AnalyticsCollections.Fields.State) != "Reserved"
            )
              throw AnalyticsError.RunIdRangeConflict(reservation.runId)

            val state = control.find(session, Filters.eq(AnalyticsCollections.Fields.Id, "analytics-report")).first()
            if (
              state == null || state.getLong("generation") != reservation.generation ||
              !Set("Hidden", "Published").contains(state.getString(AnalyticsCollections.Fields.State))
            )
              throw AnalyticsError.RunIdRangeConflict(reservation.runId)
            val lastRevision = state.getLong("lastPublishedRevision")
            if (reservation.revision <= lastRevision)
              throw AnalyticsError.RunIdRangeConflict(reservation.runId)

            val cas = casUpdate(
              session,
              control,
              Filters.and(
                Filters.eq(AnalyticsCollections.Fields.Id, "analytics-report"),
                Filters.eq("generation", reservation.generation),
                Filters.in(AnalyticsCollections.Fields.State, Set("Hidden", "Published").asJava),
                Filters.lt("lastPublishedRevision", reservation.revision)
              ),
              Updates.combine(
                Updates.set(AnalyticsCollections.Fields.State, "Published"),
                Updates.set("lastPublishedRevision", reservation.revision),
                Updates.set("lastRunId", reservation.runId),
                Updates.unset("hiddenAt")
              )
            )
            if (!cas) throw AnalyticsError.RunIdRangeConflict(reservation.runId)

            snapshots.replaceOne(
              session,
              Filters.eq(AnalyticsCollections.Fields.Id, "current"),
              reportDocument(reservation, report, expiresAt),
              new ReplaceOptions().upsert(true)
            )
            reservations.updateOne(
              session,
              Filters.and(
                Filters.eq(AnalyticsCollections.Fields.Id, reservation.runId),
                Filters.eq(AnalyticsCollections.Fields.State, "Reserved")
              ),
              Updates.set(AnalyticsCollections.Fields.State, "Published")
            )
            val completed = casUpdate(
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
                Updates.set("completedAt", Date.from(completedAt)),
                Updates.set(
                  AnalyticsCollections.Fields.ExpiresAt,
                  Date.from(completedAt.plusSeconds(AnalyticsRetention.DeletionMarkerDays.toLong * 86400L))
                ),
                Updates.unset(AnalyticsCollections.Fields.LeaseToken),
                Updates.unset(AnalyticsCollections.Fields.LeaseUntil)
              )
            )
            if (!completed) throw AnalyticsError.ErasureNotReady

            val completion = database.getCollection(AnalyticsCollections.ErasureCompletions)
            completion.updateOne(
              session,
              Filters.eq(AnalyticsCollections.Fields.Id, claim.requestId),
              request.getString(AnalyticsCollections.Fields.ReceiptId) match {
                case receiptId if receiptId != null =>
                  Updates.combine(
                    Updates.setOnInsert(AnalyticsCollections.Fields.Id, claim.requestId),
                    Updates.setOnInsert("completedAt", Date.from(completedAt)),
                    Updates.setOnInsert(AnalyticsCollections.Fields.ReceiptId, receiptId)
                  )
                case _ =>
                  Updates.combine(
                    Updates.setOnInsert(AnalyticsCollections.Fields.Id, claim.requestId),
                    Updates.setOnInsert("completedAt", Date.from(completedAt))
                  )
              },
              new com.mongodb.client.model.UpdateOptions().upsert(true)
            )
            ()
          }))
        }
        .adaptError {
          case error: AnalyticsError => error
          case NonFatal(cause)       => AnalyticsError.MongoConnectionFailure(cause)
        }

  private def decodeReservation(document: Document): AnalyticsReportReservation =
    AnalyticsReportReservation(
      document.getString(AnalyticsCollections.Fields.Id),
      document.getString("rangeFingerprint"),
      document.getLong("generation"),
      document.getLong("revision")
    )

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
      .append("generation", reservation.generation)
      .append("revision", reservation.revision)
      .append("runId", reservation.runId)
      .append("asOf", Date.from(report.asOf))
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
