package com.example.hiring.analytics

import cats.effect.IO
import com.mongodb.client.{MongoClient, MongoDatabase}
import com.mongodb.client.model.{Filters, ReplaceOptions, Updates}
import org.bson.Document

import java.time.Instant
import java.util.Date
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** Durable publication boundary shared with the operational report reader. */
trait AnalyticsReportPublisher {
  def reserve(runId: String, rangeFingerprint: String, now: java.time.Instant): IO[AnalyticsReportReservation]
  def publish(
      reservation: AnalyticsReportReservation,
      report: AnalyticsReportOutput,
      expiresAt: java.time.Instant
  ): IO[Unit]
  def publishErasure(
      reservation: AnalyticsReportReservation,
      report: AnalyticsReportOutput,
      expiresAt: java.time.Instant,
      claim: ErasureClaim,
      completedAt: Instant
  ): IO[Unit] =
    IO.raiseError(AnalyticsError.InvalidConfiguration("guarded erasure publication is not configured"))
}

final case class AnalyticsReportReservation(runId: String, rangeFingerprint: String, generation: Long, revision: Long)

/** Sync Mongo calls are isolated on the blocking pool. Mongo owns revision allocation and the publication CAS. */
final class MongoAnalyticsReportPublisher(client: MongoClient, database: MongoDatabase) extends AnalyticsReportPublisher {
  private val control = database.getCollection("analytics_report_control")
  private val reservations = database.getCollection("analytics_report_runs")
  private val snapshots = database.getCollection("analytics_report_snapshots")

  override def reserve(runId: String, rangeFingerprint: String, now: Instant): IO[AnalyticsReportReservation] =
    IO.blocking {
      require(runId != null && runId.trim.nonEmpty && rangeFingerprint != null && rangeFingerprint.trim.nonEmpty)
      val previous = reservations.find(Filters.eq("_id", runId)).first()
      if (previous != null) {
        if (previous.getString("rangeFingerprint") != rangeFingerprint)
          throw AnalyticsError.RunIdRangeConflict(runId)
        val existing = decodeReservation(previous)
        if (previous.getString("state") != "Reserved") existing
        else {
          val session = client.startSession()
          try session.withTransaction(() => {
            val current = control.find(session, Filters.eq("_id", "analytics-report")).first()
            if (current == null) throw AnalyticsError.InvalidConfiguration("analytics report control is unavailable")
            val generation = current.getLong("generation")
            val lastPublishedRevision = current.getLong("lastPublishedRevision")
            if (generation <= existing.generation && lastPublishedRevision < existing.revision) existing
            else {
              val changed = control.updateOne(
                session,
                Filters.eq("_id", "analytics-report"),
                Updates.inc("nextRevision", 1L)
              )
              if (changed.getMatchedCount != 1L)
                throw AnalyticsError.InvalidConfiguration("analytics report control is unavailable")
              val updated = control.find(session, Filters.eq("_id", "analytics-report")).first()
              val refreshed = existing.copy(
                generation = updated.getLong("generation"),
                revision = updated.getLong("nextRevision")
              )
              val replaced = reservations.updateOne(
                session,
                Filters.and(Filters.eq("_id", runId), Filters.eq("state", "Reserved")),
                Updates.combine(
                  Updates.set("generation", refreshed.generation),
                  Updates.set("revision", refreshed.revision),
                  Updates.set("createdAt", Date.from(now)),
                  Updates.set("expiresAt", Date.from(now.plusSeconds(90L * 24L * 60L * 60L)))
                )
              )
              if (replaced.getMatchedCount != 1L)
                throw AnalyticsError.RunIdRangeConflict(runId)
              refreshed
            }
          })
          finally session.close()
        }
      } else {
        val session = client.startSession()
        try
          session.withTransaction(() => {
            val state = control.find(session, Filters.eq("_id", "analytics-report")).first()
            if (state == null) throw AnalyticsError.InvalidConfiguration("analytics report control is not initialized")
            val changed = control.updateOne(
              session,
              Filters.eq("_id", "analytics-report"),
              Updates.inc("nextRevision", 1L)
            )
            if (changed.getMatchedCount != 1L) throw AnalyticsError.InvalidConfiguration("analytics report control is unavailable")
            val updated = control.find(session, Filters.eq("_id", "analytics-report")).first()
            val value = AnalyticsReportReservation(
              runId,
              rangeFingerprint,
              updated.getLong("generation"),
              updated.getLong("nextRevision")
            )
            reservations.insertOne(session, new Document("_id", runId)
              .append("rangeFingerprint", rangeFingerprint)
              .append("generation", value.generation)
              .append("revision", value.revision)
              .append("state", "Reserved")
              .append("createdAt", Date.from(now))
              .append("expiresAt", Date.from(now.plusSeconds(90L * 24L * 60L * 60L))))
            value
          })
        finally session.close()
      }
    }.adaptError {
      case error: AnalyticsError => error
      case NonFatal(cause)       => AnalyticsError.MongoConnectionFailure(cause)
    }

  override def publish(
      reservation: AnalyticsReportReservation,
      report: AnalyticsReportOutput,
      expiresAt: Instant
  ): IO[Unit] = IO.blocking {
    require(expiresAt.isAfter(report.asOf))
    val session = client.startSession()
    try session.withTransaction(() => {
      val reserved = reservations.find(session, Filters.eq("_id", reservation.runId)).first()
      if (reserved == null || decodeReservation(reserved) != reservation)
        throw AnalyticsError.RunIdRangeConflict(reservation.runId)
      val state = control.find(session, Filters.eq("_id", "analytics-report")).first()
      if (state == null || state.getLong("generation") != reservation.generation ||
          !Set("Published", "Unpublished").contains(state.getString("state")))
        throw AnalyticsError.RunIdRangeConflict(reservation.runId)
      val lastRevision = state.getLong("lastPublishedRevision")
      val alreadyPublished = lastRevision == reservation.revision && state.getString("lastRunId") == reservation.runId
      val current = snapshots.find(session, Filters.eq("_id", "current")).first()
      val currentUntil = Option(current).flatMap(value => Option(value.getDate("expiresAt")))
      val currentMatches = Option(current).exists(value =>
        value.getLong("generation") == reservation.generation && value.getLong("revision") == reservation.revision &&
          value.getString("runId") == reservation.runId
      )
      if (alreadyPublished && currentMatches && currentUntil.exists(_.after(new Date()))) ()
      else if (alreadyPublished && current != null && currentUntil.exists(_.after(new Date())))
        throw AnalyticsError.RunIdRangeConflict(reservation.runId)
      else if (alreadyPublished) {
        snapshots.replaceOne(
          session,
          Filters.eq("_id", "current"),
          reportDocument(reservation, report, expiresAt),
          new ReplaceOptions().upsert(true)
        )
        ()
      }
      else if (reservation.revision <= lastRevision)
        throw AnalyticsError.RunIdRangeConflict(reservation.runId)
      else {
      val cas = control.updateOne(
        session,
        Filters.and(
          Filters.eq("_id", "analytics-report"),
          Filters.eq("generation", reservation.generation),
          Filters.lt("lastPublishedRevision", reservation.revision),
          Filters.in("state", Set("Published", "Unpublished").asJava)
        ),
        Updates.combine(
          Updates.set("state", "Published"),
          Updates.set("lastPublishedRevision", reservation.revision),
          Updates.set("lastRunId", reservation.runId),
          Updates.unset("hiddenAt")
        )
      )
      if (cas.getMatchedCount != 1L) throw AnalyticsError.RunIdRangeConflict(reservation.runId)
      snapshots.replaceOne(
        session,
        Filters.eq("_id", "current"),
        reportDocument(reservation, report, expiresAt),
        new ReplaceOptions().upsert(true)
      )
      reservations.updateOne(session, Filters.eq("_id", reservation.runId), Updates.set("state", "Published"))
      ()
      }
    })
    finally session.close()
  }.adaptError {
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
  ): IO[Unit] = IO.blocking {
    if (!expiresAt.isAfter(report.asOf) || !expiresAt.isAfter(completedAt))
      throw AnalyticsError.InvalidConfiguration("erasure snapshot expiry must follow publication time")
    val session = client.startSession()
    try session.withTransaction(() => {
      val requestFilter = Filters.and(
        Filters.eq("_id", claim.requestId),
        Filters.eq("state", "Processing"),
        Filters.eq("leaseToken", claim.leaseToken),
        Filters.gt("leaseUntil", Date.from(completedAt)),
        Filters.eq("phase", ErasurePhase.ReadyToPublish.toString)
      )
      val request = database.getCollection("analytics_erasure_requests").find(session, requestFilter).first()
      val user = database.getCollection("users").find(session, Filters.eq("_id", claim.requestId)).first()
      val fence = database.getCollection("outbox_subject_fences")
        .find(session, Filters.and(Filters.eq("_id", claim.requestId), Filters.eq("deleted", true))).first()
      if (request == null || user == null || user.getString("accountStatus") != "Deleted" || fence == null)
        throw AnalyticsError.ErasureNotReady
      val nonReadyOther = Filters.and(
        Filters.ne("_id", claim.requestId),
        Filters.in("state", "Pending", "Processing"),
        Filters.ne("phase", ErasurePhase.ReadyToPublish.toString)
      )
      if (database.getCollection("analytics_erasure_requests").countDocuments(session, nonReadyOther) != 0L)
        throw AnalyticsError.ErasureNotReady

      val reserved = reservations.find(session, Filters.eq("_id", reservation.runId)).first()
      if (reserved == null || decodeReservation(reserved) != reservation ||
          reserved.getString("state") != "Reserved")
        throw AnalyticsError.RunIdRangeConflict(reservation.runId)

      val state = control.find(session, Filters.eq("_id", "analytics-report")).first()
      if (state == null || state.getLong("generation") != reservation.generation ||
          !Set("Hidden", "Published").contains(state.getString("state")))
        throw AnalyticsError.RunIdRangeConflict(reservation.runId)
      val lastRevision = state.getLong("lastPublishedRevision")
      if (reservation.revision <= lastRevision)
        throw AnalyticsError.RunIdRangeConflict(reservation.runId)

      val cas = control.updateOne(
        session,
        Filters.and(
          Filters.eq("_id", "analytics-report"),
          Filters.eq("generation", reservation.generation),
          Filters.in("state", Set("Hidden", "Published").asJava),
          Filters.lt("lastPublishedRevision", reservation.revision)
        ),
        Updates.combine(
          Updates.set("state", "Published"),
          Updates.set("lastPublishedRevision", reservation.revision),
          Updates.set("lastRunId", reservation.runId),
          Updates.unset("hiddenAt")
        )
      )
      if (cas.getMatchedCount != 1L) throw AnalyticsError.RunIdRangeConflict(reservation.runId)

      snapshots.replaceOne(
        session,
        Filters.eq("_id", "current"),
        reportDocument(reservation, report, expiresAt),
        new ReplaceOptions().upsert(true)
      )
      reservations.updateOne(
        session,
        Filters.and(Filters.eq("_id", reservation.runId), Filters.eq("state", "Reserved")),
        Updates.set("state", "Published")
      )
      val completed = database.getCollection("analytics_erasure_requests").updateOne(
        session,
        requestFilter,
        Updates.combine(
          Updates.set("state", "Complete"),
          Updates.set("phase", ErasurePhase.ReportPublished.toString),
          Updates.set("progress", 0),
          Updates.set("progressKey", ErasurePhase.ReportPublished.ordinal.toLong * MongoAnalyticsErasureWorkerStore.ProgressPerPhase),
          Updates.set("completedAt", Date.from(completedAt)),
          Updates.set("expiresAt", Date.from(completedAt.plusSeconds(AnalyticsRetention.DeletionMarkerDays.toLong * 86400L))),
          Updates.unset("leaseToken"),
          Updates.unset("leaseUntil")
        )
      )
      if (completed.getMatchedCount != 1L) throw AnalyticsError.ErasureNotReady

      val completion = database.getCollection("analytics_erasure_completions")
      completion.updateOne(
        session,
        Filters.eq("_id", claim.requestId),
        request.getString("receiptId") match {
          case receiptId if receiptId != null => Updates.combine(
            Updates.setOnInsert("_id", claim.requestId),
            Updates.setOnInsert("completedAt", Date.from(completedAt)),
            Updates.setOnInsert("receiptId", receiptId)
          )
          case _ => Updates.combine(
            Updates.setOnInsert("_id", claim.requestId),
            Updates.setOnInsert("completedAt", Date.from(completedAt))
          )
        },
        new com.mongodb.client.model.UpdateOptions().upsert(true)
      )
      ()
    })
    finally session.close()
  }.adaptError {
    case error: AnalyticsError => error
    case NonFatal(cause)       => AnalyticsError.MongoConnectionFailure(cause)
  }

  private def decodeReservation(document: Document): AnalyticsReportReservation =
    AnalyticsReportReservation(
      document.getString("_id"),
      document.getString("rangeFingerprint"),
      document.getLong("generation"),
      document.getLong("revision")
    )

  private def reportDocument(
      reservation: AnalyticsReportReservation,
      report: AnalyticsReportOutput,
      expiresAt: Instant
  ): Document = {
    val document = new Document("_id", "current")
      .append("state", "Published")
      .append("generation", reservation.generation)
      .append("revision", reservation.revision)
      .append("runId", reservation.runId)
      .append("asOf", Date.from(report.asOf))
      .append("expiresAt", Date.from(expiresAt))
      .append("funnel", report.funnel.map(day => new Document("day", Date.from(day.day))
        .append("created", day.created).append("accepted", day.accepted).append("declined", day.declined)
        .append("interview", day.interview).append("hired", day.hired).append("rejected", day.rejected)).asJava)
      .append("skillPostingActivity", report.skillPostingActivity.map(row => new Document("day", Date.from(row.day))
        .append("skill", row.skill).append("postings", row.postings)).asJava)
    report.timeToHire.fold(document) { value =>
      document.append("timeToHire", new Document("p50Hours", value.p50Hours).append("p75Hours", value.p75Hours)
        .append("p90Hours", value.p90Hours).append("p95Hours", value.p95Hours)
        .append("eligibleCount", value.eligibleCount).append("excludedCount", value.excludedCount))
    }
  }
}

object AnalyticsReportPublisher {
  val unavailable: AnalyticsReportPublisher = new AnalyticsReportPublisher {
    override def reserve(runId: String, rangeFingerprint: String, now: java.time.Instant): IO[AnalyticsReportReservation] =
      IO.raiseError(AnalyticsError.InvalidConfiguration("analytics report publisher is not configured"))

    override def publish(
      reservation: AnalyticsReportReservation,
      report: AnalyticsReportOutput,
      expiresAt: java.time.Instant
    ): IO[Unit] = IO.raiseError(AnalyticsError.InvalidConfiguration("analytics report publisher is not configured"))

    override def publishErasure(
        reservation: AnalyticsReportReservation,
        report: AnalyticsReportOutput,
        expiresAt: java.time.Instant,
        claim: ErasureClaim,
        completedAt: java.time.Instant
    ): IO[Unit] = IO.raiseError(AnalyticsError.InvalidConfiguration("analytics erasure publisher is not configured"))
  }
}
