package com.example.hiring.analytics.adapter.mongo

import com.example.hiring.analytics.config.AnalyticsPositiveInt.*

import com.example.hiring.analytics.config.AnalyticsOperationalSettings
import com.example.hiring.analytics.domain.AnalyticsReportOutput
import com.example.hiring.analytics.domain.RangeFingerprint
import com.example.hiring.analytics.domain.RunId
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.AnalyticsReportPublisher
import com.example.hiring.analytics.service.batch.AnalyticsReportReservation
import com.example.hiring.analytics.service.erasure.ErasureClaim
import com.example.hiring.analytics.service.erasure.ErasurePhase
import com.example.hiring.analytics.service.erasure.ErasureRequestState

import cats.data.EitherT
import cats.effect.{Async, Clock}
import cats.syntax.all.*
import io.github.iltotore.iron.*
import com.mongodb.client.model.{Filters, ReplaceOptions, UpdateOptions, Updates}
import com.mongodb.reactivestreams.client.MongoCollection as ReactiveMongoCollection
import mongo4cats.client.{ClientSession, MongoClient}
import mongo4cats.database.MongoDatabase
import org.bson.Document
import org.bson.conversions.Bson

import java.time.Instant
import java.util.Date
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** Report revision allocation and publication use Mongo's reactive driver and transaction boundary. */
final class MongoAnalyticsReportPublisher[F[_]: Async: Clock](
    client: MongoClient[F],
    database: MongoDatabase[F],
    operational: AnalyticsOperationalSettings
) extends AnalyticsReportPublisher[F] {
  private val clock = Clock[F]
  private val streams = new MongoPublisherStream(operational)
  private val rawDatabase = database.underlying
  private val control = rawDatabase.getCollection(AnalyticsCollections.ReportControl, classOf[Document])
  private val reservations = rawDatabase.getCollection(AnalyticsCollections.ReportRuns, classOf[Document])
  private val snapshots = rawDatabase.getCollection(AnalyticsCollections.ReportSnapshots, classOf[Document])
  private val pojoDatabase = rawDatabase.withCodecRegistry(MongoPojoCodecs.registry)
  private val typedControl = pojoDatabase.getCollection(
    AnalyticsCollections.ReportControl,
    classOf[MongoPojoCodecs.ReportRecord]
  )
  private val typedReservations = pojoDatabase.getCollection(
    AnalyticsCollections.ReportRuns,
    classOf[MongoPojoCodecs.ReportRecord]
  )
  private val typedSnapshots = pojoDatabase.getCollection(
    AnalyticsCollections.ReportSnapshots,
    classOf[MongoPojoCodecs.ReportRecord]
  )

  private type Result[A] = EitherT[F, AnalyticsError, A]
  private def lift[A](io: F[A]): Result[A] = EitherT.liftF(io)
  private def reject[A](error: AnalyticsError): Result[A] = EitherT.leftT[F, A](error)
  private def result[A](value: Either[AnalyticsError, A]): Result[A] = EitherT.fromEither[F](value)
  private def transactional[A](work: ClientSession[F] => Result[A]): Result[A] =
    EitherT(MongoSession.resource(client, streams).use(session => streams.transaction(session)(work(session)).value))

  private def rethrow[A](result: Result[A]): F[A] =
    result.rethrowT.adaptError {
      case error: AnalyticsError => error
      case NonFatal(cause)       => AnalyticsError.MongoConnectionFailure(cause)
    }

  private def allocateRevision(session: ClientSession[F]): Result[(Long, Long)] =
    for {
      changed <- lift(
        casUpdate(
          session,
          control,
          Filters.eq(AnalyticsCollections.Fields.Id, "analytics-report"),
          Updates.inc(AnalyticsCollections.Fields.NextRevision, 1L)
        )
      )
      _ <- result(
        Either.cond(changed, (), AnalyticsError.InvalidConfiguration("analytics report control is unavailable"))
      )
      updated <- lift(
        streams.optional(
          typedControl.find(session.underlying, Filters.eq(AnalyticsCollections.Fields.Id, "analytics-report")).first
        )
      )
      updatedControl <- result(
        updated.toRight(AnalyticsError.InvalidConfiguration("analytics report control is unavailable"))
      )
      updatedValue <- result(MongoAnalyticsReportRecords.decodeControl(updatedControl))
      revision <- result(
        updatedValue.nextRevision.toRight(
          AnalyticsError.InvalidConfiguration("analytics report control is unavailable")
        )
      )
    } yield updatedValue.generation -> revision

  override def reserve(runId: RunId, rangeFingerprint: RangeFingerprint, now: Instant): F[AnalyticsReportReservation] =
    rethrow(reserveResult(runId, rangeFingerprint, now))

  private def reserveResult(
      runId: RunId,
      rangeFingerprint: RangeFingerprint,
      now: Instant
  ): Result[AnalyticsReportReservation] =
    lift(streams.optional(typedReservations.find(Filters.eq(AnalyticsCollections.Fields.Id, runId.value)).first))
      .flatMap {
        case Some(previous) =>
          result(MongoAnalyticsReportRecords.decodeRun(previous)).flatMap { record =>
            val existing = record.reservation
            if (existing.rangeFingerprint != rangeFingerprint)
              reject(AnalyticsError.RunIdRangeConflict(runId.value))
            else if (record.state != "Reserved") EitherT.pure[F, AnalyticsError](existing)
            else
              transactional { session =>
                for {
                  current <- lift(
                    streams.optional(
                      typedControl
                        .find(session.underlying, Filters.eq(AnalyticsCollections.Fields.Id, "analytics-report"))
                        .first
                    )
                  )
                  controlRecord <- result(
                    current.toRight(AnalyticsError.InvalidConfiguration("analytics report control is unavailable"))
                  )
                  decoded <- result(MongoAnalyticsReportRecords.decodeControl(controlRecord))
                  refreshed <-
                    if (decoded.generation <= existing.generation && decoded.lastPublishedRevision < existing.revision)
                      EitherT.pure[F, AnalyticsError](existing)
                    else
                      for {
                        allocated <- allocateRevision(session)
                        value = existing.copy(generation = allocated._1, revision = allocated._2)
                        replaced <- lift(
                          casUpdate(
                            session,
                            reservations,
                            Filters.and(
                              Filters.eq(AnalyticsCollections.Fields.Id, runId.value),
                              Filters.eq(AnalyticsCollections.Fields.State, "Reserved")
                            ),
                            Updates.combine(
                              Updates.set(AnalyticsCollections.Fields.Generation, value.generation),
                              Updates.set(AnalyticsCollections.Fields.Revision, value.revision),
                              Updates.set(AnalyticsCollections.Fields.CreatedAt, Date.from(now)),
                              Updates.set(
                                AnalyticsCollections.Fields.ExpiresAt,
                                Date.from(now.plusMillis(operational.reportReservationTtl.toMillis))
                              )
                            )
                          )
                        )
                        _ <- result(Either.cond(replaced, (), AnalyticsError.RunIdRangeConflict(runId.value)))
                      } yield value
                } yield refreshed
              }
          }
        case None =>
          transactional { session =>
            for {
              current <- lift(
                streams.optional(
                  typedControl
                    .find(session.underlying, Filters.eq(AnalyticsCollections.Fields.Id, "analytics-report"))
                    .first
                )
              )
              _ <- result(
                current.toRight(
                  AnalyticsError.InvalidConfiguration("analytics report control is not initialized")
                )
              )
              allocated <- allocateRevision(session)
              value = AnalyticsReportReservation(runId, rangeFingerprint, allocated._1, allocated._2)
              _ <- lift(
                streams.one(
                  reservations.insertOne(
                    session.underlying,
                    new Document(AnalyticsCollections.Fields.Id, runId.value)
                      .append(AnalyticsCollections.Fields.RangeFingerprint, rangeFingerprint.value)
                      .append(AnalyticsCollections.Fields.Generation, value.generation)
                      .append(AnalyticsCollections.Fields.Revision, value.revision)
                      .append(AnalyticsCollections.Fields.State, "Reserved")
                      .append(AnalyticsCollections.Fields.CreatedAt, Date.from(now))
                      .append(
                        AnalyticsCollections.Fields.ExpiresAt,
                        Date.from(now.plusMillis(operational.reportReservationTtl.toMillis))
                      )
                  )
                )
              )
            } yield value
          }
      }

  override def publish(
      reservation: AnalyticsReportReservation,
      report: AnalyticsReportOutput,
      expiresAt: Instant
  ): F[Unit] =
    rethrow(publishResult(reservation, report, expiresAt))

  private def publishResult(
      reservation: AnalyticsReportReservation,
      report: AnalyticsReportOutput,
      expiresAt: Instant
  ): Result[Unit] =
    if (!expiresAt.isAfter(report.asOf))
      reject(AnalyticsError.InvalidConfiguration("report expiry must follow publication time"))
    else
      transactional { session =>
        for {
          reserved <- lift(
            streams.optional(
              typedReservations
                .find(session.underlying, Filters.eq(AnalyticsCollections.Fields.Id, reservation.runId.value))
                .first
            )
          )
          reservedRecord <- result(
            reserved.toRight(AnalyticsError.RunIdRangeConflict(reservation.runId.value))
          )
          decoded <- result(MongoAnalyticsReportRecords.decodeRun(reservedRecord))
          _ <- result(
            Either.cond(
              decoded.reservation == reservation,
              (),
              AnalyticsError.RunIdRangeConflict(reservation.runId.value)
            )
          )
          state <- lift(
            streams.optional(
              typedControl
                .find(session.underlying, Filters.eq(AnalyticsCollections.Fields.Id, "analytics-report"))
                .first
            )
          )
          stateRecord <- result(state.toRight(AnalyticsError.RunIdRangeConflict(reservation.runId.value)))
          decodedState <- result(MongoAnalyticsReportRecords.decodeControl(stateRecord))
          currentSnapshotRecord <- lift(
            streams.optional(
              typedSnapshots.find(session.underlying, Filters.eq(AnalyticsCollections.Fields.Id, "current")).first
            )
          )
          currentSnapshot <- result(currentSnapshotRecord.traverse(MongoAnalyticsReportRecords.decodeSnapshot))
          _ <- result(
            Either.cond(
              decodedState.generation == reservation.generation &&
                Set("Published", "Unpublished").contains(decodedState.state),
              (),
              AnalyticsError.RunIdRangeConflict(reservation.runId.value)
            )
          )
          currentTime <- lift(clock.realTimeInstant)
          snapshotRecord <- lift(
            streams.optional(
              typedSnapshots.find(session.underlying, Filters.eq(AnalyticsCollections.Fields.Id, "current")).first
            )
          )
          snapshot <- result(snapshotRecord.traverse(MongoAnalyticsReportRecords.decodeSnapshot))
          alreadyPublished = decodedState.lastPublishedRevision == reservation.revision &&
            decodedState.lastRunId.getOrElse("") == reservation.runId.value
          _ <- result(
            Either.cond(
              decoded.state == "Reserved" || (decoded.state == "Published" && alreadyPublished),
              (),
              AnalyticsError.RunIdRangeConflict(reservation.runId.value)
            )
          )
          _ <-
            if (
              alreadyPublished && snapshot
                .exists(v => v.matches(reservation) && v.expiresAt.exists(_.after(Date.from(currentTime))))
            )
              EitherT.pure[F, AnalyticsError](())
            else if (alreadyPublished && snapshot.exists(_.expiresAt.exists(_.after(Date.from(currentTime)))))
              reject(AnalyticsError.RunIdRangeConflict(reservation.runId.value))
            else if (alreadyPublished)
              lift(
                streams.one(
                  snapshots.replaceOne(
                    session.underlying,
                    Filters.eq(AnalyticsCollections.Fields.Id, "current"),
                    reportDocument(
                      reservation,
                      report,
                      expiresAt,
                      snapshot.fold(Map.empty[String, org.bson.BsonValue])(_.extraFields)
                    ),
                    new ReplaceOptions().upsert(true)
                  )
                )
              ).void
            else if (reservation.revision <= decodedState.lastPublishedRevision)
              reject(AnalyticsError.RunIdRangeConflict(reservation.runId.value))
            else
              for {
                changed <- lift(
                  casUpdate(
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
                      Updates.set(AnalyticsCollections.Fields.LastRunId, reservation.runId.value),
                      Updates.unset(AnalyticsCollections.Fields.HiddenAt)
                    )
                  )
                )
                _ <- result(Either.cond(changed, (), AnalyticsError.RunIdRangeConflict(reservation.runId.value)))
                _ <- lift(
                  streams.one(
                    snapshots.replaceOne(
                      session.underlying,
                      Filters.eq(AnalyticsCollections.Fields.Id, "current"),
                      reportDocument(
                        reservation,
                        report,
                        expiresAt,
                        snapshot.fold(Map.empty[String, org.bson.BsonValue])(_.extraFields)
                      ),
                      new ReplaceOptions().upsert(true)
                    )
                  )
                )
                _ <- lift(
                  streams.one(
                    reservations.updateOne(
                      session.underlying,
                      Filters.eq(AnalyticsCollections.Fields.Id, reservation.runId.value),
                      Updates.set(AnalyticsCollections.Fields.State, "Published")
                    )
                  )
                )
              } yield ()
        } yield ()
      }

  /** Erasure reveal and durable completion are one Mongo transaction guarded by the current worker lease. */
  override def publishErasure(
      reservation: AnalyticsReportReservation,
      report: AnalyticsReportOutput,
      expiresAt: Instant,
      claim: ErasureClaim,
      completedAt: Instant
  ): F[Unit] =
    rethrow(publishErasureResult(reservation, report, expiresAt, claim, completedAt))

  private def publishErasureResult(
      reservation: AnalyticsReportReservation,
      report: AnalyticsReportOutput,
      expiresAt: Instant,
      claim: ErasureClaim,
      completedAt: Instant
  ): Result[Unit] =
    if (!expiresAt.isAfter(report.asOf) || !expiresAt.isAfter(completedAt))
      reject(AnalyticsError.InvalidConfiguration("erasure snapshot expiry must follow publication time"))
    else
      transactional { session =>
        val requestFilter = Filters.and(
          Filters.eq(AnalyticsCollections.Fields.Id, claim.requestId.value),
          Filters.eq(AnalyticsCollections.Fields.State, ErasureRequestState.Processing.persistedName),
          Filters.eq(AnalyticsCollections.Fields.LeaseToken, claim.leaseToken),
          Filters.gt(AnalyticsCollections.Fields.LeaseUntil, Date.from(completedAt)),
          Filters.eq(AnalyticsCollections.Fields.Phase, ErasurePhase.ReadyToPublish.persistedName)
        )
        val nonReadyOther = Filters.and(
          Filters.ne(AnalyticsCollections.Fields.Id, claim.requestId.value),
          Filters.in(
            AnalyticsCollections.Fields.State,
            ErasureRequestState.Pending.persistedName,
            ErasureRequestState.Processing.persistedName
          ),
          Filters.ne(AnalyticsCollections.Fields.Phase, ErasurePhase.ReadyToPublish.persistedName)
        )
        val erasureRequests = rawDatabase.getCollection(AnalyticsCollections.ErasureRequests, classOf[Document])
        val users = rawDatabase.getCollection(AnalyticsCollections.Users, classOf[Document])
        val fences = rawDatabase.getCollection(AnalyticsCollections.OutboxSubjectFences, classOf[Document])
        val completion = rawDatabase.getCollection(AnalyticsCollections.ErasureCompletions, classOf[Document])
        for {
          request <- lift(streams.optional(erasureRequests.find(session.underlying, requestFilter).first))
          requestDoc <- result(request.toRight(AnalyticsError.ErasureNotReady))
          receiptId <- result {
            import BsonValueDecoder.given
            BsonDecoder.optional[String](
              requestDoc,
              AnalyticsCollections.Fields.ReceiptId,
              AnalyticsError.InvalidConfiguration("analytics report record is malformed")
            )
          }
          user <- lift(
            streams.optional(
              users.find(session.underlying, Filters.eq(AnalyticsCollections.Fields.Id, claim.requestId.value)).first
            )
          )
          userDoc <- result(user.toRight(AnalyticsError.ErasureNotReady))
          accountStatus <- result(requiredString(userDoc, AnalyticsCollections.Fields.AccountStatus))
          _ <- result(Either.cond(accountStatus == "Deleted", (), AnalyticsError.ErasureNotReady))
          fence <- lift(
            streams.optional(
              fences
                .find(
                  session.underlying,
                  Filters.and(
                    Filters.eq(AnalyticsCollections.Fields.Id, claim.requestId.value),
                    Filters.eq(AnalyticsCollections.Fields.Deleted, true)
                  )
                )
                .first
            )
          )
          _ <- result(Either.cond(fence.nonEmpty, (), AnalyticsError.ErasureNotReady))
          nonReadyCount <- lift(streams.one(erasureRequests.countDocuments(session.underlying, nonReadyOther)))
          _ <- result(Either.cond(nonReadyCount == 0L, (), AnalyticsError.ErasureNotReady))
          currentSnapshotRecord <- lift(
            streams.optional(
              typedSnapshots.find(session.underlying, Filters.eq(AnalyticsCollections.Fields.Id, "current")).first
            )
          )
          currentSnapshot <- result(currentSnapshotRecord.traverse(MongoAnalyticsReportRecords.decodeSnapshot))
          reserved <- lift(
            streams.optional(
              typedReservations
                .find(session.underlying, Filters.eq(AnalyticsCollections.Fields.Id, reservation.runId.value))
                .first
            )
          )
          reservedRecord <- result(reserved.toRight(AnalyticsError.RunIdRangeConflict(reservation.runId.value)))
          reservedValue <- result(MongoAnalyticsReportRecords.decodeRun(reservedRecord))
          _ <- result(
            Either.cond(
              reservedValue.reservation == reservation && reservedValue.state == "Reserved",
              (),
              AnalyticsError.RunIdRangeConflict(reservation.runId.value)
            )
          )
          state <- lift(
            streams.optional(
              typedControl
                .find(session.underlying, Filters.eq(AnalyticsCollections.Fields.Id, "analytics-report"))
                .first
            )
          )
          stateRecord <- result(state.toRight(AnalyticsError.RunIdRangeConflict(reservation.runId.value)))
          decodedState <- result(MongoAnalyticsReportRecords.decodeControl(stateRecord))
          _ <- result(
            Either.cond(
              decodedState.generation == reservation.generation && Set("Hidden", "Published")
                .contains(decodedState.state),
              (),
              AnalyticsError.RunIdRangeConflict(reservation.runId.value)
            )
          )
          _ <- result(
            Either.cond(
              reservation.revision > decodedState.lastPublishedRevision,
              (),
              AnalyticsError.RunIdRangeConflict(reservation.runId.value)
            )
          )
          changed <- lift(
            casUpdate(
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
                Updates.set(AnalyticsCollections.Fields.LastRunId, reservation.runId.value),
                Updates.unset(AnalyticsCollections.Fields.HiddenAt)
              )
            )
          )
          _ <- result(Either.cond(changed, (), AnalyticsError.RunIdRangeConflict(reservation.runId.value)))
          _ <- lift(
            streams.one(
              snapshots.replaceOne(
                session.underlying,
                Filters.eq(AnalyticsCollections.Fields.Id, "current"),
                reportDocument(
                  reservation,
                  report,
                  expiresAt,
                  currentSnapshot.fold(Map.empty[String, org.bson.BsonValue])(_.extraFields)
                ),
                new ReplaceOptions().upsert(true)
              )
            )
          )
          _ <- lift(
            streams.one(
              reservations.updateOne(
                session.underlying,
                Filters.and(
                  Filters.eq(AnalyticsCollections.Fields.Id, reservation.runId.value),
                  Filters.eq(AnalyticsCollections.Fields.State, "Reserved")
                ),
                Updates.set(AnalyticsCollections.Fields.State, "Published")
              )
            )
          )
          completed <- lift(
            casUpdate(
              session,
              erasureRequests,
              requestFilter,
              Updates.combine(
                Updates.set(AnalyticsCollections.Fields.State, ErasureRequestState.Complete.persistedName),
                Updates.set(AnalyticsCollections.Fields.Phase, ErasurePhase.ReportPublished.persistedName),
                Updates.set(AnalyticsCollections.Fields.Progress, 0),
                Updates.set(
                  AnalyticsCollections.Fields.ProgressKey,
                  ErasurePhase.ReportPublished.ordinal.toLong * ErasurePhase.ProgressPerPhase
                ),
                Updates.set(AnalyticsCollections.Fields.CompletedAt, Date.from(completedAt)),
                Updates.set(
                  AnalyticsCollections.Fields.ExpiresAt,
                  Date.from(
                    completedAt.plus(java.time.Duration.ofDays(operational.retention.deletionMarkerDays.value.toLong))
                  )
                ),
                Updates.unset(AnalyticsCollections.Fields.LeaseToken),
                Updates.unset(AnalyticsCollections.Fields.LeaseUntil)
              )
            )
          )
          _ <- result(Either.cond(completed, (), AnalyticsError.ErasureNotReady))
          updates = receiptId.fold(
            Updates.combine(
              Updates.setOnInsert(AnalyticsCollections.Fields.Id, claim.requestId.value),
              Updates.setOnInsert(AnalyticsCollections.Fields.CompletedAt, Date.from(completedAt))
            ): Bson
          )(id =>
            Updates.combine(
              Updates.setOnInsert(AnalyticsCollections.Fields.Id, claim.requestId.value),
              Updates.setOnInsert(AnalyticsCollections.Fields.CompletedAt, Date.from(completedAt)),
              Updates.setOnInsert(AnalyticsCollections.Fields.ReceiptId, id)
            )
          )
          _ <- lift(
            streams.one(
              completion.updateOne(
                session.underlying,
                Filters.eq(AnalyticsCollections.Fields.Id, claim.requestId.value),
                updates,
                new UpdateOptions().upsert(true)
              )
            )
          )
        } yield ()
      }

  private def requiredString(document: Document, field: String): Either[AnalyticsError, String] = {
    import BsonValueDecoder.given
    BsonDecoder.required[String](
      document,
      field,
      AnalyticsError.InvalidConfiguration("analytics report record is malformed")
    )
  }

  private def casUpdate(
      session: ClientSession[F],
      collection: ReactiveMongoCollection[Document],
      filter: Bson,
      update: Bson
  ): F[Boolean] =
    streams.one(collection.updateOne(session.underlying, filter, update)).map(_.getMatchedCount == 1L)

  private def reportDocument(
      reservation: AnalyticsReportReservation,
      report: AnalyticsReportOutput,
      expiresAt: Instant,
      extraFields: Map[String, org.bson.BsonValue] = Map.empty
  ): Document = {
    val document = new Document(AnalyticsCollections.Fields.Id, "current")
      .append(AnalyticsCollections.Fields.State, "Published")
      .append(AnalyticsCollections.Fields.Generation, reservation.generation)
      .append(AnalyticsCollections.Fields.Revision, reservation.revision)
      .append(AnalyticsCollections.Fields.RunId, reservation.runId.value)
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
    val withExtras = extraFields.foldLeft(document) { case (current, (name, value)) =>
      current.append(name, value)
    }
    report.timeToHire.fold(withExtras) { value =>
      withExtras.append(
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
