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
import com.mongodb.client.model.{Filters, Projections, UpdateOptions, Updates}
import mongo4cats.client.{ClientSession, MongoClient}
import mongo4cats.collection.MongoCollection
import mongo4cats.database.MongoDatabase
import mongo4cats.errors.MongoJsonParsingException
import org.bson.Document
import org.bson.conversions.Bson

import java.time.Instant
import java.util.Date
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** Report revision allocation and publication use typed Mongo collections and transaction boundaries. */
final class MongoAnalyticsReportPublisher[F[_]: Async](
    client: MongoClient[F],
    database: MongoDatabase[F],
    operational: AnalyticsOperationalSettings
) extends AnalyticsReportPublisher[F] {
  private val clock = Clock[F]
  private val streams = new MongoPublisherStream(operational)
  private val controlProjection = Projections.include(
    AnalyticsCollections.Fields.Id,
    AnalyticsCollections.Fields.Generation,
    AnalyticsCollections.Fields.NextRevision,
    AnalyticsCollections.Fields.LastPublishedRevision,
    AnalyticsCollections.Fields.LastRunId,
    AnalyticsCollections.Fields.State
  )
  private val reservationProjection = Projections.include(
    AnalyticsCollections.Fields.Id,
    AnalyticsCollections.Fields.RangeFingerprint,
    AnalyticsCollections.Fields.Generation,
    AnalyticsCollections.Fields.Revision,
    AnalyticsCollections.Fields.State,
    AnalyticsCollections.Fields.CreatedAt,
    AnalyticsCollections.Fields.ExpiresAt
  )
  private final case class Collections(
      control: MongoCollection[F, AnalyticsMongoRecords.ReportControl],
      reservations: MongoCollection[F, AnalyticsMongoRecords.ReportRun],
      snapshots: MongoCollection[F, AnalyticsMongoRecords.ReportSnapshotMetadata],
      requests: MongoCollection[F, AnalyticsMongoRecords.ErasureRequest],
      users: MongoCollection[F, AnalyticsMongoRecords.UserAccountStatus],
      fences: MongoCollection[F, AnalyticsMongoRecords.PublisherFence],
      completions: MongoCollection[F, AnalyticsMongoRecords.ErasureCompletion]
  )

  private def collections: F[Collections] =
    (
      database.getCollection[AnalyticsMongoRecords.ReportControl](
        AnalyticsCollections.ReportControl,
        AnalyticsMongoRecords.reportControlRegistry
      ),
      database.getCollection[AnalyticsMongoRecords.ReportRun](
        AnalyticsCollections.ReportRuns,
        AnalyticsMongoRecords.reportRunRegistry
      ),
      database.getCollection[AnalyticsMongoRecords.ReportSnapshotMetadata](
        AnalyticsCollections.ReportSnapshots,
        AnalyticsMongoRecords.reportSnapshotRegistry
      ),
      database.getCollection[AnalyticsMongoRecords.ErasureRequest](
        AnalyticsCollections.ErasureRequests,
        AnalyticsMongoRecords.erasureRequestRegistry
      ),
      database.getCollection[AnalyticsMongoRecords.UserAccountStatus](
        AnalyticsCollections.Users,
        AnalyticsMongoRecords.userAccountStatusRegistry
      ),
      database.getCollection[AnalyticsMongoRecords.PublisherFence](
        AnalyticsCollections.OutboxSubjectFences,
        AnalyticsMongoRecords.publisherFenceRegistry
      ),
      database.getCollection[AnalyticsMongoRecords.ErasureCompletion](
        AnalyticsCollections.ErasureCompletions,
        AnalyticsMongoRecords.erasureCompletionRegistry
      )
    ).mapN(Collections.apply)

  private type Result[A] = EitherT[F, AnalyticsError, A]
  private def lift[A](io: F[A]): Result[A] = EitherT.liftF(io)
  private def reject[A](error: AnalyticsError): Result[A] = EitherT.leftT[F, A](error)
  private def result[A](value: Either[AnalyticsError, A]): Result[A] = EitherT.fromEither[F](value)
  private def withCollections[A](work: Collections => Result[A]): Result[A] =
    lift(collections).flatMap(work)
  private def transactional[A](work: ClientSession[F] => Result[A]): Result[A] =
    EitherT(MongoSession.resource(client, streams).use(session => streams.transaction(session)(work(session)).value))

  private def rethrow[A](result: Result[A]): F[A] =
    result.rethrowT.adaptError {
      case error: AnalyticsError        => error
      case _: MongoJsonParsingException =>
        AnalyticsError.InvalidConfiguration("analytics report record is malformed")
      case NonFatal(cause) => AnalyticsError.MongoConnectionFailure(cause)
    }

  private def allocateRevision(session: ClientSession[F], collections: Collections): Result[(Long, Long)] =
    for {
      changed <- lift(
        casUpdate(
          session,
          collections.control,
          Filters.eq(AnalyticsCollections.Fields.Id, "analytics-report"),
          Updates.inc(AnalyticsCollections.Fields.NextRevision, 1L)
        )
      )
      _ <- result(
        Either.cond(changed, (), AnalyticsError.InvalidConfiguration("analytics report control is unavailable"))
      )
      updated <- lift(
        streams.optional(
          collections.control.underlying
            .find(session.underlying, Filters.eq(AnalyticsCollections.Fields.Id, "analytics-report"))
            .projection(controlProjection)
            .first
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
  ): Result[AnalyticsReportReservation] = withCollections { collections =>
    lift(
      collections.reservations
        .find(Filters.eq(AnalyticsCollections.Fields.Id, runId.value))
        .projection(reservationProjection)
        .first
    )
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
                      collections.control.underlying
                        .find(session.underlying, Filters.eq(AnalyticsCollections.Fields.Id, "analytics-report"))
                        .projection(controlProjection)
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
                        allocated <- allocateRevision(session, collections)
                        value = existing.copy(generation = allocated._1, revision = allocated._2)
                        replaced <- lift(
                          casUpdate(
                            session,
                            collections.reservations,
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
                  collections.control.underlying
                    .find(session.underlying, Filters.eq(AnalyticsCollections.Fields.Id, "analytics-report"))
                    .projection(controlProjection)
                    .first
                )
              )
              _ <- result(
                current.toRight(
                  AnalyticsError.InvalidConfiguration("analytics report control is not initialized")
                )
              )
              allocated <- allocateRevision(session, collections)
              value = AnalyticsReportReservation(runId, rangeFingerprint, allocated._1, allocated._2)
              _ <- lift(
                streams.one(
                  collections.reservations.underlying.insertOne(
                    session.underlying,
                    AnalyticsMongoRecords.ReportRun(
                      runId.value,
                      rangeFingerprint.value,
                      value.generation,
                      value.revision,
                      "Reserved",
                      Some(now),
                      Some(now.plusMillis(operational.reportReservationTtl.toMillis))
                    )
                  )
                )
              )
            } yield value
          }
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
  ): Result[Unit] = withCollections { collections =>
    if (!expiresAt.isAfter(report.asOf))
      reject(AnalyticsError.InvalidConfiguration("report expiry must follow publication time"))
    else
      transactional { session =>
        for {
          reserved <- lift(
            streams.optional(
              collections.reservations.underlying
                .find(session.underlying, Filters.eq(AnalyticsCollections.Fields.Id, reservation.runId.value))
                .projection(reservationProjection)
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
              collections.control.underlying
                .find(session.underlying, Filters.eq(AnalyticsCollections.Fields.Id, "analytics-report"))
                .projection(controlProjection)
                .first
            )
          )
          stateRecord <- result(state.toRight(AnalyticsError.RunIdRangeConflict(reservation.runId.value)))
          decodedState <- result(MongoAnalyticsReportRecords.decodeControl(stateRecord))
          _ <- result(
            Either.cond(
              decodedState.generation == reservation.generation &&
                Set("Published", "Unpublished").contains(decodedState.state),
              (),
              AnalyticsError.RunIdRangeConflict(reservation.runId.value)
            )
          )
          currentTime <- lift(clock.realTimeInstant)
          snapshotRecord <- lift(readSnapshot(session, collections.snapshots))
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
                .exists(v => v.matches(reservation) && v.expiresAt.exists(_.isAfter(currentTime)))
            )
              EitherT.pure[F, AnalyticsError](())
            else if (alreadyPublished && snapshot.exists(_.expiresAt.exists(_.isAfter(currentTime))))
              reject(AnalyticsError.RunIdRangeConflict(reservation.runId.value))
            else if (alreadyPublished)
              lift(
                streams.one(
                  collections.snapshots.underlying.updateOne(
                    session.underlying,
                    Filters.eq(AnalyticsCollections.Fields.Id, "current"),
                    reportUpdate(reservation, report, expiresAt),
                    new UpdateOptions().upsert(true)
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
                    collections.control,
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
                    collections.snapshots.underlying.updateOne(
                      session.underlying,
                      Filters.eq(AnalyticsCollections.Fields.Id, "current"),
                      reportUpdate(reservation, report, expiresAt),
                      new UpdateOptions().upsert(true)
                    )
                  )
                )
                _ <- lift(
                  streams.one(
                    collections.reservations.underlying.updateOne(
                      session.underlying,
                      Filters.eq(AnalyticsCollections.Fields.Id, reservation.runId.value),
                      Updates.set(AnalyticsCollections.Fields.State, "Published")
                    )
                  )
                )
              } yield ()
        } yield ()
      }
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
  ): Result[Unit] = withCollections { collections =>
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
        for {
          request <- lift(
            streams.optional(
              collections.requests.underlying
                .find(session.underlying, requestFilter)
                .projection(Projections.include(AnalyticsCollections.Fields.Id, AnalyticsCollections.Fields.ReceiptId))
                .first
            )
          )
          requestDoc <- result(request.toRight(AnalyticsError.GuardedErasurePublicationRejected))
          receiptId = requestDoc.receiptId
          user <- lift(
            streams.optional(
              collections.users.underlying
                .find(session.underlying, Filters.eq(AnalyticsCollections.Fields.Id, claim.requestId.value))
                .projection(
                  Projections.include(AnalyticsCollections.Fields.Id, AnalyticsCollections.Fields.AccountStatus)
                )
                .first
            )
          )
          userDoc <- result(user.toRight(AnalyticsError.GuardedErasurePublicationRejected))
          accountStatus = userDoc.accountStatus
          _ <- result(Either.cond(accountStatus == "Deleted", (), AnalyticsError.GuardedErasurePublicationRejected))
          fence <- lift(
            streams.optional(
              collections.fences.underlying
                .find(
                  session.underlying,
                  Filters.and(
                    Filters.eq(AnalyticsCollections.Fields.Id, claim.requestId.value),
                    Filters.eq(AnalyticsCollections.Fields.Deleted, true)
                  )
                )
                .projection(
                  Projections.include(
                    AnalyticsCollections.Fields.Id,
                    AnalyticsCollections.Fields.Deleted,
                    AnalyticsCollections.Fields.LeaseToken,
                    AnalyticsCollections.Fields.LeaseUntil
                  )
                )
                .first
            )
          )
          _ <- result(Either.cond(fence.nonEmpty, (), AnalyticsError.GuardedErasurePublicationRejected))
          nonReadyCount <- lift(
            streams.one(collections.requests.underlying.countDocuments(session.underlying, nonReadyOther))
          )
          _ <- result(Either.cond(nonReadyCount == 0L, (), AnalyticsError.GuardedErasurePublicationRejected))
          reserved <- lift(
            streams.optional(
              collections.reservations.underlying
                .find(session.underlying, Filters.eq(AnalyticsCollections.Fields.Id, reservation.runId.value))
                .projection(reservationProjection)
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
              collections.control.underlying
                .find(session.underlying, Filters.eq(AnalyticsCollections.Fields.Id, "analytics-report"))
                .projection(controlProjection)
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
              collections.control,
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
              collections.snapshots.underlying.updateOne(
                session.underlying,
                Filters.eq(AnalyticsCollections.Fields.Id, "current"),
                reportUpdate(reservation, report, expiresAt),
                new UpdateOptions().upsert(true)
              )
            )
          )
          _ <- lift(
            streams.one(
              collections.reservations.underlying.updateOne(
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
              collections.requests,
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
          _ <- result(Either.cond(completed, (), AnalyticsError.GuardedErasurePublicationRejected))
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
              collections.completions.underlying.updateOne(
                session.underlying,
                Filters.eq(AnalyticsCollections.Fields.Id, claim.requestId.value),
                updates,
                new UpdateOptions().upsert(true)
              )
            )
          )
        } yield ()
      }
  }

  private def casUpdate(
      session: ClientSession[F],
      collection: MongoCollection[F, ?],
      filter: Bson,
      update: Bson
  ): F[Boolean] =
    streams.one(collection.underlying.updateOne(session.underlying, filter, update)).map(_.getMatchedCount == 1L)

  private def readSnapshot(
      session: ClientSession[F],
      snapshots: MongoCollection[F, AnalyticsMongoRecords.ReportSnapshotMetadata]
  ): F[Option[AnalyticsMongoRecords.ReportSnapshotMetadata]] =
    streams.optional(
      snapshots.underlying
        .find(session.underlying, Filters.eq(AnalyticsCollections.Fields.Id, "current"))
        .projection(
          Projections.include(
            AnalyticsCollections.Fields.Id,
            AnalyticsCollections.Fields.Generation,
            AnalyticsCollections.Fields.Revision,
            AnalyticsCollections.Fields.RunId,
            AnalyticsCollections.Fields.ExpiresAt
          )
        )
        .first
    )

  private def reportUpdate(
      reservation: AnalyticsReportReservation,
      report: AnalyticsReportOutput,
      expiresAt: Instant
  ): Bson = {
    val sets = Vector[Bson](
      Updates.set(AnalyticsCollections.Fields.Id, "current"),
      Updates.set(AnalyticsCollections.Fields.State, "Published"),
      Updates.set(AnalyticsCollections.Fields.Generation, reservation.generation),
      Updates.set(AnalyticsCollections.Fields.Revision, reservation.revision),
      Updates.set(AnalyticsCollections.Fields.RunId, reservation.runId.value),
      Updates.set(AnalyticsCollections.Fields.AsOf, Date.from(report.asOf)),
      Updates.set(AnalyticsCollections.Fields.ExpiresAt, Date.from(expiresAt)),
      Updates.set(
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
      ),
      Updates.set(
        "skillPostingActivity",
        report.skillPostingActivity
          .map(row =>
            new Document("day", Date.from(row.day)).append("skill", row.skill).append("postings", row.postings)
          )
          .asJava
      )
    )
    val timeToHire = report.timeToHire.fold(Vector(Updates.unset("timeToHire")): Vector[Bson]) { value =>
      Vector(
        Updates.set(
          "timeToHire",
          new Document("p50Hours", value.p50Hours)
            .append("p75Hours", value.p75Hours)
            .append("p90Hours", value.p90Hours)
            .append("p95Hours", value.p95Hours)
            .append("eligibleCount", value.eligibleCount)
            .append("excludedCount", value.excludedCount)
        )
      )
    }
    Updates.combine((sets ++ timeToHire).asJava)
  }
}
