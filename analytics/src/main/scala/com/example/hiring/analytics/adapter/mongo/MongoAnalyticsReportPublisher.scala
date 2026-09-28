package com.example.hiring.analytics.adapter.mongo
import com.example.hiring.analytics.service.keyretirement.*
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.errors.*
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.config.*
import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.adapter.mongo.*
import com.example.hiring.analytics.adapter.kafka.*
import com.example.hiring.analytics.adapter.local.*
import com.example.hiring.analytics.service.erasure.*

import com.example.hiring.analytics.domain.AnalyticsReportOutput
import com.example.hiring.analytics.service.batch.{AnalyticsReportPublisher, AnalyticsReportReservation}

import com.example.hiring.analytics.*
import com.example.hiring.analytics.adapter.mongo.MongoAnalyticsReportRecords.*

import cats.data.EitherT
import cats.effect.{Async, Clock}
import cats.syntax.all.*
import com.mongodb.client.model.{Filters, ReplaceOptions, UpdateOptions, Updates}
import com.mongodb.reactivestreams.client.{ClientSession, MongoClient, MongoCollection, MongoDatabase}
import org.bson.Document
import org.bson.conversions.Bson

import java.time.Instant
import java.util.Date
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** Report revision allocation and publication use Mongo's reactive driver and transaction boundary. */
final class MongoAnalyticsReportPublisher[F[_]: Async: Clock](
    client: MongoClient,
    database: MongoDatabase,
    operational: AnalyticsOperationalSettings
) extends AnalyticsReportPublisher[F] {
  private val F = Async[F]
  private val clock = Clock[F]
  private val streams = new MongoPublisherStream(operational)
  private val control = database.getCollection(AnalyticsCollections.ReportControl, classOf[Document])
  private val reservations = database.getCollection(AnalyticsCollections.ReportRuns, classOf[Document])
  private val snapshots = database.getCollection(AnalyticsCollections.ReportSnapshots, classOf[Document])
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

  private type Result[A] = EitherT[F, AnalyticsError, A]
  private def lift[A](io: F[A]): Result[A] = EitherT.liftF(io)
  private def reject[A](error: AnalyticsError): Result[A] = EitherT.leftT[F, A](error)
  private def result[A](value: Either[AnalyticsError, A]): Result[A] = EitherT.fromEither[F](value)

  private def allocateRevision(session: ClientSession): Result[(Long, Long)] =
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
          typedControl.find(session, Filters.eq(AnalyticsCollections.Fields.Id, "analytics-report")).first()
        )
      )
      updatedControl <- result(
        updated.toRight(AnalyticsError.InvalidConfiguration("analytics report control is unavailable"))
      )
      updatedValue <- result(updatedControl.value)
      revision <- result(
        updatedValue.nextRevision.toRight(
          AnalyticsError.InvalidConfiguration("analytics report control is unavailable")
        )
      )
    } yield updatedValue.generation -> revision

  override def reserve(runId: RunId, rangeFingerprint: RangeFingerprint, now: Instant): F[AnalyticsReportReservation] =
    streams
      .optional(typedReservations.find(Filters.eq(AnalyticsCollections.Fields.Id, runId.value)).first())
      .flatMap {
        case Some(previous) =>
          F.fromEither(previous.value).flatMap { record =>
            val existing = record.reservation
            if (existing.rangeFingerprint != rangeFingerprint)
              F.raiseError(AnalyticsError.RunIdRangeConflict(runId.value))
            else if (record.state != "Reserved") F.pure(existing)
            else
              MongoSession.resource(client, streams).use { session =>
                val work = for {
                  current <- lift(
                    streams.optional(
                      typedControl
                        .find(session, Filters.eq(AnalyticsCollections.Fields.Id, "analytics-report"))
                        .first()
                    )
                  )
                  controlRecord <- result(
                    current.toRight(AnalyticsError.InvalidConfiguration("analytics report control is unavailable"))
                  )
                  decoded <- result(controlRecord.value)
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
                streams.transaction(session)(work.value.flatMap(F.fromEither))
              }
          }
        case None =>
          MongoSession.resource(client, streams).use { session =>
            val work = for {
              current <- lift(
                streams.optional(
                  typedControl.find(session, Filters.eq(AnalyticsCollections.Fields.Id, "analytics-report")).first()
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
                    session,
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
            streams.transaction(session)(work.value.flatMap(F.fromEither))
          }
      }
      .adaptError {
        case error: AnalyticsError => error
        case NonFatal(cause)       => AnalyticsError.MongoConnectionFailure(cause)
      }

  override def publish(
      reservation: AnalyticsReportReservation,
      report: AnalyticsReportOutput,
      expiresAt: Instant
  ): F[Unit] =
    if (!expiresAt.isAfter(report.asOf))
      F.raiseError(AnalyticsError.InvalidConfiguration("report expiry must follow publication time"))
    else
      MongoSession
        .resource(client, streams)
        .use { session =>
          val work = for {
            reserved <- lift(
              streams.optional(
                typedReservations
                  .find(session, Filters.eq(AnalyticsCollections.Fields.Id, reservation.runId.value))
                  .first()
              )
            )
            reservedRecord <- result(
              reserved.toRight(AnalyticsError.RunIdRangeConflict(reservation.runId.value))
            )
            decoded <- result(reservedRecord.value)
            _ <- result(
              Either.cond(
                decoded.reservation == reservation,
                (),
                AnalyticsError.RunIdRangeConflict(reservation.runId.value)
              )
            )
            state <- lift(
              streams.optional(
                typedControl.find(session, Filters.eq(AnalyticsCollections.Fields.Id, "analytics-report")).first()
              )
            )
            stateRecord <- result(state.toRight(AnalyticsError.RunIdRangeConflict(reservation.runId.value)))
            decodedState <- result(stateRecord.value)
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
                typedSnapshots.find(session, Filters.eq(AnalyticsCollections.Fields.Id, "current")).first()
              )
            )
            snapshot <- result(snapshotRecord.traverse(_.value))
            alreadyPublished = decodedState.lastPublishedRevision == reservation.revision &&
              decodedState.lastRunId.getOrElse("") == reservation.runId.value
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
                      session,
                      Filters.eq(AnalyticsCollections.Fields.Id, "current"),
                      reportDocument(reservation, report, expiresAt),
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
                        session,
                        Filters.eq(AnalyticsCollections.Fields.Id, "current"),
                        reportDocument(reservation, report, expiresAt),
                        new ReplaceOptions().upsert(true)
                      )
                    )
                  )
                  _ <- lift(
                    streams.one(
                      reservations.updateOne(
                        session,
                        Filters.eq(AnalyticsCollections.Fields.Id, reservation.runId.value),
                        Updates.set(AnalyticsCollections.Fields.State, "Published")
                      )
                    )
                  )
                } yield ()
          } yield ()
          streams.transaction(session)(work.value.flatMap(F.fromEither))
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
  ): F[Unit] =
    if (!expiresAt.isAfter(report.asOf) || !expiresAt.isAfter(completedAt))
      F.raiseError(AnalyticsError.InvalidConfiguration("erasure snapshot expiry must follow publication time"))
    else
      MongoSession
        .resource(client, streams)
        .use { session =>
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
          val erasureRequests = database.getCollection(AnalyticsCollections.ErasureRequests, classOf[Document])
          val users = database.getCollection(AnalyticsCollections.Users, classOf[Document])
          val fences = database.getCollection(AnalyticsCollections.OutboxSubjectFences, classOf[Document])
          val completion = database.getCollection(AnalyticsCollections.ErasureCompletions, classOf[Document])
          val work = for {
            request <- lift(streams.optional(erasureRequests.find(session, requestFilter).first()))
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
                users.find(session, Filters.eq(AnalyticsCollections.Fields.Id, claim.requestId.value)).first()
              )
            )
            userDoc <- result(user.toRight(AnalyticsError.ErasureNotReady))
            accountStatus <- result(requiredString(userDoc, AnalyticsCollections.Fields.AccountStatus))
            _ <- result(Either.cond(accountStatus == "Deleted", (), AnalyticsError.ErasureNotReady))
            fence <- lift(
              streams.optional(
                fences
                  .find(
                    session,
                    Filters.and(
                      Filters.eq(AnalyticsCollections.Fields.Id, claim.requestId.value),
                      Filters.eq(AnalyticsCollections.Fields.Deleted, true)
                    )
                  )
                  .first()
              )
            )
            _ <- result(Either.cond(fence.nonEmpty, (), AnalyticsError.ErasureNotReady))
            nonReadyCount <- lift(streams.one(erasureRequests.countDocuments(session, nonReadyOther)))
            _ <- result(Either.cond(nonReadyCount == 0L, (), AnalyticsError.ErasureNotReady))
            reserved <- lift(
              streams.optional(
                typedReservations
                  .find(session, Filters.eq(AnalyticsCollections.Fields.Id, reservation.runId.value))
                  .first()
              )
            )
            reservedRecord <- result(reserved.toRight(AnalyticsError.RunIdRangeConflict(reservation.runId.value)))
            reservedValue <- result(reservedRecord.value)
            _ <- result(
              Either.cond(
                reservedValue.reservation == reservation && reservedValue.state == "Reserved",
                (),
                AnalyticsError.RunIdRangeConflict(reservation.runId.value)
              )
            )
            state <- lift(
              streams.optional(
                typedControl.find(session, Filters.eq(AnalyticsCollections.Fields.Id, "analytics-report")).first()
              )
            )
            stateRecord <- result(state.toRight(AnalyticsError.RunIdRangeConflict(reservation.runId.value)))
            decodedState <- result(stateRecord.value)
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
                  session,
                  Filters.eq(AnalyticsCollections.Fields.Id, "current"),
                  reportDocument(reservation, report, expiresAt),
                  new ReplaceOptions().upsert(true)
                )
              )
            )
            _ <- lift(
              streams.one(
                reservations.updateOne(
                  session,
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
                      completedAt.plus(java.time.Duration.ofDays(operational.retention.deletionMarkerDays.toLong))
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
                  session,
                  Filters.eq(AnalyticsCollections.Fields.Id, claim.requestId.value),
                  updates,
                  new UpdateOptions().upsert(true)
                )
              )
            )
          } yield ()
          streams.transaction(session)(work.value.flatMap(F.fromEither))
        }
        .adaptError {
          case error: AnalyticsError => error
          case NonFatal(cause)       => AnalyticsError.MongoConnectionFailure(cause)
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
      session: ClientSession,
      collection: MongoCollection[Document],
      filter: Bson,
      update: Bson
  ): F[Boolean] =
    streams.one(collection.updateOne(session, filter, update)).map(_.getMatchedCount == 1L)

  private def reportDocument(
      reservation: AnalyticsReportReservation,
      report: AnalyticsReportOutput,
      expiresAt: Instant
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
