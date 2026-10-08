package com.example.hiring.analytics.adapter.mongo

import cats.effect.Async
import cats.syntax.all.*
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.*
import com.mongodb.{MongoException, ReadConcern, WriteConcern}
import com.mongodb.client.model.{Filters, Updates, Indexes, IndexOptions}
import io.circe.generic.auto.*
import mongo4cats.circe.MongoJsonCodecs
import mongo4cats.database.MongoDatabase

import java.time.Instant
import java.util.Date
import java.util.concurrent.TimeUnit
import scala.util.control.NonFatal
import scala.jdk.CollectionConverters.*

/** Natural-key Mongo journal. Completed request digests survive detailed-coordinate compaction. */
private[analytics] final class MongoAnalyticsLateFactReplayJournal[F[_]: Async](
    database: MongoDatabase[F],
    streams: MongoPublisherStream,
    lakehouseRoot: String
) extends AnalyticsLateFactReplayJournal[F] {
  import MongoAnalyticsLateFactReplayJournal.*
  private val F = Async[F]
  // PCRE equivalent of Character.isWhitespace, used by Iron Blank.
  private val blankRunIdPattern =
    "^[\\x{0009}-\\x{000D}\\x{001C}-\\x{0020}\\x{1680}\\x{2000}-\\x{2006}\\x{2008}-\\x{200A}\\x{2028}\\x{2029}\\x{205F}\\x{3000}]*$"
  private val lakehouseId = AnalyticsLakehouseIdentity
    .from(lakehouseRoot)
    .leftMap(_ => AnalyticsError.InvalidConfiguration("replay journal lakehouse identity is invalid"))
  private val records = database
    .withReadConcern(ReadConcern.MAJORITY)
    .getCollection[Record](CollectionName, registry)
    .map(_.withWriteConcern(WriteConcern.MAJORITY.withJournal(true).withWTimeout(15000L, TimeUnit.MILLISECONDS)))

  /** Repeatable index provisioning; no TTL can remove immutable request bindings. */
  def ensureIndexes: F[Unit] = guarded(
    records
      .flatMap(value =>
        streams.one(
          value.underlying.createIndex(
            Indexes.compoundIndex(Indexes.ascending("lakehouseId"), Indexes.ascending("publishedAt")),
            new IndexOptions()
              .name("analytics_late_fact_replay_details_retention_v1")
              .partialFilterExpression(
                Filters.and(Filters.eq("progress", "Published"), Filters.exists("coordinates", true))
              )
          )
        )
      )
      .void *> records
      .flatMap(value =>
        streams.one(
          value.underlying.createIndex(
            Indexes.compoundIndex(Indexes.ascending("lakehouseId"), Indexes.ascending("progress")),
            new IndexOptions().name("analytics_late_fact_replay_receipt_dependencies_v1")
          )
        )
      )
      .void
  )

  private def id(requestId: AnalyticsReplayRequestId): F[String] = F
    .fromEither(lakehouseId)
    .map(_ + ":" + requestId.value)

  override def load(requestId: AnalyticsReplayRequestId): F[Option[AnalyticsLateFactReplayRecord]] =
    guarded(for {
      key <- id(requestId)
      stored <- records.flatMap(value => streams.optional(value.underlying.find(Filters.eq("_id", key)).first()))
      decoded <- stored.traverse(value => decode(requestId, value).liftTo[F])
    } yield decoded)

  override def prepare(
      request: AnalyticsLateFactReplayRequest,
      reservation: AnalyticsReportReservation,
      at: Instant
  ): F[AnalyticsLateFactReplayRecord] = guarded(for {
    _ <- validateReservation(request, 0, reservation).liftTo[F]
    key <- id(request.requestId)
    root <- F.fromEither(lakehouseId)
    value = Record(
      key,
      root,
      request.requestId.value,
      request.selectionDigest,
      Some(
        request.coordinates.map(coordinate =>
          Coordinate(
            AnalyticsTopic.unwrap(coordinate.topic),
            AnalyticsPartition.unwrap(coordinate.partition),
            AnalyticsOffset.unwrap(coordinate.offset)
          )
        )
      ),
      AnalyticsLateFactReplayProgress.Prepared.toString,
      0,
      reservation.runId.value,
      reservation.rangeFingerprint.value,
      reservation.generation,
      reservation.revision,
      at,
      at,
      None
    )
    _ <- records.flatMap(_.insertOne(value)).void.handleErrorWith {
      case error: MongoException if error.getCode == 11000 => F.unit
      case error                                           => F.raiseError(error)
    }
    existing <- required(request)
    _ <- F.raiseUnless(existing.publicationAttempt == 0 && existing.reservation == reservation)(Conflict)
  } yield existing)

  override def markFactsMerged(request: AnalyticsLateFactReplayRequest, at: Instant): F[Unit] =
    transition(request, AnalyticsLateFactReplayProgress.Prepared, AnalyticsLateFactReplayProgress.FactsMerged, at)

  override def markPublished(request: AnalyticsLateFactReplayRequest, at: Instant): F[Unit] =
    transition(request, AnalyticsLateFactReplayProgress.FactsMerged, AnalyticsLateFactReplayProgress.Published, at)

  private def transition(
      request: AnalyticsLateFactReplayRequest,
      previous: AnalyticsLateFactReplayProgress,
      next: AnalyticsLateFactReplayProgress,
      at: Instant
  ): F[Unit] = guarded(for {
    current <- required(request)
    _ <- current.progress match {
      case value
          if value == next || (next == AnalyticsLateFactReplayProgress.FactsMerged &&
            value == AnalyticsLateFactReplayProgress.Published) =>
        F.unit
      case value if value == previous =>
        for {
          key <- id(request.requestId)
          update = Updates.combine(Updates.set("progress", next.toString), Updates.set("updatedAt", Date.from(at)))
          withPublication =
            if (next == AnalyticsLateFactReplayProgress.Published)
              Updates.combine(update, Updates.set("publishedAt", Date.from(at)))
            else update
          changed <- records.flatMap(value =>
            streams.one(
              value.underlying.updateOne(
                Filters.and(
                  Filters.eq("_id", key),
                  Filters.eq("selectionDigest", request.selectionDigest),
                  Filters.eq("publicationAttempt", current.publicationAttempt),
                  Filters.eq("progress", previous.toString)
                ),
                withPublication
              )
            )
          )
          _ <- F.raiseUnless(changed.getMatchedCount == 1L)(Conflict)
        } yield ()
      case _ => F.raiseError[Unit](Conflict)
    }
  } yield ())

  override def advancePublicationAttempt(
      request: AnalyticsLateFactReplayRequest,
      expectedAttempt: Int,
      reservation: AnalyticsReportReservation,
      at: Instant
  ): F[AnalyticsLateFactReplayRecord] = guarded(for {
    _ <- F.raiseUnless(
      expectedAttempt >= 0 && expectedAttempt + 1 < AnalyticsLateFactReplayService.MaximumPublicationAttempts
    )(Conflict)
    _ <- validateReservation(request, expectedAttempt + 1, reservation).liftTo[F]
    current <- required(request)
    key <- id(request.requestId)
    _ <-
      if (current.publicationAttempt == expectedAttempt + 1 && current.reservation == reservation) F.unit
      else if (
        current.publicationAttempt == expectedAttempt && current.progress != AnalyticsLateFactReplayProgress.Published
      )
        records
          .flatMap(value =>
            streams.one(
              value.underlying.updateOne(
                Filters.and(
                  Filters.eq("_id", key),
                  Filters.eq("selectionDigest", request.selectionDigest),
                  Filters.eq("publicationAttempt", expectedAttempt),
                  Filters.ne("progress", "Published")
                ),
                Updates.combine(
                  Updates.set("publicationAttempt", expectedAttempt + 1),
                  Updates.set("progress", AnalyticsLateFactReplayProgress.Prepared.toString),
                  Updates.set("runId", reservation.runId.value),
                  Updates.set("rangeFingerprint", reservation.rangeFingerprint.value),
                  Updates.set("generation", reservation.generation),
                  Updates.set("revision", reservation.revision),
                  Updates.set("updatedAt", Date.from(at))
                )
              )
            )
          )
          .flatMap(result => F.raiseUnless(result.getMatchedCount == 1L)(Conflict))
      else F.raiseError[Unit](Conflict)
    next <- required(request)
    _ <- F.raiseUnless(next.publicationAttempt == expectedAttempt + 1 && next.reservation == reservation)(Conflict)
  } yield next)

  /** No TTL: remove only completed coordinate details; retain request identity and receipt indefinitely. */
  def compactCompleted(at: Instant): F[Long] = guarded(for {
    root <- F.fromEither(lakehouseId)
    result <- records.flatMap(value =>
      streams.one(
        value.underlying.updateMany(
          Filters.and(
            Filters.eq("lakehouseId", root),
            Filters.eq("progress", "Published"),
            Filters.lt("publishedAt", Date.from(at.minusSeconds(CompletedDetailsRetentionDays * 86400L))),
            Filters.exists("coordinates", true)
          ),
          Updates.unset("coordinates")
        )
      )
    )
  } yield result.getModifiedCount)

  /** Preserve receipts for all unfinished requests and completed requests with retained detail. */
  def referencedPublicationRunIds(candidates: Set[RunId]): F[Set[RunId]] = guarded(for {
    root <- F.fromEither(lakehouseId)
    _ <- records
      .flatMap(value =>
        streams.optional(
          value.underlying
            .find(
              Filters.and(
                Filters.eq("lakehouseId", root),
                Filters.or(Filters.ne("progress", "Published"), Filters.exists("coordinates", true)),
                Filters
                  .or(
                    Filters.expr(
                      new org.bson.Document(
                        "$ne",
                        Vector[AnyRef](new org.bson.Document("$type", "$runId"), "string").asJava
                      )
                    ),
                    Filters.regex("runId", blankRunIdPattern)
                  )
              )
            )
            .first
        )
      )
      .flatMap(value => F.raiseWhen(value.nonEmpty)(Conflict))
    dependencies <- records.flatMap(value =>
      streams
        .stream(
          value.underlying.find(
            Filters.and(
              Filters.eq("lakehouseId", root),
              Filters.in("runId", candidates.toVector.map(_.value).asJava),
              Filters.or(Filters.ne("progress", "Published"), Filters.exists("coordinates", true))
            )
          )
        )
        .evalMap { stored =>
          for {
            requestId <- F.fromEither(AnalyticsReplayRequestId.from(stored.requestId).leftMap(_ => Conflict))
            decoded <- F.fromEither(decode(requestId, stored))
          } yield decoded.reservation.runId
        }
        .compile
        .fold(Set.empty[RunId])(_ + _)
    )
  } yield dependencies)

  private def required(request: AnalyticsLateFactReplayRequest): F[AnalyticsLateFactReplayRecord] =
    load(request.requestId)
      .flatMap(_.liftTo[F](Conflict))
      .flatTap(value => F.raiseUnless(value.selectionDigest == request.selectionDigest)(Conflict))

  private def decode(
      requestId: AnalyticsReplayRequestId,
      value: Record
  ): Either[AnalyticsError, AnalyticsLateFactReplayRecord] =
    for {
      root <- lakehouseId
      _ <- Either.cond(
        value._id == root + ":" + requestId.value && value.lakehouseId == root && value.requestId == requestId.value,
        (),
        Conflict
      )
      _ <- Either.cond(value.selectionDigest.matches("[a-f0-9]{64}"), (), Conflict)
      progress <- AnalyticsLateFactReplayProgress.values.find(_.toString == value.progress).toRight(Conflict)
      _ <- Either.cond(
        value.publicationAttempt >= 0 && value.publicationAttempt < AnalyticsLateFactReplayService.MaximumPublicationAttempts,
        (),
        Conflict
      )
      _ <- value.coordinates match {
        case Some(coordinates) if coordinates.size <= AnalyticsLateFactReplayRequest.MaximumCoordinates =>
          AnalyticsLateFactReplayRequest
            .from(value.requestId, coordinates.map(row => (row.topic, row.partition, row.offset)))
            .toEither
            .leftMap(_ => Conflict)
            .flatMap(request => Either.cond(request.selectionDigest == value.selectionDigest, (), Conflict))
        case Some(_) => Left(Conflict)
        case None    => Either.cond(progress == AnalyticsLateFactReplayProgress.Published, (), Conflict)
      }
      runId <- RunId.from(value.runId).leftMap(_ => Conflict)
      fingerprint <- RangeFingerprint.from(value.rangeFingerprint).leftMap(_ => Conflict)
      _ <- Either.cond(
        value.runId == s"late-replay-${requestId.value}-${value.publicationAttempt}" &&
          value.rangeFingerprint == value.selectionDigest && value.generation >= 0 && value.revision >= 0,
        (),
        Conflict
      )
      _ <- Either.cond(
        progress != AnalyticsLateFactReplayProgress.Published || value.publishedAt.nonEmpty,
        (),
        Conflict
      )
    } yield AnalyticsLateFactReplayRecord(
      value.selectionDigest,
      progress,
      value.publicationAttempt,
      AnalyticsReportReservation(runId, fingerprint, value.generation, value.revision)
    )

  private def validateReservation(
      request: AnalyticsLateFactReplayRequest,
      attempt: Int,
      reservation: AnalyticsReportReservation
  ): Either[AnalyticsError, Unit] =
    AnalyticsLateFactReplayService.reservationIdentityFor(request, attempt).flatMap { case (runId, fingerprint) =>
      Either.cond(
        reservation.runId == runId && reservation.rangeFingerprint == fingerprint &&
          reservation.generation >= 0L && reservation.revision >= 0L,
        (),
        Conflict
      )
    }

  private def guarded[A](work: F[A]): F[A] = work.adaptError {
    case error: AnalyticsError                          => error
    case _: mongo4cats.errors.MongoJsonParsingException => Conflict
    case NonFatal(cause)                                => AnalyticsError.MongoConnectionFailure(cause)
  }
}

private[analytics] object MongoAnalyticsLateFactReplayJournal {
  private object mongoDateCodecs extends MongoJsonCodecs
  import mongoDateCodecs.*

  val CollectionName: String = "analytics_late_fact_replay_requests"
  val CompletedDetailsRetentionDays: Long = 31L
  private val Conflict = AnalyticsError.LateFactReplayRequestConflict
  private final case class Coordinate(topic: String, partition: Int, offset: Long)
  private final case class Record(
      _id: String,
      lakehouseId: String,
      requestId: String,
      selectionDigest: String,
      coordinates: Option[Vector[Coordinate]],
      progress: String,
      publicationAttempt: Int,
      runId: String,
      rangeFingerprint: String,
      generation: Long,
      revision: Long,
      preparedAt: Instant,
      updatedAt: Instant,
      publishedAt: Option[Instant]
  )
  private val registry = AnalyticsMongoRecords
    .registry[Record](Set("offset", "generation", "revision"), Set("partition", "publicationAttempt"))
}
