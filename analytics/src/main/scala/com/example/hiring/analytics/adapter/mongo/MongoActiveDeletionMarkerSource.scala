package com.example.hiring.analytics.adapter.mongo

import com.example.hiring.analytics.domain.AccountSubjectId
import com.example.hiring.analytics.domain.SubjectPseudonymizer
import com.example.hiring.analytics.domain.SubjectToken
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.errors.AnalyticsErrorTranslation.translating
import com.example.hiring.analytics.service.erasure.ErasureRequestState
import com.example.hiring.analytics.service.batch.{ActiveDeletionMarkerSource, ActiveDeletionMarkers}

import cats.effect.{Async, Clock}
import cats.syntax.all.*
import mongo4cats.database.MongoDatabase
import mongo4cats.errors.MongoJsonParsingException
import com.mongodb.client.model.{Filters, Projections, Sorts}
import fs2.Stream
import org.bson.Document

import java.util.Date
import java.time.Instant
import scala.jdk.CollectionConverters.*

/** Reads pending account-erasure requests before an analytics run can mutate Delta data. */
private[analytics] final class MongoActiveDeletionMarkerSource[F[_]: Async](
    database: MongoDatabase[F],
    pseudonymizer: SubjectPseudonymizer,
    streams: MongoPublisherStream,
    private[analytics] val maximumPendingMarkers: Int = MongoActiveDeletionMarkerSource.MaximumPendingMarkers
) extends ActiveDeletionMarkerSource[F] {
  private val requestCollection = com.example.hiring.analytics.adapter.mongo.AnalyticsCollections.ErasureRequests

  private def activeRequests(now: Instant): Stream[F, AnalyticsMongoRecords.ErasureRequest] =
    Stream
      .eval(
        database.getCollection[AnalyticsMongoRecords.ErasureRequest](
          requestCollection,
          AnalyticsMongoRecords.erasureRequestRegistry
        )
      )
      .flatMap(collection =>
        streams.stream(
          collection.underlying
            .find(
              Filters.or(
                // Invalid states must reach the decoder, including arrays containing a valid state.
                Filters.nin(
                  AnalyticsCollections.Fields.State,
                  ErasureRequestState.values.map(_.persistedName).toList.asJava
                ),
                Filters.expr(
                  new Document(
                    "$ne",
                    List(new Document("$type", s"$$${AnalyticsCollections.Fields.State}"), "string").asJava
                  )
                ),
                Filters.in(
                  AnalyticsCollections.Fields.State,
                  ErasureRequestState.Pending.persistedName,
                  ErasureRequestState.Processing.persistedName
                ),
                Filters.and(
                  Filters.eq(AnalyticsCollections.Fields.State, ErasureRequestState.Complete.persistedName),
                  Filters.or(
                    Filters.gt(AnalyticsCollections.Fields.ExpiresAt, Date.from(now)),
                    Filters.expr(
                      new Document(
                        "$ne",
                        List(
                          new Document("$type", s"$$${AnalyticsCollections.Fields.ExpiresAt}"),
                          "date"
                        ).asJava
                      )
                    )
                  )
                )
              )
            )
            .projection(
              Projections.include(
                AnalyticsCollections.Fields.Id,
                AnalyticsCollections.Fields.State,
                AnalyticsCollections.Fields.ExpiresAt
              )
            )
            .sort(Sorts.ascending(AnalyticsCollections.Fields.Id))
            .batchSize(256)
        )
      )
      .handleErrorWith {
        case error: AnalyticsError        => Stream.raiseError[F](error)
        case _: MongoJsonParsingException => Stream.raiseError[F](AnalyticsError.MalformedMarker)
        case cause                        => Stream.raiseError[F](AnalyticsError.MarkerStorageFailure(cause))
      }

  override def activeDeletionMarkers: F[ActiveDeletionMarkers] =
    for {
      _ <- Async[F].raiseWhen(maximumPendingMarkers <= 0)(
        AnalyticsError.InvalidConfiguration("maximum pending marker count must be positive")
      )
      collectionNames <- database.listCollectionNames.translating(AnalyticsError.MarkerStorageFailure(_))
      collectionExists = collectionNames.exists(_ == requestCollection)
      _ <- Async[F].raiseUnless(collectionExists)(AnalyticsError.MissingMarkerCollection)
      now <- Clock[F].realTimeInstant
      markers <- activeRequests(now)
        .evalMap(request =>
          Async[F].fromEither(MongoActiveDeletionMarkerSource.activeMarker(request, now, pseudonymizer))
        )
        .unNone
        .take(maximumPendingMarkers.toLong + 1L)
        .compile
        .toVector
      _ <- Async[F].raiseWhen(markers.size > maximumPendingMarkers)(
        AnalyticsError.MarkerLimitExceeded(maximumPendingMarkers)
      )
    } yield ActiveDeletionMarkers(markers.flatMap(_.tokens).distinct, markers.exists(_.inProgress))
}

private[analytics] object MongoActiveDeletionMarkerSource {
  val MaximumPendingMarkers: Int = 100000

  def tokenFor(
      request: AnalyticsMongoRecords.ErasureRequest,
      pseudonymizer: SubjectPseudonymizer
  ): Either[AnalyticsError, SubjectToken] =
    tokensFor(request, pseudonymizer).map(_.head)

  private[analytics] def tokensFor(
      request: AnalyticsMongoRecords.ErasureRequest,
      pseudonymizer: SubjectPseudonymizer
  ): Either[AnalyticsError, Vector[SubjectToken]] = {
    val subjectId = request._id
    val validId = AccountSubjectId.from(subjectId).isRight
    Either.cond(validId, subjectId, AnalyticsError.MalformedMarker).flatMap { id =>
      pseudonymizer.matchingTokens(id).leftMap(AnalyticsError.InvalidConfiguration(_))
    }
  }

  /** The tokens an active request excludes and whether it still hides the report (it has not completed). */
  private[analytics] final case class ActiveMarker(tokens: Vector[SubjectToken], inProgress: Boolean)

  private[analytics] def activeMarker(
      request: AnalyticsMongoRecords.ErasureRequest,
      now: Instant,
      pseudonymizer: SubjectPseudonymizer
  ): Either[AnalyticsError, Option[ActiveMarker]] =
    request.state.flatMap(ErasureRequestState.fromString).toRight(AnalyticsError.MalformedMarker).flatMap {
      case ErasureRequestState.Pending | ErasureRequestState.Processing =>
        tokensFor(request, pseudonymizer).map(tokens => Some(ActiveMarker(tokens, inProgress = true)))
      case ErasureRequestState.Complete =>
        request.expiresAt
          .map(Date.from)
          .toRight(AnalyticsError.MalformedMarker)
          .flatMap(expiresAt =>
            if (expiresAt.after(Date.from(now)))
              tokensFor(request, pseudonymizer).map(tokens => Some(ActiveMarker(tokens, inProgress = false)))
            else Right(None)
          )
    }
}
