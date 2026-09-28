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

import com.example.hiring.analytics.*
import com.example.hiring.analytics.adapter.mongo.{AnalyticsCollections, BsonDecoder, BsonValueDecoder}
import com.example.hiring.analytics.adapter.spark.ActiveDeletionMarkerSource

import cats.effect.{Clock, IO}
import cats.syntax.all.*
import com.mongodb.reactivestreams.client.MongoDatabase
import com.mongodb.client.model.{Filters, Sorts}
import fs2.Stream
import com.example.hiring.analytics.adapter.mongo.MongoCursorStream
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.bson.Document

import java.util.UUID
import java.util.Date
import java.time.Instant
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** Reads pending account-erasure requests before an analytics run can mutate Delta data. */
final class MongoActiveDeletionMarkerSource(
    database: MongoDatabase,
    pseudonymizer: SubjectPseudonymizer,
    private[analytics] val maximumPendingMarkers: Int = MongoActiveDeletionMarkerSource.MaximumPendingMarkers,
    clock: Clock[IO] = Clock[IO]
) extends ActiveDeletionMarkerSource {
  private val requestCollection = com.example.hiring.analytics.adapter.mongo.AnalyticsCollections.ErasureRequests

  private def activeRequests(now: Instant): Stream[IO, Document] =
    MongoCursorStream(
      database
        .getCollection(requestCollection, classOf[Document])
        .find(
          Filters.or(
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
        .sort(Sorts.ascending(AnalyticsCollections.Fields.Id))
        .batchSize(256)
    ).handleErrorWith {
      case error: AnalyticsError => Stream.raiseError[IO](error)
      case cause                 => Stream.raiseError[IO](AnalyticsError.MarkerStorageFailure(cause))
    }

  override def activeSubjectTokens(spark: SparkSession): IO[DataFrame] =
    for {
      _ <- IO.raiseWhen(maximumPendingMarkers <= 0)(
        AnalyticsError.InvalidConfiguration("maximum pending marker count must be positive")
      )
      collectionExists <- com.example.hiring.analytics.adapter.mongo.MongoPublisherStream
        .optional(database.listCollections().filter(Filters.eq("name", requestCollection)).first())
        .map(_.isDefined)
        .adaptError { case NonFatal(cause) => AnalyticsError.MarkerStorageFailure(cause) }
      _ <- IO.raiseUnless(collectionExists)(AnalyticsError.MissingMarkerCollection)
      now <- clock.realTimeInstant
      tokens <- activeRequests(now)
        .evalMap(request => IO.fromEither(MongoActiveDeletionMarkerSource.activeTokens(request, now, pseudonymizer)))
        .unNone
        .take(maximumPendingMarkers.toLong + 1L)
        .compile
        .toVector
      _ <- IO.raiseWhen(tokens.size > maximumPendingMarkers)(
        AnalyticsError.MarkerLimitExceeded(maximumPendingMarkers)
      )
      distinctTokens = tokens.flatten.distinct
      frame <- IO
        .blocking(
          spark.createDataFrame(
            distinctTokens.map(token => Row(token.value)).asJava,
            StructType(
              Seq(
                StructField(
                  com.example.hiring.analytics.adapter.mongo.AnalyticsCollections.Fields.SubjectToken,
                  StringType,
                  nullable = false
                )
              )
            )
          )
        )
        .adaptError { case NonFatal(cause) => AnalyticsError.LakehouseFailure(cause) }
    } yield frame
}

private[analytics] object MongoActiveDeletionMarkerSource {
  val MaximumPendingMarkers: Int = 100000

  def tokenFor(request: Document, pseudonymizer: SubjectPseudonymizer): Either[AnalyticsError, SubjectToken] =
    tokensFor(request, pseudonymizer).map(_.head)

  def tokensFor(
      request: Document,
      pseudonymizer: SubjectPseudonymizer
  ): Either[AnalyticsError, Vector[SubjectToken]] = {
    import BsonValueDecoder.given
    BsonDecoder
      .required[String](request, AnalyticsCollections.Fields.Id, AnalyticsError.MalformedMarker)
      .flatMap { subjectId =>
        val validId = scala.util.Try(UUID.fromString(subjectId)).toOption.exists(_.toString == subjectId)
        Either.cond(validId, subjectId, AnalyticsError.MalformedMarker).flatMap { id =>
          pseudonymizer
            .matchingTokens(id)
            .flatMap(_.traverse(SubjectToken.fromHmac))
            .leftMap(AnalyticsError.InvalidConfiguration.apply)
        }
      }
  }

  private[analytics] def activeTokens(
      request: Document,
      now: Instant,
      pseudonymizer: SubjectPseudonymizer
  ): Either[AnalyticsError, Option[Vector[SubjectToken]]] = {
    import BsonValueDecoder.given
    BsonDecoder
      .required[ErasureRequestState](
        request,
        AnalyticsCollections.Fields.State,
        AnalyticsError.MalformedMarker
      )
      .flatMap {
        case ErasureRequestState.Pending | ErasureRequestState.Processing =>
          tokensFor(request, pseudonymizer).map(Some(_))
        case ErasureRequestState.Complete =>
          BsonDecoder
            .required[Date](request, AnalyticsCollections.Fields.ExpiresAt, AnalyticsError.MalformedMarker)
            .flatMap(expiresAt =>
              if (expiresAt.after(Date.from(now))) tokensFor(request, pseudonymizer).map(Some(_))
              else Right(None)
            )
      }
  }
}
