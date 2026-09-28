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
import cats.effect.{Async, Clock}
import cats.syntax.all.*
import com.mongodb.reactivestreams.client.MongoDatabase
import com.mongodb.client.model.{Filters, Sorts}
import fs2.Stream
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.bson.Document

import java.util.UUID
import java.util.Date
import java.time.Instant
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** Reads pending account-erasure requests before an analytics run can mutate Delta data. */
private[analytics] final class MongoActiveDeletionMarkerSource[F[_]: Async: Clock](
    database: MongoDatabase,
    pseudonymizer: SubjectPseudonymizer,
    streams: MongoPublisherStream,
    sparkExecution: SparkBlockingExecution[F] =
      SparkBlockingExecution.forTests[F](scala.concurrent.ExecutionContext.parasitic),
    private[analytics] val maximumPendingMarkers: Int = MongoActiveDeletionMarkerSource.MaximumPendingMarkers
) extends ActiveDeletionMarkerSource[F] {
  private val requestCollection = com.example.hiring.analytics.adapter.mongo.AnalyticsCollections.ErasureRequests

  private def activeRequests(now: Instant): Stream[F, Document] =
    streams
      .stream[F, Document](
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
          // Mongo's server-cursor batch hint is separate from the FS2 demand buffer configured on streams.
          .batchSize(256)
      )
      .handleErrorWith {
        case error: AnalyticsError => Stream.raiseError[F](error)
        case cause                 => Stream.raiseError[F](AnalyticsError.MarkerStorageFailure(cause))
      }

  override def activeSubjectTokens(spark: SparkSession): F[DataFrame] =
    for {
      _ <- Async[F].raiseWhen(maximumPendingMarkers <= 0)(
        AnalyticsError.InvalidConfiguration("maximum pending marker count must be positive")
      )
      collectionExists <- streams
        .optional(database.listCollections().filter(Filters.eq("name", requestCollection)).first())
        .map(_.isDefined)
        .adaptError {
          case error: AnalyticsError => error
          case NonFatal(cause)       => AnalyticsError.MarkerStorageFailure(cause)
        }
      _ <- Async[F].raiseUnless(collectionExists)(AnalyticsError.MissingMarkerCollection)
      now <- Clock[F].realTimeInstant
      tokens <- activeRequests(now)
        .evalMap(request =>
          Async[F].fromEither(MongoActiveDeletionMarkerSource.activeTokens(request, now, pseudonymizer))
        )
        .unNone
        .take(maximumPendingMarkers.toLong + 1L)
        .compile
        .toVector
      _ <- Async[F].raiseWhen(tokens.size > maximumPendingMarkers)(
        AnalyticsError.MarkerLimitExceeded(maximumPendingMarkers)
      )
      distinctTokens = tokens.flatten.distinct
      frame <- sparkExecution(
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
        .adaptError {
          case error: AnalyticsError => error
          case NonFatal(cause)       => AnalyticsError.LakehouseFailure(cause)
        }
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
