package com.example.hiring.analytics.erasure

import com.example.hiring.analytics.*
import com.example.hiring.analytics.mongo.AnalyticsCollections
import com.example.hiring.analytics.batch.ActiveDeletionMarkerSource

import cats.effect.{Clock, IO}
import cats.syntax.all.*
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.{Filters, Sorts}
import fs2.Stream
import com.example.hiring.analytics.mongo.MongoCursorStream
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
  private val requestCollection = com.example.hiring.analytics.mongo.AnalyticsCollections.ErasureRequests

  private def mongo[A](work: => A): IO[A] =
    IO.blocking(work).adaptError { case NonFatal(cause) => AnalyticsError.MarkerStorageFailure(cause) }

  private def activeRequests(now: Instant): Stream[IO, Document] =
    MongoCursorStream(
      database
        .getCollection(requestCollection, classOf[Document])
        .find(
          Filters.or(
            Filters.in(AnalyticsCollections.Fields.State, "Pending", "Processing"),
            Filters.and(
              Filters.eq(AnalyticsCollections.Fields.State, "Complete"),
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
        .iterator()
    ).handleErrorWith {
      case error: AnalyticsError => Stream.raiseError[IO](error)
      case cause                 => Stream.raiseError[IO](AnalyticsError.MarkerStorageFailure(cause))
    }

  override def activeSubjectTokens(spark: SparkSession): IO[DataFrame] =
    for {
      _ <- IO.raiseWhen(maximumPendingMarkers <= 0)(
        AnalyticsError.InvalidConfiguration("maximum pending marker count must be positive")
      )
      collectionExists <- mongo(
        database.listCollectionNames().filter(Filters.eq("name", requestCollection)).first() != null
      )
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
                  com.example.hiring.analytics.mongo.AnalyticsCollections.Fields.SubjectToken,
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

  def tokensFor(request: Document, pseudonymizer: SubjectPseudonymizer): Either[AnalyticsError, Vector[SubjectToken]] =
    request.get(AnalyticsCollections.Fields.Id) match {
      case subjectId: String =>
        try {
          val parsed = UUID.fromString(subjectId)
          if (parsed.toString == subjectId)
            pseudonymizer
              .matchingTokens(subjectId)
              .traverse(SubjectToken.fromHmac)
              .leftMap(AnalyticsError.InvalidConfiguration.apply)
          else Left(AnalyticsError.MalformedMarker)
        } catch {
          case _: IllegalArgumentException => Left(AnalyticsError.MalformedMarker)
        }
      case _ => Left(AnalyticsError.MalformedMarker)
    }

  private[analytics] def activeTokens(
      request: Document,
      now: Instant,
      pseudonymizer: SubjectPseudonymizer
  ): Either[AnalyticsError, Option[Vector[SubjectToken]]] =
    request.get(AnalyticsCollections.Fields.State) match {
      case "Pending" | "Processing" => tokensFor(request, pseudonymizer).map(Some(_))
      case "Complete"               =>
        request.get(AnalyticsCollections.Fields.ExpiresAt) match {
          case expiresAt: Date if expiresAt.after(Date.from(now)) => tokensFor(request, pseudonymizer).map(Some(_))
          case _: Date                                            => Right(None)
          case _                                                  => Left(AnalyticsError.MalformedMarker)
        }
      case _ => Left(AnalyticsError.MalformedMarker)
    }
}
