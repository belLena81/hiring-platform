package com.example.hiring.analytics

import cats.effect.{Clock, IO, Resource}
import cats.syntax.all._
import com.mongodb.client.{MongoCursor, MongoDatabase}
import com.mongodb.client.model.{Filters, Sorts}
import fs2.Stream
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.bson.Document

import java.util.UUID
import java.util.Date
import java.time.Instant
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

/** Reads pending account-erasure requests before an analytics run can mutate Delta data. */
final class MongoActiveDeletionMarkerSource(
    database: MongoDatabase,
    pseudonymizer: SubjectPseudonymizer,
    private[analytics] val maximumPendingMarkers: Int = MongoActiveDeletionMarkerSource.MaximumPendingMarkers,
    clock: Clock[IO] = Clock[IO]
) extends ActiveDeletionMarkerSource {
  private val requestCollection = "analytics_erasure_requests"

  private def mongo[A](work: => A): IO[A] =
    IO.blocking(work).adaptError { case NonFatal(cause) => AnalyticsError.MarkerStorageFailure(cause) }

  private def activeRequests(now: Instant): Stream[IO, Document] =
    Stream
      .resource(
        Resource.make(
          mongo[MongoCursor[Document]](
            database
              .getCollection(requestCollection, classOf[Document])
              .find(
                Filters.or(
                  Filters.in("state", "Pending", "Processing"),
                  Filters.and(Filters.eq("state", "Complete"), Filters.gt("expiresAt", Date.from(now)))
                )
              )
              .sort(Sorts.ascending("_id"))
              .batchSize(256)
              .iterator()
          )
        )(cursor => mongo(cursor.close()))
      )
      .flatMap { cursor =>
        Stream.repeatEval(mongo(if (cursor.hasNext) Some(cursor.next()) else None)).unNoneTerminate
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
      requests <- activeRequests(now).take(maximumPendingMarkers.toLong + 1L).compile.toVector
      _ <- IO.raiseWhen(requests.size > maximumPendingMarkers)(
        AnalyticsError.MarkerLimitExceeded(maximumPendingMarkers)
      )
      tokens <- IO.fromEither(requests.traverse(MongoActiveDeletionMarkerSource.tokensFor(_, pseudonymizer)))
      distinctTokens = tokens.flatten.distinct
      frame <- IO
        .blocking(
          spark.createDataFrame(
            distinctTokens.map(token => Row(token.value)).asJava,
            StructType(Seq(StructField("subjectToken", StringType, nullable = false)))
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
    request.get("_id") match {
      case subjectId: String =>
        try {
          val parsed = UUID.fromString(subjectId)
          if (parsed.toString == subjectId)
            Right(pseudonymizer.matchingTokens(subjectId).map(SubjectToken.fromHmac))
          else Left(AnalyticsError.MalformedMarker)
        } catch {
          case _: IllegalArgumentException => Left(AnalyticsError.MalformedMarker)
        }
      case _ => Left(AnalyticsError.MalformedMarker)
    }
}
