package com.example.hiring.analytics

import cats.effect.{IO, Resource}
import cats.syntax.all._
import com.mongodb.MongoException
import com.mongodb.client.{MongoCursor, MongoDatabase}
import com.mongodb.client.model.{Filters, Sorts}
import fs2.Stream
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.bson.Document

import java.util.UUID
import scala.jdk.CollectionConverters._

/** Reads pending account-erasure requests before an analytics run can mutate Delta data. */
final class MongoActiveDeletionMarkerSource(
    database: MongoDatabase,
    pseudonymizer: SubjectPseudonymizer,
    private[analytics] val maximumPendingMarkers: Int = MongoActiveDeletionMarkerSource.MaximumPendingMarkers
) extends ActiveDeletionMarkerSource {
  require(maximumPendingMarkers > 0, "maximum pending marker count must be positive")

  private val requestCollection = "analytics_erasure_requests"

  private def pendingRequests: Stream[IO, Document] =
    Stream.resource(
      Resource.make(
        IO.blocking[MongoCursor[Document]](
          database
            .getCollection(requestCollection, classOf[Document])
            .find(Filters.eq("state", "Pending"))
            .sort(Sorts.ascending("_id"))
            .batchSize(256)
            .iterator()
        )
      )(cursor => IO.blocking(cursor.close()))
    ).flatMap { cursor =>
      Stream.repeatEval(IO.blocking(if (cursor.hasNext) Some(cursor.next()) else None)).unNoneTerminate
    }

  override def activeSubjectTokens(spark: SparkSession): IO[DataFrame] =
    (for {
      collectionExists <- IO.blocking(
        database.listCollectionNames().filter(Filters.eq("name", requestCollection)).first() != null
      )
      _ <- IO.raiseUnless(collectionExists)(AnalyticsError.MissingMarkerCollection)
      requests <- pendingRequests.take(maximumPendingMarkers.toLong + 1L).compile.toVector
      _ <- IO.raiseWhen(requests.size > maximumPendingMarkers)(
        AnalyticsError.MarkerLimitExceeded(maximumPendingMarkers)
      )
      tokens <- IO.fromEither(requests.traverse(MongoActiveDeletionMarkerSource.tokenFor(_, pseudonymizer)))
      frame <- IO.blocking(spark.createDataFrame(
        tokens.map(token => Row(token.value)).asJava,
        StructType(Seq(StructField("subjectToken", StringType, nullable = false)))
      ))
    } yield frame).adaptError { case cause: MongoException => AnalyticsError.MarkerStorageFailure(cause) }
}

private[analytics] object MongoActiveDeletionMarkerSource {
  val MaximumPendingMarkers: Int = 100000

  def tokenFor(request: Document, pseudonymizer: SubjectPseudonymizer): Either[AnalyticsError, SubjectToken] =
    request.get("_id") match {
      case subjectId: String =>
        try {
          val parsed = UUID.fromString(subjectId)
          if (parsed.toString == subjectId) Right(SubjectToken.fromHmac(pseudonymizer.token(subjectId)))
          else Left(AnalyticsError.MalformedMarker)
        } catch {
          case _: IllegalArgumentException => Left(AnalyticsError.MalformedMarker)
        }
      case _ => Left(AnalyticsError.MalformedMarker)
    }
}
