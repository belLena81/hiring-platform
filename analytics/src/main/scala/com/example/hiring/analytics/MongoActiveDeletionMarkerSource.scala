package com.example.hiring.analytics

import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.{Filters, Sorts}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.bson.Document

import java.util.UUID

/** Reads pending account-erasure requests before an analytics run can mutate Delta data. */
final class MongoActiveDeletionMarkerSource(
    database: MongoDatabase,
    pseudonymizer: SubjectPseudonymizer,
    private[analytics] val maximumPendingMarkers: Int = MongoActiveDeletionMarkerSource.MaximumPendingMarkers
) extends ActiveDeletionMarkerSource {
  require(maximumPendingMarkers > 0, "maximum pending marker count must be positive")

  override def activeSubjectTokens(spark: SparkSession): DataFrame = {
    val requestCollection = "analytics_erasure_requests"
    val collectionExists = database.listCollectionNames().filter(Filters.eq("name", requestCollection)).first() != null
    if (!collectionExists)
      throw new IllegalStateException("analytics erasure request collection is unavailable")

    val cursor = database
      .getCollection(requestCollection, classOf[Document])
      .find(Filters.eq("state", "Pending"))
      .sort(Sorts.ascending("_id"))
      .batchSize(256)
      .iterator()

    val tokens = Vector.newBuilder[String]
    var markerCount = 0
    try
      while (cursor.hasNext) {
        markerCount += 1
        if (markerCount > maximumPendingMarkers)
          throw new IllegalStateException("pending analytics erasure marker limit exceeded")
        tokens += MongoActiveDeletionMarkerSource.tokenFor(cursor.next(), pseudonymizer)
      }
    finally cursor.close()

    import spark.implicits._
    tokens.result().toDF("subjectToken")
  }
}

private[analytics] object MongoActiveDeletionMarkerSource {
  val MaximumPendingMarkers: Int = 100000

  def tokenFor(request: Document, pseudonymizer: SubjectPseudonymizer): String =
    request.get("_id") match {
      case subjectId: String =>
        try {
          val parsed = UUID.fromString(subjectId)
          require(parsed.toString == subjectId, "subject id must use canonical UUID form")
          pseudonymizer.token(parsed.toString)
        } catch {
          case _: IllegalArgumentException =>
            throw new IllegalStateException("pending analytics erasure request has an invalid subject id")
        }
      case _ => throw new IllegalStateException("pending analytics erasure request has no string subject id")
    }
}
