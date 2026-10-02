/** Spark-only privacy transforms. Raw identifiers exist only in the input frame before `silver`. */
package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.domain.AnalyticsEventType
import com.example.hiring.analytics.domain.SubjectPseudonymizer
import com.example.hiring.analytics.errors.AnalyticsError

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{array, array_intersect, col, lit, size}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField}

/** Deterministically maps an operational subject identifier to a versioned, opaque token.
  *
  * The HMAC key is owned by runtime configuration and is never written to a dataframe, Delta table, log, or manifest. A
  * new key needs a new token version and a rebuild.
  */
object AnalyticsSubjectPrivacy {
  private val SubjectTokenColumn = "subjectToken"
  private val SubjectTokensColumn = "subjectTokens"

  /** Candidate identity takes precedence for application events. Events without a candidate use the authenticated actor
    * identifier, ensuring every valid operational event has one token.
    */
  def withSubjectToken(events: DataFrame, pseudonymizer: SubjectPseudonymizer): DataFrame = {
    val source = events.drop(SubjectTokenColumn, SubjectTokensColumn)
    val schema = source.schema
      .add(StructField(SubjectTokenColumn, StringType, nullable = true))
      .add(StructField(SubjectTokensColumn, ArrayType(StringType, containsNull = false), nullable = false))
    val actorIndex = source.schema.fieldIndex("actorId")
    val eventTypeIndex = source.schema.fieldIndex("eventType")
    val payloadIndex = source.schema.fieldIndex("payload")
    val tokenFactory = pseudonymizer.primaryTokenFactory

    val rows = source.rdd.mapPartitions { partition =>
      val tokenizer = tokenFactory.partitionTokenizer()
      partition.map { row =>
        val payload = Option(row.getAs[Row](payloadIndex))
        val candidateId = payload.flatMap(value => Option(value.getAs[String]("candidateId"))).fold("")(_.trim)
        val actorId = Option(row.getAs[String](actorIndex))
        val eventType = Option(row.getAs[String](eventTypeIndex))
        val searchKind = payload.flatMap(value => Option(value.getAs[String]("searchKind")))
        val candidateSearch = searchKind.contains("candidateMatches")

        val actorToken = actorId.filter(_.trim.nonEmpty).map(tokenizer.primaryToken)
        val candidateToken = Option.when(candidateId.nonEmpty)(tokenizer.primaryToken(candidateId))
        val searchResultIds =
          if (!candidateSearch) Vector.empty
          else
            eventType match {
              case Some(value) if value == AnalyticsEventType.SearchPerformed.wire =>
                payload
                  .flatMap(result => Option(result.getAs[scala.collection.Seq[Row]]("results")))
                  .toVector
                  .flatten
                  .flatMap(result => Option(result.getAs[String]("resultId")))
              case Some(value) if value == AnalyticsEventType.SearchResultClicked.wire =>
                payload.flatMap(result => Option(result.getAs[String]("resultId"))).toVector
              case _ => Vector.empty
            }
        val tokens = (actorToken.toVector ++ candidateToken.toVector ++ searchResultIds
          .filter(_.trim.nonEmpty)
          .map(tokenizer.primaryToken)).distinct.map(_.value)
        val subjectToken = candidateToken.orElse(actorToken).map(_.value).orNull
        Row.fromSeq(row.toSeq ++ Seq(subjectToken, tokens))
      }
    }
    source.sparkSession.createDataFrame(rows, schema)
  }

  /** Removes data for active erasure markers before it can be merged into Silver. Marker sources expose only the
    * already-HMACed token; this transform has no persistence or Mongo dependency.
    */
  def excludeActiveDeletionMarkers(
      events: DataFrame,
      activeMarkerTokens: DataFrame
  ): Either[AnalyticsError, DataFrame] = {
    val missing = Vector(
      Option.when(!events.columns.contains(SubjectTokenColumn))(SubjectTokenColumn),
      Option.when(!events.columns.contains(SubjectTokensColumn))(SubjectTokensColumn),
      Option.when(!activeMarkerTokens.columns.contains(SubjectTokenColumn))(SubjectTokenColumn)
    ).flatten
    if (missing.nonEmpty) Left(AnalyticsError.InvalidSourceSchema(missing.distinct))
    else {
      val eventTokens = col(SubjectTokensColumn)
      val activeTokens = activeMarkerTokens
        .select(col(SubjectTokenColumn).as("activeSubjectToken"))
        .filter(col("activeSubjectToken").isNotNull)
        .distinct()
      Right(
        events.join(
          activeTokens,
          size(array_intersect(eventTokens, array(col("activeSubjectToken")))) > lit(0),
          "left_anti"
        )
      )
    }
  }

  def emptyMarkers(events: DataFrame): DataFrame =
    events.sparkSession.createDataFrame(
      events.sparkSession.sparkContext.emptyRDD[Row],
      org.apache.spark.sql.types.StructType(
        Seq(StructField(SubjectTokenColumn, StringType, nullable = false))
      )
    )
}
