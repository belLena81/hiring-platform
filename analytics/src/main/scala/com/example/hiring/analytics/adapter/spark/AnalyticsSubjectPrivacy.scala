package com.example.hiring.analytics.adapter.spark
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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{
  array,
  array_distinct,
  array_intersect,
  col,
  concat,
  filter as arrayFilter,
  flatten,
  lit,
  size,
  trim,
  udf,
  when,
  transform
}
import org.apache.spark.sql.types.{ArrayType, StringType}

/** Deterministically maps an operational subject identifier to a versioned, opaque token.
  *
  * The HMAC key is owned by runtime configuration and is never written to a dataframe, Delta table, log, or manifest. A
  * new key needs a new token version and a rebuild.
  */
object AnalyticsSubjectPrivacy {
  private val SubjectTokenColumn = "subjectToken"

  /** Candidate identity takes precedence for application events. Events without a candidate use the authenticated actor
    * identifier, ensuring every valid operational event has one token.
    */
  def withSubjectToken(events: DataFrame, pseudonymizer: SubjectPseudonymizer): DataFrame = {
    val tokenize = udf(
      (value: String) => Option(value).filter(_.trim.nonEmpty).map(pseudonymizer.token),
      StringType
    )
    val tokenizeAll = udf(
      (value: String) =>
        Option(value)
          .filter(_.trim.nonEmpty)
          .fold(Vector.empty[String])(value => Vector(pseudonymizer.tokenForNewRows(value))),
      ArrayType(StringType, containsNull = false)
    )
    val candidateId = trim(col("payload.candidateId"))
    val actorToken = tokenize(col("actorId"))
    val candidateToken = tokenize(candidateId)
    val candidateSearch = col("payload.searchKind") === lit("candidateMatches")
    val emptyTokens = array().cast(ArrayType(StringType, containsNull = true))
    val candidateResultTokens = when(
      col("eventType") === lit(AnalyticsEventType.SearchPerformed.wire) && candidateSearch,
      transform(col("payload.results"), result => tokenizeAll(result.getField("resultId")))
    ).when(
      col("eventType") === lit(AnalyticsEventType.SearchResultClicked.wire) && candidateSearch,
      array(tokenizeAll(col("payload.resultId")))
    ).otherwise(array().cast(ArrayType(ArrayType(StringType, containsNull = false), containsNull = true)))
    val candidateIdentityTokens = when(candidateId =!= lit(""), tokenizeAll(candidateId))
      .otherwise(array().cast(ArrayType(StringType, containsNull = false)))
    val tokenArrays = concat(array(tokenizeAll(col("actorId")), candidateIdentityTokens), candidateResultTokens)
    val allTokens = array_distinct(arrayFilter(flatten(tokenArrays), token => token.isNotNull))
    val subject = when(candidateId =!= lit(""), candidateToken).otherwise(actorToken)
    events
      .withColumn(SubjectTokenColumn, subject)
      .withColumn("subjectTokens", allTokens)
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
      Option.when(!events.columns.contains("subjectTokens"))("subjectTokens"),
      Option.when(!activeMarkerTokens.columns.contains(SubjectTokenColumn))(SubjectTokenColumn)
    ).flatten
    if (missing.nonEmpty) Left(AnalyticsError.InvalidSourceSchema(missing.distinct))
    else {
      val eventTokens = col("subjectTokens")
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
      events.sparkSession.sparkContext.emptyRDD[org.apache.spark.sql.Row],
      org.apache.spark.sql.types.StructType(
        Seq(
          org.apache.spark.sql.types
            .StructField(SubjectTokenColumn, org.apache.spark.sql.types.StringType, nullable = false)
        )
      )
    )
}
