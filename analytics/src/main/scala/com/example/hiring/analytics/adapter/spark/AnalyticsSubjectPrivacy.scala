/** Spark-only privacy transforms. Raw identifiers exist only in the input frame before `silver`. */
package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.domain.AnalyticsEventType
import com.example.hiring.analytics.domain.SubjectPseudonymizer
import com.example.hiring.analytics.errors.AnalyticsError

import org.apache.spark.sql.{Column, DataFrame, Row}
import java.util.Locale
import org.apache.spark.sql.api.java.UDF3
import org.apache.spark.sql.functions.{col, explode, udf}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

/** Deterministically maps an operational subject identifier to a versioned, opaque token.
  *
  * The HMAC key is owned by runtime configuration and is never written to a dataframe, Delta table, log, or manifest. A
  * new key needs a new token version and a rebuild.
  */
object AnalyticsSubjectPrivacy {
  private val SubjectTokenColumn = Columns.SubjectToken
  private val SubjectTokensColumn = Columns.SubjectTokens
  private val TokenResult = "_hiringSubjectTokenResult"
  private val ExcludedPrefix = "_hiringExcluded_"
  private val EventMarkerToken = "_hiringEventSubjectToken"
  private val ActiveMarkerToken = "activeSubjectToken"

  private val tokenResultType: StructType = StructType(
    Seq(
      StructField(SubjectTokenColumn, StringType, nullable = true),
      StructField(SubjectTokensColumn, ArrayType(StringType, containsNull = false), nullable = false)
    )
  )

  /** The deterministic tokenizing function. Its only captured state is the tokenizer constructor (for production, the
    * primary-key token factory), and the tokenizer itself is created lazily once per deserialized copy, which is one
    * per Spark task. Rows in a task are processed by one thread, and the accessor is synchronized for any other caller.
    */
  private[analytics] final class SubjectTokenFunction(newTokenizer: () => String => String)
      extends UDF3[String, String, Row, Row] {
    @transient private var tokenizer: String => String = null

    private def tokenize(subjectId: String): String = {
      // The HMAC Mac inside the tokenizer is not thread-safe, so creation and use share one lock.
      synchronized {
        if (tokenizer == null) tokenizer = newTokenizer()
        tokenizer(subjectId)
      }
    }

    override def call(actor: String, eventType: String, payload: Row): Row = {
      val payloadRow = Option(payload)
      val candidateId =
        payloadRow
          .flatMap(value => Option(value.getAs[String]("candidateId")))
          .fold("")(_.trim.toLowerCase(Locale.ROOT))
      val actorId = Option(actor).map(_.toLowerCase(Locale.ROOT))
      val searchKind = payloadRow.flatMap(value => Option(value.getAs[String]("searchKind")))
      val candidateSearch = searchKind.contains("candidateMatches")

      val actorToken = actorId.filter(_.trim.nonEmpty).map(tokenize)
      val candidateToken = Option.when(candidateId.nonEmpty)(tokenize(candidateId))
      val searchResultIds =
        if (!candidateSearch) Vector.empty
        else
          Option(eventType) match {
            case Some(value) if value == AnalyticsEventType.SearchPerformed.wire =>
              payloadRow
                .flatMap(result => Option(result.getAs[scala.collection.Seq[Row]]("results")))
                .toVector
                .flatten
                .flatMap(result => Option(result.getAs[String]("resultId")))
            case Some(value) if value == AnalyticsEventType.SearchResultClicked.wire =>
              payloadRow.flatMap(result => Option(result.getAs[String]("resultId"))).toVector
            case _ => Vector.empty
          }
      val tokens = (actorToken.toVector ++ candidateToken.toVector ++ searchResultIds
        .filter(_.trim.nonEmpty)
        .map(_.toLowerCase(Locale.ROOT))
        .map(tokenize)).distinct
      Row(candidateToken.orElse(actorToken).orNull, tokens)
    }
  }

  /** Candidate identity takes precedence for application events. Events without a candidate use the authenticated actor
    * identifier, ensuring every valid operational event has one token.
    */
  def withSubjectToken(events: DataFrame, pseudonymizer: SubjectPseudonymizer): DataFrame =
    attachTokens(events.drop(SubjectTokenColumn, SubjectTokensColumn), tokenizerFor(pseudonymizer))

  /** Adds subject tokens only when the frame does not carry both columns yet, so a frame tokenized once is never
    * tokenized again. Contract: existing `subjectToken`/`subjectTokens` columns are trusted as they are, so they must
    * come from this pipeline's own tokenization (`analyzed` or `withSubjectToken`), never from external input.
    */
  private[analytics] def ensureSubjectTokens(events: DataFrame, pseudonymizer: SubjectPseudonymizer): DataFrame =
    if (events.columns.contains(SubjectTokenColumn) && events.columns.contains(SubjectTokensColumn)) events
    else withSubjectToken(events, pseudonymizer)

  /** Tokenizes with an explicit tokenizer constructor; tests use it to count tokenizer work. */
  private[analytics] def withSubjectToken(events: DataFrame, newTokenizer: () => String => String): DataFrame =
    attachTokens(events.drop(SubjectTokenColumn, SubjectTokensColumn), newTokenizer)

  private def tokenizerFor(pseudonymizer: SubjectPseudonymizer): () => String => String = {
    val tokenFactory = pseudonymizer.primaryTokenFactory
    () => {
      val tokenizer = tokenFactory.partitionTokenizer()
      subjectId => tokenizer.primaryToken(subjectId).value
    }
  }

  private def attachTokens(source: DataFrame, newTokenizer: () => String => String): DataFrame = {
    val tokenize = udf(new SubjectTokenFunction(newTokenizer), tokenResultType).asNonNullable()
    val existing = source.columns.map(name => col(s"`$name`"))
    source
      .select(existing :+ tokenize(col("actorId"), col("eventType"), col("payload")).as(TokenResult)*)
      .select(
        existing ++ Seq(
          col(s"$TokenResult.$SubjectTokenColumn").as(SubjectTokenColumn),
          col(s"$TokenResult.$SubjectTokensColumn").as(SubjectTokensColumn)
        )*
      )
  }

  /** The columns that identify one input row for marker exclusion: the Kafka coordinate when present, else the event
    * ID.
    */
  private def rowKey(events: DataFrame): Either[AnalyticsError, Vector[String]] = {
    val coordinate = Vector(Columns.Topic, Columns.Partition, Columns.Offset)
    if (coordinate.forall(events.columns.contains)) Right(coordinate)
    else if (events.columns.contains(Columns.EventId)) Right(Vector(Columns.EventId))
    else Left(AnalyticsError.InvalidSourceSchema(coordinate.filterNot(events.columns.contains)))
  }

  /** Removes data for active erasure markers before it can be merged into Silver. Marker sources expose only the
    * already-HMACed token; this transform has no persistence or Mongo dependency.
    *
    * A row is excluded when any of its `subjectTokens` equals an active marker. This is two equality joins: the row's
    * tokens are exploded and semi-joined with the distinct markers to find the excluded row keys, and the rows with
    * those keys are anti-joined away. Neither join needs a nested-loop or Cartesian plan.
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
    else
      rowKey(events).map { keys =>
        val activeTokens = activeMarkerTokens
          .select(col(SubjectTokenColumn).as(ActiveMarkerToken))
          .filter(col(ActiveMarkerToken).isNotNull)
          .distinct()
        val excludedKeys = events
          .select(keys.map(key => col(s"`$key`")) :+ explode(col(SubjectTokensColumn)).as(EventMarkerToken)*)
          .join(activeTokens, col(EventMarkerToken) === col(ActiveMarkerToken), "left_semi")
          .select(keys.map(key => col(s"`$key`").as(ExcludedPrefix + key))*)
          .distinct()
        val sameRow: Column = keys.map(key => col(s"`$key`") <=> col(ExcludedPrefix + key)).reduce(_ && _)
        events.join(excludedKeys, sameRow, "left_anti")
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
