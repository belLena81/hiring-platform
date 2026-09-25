package com.example.hiring.analytics

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
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.types.{ArrayType, StringType}

import java.nio.charset.StandardCharsets
import java.util.Base64
import scala.jdk.CollectionConverters.*
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

/** Deterministically maps an operational subject identifier to a versioned, opaque token.
  *
  * The HMAC key is owned by runtime configuration and is never written to a dataframe, Delta table, log, or manifest. A
  * new key needs a new token version and a rebuild.
  */
final class SubjectPseudonymizer private (keys: Vector[(String, Array[Byte])]) extends Serializable {
  private val copiedKeys = keys.map { case (version, key) => version -> key.clone() }
  val primaryKeyId: String = copiedKeys.head._1
  val keyIds: Set[String] = copiedKeys.iterator.map(_._1).toSet
  private[analytics] val keyVerifiers: Vector[(String, String)] = copiedKeys.map { key =>
    val token = tokenFor("hiring-analytics-key-continuity-v1", key)
    key._1 -> token.substring(key._1.length + 1)
  }

  def typedToken(subjectId: String): SubjectToken = {
    require(subjectId != null && subjectId.nonEmpty, "subject id must be non-empty")
    SubjectToken.fromHmac(tokenFor(subjectId, copiedKeys.head))
  }

  def token(subjectId: String): String = typedToken(subjectId).value

  /** Tokens written to new analytics rows use only the active primary key. */
  def tokenForNewRows(subjectId: String): String = token(subjectId)

  /** Tokens used to match existing rows include every configured primary/retiring key. */
  def matchingTokens(subjectId: String): Vector[String] = {
    require(subjectId != null && subjectId.nonEmpty, "subject id must be non-empty")
    copiedKeys.map(key => tokenFor(subjectId, key))
  }

  private def tokenFor(subjectId: String, key: (String, Array[Byte])) = {
    val mac = Mac.getInstance(SubjectPseudonymizer.Algorithm)
    mac.init(new SecretKeySpec(key._2, SubjectPseudonymizer.Algorithm))
    val digest = mac.doFinal(subjectId.getBytes(StandardCharsets.UTF_8))
    key._1 + "_" + Base64.getUrlEncoder.withoutPadding().encodeToString(digest)
  }
}

object SubjectPseudonymizer {
  private val Algorithm = "HmacSHA256"
  private val KeyIdPattern = "[A-Za-z0-9-]{1,40}".r
  private val MinimumKeyBytes = 32

  def fromSecret(secret: Array[Byte]): SubjectPseudonymizer =
    fromKeyRing("hmac-v1", secret, Vector.empty)

  def fromKeyRing(
      primaryKeyId: String,
      secret: Array[Byte],
      previousKeys: Vector[(String, Array[Byte])]
  ): SubjectPseudonymizer = {
    val allKeys = (primaryKeyId -> secret) +: previousKeys
    val ids = allKeys.map(_._1)
    if (
      allKeys.exists { case (id, key) =>
        id == null || !KeyIdPattern.matches(id) || key == null || key.length < MinimumKeyBytes
      } ||
      ids.distinct.size != ids.size
    )
      throw AnalyticsError.InvalidConfiguration(
        s"HMAC key IDs are invalid or key material is shorter than $MinimumKeyBytes bytes"
      )
    new SubjectPseudonymizer(allKeys)
  }

  def fromBase64(secret: String): SubjectPseudonymizer =
    fromBase64(secret, "hmac-v1", None, None)

  def fromBase64(
      secret: String,
      primaryKeyId: String,
      previousKeyId: Option[String],
      previousSecret: Option[String]
  ): SubjectPseudonymizer = {
    if (secret == null || secret.isEmpty)
      throw AnalyticsError.InvalidConfiguration("base64 HMAC secret must be non-empty")
    val previous = (previousKeyId, previousSecret) match {
      case (None, None)                                                       => Vector.empty
      case (Some(id), Some(value)) if id.trim.nonEmpty && value.trim.nonEmpty => Vector(id -> decode(value))
      case _ => throw AnalyticsError.InvalidConfiguration("previous HMAC key ID and secret must be configured together")
    }
    fromKeyRing(primaryKeyId, decode(secret), previous)
  }

  private def decode(value: String): Array[Byte] =
    try Base64.getDecoder.decode(value)
    catch {
      case _: IllegalArgumentException => throw AnalyticsError.InvalidConfiguration("base64 HMAC secret is malformed")
    }
}

/** Spark-only privacy transforms. Raw identifiers exist only in the input frame before `silver`. */
object AnalyticsSubjectPrivacy {
  private val SubjectTokenColumn = "subjectToken"

  /** Candidate identity takes precedence for application events. Events without a candidate use the authenticated actor
    * identifier, ensuring every valid operational event has one token.
    */
  def withSubjectToken(events: DataFrame, pseudonymizer: SubjectPseudonymizer): DataFrame = {
    val tokenize = udf(
      new UDF1[String, String] {
        override def call(value: String): String =
          Option(value).filter(_.trim.nonEmpty).map(pseudonymizer.token).orNull
      },
      StringType
    )
    val tokenizeAll = udf(
      new UDF1[String, java.util.List[String]] {
        override def call(value: String): java.util.List[String] =
          Option(value).filter(_.trim.nonEmpty).map(value => Vector(pseudonymizer.tokenForNewRows(value)).asJava).orNull
      },
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
  def excludeActiveDeletionMarkers(events: DataFrame, activeMarkerTokens: DataFrame): DataFrame = {
    if (!events.columns.contains(SubjectTokenColumn))
      throw AnalyticsError.InvalidSourceSchema(Vector(SubjectTokenColumn))
    if (!activeMarkerTokens.columns.contains(SubjectTokenColumn))
      throw AnalyticsError.InvalidSourceSchema(Vector(SubjectTokenColumn))
    val eventTokens = col("subjectTokens")
    val activeTokens = activeMarkerTokens
      .select(col(SubjectTokenColumn).as("activeSubjectToken"))
      .filter(col("activeSubjectToken").isNotNull)
      .distinct()
    events.join(
      activeTokens,
      size(array_intersect(eventTokens, array(col("activeSubjectToken")))) > lit(0),
      "left_anti"
    )
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
