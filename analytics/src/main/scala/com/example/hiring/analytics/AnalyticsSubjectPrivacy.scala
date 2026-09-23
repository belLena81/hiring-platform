package com.example.hiring.analytics

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{coalesce, col, get_json_object, lit, trim, udf, when}

import java.nio.charset.StandardCharsets
import java.util.Base64
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

/** Deterministically maps an operational subject identifier to a versioned, opaque token.
  *
  * The HMAC key is owned by runtime configuration and is never written to a dataframe, Delta table, log, or manifest. A
  * new key needs a new token version and a rebuild.
  */
final class SubjectPseudonymizer private (secret: Array[Byte]) extends Serializable {
  private val key = secret.clone()

  def token(subjectId: String): String = {
    require(subjectId != null && subjectId.nonEmpty, "subject id must be non-empty")
    val mac = Mac.getInstance(SubjectPseudonymizer.Algorithm)
    mac.init(new SecretKeySpec(key, SubjectPseudonymizer.Algorithm))
    val digest = mac.doFinal(subjectId.getBytes(StandardCharsets.UTF_8))
    s"${SubjectPseudonymizer.TokenVersion}_${Base64.getUrlEncoder.withoutPadding().encodeToString(digest)}"
  }
}

object SubjectPseudonymizer {
  private val Algorithm = "HmacSHA256"
  private val TokenVersion = "hmac-v1"

  def fromSecret(secret: Array[Byte]): SubjectPseudonymizer = {
    require(secret != null && secret.nonEmpty, "HMAC secret must be non-empty")
    new SubjectPseudonymizer(secret)
  }

  def fromBase64(secret: String): SubjectPseudonymizer = {
    require(secret != null && secret.nonEmpty, "base64 HMAC secret must be non-empty")
    fromSecret(Base64.getDecoder.decode(secret))
  }
}

/** Spark-only privacy transforms. Raw identifiers exist only in the input frame before `silver`. */
object AnalyticsSubjectPrivacy {
  private val CandidateIdJsonPath = "$.payload.candidateId"
  private val SubjectTokenColumn = "subjectToken"

  /** Candidate identity takes precedence for application events. Events without a candidate use the authenticated actor
    * identifier, ensuring every valid operational event has one token.
    */
  def withSubjectToken(events: DataFrame, pseudonymizer: SubjectPseudonymizer): DataFrame = {
    val tokenize = udf((value: String) => pseudonymizer.token(value))
    val candidateId = trim(get_json_object(col("rawValue"), CandidateIdJsonPath))
    val subjectId = when(candidateId =!= lit(""), candidateId).otherwise(col("actorId"))
    events.withColumn(SubjectTokenColumn, tokenize(coalesce(subjectId, lit(""))))
  }

  /** Removes data for active erasure markers before it can be merged into Silver. Marker sources expose only the
    * already-HMACed token; this transform has no persistence or Mongo dependency.
    */
  def excludeActiveDeletionMarkers(events: DataFrame, activeMarkerTokens: DataFrame): DataFrame = {
    require(events.columns.contains(SubjectTokenColumn), "events must contain a subjectToken")
    require(activeMarkerTokens.columns.contains(SubjectTokenColumn), "markers must contain a subjectToken")
    events.join(
      activeMarkerTokens.select(col(SubjectTokenColumn)).filter(col(SubjectTokenColumn).isNotNull).distinct(),
      Seq(SubjectTokenColumn),
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
