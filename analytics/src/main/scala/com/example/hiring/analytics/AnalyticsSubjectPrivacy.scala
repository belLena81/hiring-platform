package com.example.hiring.analytics

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{coalesce, col, lit, trim, udf, when}
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.types.StringType

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

  def typedToken(subjectId: String): SubjectToken = {
    require(subjectId != null && subjectId.nonEmpty, "subject id must be non-empty")
    val mac = Mac.getInstance(SubjectPseudonymizer.Algorithm)
    mac.init(new SecretKeySpec(key, SubjectPseudonymizer.Algorithm))
    val digest = mac.doFinal(subjectId.getBytes(StandardCharsets.UTF_8))
    SubjectToken.fromHmac(
      s"${SubjectPseudonymizer.TokenVersion}_${Base64.getUrlEncoder.withoutPadding().encodeToString(digest)}"
    )
  }

  def token(subjectId: String): String = typedToken(subjectId).value
}

object SubjectPseudonymizer {
  private val Algorithm = "HmacSHA256"
  private val TokenVersion = "hmac-v1"

  def fromSecret(secret: Array[Byte]): SubjectPseudonymizer = {
    if (secret == null || secret.isEmpty)
      throw AnalyticsError.InvalidConfiguration("HMAC secret must be non-empty")
    new SubjectPseudonymizer(secret)
  }

  def fromBase64(secret: String): SubjectPseudonymizer = {
    if (secret == null || secret.isEmpty)
      throw AnalyticsError.InvalidConfiguration("base64 HMAC secret must be non-empty")
    val decoded = try Base64.getDecoder.decode(secret)
    catch {
      case _: IllegalArgumentException =>
        throw AnalyticsError.InvalidConfiguration("base64 HMAC secret is malformed")
    }
    fromSecret(decoded)
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
        override def call(value: String): String = pseudonymizer.typedToken(value).value
      },
      StringType
    )
    val candidateId = trim(col("payload.candidateId"))
    val subjectId = when(candidateId =!= lit(""), candidateId).otherwise(col("actorId"))
    events.withColumn(SubjectTokenColumn, tokenize(coalesce(subjectId, lit(""))))
  }

  /** Removes data for active erasure markers before it can be merged into Silver. Marker sources expose only the
    * already-HMACed token; this transform has no persistence or Mongo dependency.
    */
  def excludeActiveDeletionMarkers(events: DataFrame, activeMarkerTokens: DataFrame): DataFrame = {
    if (!events.columns.contains(SubjectTokenColumn))
      throw AnalyticsError.InvalidSourceSchema(Vector(SubjectTokenColumn))
    if (!activeMarkerTokens.columns.contains(SubjectTokenColumn))
      throw AnalyticsError.InvalidSourceSchema(Vector(SubjectTokenColumn))
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
