package com.example.hiring.analytics
import com.example.hiring.analytics.domain.*

import cats.data.ValidatedNec

/** Test fixture helpers keep invalid privacy configuration explicit at the validation boundary. */
private[analytics] object AnalyticsTestSubjectPseudonymizer {
  def fromSecret(secret: Array[Byte]): SubjectPseudonymizer =
    fromKeyRing("hmac-v1", secret, Vector.empty)

  def fromKeyRing(
      primaryKeyId: String,
      secret: Array[Byte],
      previousKeys: Vector[(String, Array[Byte])]
  ): SubjectPseudonymizer =
    unwrap(SubjectPseudonymizer.validatedKeyRing(primaryKeyId, secret, previousKeys))

  def fromBase64(secret: String): SubjectPseudonymizer =
    fromBase64(secret, "hmac-v1", None, None)

  def fromBase64(
      secret: String,
      primaryKeyId: String,
      previousKeyId: Option[String],
      previousSecret: Option[String]
  ): SubjectPseudonymizer =
    unwrap(SubjectPseudonymizer.validateFromBase64(Option(secret), primaryKeyId, previousKeyId, previousSecret))

  def token(pseudonymizer: SubjectPseudonymizer, subjectId: String): SubjectToken =
    pseudonymizer.typedToken(subjectId).fold(error => throw new AssertionError(error), identity)

  def tokenValue(pseudonymizer: SubjectPseudonymizer, subjectId: String): String =
    token(pseudonymizer, subjectId).value

  private def unwrap(value: ValidatedNec[String, SubjectPseudonymizer]): SubjectPseudonymizer =
    value.toEither.fold(errors => throw new AssertionError(errors.toChain.toList.mkString("; ")), identity)
}
