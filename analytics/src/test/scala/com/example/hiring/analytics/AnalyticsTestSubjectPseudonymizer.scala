package com.example.hiring.analytics
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

  private def unwrap(value: ValidatedNec[String, SubjectPseudonymizer]): SubjectPseudonymizer =
    value.toEither.fold(errors => throw new AssertionError(errors.toChain.toList.mkString("; ")), identity)
}
