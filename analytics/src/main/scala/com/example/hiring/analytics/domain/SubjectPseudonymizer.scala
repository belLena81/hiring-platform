package com.example.hiring.analytics.domain

import cats.data.{NonEmptyVector, ValidatedNec}
import cats.syntax.all.*
import io.github.iltotore.iron.*

import java.nio.charset.StandardCharsets
import java.util.Base64
import scala.collection.mutable
import scala.jdk.CollectionConverters.*
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

final class SubjectPseudonymizer private (
    primaryKey: (String, Array[Byte]),
    previousKeys: Vector[(String, Array[Byte])]
) extends Serializable {
  private val copiedKeys: NonEmptyVector[(String, Array[Byte])] = NonEmptyVector.of(
    primaryKey._1 -> primaryKey._2.clone(),
    previousKeys.map { case (version, key) => version -> key.clone() }*
  )
  @transient private lazy val threadMacs: ThreadLocal[mutable.Map[String, Mac]] =
    ThreadLocal.withInitial(() => mutable.Map.empty[String, Mac])
  val primaryKeyId: String = copiedKeys.head._1
  val keyIds: Set[String] = copiedKeys.iterator.map(_._1).toSet
  private[analytics] val keyVerifiers: Vector[(String, String)] = copiedKeys.toVector.map { key =>
    val token = tokenFor("hiring-analytics-key-continuity-v1", key)
    key._1 -> token.substring(key._1.length + 1)
  }

  def typedToken(subjectId: String): Either[String, SubjectToken] = {
    Option(subjectId)
      .filter(_.nonEmpty)
      .toRight("subject id must be non-empty")
      .flatMap(id => SubjectToken.fromHmac(tokenFor(id, copiedKeys.head)))
  }

  def token(subjectId: String): String = tokenFor(subjectId, copiedKeys.head)

  /** Tokens written to new analytics rows use only the active primary key. */
  def tokenForNewRows(subjectId: String): String = token(subjectId)

  /** Tokens used to match existing rows include every configured primary/retiring key. */
  def matchingTokens(subjectId: String): Either[String, Vector[String]] =
    Option(subjectId)
      .filter(_.nonEmpty)
      .toRight("subject id must be non-empty")
      .map(id => copiedKeys.toVector.map(key => tokenFor(id, key)))

  private def tokenFor(subjectId: String, key: (String, Array[Byte])) = {
    val mac = threadMacs
      .get()
      .getOrElseUpdate(
        key._1, {
          val initialized = Mac.getInstance(SubjectPseudonymizer.Algorithm)
          initialized.init(new SecretKeySpec(key._2, SubjectPseudonymizer.Algorithm))
          initialized
        }
      )
    val digest = mac.doFinal(subjectId.getBytes(StandardCharsets.UTF_8))
    key._1 + "_" + Base64.getUrlEncoder.withoutPadding().encodeToString(digest)
  }
}

object SubjectPseudonymizer {
  private val Algorithm = "HmacSHA256"
  private val KeyIdPattern = "[A-Za-z0-9-]{1,40}".r
  private val MinimumKeyBytes = 32

  def validatedKeyRing(
      primaryKeyId: String,
      secret: Array[Byte],
      previousKeys: Vector[(String, Array[Byte])]
  ): ValidatedNec[String, SubjectPseudonymizer] = {
    val allKeys = (primaryKeyId -> secret) +: previousKeys
    val ids = allKeys.map(_._1)
    val idAndKeyErrors = allKeys.zipWithIndex.flatMap { case ((id, key), index) =>
      Vector(
        Option.when(!KeyIdPattern.matches(id))(s"HMAC key ID at index $index is invalid"),
        Option.when(key.length < MinimumKeyBytes)(
          s"HMAC key material at index $index must contain at least $MinimumKeyBytes bytes"
        )
      ).flatten
    }
    val duplicateIds = Option.when(ids.distinct.size != ids.size)("HMAC key IDs must be unique").toVector
    val errors = idAndKeyErrors ++ duplicateIds
    cats.data.NonEmptyChain.fromSeq(errors) match {
      case Some(problems) => cats.data.Validated.Invalid(problems)
      case None           => cats.data.Validated.Valid(new SubjectPseudonymizer(primaryKeyId -> secret, previousKeys))
    }
  }

  def validateFromBase64(
      secret: Option[String],
      primaryKeyId: String,
      previousKeyId: Option[String],
      previousSecret: Option[String]
  ): ValidatedNec[String, SubjectPseudonymizer] = {
    val primary = decode(secret, "HIRING_ANALYTICS_HMAC_SECRET_BASE64")
    val configuredPreviousKeyId = previousKeyId.filter(_.trim.nonEmpty)
    val configuredPreviousSecret = previousSecret.filter(_.trim.nonEmpty)
    val previous = (configuredPreviousKeyId, configuredPreviousSecret) match {
      case (None, None)            => Vector.empty[(String, Array[Byte])].validNec[String]
      case (Some(id), Some(value)) =>
        (
          id.refineEither[io.github.iltotore.iron.constraint.any.Not[io.github.iltotore.iron.constraint.string.Blank]]
            .leftMap(_ => "HIRING_ANALYTICS_HMAC_PREVIOUS_KEY_ID must be non-empty")
            .toValidatedNec,
          decode(Some(value), "HIRING_ANALYTICS_HMAC_PREVIOUS_SECRET_BASE64")
        ).mapN(_ -> _).map(Vector(_))
      case _ => "previous HMAC key ID and secret must be configured together".invalidNec
    }
    (
      primaryKeyId
        .refineEither[io.github.iltotore.iron.constraint.any.Not[io.github.iltotore.iron.constraint.string.Blank]]
        .leftMap(_ => "HIRING_ANALYTICS_HMAC_KEY_ID must be non-empty")
        .toValidatedNec,
      primary,
      previous
    ).mapN { (id, key, oldKeys) => (id: String, key, oldKeys) }
      .andThen { case (id, key, oldKeys) => validatedKeyRing(id, key, oldKeys) }
  }

  private def decode(value: Option[String], name: String): ValidatedNec[String, Array[Byte]] =
    value.filter(_.nonEmpty) match {
      case None          => s"$name is required".invalidNec
      case Some(encoded) =>
        Either
          .catchNonFatal(Base64.getDecoder.decode(encoded))
          .leftMap(_ => s"$name is malformed")
          .toValidatedNec
          .andThen { bytes =>
            if (bytes.length < MinimumKeyBytes)
              s"$name must decode to at least $MinimumKeyBytes bytes".invalidNec
            else bytes.validNec
          }
    }

}

/** Spark-only privacy transforms. Raw identifiers exist only in the input frame before `silver`. */
