package com.example.hiring.analytics

import com.example.hiring.analytics.mongo.{
  BsonDecoder,
  BsonValueDecoder,
  MongoAnalyticsLakehouseLock,
  MongoPublisherStream
}

import cats.effect.IO
import cats.syntax.all.*
import com.mongodb.{ReadConcern, WriteConcern}
import com.mongodb.reactivestreams.client.MongoDatabase
import org.bson.Document

import java.time.Instant
import java.util.Date
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.concurrent.TimeUnit
import scala.util.control.NonFatal

/** Permanent evidence that one anchored key may be omitted from this exact lakehouse's runtime key ring. */
private[analytics] final case class HmacKeyRetirementAuthorization(
    lakehouseId: String,
    keyId: String,
    originalVerifier: String,
    evidenceFacts: String,
    evidenceDigest: String,
    authorizedAt: Instant
)

private[analytics] object HmacKeyRetirementAuthorization {
  private[analytics] given BsonDecoder[HmacKeyRetirementAuthorization] = BsonDecoder.instance { document =>
    import BsonValueDecoder.given
    val malformed = AnalyticsError.InvalidConfiguration("HMAC key retirement authorization is malformed")
    for {
      lakehouse <- BsonDecoder.required[String](document, "lakehouseId", malformed)
      keyId <- BsonDecoder.required[String](document, "keyId", malformed)
      verifier <- BsonDecoder.required[String](document, "originalVerifier", malformed)
      digest <- BsonDecoder.required[String](document, "evidenceDigest", malformed)
      facts <- BsonDecoder.required[String](document, "evidenceFacts", malformed)
      at <- BsonDecoder.required[Date](document, "authorizedAt", malformed).map(_.toInstant)
      result <- validate(HmacKeyRetirementAuthorization(lakehouse, keyId, verifier, facts, digest, at))
      storedId <- BsonDecoder.required[String](document, "_id", malformed)
      _ <- Either.cond(
        storedId == s"${result.lakehouseId}:${result.keyId}",
        (),
        AnalyticsError.InvalidConfiguration("HMAC key retirement authorization identity is malformed")
      )
    } yield result
  }

  def lakehouseId(root: String): Either[AnalyticsError, String] = MongoAnalyticsLakehouseLock.lockId(root)

  def digest(facts: String): String = MessageDigest
    .getInstance("SHA-256")
    .digest(facts.getBytes(StandardCharsets.UTF_8))
    .map(byte => f"${byte & 0xff}%02x")
    .mkString

  def validate(value: HmacKeyRetirementAuthorization): Either[AnalyticsError, HmacKeyRetirementAuthorization] =
    Either.cond(
      value != null && value.lakehouseId != null && value.lakehouseId.matches("[0-9a-f]{64}") &&
        value.keyId != null && value.keyId.matches("[A-Za-z0-9-]{1,40}") &&
        value.originalVerifier != null && value.originalVerifier.matches("[A-Za-z0-9_-]{43}") &&
        value.evidenceFacts != null && value.evidenceFacts.nonEmpty && value.evidenceFacts.length <= 8192 &&
        value.evidenceDigest != null && value.evidenceDigest == digest(value.evidenceFacts) &&
        value.authorizedAt != null,
      value,
      AnalyticsError.InvalidConfiguration("HMAC key retirement authorization is malformed")
    )
}

/** The caller must hold the shared lakehouse mutex for all reads and writes. */
private[analytics] trait HmacKeyRetirementAuthorizationStore {
  def list(root: String): IO[Vector[HmacKeyRetirementAuthorization]]
  def insert(root: String, authorization: HmacKeyRetirementAuthorization): IO[Unit]
}

private[analytics] object HmacKeyRetirementAuthorizationStore {
  val unavailable: HmacKeyRetirementAuthorizationStore = new HmacKeyRetirementAuthorizationStore {
    override def list(root: String): IO[Vector[HmacKeyRetirementAuthorization]] = IO.pure(Vector.empty)
    override def insert(root: String, authorization: HmacKeyRetirementAuthorization): IO[Unit] =
      IO.raiseError(AnalyticsError.InvalidConfiguration("HMAC key retirement authorization store is unavailable"))
  }
}

/** Immutable Mongo record, read with majority concern and inserted with majority+journal acknowledgement. */
private[analytics] final class MongoHmacKeyRetirementAuthorizationStore(database: MongoDatabase)
    extends HmacKeyRetirementAuthorizationStore {
  private val collection = database
    .getCollection("analytics_hmac_key_retirements", classOf[Document])
    .withReadConcern(ReadConcern.MAJORITY)
    .withWriteConcern(WriteConcern.MAJORITY.withJournal(true).withWTimeout(15000L, TimeUnit.MILLISECONDS))

  private def id(value: HmacKeyRetirementAuthorization): String = value.lakehouseId + ":" + value.keyId

  private def decode(document: Document): Either[AnalyticsError, HmacKeyRetirementAuthorization] =
    BsonDecoder[HmacKeyRetirementAuthorization].decode(document)

  override def list(root: String): IO[Vector[HmacKeyRetirementAuthorization]] =
    IO.fromEither(HmacKeyRetirementAuthorization.lakehouseId(root)).flatMap { lakehouseId =>
      MongoPublisherStream
        .stream(collection.find(new Document("lakehouseId", lakehouseId)))
        .compile
        .toVector
        .flatMap(_.traverse(decode).liftTo[IO])
        .adaptError { case NonFatal(_) =>
          AnalyticsError.InvalidConfiguration("HMAC key retirement authorization is unavailable or malformed")
        }
    }

  override def insert(root: String, authorization: HmacKeyRetirementAuthorization): IO[Unit] =
    for {
      expectedLakehouse <- IO.fromEither(HmacKeyRetirementAuthorization.lakehouseId(root))
      checked <- IO.fromEither(HmacKeyRetirementAuthorization.validate(authorization))
      _ <- IO.raiseUnless(checked.lakehouseId == expectedLakehouse)(
        AnalyticsError.InvalidConfiguration("HMAC key retirement authorization targets another lakehouse")
      )
      _ <- MongoPublisherStream
        .drain {
          collection.insertOne(
            new Document("_id", id(checked))
              .append("lakehouseId", checked.lakehouseId)
              .append("keyId", checked.keyId)
              .append("originalVerifier", checked.originalVerifier)
              .append("evidenceFacts", checked.evidenceFacts)
              .append("evidenceDigest", checked.evidenceDigest)
              .append("authorizedAt", Date.from(checked.authorizedAt))
          )
        }
        .adaptError { case NonFatal(_) =>
          AnalyticsError.InvalidConfiguration("HMAC key retirement authorization could not be persisted")
        }
    } yield ()
}
