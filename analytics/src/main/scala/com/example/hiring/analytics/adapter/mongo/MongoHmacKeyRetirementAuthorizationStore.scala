package com.example.hiring.analytics.adapter.mongo

import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.keyretirement.HmacKeyRetirementAuthorization
import com.example.hiring.analytics.service.keyretirement.HmacKeyRetirementAuthorizationStore

import cats.effect.kernel.Async
import cats.syntax.all.*
import com.mongodb.{ReadConcern, WriteConcern}
import mongo4cats.codecs.CodecRegistry
import mongo4cats.database.MongoDatabase
import org.bson.Document

import java.util.Date
import java.util.concurrent.TimeUnit
import scala.util.control.NonFatal

private[analytics] object HmacKeyRetirementAuthorizationBson {
  import com.example.hiring.analytics.service.keyretirement.HmacKeyRetirementAuthorization
  import com.example.hiring.analytics.errors.AnalyticsError
  import com.example.hiring.analytics.adapter.mongo.{BsonDecoder, BsonValueDecoder}
  import java.util.Date
  def decode(document: Document): Either[AnalyticsError, HmacKeyRetirementAuthorization] = {
    import BsonValueDecoder.given
    val malformed = AnalyticsError.InvalidConfiguration("HMAC key retirement authorization is malformed")
    for {
      lakehouse <- BsonDecoder.required[String](document, "lakehouseId", malformed)
      keyId <- BsonDecoder.required[String](document, "keyId", malformed)
      verifier <- BsonDecoder.required[String](document, "originalVerifier", malformed)
      digest <- BsonDecoder.required[String](document, "evidenceDigest", malformed)
      facts <- BsonDecoder.required[String](document, "evidenceFacts", malformed)
      at <- BsonDecoder.required[Date](document, "authorizedAt", malformed).map(_.toInstant)
      result <- HmacKeyRetirementAuthorization.validate(
        HmacKeyRetirementAuthorization(lakehouse, keyId, verifier, facts, digest, at)
      )
      storedId <- BsonDecoder.required[String](document, "_id", malformed)
      expectedId = result.lakehouseId + ":" + result.keyId
      _ <- Either.cond(storedId == expectedId, (), malformed)
    } yield result
  }

  given BsonDecoder[HmacKeyRetirementAuthorization] = BsonDecoder.instance(decode)
}

/** Immutable Mongo record, read with majority concern and inserted with majority+journal acknowledgement. */
private[analytics] final class MongoHmacKeyRetirementAuthorizationStore[F[_]: Async](
    database: MongoDatabase[F],
    streams: MongoPublisherStream
) extends HmacKeyRetirementAuthorizationStore[F] {
  private val collection = database
    .withReadConcern(ReadConcern.MAJORITY)
    .getCollection[Document]("analytics_hmac_key_retirements", CodecRegistry.Default)
    .map(_.withWriteConcern(WriteConcern.MAJORITY.withJournal(true).withWTimeout(15000L, TimeUnit.MILLISECONDS)))

  private def id(value: HmacKeyRetirementAuthorization): String = value.lakehouseId + ":" + value.keyId

  private def decode(document: Document): Either[AnalyticsError, HmacKeyRetirementAuthorization] =
    HmacKeyRetirementAuthorizationBson.decode(document)

  override def list(root: String): F[Vector[HmacKeyRetirementAuthorization]] =
    Async[F].fromEither(MongoAnalyticsLakehouseLock.lockId(root)).flatMap { lakehouseId =>
      collection
        .flatMap(value =>
          streams
            .stream(capacity => value.find(new Document("lakehouseId", lakehouseId)).boundedStream(capacity))
            .compile
            .toVector
        )
        .flatMap(_.traverse(decode).liftTo[F])
        .adaptError {
          case error: AnalyticsError => error
          case NonFatal(_)           =>
            AnalyticsError.InvalidConfiguration("HMAC key retirement authorization is unavailable or malformed")
        }
    }

  override def insert(root: String, authorization: HmacKeyRetirementAuthorization): F[Unit] =
    for {
      expectedLakehouse <- Async[F].fromEither(MongoAnalyticsLakehouseLock.lockId(root))
      checked <- Async[F].fromEither(HmacKeyRetirementAuthorization.validate(authorization))
      _ <- Async[F].raiseUnless(checked.lakehouseId == expectedLakehouse)(
        AnalyticsError.InvalidConfiguration("HMAC key retirement authorization targets another lakehouse")
      )
      _ <- collection
        .flatMap(
          _.insertOne(
            new Document("_id", id(checked))
              .append("lakehouseId", checked.lakehouseId)
              .append("keyId", checked.keyId)
              .append("originalVerifier", checked.originalVerifier)
              .append("evidenceFacts", checked.evidenceFacts)
              .append("evidenceDigest", checked.evidenceDigest)
              .append("authorizedAt", Date.from(checked.authorizedAt)),
            new mongo4cats.models.collection.InsertOneOptions()
          )
        )
        .adaptError {
          case error: AnalyticsError => error
          case NonFatal(_)           =>
            AnalyticsError.InvalidConfiguration("HMAC key retirement authorization could not be persisted")
        }
    } yield ()
}
