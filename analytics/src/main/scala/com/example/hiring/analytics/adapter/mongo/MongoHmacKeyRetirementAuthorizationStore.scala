package com.example.hiring.analytics.adapter.mongo

import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.errors.AnalyticsErrorTranslation.translating
import com.example.hiring.analytics.service.keyretirement.HmacKeyRetirementAuthorization
import com.example.hiring.analytics.service.keyretirement.HmacKeyRetirementAuthorizationStore

import cats.effect.kernel.Async
import cats.syntax.all.*
import com.mongodb.{ReadConcern, WriteConcern}
import com.mongodb.client.model.Projections
import mongo4cats.database.MongoDatabase
import org.bson.Document

import java.util.concurrent.TimeUnit

/** Immutable Mongo record, read with majority concern and inserted with majority+journal acknowledgement. */
private[analytics] final class MongoHmacKeyRetirementAuthorizationStore[F[_]: Async](
    database: MongoDatabase[F],
    streams: MongoPublisherStream
) extends HmacKeyRetirementAuthorizationStore[F] {
  private val collection = database
    .withReadConcern(ReadConcern.MAJORITY)
    .getCollection[AnalyticsMongoRecords.HmacAuthorization](
      "analytics_hmac_key_retirements",
      AnalyticsMongoRecords.hmacAuthorizationRegistry
    )
    .map(_.withWriteConcern(WriteConcern.MAJORITY.withJournal(true).withWTimeout(15000L, TimeUnit.MILLISECONDS)))

  private def id(value: HmacKeyRetirementAuthorization): String = value.lakehouseId + ":" + value.keyId

  private def decode(
      record: AnalyticsMongoRecords.HmacAuthorization
  ): Either[AnalyticsError, HmacKeyRetirementAuthorization] = {
    val malformed = AnalyticsError.InvalidConfiguration("HMAC key retirement authorization is malformed")
    for {
      result <- HmacKeyRetirementAuthorization.validate(
        HmacKeyRetirementAuthorization(
          record.lakehouseId,
          record.keyId,
          record.originalVerifier,
          record.evidenceFacts,
          record.evidenceDigest,
          record.authorizedAt
        )
      )
      _ <- Either.cond(record._id == id(result), (), malformed)
    } yield result
  }

  override def list(root: String): F[Vector[HmacKeyRetirementAuthorization]] =
    Async[F].fromEither(MongoAnalyticsLakehouseLock.lockId(root)).flatMap { lakehouseId =>
      collection
        .flatMap(value =>
          streams
            .stream(capacity =>
              value
                .find(new Document("lakehouseId", lakehouseId))
                .projection(
                  Projections.include(
                    AnalyticsCollections.Fields.Id,
                    "lakehouseId",
                    "keyId",
                    "originalVerifier",
                    "evidenceFacts",
                    "evidenceDigest",
                    "authorizedAt"
                  )
                )
                .boundedStream(capacity)
            )
            .compile
            .toVector
        )
        .flatMap(_.traverse(decode).liftTo[F])
        .translating(cause =>
          AnalyticsError.InvalidConfiguration(
            "HMAC key retirement authorization is unavailable or malformed",
            Some(cause)
          )
        )
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
            AnalyticsMongoRecords.HmacAuthorization(
              id(checked),
              checked.lakehouseId,
              checked.keyId,
              checked.originalVerifier,
              checked.evidenceFacts,
              checked.evidenceDigest,
              checked.authorizedAt
            ),
            new mongo4cats.models.collection.InsertOneOptions()
          )
        )
        .translating(cause =>
          AnalyticsError.InvalidConfiguration("HMAC key retirement authorization could not be persisted", Some(cause))
        )
    } yield ()
}
