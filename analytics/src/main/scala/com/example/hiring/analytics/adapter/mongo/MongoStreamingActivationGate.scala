package com.example.hiring.analytics.adapter.mongo

import com.example.hiring.analytics.domain.{StreamingActivationAuthorization, StreamingActivationIdentity}
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.errors.AnalyticsErrorTranslation.translating
import com.example.hiring.analytics.service.streaming.StreamingActivationGate

import cats.effect.Async
import cats.syntax.all.*
import com.mongodb.{ReadConcern, WriteConcern}
import mongo4cats.database.MongoDatabase
import java.util.concurrent.TimeUnit

/** Read-only runtime gate. Provisioning is an operator action after the Phase 6 audit and independent signoff. */
private[analytics] final class MongoStreamingActivationGate[F[_]: Async](
    database: MongoDatabase[F],
    streams: MongoPublisherStream
) extends StreamingActivationGate[F] {
  import MongoStreamingActivationGate.*

  private val collection = database
    .withReadConcern(ReadConcern.MAJORITY)
    .getCollection[AnalyticsMongoRecords.StreamingActivation](
      CollectionName,
      AnalyticsMongoRecords.streamingActivationRegistry
    )
    .map(_.withWriteConcern(WriteConcern.MAJORITY.withJournal(true).withWTimeout(15000L, TimeUnit.MILLISECONDS)))

  override def requireAuthorized(identity: StreamingActivationIdentity, grantId: String): F[java.time.Instant] =
    collection
      .flatMap(value =>
        streams
          .stream(capacity => value.find(new org.bson.Document("_id", grantId)).boundedStream(capacity))
          .compile
          .last
      )
      .flatMap {
        case None         => Async[F].raiseError[java.time.Instant](ClosedGate)
        case Some(record) =>
          val authorization = StreamingActivationAuthorization(
            StreamingActivationIdentity(
              record.streamId,
              record.sourceIdentity,
              record.lakehouseId,
              record.contractFingerprint,
              record.settingsFingerprint
            ),
            record.grantId,
            record.validFrom,
            record.expiresAt,
            record.evidenceReferences,
            record.independentReviewerReferences,
            record.evidenceDigest
          )
          Async[F].realTimeInstant.flatMap { now =>
            Async[F]
              .fromEither(
                StreamingActivationAuthorization
                  .validate(authorization, identity, grantId, now)
                  .leftMap(_ => ClosedGate)
              )
              .map(_.expiresAt)
          }
      }
      .translating(AnalyticsError.MongoConnectionFailure(_))
}

private object MongoStreamingActivationGate {
  val CollectionName = "analytics_streaming_activation"
  val ClosedGate = AnalyticsError.InvalidConfiguration(
    "analytics streaming activation is absent, malformed, or does not match this runtime"
  )

}
