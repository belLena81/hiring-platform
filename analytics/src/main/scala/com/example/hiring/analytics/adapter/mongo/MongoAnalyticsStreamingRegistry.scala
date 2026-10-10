package com.example.hiring.analytics.adapter.mongo

import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.errors.AnalyticsErrorTranslation.translating
import com.example.hiring.analytics.service.batch.AnalyticsStreamingRegistry

import cats.effect.Async
import cats.syntax.all.*
import com.mongodb.WriteConcern
import mongo4cats.database.MongoDatabase
import org.bson.Document

import java.util.concurrent.TimeUnit

/** Durable stream ownership, separate from the short-lived process mutex and activation grant. */
private[analytics] final class MongoAnalyticsStreamingRegistry[F[_]: Async](
    database: MongoDatabase[F],
    streams: MongoPublisherStream
) extends AnalyticsStreamingRegistry[F] {
  import MongoAnalyticsStreamingRegistry.*

  private val registrations = database
    .getCollection[AnalyticsMongoRecords.StreamingLakehouseRegistration](
      CollectionName,
      AnalyticsMongoRecords.streamingLakehouseRegistrationRegistry
    )
    .map(
      _.withWriteConcern(
        WriteConcern.MAJORITY.withJournal(true).withWTimeout(WriteTimeoutMillis, TimeUnit.MILLISECONDS)
      )
    )

  override def registerLakehouse(lakehouseRoot: String): F[Unit] =
    Async[F].fromEither(AnalyticsStreamingRegistry.lakehouseId(lakehouseRoot)).flatMap { id =>
      registrations
        .flatMap(_.insertOne(AnalyticsMongoRecords.StreamingLakehouseRegistration(id, id)))
        .void
        .recoverWith { case MongoDuplicateKey() => verifyExisting(id) }
        .translating(AnalyticsError.MongoConnectionFailure(_))
    }

  override def rejectBatchIfRegistered(lakehouseRoot: String): F[Unit] =
    Async[F].fromEither(AnalyticsStreamingRegistry.lakehouseId(lakehouseRoot)).flatMap { id =>
      registrations
        .flatMap(collection =>
          streams.stream(capacity => collection.find(new Document("_id", id)).boundedStream(capacity)).compile.last
        )
        .flatMap {
          case Some(record) if record.lakehouseId == id =>
            Async[F].raiseError[Unit](
              AnalyticsError.InvalidConfiguration("one-shot analytics batch is disabled for a streaming lakehouse")
            )
          case Some(_) =>
            Async[F].raiseError[Unit](
              AnalyticsError.DataIntegrityViolation("streaming lakehouse registration is inconsistent")
            )
          case None => Async[F].unit
        }
        .translating(AnalyticsError.MongoConnectionFailure(_))
    }

  private def verifyExisting(id: String): F[Unit] =
    registrations
      .flatMap(collection =>
        streams.stream(capacity => collection.find(new Document("_id", id)).boundedStream(capacity)).compile.last
      )
      .flatMap {
        case Some(record) if record.lakehouseId == id => Async[F].unit
        case _                                        =>
          Async[F].raiseError(AnalyticsError.DataIntegrityViolation("streaming lakehouse registration is inconsistent"))
      }
      .translating(AnalyticsError.MongoConnectionFailure(_))

}

private object MongoAnalyticsStreamingRegistry {
  val CollectionName = "analytics_streaming_lakehouses"
  private val WriteTimeoutMillis = 15000L
}
