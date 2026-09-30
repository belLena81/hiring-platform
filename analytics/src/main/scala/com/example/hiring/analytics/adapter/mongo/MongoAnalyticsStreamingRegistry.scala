package com.example.hiring.analytics.adapter.mongo

import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.AnalyticsStreamingRegistry

import cats.effect.Async
import cats.syntax.all.*
import mongo4cats.database.MongoDatabase
import org.bson.Document

import scala.util.control.NonFatal

/** Uses the immutable activation inventory as a durable batch-exclusion registry. */
private[analytics] final class MongoAnalyticsStreamingRegistry[F[_]: Async](
    database: MongoDatabase[F],
    streams: MongoPublisherStream
) extends AnalyticsStreamingRegistry[F] {
  override def rejectBatchIfRegistered(lakehouseRoot: String): F[Unit] =
    Async[F]
      .fromEither(AnalyticsStreamingRegistry.ownerLockRoot(lakehouseRoot))
      .flatMap(root => Async[F].fromEither(MongoAnalyticsLakehouseLock.lockId(root)))
      .flatMap { ownerLockId =>
        database
          .getCollection[AnalyticsMongoRecords.LakehouseLock](
            MongoAnalyticsLakehouseLock.CollectionName,
            AnalyticsMongoRecords.lakehouseLockRegistry
          )
          .flatMap { collection =>
            streams
              .stream(capacity => collection.find(new Document("_id", ownerLockId)).boundedStream(capacity))
              .compile
              .last
          }
          .flatMap {
            case Some(_) =>
              Async[F].raiseError[Unit](
                AnalyticsError.InvalidConfiguration("one-shot analytics batch is disabled for a streaming lakehouse")
              )
            case None => Async[F].unit
          }
      }
      .adaptError {
        case error: AnalyticsError => error
        case NonFatal(cause)       => AnalyticsError.MongoConnectionFailure(cause)
      }
}
