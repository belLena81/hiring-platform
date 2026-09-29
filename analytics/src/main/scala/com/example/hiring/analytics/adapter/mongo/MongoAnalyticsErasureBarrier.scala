package com.example.hiring.analytics.adapter.mongo

import com.example.hiring.analytics.domain.AccountSubjectId
import com.example.hiring.analytics.domain.AnalyticsTopic
import com.example.hiring.analytics.domain.{AnalyticsOffset, AnalyticsPartition}
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.erasure.*

import cats.effect.{Async, Resource}
import cats.syntax.all.*
import mongo4cats.client.MongoClient
import mongo4cats.collection.MongoCollection
import mongo4cats.database.MongoDatabase
import com.mongodb.client.model.{Filters, Projections, Updates}
import org.bson.Document

import java.time.Instant
import scala.jdk.CollectionConverters.*

/** Mongo persistence for the durable Kafka retention barrier. */
final class MongoAnalyticsErasureBarrier[F[_]: Async] private (
    requests: MongoCollection[F, AnalyticsMongoRecords.ErasureRequest],
    streams: MongoPublisherStream
) extends MongoAnalyticsErasureStoreSupport[F](streams)
    with ErasureBarrier[F] {
  import MongoAnalyticsErasureStoreSupport.*

  def persistBarrier(claim: ErasureClaim, barrier: KafkaRetentionBarrier, now: Instant): F[ErasureUpdate] = mongo {
    val update = Updates.set(
      AnalyticsCollections.Fields.KafkaRetentionBarrier,
      new Document(AnalyticsCollections.Fields.Topic, AnalyticsTopic.unwrap(barrier.topic)).append(
        AnalyticsCollections.Fields.Partitions,
        barrier.partitions
          .map(partition =>
            new Document(AnalyticsCollections.Fields.PartitionNumber, AnalyticsPartition.unwrap(partition.number))
              .append(
                AnalyticsCollections.Fields.EndOffsetExclusive,
                AnalyticsOffset.unwrap(partition.endOffsetExclusive)
              )
          )
          .asJava
      )
    )
    matchedUpdate(requests, ownedClaim(claim, now), update).map(toErasureUpdate)
  }

  def readBarrier(requestId: AccountSubjectId): F[Option[KafkaRetentionBarrier]] = mongo {
    requests
      .find(Filters.eq(AnalyticsCollections.Fields.Id, requestId.value))
      .projection(
        Projections.include(AnalyticsCollections.Fields.Id, AnalyticsCollections.Fields.KafkaRetentionBarrier)
      )
      .first
      .map(
        _.traverse(document => decodeRetentionBarrier(document.kafkaRetentionBarrier)).map(_.flatten)
      )
  }.flatMap(Async[F].fromEither)
}

object MongoAnalyticsErasureBarrier {
  def resource[F[_]: Async](
      database: MongoDatabase[F],
      streams: MongoPublisherStream,
      collectionName: String = MongoAnalyticsErasureStoreSupport.RequestCollection
  ): Resource[F, MongoAnalyticsErasureBarrier[F]] =
    Resource
      .eval(
        database.getCollection[AnalyticsMongoRecords.ErasureRequest](
          collectionName,
          AnalyticsMongoRecords.erasureRequestRegistry
        )
      )
      .map(requests => new MongoAnalyticsErasureBarrier(requests, streams))
}

/** Resource-owned composition of the three erasure Mongo ports. */
private[analytics] final case class MongoAnalyticsErasureStores[F[_]](
    queue: MongoAnalyticsErasureQueue[F],
    progress: MongoAnalyticsErasureProgress[F],
    barrier: MongoAnalyticsErasureBarrier[F]
)

private[analytics] object MongoAnalyticsErasureStores {
  def resource[F[_]: Async](
      client: MongoClient[F],
      database: MongoDatabase[F],
      streams: MongoPublisherStream
  ): Resource[F, MongoAnalyticsErasureStores[F]] =
    for {
      queue <- MongoAnalyticsErasureQueue.resource(database, streams)
      progress <- MongoAnalyticsErasureProgress.resource(client, database, streams)
      barrier <- MongoAnalyticsErasureBarrier.resource(database, streams)
    } yield MongoAnalyticsErasureStores(queue, progress, barrier)
}
