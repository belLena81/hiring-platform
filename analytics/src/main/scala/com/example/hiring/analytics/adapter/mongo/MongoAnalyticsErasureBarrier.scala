package com.example.hiring.analytics.adapter.mongo

import com.example.hiring.analytics.domain.AccountSubjectId
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.erasure.*

import cats.data.{Chain, EitherT}
import cats.effect.{Async, Resource}
import cats.syntax.all.*
import mongo4cats.client.{ClientSession, MongoClient}
import mongo4cats.collection.MongoCollection
import mongo4cats.database.MongoDatabase
import mongo4cats.codecs.CodecRegistry
import mongo4cats.circe.MongoJsonCodecs
import io.circe.{Decoder, Json}
import org.bson.BsonDocument
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.codecs.configuration.CodecProvider
import com.mongodb.client.model.{FindOneAndUpdateOptions, Filters, ReturnDocument, Sorts, Updates}
import com.mongodb.client.model.{ReplaceOneModel, ReplaceOptions, WriteModel}
import org.bson.Document
import org.bson.conversions.Bson

import java.time.Instant
import java.util.{Date, UUID}
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** Mongo persistence for the durable Kafka retention barrier. */
final class MongoAnalyticsErasureBarrier[F[_]: Async] private (
    requests: MongoCollection[F, Json],
    streams: MongoPublisherStream
) extends MongoAnalyticsErasureStoreSupport[F](streams)
    with ErasureBarrier[F] {
  import MongoAnalyticsErasureStoreSupport.*

  def persistBarrier(claim: ErasureClaim, barrier: KafkaRetentionBarrier, now: Instant): F[ErasureUpdate] = mongo {
    val partitionDocuments = barrier.partitions
      .map(partition =>
        new Document(AnalyticsCollections.Fields.PartitionNumber, partition.number)
          .append(AnalyticsCollections.Fields.EndOffsetExclusive, partition.endOffsetExclusive)
      )
      .asJava
    val update = Updates.set(
      AnalyticsCollections.Fields.KafkaRetentionBarrier,
      new Document(AnalyticsCollections.Fields.Topic, barrier.topic)
        .append(AnalyticsCollections.Fields.Partitions, partitionDocuments)
    )
    matchedUpdate(requests, ownedClaim(claim, now), update).map(toErasureUpdate)
  }

  def readBarrier(requestId: AccountSubjectId): F[Option[KafkaRetentionBarrier]] = mongo {
    streams
      .optional(requests.find(Filters.eq(AnalyticsCollections.Fields.Id, requestId.value)).first)
      .map(
        _.traverse(document =>
          readOptional[Json](document, AnalyticsCollections.Fields.KafkaRetentionBarrier)
            .flatMap(_.traverse(decodeRetentionBarrier))
        ).map(_.flatten)
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
      .eval(database.getCollection[Json](collectionName, MongoAnalyticsErasureStoreSupport.jsonRegistry))
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
