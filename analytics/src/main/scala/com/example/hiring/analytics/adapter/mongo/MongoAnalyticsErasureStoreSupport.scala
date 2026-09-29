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
import org.bson.codecs.{BsonDocumentCodec, Codec, DecoderContext, EncoderContext}
import org.bson.codecs.configuration.CodecRegistries
import com.mongodb.client.model.{FindOneAndUpdateOptions, Filters, ReturnDocument, Sorts, Updates}
import com.mongodb.client.model.{ReplaceOneModel, ReplaceOptions, WriteModel}
import org.bson.Document
import org.bson.conversions.Bson

import java.time.Instant
import java.util.{Date, UUID}
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** Shared JSON decoding, lease predicates, and Mongo error translation for erasure adapters. */
private[analytics] abstract class MongoAnalyticsErasureStoreSupport[F[_]: Async](
    protected val streams: MongoPublisherStream
) {
  import MongoAnalyticsErasureStoreSupport.*
  import jsonCodecs.*

  protected given Decoder[Instant] = jsonCodecs.instantDecoder
  protected given Decoder[Date] = jsonCodecs.instantDecoder.map(Date.from)

  protected def readField[A: Decoder](document: Json, name: String): Either[AnalyticsError, A] =
    document.hcursor.get[A](name).leftMap(_ => AnalyticsError.MalformedMarker)

  protected def readOptional[A: Decoder](document: Json, name: String): Either[AnalyticsError, Option[A]] =
    document.hcursor.get[Option[A]](name).leftMap(_ => AnalyticsError.MalformedMarker)

  protected def readInt32(document: Json, name: String): Either[AnalyticsError, Int] = int32(document, name)
  protected def readLong64(document: Json, name: String): Either[AnalyticsError, Long] = int64(document, name)

  protected def ownedClaim(claim: ErasureClaim, now: Instant): Bson = ownedClaimFilter(claim, now)
  protected def toErasureUpdate(matched: Boolean): ErasureUpdate =
    if (matched) ErasureUpdate.Applied else ErasureUpdate.LeaseLost

  protected def progressKeyFilter(value: Long): Bson =
    if (value == 0L)
      Filters.or(
        Filters.eq(AnalyticsCollections.Fields.ProgressKey, 0L),
        Filters.exists(AnalyticsCollections.Fields.ProgressKey, false)
      )
    else Filters.eq(AnalyticsCollections.Fields.ProgressKey, value)

  protected def matchedUpdate(collection: MongoCollection[F, Json], filter: Bson, update: Bson): F[Boolean] =
    collection.updateOne(filter, update, new com.mongodb.client.model.UpdateOptions()).map(_.getMatchedCount == 1L)

  protected def matchedUpdate(
      session: ClientSession[F],
      collection: MongoCollection[F, Json],
      filter: Bson,
      update: Bson
  ): F[Boolean] =
    streams.one(collection.underlying.updateOne(session.underlying, filter, update)).map(_.getMatchedCount == 1L)

  protected def mongo[A](work: => F[A]): F[A] = Async[F].defer(work).adaptError {
    case error: AnalyticsError => error
    case NonFatal(cause)       => AnalyticsError.MarkerStorageFailure(cause)
  }
}

private[analytics] object MongoAnalyticsErasureStoreSupport {
  val RequestCollection = AnalyticsCollections.ErasureRequests
  val HeartbeatCollection = AnalyticsCollections.ErasureHeartbeats
  val HeartbeatId = "analytics-erasure"
  val MaximumClaimPageSize = 100
  val ProgressPerPhase = ErasurePhase.ProgressPerPhase

  private object jsonCodecs extends MongoJsonCodecs
  import jsonCodecs.*
  private val jsonDecoder = jsonCodecs.deriveJsonBsonValueDecoder[Json]
  private val jsonEncoder = jsonCodecs.deriveJsonBsonValueEncoder[Json]
  private val bsonDocumentCodec = new BsonDocumentCodec()

  private def toBsonDocument(value: Json): BsonDocument =
    jsonEncoder
      .encode(value)
      .asDocument
      .getOrElse(throw new IllegalArgumentException("Mongo JSON collection values must be documents"))
      .toBsonDocument

  private def fromBsonDocument(document: BsonDocument): Json =
    jsonDecoder
      .decode(mongo4cats.bson.BsonValue.document(mongo4cats.bson.Document.fromJava(document)))
      .getOrElse(throw new IllegalArgumentException("malformed BSON JSON document"))

  private[analytics] val jsonCodec: Codec[Json] = new Codec[Json] {
    override def getEncoderClass: Class[Json] = classOf[Json]

    override def encode(writer: org.bson.BsonWriter, value: Json, context: EncoderContext): Unit = {
      bsonDocumentCodec.encode(writer, toBsonDocument(value), context)
    }

    override def decode(reader: org.bson.BsonReader, context: DecoderContext): Json = {
      fromBsonDocument(bsonDocumentCodec.decode(reader, context))
    }
  }

  private[analytics] val jsonRegistry =
    CodecRegistry.mergeWithDefault(CodecRegistries.fromCodecs(jsonCodec))

  private[analytics] def jsonFromBson(document: Document): Json = {
    val raw = document.toBsonDocument(classOf[Document], com.mongodb.MongoClientSettings.getDefaultCodecRegistry)
    fromBsonDocument(raw)
  }

  private def malformed = AnalyticsError.MalformedMarker
  private def field[A: Decoder](document: Json, key: String): Either[AnalyticsError, A] =
    document.hcursor.get[A](key).leftMap(_ => malformed)
  private def optional[A: Decoder](document: Json, key: String): Either[AnalyticsError, Option[A]] =
    document.hcursor.get[Option[A]](key).leftMap(_ => malformed)
  private def int32(document: Json, key: String): Either[AnalyticsError, Int] =
    document.hcursor.get[Int](key).leftMap(_ => malformed)
  private def int64(document: Json, key: String): Either[AnalyticsError, Long] =
    document.hcursor.get[Long](key).leftMap(_ => malformed)

  private[analytics] def decodeRetentionBarrier(document: Json): Either[AnalyticsError, KafkaRetentionBarrier] =
    for {
      topic <- field[String](document, AnalyticsCollections.Fields.Topic)
      rows <- field[Vector[Json]](document, AnalyticsCollections.Fields.Partitions)
      partitions <- rows.traverse(row =>
        for {
          number <- int32(row, AnalyticsCollections.Fields.PartitionNumber)
          offset <- int64(row, AnalyticsCollections.Fields.EndOffsetExclusive)
        } yield KafkaRetentionBarrier.Partition(number, offset)
      )
      valid <- KafkaRetentionBarrier.validate(KafkaRetentionBarrier(topic, partitions))
    } yield valid

  private[analytics] final case class PublisherFence(
      deleted: Boolean,
      leaseToken: Option[String],
      leaseUntil: Option[Instant]
  )

  private[analytics] def decodePublisherFence(document: Json): Either[AnalyticsError, PublisherFence] =
    for {
      deleted <- optional[Boolean](document, AnalyticsCollections.Fields.Deleted)
      leaseToken <- optional[String](document, AnalyticsCollections.Fields.LeaseToken)
      leaseUntil <- optional[Instant](document, AnalyticsCollections.Fields.LeaseUntil)
    } yield PublisherFence(deleted.getOrElse(false), leaseToken, leaseUntil)

  private[analytics] def ownedClaimFilter(claim: ErasureClaim, now: Instant): Bson =
    Filters.and(
      Filters.eq(AnalyticsCollections.Fields.Id, claim.requestId.value),
      Filters.eq(AnalyticsCollections.Fields.State, ErasureRequestState.Processing.persistedName),
      Filters.eq(AnalyticsCollections.Fields.LeaseToken, claim.leaseToken),
      Filters.gt(AnalyticsCollections.Fields.LeaseUntil, Date.from(now))
    )

  private[analytics] def decodeClaim(document: Json): Option[ErasureClaim] = {
    val decoded = for {
      fencingVersion <- int32(document, AnalyticsCollections.Fields.FencingVersion)
      _ <- Either.cond(fencingVersion == 1, (), malformed)
      rawId <- field[String](document, AnalyticsCollections.Fields.Id)
      _ <- Either.cond(scala.util.Try(UUID.fromString(rawId)).toOption.exists(_.toString == rawId), (), malformed)
      id <- AccountSubjectId.from(rawId).leftMap(_ => malformed)
      leaseToken <- field[String](document, AnalyticsCollections.Fields.LeaseToken)
      _ <- Either.cond(
        scala.util.Try(UUID.fromString(leaseToken)).toOption.exists(_.toString == leaseToken),
        (),
        malformed
      )
      leaseExpiry <- field[Instant](document, AnalyticsCollections.Fields.LeaseUntil)
      storedPhase <- optional[String](document, AnalyticsCollections.Fields.Phase)
      currentPhase <- storedPhase.fold[Either[AnalyticsError, ErasurePhase]](Right(ErasurePhase.Requested))(phase =>
        ErasurePhase.fromString(phase).toRight(malformed)
      )
      progress <- document.hcursor
        .get[Json](AnalyticsCollections.Fields.Progress)
        .toOption
        .fold[Either[AnalyticsError, Int]](Right(0))(_ => int32(document, AnalyticsCollections.Fields.Progress))
      progressKey <- document.hcursor
        .get[Json](AnalyticsCollections.Fields.ProgressKey)
        .toOption
        .fold[Either[AnalyticsError, Long]](Right(0L))(_ => int64(document, AnalyticsCollections.Fields.ProgressKey))
      attempts <- document.hcursor
        .get[Json](AnalyticsCollections.Fields.AttemptCount)
        .toOption
        .fold[Either[AnalyticsError, Int]](Right(0))(_ => int32(document, AnalyticsCollections.Fields.AttemptCount))
      _ <- Either.cond(
        progress >= 0 && progress < ErasurePhase.ProgressPerPhase && attempts >= 0 &&
          progressKey == currentPhase.ordinal.toLong * ErasurePhase.ProgressPerPhase.toLong + progress.toLong,
        (),
        malformed
      )
    } yield ErasureClaim(id, leaseToken, leaseExpiry, currentPhase, progress, progressKey, attempts)
    decoded.toOption
  }
}
