package com.example.hiring.analytics.adapter.mongo

import io.circe.generic.auto.*
import io.circe.{Decoder, Encoder}
import mongo4cats.circe.MongoJsonCodecs
import mongo4cats.codecs.CodecRegistry
import org.bson.{BsonArray, BsonDocument, BsonDocumentWriter, BsonInt64, BsonValue}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.codecs.configuration.{CodecProvider, CodecRegistry as DriverCodecRegistry}
import com.mongodb.MongoClientSettings

import java.time.Instant
import scala.jdk.CollectionConverters.*
import scala.reflect.ClassTag

/** Persisted analytics Mongo shapes. These records stay inside the Mongo adapter boundary. */
private[analytics] object AnalyticsMongoRecords {
  final case class ErasureRequest(
      _id: String,
      fencingVersion: Option[Int] = None,
      state: Option[String] = None,
      phase: Option[String] = None,
      leaseToken: Option[String] = None,
      leaseUntil: Option[Instant] = None,
      resumeAfter: Option[Instant] = None,
      failureCategory: Option[String] = None,
      attemptCount: Option[Int] = None,
      repairRequired: Option[Boolean] = None,
      requestedAt: Option[Instant] = None,
      receiptId: Option[String] = None,
      transactionalIds: Option[Vector[String]] = None,
      subjectIds: Option[Vector[String]] = None,
      subjectRefsVersion: Option[Int] = None,
      progress: Option[Int] = None,
      progressKey: Option[Long] = None,
      deltaPurgedAt: Option[Instant] = None,
      deltaGeneration: Option[Long] = None,
      deltaAffectedRows: Option[Long] = None,
      deltaEvidenceRevision: Option[Long] = None,
      kafkaRetentionBarrier: Option[RetentionBarrier] = None,
      expiresAt: Option[Instant] = None,
      deleted: Option[Boolean] = None
  )

  final case class RetentionBarrier(topic: String, partitions: Vector[RetentionPartition])
  final case class RetentionPartition(number: Int, endOffsetExclusive: Long)
  final case class PublisherFence(
      _id: String,
      deleted: Option[Boolean],
      leaseToken: Option[String],
      leaseUntil: Option[Instant]
  )
  final case class WorkerHeartbeat(
      _id: String,
      state: Option[String],
      leaseUntil: Option[Instant],
      updatedAt: Option[Instant]
  )
  final case class MigrationEntry(_id: String, state: Option[String])
  final case class EventOutboxReferences(subjectRefsVersion: Option[Int], subjectIds: Option[Vector[String]])
  final case class DeltaFileEvidence(_id: String, requestId: String, filePath: String)
  final case class ErasureCompletion(_id: String, completedAt: Option[Instant], receiptId: Option[String])
  final case class ReportControl(
      _id: String,
      generation: Long,
      nextRevision: Option[Long],
      lastPublishedRevision: Long,
      lastRunId: Option[String],
      state: String
  )
  final case class ReportRun(
      _id: String,
      rangeFingerprint: String,
      generation: Long,
      revision: Long,
      state: String,
      createdAt: Option[Instant] = None,
      expiresAt: Option[Instant] = None
  )
  final case class ReportSnapshotMetadata(
      _id: String,
      generation: Long,
      revision: Long,
      runId: String,
      expiresAt: Option[Instant]
  )
  final case class ReportFunnelDay(
      day: Instant,
      created: Long,
      accepted: Long,
      declined: Long,
      interview: Long,
      hired: Long,
      rejected: Long
  )
  final case class ReportSkillPostingDay(day: Instant, skill: String, postings: Long)
  final case class ReportTimeToHire(
      p50Hours: Double,
      p75Hours: Double,
      p90Hours: Double,
      p95Hours: Double,
      eligibleCount: Long,
      excludedCount: Long
  )
  final case class HmacAuthorization(
      _id: String,
      lakehouseId: String,
      keyId: String,
      originalVerifier: String,
      evidenceFacts: String,
      evidenceDigest: String,
      authorizedAt: Instant
  )
  final case class StreamingActivation(
      _id: String,
      streamId: String,
      sourceIdentity: String,
      lakehouseId: String,
      contractFingerprint: String,
      settingsFingerprint: String,
      evidenceReferences: Vector[String],
      independentReviewerReferences: Vector[String],
      evidenceDigest: String
  )
  final case class UserAccountStatus(_id: String, accountStatus: String)
  final case class LakehouseLock(_id: String, ownerToken: String, acquiredAt: Instant)

  private object circeCodecs extends MongoJsonCodecs
  import circeCodecs.*

  private[analytics] def registry[A: ClassTag: Encoder: Decoder](
      longFields: Set[String] = Set.empty,
      intFields: Set[String] = Set.empty
  ): CodecRegistry = {
    val derived = CodecRegistry.mergeWithDefault(CodecRegistry.from(circeCodecs.deriveCirceCodecProvider[A].get))
    if (longFields.isEmpty && intFields.isEmpty) derived
    else {
      val recordClass = summon[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]]
      val recordCodec = derived.get(recordClass)
      val documentCodec = MongoClientSettings.getDefaultCodecRegistry.get(classOf[BsonDocument])
      val widthPreservingCodec = new Codec[A] {
        override def encode(writer: org.bson.BsonWriter, value: A, context: EncoderContext): Unit = {
          val document = new BsonDocument()
          recordCodec.encode(new BsonDocumentWriter(document), value, context)
          documentCodec.encode(writer, preserveLongWidths(document, longFields), context)
        }

        override def decode(reader: org.bson.BsonReader, context: DecoderContext): A = {
          val document = documentCodec.decode(reader, context)
          validateNumericWidths(document, longFields, intFields)
          val positioned = new org.bson.BsonDocumentReader(document)
          positioned.readBsonType()
          recordCodec.decode(positioned, context)
        }

        override def getEncoderClass: Class[A] = recordClass
      }
      val provider = new CodecProvider {
        override def get[T](clazz: Class[T], registry: DriverCodecRegistry): Codec[T] =
          if (clazz == recordClass) widthPreservingCodec.asInstanceOf[Codec[T]] else null
      }
      CodecRegistry.mergeWithDefault(CodecRegistry.from(provider))
    }
  }

  private def preserveLongWidths(document: BsonDocument, longFields: Set[String]): BsonDocument = {
    def widen(value: BsonValue): BsonValue = value match {
      case nested: BsonDocument =>
        val widened = new BsonDocument()
        nested.forEach((key, field) =>
          widened.put(
            key,
            if (longFields.contains(key) && field.isInt32) new BsonInt64(field.asInt32().getValue.toLong)
            else widen(field)
          )
        )
        widened
      case array: BsonArray => new BsonArray(array.getValues.asScala.map(widen).asJava)
      case other            => other
    }
    widen(document).asDocument()
  }

  private def validateNumericWidths(document: BsonDocument, longFields: Set[String], intFields: Set[String]): Unit = {
    def invalid(name: String, expected: String): Nothing =
      throw mongo4cats.errors.MongoJsonParsingException(s"BSON field $name must be $expected", None)

    def visit(value: BsonValue): Unit = value match {
      case nested: BsonDocument =>
        nested.forEach((name, field) => {
          if (longFields.contains(name) && !field.isNull && !field.isInt64) invalid(name, "Int64")
          if (intFields.contains(name) && !field.isNull && !field.isInt32) invalid(name, "Int32")
          visit(field)
        })
      case array: BsonArray => array.getValues.asScala.foreach(visit)
      case _                => ()
    }
    visit(document)
  }

  val erasureRequestRegistry: CodecRegistry = registry[ErasureRequest](
    Set("progressKey", "deltaGeneration", "deltaAffectedRows", "deltaEvidenceRevision", "endOffsetExclusive"),
    Set("fencingVersion", "attemptCount", "subjectRefsVersion", "progress", "number")
  )
  val publisherFenceRegistry: CodecRegistry = registry[PublisherFence]()
  val workerHeartbeatRegistry: CodecRegistry = registry[WorkerHeartbeat]()
  val migrationEntryRegistry: CodecRegistry = registry[MigrationEntry]()
  val eventOutboxReferencesRegistry: CodecRegistry =
    registry[EventOutboxReferences](intFields = Set("subjectRefsVersion"))
  val deltaFileEvidenceRegistry: CodecRegistry = registry[DeltaFileEvidence]()
  val erasureCompletionRegistry: CodecRegistry = registry[ErasureCompletion]()
  val reportControlRegistry: CodecRegistry =
    registry[ReportControl](Set("generation", "nextRevision", "lastPublishedRevision"))
  val reportRunRegistry: CodecRegistry = registry[ReportRun](Set("generation", "revision"))
  val reportSnapshotRegistry: CodecRegistry = registry[ReportSnapshotMetadata](Set("generation", "revision"))
  val hmacAuthorizationRegistry: CodecRegistry = registry[HmacAuthorization]()
  val streamingActivationRegistry: CodecRegistry = registry[StreamingActivation]()
  val userAccountStatusRegistry: CodecRegistry = registry[UserAccountStatus]()
  val lakehouseLockRegistry: CodecRegistry = registry[LakehouseLock]()
}
