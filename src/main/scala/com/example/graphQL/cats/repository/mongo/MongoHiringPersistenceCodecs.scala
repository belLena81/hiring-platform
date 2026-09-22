package com.example.graphQL.cats.repository.mongo

import org.bson.{BsonDocument, BsonDocumentReader, BsonDocumentWriter, Document}
import org.bson.codecs.{Codec, DecoderContext, DocumentCodec, EncoderContext}
import org.bson.codecs.configuration.{CodecRegistries, CodecRegistry}
import org.mongodb.scala.bson.codecs.Macros as ScalaMacros
import org.mongodb.scala.bson.codecs.IterableCodecProvider
import com.mongodb.MongoClientSettings

import java.util.Date

/** Persistence-shaped records used by the generated BSON codecs. */
private[mongo] object MongoHiringPersistenceCodecs {
  final case class StoredLocation(country: String, city: String, remote: Boolean)
  final case class StoredProfile(
      kind: String,
      skills: Option[List[String]],
      experienceSummary: Option[String],
      resumeRef: Option[String],
      organizationName: Option[String],
      jobTitle: Option[String]
  )
  final case class StoredEmbeddingMeta(model: String, sourceHash: String, updatedAt: Date)
  final case class StoredEmbeddingFields(embedding: List[Double], embeddingMeta: StoredEmbeddingMeta)

  final case class StoredUser(
      _id: String,
      email: Option[String],
      emailCanonical: Option[String],
      name: String,
      nameCanonical: String,
      role: String,
      profile: Option[StoredProfile],
      createdAt: Date,
      accountStatus: String,
      deletedAt: Option[Date],
      adminSingletonKey: Option[String],
      passwordHash: Option[String],
      embedding: Option[List[Double]],
      embeddingMeta: Option[StoredEmbeddingMeta]
  )

  final case class StoredJob(
      _id: String,
      recruiterId: String,
      title: String,
      description: String,
      requirements: List[String],
      skills: List[String],
      location: StoredLocation,
      status: String,
      createdAt: Date,
      updatedAt: Date,
      closedAt: Option[Date],
      embedding: Option[List[Double]],
      embeddingMeta: Option[StoredEmbeddingMeta]
  )

  final case class StoredApplication(
      _id: String,
      candidateId: String,
      jobId: String,
      status: String,
      createdAt: Date,
      updatedAt: Date
  )

  final case class StoredApplicationEvent(
      _id: String,
      applicationId: String,
      previousStatus: Option[String],
      newStatus: String,
      actorId: String,
      occurredAt: Date,
      feedback: Option[String],
      reason: Option[String]
  )

  final case class StoredOperationalEvent(
      _id: String,
      topic: String,
      eventType: String,
      occurredAt: Date,
      aggregateType: String,
      aggregateId: String,
      actorId: String,
      payload: String,
      envelopeBytes: Array[Byte],
      partitionKey: String
  )

  final case class StoredOutboxRecord(
      _id: String,
      topic: String,
      eventType: String,
      occurredAt: Date,
      aggregateType: String,
      aggregateId: String,
      actorId: String,
      payload: String,
      envelopeBytes: Array[Byte],
      partitionKey: String,
      state: String,
      attempts: Int,
      availableAt: Date,
      leaseOwner: Option[String],
      leaseToken: Option[String],
      createdAt: Date,
      updatedAt: Date
  )

  final case class StoredSearchSessionResult(resultId: String, rank: Int, score: Double)
  final case class StoredSearchSession(
      _id: String,
      actorId: String,
      searchKind: String,
      query: Option[String],
      filter: String,
      model: Option[String],
      results: List[StoredSearchSessionResult],
      occurredAt: Date,
      expiresAt: Date
  )

  private val providers = CodecRegistries.fromProviders(
    IterableCodecProvider.apply(),
    ScalaMacros.createCodecProviderIgnoreNone[StoredLocation](),
    ScalaMacros.createCodecProviderIgnoreNone[StoredProfile](),
    ScalaMacros.createCodecProviderIgnoreNone[StoredEmbeddingMeta](),
    ScalaMacros.createCodecProviderIgnoreNone[StoredEmbeddingFields](),
    ScalaMacros.createCodecProviderIgnoreNone[StoredUser](),
    ScalaMacros.createCodecProviderIgnoreNone[StoredJob](),
    ScalaMacros.createCodecProviderIgnoreNone[StoredApplication](),
    ScalaMacros.createCodecProviderIgnoreNone[StoredApplicationEvent](),
    ScalaMacros.createCodecProviderIgnoreNone[StoredOperationalEvent](),
    ScalaMacros.createCodecProvider[StoredOutboxRecord](),
    ScalaMacros.createCodecProviderIgnoreNone[StoredSearchSessionResult](),
    ScalaMacros.createCodecProvider[StoredSearchSession]()
  )

  private val registry: CodecRegistry = CodecRegistries.fromRegistries(
    providers,
    MongoClientSettings.getDefaultCodecRegistry
  )

  private val documentCodec = new DocumentCodec(registry)
  private val locationCodec = ScalaMacros.createCodecIgnoreNone[StoredLocation](registry)
  private val profileCodec = ScalaMacros.createCodecIgnoreNone[StoredProfile](registry)
  private val embeddingCodec = ScalaMacros.createCodecIgnoreNone[StoredEmbeddingFields](registry)
  private val userCodec = ScalaMacros.createCodecIgnoreNone[StoredUser](registry)
  private val jobCodec = ScalaMacros.createCodecIgnoreNone[StoredJob](registry)
  private val applicationCodec = ScalaMacros.createCodecIgnoreNone[StoredApplication](registry)
  private val applicationEventCodec = ScalaMacros.createCodecIgnoreNone[StoredApplicationEvent](registry)
  private val operationalEventCodec = ScalaMacros.createCodecIgnoreNone[StoredOperationalEvent](registry)
  private val outboxCodec = ScalaMacros.createCodec[StoredOutboxRecord](registry)
  private val searchSessionCodec = ScalaMacros.createCodec[StoredSearchSession](registry)

  def location(value: StoredLocation): Document = encode(value, locationCodec)
  def profile(value: StoredProfile): Document = encode(value, profileCodec)
  def embedding(value: StoredEmbeddingFields): Document = encode(value, embeddingCodec)
  def user(value: StoredUser): Document = encode(value, userCodec)
  def job(value: StoredJob): Document = encode(value, jobCodec)
  def application(value: StoredApplication): Document = encode(value, applicationCodec)
  def applicationEvent(value: StoredApplicationEvent): Document = encode(value, applicationEventCodec)
  def operationalEvent(value: StoredOperationalEvent): Document = encode(value, operationalEventCodec)
  def outbox(value: StoredOutboxRecord): Document = encode(value, outboxCodec)
  def searchSession(value: StoredSearchSession): Document = encode(value, searchSessionCodec)

  private def encode[A](value: A, codec: Codec[A]): Document = {
    val bson = new BsonDocument()
    codec.encode(new BsonDocumentWriter(bson), value, EncoderContext.builder().build())
    documentCodec.decode(new BsonDocumentReader(bson), DecoderContext.builder().build())
  }
}
