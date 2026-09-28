package com.example.hiring.analytics.adapter.mongo

import com.mongodb.MongoClientSettings
import org.bson.{BsonValue, Document}
import org.bson.codecs.configuration.{CodecRegistries, CodecRegistry}
import org.bson.codecs.pojo.annotations.{BsonExtraElements, BsonProperty}
import org.bson.codecs.pojo.PojoCodecProvider

import scala.compiletime.uninitialized

/** Official driver POJO codecs for analytics Mongo persistence records. */
private[analytics] object MongoPojoCodecs {
  final class ReportRecord() {
    private var idValue: BsonValue = uninitialized
    private var rangeFingerprintValue: BsonValue = uninitialized
    private var generationValue: BsonValue = uninitialized
    private var revisionValue: BsonValue = uninitialized
    private var nextRevisionValue: BsonValue = uninitialized
    private var lastPublishedRevisionValue: BsonValue = uninitialized
    private var lastRunIdValue: BsonValue = uninitialized
    private var stateValue: BsonValue = uninitialized
    private var runIdValue: BsonValue = uninitialized
    private var expiresAtValue: BsonValue = uninitialized
    private var extraFields: Document = uninitialized

    @BsonProperty("_id") def getId: BsonValue = idValue
    @BsonProperty("_id") def setId(value: BsonValue): Unit = idValue = value
    @BsonProperty("rangeFingerprint") def getRangeFingerprint: BsonValue = rangeFingerprintValue
    @BsonProperty("rangeFingerprint") def setRangeFingerprint(value: BsonValue): Unit = rangeFingerprintValue = value
    @BsonProperty("generation") def getGeneration: BsonValue = generationValue
    @BsonProperty("generation") def setGeneration(value: BsonValue): Unit = generationValue = value
    @BsonProperty("revision") def getRevision: BsonValue = revisionValue
    @BsonProperty("revision") def setRevision(value: BsonValue): Unit = revisionValue = value
    @BsonProperty("nextRevision") def getNextRevision: BsonValue = nextRevisionValue
    @BsonProperty("nextRevision") def setNextRevision(value: BsonValue): Unit = nextRevisionValue = value
    @BsonProperty("lastPublishedRevision") def getLastPublishedRevision: BsonValue = lastPublishedRevisionValue
    @BsonProperty("lastPublishedRevision")
    def setLastPublishedRevision(value: BsonValue): Unit = lastPublishedRevisionValue = value
    @BsonProperty("lastRunId") def getLastRunId: BsonValue = lastRunIdValue
    @BsonProperty("lastRunId") def setLastRunId(value: BsonValue): Unit = lastRunIdValue = value
    @BsonProperty("state") def getState: BsonValue = stateValue
    @BsonProperty("state") def setState(value: BsonValue): Unit = stateValue = value
    @BsonProperty("runId") def getRunId: BsonValue = runIdValue
    @BsonProperty("runId") def setRunId(value: BsonValue): Unit = runIdValue = value
    @BsonProperty("expiresAt") def getExpiresAt: BsonValue = expiresAtValue
    @BsonProperty("expiresAt") def setExpiresAt(value: BsonValue): Unit = expiresAtValue = value
    @BsonExtraElements def getExtraFields: Document = extraFields
    def setExtraFields(value: Document): Unit = extraFields = value
  }

  private val provider = PojoCodecProvider
    .builder()
    .register(classOf[ReportRecord])
    .build()

  val registry: CodecRegistry = CodecRegistries.fromRegistries(
    MongoClientSettings.getDefaultCodecRegistry,
    CodecRegistries.fromProviders(provider)
  )
}
