package com.example.hiring.analytics.adapter.mongo

import com.mongodb.MongoClientSettings
import org.bson.{BsonDocument, BsonDocumentCodec, BsonValue}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.codecs.configuration.{CodecProvider, CodecRegistries, CodecRegistry}
import mongo4cats.codecs.MongoCodecProvider

import scala.jdk.CollectionConverters.*

/** Immutable BSON persistence record for analytics report documents. */
private[analytics] object MongoPojoCodecs {
  final case class ReportRecord(fields: Map[String, BsonValue]) {
    def get(name: String): BsonValue = fields.getOrElse(name, null)
    def extraFields(known: Set[String]): Map[String, BsonValue] = fields.filterNot { case (name, _) => known(name) }
  }

  private val documentCodec = new BsonDocumentCodec()
  private val reportRecordCodec = new Codec[ReportRecord] {
    override def getEncoderClass: Class[ReportRecord] = classOf[ReportRecord]

    override def encode(writer: org.bson.BsonWriter, value: ReportRecord, context: EncoderContext): Unit =
      documentCodec.encode(writer, BsonDocument(value.fields.asJava), context)

    override def decode(reader: org.bson.BsonReader, context: DecoderContext): ReportRecord =
      ReportRecord(documentCodec.decode(reader, context).asScala.toMap)
  }

  val provider: MongoCodecProvider[ReportRecord] = new MongoCodecProvider[ReportRecord] {
    override def get: CodecProvider = new CodecProvider {
      override def get[T](clazz: Class[T], registry: CodecRegistry): Codec[T] =
        if (classOf[ReportRecord].isAssignableFrom(clazz)) reportRecordCodec.asInstanceOf[Codec[T]] else null
    }
  }

  val registry: CodecRegistry = CodecRegistries.fromRegistries(
    MongoClientSettings.getDefaultCodecRegistry,
    CodecRegistries.fromCodecs(reportRecordCodec)
  )
}
