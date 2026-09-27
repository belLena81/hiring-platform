package com.example.hiring.analytics.mongo

import com.example.hiring.analytics.AnalyticsError
import com.example.hiring.analytics.batch.AnalyticsReportReservation

import org.bson.{BsonReader, BsonWriter, Document}
import org.bson.codecs.{Codec, DecoderContext, DocumentCodec, EncoderContext}
import org.bson.codecs.configuration.{CodecRegistries, CodecRegistry}

import java.util.Date

/** Typed views of the stable report metadata. The underlying document is retained so encoding preserves every existing
  * BSON field, including fields that this adapter does not read.
  */
private[analytics] object MongoAnalyticsReportRecords {
  final case class Run(reservation: AnalyticsReportReservation, state: String, document: Document)
  final case class Control(
      generation: Long,
      nextRevision: Option[Long],
      lastPublishedRevision: Long,
      lastRunId: Option[String],
      state: String,
      document: Document
  )
  final case class Snapshot(
      generation: Long,
      revision: Long,
      runId: String,
      expiresAt: Option[Date],
      document: Document
  ) {
    def matches(reservation: AnalyticsReportReservation): Boolean =
      generation == reservation.generation && revision == reservation.revision && runId == reservation.runId
  }

  final case class DecodedRun(value: Either[AnalyticsError, Run], document: Document)
  final case class DecodedControl(value: Either[AnalyticsError, Control], document: Document)
  final case class DecodedSnapshot(value: Either[AnalyticsError, Snapshot], document: Document)

  private val malformed = AnalyticsError.InvalidConfiguration("analytics report record is malformed")

  private def string(document: Document, field: String): Either[AnalyticsError, String] =
    Option(document.get(field)).collect { case value: String => value }.toRight(malformed)

  private def long(document: Document, field: String): Either[AnalyticsError, Long] =
    Option(document.get(field)).collect { case value: java.lang.Long => value.longValue() }.toRight(malformed)

  private def optionalString(document: Document, field: String): Either[AnalyticsError, Option[String]] =
    Option(document.get(field)) match {
      case None                => Right(None)
      case Some(value: String) => Right(Some(value))
      case _                   => Left(malformed)
    }

  private def optionalLong(document: Document, field: String): Either[AnalyticsError, Option[Long]] =
    Option(document.get(field)) match {
      case None                        => Right(None)
      case Some(value: java.lang.Long) => Right(Some(value.longValue()))
      case _                           => Left(malformed)
    }

  private def optionalDate(document: Document, field: String): Either[AnalyticsError, Option[Date]] =
    Option(document.get(field)) match {
      case None              => Right(None)
      case Some(value: Date) => Right(Some(value))
      case _                 => Left(malformed)
    }

  private def run(document: Document): Either[AnalyticsError, Run] =
    for {
      id <- string(document, AnalyticsCollections.Fields.Id)
      fingerprint <- string(document, AnalyticsCollections.Fields.RangeFingerprint)
      generation <- long(document, AnalyticsCollections.Fields.Generation)
      revision <- long(document, AnalyticsCollections.Fields.Revision)
      state <- string(document, AnalyticsCollections.Fields.State)
    } yield Run(AnalyticsReportReservation(id, fingerprint, generation, revision), state, document)

  private def control(document: Document): Either[AnalyticsError, Control] =
    for {
      generation <- long(document, AnalyticsCollections.Fields.Generation)
      nextRevision <- optionalLong(document, AnalyticsCollections.Fields.NextRevision)
      lastRevision <- long(document, AnalyticsCollections.Fields.LastPublishedRevision)
      lastRunId <- optionalString(document, AnalyticsCollections.Fields.LastRunId)
      state <- string(document, AnalyticsCollections.Fields.State)
    } yield Control(generation, nextRevision, lastRevision, lastRunId, state, document)

  private def snapshot(document: Document): Either[AnalyticsError, Snapshot] =
    for {
      generation <- long(document, AnalyticsCollections.Fields.Generation)
      revision <- long(document, AnalyticsCollections.Fields.Revision)
      runId <- string(document, AnalyticsCollections.Fields.RunId)
      expiresAt <- optionalDate(document, AnalyticsCollections.Fields.ExpiresAt)
    } yield Snapshot(generation, revision, runId, expiresAt, document)

  private class DocumentViewCodec[A](clazz: Class[A], wrap: Document => A, unwrap: A => Document) extends Codec[A] {
    private val documentCodec = new DocumentCodec()
    override def getEncoderClass: Class[A] = clazz
    override def decode(reader: BsonReader, context: DecoderContext): A =
      wrap(documentCodec.decode(reader, context))
    override def encode(writer: BsonWriter, value: A, context: EncoderContext): Unit =
      documentCodec.encode(writer, unwrap(value), context)
  }

  def registry(parent: CodecRegistry): CodecRegistry = CodecRegistries.fromRegistries(
    CodecRegistries.fromCodecs(
      new DocumentViewCodec(classOf[DecodedRun], document => DecodedRun(run(document), document), _.document),
      new DocumentViewCodec(
        classOf[DecodedControl],
        document => DecodedControl(control(document), document),
        _.document
      ),
      new DocumentViewCodec(
        classOf[DecodedSnapshot],
        document => DecodedSnapshot(snapshot(document), document),
        _.document
      )
    ),
    parent
  )
}
