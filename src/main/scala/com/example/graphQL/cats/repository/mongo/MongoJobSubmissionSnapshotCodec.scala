package com.example.graphQL.cats.repository.mongo

import cats.data.ValidatedNel
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.{JobStatus, JobSubmissionSnapshot}
import com.example.graphQL.cats.domain.model.Identifiers.JobId
import com.example.graphQL.cats.shared.Parsing
import com.mongodb.client.model.Projections
import org.bson.Document

/** Strict decoding of the three persisted facts used by application submission. */
private[mongo] object MongoJobSubmissionSnapshotCodec {
  import MongoHiringCodecs.StoredDocumentError
  private val fields = Set(MongoFields.Id, MongoFields.Status, MongoFields.Version)
  val projection = Projections.include(MongoFields.Id, MongoFields.Status, MongoFields.Version)

  def read(document: Document): ValidatedNel[StoredDocumentError, JobSubmissionSnapshot] = {
    def string(field: String): Either[StoredDocumentError, String] = Option(document.get(field)) match {
      case Some(value: String) => Right(value)
      case None                => Left(StoredDocumentError.MissingField(field))
      case _                   => Left(StoredDocumentError.InvalidField(field))
    }
    val id = string(MongoFields.Id).flatMap(raw =>
      Parsing
        .parseUuid(raw)
        .toOption
        .filter(_.toString == raw)
        .map(JobId.apply)
        .toRight(StoredDocumentError.InvalidField(MongoFields.Id))
    )
    val status = string(MongoFields.Status).flatMap(raw =>
      JobStatus.values.find(_.toString == raw).toRight(StoredDocumentError.InvalidField(MongoFields.Status))
    )
    val revision = Option(document.get(MongoFields.Version)) match {
      case Some(value: java.lang.Long) if value.longValue >= 0L => Right(value.longValue)
      case None => Left(StoredDocumentError.MissingField(MongoFields.Version))
      case _    => Left(StoredDocumentError.InvalidField(MongoFields.Version))
    }
    if (document.keySet().size() != fields.size || !fields.forall(document.containsKey))
      StoredDocumentError.InconsistentDocument.invalidNel
    else (id.toValidatedNel, status.toValidatedNel, revision.toValidatedNel).mapN(JobSubmissionSnapshot.apply)
  }
}
