package com.example.graphQL.cats.repository.mongo

import cats.data.ValidatedNel
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.{JobStatus, JobSubmissionSnapshot}
import com.example.graphQL.cats.domain.model.Identifiers.JobId
import com.mongodb.client.model.Projections
import org.bson.Document

/** Strict decoding of the three persisted facts used by application submission. */
private[mongo] object MongoJobSubmissionSnapshotCodec {
  import MongoHiringCodecs.StoredDocumentError
  private val fields = Set(MongoFields.Id, MongoFields.Status, MongoFields.Version)
  val projection = Projections.include(MongoFields.Id, MongoFields.Status, MongoFields.Version)

  def read(document: Document): ValidatedNel[StoredDocumentError, JobSubmissionSnapshot] = {
    val id = MongoDocumentFields.requiredUuid(document, MongoFields.Id).map(JobId.apply)
    val status =
      MongoDocumentFields.requiredEnum(document, MongoFields.Status)(MongoDocumentFields.byName(JobStatus.values))
    val revision = MongoDocumentFields.requiredInt64(document, MongoFields.Version, min = 0L)
    if (document.keySet().size() != fields.size || !fields.forall(document.containsKey))
      StoredDocumentError.InconsistentDocument.invalidNel
    else (id.toValidatedNel, status.toValidatedNel, revision.toValidatedNel).mapN(JobSubmissionSnapshot.apply)
  }
}
