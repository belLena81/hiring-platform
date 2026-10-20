package com.example.graphQL.cats.repository.mongo

import cats.data.ValidatedNel
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.{AccountStatus, JobStatus, UserRole}
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.repository.mongo.MongoHiringCodecs.StoredDocumentError
import com.example.graphQL.cats.service.port.{ActorRecord, JobOwner}
import com.mongodb.client.model.Projections
import org.bson.Document
import org.bson.conversions.Bson

/** The field sets each read path asks the store for. Authorization reads and list reads never transfer the embedding
  * vector or the credential; only the embedding pipeline and search ranking read a whole document.
  */
private[mongo] object MongoReadProjections {

  /** A user with its profile, without credential and embedding. */
  val userProfile: Bson =
    Projections.exclude(MongoFields.PasswordHash, MongoFields.Embedding, MongoFields.EmbeddingMeta)

  /** A user including its embedding, without the credential, for the embedding pipeline and search ranking. */
  val userWithEmbedding: Bson = Projections.exclude(MongoFields.PasswordHash)

  /** A job without its embedding. */
  val jobDisplay: Bson = Projections.exclude(MongoFields.Embedding, MongoFields.EmbeddingMeta)

  val actor: Bson =
    Projections.include(MongoFields.Id, MongoFields.Role, MongoFields.AccountStatus, MongoFields.AdminSingletonKey)

  val jobOwner: Bson = Projections.include(MongoFields.Id, MongoFields.RecruiterId, MongoFields.Status)

  /** Only the revision, for compare-and-set writes whose other inputs the caller already holds. */
  val revision: Bson = Projections.include(MongoFields.Id, MongoFields.Version)

  /** The same exclusions as an aggregation stage, for `$lookup` sub-pipelines. */
  val userProfileStage: Document =
    excludeStage(MongoFields.PasswordHash, MongoFields.Embedding, MongoFields.EmbeddingMeta)
  val jobDisplayStage: Document = excludeStage(MongoFields.Embedding, MongoFields.EmbeddingMeta)

  private def excludeStage(fields: String*): Document =
    new Document("$project", fields.foldLeft(new Document())((projection, field) => projection.append(field, 0)))
}

/** Strict decoding of the minimal authorization projections: exactly the projected fields, nothing else. */
private[mongo] object MongoAuthorizationProjectionCodecs {
  private val actorFields =
    Set(MongoFields.Id, MongoFields.Role, MongoFields.AccountStatus, MongoFields.AdminSingletonKey)
  private val ownerFields = Set(MongoFields.Id, MongoFields.RecruiterId, MongoFields.Status)
  private val singletonAdminKey = "singleton-admin"

  def readActor(document: Document): ValidatedNel[StoredDocumentError, ActorRecord] =
    unexpected(document, actorFields).andThen { _ =>
      (
        MongoDocumentFields.requiredUuid(document, MongoFields.Id).map(UserId.apply).toValidatedNel,
        MongoDocumentFields
          .requiredEnum(document, MongoFields.Role)(MongoDocumentFields.byName(UserRole.values))
          .toValidatedNel,
        MongoDocumentFields
          .requiredEnum(document, MongoFields.AccountStatus)(MongoDocumentFields.byName(AccountStatus.values))
          .toValidatedNel,
        MongoDocumentFields.optionalString(document, MongoFields.AdminSingletonKey).toValidatedNel
      ).mapN((id, role, status, key) => ActorRecord(id, role, status, key.contains(singletonAdminKey)))
    }

  def readOwner(document: Document): ValidatedNel[StoredDocumentError, JobOwner] =
    unexpected(document, ownerFields).andThen { _ =>
      (
        MongoDocumentFields.requiredUuid(document, MongoFields.Id).map(JobId.apply).toValidatedNel,
        MongoDocumentFields.requiredUuid(document, MongoFields.RecruiterId).map(UserId.apply).toValidatedNel,
        MongoDocumentFields
          .requiredEnum(document, MongoFields.Status)(MongoDocumentFields.byName(JobStatus.values))
          .toValidatedNel
      ).mapN(JobOwner.apply)
    }

  private def unexpected(document: Document, allowed: Set[String]): ValidatedNel[StoredDocumentError, Unit] = {
    import scala.jdk.CollectionConverters.*
    document.keySet.asScala
      .find(!allowed.contains(_))
      .fold(().validNel)(f => StoredDocumentError.InvalidField(f).invalidNel)
  }
}
