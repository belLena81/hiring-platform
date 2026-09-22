package com.example.graphQL.cats.repository.mongo

import cats.data.ValidatedNel
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.shared.events.*
import MongoHiringPersistenceCodecs.*
import io.circe.Json
import io.circe.parser.parse
import org.bson.Document

import java.time.Instant
import java.util.{Date, UUID}
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** BSON decoding is total because persisted values are untrusted input. */
private[mongo] object MongoHiringCodecs {
  enum StoredDocumentError {
    case MissingField(field: String)
    case InvalidField(field: String)
    case InconsistentDocument
  }

  import StoredDocumentError.*

  def userWithPassword(value: User, passwordHash: String): Document =
    MongoHiringPersistenceCodecs.user(storedUser(value, Some(passwordHash)))

  def user(value: User): Document =
    MongoHiringPersistenceCodecs.user(storedUser(value, None))

  def readUser(document: Document): ValidatedNel[StoredDocumentError, User] =
    noUnexpectedFields(document, UserFields).andThen { _ =>
    (
      uuid(document, "_id").toValidatedNel.map(UserId.apply),
      optionalString(document, "email").toValidatedNel,
      requiredString(document, "name").toValidatedNel,
      enumValue(document, "role", UserRole.values).toValidatedNel,
      readUserProfile(document),
      instant(document, "createdAt").toValidatedNel,
      enumValue(document, "accountStatus", AccountStatus.values).toValidatedNel,
      optionalInstant(document, "deletedAt").toValidatedNel,
      optionalString(document, "adminSingletonKey").toValidatedNel.map(_.contains("singleton-admin")),
      readEmbedding(document)
    ).mapN { (id, email, name, role, profile, createdAt, accountStatus, deletedAt, adminSingleton, embedding) =>
      User(id, email, name, role, profile, createdAt, adminSingleton, embedding, accountStatus, deletedAt)
    }.andThen { user =>
      Either.cond(user.roleProfileIsValid, user, InconsistentDocument).toValidatedNel
    }
    }

  def readCredentials(document: Document): ValidatedNel[StoredDocumentError, Option[AccountCredentials]] =
    optionalString(document, "passwordHash").toValidatedNel.andThen(_.traverse(hash => readUser(document).map(AccountCredentials(_, hash))))

  def job(job: Job): Document = MongoHiringPersistenceCodecs.job(storedJob(job))

  def readJob(document: Document): ValidatedNel[StoredDocumentError, Job] =
    noUnexpectedFields(document, JobFields).andThen { _ =>
    (
      uuid(document, "_id").toValidatedNel.map(JobId.apply),
      uuid(document, "recruiterId").toValidatedNel.map(UserId.apply),
      requiredString(document, "title").toValidatedNel,
      requiredString(document, "description").toValidatedNel,
      stringList(document, "requirements"),
      stringList(document, "skills").map(_.toSet),
      nestedDocument(document, "location").toValidatedNel.andThen(readLocation),
      enumValue(document, "status", JobStatus.values).toValidatedNel,
      instant(document, "createdAt").toValidatedNel,
      instant(document, "updatedAt").toValidatedNel,
      optionalInstant(document, "closedAt").toValidatedNel,
      readEmbedding(document)
    ).mapN(Job.apply)
    }

  def application(application: Application): Document =
    MongoHiringPersistenceCodecs.application(StoredApplication(
      application.id.value.toString,
      application.candidateId.value.toString,
      application.jobId.value.toString,
      application.status.toString,
      Date.from(application.createdAt),
      Date.from(application.updatedAt)
    ))

  def readApplication(document: Document): ValidatedNel[StoredDocumentError, Application] =
    noUnexpectedFields(document, ApplicationFields).andThen { _ =>
    (
      uuid(document, "_id").toValidatedNel.map(ApplicationId.apply),
      uuid(document, "candidateId").toValidatedNel.map(UserId.apply),
      uuid(document, "jobId").toValidatedNel.map(JobId.apply),
      enumValue(document, "status", ApplicationStatus.values).toValidatedNel,
      instant(document, "createdAt").toValidatedNel,
      instant(document, "updatedAt").toValidatedNel
    ).mapN(Application.apply)
    }

  def event(event: ApplicationEvent): Document =
    MongoHiringPersistenceCodecs.applicationEvent(StoredApplicationEvent(
      event.id.value.toString,
      event.applicationId.value.toString,
      event.previousStatus.map(_.toString),
      event.newStatus.toString,
      event.actorId.value.toString,
      Date.from(event.occurredAt),
      event.feedback,
      event.reason
    ))

  def readEvent(document: Document): ValidatedNel[StoredDocumentError, ApplicationEvent] =
    (
      uuid(document, "_id").toValidatedNel.map(ApplicationEventId.apply),
      uuid(document, "applicationId").toValidatedNel.map(ApplicationId.apply),
      optionalEnum(document, "previousStatus", ApplicationStatus.values).toValidatedNel,
      enumValue(document, "newStatus", ApplicationStatus.values).toValidatedNel,
      uuid(document, "actorId").toValidatedNel.map(UserId.apply),
      instant(document, "occurredAt").toValidatedNel,
      optionalString(document, "feedback").toValidatedNel,
      optionalString(document, "reason").toValidatedNel
    ).mapN(ApplicationEvent.apply)

  def operationalEvent(value: OperationalEventEnvelope): Document =
    MongoHiringPersistenceCodecs.operationalEvent(storedOperationalEvent(value))

  def readOperationalEvent(document: Document): ValidatedNel[StoredDocumentError, OperationalEventEnvelope] =
    (
      uuid(document, "_id").toValidatedNel,
      enumValue(document, "eventType", OperationalEventType.values).toValidatedNel,
      instant(document, "occurredAt").toValidatedNel,
      enumValue(document, "aggregateType", OperationalAggregateType.values).toValidatedNel,
      requiredString(document, "aggregateId").toValidatedNel,
      uuid(document, "actorId").toValidatedNel.map(UserId.apply),
      requiredString(document, "payload").toValidatedNel.andThen(value => json("payload")(value).toValidatedNel)
    ).mapN(OperationalEventEnvelope.apply)

  def outboxRecord(value: OperationalEventEnvelope, now: Instant): Document = {
    val event = storedOperationalEvent(value)
    MongoHiringPersistenceCodecs.outbox(StoredOutboxRecord(
      event._id, event.topic, event.eventType, event.occurredAt, event.aggregateType, event.aggregateId,
      event.actorId, event.payload, event.envelopeBytes, event.partitionKey, "Retryable", 0,
      Date.from(now), None, None, Date.from(now), Date.from(now)
    ))
  }

  def searchSession(value: SearchSession): Document =
    MongoHiringPersistenceCodecs.searchSession(StoredSearchSession(
      value.id.toString,
      value.actorId.value.toString,
      value.searchKind,
      value.query,
      value.filter.noSpaces,
      value.model,
      value.results.map(result => StoredSearchSessionResult(result.resultId, result.rank, result.score)),
      Date.from(value.occurredAt),
      Date.from(value.expiresAt)
    ))

  def readSearchSession(document: Document): ValidatedNel[StoredDocumentError, SearchSession] =
    (
      uuid(document, "_id").toValidatedNel,
      uuid(document, "actorId").toValidatedNel.map(UserId.apply),
      requiredString(document, "searchKind").toValidatedNel,
      optionalString(document, "query").toValidatedNel,
      requiredString(document, "filter").toValidatedNel.andThen(value => json("filter")(value).toValidatedNel),
      optionalString(document, "model").toValidatedNel,
      resultList(document, "results"),
      instant(document, "occurredAt").toValidatedNel,
      instant(document, "expiresAt").toValidatedNel
    ).mapN(SearchSession.apply)

  private val UserFields = Set("_id", "email", "emailCanonical", "name", "nameCanonical", "role", "profile",
    "createdAt", "updatedAt", "accountStatus", "deletedAt", "adminSingletonKey", "passwordHash", "embedding", "embeddingMeta")
  private val JobFields = Set("_id", "recruiterId", "title", "description", "requirements", "skills", "location",
    "status", "createdAt", "updatedAt", "closedAt", "embedding", "embeddingMeta")
  private val ApplicationFields = Set("_id", "candidateId", "jobId", "status", "createdAt", "updatedAt")

  private def noUnexpectedFields(document: Document, fields: Set[String]): ValidatedNel[StoredDocumentError, Unit] =
    document.keySet.asScala.find(!fields.contains(_)).fold(().validNel)(field => InvalidField(field).invalidNel)

  private def readLocation(document: Document): ValidatedNel[StoredDocumentError, Location] =
    (requiredString(document, "country").toValidatedNel, requiredString(document, "city").toValidatedNel, requiredBoolean(document, "remote").toValidatedNel).mapN(Location(_, _, _))

  private def readUserProfile(document: Document): ValidatedNel[StoredDocumentError, Option[UserProfile]] =
    optionalDocument(document, "profile").toValidatedNel.andThen(_.traverse(readProfile))

  private[mongo] def profile(value: UserProfile): Document = value match {
    case UserProfile.Candidate(profile) =>
      MongoHiringPersistenceCodecs.profile(StoredProfile(
        "Candidate", Some(profile.skills.toList.sorted), profile.experienceSummary, profile.resumeRef, None, None
      ))
    case UserProfile.Recruiter(profile) =>
      MongoHiringPersistenceCodecs.profile(StoredProfile(
        "Recruiter", None, None, None, Some(profile.organizationName), profile.jobTitle
      ))
  }

  private def readProfile(document: Document): ValidatedNel[StoredDocumentError, UserProfile] =
    optionalString(document, "kind").toValidatedNel.andThen {
      case Some("Candidate") => readCandidateProfile(document).map(UserProfile.Candidate.apply)
      case Some("Recruiter") => readRecruiterProfile(document).map(UserProfile.Recruiter.apply)
      case Some(_) => InvalidField("profile.kind").invalidNel
      // Profile documents written before the discriminator were candidate profiles.
      case None => readCandidateProfile(document).map(UserProfile.Candidate.apply)
    }

  private def readCandidateProfile(document: Document): ValidatedNel[StoredDocumentError, CandidateProfile] =
    (stringList(document, "skills").map(_.toSet), optionalString(document, "experienceSummary").toValidatedNel, optionalString(document, "resumeRef").toValidatedNel).mapN(CandidateProfile(_, _, _))

  private def readRecruiterProfile(document: Document): ValidatedNel[StoredDocumentError, RecruiterProfile] =
    (requiredString(document, "organizationName").toValidatedNel, optionalString(document, "jobTitle").toValidatedNel).mapN(RecruiterProfile(_, _))

  def embeddingDocument(embedding: EntityEmbedding): Document =
    MongoHiringPersistenceCodecs.embedding(StoredEmbeddingFields(
      embedding.values.map(_.toDouble), StoredEmbeddingMeta(embedding.meta.model, embedding.meta.sourceHash, Date.from(embedding.meta.updatedAt))
    ))

  private def readEmbedding(document: Document): ValidatedNel[StoredDocumentError, Option[EntityEmbedding]] =
    (optionalNumberList(document, "embedding"), optionalDocument(document, "embeddingMeta").toValidatedNel).mapN((values, meta) => (values, meta)).andThen {
      case (None, None) => None.validNel
      case (Some(values), Some(meta)) =>
        (requiredString(meta, "model").toValidatedNel, requiredString(meta, "sourceHash").toValidatedNel, instant(meta, "updatedAt").toValidatedNel).mapN {
          (model, sourceHash, updatedAt) => Some(EntityEmbedding(values.map(_.floatValue), EmbeddingMeta(model, sourceHash, updatedAt)))
        }
      case _ => InconsistentDocument.invalidNel
    }

  private def uuid(document: Document, field: String): Either[StoredDocumentError, UUID] = requiredString(document, field).flatMap(value => attempt(field)(UUID.fromString(value)))
  private def instant(document: Document, field: String): Either[StoredDocumentError, Instant] = requiredDate(document, field).map(_.toInstant)
  private def optionalInstant(document: Document, field: String): Either[StoredDocumentError, Option[Instant]] = optionalDate(document, field).map(_.map(_.toInstant))
  private def enumValue[A](document: Document, field: String, values: Array[A]): Either[StoredDocumentError, A] = requiredString(document, field).flatMap(value => values.find(_.toString == value).toRight(InvalidField(field)))
  private def optionalEnum[A](document: Document, field: String, values: Array[A]): Either[StoredDocumentError, Option[A]] = optionalString(document, field).flatMap(_.traverse(value => values.find(_.toString == value).toRight(InvalidField(field))))

  private def requiredString(document: Document, field: String): Either[StoredDocumentError, String] = Option(document.get(field)) match {
    case None => Left(MissingField(field)); case Some(value: String) => Right(value); case Some(_) => Left(InvalidField(field))
  }
  private def optionalString(document: Document, field: String): Either[StoredDocumentError, Option[String]] = Option(document.get(field)) match {
    case None => Right(None); case Some(value: String) => Right(Some(value)); case Some(_) => Left(InvalidField(field))
  }
  private def requiredDate(document: Document, field: String): Either[StoredDocumentError, Date] = Option(document.get(field)) match {
    case None => Left(MissingField(field)); case Some(value: Date) => Right(value); case Some(_) => Left(InvalidField(field))
  }
  private def optionalDate(document: Document, field: String): Either[StoredDocumentError, Option[Date]] = Option(document.get(field)) match {
    case None => Right(None); case Some(value: Date) => Right(Some(value)); case Some(_) => Left(InvalidField(field))
  }
  private def requiredBoolean(document: Document, field: String): Either[StoredDocumentError, Boolean] = Option(document.get(field)) match {
    case Some(value: java.lang.Boolean) => Right(value.booleanValue); case None => Left(MissingField(field)); case _ => Left(InvalidField(field))
  }
  private def requiredInt(document: Document, field: String): Either[StoredDocumentError, Int] = Option(document.get(field)) match {
    case Some(value: Number) => Right(value.intValue); case None => Left(MissingField(field)); case _ => Left(InvalidField(field))
  }
  private def nestedDocument(document: Document, field: String): Either[StoredDocumentError, Document] = optionalDocument(document, field).flatMap(_.toRight(MissingField(field)))
  private def optionalDocument(document: Document, field: String): Either[StoredDocumentError, Option[Document]] = Option(document.get(field)) match {
    case None => Right(None); case Some(value: Document) => Right(Some(value)); case Some(_) => Left(InvalidField(field))
  }
  private def stringList(document: Document, field: String): ValidatedNel[StoredDocumentError, List[String]] = Option(document.get(field)) match {
    case Some(values: java.util.List[?]) => values.asScala.toList.traverse { case value: String => value.validNel; case _ => InvalidField(field).invalidNel }
    case None => MissingField(field).invalidNel; case _ => InvalidField(field).invalidNel
  }
  private def optionalNumberList(document: Document, field: String): ValidatedNel[StoredDocumentError, Option[List[Number]]] = Option(document.get(field)) match {
    case None => None.validNel
    case Some(values: java.util.List[?]) => values.asScala.toList.traverse { case value: Number => value.validNel; case _ => InvalidField(field).invalidNel }.map(Some(_))
    case _ => InvalidField(field).invalidNel
  }
  private def resultList(document: Document, field: String): ValidatedNel[StoredDocumentError, List[SearchSessionResult]] = Option(document.get(field)) match {
    case Some(values: java.util.List[?]) =>
      values.asScala.toList.traverse {
        case value: Document =>
          (requiredString(value, "resultId").toValidatedNel, requiredInt(value, "rank").toValidatedNel, requiredNumber(value, "score").toValidatedNel)
            .mapN((id, rank, score) => SearchSessionResult(id, rank, score.doubleValue))
        case _ => InvalidField(field).invalidNel
      }
    case None => MissingField(field).invalidNel
    case _ => InvalidField(field).invalidNel
  }
  private def requiredNumber(document: Document, field: String): Either[StoredDocumentError, Number] = Option(document.get(field)) match {
    case Some(value: Number) => Right(value); case None => Left(MissingField(field)); case _ => Left(InvalidField(field))
  }
  private def json(field: String)(value: String): Either[StoredDocumentError, Json] =
    parse(value).leftMap(_ => InvalidField(field))
  private def attempt[A](field: String)(value: => A): Either[StoredDocumentError, A] = try Right(value) catch { case NonFatal(_) => Left(InvalidField(field)) }

  private def storedUser(value: User, passwordHash: Option[String]): StoredUser = {
    val emailCanonical = value.email.map(AccountName.canonical)
    val embedding = value.embedding.map(_.values.map(_.toDouble))
    val embeddingMeta = value.embedding.map(embedding => StoredEmbeddingMeta(
      embedding.meta.model, embedding.meta.sourceHash, Date.from(embedding.meta.updatedAt)
    ))
    StoredUser(
      value.id.value.toString,
      value.email,
      emailCanonical,
      value.name,
      AccountName.canonical(value.name),
      value.role.toString,
      value.profile.map(profile => profile match {
        case UserProfile.Candidate(candidate) => StoredProfile(
          "Candidate", Some(candidate.skills.toList.sorted), candidate.experienceSummary, candidate.resumeRef, None, None
        )
        case UserProfile.Recruiter(recruiter) => StoredProfile(
          "Recruiter", None, None, None, Some(recruiter.organizationName), recruiter.jobTitle
        )
      }),
      Date.from(value.createdAt),
      value.accountStatus.toString,
      value.deletedAt.map(Date.from),
      Option.when(value.role == UserRole.Admin && value.adminSingleton)("singleton-admin"),
      passwordHash,
      embedding,
      embeddingMeta
    )
  }

  private def storedJob(value: Job): StoredJob = {
    val embedding = value.embedding.map(_.values.map(_.toDouble))
    val embeddingMeta = value.embedding.map(embedding => StoredEmbeddingMeta(
      embedding.meta.model, embedding.meta.sourceHash, Date.from(embedding.meta.updatedAt)
    ))
    StoredJob(
      value.id.value.toString,
      value.recruiterId.value.toString,
      value.title,
      value.description,
      value.requirements,
      value.skills.toList.sorted,
      StoredLocation(value.location.country, value.location.city, value.location.remote),
      value.status.toString,
      Date.from(value.createdAt),
      Date.from(value.updatedAt),
      value.closedAt.map(Date.from),
      embedding,
      embeddingMeta
    )
  }

  private def storedOperationalEvent(value: OperationalEventEnvelope): StoredOperationalEvent =
    StoredOperationalEvent(
      value.eventId.toString,
      OperationalEventEnvelope.Topic,
      value.eventType.toString,
      Date.from(value.occurredAt),
      value.aggregateType.toString,
      value.aggregateId,
      value.actorId.value.toString,
      value.payload.noSpaces,
      OperationalEventJson.bytes(value),
      value.partitionKey
    )
}
