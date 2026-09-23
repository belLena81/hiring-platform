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
    decode(document, UserFields)(MongoHiringPersistenceCodecs.decodeUser).andThen(readUser)

  private def readUser(value: StoredUser): ValidatedNel[StoredDocumentError, User] =
    (
      uuid("_id", value._id).toValidatedNel.map(UserId.apply),
      value.email.validNel,
      value.name.validNel,
      enumValue("role", value.role, UserRole.values).toValidatedNel,
      readUserProfile(value.profile),
      value.createdAt.toInstant.validNel,
      enumValue("accountStatus", value.accountStatus, AccountStatus.values).toValidatedNel,
      value.deletedAt.map(_.toInstant).validNel,
      value.adminSingletonKey.map(_.contains("singleton-admin")).getOrElse(false).validNel,
      readEmbedding(value.embedding, value.embeddingMeta)
    ).mapN { (id, email, name, role, profile, createdAt, accountStatus, deletedAt, adminSingleton, embedding) =>
      User(id, email, name, role, profile, createdAt, adminSingleton, embedding, accountStatus, deletedAt)
    }.andThen { user =>
      Either.cond(user.roleProfileIsValid, user, InconsistentDocument).toValidatedNel
    }

  def readCredentials(document: Document): ValidatedNel[StoredDocumentError, Option[AccountCredentials]] =
    decode(document, UserFields)(MongoHiringPersistenceCodecs.decodeUser).andThen { value =>
      value.passwordHash.fold(None.validNel)(hash => readUser(value).map(AccountCredentials(_, hash).some))
    }

  def job(job: Job): Document = MongoHiringPersistenceCodecs.job(storedJob(job))

  def readJob(document: Document): ValidatedNel[StoredDocumentError, Job] =
    decode(document, JobFields)(MongoHiringPersistenceCodecs.decodeJob).andThen(readJob)

  private def readJob(value: StoredJob): ValidatedNel[StoredDocumentError, Job] =
    (
      uuid("_id", value._id).toValidatedNel.map(JobId.apply),
      uuid("recruiterId", value.recruiterId).toValidatedNel.map(UserId.apply),
      value.title.validNel,
      value.description.validNel,
      value.requirements.validNel,
      value.skills.toSet.validNel,
      Location(value.location.country, value.location.city, value.location.remote).validNel,
      enumValue("status", value.status, JobStatus.values).toValidatedNel,
      value.createdAt.toInstant.validNel,
      value.updatedAt.toInstant.validNel,
      value.closedAt.map(_.toInstant).validNel,
      readEmbedding(value.embedding, value.embeddingMeta)
    ).mapN(Job.apply)

  def application(application: Application): Document =
    MongoHiringPersistenceCodecs.application(
      StoredApplication(
        application.id.value.toString,
        application.candidateId.value.toString,
        application.jobId.value.toString,
        application.status.toString,
        Date.from(application.createdAt),
        Date.from(application.updatedAt)
      )
    )

  def readApplication(document: Document): ValidatedNel[StoredDocumentError, Application] =
    decode(document, ApplicationFields)(MongoHiringPersistenceCodecs.decodeApplication).andThen { value =>
      (
        uuid("_id", value._id).toValidatedNel.map(ApplicationId.apply),
        uuid("candidateId", value.candidateId).toValidatedNel.map(UserId.apply),
        uuid("jobId", value.jobId).toValidatedNel.map(JobId.apply),
        enumValue("status", value.status, ApplicationStatus.values).toValidatedNel,
        value.createdAt.toInstant.validNel,
        value.updatedAt.toInstant.validNel
      ).mapN(Application.apply)
    }

  def event(event: ApplicationEvent): Document =
    MongoHiringPersistenceCodecs.applicationEvent(
      StoredApplicationEvent(
        event.id.value.toString,
        event.applicationId.value.toString,
        event.previousStatus.map(_.toString),
        event.newStatus.toString,
        event.actorId.value.toString,
        Date.from(event.occurredAt),
        event.feedback,
        event.reason
      )
    )

  def readEvent(document: Document): ValidatedNel[StoredDocumentError, ApplicationEvent] =
    decode(document, ApplicationEventFields)(MongoHiringPersistenceCodecs.decodeApplicationEvent).andThen { value =>
      (
        uuid("_id", value._id).toValidatedNel.map(ApplicationEventId.apply),
        uuid("applicationId", value.applicationId).toValidatedNel.map(ApplicationId.apply),
        optionalEnum("previousStatus", value.previousStatus, ApplicationStatus.values).toValidatedNel,
        enumValue("newStatus", value.newStatus, ApplicationStatus.values).toValidatedNel,
        uuid("actorId", value.actorId).toValidatedNel.map(UserId.apply),
        value.occurredAt.toInstant.validNel,
        value.feedback.validNel,
        value.reason.validNel
      ).mapN(ApplicationEvent.apply)
    }

  def operationalEvent(value: OperationalEventEnvelope): Document =
    MongoHiringPersistenceCodecs.operationalEvent(storedOperationalEvent(value))

  def readOperationalEvent(document: Document): ValidatedNel[StoredDocumentError, OperationalEventEnvelope] = {
    val fields = if (isOutbox(document)) OutboxFields else OperationalEventFields
    decode(document, fields)(MongoHiringPersistenceCodecs.decodeOperationalEvent).andThen(readOperationalEvent)
  }

  private def readOperationalEvent(
      value: StoredOperationalEvent
  ): ValidatedNel[StoredDocumentError, OperationalEventEnvelope] =
    (
      uuid("_id", value._id).toValidatedNel,
      enumValue("eventType", value.eventType, OperationalEventType.values).toValidatedNel,
      value.occurredAt.toInstant.validNel,
      enumValue("aggregateType", value.aggregateType, OperationalAggregateType.values).toValidatedNel,
      value.aggregateId.validNel,
      uuid("actorId", value.actorId).toValidatedNel.map(UserId.apply),
      parseJson("payload", value.payload).toValidatedNel
    ).mapN(OperationalEventEnvelope.apply)

  def outboxRecord(value: OperationalEventEnvelope, now: Instant): Document = {
    val event = storedOperationalEvent(value)
    MongoHiringPersistenceCodecs.outbox(
      StoredOutboxRecord(
        event._id,
        event.topic,
        event.eventType,
        event.occurredAt,
        event.aggregateType,
        event.aggregateId,
        event.actorId,
        event.payload,
        event.envelopeBytes,
        event.partitionKey,
        "Retryable",
        0,
        Date.from(now),
        None,
        None,
        Date.from(now),
        Date.from(now)
      )
    )
  }

  def searchSession(value: SearchSession): Document =
    MongoHiringPersistenceCodecs.searchSession(
      StoredSearchSession(
        value.id.toString,
        value.actorId.value.toString,
        value.searchKind,
        value.query,
        value.filter.noSpaces,
        value.model,
        value.results.map(result => StoredSearchSessionResult(result.resultId, result.rank, result.score)),
        Date.from(value.occurredAt),
        Date.from(value.expiresAt)
      )
    )

  def readSearchSession(document: Document): ValidatedNel[StoredDocumentError, SearchSession] =
    decode(document, SearchSessionFields)(MongoHiringPersistenceCodecs.decodeSearchSession).andThen { value =>
      (
        uuid("_id", value._id).toValidatedNel,
        uuid("actorId", value.actorId).toValidatedNel.map(UserId.apply),
        value.searchKind.validNel,
        value.query.validNel,
        parseJson("filter", value.filter).toValidatedNel,
        value.model.validNel,
        value.results.map(result => SearchSessionResult(result.resultId, result.rank, result.score)).validNel,
        value.occurredAt.toInstant.validNel,
        value.expiresAt.toInstant.validNel
      ).mapN(SearchSession.apply)
    }

  private val UserFields = Set(
    "_id",
    "email",
    "emailCanonical",
    "name",
    "nameCanonical",
    "role",
    "profile",
    "createdAt",
    "updatedAt",
    "accountStatus",
    "deletedAt",
    "adminSingletonKey",
    "passwordHash",
    "embedding",
    "embeddingMeta"
  )
  private val JobFields = Set(
    "_id",
    "recruiterId",
    "title",
    "description",
    "requirements",
    "skills",
    "location",
    "status",
    "createdAt",
    "updatedAt",
    "closedAt",
    "embedding",
    "embeddingMeta"
  )
  private val ApplicationFields = Set("_id", "candidateId", "jobId", "status", "createdAt", "updatedAt")
  private val ApplicationEventFields =
    Set("_id", "applicationId", "previousStatus", "newStatus", "actorId", "occurredAt", "feedback", "reason")
  private val OperationalEventFields = Set(
    "_id",
    "topic",
    "eventType",
    "occurredAt",
    "aggregateType",
    "aggregateId",
    "actorId",
    "payload",
    "envelopeBytes",
    "partitionKey"
  )
  private val OutboxFields = OperationalEventFields ++ Set(
    "state",
    "attempts",
    "availableAt",
    "leaseOwner",
    "leaseToken",
    "createdAt",
    "updatedAt"
  )
  private val SearchSessionFields =
    Set("_id", "actorId", "searchKind", "query", "filter", "model", "results", "occurredAt", "expiresAt")

  private def decode[A](document: Document, fields: Set[String])(
      decoder: Document => Either[Throwable, A]
  ): ValidatedNel[StoredDocumentError, A] =
    noUnexpectedFields(document, fields).andThen(_ => decoder(document).leftMap(codecError).toValidatedNel)

  private def codecError(error: Throwable): StoredDocumentError = {
    def missingField(current: Throwable): Option[String] =
      Option(current).flatMap { value =>
        Option(value.getMessage)
          .collect {
            case message if message.startsWith("Missing field: ") => message.stripPrefix("Missing field: ")
          }
          .orElse(Option(value.getCause).flatMap(missingField))
      }

    missingField(error).map(MissingField.apply).getOrElse(InvalidField("document"))
  }

  private def noUnexpectedFields(document: Document, fields: Set[String]): ValidatedNel[StoredDocumentError, Unit] =
    document.keySet.asScala.find(!fields.contains(_)).fold(().validNel)(field => InvalidField(field).invalidNel)

  private def readUserProfile(profile: Option[StoredProfile]): ValidatedNel[StoredDocumentError, Option[UserProfile]] =
    profile.traverse(readProfile)

  private[mongo] def profile(value: UserProfile): Document = value match {
    case UserProfile.Candidate(profile) =>
      MongoHiringPersistenceCodecs.profile(
        StoredProfile(
          "Candidate",
          Some(profile.skills.toList.sorted),
          profile.experienceSummary,
          profile.resumeRef,
          None,
          None
        )
      )
    case UserProfile.Recruiter(profile) =>
      MongoHiringPersistenceCodecs.profile(
        StoredProfile(
          "Recruiter",
          None,
          None,
          None,
          Some(profile.organizationName),
          profile.jobTitle
        )
      )
  }

  private def readProfile(profile: StoredProfile): ValidatedNel[StoredDocumentError, UserProfile] =
    profile.kind match {
      case "Candidate" | "" =>
        (
          required(profile.skills, "profile.skills").map(_.toSet),
          profile.experienceSummary.validNel,
          profile.resumeRef.validNel
        ).mapN(CandidateProfile.apply).map(UserProfile.Candidate.apply)
      case "Recruiter" =>
        (
          required(profile.organizationName, "profile.organizationName"),
          profile.jobTitle.validNel
        ).mapN(RecruiterProfile.apply).map(UserProfile.Recruiter.apply)
      case _ => InvalidField("profile.kind").invalidNel
    }

  private def required[A](value: Option[A], field: String): ValidatedNel[StoredDocumentError, A] =
    value.fold(MissingField(field).invalidNel)(_.validNel)

  private def readEmbedding(
      values: Option[List[Double]],
      meta: Option[StoredEmbeddingMeta]
  ): ValidatedNel[StoredDocumentError, Option[EntityEmbedding]] =
    (values, meta) match {
      case (None, None)                 => None.validNel
      case (Some(numbers), Some(value)) =>
        Some(
          EntityEmbedding(
            numbers.map(_.toFloat),
            EmbeddingMeta(value.model, value.sourceHash, value.updatedAt.toInstant)
          )
        ).validNel
      case _ => InconsistentDocument.invalidNel
    }

  def embeddingDocument(embedding: EntityEmbedding): Document =
    MongoHiringPersistenceCodecs.embedding(
      StoredEmbeddingFields(
        embedding.values.map(_.toDouble),
        StoredEmbeddingMeta(embedding.meta.model, embedding.meta.sourceHash, Date.from(embedding.meta.updatedAt))
      )
    )

  private def isOutbox(document: Document): Boolean =
    document.keySet.asScala.exists(Set("state", "attempts", "availableAt", "leaseOwner", "leaseToken").contains)

  private def uuid(field: String, value: String): Either[StoredDocumentError, UUID] =
    attempt(field)(UUID.fromString(value))

  private def enumValue[A](field: String, value: String, values: Array[A]): Either[StoredDocumentError, A] =
    values.find(_.toString == value).toRight(InvalidField(field))

  private def optionalEnum[A](
      field: String,
      value: Option[String],
      values: Array[A]
  ): Either[StoredDocumentError, Option[A]] =
    value.traverse(enumValue(field, _, values))

  private def parseJson(field: String, value: String): Either[StoredDocumentError, Json] =
    parse(value).leftMap(_ => InvalidField(field))

  private def attempt[A](field: String)(value: => A): Either[StoredDocumentError, A] =
    try Right(value)
    catch { case NonFatal(_) => Left(InvalidField(field)) }

  private def storedUser(value: User, passwordHash: Option[String]): StoredUser = {
    val emailCanonical = value.email.map(AccountName.canonical)
    val embedding = value.embedding.map(_.values.map(_.toDouble))
    val embeddingMeta = value.embedding.map(embedding =>
      StoredEmbeddingMeta(
        embedding.meta.model,
        embedding.meta.sourceHash,
        Date.from(embedding.meta.updatedAt)
      )
    )
    StoredUser(
      value.id.value.toString,
      value.email,
      emailCanonical,
      value.name,
      AccountName.canonical(value.name),
      value.role.toString,
      value.profile.map {
        case UserProfile.Candidate(candidate) =>
          StoredProfile(
            "Candidate",
            Some(candidate.skills.toList.sorted),
            candidate.experienceSummary,
            candidate.resumeRef,
            None,
            None
          )
        case UserProfile.Recruiter(recruiter) =>
          StoredProfile(
            "Recruiter",
            None,
            None,
            None,
            Some(recruiter.organizationName),
            recruiter.jobTitle
          )
      },
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
    val embeddingMeta = value.embedding.map(embedding =>
      StoredEmbeddingMeta(
        embedding.meta.model,
        embedding.meta.sourceHash,
        Date.from(embedding.meta.updatedAt)
      )
    )
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
