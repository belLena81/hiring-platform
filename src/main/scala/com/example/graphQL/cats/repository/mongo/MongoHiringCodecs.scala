package com.example.graphQL.cats.repository.mongo

import cats.data.ValidatedNel
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.shared.events.*
import io.circe.Json
import io.circe.parser.parse
import org.bson.Document

import java.time.Instant
import java.util.{Date, UUID}
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** BSON decoding is total because persisted values are untrusted input, including values from older binaries. */
private[mongo] object MongoHiringCodecs {
  enum StoredDocumentError {
    case MissingField(field: String)
    case InvalidField(field: String)
    case InconsistentDocument
  }

  import StoredDocumentError.*

  def userWithPassword(value: User, passwordHash: String): Document = user(value).append("passwordHash", passwordHash)

  def user(value: User): Document = {
    val document = new Document("_id", value.id.value.toString)
      .append("schemaVersion", 3).append("name", value.name).append("nameCanonical", AccountName.canonical(value.name))
      .append("role", value.role.toString).append("accountStatus", value.accountStatus.toString)
      .append("version", java.lang.Long.valueOf(value.version)).append("createdAt", Date.from(value.createdAt))
    value.email.foreach(email => document.append("email", email).append("emailCanonical", AccountName.canonical(email)))
    if (value.role == UserRole.Admin && value.adminSingleton) {
      document.append("adminSingletonKey", "singleton-admin")
      ()
    }
    value.profile.foreach(profileValue => document.append("profile", profile(profileValue)))
    value.deletedAt.foreach(deletedAt => document.append("deletedAt", Date.from(deletedAt)))
    appendOptionalEmbedding(document, value.embedding)
  }

  def readUser(document: Document): ValidatedNel[StoredDocumentError, User] =
    (
      uuid(document, "_id").toValidatedNel.map(UserId.apply),
      optionalString(document, "email").toValidatedNel,
      requiredString(document, "name").toValidatedNel,
      enumValue(document, "role", UserRole.values).toValidatedNel,
      readUserProfile(document),
      instant(document, "createdAt").toValidatedNel,
      optionalEnum(document, "accountStatus", AccountStatus.values).toValidatedNel.map(_.getOrElse(AccountStatus.Active)),
      optionalInstant(document, "deletedAt").toValidatedNel,
      optionalLong(document, "version").toValidatedNel.map(_.getOrElse(0L)),
      optionalString(document, "adminSingletonKey").toValidatedNel.map(_.contains("singleton-admin")),
      readEmbedding(document)
    ).mapN { (id, email, name, role, profile, createdAt, accountStatus, deletedAt, version, adminSingleton, embedding) =>
      User(id, email, name, role, profile, createdAt, adminSingleton, embedding, accountStatus, deletedAt, version)
    }.andThen { user =>
      Either.cond(user.roleProfileIsValid, user, InconsistentDocument).toValidatedNel
    }

  def readCredentials(document: Document): ValidatedNel[StoredDocumentError, Option[AccountCredentials]] =
    optionalString(document, "passwordHash").toValidatedNel.andThen(_.traverse(hash => readUser(document).map(AccountCredentials(_, hash))))

  def job(job: Job): Document =
    appendOptionalEmbedding(
      appendOptionalDate(new Document("_id", job.id.value.toString).append("schemaVersion", 1)
        .append("version", java.lang.Long.valueOf(job.version)).append("recruiterId", job.recruiterId.value.toString)
        .append("title", job.title).append("description", job.description).append("requirements", job.requirements.asJava)
        .append("skills", job.skills.toList.sorted.asJava).append("location", location(job.location)).append("status", job.status.toString)
        .append("createdAt", Date.from(job.createdAt)).append("updatedAt", Date.from(job.updatedAt)), "closedAt", job.closedAt),
      job.embedding
    )

  def readJob(document: Document): ValidatedNel[StoredDocumentError, Job] =
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
      optionalLong(document, "version").toValidatedNel.map(_.getOrElse(0L)),
      readEmbedding(document)
    ).mapN(Job.apply)

  def application(application: Application): Document =
    new Document("_id", application.id.value.toString).append("schemaVersion", 1).append("candidateId", application.candidateId.value.toString)
      .append("jobId", application.jobId.value.toString).append("status", application.status.toString)
      .append("createdAt", Date.from(application.createdAt)).append("updatedAt", Date.from(application.updatedAt))
      .append("version", java.lang.Long.valueOf(application.version))

  def readApplication(document: Document): ValidatedNel[StoredDocumentError, Application] =
    (
      uuid(document, "_id").toValidatedNel.map(ApplicationId.apply),
      uuid(document, "candidateId").toValidatedNel.map(UserId.apply),
      uuid(document, "jobId").toValidatedNel.map(JobId.apply),
      enumValue(document, "status", ApplicationStatus.values).toValidatedNel,
      instant(document, "createdAt").toValidatedNel,
      instant(document, "updatedAt").toValidatedNel,
      optionalLong(document, "version").toValidatedNel.map(_.getOrElse(0L))
    ).mapN(Application.apply)

  def event(event: ApplicationEvent): Document =
    appendOptionalString(appendOptionalString(appendOptionalString(new Document("_id", event.id.value.toString)
      .append("schemaVersion", 1).append("applicationId", event.applicationId.value.toString).append("newStatus", event.newStatus.toString)
      .append("actorId", event.actorId.value.toString).append("occurredAt", Date.from(event.occurredAt)), "previousStatus", event.previousStatus.map(_.toString)), "feedback", event.feedback), "reason", event.reason)

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
    new Document("_id", value.eventId.toString)
      .append("schemaVersion", value.schemaVersion)
      .append("topic", OperationalEventEnvelope.Topic)
      .append("eventType", value.eventType.toString)
      .append("occurredAt", Date.from(value.occurredAt))
      .append("aggregateType", value.aggregateType.toString)
      .append("aggregateId", value.aggregateId)
      .append("aggregateVersion", java.lang.Long.valueOf(value.aggregateVersion))
      .append("sequence", java.lang.Long.valueOf(value.sequence))
      .append("actorId", value.actorId.value.toString)
      .append("payload", value.payload.noSpaces)
      .append("envelopeBytes", OperationalEventJson.bytes(value))
      .append("partitionKey", value.partitionKey)

  def readOperationalEvent(document: Document): ValidatedNel[StoredDocumentError, OperationalEventEnvelope] =
    (
      uuid(document, "_id").toValidatedNel,
      enumValue(document, "eventType", OperationalEventType.values).toValidatedNel,
      requiredInt(document, "schemaVersion").toValidatedNel.andThen { schemaVersion =>
        Either.cond(schemaVersion == OperationalEventEnvelope.SchemaVersion, schemaVersion, InvalidField("schemaVersion")).toValidatedNel
      },
      instant(document, "occurredAt").toValidatedNel,
      enumValue(document, "aggregateType", OperationalAggregateType.values).toValidatedNel,
      requiredString(document, "aggregateId").toValidatedNel,
      requiredLong(document, "aggregateVersion").toValidatedNel,
      requiredLong(document, "sequence").toValidatedNel,
      uuid(document, "actorId").toValidatedNel.map(UserId.apply),
      requiredString(document, "payload").toValidatedNel.andThen(value => json("payload")(value).toValidatedNel)
    ).mapN(OperationalEventEnvelope.apply)

  def outboxRecord(value: OperationalEventEnvelope, now: Instant): Document =
    operationalEvent(value)
      .append("state", "Retryable")
      .append("attempts", java.lang.Integer.valueOf(0))
      .append("availableAt", Date.from(now))
      .append("leaseOwner", null)
      .append("leaseToken", null)
      .append("createdAt", Date.from(now))
      .append("updatedAt", Date.from(now))

  def searchSession(value: SearchSession): Document =
    new Document("_id", value.id.toString)
      .append("actorId", value.actorId.value.toString)
      .append("searchKind", value.searchKind)
      .append("query", value.query.orNull)
      .append("filter", value.filter.noSpaces)
      .append("model", value.model.orNull)
      .append("modelVersion", value.modelVersion.map(Int.box).orNull)
      .append("results", value.results.map(result =>
        new Document("resultId", result.resultId)
          .append("rank", java.lang.Integer.valueOf(result.rank))
          .append("score", java.lang.Double.valueOf(result.score))
      ).asJava)
      .append("occurredAt", Date.from(value.occurredAt))
      .append("expiresAt", Date.from(value.expiresAt))

  def readSearchSession(document: Document): ValidatedNel[StoredDocumentError, SearchSession] =
    (
      uuid(document, "_id").toValidatedNel,
      uuid(document, "actorId").toValidatedNel.map(UserId.apply),
      requiredString(document, "searchKind").toValidatedNel,
      optionalString(document, "query").toValidatedNel,
      requiredString(document, "filter").toValidatedNel.andThen(value => json("filter")(value).toValidatedNel),
      optionalString(document, "model").toValidatedNel,
      optionalInt(document, "modelVersion").toValidatedNel,
      resultList(document, "results"),
      instant(document, "occurredAt").toValidatedNel,
      instant(document, "expiresAt").toValidatedNel
    ).mapN(SearchSession.apply)

  private def location(location: Location): Document = new Document("country", location.country).append("city", location.city).append("remote", location.remote)

  private def readLocation(document: Document): ValidatedNel[StoredDocumentError, Location] =
    (requiredString(document, "country").toValidatedNel, requiredString(document, "city").toValidatedNel, requiredBoolean(document, "remote").toValidatedNel).mapN(Location(_, _, _))

  private def readUserProfile(document: Document): ValidatedNel[StoredDocumentError, Option[UserProfile]] = {
    val current = optionalDocument(document, "profile").toValidatedNel.andThen(_.traverse(readProfile))
    val legacyRecruiter = optionalDocument(document, "recruiterProfile").toValidatedNel.andThen(_.traverse(readRecruiterProfile))
    (current, legacyRecruiter).mapN {
      case (Some(profileValue), None) => Right(Some(profileValue))
      case (None, Some(profileValue)) => Right(Some(UserProfile.Recruiter(profileValue)))
      case (None, None) => Right(None)
      case (Some(_), Some(_)) => Left(InconsistentDocument)
    }.andThen(_.toValidatedNel)
  }

  private[mongo] def profile(profile: UserProfile): Document = profile match {
    case UserProfile.Candidate(value) => appendOptionalString(appendOptionalString(new Document("kind", "Candidate").append("skills", value.skills.toList.sorted.asJava), "experienceSummary", value.experienceSummary), "resumeRef", value.resumeRef)
    case UserProfile.Recruiter(value) => appendOptionalString(new Document("kind", "Recruiter").append("organizationName", value.organizationName), "jobTitle", value.jobTitle)
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
    new Document("embedding", embedding.values.map(float => java.lang.Double.valueOf(float.toDouble)).asJava).append("embeddingMeta", embeddingMeta(embedding.meta))

  private def embeddingMeta(meta: EmbeddingMeta): Document =
    new Document("model", meta.model).append("version", java.lang.Integer.valueOf(meta.version)).append("sourceHash", meta.sourceHash).append("updatedAt", Date.from(meta.updatedAt))

  private def readEmbedding(document: Document): ValidatedNel[StoredDocumentError, Option[EntityEmbedding]] =
    (optionalNumberList(document, "embedding"), optionalDocument(document, "embeddingMeta").toValidatedNel).mapN((values, meta) => (values, meta)).andThen {
      case (None, None) => None.validNel
      case (Some(values), Some(meta)) =>
        (requiredString(meta, "model").toValidatedNel, requiredInt(meta, "version").toValidatedNel, requiredString(meta, "sourceHash").toValidatedNel, instant(meta, "updatedAt").toValidatedNel).mapN {
          (model, version, sourceHash, updatedAt) => Some(EntityEmbedding(values.map(_.floatValue), EmbeddingMeta(model, version, sourceHash, updatedAt)))
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
  private def optionalLong(document: Document, field: String): Either[StoredDocumentError, Option[Long]] = Option(document.get(field)) match {
    case None => Right(None); case Some(value: Number) => Right(Some(value.longValue)); case Some(_) => Left(InvalidField(field))
  }
  private def requiredInt(document: Document, field: String): Either[StoredDocumentError, Int] = Option(document.get(field)) match {
    case Some(value: Number) => Right(value.intValue); case None => Left(MissingField(field)); case _ => Left(InvalidField(field))
  }
  private def optionalInt(document: Document, field: String): Either[StoredDocumentError, Option[Int]] = Option(document.get(field)) match {
    case None => Right(None); case Some(value: Number) => Right(Some(value.intValue)); case Some(_) => Left(InvalidField(field))
  }
  private def requiredLong(document: Document, field: String): Either[StoredDocumentError, Long] = Option(document.get(field)) match {
    case Some(value: Number) => Right(value.longValue); case None => Left(MissingField(field)); case _ => Left(InvalidField(field))
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

  private def appendOptionalString(document: Document, field: String, value: Option[String]): Document = { value.foreach(document.append(field, _)); document }
  private def appendOptionalDate(document: Document, field: String, value: Option[Instant]): Document = { value.foreach(instant => document.append(field, Date.from(instant))); document }
  private def appendOptionalEmbedding(document: Document, value: Option[EntityEmbedding]): Document = { value.foreach { embedding => document.append("embedding", embedding.values.map(float => java.lang.Double.valueOf(float.toDouble)).asJava); document.append("embeddingMeta", embeddingMeta(embedding.meta)) }; document }
}
