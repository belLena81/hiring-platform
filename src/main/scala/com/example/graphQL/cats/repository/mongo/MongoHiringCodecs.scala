package com.example.graphQL.cats.repository.mongo

import cats.data.ValidatedNel
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.service.events.*
import com.example.graphQL.cats.shared.Parsing
import com.example.graphQL.cats.service.port.Versioned
import MongoHiringPersistenceCodecs.*
import io.circe.Json
import io.circe.parser.parse
import org.bson.Document

import java.time.Instant
import java.util.{Date, UUID}
import scala.jdk.CollectionConverters.*

/** BSON decoding is total because persisted values are untrusted input. */
private[mongo] object MongoHiringCodecs {
  enum StoredDocumentError {
    case MissingField(field: String)
    case InvalidField(field: String)
    case InconsistentDocument
  }

  import StoredDocumentError.*

  def userWithPassword(value: User, passwordHash: PasswordHash, version: Long = 0L): Document =
    MongoHiringPersistenceCodecs.user(storedUser(value, Some(passwordHash.encoded), version))

  def user(value: User, version: Long = 0L): Document =
    MongoHiringPersistenceCodecs.user(storedUser(value, None, version))

  def readUser(document: Document): ValidatedNel[StoredDocumentError, User] =
    readVersionedUser(document).map(_.value)

  def readVersionedUser(document: Document): ValidatedNel[StoredDocumentError, Versioned[User]] =
    decode(document, UserFields)(MongoHiringPersistenceCodecs.decodeUser).andThen(readVersionedUser)

  private def readVersionedUser(value: StoredUser): ValidatedNel[StoredDocumentError, Versioned[User]] =
    (
      uuid(MongoFields.Id, value._id).toValidatedNel.map(UserId.apply),
      value.email.traverse(email =>
        EmailAddress.from(email).leftMap(_ => InvalidField(MongoFields.Email)).toValidatedNel
      ),
      value.name.validNel,
      enumValue(MongoFields.Role, value.role, UserRole.values).toValidatedNel,
      readUserProfile(value.profile),
      value.createdAt.toInstant.validNel,
      enumValue(MongoFields.AccountStatus, value.accountStatus, AccountStatus.values).toValidatedNel,
      value.deletedAt.map(_.toInstant).validNel,
      value.adminSingletonKey.map(_.contains("singleton-admin")).getOrElse(false).validNel,
      readEmbedding(value.embedding, value.embeddingMeta)
    ).mapN { (id, email, name, role, profile, createdAt, accountStatus, deletedAt, adminSingleton, embedding) =>
      Versioned(
        User(id, email, name, role, profile, createdAt, adminSingleton, embedding, accountStatus, deletedAt),
        value.version
      )
    }.andThen { user =>
      if (!user.value.roleProfileIsValid) InconsistentDocument.invalidNel
      else Either.cond(user.version >= 0L, user, InvalidField(MongoFields.Version)).toValidatedNel
    }

  def readCredentials(document: Document): ValidatedNel[StoredDocumentError, Option[AccountCredentials]] =
    decode(document, UserFields)(MongoHiringPersistenceCodecs.decodeUser).andThen { value =>
      value.passwordHash.fold(None.validNel)(hash =>
        readVersionedUser(value).map(v => AccountCredentials(v.value, PasswordHash.fromEncoded(hash)).some)
      )
    }

  def job(job: Job, version: Long = 0L): Document = MongoHiringPersistenceCodecs.job(storedJob(job, version))

  def readJob(document: Document): ValidatedNel[StoredDocumentError, Job] =
    readVersionedJob(document).map(_.value)

  def readVersionedJob(document: Document): ValidatedNel[StoredDocumentError, Versioned[Job]] =
    decode(document, JobFields)(MongoHiringPersistenceCodecs.decodeJob).andThen(readVersionedJob)

  private def readVersionedJob(value: StoredJob): ValidatedNel[StoredDocumentError, Versioned[Job]] =
    (
      uuid(MongoFields.Id, value._id).toValidatedNel.map(JobId.apply),
      uuid(MongoFields.RecruiterId, value.recruiterId).toValidatedNel.map(UserId.apply),
      value.title.validNel,
      value.description.validNel,
      value.requirements.validNel,
      value.skills.toSet.validNel,
      Location(value.location.country, value.location.city, value.location.remote).validNel,
      enumValue(MongoFields.Status, value.status, JobStatus.values).toValidatedNel,
      value.createdAt.toInstant.validNel,
      value.updatedAt.toInstant.validNel,
      value.closedAt.map(_.toInstant).validNel,
      readEmbedding(value.embedding, value.embeddingMeta)
    ).mapN(Job.apply).map(Versioned(_, value.version)).andThen { job =>
      Either.cond(job.version >= 0L, job, InvalidField(MongoFields.Version)).toValidatedNel
    }

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
        uuid(MongoFields.Id, value._id).toValidatedNel.map(ApplicationId.apply),
        uuid(MongoFields.CandidateId, value.candidateId).toValidatedNel.map(UserId.apply),
        uuid(MongoFields.JobId, value.jobId).toValidatedNel.map(JobId.apply),
        enumValue(MongoFields.Status, value.status, ApplicationStatus.values).toValidatedNel,
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
        uuid(MongoFields.Id, value._id).toValidatedNel.map(ApplicationEventId.apply),
        uuid(MongoFields.ApplicationId, value.applicationId).toValidatedNel.map(ApplicationId.apply),
        optionalEnum(MongoFields.PreviousStatus, value.previousStatus, ApplicationStatus.values).toValidatedNel,
        enumValue(MongoFields.NewStatus, value.newStatus, ApplicationStatus.values).toValidatedNel,
        uuid(MongoFields.ActorId, value.actorId).toValidatedNel.map(UserId.apply),
        value.occurredAt.toInstant.validNel,
        value.feedback.validNel,
        value.reason.validNel
      ).mapN(ApplicationEvent.apply)
    }

  def operationalEvent(value: OperationalEventEnvelope): Document =
    MongoHiringPersistenceCodecs.operationalEvent(storedOperationalEvent(value))

  def readOperationalEvent(document: Document): ValidatedNel[StoredDocumentError, OperationalEventEnvelope] = {
    val eventDocument = if (isOutbox(document)) projectOperationalEvent(document) else document
    val fields = if (isOutbox(document)) OutboxFields else OperationalEventFields
    noUnexpectedFields(document, fields)
      .andThen(_ => decode(eventDocument, OperationalEventFields)(MongoHiringPersistenceCodecs.decodeOperationalEvent))
      .andThen(readOperationalEvent)
  }

  private def projectOperationalEvent(document: Document): Document =
    val event = new Document()
    OperationalEventFields.foreach(field => Option(document.get(field)).foreach(value => event.put(field, value)))
    event

  private def readOperationalEvent(
      value: StoredOperationalEvent
  ): ValidatedNel[StoredDocumentError, OperationalEventEnvelope] =
    (
      uuid(MongoFields.Id, value._id).toValidatedNel,
      enumValue(MongoFields.EventType, value.eventType, OperationalEventType.values).toValidatedNel,
      value.occurredAt.toInstant.validNel,
      enumValue(MongoFields.AggregateType, value.aggregateType, OperationalAggregateType.values).toValidatedNel,
      value.aggregateId.validNel,
      uuid(MongoFields.ActorId, value.actorId).toValidatedNel.map(UserId.apply),
      parseJson(MongoFields.Payload, value.payload).toValidatedNel
    ).mapN(OperationalEventEnvelope.apply)

  def outboxRecord(value: OperationalEventEnvelope, now: Instant): Either[String, Document] =
    for {
      subjectIds <- outboxSubjectIds(value)
      document = outboxDocument(value, now, subjectIds)
    } yield document

  private def outboxDocument(value: OperationalEventEnvelope, now: Instant, subjectIds: List[String]): Document = {
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
        Some(subjectIds),
        Some(1),
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

  private def outboxSubjectIds(value: OperationalEventEnvelope): Either[String, List[String]] = {
    val payload = value.payload.hcursor
    def requiredUuid(field: String): Either[String, String] =
      payload.get[String](field).leftMap(_ => s"missing outbox subject field: $field").flatMap(parseSubjectId)

    def parseSubjectId(raw: String): Either[String, String] =
      Parsing.parseUuid(raw).leftMap(_ => "invalid outbox subject id").map(_.toString)

    val candidates: Either[String, List[String]] = value.eventType match {
      case OperationalEventType.APPLICATION_CREATED | OperationalEventType.APPLICATION_STATUS_CHANGED |
          OperationalEventType.CANDIDATE_HIRED =>
        requiredUuid(MongoFields.CandidateId).map(List(_))
      case OperationalEventType.SEARCH_PERFORMED =>
        payload.get[String](MongoFields.SearchKind).leftMap(_ => "search event has no search kind").flatMap {
          case "candidateMatches" =>
            payload
              .get[List[io.circe.Json]](MongoFields.Results)
              .leftMap(_ => "candidate search event has malformed results")
              .flatMap(
                _.traverse(_.hcursor.get[String](MongoFields.ResultId).leftMap(_ => "candidate result has no id"))
              )
              .flatMap(_.traverse(parseSubjectId))
          case "jobs" | "recommendedJobs" | "semanticJobSearch" => Right(Nil)
          case _                                                => Left("search event has an unknown search kind")
        }
      case OperationalEventType.SEARCH_RESULT_CLICKED =>
        payload.get[String](MongoFields.SearchKind).leftMap(_ => "search click has no search kind").flatMap {
          case "candidateMatches"                               => requiredUuid(MongoFields.ResultId).map(List(_))
          case "jobs" | "recommendedJobs" | "semanticJobSearch" => Right(Nil)
          case _                                                => Left("search click has an unknown search kind")
        }
      case _ => Right(Nil)
    }

    candidates.map(values => (value.actorId.value.toString :: values).distinct.sorted)
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
        uuid(MongoFields.Id, value._id).toValidatedNel,
        uuid(MongoFields.ActorId, value.actorId).toValidatedNel.map(UserId.apply),
        value.searchKind.validNel,
        value.query.validNel,
        parseJson(MongoFields.Filter, value.filter).toValidatedNel,
        value.model.validNel,
        value.results.map(result => SearchSessionResult(result.resultId, result.rank, result.score)).validNel,
        value.occurredAt.toInstant.validNel,
        value.expiresAt.toInstant.validNel
      ).mapN(SearchSession.apply)
    }

  private val UserFields = Set(
    MongoFields.Id,
    MongoFields.Version,
    MongoFields.Email,
    MongoFields.EmailCanonical,
    MongoFields.Name,
    MongoFields.NameCanonical,
    MongoFields.Role,
    MongoFields.Profile,
    MongoFields.CreatedAt,
    MongoFields.UpdatedAt,
    MongoFields.AccountStatus,
    MongoFields.DeletedAt,
    MongoFields.AdminSingletonKey,
    MongoFields.PasswordHash,
    MongoFields.Embedding,
    MongoFields.EmbeddingMeta
  )
  private val JobFields = Set(
    MongoFields.Id,
    MongoFields.Version,
    MongoFields.RecruiterId,
    MongoFields.Title,
    MongoFields.Description,
    MongoFields.Requirements,
    MongoFields.Skills,
    MongoFields.Location,
    MongoFields.Status,
    MongoFields.CreatedAt,
    MongoFields.UpdatedAt,
    MongoFields.ClosedAt,
    MongoFields.Embedding,
    MongoFields.EmbeddingMeta
  )
  private val ApplicationFields = Set(
    MongoFields.Id,
    MongoFields.CandidateId,
    MongoFields.JobId,
    MongoFields.Status,
    MongoFields.CreatedAt,
    MongoFields.UpdatedAt
  )
  private val ApplicationEventFields =
    Set(
      MongoFields.Id,
      MongoFields.ApplicationId,
      MongoFields.PreviousStatus,
      MongoFields.NewStatus,
      MongoFields.ActorId,
      MongoFields.OccurredAt,
      MongoFields.Feedback,
      MongoFields.Reason
    )
  private val OperationalEventFields = Set(
    MongoFields.Id,
    MongoFields.Topic,
    MongoFields.EventType,
    MongoFields.OccurredAt,
    MongoFields.AggregateType,
    MongoFields.AggregateId,
    MongoFields.ActorId,
    MongoFields.Payload,
    MongoFields.EnvelopeBytes,
    MongoFields.PartitionKey
  )
  private val OutboxFields = OperationalEventFields ++ Set(
    MongoFields.State,
    MongoFields.Attempts,
    MongoFields.AvailableAt,
    MongoFields.LeaseOwner,
    MongoFields.LeaseToken,
    MongoFields.LeaseUntil,
    MongoFields.CreatedAt,
    MongoFields.UpdatedAt,
    MongoFields.SubjectIds,
    MongoFields.SubjectRefsVersion
  )
  private val SearchSessionFields =
    Set(
      MongoFields.Id,
      MongoFields.ActorId,
      MongoFields.SearchKind,
      MongoFields.Query,
      MongoFields.Filter,
      MongoFields.Model,
      MongoFields.Results,
      MongoFields.OccurredAt,
      MongoFields.ExpiresAt
    )

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

    missingField(error).map(MissingField.apply).getOrElse(InvalidField(MongoFields.Id))
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
          None,
          profile.currentResidence.map(residence =>
            MongoHiringPersistenceCodecs.StoredCandidateResidence(
              residence.country,
              residence.city,
              residence.country.trim.toLowerCase(java.util.Locale.ROOT),
              residence.city.map(_.trim.toLowerCase(java.util.Locale.ROOT))
            )
          ),
          profile.availabilityStatus.map(_.toString),
          Some(profile.recruiterSearchOptIn),
          Some(profile.skills.toList.map(_.trim.toLowerCase(java.util.Locale.ROOT)).sorted)
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
          profile.jobTitle,
          None,
          None,
          None
        )
      )
  }

  private def readProfile(profile: StoredProfile): ValidatedNel[StoredDocumentError, UserProfile] =
    profile.kind match {
      case "Candidate" | "" =>
        (
          required(profile.skills, MongoFields.ProfileSkills).map(_.toSet),
          profile.experienceSummary.validNel,
          profile.resumeRef.validNel,
          profile.currentResidence.traverse(residence =>
            (residence.country.validNel, residence.city.validNel).mapN(CandidateResidence.apply)
          ),
          profile.availabilityStatus.traverse(value =>
            enumValue(MongoFields.ProfileAvailabilityStatus, value, CandidateAvailabilityStatus.values).toValidatedNel
          ),
          profile.recruiterSearchOptIn.getOrElse(false).validNel
        ).mapN(CandidateProfile.apply).map(UserProfile.Candidate.apply)
      case "Recruiter" =>
        (
          required(profile.organizationName, s"${MongoFields.Profile}.${MongoFields.OrganizationName}"),
          profile.jobTitle.validNel
        ).mapN(RecruiterProfile.apply).map(UserProfile.Recruiter.apply)
      case _ => InvalidField(s"${MongoFields.Profile}.${MongoFields.Kind}").invalidNel
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
    document.keySet.asScala.exists(
      Set(
        MongoFields.State,
        MongoFields.Attempts,
        MongoFields.AvailableAt,
        MongoFields.LeaseOwner,
        MongoFields.LeaseToken
      ).contains
    )

  private def uuid(field: String, value: String): Either[StoredDocumentError, UUID] =
    Parsing.parseUuid(value).leftMap(_ => InvalidField(field))

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

  private def storedUser(value: User, passwordHash: Option[String], version: Long): StoredUser = {
    val emailCanonical = value.email.map(email => AccountName.canonical(email.value))
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
      version,
      value.email.map(_.value),
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
            None,
            candidate.currentResidence.map(residence =>
              MongoHiringPersistenceCodecs.StoredCandidateResidence(
                residence.country,
                residence.city,
                residence.country.trim.toLowerCase(java.util.Locale.ROOT),
                residence.city.map(_.trim.toLowerCase(java.util.Locale.ROOT))
              )
            ),
            candidate.availabilityStatus.map(_.toString),
            Some(candidate.recruiterSearchOptIn),
            Some(candidate.skills.toList.map(_.trim.toLowerCase(java.util.Locale.ROOT)).sorted)
          )
        case UserProfile.Recruiter(recruiter) =>
          StoredProfile(
            "Recruiter",
            None,
            None,
            None,
            Some(recruiter.organizationName),
            recruiter.jobTitle,
            None,
            None,
            None
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

  private def storedJob(value: Job, version: Long): StoredJob = {
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
      version,
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
