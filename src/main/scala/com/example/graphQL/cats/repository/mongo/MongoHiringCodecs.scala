package com.example.graphQL.cats.repository.mongo

import cats.data.{Validated, ValidatedNel}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.pagination.JobProjection
import com.example.graphQL.cats.service.events.*
import com.example.graphQL.cats.service.port.Versioned
import MongoHiringPersistenceCodecs.*
import com.example.graphQL.cats.infrastructure.events.{OperationalEventWireCodec, SearchFilterWireCodec}
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
    withEmbedding(
      MongoHiringPersistenceCodecs.user(storedUser(value, Some(passwordHash.encoded), version)),
      value.embedding
    )

  def user(value: User, version: Long = 0L): Document =
    withEmbedding(MongoHiringPersistenceCodecs.user(storedUser(value, None, version)), value.embedding)

  /** The vector is the one field the generated codecs do not carry: it is stored as a float32 BSON vector. */
  private def withEmbedding(document: Document, embedding: Option[EntityEmbedding]): Document = {
    embedding.foreach(value => document.put(MongoFields.Embedding, MongoEmbeddingVectors.encode(value.values)))
    document
  }

  private def withoutEmbedding(document: Document): Document =
    if (!document.containsKey(MongoFields.Embedding)) document
    else {
      val copy = new Document(document)
      copy.remove(MongoFields.Embedding)
      copy
    }

  private def storedVector(document: Document): ValidatedNel[StoredDocumentError, Option[List[Float]]] =
    Option(document.get(MongoFields.Embedding)).traverse(raw =>
      MongoEmbeddingVectors.decode(raw).toValidNel(InvalidField(MongoFields.Embedding))
    )

  def readUser(document: Document): ValidatedNel[StoredDocumentError, User] =
    readVersionedUser(document).map(_.value)

  def readVersionedUser(document: Document): ValidatedNel[StoredDocumentError, Versioned[User]] =
    (
      decode(withoutEmbedding(document), storedUserFields)(MongoHiringPersistenceCodecs.decodeUser),
      storedVector(document)
    ).tupled.andThen { case (stored, vector) => readVersionedUser(stored, vector) }

  private def readVersionedUser(
      value: StoredUser,
      vector: Option[List[Float]]
  ): ValidatedNel[StoredDocumentError, Versioned[User]] =
    (
      uuid(MongoFields.Id, value._id).toValidatedNel.map(UserId.apply),
      value.email.traverse(email =>
        EmailAddress.from(email).leftMap(_ => InvalidField(MongoFields.Email)).toValidatedNel
      ),
      value.name.validNel,
      enumValue(MongoFields.Role, value.role, UserRole.values).toValidatedNel,
      readUserProfile(value.profile),
      value.createdAt.toInstant.validNel,
      readEmbedding(vector, value.embeddingMeta),
      enumValue(MongoFields.AccountStatus, value.accountStatus, AccountStatus.values)
        .flatMap(accountState(_, value.deletedAt.map(_.toInstant)))
        .toValidatedNel
    ).mapN { (id, email, name, role, profile, createdAt, embedding, state) =>
      Versioned(User(id, email, name, role, profile, createdAt, embedding, state), value.version)
    }.andThen { user =>
      if (!user.value.roleProfileIsValid || !hasAdminMarkerIffAdmin(user.value, value.adminSingletonKey))
        InconsistentDocument.invalidNel
      else Either.cond(user.version >= 0L, user, InvalidField(MongoFields.Version)).toValidatedNel
    }

  /** The status and its instant are one fact: a Deleted account has `deletedAt`, an Active one does not. */
  private def accountState(
      status: AccountStatus,
      deletedAt: Option[Instant]
  ): Either[StoredDocumentError, AccountState] =
    (status, deletedAt) match {
      case (AccountStatus.Active, None)      => AccountState.Active.asRight
      case (AccountStatus.Deleted, Some(at)) => AccountState.Deleted(at).asRight
      case _                                 => InconsistentDocument.asLeft
    }

  /** The unique-index marker is stored for the singleton Admin only; the domain derives it from the role. A deleted
    * account keeps whatever marker it had, which no longer says anything.
    */
  private def hasAdminMarkerIffAdmin(user: User, marker: Option[String]): Boolean =
    user.state match {
      case AccountState.Active     => marker.contains(SingletonAdminMarker) == (user.role == UserRole.Admin)
      case AccountState.Deleted(_) => true
    }

  private[mongo] val SingletonAdminMarker = "singleton-admin"

  def readCredentials(document: Document): ValidatedNel[StoredDocumentError, Option[AccountCredentials]] =
    (
      decode(withoutEmbedding(document), storedUserFields)(MongoHiringPersistenceCodecs.decodeUser),
      storedVector(document)
    ).tupled.andThen { case (value, vector) =>
      value.passwordHash.fold(None.validNel)(hash =>
        readVersionedUser(value, vector).map(v => AccountCredentials(v.value, PasswordHash.fromEncoded(hash)).some)
      )
    }

  def job(job: Job, version: Long = 0L): Document =
    withEmbedding(MongoHiringPersistenceCodecs.job(storedJob(job, version)), job.embedding)

  def readJob(document: Document): ValidatedNel[StoredDocumentError, Job] =
    readVersionedJob(document).map(_.value)

  def readVersionedJob(document: Document): ValidatedNel[StoredDocumentError, Versioned[Job]] =
    (
      decode(withoutEmbedding(document), storedJobFields)(MongoHiringPersistenceCodecs.decodeJob),
      storedVector(document)
    ).tupled.andThen { case (stored, vector) => readVersionedJob(stored, vector) }

  /** A job read with the given projection: a summary read leaves out the posting text, which the decoded job then
    * carries as empty (`description` blank, `requirements` empty). Only a caller that selected neither may use it.
    */
  def readJob(document: Document, projection: JobProjection): ValidatedNel[StoredDocumentError, Job] =
    projection match {
      case JobProjection.Full    => readJob(document)
      case JobProjection.Summary => readJob(withEmptyPosting(document))
    }

  private def withEmptyPosting(document: Document): Document = {
    val completed = new Document(document)
    val _ = completed.putIfAbsent(MongoFields.Description, "")
    val _ = completed.putIfAbsent(MongoFields.Requirements, List.empty[String].asJava)
    completed
  }

  private def readVersionedJob(
      value: StoredJob,
      vector: Option[List[Float]]
  ): ValidatedNel[StoredDocumentError, Versioned[Job]] =
    (
      uuid(MongoFields.Id, value._id).toValidatedNel.map(JobId.apply),
      uuid(MongoFields.RecruiterId, value.recruiterId).toValidatedNel.map(UserId.apply),
      value.title.validNel,
      value.description.validNel,
      value.requirements.validNel,
      value.skills.toSet.validNel,
      decodeLocation(value.location),
      enumValue(MongoFields.Status, value.status, JobStatus.values)
        .flatMap(jobState(_, value.closedAt.map(_.toInstant)))
        .toValidatedNel,
      value.createdAt.toInstant.validNel,
      value.updatedAt.toInstant.validNel,
      readEmbedding(vector, value.embeddingMeta)
    ).mapN(Job.apply).map(Versioned(_, value.version)).andThen { job =>
      Either.cond(job.version >= 0L, job, InvalidField(MongoFields.Version)).toValidatedNel
    }

  /** The status and its instant are one fact: a Closed job has `closedAt`, a Draft or Open one does not. */
  private def jobState(status: JobStatus, closedAt: Option[Instant]): Either[StoredDocumentError, JobState] =
    (status, closedAt) match {
      case (JobStatus.Draft, None)      => JobState.Draft.asRight
      case (JobStatus.Open, None)       => JobState.Open.asRight
      case (JobStatus.Closed, Some(at)) => JobState.Closed(at).asRight
      case _                            => InconsistentDocument.asLeft
    }

  def application(application: Application): Document =
    MongoHiringPersistenceCodecs.application(
      StoredApplication(
        application.id.value.toString,
        application.candidateId.value.toString,
        application.jobId.value.toString,
        application.status.toString,
        application.createdAt.toDate,
        application.updatedAt.toDate
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
        event.occurredAt.toDate,
        event.feedback,
        event.reason,
        event.actorRole.toString
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
        value.reason.validNel,
        enumValue(MongoFields.ActorRole, value.actorRole, UserRole.values).toValidatedNel
      ).mapN(ApplicationEvent.apply)
    }

  def operationalEvent(value: OperationalEventEnvelope): Document =
    MongoHiringPersistenceCodecs.operationalEvent(storedOperationalEvent(value))

  // Envelope size is enforced at write (`outboxRecord`); `OperationalEventWireCodec.decode` re-checks it on this read.
  def readOperationalEvent(document: Document): ValidatedNel[StoredDocumentError, OperationalEventEnvelope] = {
    val eventDocument = if (isOutbox(document)) projectOperationalEvent(document) else document
    val fields = if (isOutbox(document)) OutboxFields else StoredEventFields
    noUnexpectedFields(document, fields)
      .andThen(_ => decode(eventDocument, StoredEventFields)(MongoHiringPersistenceCodecs.decodeOperationalEvent))
      .andThen(readOperationalEvent)
  }

  private def decodeLocation(value: StoredLocation): ValidatedNel[StoredDocumentError, Location] = {
    val coordinates = value.point match {
      case None                                                                  => None.validNel
      case Some(point) if point.`type` == "Point" && point.coordinates.size == 2 =>
        GeoPoint
          .validate(point.coordinates(1), point.coordinates(0))
          .leftMap(_ => InvalidField("location.point"))
          .toValidatedNel
          .map(Some(_))
      case Some(_) => InvalidField("location.point").invalidNel
    }
    coordinates
      .andThen(point => Location.validate(value.country, value.city.filter(_.nonEmpty), value.remote, point))
      .leftMap(_.map(_ => InvalidField("location")))
  }

  private def projectOperationalEvent(document: Document): Document =
    val event = new Document()
    OperationalEventFields.foreach(field => Option(document.get(field)).foreach(value => event.put(field, value)))
    event

  /** The envelope bytes are the single stored copy of the event; the top-level fields only serve queries and must agree
    * with them.
    */
  private def readOperationalEvent(
      value: StoredOperationalEvent
  ): ValidatedNel[StoredDocumentError, OperationalEventEnvelope] =
    (
      uuid(MongoFields.Id, value._id).toValidatedNel,
      enumValue(MongoFields.EventType, value.eventType, OperationalEventType.values).toValidatedNel,
      enumValue(MongoFields.AggregateType, value.aggregateType, OperationalAggregateType.values).toValidatedNel,
      uuid(MongoFields.ActorId, value.actorId).toValidatedNel
    ).tupled
      .andThen { _ =>
        OperationalEventWireCodec
          .decode(value.envelopeBytes)
          .leftMap(_ => InvalidField(MongoFields.EnvelopeBytes))
          .toValidatedNel
      }
      .andThen { event =>
        val consistent = event.eventId.toString == value._id &&
          event.eventType.toString == value.eventType &&
          event.occurredAt.toDate == value.occurredAt &&
          event.aggregateType.toString == value.aggregateType &&
          event.aggregateId == value.aggregateId &&
          event.actorId.value.toString == value.actorId &&
          event.partitionKey == value.partitionKey
        Validated.condNel(consistent, event, InconsistentDocument)
      }

  def outboxRecord(
      value: OperationalEventEnvelope,
      now: Instant,
      maxEnvelopeBytes: Int = OperationalEventWireCodec.MaxEnvelopeBytes
  ): Either[String, Document] =
    for {
      valid <- OperationalEventWireCodec.validate(value, maxEnvelopeBytes).leftMap(_.toString)
      subjectIds <- outboxSubjectIds(valid)
      document <- Either
        .catchNonFatal(outboxDocument(valid, now, subjectIds))
        .leftMap(_ => "Invalid outbox representation")
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
        event.envelopeBytes,
        event.partitionKey,
        "Retryable",
        0,
        now.toDate,
        None,
        None,
        now.toDate,
        now.toDate
      )
    )
  }

  private[mongo] def outboxSubjectIds(value: OperationalEventEnvelope): Either[String, List[String]] =
    value.payload.subjectIds
      .leftMap(_.toString)
      .map(subjects => (value.actorId.value :: subjects).map(_.toString).distinct.sorted)

  def searchSession(value: SearchSession): Document =
    MongoHiringPersistenceCodecs.searchSession(
      StoredSearchSession(
        value.id.toString,
        value.actorId.value.toString,
        value.searchKind.wire,
        value.query,
        SearchFilterWireCodec.encodeString(value.filter),
        value.model,
        value.results.map(result => StoredSearchSessionResult(result.resultId, result.rank, result.score)),
        value.occurredAt.toDate,
        value.expiresAt.toDate
      )
    )

  def readSearchSession(document: Document): ValidatedNel[StoredDocumentError, SearchSession] =
    decode(document, SearchSessionFields)(MongoHiringPersistenceCodecs.decodeSearchSession).andThen { value =>
      (
        uuid(MongoFields.Id, value._id).toValidatedNel,
        uuid(MongoFields.ActorId, value.actorId).toValidatedNel.map(UserId.apply),
        OperationalEventPayload.SearchKind.values
          .find(_.wire == value.searchKind)
          .toRight(InvalidField(MongoFields.SearchKind))
          .toValidatedNel,
        value.query.validNel,
        SearchFilterWireCodec.decodeString(value.filter).leftMap(_ => InvalidField(MongoFields.Filter)).toValidatedNel,
        value.model.validNel,
        value.results.map(result => SearchSessionResult(result.resultId, result.rank, result.score)).validNel,
        value.occurredAt.toInstant.validNel,
        value.expiresAt.toInstant.validNel
      ).mapN(SearchSession.apply)
    }

  private val storedUserFields = UserFields.stored.toSet
  private val storedJobFields = JobFields.stored.toSet
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
      MongoFields.Reason,
      MongoFields.ActorRole
    )
  private val OperationalEventFields = Set(
    MongoFields.Id,
    MongoFields.Topic,
    MongoFields.EventType,
    MongoFields.OccurredAt,
    MongoFields.AggregateType,
    MongoFields.AggregateId,
    MongoFields.ActorId,
    MongoFields.EnvelopeBytes,
    MongoFields.PartitionKey
  )
  // Migration 026 removes `payload`; readers tolerate (and ignore) it until that proof holds, so migration 003's
  // backfill still decodes a row that has not been rewritten yet.
  private val StoredEventFields = OperationalEventFields + MongoFields.Payload
  private val OutboxFields = StoredEventFields ++ Set(
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

    missingField(error).map(MissingField.apply).getOrElse(InvalidField("document"))
  }

  private def noUnexpectedFields(document: Document, fields: Set[String]): ValidatedNel[StoredDocumentError, Unit] =
    document.keySet.asScala.find(!fields.contains(_)).fold(().validNel)(field => InvalidField(field).invalidNel)

  private def readUserProfile(profile: Option[StoredProfile]): ValidatedNel[StoredDocumentError, Option[UserProfile]] =
    profile.traverse(readProfile)

  private[mongo] def profile(value: UserProfile): Document =
    MongoHiringPersistenceCodecs.profile(storedProfile(value))

  /** The one mapping of a profile to its stored form, including the lowercase sidecars the indexes and the search
    * eligibility filter read; the profile document and the whole user document are both built from it.
    */
  private def storedProfile(value: UserProfile): StoredProfile = {
    value match {
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
              SearchTerm.canonical(residence.country),
              residence.city.map(SearchTerm.canonical)
            )
          ),
          candidate.availabilityStatus.map(_.toString),
          Some(candidate.recruiterSearchOptIn),
          Some(candidate.skills.toList.map(SearchTerm.canonical).sorted)
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
    }
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
      values: Option[List[Float]],
      meta: Option[StoredEmbeddingMeta]
  ): ValidatedNel[StoredDocumentError, Option[EntityEmbedding]] =
    (values, meta) match {
      case (None, None)                 => None.validNel
      case (Some(numbers), Some(value)) =>
        Some(
          EntityEmbedding(
            numbers,
            EmbeddingMeta(value.model, value.sourceHash, value.updatedAt.toInstant)
          )
        ).validNel
      case _ => InconsistentDocument.invalidNel
    }

  /** The `embedding` vector and `embeddingMeta` fields written by the derived-data (embedding) update. */
  def embeddingDocument(embedding: EntityEmbedding): Document =
    withEmbedding(
      MongoHiringPersistenceCodecs.embedding(StoredEmbeddingFields(storedEmbeddingMeta(embedding))),
      Some(embedding)
    )

  private def storedEmbeddingMeta(embedding: EntityEmbedding): StoredEmbeddingMeta =
    StoredEmbeddingMeta(embedding.meta.model, embedding.meta.sourceHash, embedding.meta.updatedAt.toDate)

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
    MongoDocumentFields.parseUuid(field, value)

  private def enumValue[A](field: String, value: String, values: Array[A]): Either[StoredDocumentError, A] =
    MongoDocumentFields.byName(values)(value).toRight(InvalidField(field))

  private def optionalEnum[A](
      field: String,
      value: Option[String],
      values: Array[A]
  ): Either[StoredDocumentError, Option[A]] =
    value.traverse(enumValue(field, _, values))

  private def storedUser(value: User, passwordHash: Option[String], version: Long): StoredUser = {
    val emailCanonical = value.email.map(email => AccountName.canonical(email.value))
    val embeddingMeta = value.embedding.map(storedEmbeddingMeta)
    StoredUser(
      value.id.value.toString,
      version,
      value.email.map(_.value),
      emailCanonical,
      value.name,
      AccountName.canonical(value.name),
      value.role.toString,
      value.profile.map(storedProfile),
      value.createdAt.toDate,
      value.createdAt.toDate,
      value.accountStatus.toString,
      value.deletedAt.map(Date.from),
      Option.when(value.role == UserRole.Admin)(SingletonAdminMarker),
      passwordHash,
      embeddingMeta
    )
  }

  private def storedJob(value: Job, version: Long): StoredJob = {
    val embeddingMeta = value.embedding.map(storedEmbeddingMeta)
    val skills = value.skills.toList.sorted
    StoredJob(
      value.id.value.toString,
      version,
      value.recruiterId.value.toString,
      value.title,
      value.description,
      value.requirements,
      skills,
      StoredLocation(
        value.location.country,
        value.location.city,
        value.location.remote,
        value.location.coordinates.map(point => StoredGeoPoint("Point", List(point.longitude, point.latitude))),
        value.location.city.map(SearchTerm.canonical)
      ),
      value.status.toString,
      value.createdAt.toDate,
      value.updatedAt.toDate,
      value.closedAt.map(Date.from),
      embeddingMeta,
      skillsCanonical = Some(skills.map(SearchTerm.canonical))
    )
  }

  private def storedOperationalEvent(value: OperationalEventEnvelope): StoredOperationalEvent =
    StoredOperationalEvent(
      value.eventId.toString,
      OperationalEventEnvelope.Topic,
      value.eventType.toString,
      value.occurredAt.toDate,
      value.aggregateType.toString,
      value.aggregateId,
      value.actorId.value.toString,
      OperationalEventWireCodec.bytes(value),
      value.partitionKey
    )
}
