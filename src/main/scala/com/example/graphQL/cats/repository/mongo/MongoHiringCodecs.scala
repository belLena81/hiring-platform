package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.*
import org.bson.Document
import java.time.Instant
import java.util.{Date, UUID}
import scala.jdk.CollectionConverters.*

private[mongo] object MongoHiringCodecs {
  def userWithPassword(value: User, passwordHash: String): Document =
    user(value).append("passwordHash", passwordHash)

  def user(value: User): Document = {
    val document = new Document("_id", value.id.value.toString)
      .append("schemaVersion", 2)
      .append("name", value.name)
      .append("nameCanonical", AccountName.canonical(value.name))
      .append("role", value.role.toString)
      .append("accountStatus", value.accountStatus.toString)
      .append("version", java.lang.Long.valueOf(value.version))
      .append("createdAt", Date.from(value.createdAt))
    value.email.foreach(email => document.append("email", email).append("emailCanonical", AccountName.canonical(email)))
    if (value.role == UserRole.Admin && value.adminSingleton) {
      document.append("adminSingletonKey", "singleton-admin")
      ()
    }
    value.profile.foreach(profileValue => document.append("profile", profile(profileValue)))
    value.recruiterProfile.foreach(profileValue => document.append("recruiterProfile", recruiterProfile(profileValue)))
    value.deletedAt.foreach(deletedAt => document.append("deletedAt", Date.from(deletedAt)))
    appendOptionalEmbedding(document, value.embedding)
  }

  def readUser(document: Document): User =
    User(
      UserId(UUID.fromString(document.getString("_id"))),
      Option(document.getString("email")),
      document.getString("name"),
      UserRole.valueOf(document.getString("role")),
      Option(document.get("profile", classOf[Document])).map(readProfile),
      instant(document, "createdAt"),
      Option(document.getString("adminSingletonKey")).contains("singleton-admin"),
      readEmbedding(document),
      Option(document.get("recruiterProfile", classOf[Document])).map(readRecruiterProfile),
      Option(document.getString("accountStatus")).map(AccountStatus.valueOf).getOrElse(AccountStatus.Active),
      Option(document.getDate("deletedAt")).map(_.toInstant),
      Option(document.get("version", classOf[Number])).fold(0L)(_.longValue)
    )

  def readCredentials(document: Document): Option[AccountCredentials] =
    Option(document.getString("passwordHash")).map(hash => AccountCredentials(readUser(document), hash))

  def job(job: Job): Document =
    appendOptionalEmbedding(
      appendOptionalDate(new Document("_id", job.id.value.toString)
        .append("schemaVersion", 1)
        .append("version", java.lang.Long.valueOf(job.version))
        .append("recruiterId", job.recruiterId.value.toString)
        .append("title", job.title)
        .append("description", job.description)
        .append("requirements", job.requirements.asJava)
        .append("skills", job.skills.toList.sorted.asJava)
        .append("location", location(job.location))
        .append("status", job.status.toString)
        .append("createdAt", Date.from(job.createdAt))
        .append("updatedAt", Date.from(job.updatedAt)),
        "closedAt",
        job.closedAt
      ),
      job.embedding
    )

  def readJob(document: Document): Job =
    Job(
      JobId(UUID.fromString(document.getString("_id"))),
      UserId(UUID.fromString(document.getString("recruiterId"))),
      document.getString("title"),
      document.getString("description"),
      stringList(document, "requirements"),
      stringList(document, "skills").toSet,
      readLocation(document.get("location", classOf[Document])),
      JobStatus.valueOf(document.getString("status")),
      instant(document, "createdAt"),
      instant(document, "updatedAt"),
      Option(document.getDate("closedAt")).map(_.toInstant),
      Option(document.get("version", classOf[Number])).fold(0L)(_.longValue),
      readEmbedding(document)
    )

  def application(application: Application): Document =
    new Document("_id", application.id.value.toString)
      .append("schemaVersion", 1)
      .append("candidateId", application.candidateId.value.toString)
      .append("jobId", application.jobId.value.toString)
      .append("status", application.status.toString)
      .append("createdAt", Date.from(application.createdAt))
      .append("updatedAt", Date.from(application.updatedAt))

  def readApplication(document: Document): Application =
    Application(
      ApplicationId(UUID.fromString(document.getString("_id"))),
      UserId(UUID.fromString(document.getString("candidateId"))),
      JobId(UUID.fromString(document.getString("jobId"))),
      ApplicationStatus.valueOf(document.getString("status")),
      instant(document, "createdAt"),
      instant(document, "updatedAt")
    )

  def event(event: ApplicationEvent): Document =
    appendOptionalString(
      appendOptionalString(
        appendOptionalString(new Document("_id", event.id.value.toString)
          .append("schemaVersion", 1)
          .append("applicationId", event.applicationId.value.toString)
          .append("newStatus", event.newStatus.toString)
          .append("actorId", event.actorId.value.toString)
          .append("occurredAt", Date.from(event.occurredAt)),
          "previousStatus",
          event.previousStatus.map(_.toString)
        ),
        "feedback",
        event.feedback
      ),
      "reason",
      event.reason
    )

  def readEvent(document: Document): ApplicationEvent =
    ApplicationEvent(
      ApplicationEventId(UUID.fromString(document.getString("_id"))),
      ApplicationId(UUID.fromString(document.getString("applicationId"))),
      Option(document.getString("previousStatus")).map(ApplicationStatus.valueOf),
      ApplicationStatus.valueOf(document.getString("newStatus")),
      UserId(UUID.fromString(document.getString("actorId"))),
      instant(document, "occurredAt"),
      Option(document.getString("feedback")),
      Option(document.getString("reason"))
    )

  private def location(location: Location): Document =
    new Document("country", location.country).append("city", location.city).append("remote", location.remote)

  private def readLocation(document: Document): Location =
    Location(document.getString("country"), document.getString("city"), document.getBoolean("remote"))

  private def profile(profile: CandidateProfile): Document =
    appendOptionalString(
      appendOptionalString(new Document("skills", profile.skills.toList.sorted.asJava), "experienceSummary", profile.experienceSummary),
      "resumeRef",
      profile.resumeRef
    )

  private def readProfile(document: Document): CandidateProfile =
    CandidateProfile(
      stringList(document, "skills").toSet,
      Option(document.getString("experienceSummary")),
      Option(document.getString("resumeRef"))
    )

  private def recruiterProfile(profile: RecruiterProfile): Document =
    appendOptionalString(new Document("organizationName", profile.organizationName), "jobTitle", profile.jobTitle)

  private def readRecruiterProfile(document: Document): RecruiterProfile =
    RecruiterProfile(document.getString("organizationName"), Option(document.getString("jobTitle")))

  def embeddingDocument(embedding: EntityEmbedding): Document =
    new Document("embedding", embedding.values.map(float => java.lang.Double.valueOf(float.toDouble)).asJava)
      .append("embeddingMeta", embeddingMeta(embedding.meta))

  private def embeddingMeta(meta: EmbeddingMeta): Document =
    new Document("model", meta.model)
      .append("version", java.lang.Integer.valueOf(meta.version))
      .append("sourceHash", meta.sourceHash)
      .append("updatedAt", Date.from(meta.updatedAt))

  private def readEmbedding(document: Document): Option[EntityEmbedding] =
    for {
      values <- Option(document.getList("embedding", classOf[Number]))
      meta <- Option(document.get("embeddingMeta", classOf[Document]))
    } yield EntityEmbedding(
      values.asScala.toList.map(_.floatValue),
      EmbeddingMeta(
        meta.getString("model"),
        meta.get("version", classOf[Number]).intValue,
        meta.getString("sourceHash"),
        instant(meta, "updatedAt")
      )
    )

  private def instant(document: Document, field: String): Instant =
    document.getDate(field).toInstant

  private def stringList(document: Document, field: String): List[String] =
    document.getList(field, classOf[String]).asScala.toList

  private def appendOptionalString(document: Document, field: String, value: Option[String]): Document = {
    value.foreach(document.append(field, _))
    document
  }

  private def appendOptionalDate(document: Document, field: String, value: Option[Instant]): Document = {
    value.foreach(instant => document.append(field, Date.from(instant)))
    document
  }

  private def appendOptionalEmbedding(document: Document, value: Option[EntityEmbedding]): Document = {
    value.foreach { embedding =>
      document.append("embedding", embedding.values.map(float => java.lang.Double.valueOf(float.toDouble)).asJava)
      document.append("embeddingMeta", embeddingMeta(embedding.meta))
    }
    document
  }
}
