package com.example.graphQL.cats.infrastructure.mongo

import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.*
import org.bson.Document
import java.time.Instant
import java.util.{Date, UUID}
import scala.jdk.CollectionConverters.*

private[mongo] object MongoHiringCodecs {
  def user(user: User): Document =
    appendOptionalString(new Document("_id", user.id.value.toString)
      .append("schemaVersion", 1)
      .append("email", user.email)
      .append("emailCanonical", user.email.toLowerCase)
      .append("name", user.name)
      .append("role", user.role.toString)
      .append("createdAt", Date.from(user.createdAt)),
      "adminSingletonKey",
      Option.when(user.role == UserRole.Admin && user.adminSingleton)("singleton-admin")
    )

  def readUser(document: Document): User =
    User(
      UserId(UUID.fromString(document.getString("_id"))),
      document.getString("email"),
      document.getString("name"),
      UserRole.valueOf(document.getString("role")),
      None,
      instant(document, "createdAt"),
      Option(document.getString("adminSingletonKey")).contains("singleton-admin")
    )

  def job(job: Job): Document =
    new Document("_id", job.id.value.toString)
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
      .append("updatedAt", Date.from(job.updatedAt))

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
      Option(document.get("version", classOf[Number])).fold(0L)(_.longValue)
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

  private def location(location: Location): Document =
    new Document("country", location.country).append("city", location.city).append("remote", location.remote)

  private def readLocation(document: Document): Location =
    Location(document.getString("country"), document.getString("city"), document.getBoolean("remote"))

  private def instant(document: Document, field: String): Instant =
    document.getDate(field).toInstant

  private def stringList(document: Document, field: String): List[String] =
    document.getList(field, classOf[String]).asScala.toList

  private def appendOptionalString(document: Document, field: String, value: Option[String]): Document = {
    value.foreach(document.append(field, _))
    document
  }
}
