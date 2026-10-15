package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.{
  Application,
  ApplicationEvent,
  ApplicationStatus,
  Job,
  JobStatus,
  Location,
  SearchableText,
  UserRole
}
import com.example.graphQL.cats.service.events.OperationalEvents
import com.example.graphQL.cats.shared.crypto.SourceHash
import cats.syntax.all.*
import com.example.graphQL.cats.service.RepositoryError
import java.time.Instant
import java.util.UUID
import munit.FunSuite

class MongoJobLocationAndEventActorRoleSpec extends FunSuite {
  private val now = Instant.parse("2026-01-01T10:00:00Z")
  private val jobId = JobId(UUID.fromString("00000000-0000-0000-0000-000000000501"))
  private val recruiterId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000502"))
  private val candidateId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000503"))
  private val applicationId = ApplicationId(UUID.fromString("00000000-0000-0000-0000-000000000504"))
  private val eventId = ApplicationEventId(UUID.fromString("00000000-0000-0000-0000-000000000505"))

  private def job(city: Option[String]) = Job(
    jobId,
    recruiterId,
    "Scala Developer",
    "Build services",
    List("Cats Effect"),
    Set("Scala"),
    Location("Cyprus", city, remote = true),
    JobStatus.Open,
    now,
    now
  )

  private def event(role: Option[UserRole]) = ApplicationEvent(
    eventId,
    applicationId,
    Some(ApplicationStatus.Interview),
    ApplicationStatus.Rejected,
    recruiterId,
    now,
    Some("No fit"),
    None,
    role
  )

  test("AC-09.2 job without a city omits the stored field and reads back without a city") {
    val document = MongoHiringCodecs.job(job(None))
    assert(!document.get("location", classOf[org.bson.Document]).containsKey("city"))
    assertEquals(MongoHiringCodecs.readJob(document).toEither, Right(job(None)))
  }

  test("AC-09.2 stored empty, null or missing city reads as no city") {
    val withCity = MongoHiringCodecs.job(job(Some("Nicosia")))
    assertEquals(MongoHiringCodecs.readJob(withCity).toEither, Right(job(Some("Nicosia"))))
    List[AnyRef]("", null).foreach { stored =>
      val document = MongoHiringCodecs.job(job(Some("Nicosia")))
      document.get("location", classOf[org.bson.Document]).put("city", stored)
      assertEquals(MongoHiringCodecs.readJob(document).toEither, Right(job(None)), s"stored city: $stored")
    }
    val missing = MongoHiringCodecs.job(job(Some("Nicosia")))
    missing.get("location", classOf[org.bson.Document]).remove("city")
    assertEquals(MongoHiringCodecs.readJob(missing).toEither, Right(job(None)))
  }

  test("AC-09.4 embedding source text and hash are identical for a missing city and the former empty city") {
    val former = job(Some(""))
    val current = job(None)
    assertEquals(SearchableText.job(current), SearchableText.job(former))
    assertEquals(SourceHash.sha256(SearchableText.job(current)), SourceHash.sha256(SearchableText.job(former)))
    assertEquals(SearchableText.job(job(Some("Nicosia"))), SearchableText.job(current))
  }

  test("AC-08.2 status event stores the acting role and reads it back") {
    val document = MongoHiringCodecs.event(event(Some(UserRole.Admin)))
    assertEquals(document.getString("actorRole"), "Admin")
    assertEquals(MongoHiringCodecs.readEvent(document).toEither, Right(event(Some(UserRole.Admin))))
  }

  test("AC-08.2 scheduled interview workflow stores the scheduler's role and old documents read without one") {
    import com.example.graphQL.cats.domain.InterviewLifecycleFixtures.workflowIn
    val workflow = workflowIn(com.example.graphQL.cats.domain.workflow.InterviewWorkflowPhase.ReservationPending)
    val scheduled = workflow.copy(initiatedByRole = Some(UserRole.Admin))
    val document = MongoInterviewWorkflowCodec.workflowDocument(scheduled)
    assertEquals(document.getString("initiatedByRole"), "Admin")
    assertEquals(MongoInterviewWorkflowCodec.decodeWorkflow(document), Right(scheduled))
    val legacy = MongoInterviewWorkflowCodec.workflowDocument(workflow.copy(initiatedByRole = None))
    assert(!legacy.containsKey("initiatedByRole"))
    assertEquals(MongoInterviewWorkflowCodec.decodeWorkflow(legacy).map(_.initiatedByRole), Right(None))
  }

  test("AC-08.3 stored event with an unknown actor role fails closed") {
    val document = MongoHiringCodecs.event(event(Some(UserRole.Admin)))
    document.put("actorRole", "Superuser")
    assertEquals(
      MongoHiringCodecs.readEvent(document).toEither.leftMap(_.toList),
      Left(List(MongoHiringCodecs.StoredDocumentError.InvalidField("actorRole")))
    )
  }

  test("AC-08.3 stored workflow with an unknown initiator role is invalid stored data") {
    import com.example.graphQL.cats.domain.InterviewLifecycleFixtures.workflowIn
    val document = MongoInterviewWorkflowCodec.workflowDocument(
      workflowIn(com.example.graphQL.cats.domain.workflow.InterviewWorkflowPhase.ReservationPending)
    )
    document.put("initiatedByRole", "Superuser")
    assertEquals(MongoInterviewWorkflowCodec.decodeWorkflow(document), Left(RepositoryError.InvalidStoredData))
  }

  test("AC-08.3 pre-existing event document without a role still decodes with no role") {
    val document = MongoHiringCodecs.event(event(None))
    assert(!document.containsKey("actorRole"))
    assertEquals(MongoHiringCodecs.readEvent(document).toEither, Right(event(None)))
  }

  test("AC-08.5 operational envelope is identical whether or not the event records an actor role") {
    val application = Application.create(applicationId, candidateId, jobId, now)
    val withRole = OperationalEvents.statusChanged(eventId.value, application, event(Some(UserRole.Admin)))
    val withoutRole = OperationalEvents.statusChanged(eventId.value, application, event(None))
    assertEquals(withRole, withoutRole)
    assertEquals(withRole.actorId, recruiterId)
    val hired = event(Some(UserRole.Admin)).copy(newStatus = ApplicationStatus.Hired)
    assertEquals(
      OperationalEvents.candidateHired(eventId.value, application, hired),
      OperationalEvents.candidateHired(eventId.value, application, hired.copy(actorRole = None))
    )
  }
}
