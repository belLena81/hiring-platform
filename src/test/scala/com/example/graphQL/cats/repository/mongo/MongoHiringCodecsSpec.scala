package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId}
import com.example.graphQL.cats.domain.model.{
  Application,
  ApplicationEvent,
  ApplicationStatus,
  CandidateProfile,
  EmbeddingMeta,
  EntityEmbedding,
  Job,
  JobStatus,
  Location,
  RecruiterProfile,
  User,
  UserProfile,
  UserRole
}
import com.example.graphQL.cats.repository.protocol.RepositoryError
import com.example.graphQL.cats.shared.events.{
  OperationalAggregateType,
  OperationalEventEnvelope,
  OperationalEventType,
  SearchSession,
  SearchSessionResult
}
import io.circe.Json
import cats.data.NonEmptyList
import java.time.Instant
import java.util.{Date, UUID}
import munit.FunSuite
import org.bson.Document
import scala.jdk.CollectionConverters.*

class MongoHiringCodecsSpec extends FunSuite {
  private val now = Instant.parse("2026-09-16T10:15:30Z")
  private val later = Instant.parse("2026-09-16T11:15:30Z")
  private val candidateId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000201"))
  private val recruiterId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000202"))
  private val jobId = JobId(UUID.fromString("00000000-0000-0000-0000-000000000203"))

  test("user codec preserves candidate profile round-trip") {
    val profile = CandidateProfile(
      Set("Scala", "Cats Effect", "MongoDB"),
      Some("Builds backend services"),
      Some("resume://candidate-201")
    )
    val user = User(
      candidateId,
      Some("candidate@example.com"),
      "Candidate",
      UserRole.Candidate,
      Some(UserProfile.Candidate(profile)),
      now
    )

    val result = MongoHiringCodecs.readUser(MongoHiringCodecs.user(user))

    assertEquals(result.toEither, Right(user))
    assertEquals(MongoHiringCodecs.user(user).get("profile", classOf[Document]).getString("kind"), "Candidate")
    assert(!MongoHiringCodecs.user(user).containsKey("recruiterProfile"))
  }

  test("user codec preserves recruiter profile as the single tagged profile") {
    val user = User(
      UserId(UUID.fromString("00000000-0000-0000-0000-000000000204")),
      None,
      "Recruiter",
      UserRole.Recruiter,
      Some(UserProfile.Recruiter(RecruiterProfile("Acme", Some("Hiring Manager")))),
      now
    )

    val document = MongoHiringCodecs.user(user)

    assertEquals(MongoHiringCodecs.readUser(document).toEither, Right(user))
    assertEquals(document.get("profile", classOf[Document]).getString("kind"), "Recruiter")
    assert(!document.containsKey("recruiterProfile"))
  }

  test("user codec keeps legacy untagged profiles as candidate profiles") {
    val user = User(
      candidateId,
      Some("candidate@example.com"),
      "Candidate",
      UserRole.Candidate,
      Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None))),
      now
    )
    val legacy = MongoHiringCodecs.user(user)
    legacy.get("profile", classOf[Document]).remove("kind")

    assertEquals(MongoHiringCodecs.readUser(legacy).toEither, Right(user))
  }

  test("user codec rejects removed schema fields") {
    val document = MongoHiringCodecs
      .user(
        User(
          candidateId,
          Some("candidate@example.com"),
          "Candidate",
          UserRole.Candidate,
          Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None))),
          now
        )
      )
      .append("schemaVersion", Int.box(1))

    assertEquals(
      MongoHiringCodecs.readUser(document).toEither,
      Left(NonEmptyList.one(MongoHiringCodecs.StoredDocumentError.InvalidField("schemaVersion")))
    )
  }

  test("job codec preserves closedAt round-trip for closed jobs") {
    val job = Job(
      jobId,
      recruiterId,
      "Senior Scala Developer",
      "Build services",
      List("Scala"),
      Set("Cats Effect"),
      Location("Cyprus", "Nicosia", remote = true),
      JobStatus.Closed,
      now,
      later,
      Some(later)
    )

    val document = MongoHiringCodecs.job(job)
    val result = MongoHiringCodecs.readJob(document)

    assertEquals(document.getDate("closedAt"), Date.from(later))
    assertEquals(result.toEither, Right(job))
  }

  test("job codec preserves embedding metadata when present") {
    val embedding = EntityEmbedding(List(0.1f, 0.2f), EmbeddingMeta("voyage-4-lite", "source-hash", later))
    val job = Job(
      jobId,
      recruiterId,
      "Senior Scala Developer",
      "Build services",
      List("Scala"),
      Set("Cats Effect"),
      Location("Cyprus", "Nicosia", remote = true),
      JobStatus.Open,
      now,
      later,
      embedding = Some(embedding)
    )

    val document = MongoHiringCodecs.job(job)
    val result = MongoHiringCodecs.readJob(document)

    assert(document.containsKey("embedding"))
    assert(document.containsKey("embeddingMeta"))
    assertEquals(result.map(_.embedding).toEither, Right(Some(embedding)))
  }

  test("job codec rejects removed revision fields") {
    val document = MongoHiringCodecs
      .job(
        Job(
          jobId,
          recruiterId,
          "Senior Scala Developer",
          "Build services",
          List("Scala"),
          Set("Cats Effect"),
          Location("Cyprus", "Nicosia", remote = true),
          JobStatus.Open,
          now,
          later
        )
      )
      .append("version", Long.box(1L))

    assertEquals(
      MongoHiringCodecs.readJob(document).toEither,
      Left(NonEmptyList.one(MongoHiringCodecs.StoredDocumentError.InvalidField("version")))
    )
  }

  test("malformed stored documents decode to non-sensitive typed errors") {
    val missingName = MongoHiringCodecs.user(
      User(candidateId, Some("candidate@example.com"), "Candidate", UserRole.Candidate, None, now)
    )
    missingName.remove("name")
    val invalidRole = MongoHiringCodecs
      .user(User(candidateId, Some("candidate@example.com"), "Candidate", UserRole.Candidate, None, now))
      .append("role", "NotARole")
    val invalidEmbedding = MongoHiringCodecs
      .job(
        Job(
          jobId,
          recruiterId,
          "Title",
          "Description",
          Nil,
          Set.empty,
          Location("Cyprus", "Nicosia", true),
          JobStatus.Open,
          now,
          now
        )
      )
      .append("embedding", List("not-a-number").asJava)
      .append("embeddingMeta", new Document("model", "model"))

    assertEquals(
      MongoHiringCodecs.readUser(missingName).toEither,
      Left(NonEmptyList.one(MongoHiringCodecs.StoredDocumentError.MissingField("name")))
    )
    assertEquals(
      MongoHiringCodecs.readUser(invalidRole).toEither,
      Left(NonEmptyList.one(MongoHiringCodecs.StoredDocumentError.InvalidField("role")))
    )
    assertEquals(
      MongoHiringCodecs.readJob(invalidEmbedding).toEither,
      Left(NonEmptyList.one(MongoHiringCodecs.StoredDocumentError.InvalidField("document")))
    )
    assertEquals(
      MongoStoredDocumentDecoding.repository(MongoHiringCodecs.readUser(missingName)),
      Left(RepositoryError.Unavailable)
    )
  }

  test("codec accumulates independent semantic user errors") {
    val malformed = MongoHiringCodecs.user(
      User(candidateId, Some("candidate@example.com"), "Candidate", UserRole.Candidate, None, now)
    )
    malformed.put("role", "NotARole")
    malformed.put("accountStatus", "NotAnAccountStatus")

    assertEquals(
      MongoHiringCodecs.readUser(malformed).toEither,
      Left(
        NonEmptyList.of(
          MongoHiringCodecs.StoredDocumentError.InvalidField("role"),
          MongoHiringCodecs.StoredDocumentError.InvalidField("accountStatus")
        )
      )
    )
  }

  test("user codec rejects role-profile mismatches and non-singleton admins") {
    val candidateWithRecruiterProfile = MongoHiringCodecs
      .user(
        User(
          candidateId,
          Some("candidate@example.com"),
          "Candidate",
          UserRole.Candidate,
          Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None))),
          now
        )
      )
      .append("profile", MongoHiringCodecs.profile(UserProfile.Recruiter(RecruiterProfile("Acme", None))))
    val adminWithoutSingleton = MongoHiringCodecs.user(
      User(
        candidateId,
        Some("admin@example.com"),
        "Admin",
        UserRole.Admin,
        None,
        now,
        adminSingleton = true
      )
    )
    adminWithoutSingleton.remove("adminSingletonKey")

    assertEquals(
      MongoHiringCodecs.readUser(candidateWithRecruiterProfile).toEither,
      Left(NonEmptyList.one(MongoHiringCodecs.StoredDocumentError.InconsistentDocument))
    )
    assertEquals(
      MongoHiringCodecs.readUser(adminWithoutSingleton).toEither,
      Left(NonEmptyList.one(MongoHiringCodecs.StoredDocumentError.InconsistentDocument))
    )
  }

  test("codec builders do not share mutable document state") {
    val user = User(candidateId, Some("candidate@example.com"), "Candidate", UserRole.Candidate, None, now)
    val first = MongoHiringCodecs.user(user)
    val second = MongoHiringCodecs.user(user)

    first.put("name", "Changed")

    assertEquals(first.getString("name"), "Changed")
    assertEquals(second.getString("name"), "Candidate")
  }

  test("generated codecs preserve event, outbox, and search-session shapes") {
    val applicationId = ApplicationId(UUID.fromString("00000000-0000-0000-0000-000000000205"))
    val applicationEventId = ApplicationEventId(UUID.fromString("00000000-0000-0000-0000-000000000206"))
    val application = Application.create(applicationId, candidateId, jobId, now)
    val event =
      ApplicationEvent(applicationEventId, applicationId, None, ApplicationStatus.Created, candidateId, now, None, None)
    val operational = OperationalEventEnvelope(
      UUID.fromString("00000000-0000-0000-0000-000000000207"),
      OperationalEventType.SEARCH_PERFORMED,
      now,
      OperationalAggregateType.Search,
      "search-205",
      candidateId,
      Json.obj("query" -> Json.fromString("Scala"))
    )
    val search = SearchSession(
      UUID.fromString("00000000-0000-0000-0000-000000000208"),
      candidateId,
      "semanticJobSearch",
      Some("Scala"),
      Json.obj("city" -> Json.fromString("Nicosia")),
      Some("test-model"),
      List(SearchSessionResult("job-203", 1, 0.95d)),
      now,
      later
    )

    assertEquals(
      MongoHiringCodecs.readApplication(MongoHiringCodecs.application(application)).toEither,
      Right(application)
    )
    assertEquals(MongoHiringCodecs.readEvent(MongoHiringCodecs.event(event)).toEither, Right(event))
    assertEquals(
      MongoHiringCodecs.readOperationalEvent(MongoHiringCodecs.operationalEvent(operational)).toEither,
      Right(operational)
    )
    assertEquals(
      MongoHiringCodecs.readOperationalEvent(MongoHiringCodecs.outboxRecord(operational, now)).toEither,
      Right(operational)
    )
    assertEquals(MongoHiringCodecs.readSearchSession(MongoHiringCodecs.searchSession(search)).toEither, Right(search))
  }
}
