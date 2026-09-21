package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.{CandidateProfile, EmbeddingMeta, EntityEmbedding, Job, JobStatus, Location, RecruiterProfile, User, UserProfile, UserRole}
import com.example.graphQL.cats.repository.protocol.RepositoryError
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
    val user = User(candidateId, Some("candidate@example.com"), "Candidate", UserRole.Candidate,
      Some(UserProfile.Candidate(profile)), now)

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

  test("user codec rejects legacy documents whose role and profile do not satisfy the current invariant") {
    val legacyDocument = new Document("_id", candidateId.value.toString)
      .append("schemaVersion", 1)
      .append("email", "candidate@example.com")
      .append("emailCanonical", "candidate@example.com")
      .append("name", "Candidate")
      .append("role", UserRole.Candidate.toString)
      .append("createdAt", Date.from(now))

    val result = MongoHiringCodecs.readUser(legacyDocument)

    assertEquals(result.toEither, Left(NonEmptyList.one(MongoHiringCodecs.StoredDocumentError.InconsistentDocument)))
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
    val embedding = EntityEmbedding(List(0.1f, 0.2f), EmbeddingMeta("voyage-4-lite", 1, "source-hash", later))
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

  test("job codec reads legacy documents without closedAt as absent close timestamp") {
    val legacyDocument = new Document("_id", jobId.value.toString)
      .append("schemaVersion", 1)
      .append("version", java.lang.Long.valueOf(0L))
      .append("recruiterId", recruiterId.value.toString)
      .append("title", "Senior Scala Developer")
      .append("description", "Build services")
      .append("requirements", List("Scala").asJava)
      .append("skills", List("Cats Effect").asJava)
      .append("location", new Document("country", "Cyprus").append("city", "Nicosia").append("remote", true))
      .append("status", JobStatus.Closed.toString)
      .append("createdAt", Date.from(now))
      .append("updatedAt", Date.from(later))

    val result = MongoHiringCodecs.readJob(legacyDocument)

    assertEquals(result.map(_.closedAt).toEither, Right(None))
  }

  test("malformed stored documents decode to non-sensitive typed errors") {
    val missingName = MongoHiringCodecs.user(User(candidateId, Some("candidate@example.com"), "Candidate", UserRole.Candidate, None, now))
    missingName.remove("name")
    val invalidRole = MongoHiringCodecs.user(User(candidateId, Some("candidate@example.com"), "Candidate", UserRole.Candidate, None, now))
      .append("role", "NotARole")
    val invalidEmbedding = MongoHiringCodecs.job(Job(jobId, recruiterId, "Title", "Description", Nil, Set.empty, Location("Cyprus", "Nicosia", true), JobStatus.Open, now, now))
      .append("embedding", List("not-a-number").asJava)
      .append("embeddingMeta", new Document("model", "model"))

    assertEquals(MongoHiringCodecs.readUser(missingName).toEither, Left(NonEmptyList.one(MongoHiringCodecs.StoredDocumentError.MissingField("name"))))
    assertEquals(MongoHiringCodecs.readUser(invalidRole).toEither, Left(NonEmptyList.one(MongoHiringCodecs.StoredDocumentError.InvalidField("role"))))
    assertEquals(MongoHiringCodecs.readJob(invalidEmbedding).toEither, Left(NonEmptyList.one(MongoHiringCodecs.StoredDocumentError.InvalidField("embedding"))))
    assertEquals(
      MongoStoredDocumentDecoding.repository(MongoHiringCodecs.readUser(missingName)),
      Left(RepositoryError.Unavailable)
    )
  }

  test("codec accumulates independent malformed user fields in document order") {
    val malformed = MongoHiringCodecs.user(User(candidateId, Some("candidate@example.com"), "Candidate", UserRole.Candidate, None, now))
    malformed.remove("name")
    malformed.put("role", "NotARole")
    malformed.put("createdAt", "not-a-date")

    assertEquals(
      MongoHiringCodecs.readUser(malformed).toEither,
      Left(NonEmptyList.of(
        MongoHiringCodecs.StoredDocumentError.MissingField("name"),
        MongoHiringCodecs.StoredDocumentError.InvalidField("role"),
        MongoHiringCodecs.StoredDocumentError.InvalidField("createdAt")
      ))
    )
  }

  test("user codec rejects role-profile mismatches and non-singleton admins") {
    val candidateWithRecruiterProfile = MongoHiringCodecs.user(User(
      candidateId,
      Some("candidate@example.com"),
      "Candidate",
      UserRole.Candidate,
      Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None))),
      now
    )).append("profile", MongoHiringCodecs.profile(UserProfile.Recruiter(RecruiterProfile("Acme", None))))
    val adminWithoutSingleton = MongoHiringCodecs.user(User(
      candidateId,
      Some("admin@example.com"),
      "Admin",
      UserRole.Admin,
      None,
      now,
      adminSingleton = true
    ))
    adminWithoutSingleton.remove("adminSingletonKey")

    assertEquals(MongoHiringCodecs.readUser(candidateWithRecruiterProfile).toEither, Left(NonEmptyList.one(MongoHiringCodecs.StoredDocumentError.InconsistentDocument)))
    assertEquals(MongoHiringCodecs.readUser(adminWithoutSingleton).toEither, Left(NonEmptyList.one(MongoHiringCodecs.StoredDocumentError.InconsistentDocument)))
  }
}
