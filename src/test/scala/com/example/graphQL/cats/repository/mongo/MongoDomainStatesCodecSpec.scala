package com.example.graphQL.cats.repository.mongo

import cats.syntax.all.*
import com.example.graphQL.cats.AccountValueFixtures.email
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.{
  AccountState,
  AccountStatus,
  CandidateAvailabilityStatus,
  CandidateProfile,
  CandidateResidence,
  EmbeddingMeta,
  EntityEmbedding,
  Job,
  JobState,
  Location,
  PasswordHash,
  RecruiterProfile,
  User,
  UserProfile,
  UserRole
}
import com.example.graphQL.cats.domain.pagination.JobProjection
import com.mongodb.MongoClientSettings
import java.time.Instant
import java.util.UUID
import munit.FunSuite
import org.bson.Document
import scala.jdk.CollectionConverters.*

/** AC-09: a closed job and a deleted account cannot be represented without their instant, the singleton marker is
  * stored for the Admin only, and the stored field names and the profile mapping have one owner.
  */
final class MongoDomainStatesCodecSpec extends FunSuite {
  private val created = Instant.parse("2026-09-16T10:15:30Z")
  private val ended = Instant.parse("2026-09-17T10:15:30Z")
  private val userId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000901"))
  private val jobId = JobId(UUID.fromString("00000000-0000-0000-0000-000000000902"))
  private val recruiterId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000903"))
  private val embedding = EntityEmbedding(List(0.1f, 0.2f), EmbeddingMeta("voyage-4-lite", "hash", ended))

  private val candidateProfile = UserProfile.Candidate(
    CandidateProfile(
      Set("Scala", "MongoDB"),
      Some("Backend engineer"),
      Some("resume://901"),
      Some(CandidateResidence("Cyprus", Some("Nicosia"))),
      Some(CandidateAvailabilityStatus.AVAILABLE_NOW),
      recruiterSearchOptIn = true
    )
  )
  private val candidate =
    User(userId, Some(email("candidate@example.com")), "Candidate", UserRole.Candidate, Some(candidateProfile), created)
  private val admin = User(userId, Some(email("admin@example.com")), "Admin", UserRole.Admin, None, created)

  private def job(state: JobState, withEmbedding: Boolean = false): Job = Job(
    jobId,
    recruiterId,
    "Scala Developer",
    "Build services",
    List("Cats Effect"),
    Set("Scala"),
    Location("Cyprus", Some("Nicosia"), remote = true),
    state,
    created,
    state.closedAt.getOrElse(created),
    embedding = Option.when(withEmbedding)(embedding)
  )

  private def keys(document: Document): Set[String] = document.keySet.asScala.toSet

  test("AC-09.1 a closed job stores its instant and a job in any other state stores none") {
    assertEquals(MongoHiringCodecs.job(job(JobState.Closed(ended))).getString("status"), "Closed")
    assert(MongoHiringCodecs.job(job(JobState.Closed(ended))).containsKey("closedAt"))
    List(JobState.Draft, JobState.Open).foreach { state =>
      assert(!MongoHiringCodecs.job(job(state)).containsKey("closedAt"), clue(state))
    }
  }

  test("AC-09.1 a stored Closed job without closedAt, or an Open job with one, fails closed") {
    val closedWithout = MongoHiringCodecs.job(job(JobState.Closed(ended)))
    closedWithout.remove("closedAt")
    val openWith = MongoHiringCodecs.job(job(JobState.Open)).append("closedAt", java.util.Date.from(ended))
    List(closedWithout, openWith).foreach { document =>
      assertEquals(
        MongoHiringCodecs.readJob(document).toEither.leftMap(_.toList),
        Left(List(MongoHiringCodecs.StoredDocumentError.InconsistentDocument))
      )
    }
  }

  test("AC-09.1 a closed job round-trips with its instant and answers its status and closedAt from the state") {
    val closed = job(JobState.Closed(ended))
    assertEquals(MongoHiringCodecs.readJob(MongoHiringCodecs.job(closed)).toEither, Right(closed))
    assertEquals((closed.status.toString, closed.closedAt), ("Closed", Some(ended)))
    assertEquals(job(JobState.Open).closedAt, None)
  }

  test("AC-09.2 a deleted account stores its instant and an active one stores none") {
    val deleted = candidate.copy(profile = None, email = None, state = AccountState.Deleted(ended))
    val document = MongoHiringCodecs.user(deleted)
    assertEquals(document.getString("accountStatus"), "Deleted")
    assertEquals(document.getDate("deletedAt"), java.util.Date.from(ended))
    assert(!MongoHiringCodecs.user(candidate).containsKey("deletedAt"))
    assertEquals(MongoHiringCodecs.readUser(document).toEither, Right(deleted))
  }

  test("AC-09.2 a stored Deleted account without deletedAt, or an Active one with it, fails closed") {
    val deleted = MongoHiringCodecs.user(candidate.copy(profile = None, state = AccountState.Deleted(ended)))
    deleted.remove("deletedAt")
    val active = MongoHiringCodecs.user(candidate).append("deletedAt", java.util.Date.from(ended))
    List(deleted, active).foreach { document =>
      assertEquals(
        MongoHiringCodecs.readUser(document).toEither.leftMap(_.toList),
        Left(List(MongoHiringCodecs.StoredDocumentError.InconsistentDocument))
      )
    }
    assertEquals(AccountState.Deleted(ended).status, AccountStatus.Deleted)
    assertEquals(AccountState.Active.deletedAt, None)
  }

  test("AC-09.3 the singleton marker is derived from the role: stored for the Admin, never for another role") {
    assertEquals(MongoHiringCodecs.user(admin).getString("adminSingletonKey"), "singleton-admin")
    assert(!MongoHiringCodecs.user(candidate).containsKey("adminSingletonKey"))
    val markedCandidate = MongoHiringCodecs.user(candidate).append("adminSingletonKey", "singleton-admin")
    assertEquals(
      MongoHiringCodecs.readUser(markedCandidate).toEither.leftMap(_.toList),
      Left(List(MongoHiringCodecs.StoredDocumentError.InconsistentDocument))
    )
    assertEquals(MongoHiringCodecs.readUser(MongoHiringCodecs.user(admin)).toEither, Right(admin))
  }

  test("AC-09.4 the application event validator requires the actor's role, as the generation 025 installed did not") {
    val required = (validator: Document) =>
      validator
        .get("$jsonSchema", classOf[Document])
        .getList("required", classOf[String])
        .asScala
        .toSet
    assert(required(MongoOperationalValidators.applicationEventsValidator).contains(MongoFields.ActorRole))
    assert(!required(MongoOperationalValidators.applicationEventsValidatorThrough025).contains(MongoFields.ActorRole))
  }

  test("AC-09.5 the profile document of a user is exactly the profile mapping, for both roles") {
    val recruiter = User(
      userId,
      Some(email("recruiter@example.com")),
      "Recruiter",
      UserRole.Recruiter,
      Some(UserProfile.Recruiter(RecruiterProfile("Acme", Some("Head of hiring")))),
      created
    )
    List(candidate, recruiter).foreach { user =>
      assertEquals(
        MongoHiringCodecs.user(user).get("profile", classOf[Document]),
        MongoHiringCodecs.profile(user.profile.get),
        clue(user.role)
      )
    }
    val stored = MongoHiringCodecs.profile(candidateProfile)
    assertEquals(stored.getList("skillsCanonical", classOf[String]).asScala.toList, List("mongodb", "scala"))
    assertEquals(
      stored.get("currentResidence", classOf[Document]).getString("cityCanonical"),
      "nicosia"
    )
  }

  test("AC-09.5 the stored field names of users and jobs are the keys the codecs write") {
    val deleted = candidate.copy(profile = None, email = None, state = AccountState.Deleted(ended))
    val users = List(
      MongoHiringCodecs.userWithPassword(
        candidate.copy(embedding = Some(embedding)),
        PasswordHash.fromEncoded("encoded")
      ),
      MongoHiringCodecs.user(deleted),
      MongoHiringCodecs.user(admin)
    )
    assertEquals(users.flatMap(keys).toSet, UserFields.stored.toSet)
    val jobs = List(job(JobState.Closed(ended), withEmbedding = true), job(JobState.Open))
    // The admission fence is written by the repository, not the codec.
    assertEquals(
      jobs.map(MongoHiringCodecs.job(_)).flatMap(keys).toSet + MongoFields.AdmissionFence,
      JobFields.stored.toSet
    )
  }

  test("AC-09.5 every field set derived from the owner is a subset of the stored fields") {
    assert((UserFields.actor ++ UserFields.erasedOnDeletion).toSet.subsetOf(UserFields.stored.toSet))
    assert(
      (JobFields.owner ++ JobFields.hiddenFromSummary ++ JobFields.eligibility).toSet.subsetOf(JobFields.stored.toSet)
    )
    assertEquals(
      JobFields.eligibility.toSet,
      JobFields.stored.toSet - MongoFields.Embedding - MongoFields.AdmissionFence
    )
  }

  test("AC-10.9 the summary projection leaves out the posting text and the vector, the display one only the vector") {
    def excluded(projection: org.bson.conversions.Bson) =
      projection
        .toBsonDocument(classOf[Document], MongoClientSettings.getDefaultCodecRegistry)
        .keySet()
        .asScala
        .toSet
    assertEquals(
      excluded(MongoReadProjections.jobList(JobProjection.Summary)),
      Set(MongoFields.Description, MongoFields.Requirements, MongoFields.Embedding, MongoFields.EmbeddingMeta)
    )
    assertEquals(
      excluded(MongoReadProjections.jobList(JobProjection.Full)),
      Set(MongoFields.Embedding, MongoFields.EmbeddingMeta)
    )
    // Everything authorization and the cursor need is still read.
    val read = JobFields.stored.toSet -- JobFields.hiddenFromSummary
    assert(Set(MongoFields.Id, MongoFields.RecruiterId, MongoFields.Status, MongoFields.CreatedAt).subsetOf(read))
  }

  test("AC-10.9 a summary document decodes with a blank description and no requirements; a full read refuses it") {
    val open = job(JobState.Open)
    val projected = MongoHiringCodecs.job(open)
    projected.remove(MongoFields.Description)
    projected.remove(MongoFields.Requirements)
    assertEquals(
      MongoHiringCodecs.readJob(projected, JobProjection.Summary).toEither,
      Right(open.copy(description = "", requirements = Nil))
    )
    assert(MongoHiringCodecs.readJob(projected, JobProjection.Full).isInvalid)
    assert(MongoHiringCodecs.readJob(projected).isInvalid)
    // A summary read of a document that does carry the text does not discard it.
    assertEquals(
      MongoHiringCodecs.readJob(MongoHiringCodecs.job(open), JobProjection.Summary).toEither,
      Right(open)
    )
  }
}
