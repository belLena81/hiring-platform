package com.example.graphQL.cats.domain

import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.{CandidateProfile, Job, JobStatus, Location, RecruiterProfile, User, UserProfile, UserRole}
import com.example.graphQL.cats.domain.policy.ApplicationSubmission
import java.time.Instant
import java.util.UUID
import munit.FunSuite

class ApplicationSubmissionSpec extends FunSuite {
  private val now = Instant.parse("2026-09-16T10:15:30Z")
  private val candidateId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000001"))
  private val recruiterId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000002"))
  private val jobId = JobId(UUID.fromString("00000000-0000-0000-0000-000000000003"))
  private val applicationId = ApplicationId(UUID.fromString("00000000-0000-0000-0000-000000000004"))

  private val candidate = User(candidateId, Some("candidate@example.com"), "Candidate", UserRole.Candidate,
    Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None))), now)
  private val recruiter = User(recruiterId, Some("recruiter@example.com"), "Recruiter", UserRole.Recruiter,
    Some(UserProfile.Recruiter(RecruiterProfile("Acme", None))), now)
  private val openJob = Job(
    jobId,
    recruiterId,
    "Senior Scala Developer",
    "Build backend services",
    List("Scala"),
    Set("Scala"),
    Location("Ukraine", "Kyiv", remote = true),
    JobStatus.Open,
    now,
    now
  )

  test("candidate can create an application for an open job") {
    val result = ApplicationSubmission.create(candidate, openJob, applicationId, now)

    assertEquals(result.map(_.candidateId), Right(candidateId))
    assertEquals(result.map(_.jobId), Right(jobId))
  }

  test("non-candidate actor is rejected with a typed domain error") {
    val result = ApplicationSubmission.create(recruiter, openJob, applicationId, now)

    assertEquals(result, Left(DomainError.CandidateRequired))
  }

  test("closed jobs reject new applications") {
    val result = ApplicationSubmission.create(candidate, openJob.copy(status = JobStatus.Closed), applicationId, now)

    assertEquals(result, Left(DomainError.JobMustBeOpen))
  }
}
