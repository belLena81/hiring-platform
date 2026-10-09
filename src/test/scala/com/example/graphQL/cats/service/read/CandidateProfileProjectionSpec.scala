package com.example.graphQL.cats.service.read

import com.example.graphQL.cats.domain.model.{
  CandidateAvailabilityStatus,
  CandidateProfile,
  CandidateResidence,
  UserRole
}
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.ActorContext
import munit.FunSuite

import java.util.UUID

final class CandidateProfileProjectionSpec extends FunSuite {
  private val ownerId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000b01"))
  private val otherId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000b02"))
  private val profile = CandidateProfile(
    Set("Scala"),
    Some("summary"),
    None,
    Some(CandidateResidence("Cyprus", Some("Limassol"))),
    Some(CandidateAvailabilityStatus.AVAILABLE_NOW),
    recruiterSearchOptIn = true
  )

  test("the owner sees owner-only matching attributes") {
    val owner = ActorContext(ownerId, UserRole.Candidate)
    assertEquals(CandidateProfileProjection.forViewer(owner, ownerId, profile), profile)
  }

  test("another candidate, a recruiter and an admin see them masked while public fields remain") {
    List(UserRole.Candidate, UserRole.Recruiter, UserRole.Admin).foreach { role =>
      val visible = CandidateProfileProjection.forViewer(ActorContext(otherId, role), ownerId, profile)
      assertEquals(visible.currentResidence, None)
      assertEquals(visible.availabilityStatus, None)
      assertEquals(visible.recruiterSearchOptIn, false)
      assertEquals(visible.skills, profile.skills)
      assertEquals(visible.experienceSummary, profile.experienceSummary)
    }
  }
}
