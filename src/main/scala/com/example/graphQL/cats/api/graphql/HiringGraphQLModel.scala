package com.example.graphQL.cats.api.graphql

import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId}

import java.time.Instant
import java.util.UUID

private[graphql] object HiringGraphQLModel {
  final case class GraphQLFailure(code: String, message: String)
  final case class ValidationError(code: String, message: String)
  final case class DomainError(code: String, message: String)
  final case class AuthSuccess(user: User, accessToken: String, expiresAt: String)
  final case class DeletionSuccess(deleted: Boolean)
  final case class InteractionSuccess(recorded: Boolean)
  final case class PageInfo(hasNextPage: Boolean, endCursor: Option[String])
  final case class Edge[A](node: A, cursor: String)
  final case class Connection[A](edges: List[Edge[A]], pageInfo: PageInfo, searchId: Option[String] = None)
  final case class JobFilterGraphQLInput(city: Option[String], skills: Option[List[String]], createdAfter: Option[Instant])
  final case class CandidateMatchProfile(skills: Set[String], experienceSummary: Option[String])
  final case class CandidateMatchCandidate(id: String, name: String, profile: Option[CandidateMatchProfile])

  enum GraphQLUserProfile {
    case Candidate(value: CandidateProfile)
    case Recruiter(value: RecruiterProfile)
  }

  final case class RankedJobPayload(job: Job, score: Double, searchMode: SearchMode, model: String, searchId: String)
  final case class RankedCandidatePayload(candidate: CandidateMatchCandidate, score: Double, searchMode: SearchMode, model: String, searchId: String)
  final case class RankedJobResults(results: List[RankedJobPayload])
  final case class RankedCandidateResults(results: List[RankedCandidatePayload])

  final case class SubmitApplicationGraphQLInput(jobId: JobId)
  final case class JobGraphQLInput(
      title: String,
      description: String,
      requirements: List[String],
      skills: List[String],
      country: String,
      city: Option[String],
      remote: Boolean
  )
  final case class UpdateJobGraphQLInput(id: JobId, patch: JobGraphQLInput)
  final case class JobActionGraphQLInput(jobId: JobId)
  final case class ApplicationActionGraphQLInput(applicationId: ApplicationId)
  final case class RejectApplicationGraphQLInput(applicationId: ApplicationId, feedback: Option[String])
  final case class DeclineApplicationGraphQLInput(applicationId: ApplicationId, reason: Option[String])
  final case class SignUpGraphQLInput(
      name: String,
      role: UserRole,
      password: String,
      skills: Option[List[String]],
      experienceSummary: Option[String],
      resumeRef: Option[String],
      organizationName: Option[String],
      jobTitle: Option[String]
  )
  final case class BootstrapAdminGraphQLInput(name: String, password: String)
  final case class LoginGraphQLInput(name: String, password: String)
  final case class UpdateProfileGraphQLInput(
      skills: Option[List[String]],
      experienceSummary: Option[String],
      resumeRef: Option[String],
      organizationName: Option[String],
      jobTitle: Option[String]
  )
  final case class RecordJobViewGraphQLInput(eventId: UUID, jobId: JobId, searchId: Option[UUID])
  final case class RecordSearchResultClickGraphQLInput(eventId: UUID, searchId: UUID, resultId: UUID)
}
