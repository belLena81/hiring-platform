package com.example.graphQL.cats.api.graphql

import cats.data.EitherT
import cats.effect.IO
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId}

import java.time.Instant

private[graphql] object HiringGraphQLModel {
  final case class GraphQLError(code: String, message: String)
  final case class PageInfo(hasNextPage: Boolean, endCursor: Option[String])
  final case class Edge[A](node: A, cursor: String)
  final case class Connection[A](edges: List[Edge[A]], pageInfo: PageInfo, errors: List[GraphQLError] = Nil, searchId: Option[String] = None)
  final case class JobPayload(job: Option[Job], errors: List[GraphQLError])
  final case class ApplicationPayload(application: Option[Application], errors: List[GraphQLError])
  final case class AccountPayload(user: Option[User], accessToken: Option[String], expiresAt: Option[String], errors: List[GraphQLError])
  final case class UserPayload(user: Option[User], errors: List[GraphQLError])
  final case class DeleteAccountPayload(deleted: Boolean, errors: List[GraphQLError])
  final case class InteractionPayload(recorded: Boolean, errors: List[GraphQLError])
  final case class JobFilterGraphQLInput(city: Option[String], skills: Option[List[String]], createdAfter: Option[Instant])
  final case class CandidateMatchProfile(skills: Set[String], experienceSummary: Option[String])
  final case class CandidateMatchCandidate(id: String, name: String, profile: Option[CandidateMatchProfile])

  enum GraphQLUserProfile {
    case Candidate(value: CandidateProfile)
    case Recruiter(value: RecruiterProfile)
  }

  final case class RankedJobPayload(job: Job, score: Double, searchMode: SearchMode, model: String, version: Int, searchId: String)
  final case class RankedCandidatePayload(candidate: CandidateMatchCandidate, score: Double, searchMode: SearchMode, model: String, version: Int, searchId: String)
  final case class RankedJobResults(results: List[RankedJobPayload], errors: List[GraphQLError])
  final case class RankedCandidateResults(results: List[RankedCandidatePayload], errors: List[GraphQLError])

  type GraphQLStep[A] = EitherT[IO, GraphQLError, A]

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
  final case class RecordJobViewGraphQLInput(eventId: String, jobId: JobId, searchId: Option[String])
  final case class RecordSearchResultClickGraphQLInput(eventId: String, searchId: String, resultId: String)
}
