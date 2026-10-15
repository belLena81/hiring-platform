package com.example.graphQL.cats.api.graphql

import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.workflow.InterviewWorkflowId

import io.circe.{Decoder, Encoder, Json}
import io.circe.syntax.*
import java.time.Instant
import java.util.{Locale, UUID}

private[graphql] object HiringGraphQLModel {
  private given Decoder[JobId] = Decoder.decodeUUID.map(JobId.apply)
  private given Decoder[ApplicationId] = Decoder.decodeUUID.map(ApplicationId.apply)
  private given Decoder[UserRole] = Decoder.decodeString.emap { value =>
    UserRole.values.find(_.toString.equalsIgnoreCase(value)).toRight(s"Unknown user role: $value")
  }
  private given Decoder[CandidateAvailabilityStatus] = Decoder.decodeString.emap { value =>
    CandidateAvailabilityStatus.values.find(_.toString.equalsIgnoreCase(value)).toRight("Unknown availability status")
  }
  private given Decoder[InterviewWorkflowId] = Decoder.decodeUUID.map(InterviewWorkflowId.apply)
  private given Decoder[SearchId] = Decoder.decodeUUID.map(SearchId.apply)
  given Encoder[InterviewWorkflowId] = Encoder.encodeUUID.contramap(_.value)
  given Encoder[SearchId] = Encoder.encodeUUID.contramap(_.value)
  given Encoder[JobId] = Encoder.encodeUUID.contramap(_.value)
  given Encoder[ApplicationId] = Encoder.encodeUUID.contramap(_.value)
  given Encoder[UserRole] = Encoder.encodeString.contramap(_.toString.toUpperCase(Locale.ROOT))
  given Encoder[CandidateAvailabilityStatus] =
    Encoder.encodeString.contramap(_.toString.toUpperCase(Locale.ROOT))

  extension [A: Encoder](input: A) def idempotencyPayload: Json = input.asJson

  final case class GraphQLFailure(
      code: String,
      message: String,
      exceptional: Boolean,
      reason: Option[String] = None,
      field: Option[String] = None
  )
  sealed trait UserError {
    def code: String
    def message: String
  }

  /** The identifier of one search, issued by the server or supplied by the client. */
  final case class SearchId(value: UUID)

  /** `field` is the input field of the first violated rule, if any. */
  final case class ValidationError(code: String, message: String, field: Option[String] = None) extends UserError

  /** `retryAfterSeconds` is set only when the operation was refused by a rate limit. */
  final case class DomainError(code: String, message: String, retryAfterSeconds: Option[Int] = None) extends UserError
  type MutationOutcome[+A] = A | ValidationError | DomainError
  final case class AuthSuccess(user: User, accessToken: String, expiresAt: Instant)
  final case class DeletionReceipt(receiptId: String, status: AccountDeletionStatus)
  final case class InteractionSuccess(eventId: UUID)
  final case class PageInfo(hasNextPage: Boolean, endCursor: Option[String])
  final case class Edge[A](node: A, cursor: String)
  final case class Connection[A](edges: List[Edge[A]], pageInfo: PageInfo)

  /** The page of a job search together with the identifier of the search that produced it. */
  final case class JobSearchConnection(connection: Connection[Job], searchId: SearchId)
  final case class NearbyJobEdge(node: Job, distanceKm: Double, cursor: String)
  final case class NearbyJobsConnection(edges: List[NearbyJobEdge], pageInfo: PageInfo)
  final case class JobFilterGraphQLInput(
      city: Option[String],
      skills: Option[List[String]],
      createdAfter: Option[Instant]
  ) derives Decoder
  final case class CandidateMatchProfile(skills: Set[String], experienceSummary: Option[String])
  final case class CandidateMatchCandidate(id: UserId, name: String, profile: Option[CandidateMatchProfile])
  final case class CandidateMatchFilter(
      requiredSkills: Option[List[String]],
      country: Option[String],
      city: Option[String],
      availabilityStatus: Option[CandidateAvailabilityStatus]
  ) derives Decoder

  enum GraphQLUserProfile {
    case Candidate(ownerId: com.example.graphQL.cats.domain.model.Identifiers.UserId, value: CandidateProfile)
    case Recruiter(value: RecruiterProfile)
  }

  final case class RankedJobPayload(
      job: Job,
      score: Double,
      searchMode: SearchMode,
      model: String,
      searchId: SearchId,
      matchedSkills: List[String],
      retrievalScore: Option[Double]
  )
  final case class RankedCandidatePayload(
      candidate: CandidateMatchCandidate,
      score: Double,
      searchMode: SearchMode,
      model: String,
      searchId: SearchId,
      matchedSkills: List[String],
      retrievalScore: Option[Double]
  )
  final case class RankedJobResults(results: List[RankedJobPayload])
  final case class RankedCandidateResults(results: List[RankedCandidatePayload])
  final case class AnalyticsReportPayload(
      snapshot: com.example.graphQL.cats.service.AnalyticsReportSnapshot
  )

  final case class ScheduleInterviewGraphQLInput(
      applicationId: ApplicationId,
      startsAt: Instant,
      endsAt: Instant,
      idempotencyKey: UUID
  ) derives Decoder,
        Encoder

  /** The shared input of every interview cancel and reschedule action. It names the workflow and the revision the
    * caller saw; the actor and its role come only from the authenticated request, never from input.
    */
  final case class InterviewActionGraphQLInput(
      workflowId: InterviewWorkflowId,
      expectedRevision: Long,
      idempotencyKey: UUID
  ) derives Decoder,
        Encoder
  final case class ProposeInterviewRescheduleGraphQLInput(
      workflowId: InterviewWorkflowId,
      expectedRevision: Long,
      startsAt: Instant,
      endsAt: Instant,
      idempotencyKey: UUID
  ) derives Decoder,
        Encoder
  final case class RepairInterviewGraphQLInput(
      workflowId: InterviewWorkflowId,
      expectedRevision: Long,
      idempotencyKey: UUID
  ) derives Decoder,
        Encoder
  final case class SubmitApplicationGraphQLInput(jobId: JobId, idempotencyKey: UUID) derives Decoder, Encoder
  final case class CreateJobGraphQLInput(
      idempotencyKey: UUID,
      title: String,
      description: String,
      requirements: List[String],
      skills: List[String],
      country: String,
      city: Option[String],
      remote: Boolean,
      coordinates: Option[GeoPointGraphQLInput]
  ) derives Decoder,
        Encoder
  final case class JobGraphQLInput(
      title: String,
      description: String,
      requirements: List[String],
      skills: List[String],
      country: String,
      city: Option[String],
      remote: Boolean,
      coordinates: Option[GeoPointGraphQLInput]
  ) derives Decoder,
        Encoder
  final case class GeoPointGraphQLInput(latitude: Double, longitude: Double) derives Decoder, Encoder
  final case class UpdateJobGraphQLInput(idempotencyKey: UUID, id: JobId, job: JobGraphQLInput) derives Decoder, Encoder
  final case class JobActionGraphQLInput(idempotencyKey: UUID, jobId: JobId) derives Decoder, Encoder
  final case class ApplicationActionGraphQLInput(idempotencyKey: UUID, applicationId: ApplicationId)
      derives Decoder,
        Encoder
  final case class RejectApplicationGraphQLInput(
      idempotencyKey: UUID,
      applicationId: ApplicationId,
      feedback: Option[String]
  ) derives Decoder,
        Encoder
  final case class DeclineApplicationGraphQLInput(
      idempotencyKey: UUID,
      applicationId: ApplicationId,
      reason: Option[String]
  ) derives Decoder,
        Encoder
  final case class ResidenceGraphQLInput(country: String, city: Option[String]) derives Decoder, Encoder
  final case class CandidateProfileGraphQLInput(
      skills: Option[List[String]],
      experienceSummary: Option[String],
      resumeRef: Option[String],
      residence: Option[ResidenceGraphQLInput],
      availabilityStatus: Option[CandidateAvailabilityStatus],
      recruiterSearchOptIn: Option[Boolean]
  ) derives Decoder,
        Encoder
  final case class RecruiterProfileGraphQLInput(organizationName: String, jobTitle: Option[String])
      derives Decoder,
        Encoder

  /** A `@oneOf` input: exactly one variant is set. The resolver checks this again, because a cached document skips
    * query validation.
    */
  final case class ProfileGraphQLInput(
      candidate: Option[CandidateProfileGraphQLInput],
      recruiter: Option[RecruiterProfileGraphQLInput]
  ) derives Decoder,
        Encoder
  final case class SignUpGraphQLInput(
      idempotencyKey: UUID,
      name: String,
      role: UserRole,
      password: String,
      profile: Option[ProfileGraphQLInput]
  ) derives Decoder,
        Encoder
  final case class LoginGraphQLInput(idempotencyKey: UUID, name: String, password: String) derives Decoder, Encoder
  final case class UpdateProfileGraphQLInput(idempotencyKey: UUID, profile: ProfileGraphQLInput)
      derives Decoder,
        Encoder
  final case class DeleteMyAccountGraphQLInput(idempotencyKey: UUID) derives Decoder, Encoder
  final case class RecordJobViewGraphQLInput(
      idempotencyKey: UUID,
      eventId: UUID,
      jobId: JobId,
      searchId: Option[SearchId]
  ) derives Decoder,
        Encoder
  final case class RecordSearchResultClickGraphQLInput(
      idempotencyKey: UUID,
      eventId: UUID,
      searchId: SearchId,
      resultId: UUID
  ) derives Decoder,
        Encoder
}
