package com.example.graphQL.cats.api.graphql

import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId}

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
  given Encoder[JobId] = Encoder.encodeUUID.contramap(_.value)
  given Encoder[ApplicationId] = Encoder.encodeUUID.contramap(_.value)
  given Encoder[UserRole] = Encoder.encodeString.contramap(_.toString.toUpperCase(Locale.ROOT))
  given Encoder[CandidateAvailabilityStatus] =
    Encoder.encodeString.contramap(_.toString.toUpperCase(Locale.ROOT))

  extension [A: Encoder](input: A) def idempotencyPayload: Json = input.asJson

  final case class GraphQLFailure(code: String, message: String, exceptional: Boolean)
  sealed trait UserError {
    def code: String
    def message: String
  }

  final case class ValidationError(code: String, message: String) extends UserError
  final case class DomainError(code: String, message: String) extends UserError
  type MutationOutcome[+A] = A | ValidationError | DomainError
  final case class AuthSuccess(user: User, accessToken: String, expiresAt: Instant)
  final case class DeletionReceipt(receiptId: String, status: AccountDeletionStatus)
  final case class InteractionSuccess(recorded: Boolean)
  final case class PageInfo(hasNextPage: Boolean, endCursor: Option[String])
  final case class Edge[A](node: A, cursor: String)
  final case class Connection[A](edges: List[Edge[A]], pageInfo: PageInfo, searchId: Option[String] = None)
  final case class NearbyJobResult(job: Job, distanceKm: Double, cursor: String)
  final case class NearbyJobsResults(results: List[NearbyJobResult], hasNextPage: Boolean)
  final case class JobFilterGraphQLInput(
      city: Option[String],
      skills: Option[List[String]],
      createdAfter: Option[Instant]
  ) derives Decoder
  final case class CandidateMatchProfile(skills: Set[String], experienceSummary: Option[String])
  final case class CandidateMatchCandidate(id: String, name: String, profile: Option[CandidateMatchProfile])
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
      searchId: String,
      matchedSkills: List[String],
      retrievalScore: Option[Double]
  )
  final case class RankedCandidatePayload(
      candidate: CandidateMatchCandidate,
      score: Double,
      searchMode: SearchMode,
      model: String,
      searchId: String,
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
  final case class RepairInterviewGraphQLInput(workflowId: UUID, expectedRevision: Long, idempotencyKey: UUID)
      derives Decoder,
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
  final case class NearbyJobsFilterGraphQLInput(
      city: Option[String],
      skills: Option[List[String]],
      createdAfter: Option[Instant]
  ) derives Decoder
  final case class UpdateJobGraphQLInput(idempotencyKey: UUID, id: JobId, patch: JobGraphQLInput)
      derives Decoder,
        Encoder
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
  final case class SignUpGraphQLInput(
      idempotencyKey: UUID,
      name: String,
      role: UserRole,
      password: String,
      skills: Option[List[String]],
      experienceSummary: Option[String],
      resumeRef: Option[String],
      currentResidenceCountry: Option[String],
      currentResidenceCity: Option[String],
      availabilityStatus: Option[CandidateAvailabilityStatus],
      recruiterSearchOptIn: Option[Boolean],
      organizationName: Option[String],
      jobTitle: Option[String]
  ) derives Decoder,
        Encoder
  final case class LoginGraphQLInput(idempotencyKey: UUID, name: String, password: String) derives Decoder, Encoder
  final case class UpdateProfileGraphQLInput(
      idempotencyKey: UUID,
      skills: Option[List[String]],
      experienceSummary: Option[String],
      resumeRef: Option[String],
      currentResidenceCountry: Option[String],
      currentResidenceCity: Option[String],
      availabilityStatus: Option[CandidateAvailabilityStatus],
      recruiterSearchOptIn: Option[Boolean],
      organizationName: Option[String],
      jobTitle: Option[String]
  ) derives Decoder,
        Encoder
  final case class DeleteMyAccountGraphQLInput(idempotencyKey: UUID) derives Decoder, Encoder
  final case class RecordJobViewGraphQLInput(idempotencyKey: UUID, eventId: UUID, jobId: JobId, searchId: Option[UUID])
      derives Decoder,
        Encoder
  final case class RecordSearchResultClickGraphQLInput(
      idempotencyKey: UUID,
      eventId: UUID,
      searchId: UUID,
      resultId: UUID
  ) derives Decoder,
        Encoder
}
