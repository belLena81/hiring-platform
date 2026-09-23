package com.example.graphQL.cats.api.graphql

import com.example.graphQL.cats.api.graphql.HiringGraphQLFetchers.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.service.ProbeResult
import sangria.schema.*
import sangria.schema.Action.deferredAction
import scala.concurrent.ExecutionContext

private[graphql] object HiringGraphQLTypes {
  lazy val healthType: ObjectType[RequestContext, Unit] =
    ObjectType("Health", fields[RequestContext, Unit](Field("status", healthStatus, resolve = _ => "UP")))
  lazy val readinessType: ObjectType[RequestContext, ProbeResult] = ObjectType(
    "Readiness",
    fields[RequestContext, ProbeResult](
      Field(
        "status",
        readinessStatus,
        resolve = context => if (context.value == ProbeResult.Ready) "READY" else "NOT_READY"
      )
    )
  )
  lazy val userErrorType: InterfaceType[RequestContext, UserError] = InterfaceType(
    "UserError",
    fields[RequestContext, UserError](
      Field("code", StringType, resolve = _.value.code),
      Field("message", StringType, resolve = _.value.message)
    )
  )
  lazy val validationErrorType: ObjectType[RequestContext, ValidationError] = ObjectType(
    "ValidationError",
    List(PossibleInterface[RequestContext, ValidationError](userErrorType)),
    fields[RequestContext, ValidationError](
      Field("code", StringType, resolve = _.value.code),
      Field("message", StringType, resolve = _.value.message)
    )
  )
  lazy val domainErrorType: ObjectType[RequestContext, DomainError] = ObjectType(
    "DomainError",
    List(PossibleInterface[RequestContext, DomainError](userErrorType)),
    fields[RequestContext, DomainError](
      Field("code", StringType, resolve = _.value.code),
      Field("message", StringType, resolve = _.value.message)
    )
  )
  lazy val pageInfoType: ObjectType[RequestContext, PageInfo] = ObjectType(
    "PageInfo",
    fields[RequestContext, PageInfo](
      Field("hasNextPage", BooleanType, resolve = _.value.hasNextPage),
      Field("endCursor", OptionType(StringType), resolve = _.value.endCursor)
    )
  )
  lazy val locationType: ObjectType[RequestContext, Location] = ObjectType(
    "Location",
    fields[RequestContext, Location](
      Field("country", StringType, resolve = _.value.country),
      Field("city", StringType, resolve = _.value.city),
      Field("remote", BooleanType, resolve = _.value.remote)
    )
  )
  lazy val candidateProfileType: ObjectType[RequestContext, CandidateProfile] = ObjectType(
    "CandidateProfile",
    fields[RequestContext, CandidateProfile](
      Field("skills", ListType(StringType), resolve = _.value.skills.toList.sorted),
      Field("experienceSummary", OptionType(StringType), resolve = _.value.experienceSummary),
      Field("resumeRef", OptionType(StringType), resolve = _.value.resumeRef)
    )
  )
  lazy val candidateMatchProfileType: ObjectType[RequestContext, CandidateMatchProfile] =
    ObjectType(
      "CandidateMatchProfile",
      fields[RequestContext, CandidateMatchProfile](
        Field("skills", ListType(StringType), resolve = _.value.skills.toList.sorted),
        Field("experienceSummary", OptionType(StringType), resolve = _.value.experienceSummary)
      )
    )
  lazy val candidateMatchCandidateType: ObjectType[RequestContext, CandidateMatchCandidate] =
    ObjectType(
      "CandidateMatchCandidate",
      fields[RequestContext, CandidateMatchCandidate](
        Field("id", IDType, resolve = _.value.id),
        Field("name", StringType, resolve = _.value.name),
        Field("profile", OptionType(candidateMatchProfileType), resolve = _.value.profile)
      )
    )
  lazy val userProfileType: OutputType[GraphQLUserProfile] =
    UnionType[RequestContext]("UserProfile", List(candidateProfileType, recruiterProfileType))
      .mapValue[GraphQLUserProfile] {
        case GraphQLUserProfile.Candidate(profile) => profile
        case GraphQLUserProfile.Recruiter(profile) => profile
      }
  lazy val userType: ObjectType[RequestContext, User] = ObjectType(
    "User",
    fields[RequestContext, User](
      Field("id", userIdType, resolve = _.value.id),
      Field(
        "email",
        OptionType(StringType),
        resolve = context =>
          emailVisibilityFetcher
            .deferOpt(context.value.id)
            // Sangria's deferred Future projection requires an EC; parasitic avoids a thread hop.
            .map(_.flatMap(_ => context.value.email))(using ExecutionContext.parasitic)
      ),
      Field("name", StringType, resolve = _.value.name),
      Field("role", userRole, resolve = _.value.role),
      Field("status", userStatus, resolve = _.value.accountStatus),
      Field(
        "profile",
        OptionType(userProfileType),
        resolve = _.value.profile.map {
          case UserProfile.Candidate(profile) => GraphQLUserProfile.Candidate(profile)
          case UserProfile.Recruiter(profile) => GraphQLUserProfile.Recruiter(profile)
        }
      ),
      instantField("createdAt", _.createdAt)
    )
  )
  lazy val recruiterProfileType: ObjectType[RequestContext, RecruiterProfile] =
    ObjectType(
      "RecruiterProfile",
      fields[RequestContext, RecruiterProfile](
        Field("organizationName", StringType, resolve = _.value.organizationName),
        Field("jobTitle", OptionType(StringType), resolve = _.value.jobTitle)
      )
    )
  lazy val jobType: ObjectType[RequestContext, Job] = ObjectType(
    "Job",
    fields[RequestContext, Job](
      Field("id", jobIdType, resolve = _.value.id),
      Field("title", StringType, resolve = _.value.title),
      Field("description", StringType, resolve = _.value.description),
      Field("requirements", ListType(StringType), resolve = _.value.requirements),
      Field("skills", ListType(StringType), resolve = _.value.skills.toList.sorted),
      Field("location", locationType, resolve = _.value.location),
      Field("status", jobStatus, resolve = _.value.status),
      instantField("createdAt", _.createdAt),
      instantField("updatedAt", _.updatedAt),
      Field("recruiter", OptionType(userType), resolve = context => usersFetcher.deferOpt(context.value.recruiterId))
    )
  )
  lazy val applicationType: ObjectType[RequestContext, Application] = ObjectType(
    "Application",
    fields[RequestContext, Application](
      Field("id", applicationIdType, resolve = _.value.id),
      Field("status", applicationStatus, resolve = _.value.status),
      instantField("createdAt", _.createdAt),
      instantField("updatedAt", _.updatedAt),
      Field("candidate", OptionType(userType), resolve = context => usersFetcher.deferOpt(context.value.candidateId)),
      Field("job", OptionType(jobType), resolve = context => jobsFetcher.deferOpt(context.value.jobId))
    )
  )
  lazy val applicationEventType: ObjectType[RequestContext, ApplicationEvent] =
    ObjectType(
      "ApplicationEvent",
      fields[RequestContext, ApplicationEvent](
        Field("id", IDType, resolve = _.value.id.value.toString),
        Field("previousStatus", OptionType(applicationStatus), resolve = _.value.previousStatus),
        Field("newStatus", applicationStatus, resolve = _.value.newStatus),
        Field("actorId", IDType, resolve = _.value.actorId.value.toString),
        instantField("occurredAt", _.occurredAt),
        Field("feedback", OptionType(StringType), resolve = _.value.feedback),
        Field("reason", OptionType(StringType), resolve = _.value.reason)
      )
    )

  lazy val jobEdgeType: ObjectType[RequestContext, Edge[Job]] = edgeType("JobEdge", jobType)
  lazy val applicationEdgeType: ObjectType[RequestContext, Edge[Application]] =
    edgeType("ApplicationEdge", applicationType)
  lazy val applicationEventEdgeType: ObjectType[RequestContext, Edge[ApplicationEvent]] =
    edgeType("ApplicationEventEdge", applicationEventType)
  lazy val jobConnectionType: ObjectType[RequestContext, Connection[Job]] = connectionType("JobConnection", jobEdgeType)
  lazy val applicationConnectionType: ObjectType[RequestContext, Connection[Application]] =
    connectionType("ApplicationConnection", applicationEdgeType)
  lazy val applicationEventConnectionType: ObjectType[RequestContext, Connection[ApplicationEvent]] =
    connectionType("ApplicationEventConnection", applicationEventEdgeType)
  lazy val userEdgeType: ObjectType[RequestContext, Edge[User]] = edgeType("UserEdge", userType)
  lazy val userConnectionType: ObjectType[RequestContext, Connection[User]] =
    connectionType("UserConnection", userEdgeType)
  lazy val authSuccessType: ObjectType[RequestContext, AuthSuccess] = ObjectType(
    "AuthSuccess",
    fields[RequestContext, AuthSuccess](
      Field("user", userType, resolve = _.value.user),
      Field("accessToken", StringType, resolve = _.value.accessToken),
      Field("expiresAt", instantType, resolve = _.value.expiresAt)
    )
  )
  lazy val deletionSuccessType: ObjectType[RequestContext, DeletionSuccess] =
    ObjectType(
      "DeletionSuccess",
      fields[RequestContext, DeletionSuccess](Field("deleted", BooleanType, resolve = _.value.deleted))
    )
  lazy val interactionSuccessType: ObjectType[RequestContext, InteractionSuccess] =
    ObjectType(
      "InteractionSuccess",
      fields[RequestContext, InteractionSuccess](Field("recorded", BooleanType, resolve = _.value.recorded))
    )
  lazy val rankedJobType: ObjectType[RequestContext, RankedJobPayload] = ObjectType(
    "RankedJob",
    fields[RequestContext, RankedJobPayload](
      Field("job", jobType, resolve = _.value.job),
      Field("score", FloatType, resolve = _.value.score),
      Field("searchMode", searchMode, resolve = _.value.searchMode),
      Field("model", StringType, resolve = _.value.model),
      Field("searchId", IDType, resolve = _.value.searchId)
    )
  )
  lazy val rankedCandidateType: ObjectType[RequestContext, RankedCandidatePayload] =
    ObjectType(
      "RankedCandidate",
      fields[RequestContext, RankedCandidatePayload](
        Field("candidate", candidateMatchCandidateType, resolve = _.value.candidate),
        Field("score", FloatType, resolve = _.value.score),
        Field("searchMode", searchMode, resolve = _.value.searchMode),
        Field("model", StringType, resolve = _.value.model),
        Field("searchId", IDType, resolve = _.value.searchId)
      )
    )
  lazy val rankedJobResultsType: ObjectType[RequestContext, RankedJobResults] =
    ObjectType(
      "RankedJobResults",
      fields[RequestContext, RankedJobResults](Field("results", ListType(rankedJobType), resolve = _.value.results))
    )
  lazy val rankedCandidateResultsType: ObjectType[RequestContext, RankedCandidateResults] =
    ObjectType(
      "RankedCandidateResults",
      fields[RequestContext, RankedCandidateResults](
        Field("results", ListType(rankedCandidateType), resolve = _.value.results)
      )
    )
  lazy val analyticsFunnelDayType
      : ObjectType[RequestContext, com.example.graphQL.cats.repository.protocol.AnalyticsFunnelDay] =
    ObjectType(
      "AnalyticsFunnelDay",
      fields[RequestContext, com.example.graphQL.cats.repository.protocol.AnalyticsFunnelDay](
        instantField("day", _.day),
        Field("created", LongType, resolve = _.value.created),
        Field("accepted", LongType, resolve = _.value.accepted),
        Field("declined", LongType, resolve = _.value.declined),
        Field("interview", LongType, resolve = _.value.interview),
        Field("hired", LongType, resolve = _.value.hired),
        Field("rejected", LongType, resolve = _.value.rejected)
      )
    )
  lazy val analyticsTimeToHireType
      : ObjectType[RequestContext, com.example.graphQL.cats.repository.protocol.AnalyticsTimeToHire] =
    ObjectType(
      "AnalyticsTimeToHire",
      fields[RequestContext, com.example.graphQL.cats.repository.protocol.AnalyticsTimeToHire](
        Field("p50Hours", FloatType, resolve = _.value.p50Hours),
        Field("p75Hours", FloatType, resolve = _.value.p75Hours),
        Field("p90Hours", FloatType, resolve = _.value.p90Hours),
        Field("p95Hours", FloatType, resolve = _.value.p95Hours),
        Field("eligibleCount", LongType, resolve = _.value.eligibleCount),
        Field("excludedCount", LongType, resolve = _.value.excludedCount)
      )
    )
  lazy val analyticsSkillPostingDayType
      : ObjectType[RequestContext, com.example.graphQL.cats.repository.protocol.AnalyticsSkillPostingDay] =
    ObjectType(
      "AnalyticsSkillPostingDay",
      fields[RequestContext, com.example.graphQL.cats.repository.protocol.AnalyticsSkillPostingDay](
        instantField("day", _.day),
        Field("skill", StringType, resolve = _.value.skill),
        Field("postings", LongType, resolve = _.value.postings)
      )
    )
  lazy val analyticsReportType: ObjectType[RequestContext, AnalyticsReportPayload] =
    ObjectType(
      "AnalyticsReport",
      fields[RequestContext, AnalyticsReportPayload](
        instantField("asOf", _.snapshot.asOf),
        Field("funnel", ListType(analyticsFunnelDayType), resolve = _.value.snapshot.funnel),
        Field("timeToHire", OptionType(analyticsTimeToHireType), resolve = _.value.snapshot.timeToHire),
        Field(
          "skillPostingActivity",
          ListType(analyticsSkillPostingDayType),
          resolve = _.value.snapshot.skillPostingActivity
        )
      )
    )

  lazy val createJobResultType: OutputType[MutationOutcome[Job]] = mutationResultType("CreateJobResult", jobType)
  lazy val updateJobResultType: OutputType[MutationOutcome[Job]] = mutationResultType("UpdateJobResult", jobType)
  lazy val publishJobResultType: OutputType[MutationOutcome[Job]] = mutationResultType("PublishJobResult", jobType)
  lazy val closeJobResultType: OutputType[MutationOutcome[Job]] = mutationResultType("CloseJobResult", jobType)
  lazy val submitApplicationResultType: OutputType[MutationOutcome[Application]] =
    mutationResultType("SubmitApplicationResult", applicationType)
  lazy val acceptApplicationResultType: OutputType[MutationOutcome[Application]] =
    mutationResultType("AcceptApplicationResult", applicationType)
  lazy val interviewApplicationResultType: OutputType[MutationOutcome[Application]] =
    mutationResultType("MoveApplicationToInterviewResult", applicationType)
  lazy val hireApplicationResultType: OutputType[MutationOutcome[Application]] =
    mutationResultType("HireApplicationResult", applicationType)
  lazy val rejectApplicationResultType: OutputType[MutationOutcome[Application]] =
    mutationResultType("RejectApplicationResult", applicationType)
  lazy val declineApplicationResultType: OutputType[MutationOutcome[Application]] =
    mutationResultType("DeclineApplicationResult", applicationType)
  lazy val signUpResultType: OutputType[MutationOutcome[AuthSuccess]] =
    mutationResultType("SignUpResult", authSuccessType)
  lazy val loginResultType: OutputType[MutationOutcome[AuthSuccess]] =
    mutationResultType("LoginResult", authSuccessType)
  lazy val bootstrapAdminResultType: OutputType[MutationOutcome[AuthSuccess]] =
    mutationResultType("BootstrapAdminResult", authSuccessType)
  lazy val updateMyProfileResultType: OutputType[MutationOutcome[User]] =
    mutationResultType("UpdateMyProfileResult", userType)
  lazy val deleteMyAccountResultType: OutputType[MutationOutcome[DeletionSuccess]] =
    mutationResultType("DeleteMyAccountResult", deletionSuccessType)
  lazy val recordJobViewResultType: OutputType[MutationOutcome[InteractionSuccess]] =
    mutationResultType("RecordJobViewResult", interactionSuccessType)
  lazy val recordSearchResultClickResultType: OutputType[MutationOutcome[InteractionSuccess]] =
    mutationResultType("RecordSearchResultClickResult", interactionSuccessType)

  private def edgeType[A](name: String, nodeType: OutputType[A]): ObjectType[RequestContext, Edge[A]] =
    ObjectType(
      name,
      fields[RequestContext, Edge[A]](
        Field("node", nodeType, resolve = _.value.node),
        Field("cursor", StringType, resolve = _.value.cursor)
      )
    )

  private def connectionType[A](
      name: String,
      edgeType: OutputType[Edge[A]]
  ): ObjectType[RequestContext, Connection[A]] =
    ObjectType(
      name,
      fields[RequestContext, Connection[A]](
        Field("edges", ListType(edgeType), resolve = _.value.edges),
        Field("pageInfo", pageInfoType, resolve = _.value.pageInfo),
        Field("searchId", OptionType(IDType), resolve = _.value.searchId)
      )
    )

  private def mutationResultType[A](
      name: String,
      successType: ObjectType[RequestContext, A]
  ): OutputType[MutationOutcome[A]] =
    UnionType[RequestContext](name, List(successType, validationErrorType, domainErrorType))
      .mapValue[MutationOutcome[A]](identity)
}
