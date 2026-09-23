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
  private def simple[T, V](name: String, tpe: OutputType[V])(get: T => V): Field[RequestContext, T] =
    Field(name, tpe, resolve = context => get(context.value))

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
      simple("code", StringType)(_.code),
      simple("message", StringType)(_.message)
    )
  )
  lazy val validationErrorType: ObjectType[RequestContext, ValidationError] = ObjectType(
    "ValidationError",
    List(PossibleInterface[RequestContext, ValidationError](userErrorType)),
    fields[RequestContext, ValidationError](
      simple("code", StringType)(_.code),
      simple("message", StringType)(_.message)
    )
  )
  lazy val domainErrorType: ObjectType[RequestContext, DomainError] = ObjectType(
    "DomainError",
    List(PossibleInterface[RequestContext, DomainError](userErrorType)),
    fields[RequestContext, DomainError](
      simple("code", StringType)(_.code),
      simple("message", StringType)(_.message)
    )
  )
  lazy val pageInfoType: ObjectType[RequestContext, PageInfo] = ObjectType(
    "PageInfo",
    fields[RequestContext, PageInfo](
      simple("hasNextPage", BooleanType)(_.hasNextPage),
      simple("endCursor", OptionType(StringType))(_.endCursor)
    )
  )
  lazy val locationType: ObjectType[RequestContext, Location] = ObjectType(
    "Location",
    fields[RequestContext, Location](
      simple("country", StringType)(_.country),
      simple("city", StringType)(_.city),
      simple("remote", BooleanType)(_.remote)
    )
  )
  lazy val candidateProfileType: ObjectType[RequestContext, CandidateProfile] = ObjectType(
    "CandidateProfile",
    fields[RequestContext, CandidateProfile](
      Field("skills", ListType(StringType), resolve = _.value.skills.toList.sorted),
      simple("experienceSummary", OptionType(StringType))(_.experienceSummary),
      simple("resumeRef", OptionType(StringType))(_.resumeRef)
    )
  )
  lazy val candidateMatchProfileType: ObjectType[RequestContext, CandidateMatchProfile] =
    ObjectType(
      "CandidateMatchProfile",
      fields[RequestContext, CandidateMatchProfile](
        Field("skills", ListType(StringType), resolve = _.value.skills.toList.sorted),
        simple("experienceSummary", OptionType(StringType))(_.experienceSummary)
      )
    )
  lazy val candidateMatchCandidateType: ObjectType[RequestContext, CandidateMatchCandidate] =
    ObjectType(
      "CandidateMatchCandidate",
      fields[RequestContext, CandidateMatchCandidate](
        simple("id", IDType)(_.id),
        simple("name", StringType)(_.name),
        simple("profile", OptionType(candidateMatchProfileType))(_.profile)
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
      simple("id", userIdType)(_.id),
      Field(
        "email",
        OptionType(StringType),
        resolve = context =>
          emailVisibilityFetcher
            .deferOpt(context.value.id)
            // Sangria's deferred Future projection requires an EC; parasitic avoids a thread hop.
            .map(_.flatMap(_ => context.value.email))(using ExecutionContext.parasitic)
      ),
      simple("name", StringType)(_.name),
      simple("role", userRole)(_.role),
      simple("status", userStatus)(_.accountStatus),
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
        simple("organizationName", StringType)(_.organizationName),
        simple("jobTitle", OptionType(StringType))(_.jobTitle)
      )
    )
  lazy val jobType: ObjectType[RequestContext, Job] = ObjectType(
    "Job",
    fields[RequestContext, Job](
      simple("id", jobIdType)(_.id),
      simple("title", StringType)(_.title),
      simple("description", StringType)(_.description),
      simple("requirements", ListType(StringType))(_.requirements),
      Field("skills", ListType(StringType), resolve = _.value.skills.toList.sorted),
      simple("location", locationType)(_.location),
      simple("status", jobStatus)(_.status),
      instantField("createdAt", _.createdAt),
      instantField("updatedAt", _.updatedAt),
      Field("recruiter", OptionType(userType), resolve = context => usersFetcher.deferOpt(context.value.recruiterId))
    )
  )
  lazy val applicationType: ObjectType[RequestContext, Application] = ObjectType(
    "Application",
    fields[RequestContext, Application](
      simple("id", applicationIdType)(_.id),
      simple("status", applicationStatus)(_.status),
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
        simple("previousStatus", OptionType(applicationStatus))(_.previousStatus),
        simple("newStatus", applicationStatus)(_.newStatus),
        Field("actorId", IDType, resolve = _.value.actorId.value.toString),
        instantField("occurredAt", _.occurredAt),
        simple("feedback", OptionType(StringType))(_.feedback),
        simple("reason", OptionType(StringType))(_.reason)
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
      simple("user", userType)(_.user),
      simple("accessToken", StringType)(_.accessToken),
      simple("expiresAt", instantType)(_.expiresAt)
    )
  )
  lazy val deletionSuccessType: ObjectType[RequestContext, DeletionSuccess] =
    ObjectType(
      "DeletionSuccess",
      fields[RequestContext, DeletionSuccess](simple("deleted", BooleanType)(_.deleted))
    )
  lazy val interactionSuccessType: ObjectType[RequestContext, InteractionSuccess] =
    ObjectType(
      "InteractionSuccess",
      fields[RequestContext, InteractionSuccess](simple("recorded", BooleanType)(_.recorded))
    )
  lazy val rankedJobType: ObjectType[RequestContext, RankedJobPayload] = ObjectType(
    "RankedJob",
    fields[RequestContext, RankedJobPayload](
      simple("job", jobType)(_.job),
      simple("score", FloatType)(_.score),
      simple("searchMode", searchMode)(_.searchMode),
      simple("model", StringType)(_.model),
      simple("searchId", IDType)(_.searchId)
    )
  )
  lazy val rankedCandidateType: ObjectType[RequestContext, RankedCandidatePayload] =
    ObjectType(
      "RankedCandidate",
      fields[RequestContext, RankedCandidatePayload](
        simple("candidate", candidateMatchCandidateType)(_.candidate),
        simple("score", FloatType)(_.score),
        simple("searchMode", searchMode)(_.searchMode),
        simple("model", StringType)(_.model),
        simple("searchId", IDType)(_.searchId)
      )
    )
  lazy val rankedJobResultsType: ObjectType[RequestContext, RankedJobResults] =
    ObjectType(
      "RankedJobResults",
      fields[RequestContext, RankedJobResults](simple("results", ListType(rankedJobType))(_.results))
    )
  lazy val rankedCandidateResultsType: ObjectType[RequestContext, RankedCandidateResults] =
    ObjectType(
      "RankedCandidateResults",
      fields[RequestContext, RankedCandidateResults](
        simple("results", ListType(rankedCandidateType))(_.results)
      )
    )
  lazy val analyticsFunnelDayType
      : ObjectType[RequestContext, com.example.graphQL.cats.repository.protocol.AnalyticsFunnelDay] =
    ObjectType(
      "AnalyticsFunnelDay",
      fields[RequestContext, com.example.graphQL.cats.repository.protocol.AnalyticsFunnelDay](
        instantField("day", _.day),
        simple("created", LongType)(_.created),
        simple("accepted", LongType)(_.accepted),
        simple("declined", LongType)(_.declined),
        simple("interview", LongType)(_.interview),
        simple("hired", LongType)(_.hired),
        simple("rejected", LongType)(_.rejected)
      )
    )
  lazy val analyticsTimeToHireType
      : ObjectType[RequestContext, com.example.graphQL.cats.repository.protocol.AnalyticsTimeToHire] =
    ObjectType(
      "AnalyticsTimeToHire",
      fields[RequestContext, com.example.graphQL.cats.repository.protocol.AnalyticsTimeToHire](
        simple("p50Hours", FloatType)(_.p50Hours),
        simple("p75Hours", FloatType)(_.p75Hours),
        simple("p90Hours", FloatType)(_.p90Hours),
        simple("p95Hours", FloatType)(_.p95Hours),
        simple("eligibleCount", LongType)(_.eligibleCount),
        simple("excludedCount", LongType)(_.excludedCount)
      )
    )
  lazy val analyticsSkillPostingDayType
      : ObjectType[RequestContext, com.example.graphQL.cats.repository.protocol.AnalyticsSkillPostingDay] =
    ObjectType(
      "AnalyticsSkillPostingDay",
      fields[RequestContext, com.example.graphQL.cats.repository.protocol.AnalyticsSkillPostingDay](
        instantField("day", _.day),
        simple("skill", StringType)(_.skill),
        simple("postings", LongType)(_.postings)
      )
    )
  lazy val analyticsReportType: ObjectType[RequestContext, AnalyticsReportPayload] =
    ObjectType(
      "AnalyticsReport",
      fields[RequestContext, AnalyticsReportPayload](
        instantField("asOf", _.snapshot.asOf),
        simple("funnel", ListType(analyticsFunnelDayType))(_.snapshot.funnel),
        simple("timeToHire", OptionType(analyticsTimeToHireType))(_.snapshot.timeToHire),
        simple("skillPostingActivity", ListType(analyticsSkillPostingDayType))(_.snapshot.skillPostingActivity)
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
        simple("node", nodeType)(_.node),
        simple("cursor", StringType)(_.cursor)
      )
    )

  private def connectionType[A](
      name: String,
      edgeType: OutputType[Edge[A]]
  ): ObjectType[RequestContext, Connection[A]] =
    ObjectType(
      name,
      fields[RequestContext, Connection[A]](
        simple("edges", ListType(edgeType))(_.edges),
        simple("pageInfo", pageInfoType)(_.pageInfo),
        simple("searchId", OptionType(IDType))(_.searchId)
      )
    )

  private def mutationResultType[A](
      name: String,
      successType: ObjectType[RequestContext, A]
  ): OutputType[MutationOutcome[A]] =
    UnionType[RequestContext](name, List(successType, validationErrorType, domainErrorType))
      .mapValue[MutationOutcome[A]](identity)
}
