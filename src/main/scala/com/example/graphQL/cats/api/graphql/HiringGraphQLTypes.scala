package com.example.graphQL.cats.api.graphql

import com.example.graphQL.cats.api.graphql.HiringGraphQLFetchers.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLDsl.{
  describedNames,
  facetsOutcomeUnion,
  outcomeUnion,
  searchOutcomeUnion
}
import com.example.graphQL.cats.service.search.*
import sangria.schema.*
import sangria.schema.Action.deferredAction

private[graphql] object HiringGraphQLTypes {
  private[graphql] def simple[T, V](name: String, tpe: OutputType[V])(get: T => V): Field[RequestContext, T] =
    Field(name, tpe, resolve = context => get(context.value))

  lazy val interviewWorkflowType
      : ObjectType[RequestContext, com.example.graphQL.cats.domain.workflow.InterviewWorkflow] = ObjectType(
    "InterviewWorkflow",
    fields[RequestContext, com.example.graphQL.cats.domain.workflow.InterviewWorkflow](
      simple("id", interviewWorkflowIdType)(_.id),
      simple("applicationId", applicationIdType)(_.applicationId),
      simple("startsAt", instantType)(_.interval.startsAt),
      simple("endsAt", instantType)(_.interval.endsAt),
      simple("revision", longType)(_.revision),
      simple("progress", interviewProgressType)(_.phase),
      simple("notifiedParticipants", ListType(interviewParticipantType))(_.notified.toList.sortBy(_.ordinal)),
      // The replacement interval held after the candidate accepted a proposal, until the swap settles.
      simple("pendingStartsAt", OptionType(instantType))(_.pendingInterval.map(_.startsAt)),
      simple("pendingEndsAt", OptionType(instantType))(_.pendingInterval.map(_.endsAt)),
      simple("cancelledAt", OptionType(instantType))(_.cancelledAt),
      // The open reschedule proposal, if any. Who proposed it is not exposed.
      simple("proposedStartsAt", OptionType(instantType))(_.proposal.map(_.interval.startsAt)),
      simple("proposedEndsAt", OptionType(instantType))(_.proposal.map(_.interval.endsAt)),
      simple("proposalExpiresAt", OptionType(instantType))(_.proposal.map(_.expiresAt)),
      simple("rescheduleRequested", BooleanType)(_.rescheduleRequestedAt.nonEmpty)
    )
  )
  lazy val scheduleInterviewResultType = mutationResultType("ScheduleInterviewResult", interviewWorkflowType)
  lazy val interviewActionResultType = mutationResultType("InterviewActionResult", interviewWorkflowType)
  lazy val repairInterviewResultType = mutationResultType("RepairInterviewResult", interviewWorkflowType)
  lazy val errorCodeType: EnumType[String] =
    describedNames(
      "ErrorCode",
      "A stable, machine-readable error code; the same strings appear as extensions.code in top-level errors.",
      GraphQLFailureCatalog.errorCodes.map { case (code, description) => (code, code, description) }
    )
  lazy val userErrorType: InterfaceType[RequestContext, UserError] = InterfaceType(
    "UserError",
    "A refusal returned as data instead of a top-level error.",
    fields[RequestContext, UserError](
      Field("code", errorCodeType, description = Some("The stable error code."), resolve = _.value.code),
      Field(
        "message",
        StringType,
        description = Some("A sanitized, human-readable message."),
        resolve = _.value.message
      )
    )
  )
  lazy val validationErrorType: ObjectType[RequestContext, ValidationError] = ObjectType(
    "ValidationError",
    "One or more input fields were invalid.",
    List(PossibleInterface[RequestContext, ValidationError](userErrorType)),
    fields[RequestContext, ValidationError](
      Field("code", errorCodeType, description = Some("The stable error code."), resolve = _.value.code),
      Field(
        "message",
        StringType,
        description = Some("All violated rules, joined into one message."),
        resolve = _.value.message
      ),
      Field(
        "field",
        OptionType(StringType),
        description = Some(
          "The GraphQL input path of the first violated rule, for example profile.candidate.residence.country or " +
            "job.title; null when that rule has no input counterpart."
        ),
        resolve = _.value.field
      )
    )
  )
  lazy val searchUnavailableType: ObjectType[RequestContext, SearchUnavailable] = ObjectType(
    "SearchUnavailable",
    "A search that is valid and permitted but cannot be served right now; sibling fields are unaffected.",
    List(PossibleInterface[RequestContext, SearchUnavailable](userErrorType)),
    fields[RequestContext, SearchUnavailable](
      Field("code", errorCodeType, description = Some("The stable error code."), resolve = _.value.code),
      Field(
        "message",
        StringType,
        description = Some("A sanitized, human-readable message."),
        resolve = _.value.message
      ),
      Field(
        "retryable",
        BooleanType,
        description = Some("True when repeating the same request later may succeed."),
        resolve = _.value.retryable
      )
    )
  )
  lazy val domainErrorType: ObjectType[RequestContext, DomainError] = ObjectType(
    "DomainError",
    "A business rule or authorization refusal.",
    List(PossibleInterface[RequestContext, DomainError](userErrorType)),
    fields[RequestContext, DomainError](
      Field("code", errorCodeType, description = Some("The stable error code."), resolve = _.value.code),
      Field(
        "message",
        StringType,
        description = Some("A sanitized, human-readable message."),
        resolve = _.value.message
      ),
      Field(
        "retryAfterSeconds",
        OptionType(IntType),
        description = Some("Seconds to wait before retrying; set only when a rate limit refused the call."),
        resolve = _.value.retryAfterSeconds
      )
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
      simple("city", OptionType(StringType))(_.city),
      simple("remote", BooleanType)(_.remote),
      simple("coordinates", OptionType(geoPointType))(_.coordinates)
    )
  )
  lazy val geoPointType: ObjectType[RequestContext, GeoPoint] = ObjectType(
    "GeoPoint",
    fields[RequestContext, GeoPoint](
      simple("latitude", FloatType)(_.latitude),
      simple("longitude", FloatType)(_.longitude)
    )
  )
  lazy val nearbyJobEdgeType: ObjectType[RequestContext, NearbyJobEdge] = ObjectType(
    "NearbyJobEdge",
    fields[RequestContext, NearbyJobEdge](
      simple("node", jobType)(_.node),
      simple("distanceKm", FloatType)(_.distanceKm),
      simple("cursor", StringType)(_.cursor)
    )
  )
  lazy val nearbyJobsConnectionType: ObjectType[RequestContext, NearbyJobsConnection] = ObjectType(
    "NearbyJobsConnection",
    fields[RequestContext, NearbyJobsConnection](
      simple("edges", ListType(nearbyJobEdgeType))(_.edges),
      simple("pageInfo", pageInfoType)(_.pageInfo)
    )
  )
  lazy val jobFacetBucketType: ObjectType[RequestContext, com.example.graphQL.cats.service.search.JobFacetBucket] =
    ObjectType(
      "JobFacetBucket",
      fields[RequestContext, com.example.graphQL.cats.service.search.JobFacetBucket](
        simple("value", StringType)(_.value),
        simple("count", longType)(_.count)
      )
    )
  lazy val jobDiscoveryFacetsType
      : ObjectType[RequestContext, com.example.graphQL.cats.service.search.JobDiscoveryFacets] =
    ObjectType(
      "JobDiscoveryFacets",
      fields[RequestContext, com.example.graphQL.cats.service.search.JobDiscoveryFacets](
        simple("skills", ListType(jobFacetBucketType))(_.skills),
        simple("countries", ListType(jobFacetBucketType))(_.countries),
        simple("cities", ListType(jobFacetBucketType))(_.cities),
        simple("remote", ListType(jobFacetBucketType))(_.remote),
        simple("truncated", BooleanType)(_.truncated),
        Field(
          "coverage",
          facetCoverageType,
          resolve = _.value.coverage match {
            case FacetCoverage.AllMatches    => "ALL_MATCHES"
            case FacetCoverage.TopMatches(_) => "TOP_MATCHES"
          }
        ),
        Field(
          "coverageLimit",
          OptionType(IntType),
          resolve = _.value.coverage match {
            case FacetCoverage.AllMatches        => None
            case FacetCoverage.TopMatches(limit) => Some(limit)
          }
        )
      )
    )
  lazy val candidateProfileType: ObjectType[RequestContext, CandidateProfileView] = ObjectType(
    "CandidateProfile",
    fields[RequestContext, CandidateProfileView](
      Field("skills", ListType(StringType), resolve = _.value.value.skills.toList.sorted),
      Field("experienceSummary", OptionType(StringType), resolve = _.value.value.experienceSummary),
      Field("resumeRef", OptionType(StringType), resolve = _.value.value.resumeRef),
      Field(
        "currentResidence",
        OptionType(candidateResidenceType),
        resolve = context => visibleProfile(context.ctx, context.value)(_.currentResidence)
      ),
      Field(
        "availabilityStatus",
        OptionType(HiringGraphQLInputs.candidateAvailabilityStatus),
        resolve = context => visibleProfile(context.ctx, context.value)(_.availabilityStatus)
      ),
      Field(
        "recruiterSearchOptIn",
        BooleanType,
        resolve = context => visibleProfile(context.ctx, context.value)(_.recruiterSearchOptIn)
      )
    )
  )
  private def visibleProfile[A](ctx: RequestContext, view: CandidateProfileView)(
      project: CandidateProfile => A
  ): scala.concurrent.Future[A] =
    ctx.effectAdapter.resolverFuture(ctx, ctx.candidateProfileFor(view.ownerId, view.value).map(project))
  lazy val candidateResidenceType: ObjectType[RequestContext, CandidateResidence] = ObjectType(
    "CandidateResidence",
    fields[RequestContext, CandidateResidence](
      simple("country", StringType)(_.country),
      simple("city", OptionType(StringType))(_.city)
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
        simple("id", userIdType)(_.id),
        simple("name", StringType)(_.name),
        simple("profile", OptionType(candidateMatchProfileType))(_.profile)
      )
    )
  lazy val userProfileType: OutputType[GraphQLUserProfile] =
    UnionType[RequestContext](
      "UserProfile",
      Some("The role-specific profile of a user: a CandidateProfile or a RecruiterProfile."),
      List(candidateProfileType, recruiterProfileType)
    )
      .mapValue[GraphQLUserProfile] {
        case GraphQLUserProfile.Candidate(ownerId, profile) => CandidateProfileView(ownerId, profile)
        case GraphQLUserProfile.Recruiter(profile)          => profile
      }
  final case class CandidateProfileView(
      ownerId: com.example.graphQL.cats.domain.model.Identifiers.UserId,
      value: CandidateProfile
  )
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
            .map(_.flatMap(_ => context.value.email.map(_.value)))(using
              context.ctx.effectAdapter.deferredExecutionContext
            )
      ),
      simple("name", StringType)(_.name),
      simple("role", userRole)(_.role),
      simple("status", userStatus)(_.accountStatus),
      Field(
        "profile",
        OptionType(userProfileType),
        resolve = context =>
          context.value.profile.map {
            case UserProfile.Candidate(profile) => GraphQLUserProfile.Candidate(context.value.id, profile)
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
      Field(
        "recruiter",
        OptionType(userType),
        resolve = context =>
          usersFetcher
            .deferOpt(
              com.example.graphQL.cats.service.read.UserRelationKey
                .JobRecruiter(context.value.id, context.value.recruiterId)
            )
            .map(_.map(_.value))(using context.ctx.effectAdapter.deferredExecutionContext)
      )
    )
  )
  lazy val applicationType: ObjectType[RequestContext, Application] = ObjectType(
    "Application",
    fields[RequestContext, Application](
      simple("id", applicationIdType)(_.id),
      simple("status", applicationStatus)(_.status),
      instantField("createdAt", _.createdAt),
      instantField("updatedAt", _.updatedAt),
      Field(
        "candidate",
        OptionType(userType),
        resolve = context =>
          usersFetcher
            .deferOpt(
              com.example.graphQL.cats.service.read.UserRelationKey
                .ApplicationCandidate(context.value.id, context.value.candidateId)
            )
            .map(_.map(_.value))(using context.ctx.effectAdapter.deferredExecutionContext)
      ),
      Field(
        "job",
        OptionType(jobType),
        resolve = context =>
          jobsFetcher
            .deferOpt(com.example.graphQL.cats.service.read.JobRelationKey(context.value.id, context.value.jobId))
            .map(_.map(_.value))(using context.ctx.effectAdapter.deferredExecutionContext)
      )
    )
  )
  lazy val applicationEventType: ObjectType[RequestContext, ApplicationEvent] =
    ObjectType(
      "ApplicationEvent",
      fields[RequestContext, ApplicationEvent](
        simple("id", applicationEventIdType)(_.id),
        simple("previousStatus", OptionType(applicationStatus))(_.previousStatus),
        simple("newStatus", applicationStatus)(_.newStatus),
        simple("actorRole", OptionType(userRole))(_.actorRole),
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
  lazy val jobSearchConnectionType: ObjectType[RequestContext, JobSearchConnection] = ObjectType(
    "JobSearchConnection",
    fields[RequestContext, JobSearchConnection](
      simple("edges", ListType(jobEdgeType))(_.connection.edges),
      simple("pageInfo", pageInfoType)(_.connection.pageInfo),
      simple("searchId", searchIdType)(_.searchId)
    )
  )
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
  lazy val deletionReceiptType: ObjectType[RequestContext, DeletionReceipt] =
    ObjectType(
      "DeletionReceipt",
      fields[RequestContext, DeletionReceipt](
        simple("receiptId", deletionReceiptIdType)(_.receiptId),
        simple("status", accountDeletionStatusType)(_.status)
      )
    )
  lazy val interactionSuccessType: ObjectType[RequestContext, InteractionSuccess] =
    ObjectType(
      "InteractionSuccess",
      fields[RequestContext, InteractionSuccess](simple("eventId", uuidType)(_.eventId))
    )
  lazy val rankedJobType: ObjectType[RequestContext, RankedJobPayload] = ObjectType(
    "RankedJob",
    fields[RequestContext, RankedJobPayload](
      simple("job", jobType)(_.job),
      simple("score", FloatType)(_.score),
      simple("searchMode", searchMode)(_.searchMode),
      simple("model", OptionType(StringType))(_.model),
      simple("searchId", searchIdType)(_.searchId),
      Field("matchedSkills", ListType(StringType), resolve = _.value.matchedSkills),
      simple("retrievalScore", OptionType(FloatType))(_.retrievalScore)
    )
  )
  lazy val rankedCandidateType: ObjectType[RequestContext, RankedCandidatePayload] =
    ObjectType(
      "RankedCandidate",
      fields[RequestContext, RankedCandidatePayload](
        simple("candidate", candidateMatchCandidateType)(_.candidate),
        simple("score", FloatType)(_.score),
        simple("searchMode", searchMode)(_.searchMode),
        simple("model", OptionType(StringType))(_.model),
        simple("searchId", searchIdType)(_.searchId),
        Field("matchedSkills", ListType(StringType), resolve = _.value.matchedSkills),
        simple("retrievalScore", OptionType(FloatType))(_.retrievalScore)
      )
    )
  private def degradedField[A](get: A => Boolean): Field[RequestContext, A] =
    Field(
      "degraded",
      BooleanType,
      description = Some(
        "True when the semantic ranking was unavailable and these are keyword (LEXICAL) results; their relevance is " +
          "not comparable with normal results."
      ),
      resolve = context => get(context.value)
    )
  private def degradedReasonField[A](get: A => Option[String]): Field[RequestContext, A] =
    Field(
      "degradedReason",
      OptionType(errorCodeType),
      description =
        Some("Why the results are degraded (PROVIDER_UNAVAILABLE or VECTOR_SEARCH_UNAVAILABLE); null otherwise."),
      resolve = context => get(context.value)
    )
  lazy val rankedJobResultsType: ObjectType[RequestContext, RankedJobResults] =
    ObjectType(
      "RankedJobResults",
      fields[RequestContext, RankedJobResults](
        simple("results", ListType(rankedJobType))(_.results),
        degradedField(_.degraded),
        degradedReasonField(_.degradedReason)
      )
    )
  lazy val rankedCandidateResultsType: ObjectType[RequestContext, RankedCandidateResults] =
    ObjectType(
      "RankedCandidateResults",
      fields[RequestContext, RankedCandidateResults](
        simple("results", ListType(rankedCandidateType))(_.results),
        degradedField(_.degraded),
        degradedReasonField(_.degradedReason)
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
  lazy val updateMyProfileResultType: OutputType[MutationOutcome[User]] =
    mutationResultType("UpdateMyProfileResult", userType)
  lazy val deleteMyAccountResultType: OutputType[MutationOutcome[DeletionReceipt]] =
    mutationResultType("DeleteMyAccountResult", deletionReceiptType)
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
        simple("pageInfo", pageInfoType)(_.pageInfo)
      )
    )

  private def searchResultType[A](
      name: String,
      successType: ObjectType[RequestContext, A]
  ): OutputType[SearchOutcome[A]] =
    searchOutcomeUnion(
      name,
      s"The outcome of the search: ${successType.name}, or SearchUnavailable when it cannot be served now, or a " +
        "typed ValidationError or DomainError. Authentication, authorization and server failures remain top-level " +
        "errors that null only this field.",
      successType,
      searchUnavailableType,
      validationErrorType,
      domainErrorType
    )
  lazy val semanticJobSearchResultType: OutputType[SearchOutcome[RankedJobResults]] =
    searchResultType("SemanticJobSearchResult", rankedJobResultsType)
  lazy val recommendedJobsResultType: OutputType[SearchOutcome[RankedJobResults]] =
    searchResultType("RecommendedJobsResult", rankedJobResultsType)
  lazy val candidateMatchesResultType: OutputType[SearchOutcome[RankedCandidateResults]] =
    searchResultType("CandidateMatchesResult", rankedCandidateResultsType)
  lazy val jobDiscoveryFacetsResultType
      : OutputType[FacetsOutcome[com.example.graphQL.cats.service.search.JobDiscoveryFacets]] =
    facetsOutcomeUnion(
      "JobDiscoveryFacetsResult",
      "The facets, or SearchUnavailable when they cannot be computed now, or a typed ValidationError. " +
        "Authentication, authorization and server failures remain top-level errors that null only this field.",
      jobDiscoveryFacetsType,
      searchUnavailableType,
      validationErrorType
    )

  private def mutationResultType[A](
      name: String,
      successType: ObjectType[RequestContext, A]
  ): OutputType[MutationOutcome[A]] =
    outcomeUnion(
      name,
      s"The outcome of the mutation: ${successType.name} on success, or a typed ValidationError or DomainError.",
      successType,
      validationErrorType,
      domainErrorType
    )
}
