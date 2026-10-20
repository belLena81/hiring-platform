package com.example.graphQL.cats.api.graphql

import com.example.graphQL.cats.api.graphql.HiringGraphQLFetchers.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLInterviewResolvers.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLAccountResolvers.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLApplicationResolvers.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLReportingResolvers.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLInteractionResolvers.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLJobResolvers.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLSearchResolvers.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLReportingTypes.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLTypes.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLDsl.resultField
import com.example.graphQL.cats.domain.model.ApplicationStatus
import com.example.graphQL.cats.api.graphql.QueryAdmission.*
import sangria.execution.deferred.DeferredResolver
import sangria.schema.*

private[graphql] object HiringGraphQLSchemaAssembly {
  private def interviewActionField[In](name: String, description: String, input: Argument[In])(
      resolve: Context[RequestContext, Unit] => HiringGraphQLResult[
        HiringGraphQLModel.MutationOutcome[com.example.graphQL.cats.domain.workflow.InterviewWorkflow]
      ]
  ): Field[RequestContext, Unit] =
    resultField(
      name,
      interviewActionResultType,
      description,
      input :: Nil,
      complexity = Some((_, _, child) => InterviewActionComplexity + child)
    )(resolve)

  lazy val deferredResolver: DeferredResolver[RequestContext] =
    DeferredResolver.fetchers(usersFetcher, jobsFetcher, emailVisibilityFetcher)

  lazy val queryType: ObjectType[RequestContext, Unit] = ObjectType(
    "Query",
    "The read operations. Every operation except introspection requires authentication.",
    fields[RequestContext, Unit](
      resultField(
        "interviewWorkflow",
        OptionType(interviewWorkflowType),
        "An interview workflow the caller may see. Null when it does not exist or is not visible to the caller, so " +
          "existence is not revealed.",
        workflowIdArgument :: Nil
      )(interviewWorkflow),
      resultField("me", OptionType(userType), "The calling user.")(accountMe(_).map(Some(_))),
      resultField(
        "analyticsReport",
        analyticsReportType,
        "Hiring analytics for a period of at most 30 days. Admin only.",
        analyticsFromArgument :: analyticsToArgument :: Nil
      )(analyticsReport),
      resultField(
        "embeddingCoverage",
        embeddingCoverageReportType,
        "Health of the search embeddings: counts only, no identifiers or content. Admin only.",
        expectedModelArgument :: Nil,
        complexity = Some(coverageComplexity)
      )(embeddingCoverage),
      resultField(
        "accountDeletionStatus",
        accountDeletionStatusType,
        "Progress of an account deletion requested by the caller.",
        deletionReceiptIdArgument :: Nil
      )(accountDeletionStatus),
      resultField(
        "users",
        userConnectionType,
        "A page of users, newest first. Admin only.",
        firstArgument :: afterArgument :: userRoleArgument :: userStatusArgument :: Nil,
        complexity = Some(connectionComplexity)
      )(users),
      resultField(
        "jobs",
        jobSearchConnectionType,
        "A page of open jobs matching the filter, newest first. The result carries the identifier of the search.",
        jobFilterArgument :: firstArgument :: afterArgument :: searchIdArgument :: Nil,
        complexity = Some(connectionComplexity)
      )(jobs),
      resultField(
        "nearbyJobs",
        nearbyJobsConnectionType,
        "Open jobs within a radius of a point, nearest first. Each edge carries its distance." + CostNote +
          "." + ExpensiveRootCaps,
        nearbyCenterArgument :: radiusKmArgument :: jobFilterArgument :: firstArgument :: afterArgument :: Nil,
        complexity = Some(expensivePageComplexity)
      )(nearbyJobs),
      resultField(
        "jobDiscoveryFacets",
        OptionType(jobDiscoveryFacetsResultType),
        "Facet counts over the open jobs that match the filter, an optional radius and an optional lexical query. " +
          s"Cost: 100 + cost of the selected subfields, or 100 + ${LexicalFacetPageEquivalent.toInt} * (1 + cost of the " +
          "selected subfields) when " +
          "`query` is given." + ExpensiveRootCaps,
        optionalFacetQueryArgument :: jobFilterArgument :: optionalNearbyCenterArgument :: optionalRadiusKmArgument :: Nil,
        complexity = Some(facetComplexity)
      )(jobDiscoveryFacets),
      resultField(
        "semanticJobSearch",
        OptionType(semanticJobSearchResultType),
        "Open jobs ranked by relevance to a query. A bounded top-N list of at most `first` results: it has no " +
          "pageInfo and cannot be paged." + CostNote + "." + ExpensiveRootCaps,
        queryArgument :: jobFilterArgument :: firstArgument :: searchIdArgument :: Nil,
        complexity = Some(expensivePageComplexity)
      )(semanticJobSearch),
      resultField(
        "recommendedJobs",
        OptionType(recommendedJobsResultType),
        "Open jobs ranked against the calling candidate's profile. A bounded top-N list of at most `first` results: " +
          "it has no pageInfo and cannot be paged." + CostNote + "." + ExpensiveRootCaps,
        firstArgument :: searchIdArgument :: Nil,
        complexity = Some(expensivePageComplexity)
      )(recommendedJobs),
      resultField(
        "candidateMatches",
        OptionType(candidateMatchesResultType),
        "Candidates ranked against a job the caller owns. A bounded top-N list of at most `first` results: it has " +
          "no pageInfo and cannot be paged." + CostNote + "." + ExpensiveRootCaps,
        jobIdArgument :: candidateSearchQueryArgument :: candidateMatchFilterArgument :: firstArgument :: searchIdArgument :: Nil,
        complexity = Some(expensivePageComplexity)
      )(candidateMatches),
      resultField("job", OptionType(jobType), "A job the caller may see.", idArgument :: Nil)(job(_).map(Some(_))),
      resultField(
        "myJobs",
        jobConnectionType,
        "A page of the calling recruiter's jobs, newest first.",
        firstArgument :: afterArgument :: jobStatusArgument :: Nil,
        complexity = Some(connectionComplexity)
      )(myJobs),
      resultField(
        "myApplications",
        applicationConnectionType,
        "A page of the calling candidate's applications, newest first.",
        firstArgument :: afterArgument :: applicationStatusArgument :: Nil,
        complexity = Some(connectionComplexity)
      )(myApplications),
      resultField(
        "jobApplications",
        applicationConnectionType,
        "A page of the applications to a job the caller owns, newest first.",
        jobIdArgument :: firstArgument :: afterArgument :: applicationStatusArgument :: Nil,
        complexity = Some(connectionComplexity)
      )(jobApplications),
      resultField(
        "applicationHistory",
        applicationEventConnectionType,
        "The append-only status history of an application the caller may see.",
        applicationIdArgument :: firstArgument :: afterArgument :: Nil,
        complexity = Some(connectionComplexity)
      )(applicationHistory)
    )
  )

  private val Idempotent = " Repeating the same idempotencyKey with the same input replays the original result."
  private val InterviewRules =
    " A refusal is returned as a typed DomainError; RATE_LIMITED carries retryAfterSeconds."

  lazy val mutationType: ObjectType[RequestContext, Unit] = ObjectType(
    "Mutation",
    "The write operations. Each takes one input object and returns a union of the success type and typed errors.",
    fields[RequestContext, Unit](
      resultField(
        "scheduleInterview",
        scheduleInterviewResultType,
        "Schedules an interview for an accepted application.",
        scheduleInterviewInputArgument :: Nil
      )(scheduleInterview),
      resultField(
        "repairInterviewWorkflow",
        repairInterviewResultType,
        "Repairs an interview workflow that stopped and needs attention. Admin only.",
        repairInterviewInputArgument :: Nil
      )(repairInterviewWorkflow),
      interviewActionField(
        "cancelInterview",
        "Cancels an interview, which rejects the application." + InterviewRules,
        interviewActionInputArgument
      )(cancelInterview),
      interviewActionField(
        "requestInterviewReschedule",
        "The candidate flags that a different time is wanted; no slot changes." + InterviewRules,
        interviewActionInputArgument
      )(requestInterviewReschedule),
      interviewActionField(
        "dismissInterviewRescheduleRequest",
        "A recruiter or Admin dismisses an open reschedule request." + InterviewRules,
        interviewActionInputArgument
      )(dismissInterviewRescheduleRequest),
      interviewActionField(
        "proposeInterviewReschedule",
        "A recruiter or Admin proposes another time; it only takes effect if the candidate accepts it in time." + InterviewRules,
        proposeInterviewRescheduleInputArgument
      )(proposeInterviewReschedule),
      interviewActionField(
        "withdrawInterviewReschedule",
        "Withdraws the open reschedule proposal." + InterviewRules,
        interviewActionInputArgument
      )(withdrawInterviewReschedule),
      interviewActionField(
        "acceptInterviewReschedule",
        "The candidate accepts the open proposal; the interview moves to the proposed time." + InterviewRules,
        interviewActionInputArgument
      )(acceptInterviewReschedule),
      interviewActionField(
        "declineInterviewReschedule",
        "The candidate declines the open proposal; the interview keeps its time." + InterviewRules,
        interviewActionInputArgument
      )(declineInterviewReschedule),
      resultField(
        "submitApplication",
        submitApplicationResultType,
        "Applies to an open job as the calling candidate; a second application to the same job is refused." +
          Idempotent,
        submitApplicationInputArgument :: Nil
      )(submitApplication),
      resultField(
        "createJob",
        createJobResultType,
        "Creates an open job owned by the calling recruiter." + Idempotent,
        createJobInputArgument :: Nil
      )(createJob),
      resultField(
        "updateJob",
        updateJobResultType,
        "Replaces the content of a job the caller owns." + Idempotent,
        updateJobInputArgument :: Nil
      )(updateJob),
      resultField(
        "publishJob",
        publishJobResultType,
        "Publishes a draft job the caller owns." + Idempotent,
        jobActionInputArgument :: Nil
      )(context => changeJob(context, _.publishJob)),
      resultField(
        "closeJob",
        closeJobResultType,
        "Closes a job the caller owns; closed jobs accept no new applications." + Idempotent,
        jobActionInputArgument :: Nil
      )(context => changeJob(context, _.closeJob)),
      resultField(
        "acceptApplication",
        acceptApplicationResultType,
        "Accepts an application to a job the caller owns." + Idempotent,
        applicationActionInputArgument :: Nil
      )(context => applicationStatusAction(context, ApplicationStatus.Accepted)),
      resultField(
        "moveApplicationToInterview",
        interviewApplicationResultType,
        "Moves an application to the interview stage." + Idempotent,
        applicationActionInputArgument :: Nil
      )(context => applicationStatusAction(context, ApplicationStatus.Interview)),
      resultField(
        "hireApplication",
        hireApplicationResultType,
        "Hires the candidate of an application." + Idempotent,
        applicationActionInputArgument :: Nil
      )(context => applicationStatusAction(context, ApplicationStatus.Hired)),
      resultField(
        "rejectApplication",
        rejectApplicationResultType,
        "Rejects an application; feedback is required." + Idempotent,
        rejectApplicationInputArgument :: Nil
      )(rejectApplication),
      resultField(
        "declineApplication",
        declineApplicationResultType,
        "Declines an application as its candidate; a reason is required." + Idempotent,
        declineApplicationInputArgument :: Nil
      )(declineApplication),
      resultField(
        "signUp",
        signUpResultType,
        "Creates a Candidate or Recruiter account and signs it in. The caller is not authenticated, so rate-limit " +
          "refusals are top-level errors with extensions.code RATE_LIMITED; expected refusals are typed errors, and availability failures (SERVICE_NOT_READY, UNAVAILABLE) are top-level errors." +
          Idempotent,
        signUpInputArgument :: Nil
      )(signUp),
      resultField(
        "login",
        loginResultType,
        "Signs in with a name and password. The caller is not authenticated, so rate-limit refusals are top-level " +
          "errors with extensions.code RATE_LIMITED; expected refusals are typed errors, and availability failures (SERVICE_NOT_READY, UNAVAILABLE) are top-level errors.",
        loginInputArgument :: Nil
      )(login),
      resultField(
        "updateMyProfile",
        updateMyProfileResultType,
        "Replaces the caller's whole profile; omitted optional parts are cleared. The variant must match the caller's " +
          "role." + Idempotent,
        updateProfileInputArgument :: Nil
      )(updateMyProfile),
      resultField(
        "deleteMyAccount",
        deleteMyAccountResultType,
        "Requests deletion of the caller's account and returns a receipt to poll with accountDeletionStatus." +
          Idempotent,
        deleteMyAccountInputArgument :: Nil
      )(deleteMyAccount),
      resultField(
        "recordJobView",
        recordJobViewResultType,
        "Records that the caller viewed a job." + Idempotent,
        recordJobViewInputArgument :: Nil
      )(recordJobView),
      resultField(
        "recordSearchResultClick",
        recordSearchResultClickResultType,
        "Records that the caller opened a search result." + Idempotent,
        recordSearchResultClickInputArgument :: Nil
      )(recordSearchResultClick)
    )
  )

  lazy val schema: Schema[RequestContext, Unit] = Schema(queryType, Some(mutationType))
}
