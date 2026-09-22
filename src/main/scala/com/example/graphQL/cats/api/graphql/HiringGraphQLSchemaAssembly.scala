package com.example.graphQL.cats.api.graphql

import com.example.graphQL.cats.api.graphql.HiringGraphQLFetchers.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLAccountResolvers.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLApplicationResolvers.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLAnalyticsResolvers.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLInteractionResolvers.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLJobResolvers.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLSearchResolvers.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLTypes.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLDsl.ioField
import com.example.graphQL.cats.domain.model.ApplicationStatus
import sangria.execution.QueryReducer
import sangria.execution.deferred.DeferredResolver
import sangria.schema.*

private[graphql] object HiringGraphQLSchemaAssembly {
  private val MaxQueryDepth = 16
  private val MaxQueryComplexity = 1000d
  private val connectionComplexity: (RequestContext, Args, Double) => Double =
    (_, args, child) => 1d + args.arg(firstArgument) * child
  final case class QueryComplexityExceeded(limit: Double)
      extends IllegalArgumentException(s"Query complexity exceeds $limit")

  lazy val queryReducers: List[QueryReducer[RequestContext, ?]] = List(
    QueryReducer.rejectMaxDepth[RequestContext](MaxQueryDepth),
    QueryReducer.rejectComplexQueries[RequestContext](MaxQueryComplexity, (_, _) =>
      QueryComplexityExceeded(MaxQueryComplexity))
  )

  lazy val deferredResolver: DeferredResolver[RequestContext] =
    DeferredResolver.fetchers(usersFetcher, jobsFetcher, emailVisibilityFetcher)

  lazy val queryType: ObjectType[RequestContext, Unit] = ObjectType("Query", fields[RequestContext, Unit](
    Field("health", healthType, resolve = _ => ()),
    ioField("readiness", readinessType)(context => context.ctx.readiness),
    ioField("me", OptionType(userType))(accountMe),
    ioField("analyticsReport", analyticsReportType, analyticsFromArgument :: analyticsToArgument :: Nil)(analyticsReport),
    ioField("users", userConnectionType, firstArgument :: afterArgument :: userRoleArgument :: userStatusArgument :: Nil,
      complexity = Some(connectionComplexity))(users),
    ioField("jobs", jobConnectionType, firstArgument :: afterArgument :: cityArgument :: skillsArgument :: createdAfterArgument :: searchIdArgument :: Nil,
      complexity = Some(connectionComplexity))(jobs),
    ioField("semanticJobSearch", rankedJobResultsType, queryArgument :: jobFilterArgument :: firstArgument :: searchIdArgument :: Nil)(semanticJobSearch),
    ioField("recommendedJobs", rankedJobResultsType, firstArgument :: searchIdArgument :: Nil)(recommendedJobs),
    ioField("candidateMatches", rankedCandidateResultsType, jobIdArgument :: firstArgument :: searchIdArgument :: Nil)(candidateMatches),
    ioField("job", OptionType(jobType), idArgument :: Nil)(job),
    ioField("myJobs", jobConnectionType, firstArgument :: afterArgument :: jobStatusArgument :: Nil,
      complexity = Some(connectionComplexity))(myJobs),
    ioField("myApplications", applicationConnectionType, firstArgument :: afterArgument :: applicationStatusArgument :: Nil,
      complexity = Some(connectionComplexity))(myApplications),
    ioField("jobApplications", applicationConnectionType, jobIdArgument :: firstArgument :: afterArgument :: applicationStatusArgument :: Nil,
      complexity = Some(connectionComplexity))(jobApplications),
    ioField("applicationHistory", applicationEventConnectionType, applicationIdArgument :: firstArgument :: afterArgument :: Nil,
      complexity = Some(connectionComplexity))(applicationHistory)
  ))

  lazy val mutationType: ObjectType[RequestContext, Unit] = ObjectType("Mutation", fields[RequestContext, Unit](
    ioField("submitApplication", submitApplicationResultType, submitApplicationInputArgument :: Nil)(submitApplication),
    ioField("createJob", createJobResultType, createJobInputArgument :: Nil)(createJob),
    ioField("updateJob", updateJobResultType, updateJobInputArgument :: Nil)(updateJob),
    ioField("publishJob", publishJobResultType, jobActionInputArgument :: Nil)(context => changeJob(context, _.publishJob)),
    ioField("closeJob", closeJobResultType, jobActionInputArgument :: Nil)(context => changeJob(context, _.closeJob)),
    ioField("acceptApplication", acceptApplicationResultType, applicationActionInputArgument :: Nil)(context => applicationStatusAction(context, ApplicationStatus.Accepted)),
    ioField("moveApplicationToInterview", interviewApplicationResultType, applicationActionInputArgument :: Nil)(context => applicationStatusAction(context, ApplicationStatus.Interview)),
    ioField("hireApplication", hireApplicationResultType, applicationActionInputArgument :: Nil)(context => applicationStatusAction(context, ApplicationStatus.Hired)),
    ioField("rejectApplication", rejectApplicationResultType, rejectApplicationInputArgument :: Nil)(rejectApplication),
    ioField("declineApplication", declineApplicationResultType, declineApplicationInputArgument :: Nil)(declineApplication),
    ioField("signUp", signUpResultType, signUpInputArgument :: Nil)(signUp),
    ioField("login", loginResultType, loginInputArgument :: Nil)(login),
    ioField("bootstrapAdmin", bootstrapAdminResultType, bootstrapAdminInputArgument :: Nil)(bootstrapAdmin),
    ioField("updateMyProfile", updateMyProfileResultType, updateProfileInputArgument :: Nil)(updateMyProfile),
    ioField("deleteMyAccount", deleteMyAccountResultType, deleteMyAccountInputArgument :: Nil)(deleteMyAccount),
    ioField("recordJobView", recordJobViewResultType, recordJobViewInputArgument :: Nil)(recordJobView),
    ioField("recordSearchResultClick", recordSearchResultClickResultType, recordSearchResultClickInputArgument :: Nil)(recordSearchResultClick)
  ))

  lazy val schema: Schema[RequestContext, Unit] = Schema(queryType, Some(mutationType))
}
