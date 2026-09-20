package com.example.graphQL.cats.api.graphql

import com.example.graphQL.cats.api.graphql.HiringGraphQLFetchers.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLAccountResolvers.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLApplicationResolvers.*
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
    ioField("me", userPayloadType)(accountMe),
    ioField("users", userConnectionType, firstArgument :: afterArgument :: userRoleArgument :: userStatusArgument :: Nil)(users),
    ioField("jobs", jobConnectionType, firstArgument :: afterArgument :: cityArgument :: skillsArgument :: createdAfterArgument :: searchIdArgument :: Nil)(jobs),
    ioField("semanticJobSearch", rankedJobResultsType, queryArgument :: jobFilterArgument :: firstArgument :: searchIdArgument :: Nil)(semanticJobSearch),
    ioField("recommendedJobs", rankedJobResultsType, firstArgument :: searchIdArgument :: Nil)(recommendedJobs),
    ioField("candidateMatches", rankedCandidateResultsType, jobIdArgument :: firstArgument :: searchIdArgument :: Nil)(candidateMatches),
    ioField("job", jobPayloadType, idArgument :: Nil)(job),
    ioField("myJobs", jobConnectionType, firstArgument :: afterArgument :: jobStatusArgument :: Nil)(myJobs),
    ioField("myApplications", applicationConnectionType, firstArgument :: afterArgument :: applicationStatusArgument :: Nil)(myApplications),
    ioField("jobApplications", applicationConnectionType, jobIdArgument :: firstArgument :: afterArgument :: applicationStatusArgument :: Nil)(jobApplications),
    ioField("applicationHistory", applicationEventConnectionType, applicationIdArgument :: firstArgument :: afterArgument :: Nil)(applicationHistory)
  ))

  lazy val mutationType: ObjectType[RequestContext, Unit] = ObjectType("Mutation", fields[RequestContext, Unit](
    ioField("submitApplication", applicationPayloadType, submitApplicationInputArgument :: Nil)(submitApplication),
    ioField("createJob", jobPayloadType, createJobInputArgument :: Nil)(createJob),
    ioField("updateJob", jobPayloadType, updateJobInputArgument :: Nil)(updateJob),
    ioField("publishJob", jobPayloadType, jobActionInputArgument :: Nil)(context => changeJob(context, _.publishJob)),
    ioField("closeJob", jobPayloadType, jobActionInputArgument :: Nil)(context => changeJob(context, _.closeJob)),
    ioField("acceptApplication", applicationPayloadType, applicationActionInputArgument :: Nil)(context => applicationStatusAction(context, ApplicationStatus.Accepted)),
    ioField("moveApplicationToInterview", applicationPayloadType, applicationActionInputArgument :: Nil)(context => applicationStatusAction(context, ApplicationStatus.Interview)),
    ioField("hireApplication", applicationPayloadType, applicationActionInputArgument :: Nil)(context => applicationStatusAction(context, ApplicationStatus.Hired)),
    ioField("rejectApplication", applicationPayloadType, rejectApplicationInputArgument :: Nil)(rejectApplication),
    ioField("declineApplication", applicationPayloadType, declineApplicationInputArgument :: Nil)(declineApplication),
    ioField("signUp", accountPayloadType, signUpInputArgument :: Nil)(signUp),
    ioField("login", accountPayloadType, loginInputArgument :: Nil)(login),
    ioField("bootstrapAdmin", accountPayloadType, bootstrapAdminInputArgument :: Nil)(bootstrapAdmin),
    ioField("updateMyProfile", userPayloadType, updateProfileInputArgument :: Nil)(updateMyProfile),
    ioField("deleteMyAccount", deleteAccountPayloadType)(deleteMyAccount),
    ioField("recordJobView", interactionPayloadType, recordJobViewInputArgument :: Nil)(recordJobView),
    ioField("recordSearchResultClick", interactionPayloadType, recordSearchResultClickInputArgument :: Nil)(recordSearchResultClick)
  ))

  lazy val schema: Schema[RequestContext, Unit] = Schema(queryType, Some(mutationType))
}
