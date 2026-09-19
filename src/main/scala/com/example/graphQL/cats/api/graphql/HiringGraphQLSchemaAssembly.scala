package com.example.graphQL.cats.api.graphql

import com.example.graphQL.cats.api.graphql.HiringGraphQLFetchers.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLResolvers.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLTypes.*
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
    Field("readiness", readinessType, resolve = context => context.ctx.readiness),
    Field("me", userPayloadType, resolve = context => context.ctx.unsafeToFuture(accountMe(context))),
    Field("users", userConnectionType, arguments = firstArgument :: afterArgument :: userRoleArgument :: userStatusArgument :: Nil,
      resolve = context => context.ctx.unsafeToFuture(users(context))),
    Field("jobs", jobConnectionType,
      arguments = firstArgument :: afterArgument :: cityArgument :: skillsArgument :: createdAfterArgument :: Nil,
      resolve = context => context.ctx.unsafeToFuture(jobs(context))),
    Field("semanticJobSearch", rankedJobResultsType,
      arguments = queryArgument :: jobFilterArgument :: firstArgument :: Nil,
      resolve = context => context.ctx.unsafeToFuture(semanticJobSearch(context))),
    Field("recommendedJobs", rankedJobResultsType,
      arguments = firstArgument :: Nil,
      resolve = context => context.ctx.unsafeToFuture(recommendedJobs(context))),
    Field("candidateMatches", rankedCandidateResultsType,
      arguments = jobIdArgument :: firstArgument :: Nil,
      resolve = context => context.ctx.unsafeToFuture(candidateMatches(context))),
    Field("job", jobPayloadType, arguments = idArgument :: Nil,
      resolve = context => context.ctx.unsafeToFuture(job(context))),
    Field("myJobs", jobConnectionType, arguments = firstArgument :: afterArgument :: jobStatusArgument :: Nil,
      resolve = context => context.ctx.unsafeToFuture(myJobs(context))),
    Field("myApplications", applicationConnectionType, arguments = firstArgument :: afterArgument :: applicationStatusArgument :: Nil,
      resolve = context => context.ctx.unsafeToFuture(myApplications(context))),
    Field("jobApplications", applicationConnectionType,
      arguments = jobIdArgument :: firstArgument :: afterArgument :: applicationStatusArgument :: Nil,
      resolve = context => context.ctx.unsafeToFuture(jobApplications(context))),
    Field("applicationHistory", applicationEventConnectionType,
      arguments = applicationIdArgument :: firstArgument :: afterArgument :: Nil,
      resolve = context => context.ctx.unsafeToFuture(applicationHistory(context)))
  ))

  lazy val mutationType: ObjectType[RequestContext, Unit] = ObjectType("Mutation", fields[RequestContext, Unit](
    Field("submitApplication", applicationPayloadType, arguments = submitApplicationInputArgument :: Nil,
      resolve = context => context.ctx.unsafeToFuture(submitApplication(context))),
    Field("createJob", jobPayloadType, arguments = createJobInputArgument :: Nil,
      resolve = context => context.ctx.unsafeToFuture(createJob(context))),
    Field("updateJob", jobPayloadType, arguments = updateJobInputArgument :: Nil,
      resolve = context => context.ctx.unsafeToFuture(updateJob(context))),
    Field("publishJob", jobPayloadType, arguments = jobActionInputArgument :: Nil,
      resolve = context => context.ctx.unsafeToFuture(changeJob(context, _.publishJob))),
    Field("closeJob", jobPayloadType, arguments = jobActionInputArgument :: Nil,
      resolve = context => context.ctx.unsafeToFuture(changeJob(context, _.closeJob))),
    Field("acceptApplication", applicationPayloadType, arguments = applicationActionInputArgument :: Nil,
      resolve = context => context.ctx.unsafeToFuture(applicationStatusAction(context, ApplicationStatus.Accepted))),
    Field("moveApplicationToInterview", applicationPayloadType, arguments = applicationActionInputArgument :: Nil,
      resolve = context => context.ctx.unsafeToFuture(applicationStatusAction(context, ApplicationStatus.Interview))),
    Field("hireApplication", applicationPayloadType, arguments = applicationActionInputArgument :: Nil,
      resolve = context => context.ctx.unsafeToFuture(applicationStatusAction(context, ApplicationStatus.Hired))),
    Field("rejectApplication", applicationPayloadType, arguments = rejectApplicationInputArgument :: Nil,
      resolve = context => context.ctx.unsafeToFuture(rejectApplication(context))),
    Field("declineApplication", applicationPayloadType, arguments = declineApplicationInputArgument :: Nil,
      resolve = context => context.ctx.unsafeToFuture(declineApplication(context))),
    Field("signUp", accountPayloadType, arguments = signUpInputArgument :: Nil,
      resolve = context => context.ctx.unsafeToFuture(signUp(context))),
    Field("login", accountPayloadType, arguments = loginInputArgument :: Nil,
      resolve = context => context.ctx.unsafeToFuture(login(context))),
    Field("bootstrapAdmin", accountPayloadType, arguments = bootstrapAdminInputArgument :: Nil,
      resolve = context => context.ctx.unsafeToFuture(bootstrapAdmin(context))),
    Field("updateMyProfile", userPayloadType, arguments = updateProfileInputArgument :: Nil,
      resolve = context => context.ctx.unsafeToFuture(updateMyProfile(context))),
    Field("deleteMyAccount", deleteAccountPayloadType,
      resolve = context => context.ctx.unsafeToFuture(deleteMyAccount(context)))
  ))

  lazy val schema: Schema[RequestContext, Unit] = Schema(queryType, Some(mutationType))
}
