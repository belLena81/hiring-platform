package com.example.graphQL.cats.api.graphql

import com.example.graphQL.cats.api.graphql.HiringGraphQLFetchers.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLInterviewResolvers.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLAccountResolvers.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLApplicationResolvers.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLAnalyticsResolvers.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLInteractionResolvers.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLJobResolvers.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLSearchResolvers.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLTypes.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLDsl.{ioField, resultField}
import com.example.graphQL.cats.domain.model.ApplicationStatus
import sangria.execution.{QueryReducer, ExecutionPath}
import sangria.ast
import sangria.execution.deferred.DeferredResolver
import sangria.schema.*

private[graphql] object HiringGraphQLSchemaAssembly {
  private val MaxQueryDepth = 16
  private val MaxQueryComplexity = 1000d
  private val connectionComplexity: (RequestContext, Args, Double) => Double =
    (_, args, child) => 1d + args.arg(firstArgument) * child
  private val expensivePageComplexity: (RequestContext, Args, Double) => Double =
    (_, args, child) => 100d + args.arg(firstArgument) * (1d + child)
  private val facetComplexity: (RequestContext, Args, Double) => Double =
    (_, _, child) => 100d + child
  private val expensiveRoots =
    Set("nearbyJobs", "jobDiscoveryFacets", "semanticJobSearch", "recommendedJobs", "candidateMatches")
  private val expensiveRootBudget = new QueryReducer[RequestContext, RequestContext] {
    type Acc = Int
    val initial = 0
    def reduceAlternatives(values: Seq[Int]): Int = values.foldLeft(0)(math.max)
    def reduceField[Val](
        fieldAcc: Int,
        childrenAcc: Int,
        path: ExecutionPath,
        context: RequestContext,
        astFields: Vector[ast.Field],
        parentType: ObjectType[RequestContext, Val],
        field: Field[RequestContext, Val],
        argumentValuesFn: QueryReducer.ArgumentValuesFn
    ): Int =
      fieldAcc + childrenAcc + (if (parentType.name == "Query" && expensiveRoots.contains(field.name)) 1 else 0)
    def reduceScalar[T](path: ExecutionPath, context: RequestContext, tpe: ScalarType[T]): Int = 0
    def reduceEnum[T](path: ExecutionPath, context: RequestContext, tpe: EnumType[T]): Int = 0
    def reduceCtx(count: Int, context: RequestContext): ReduceAction[RequestContext, RequestContext] =
      // Sangria reducers reject through the framework throwable boundary before any resolver starts.
      if (count > context.discoveryMaxRoots)
        throw context.effectAdapter.complexityRejected(context.discoveryMaxRoots.toDouble)
      else context
  }
  lazy val queryReducers: List[QueryReducer[RequestContext, ?]] = List(
    expensiveRootBudget,
    QueryReducer.rejectMaxDepth[RequestContext](MaxQueryDepth),
    QueryReducer
      .rejectComplexQueries[RequestContext](
        MaxQueryComplexity,
        (_, context) => context.effectAdapter.complexityRejected(MaxQueryComplexity)
      )
  )

  lazy val deferredResolver: DeferredResolver[RequestContext] =
    DeferredResolver.fetchers(usersFetcher, jobsFetcher, emailVisibilityFetcher)

  lazy val queryType: ObjectType[RequestContext, Unit] = ObjectType(
    "Query",
    fields[RequestContext, Unit](
      resultField("interviewWorkflow", interviewWorkflowType, workflowIdArgument :: Nil)(interviewWorkflow),
      Field("health", healthType, resolve = _ => ()),
      ioField("readiness", readinessType)(context => context.ctx.readiness),
      resultField("me", OptionType(userType))(accountMe(_).map(Some(_))),
      resultField("analyticsReport", analyticsReportType, analyticsFromArgument :: analyticsToArgument :: Nil)(
        analyticsReport
      ),
      resultField("accountDeletionStatus", accountDeletionStatusType, deletionReceiptIdArgument :: Nil)(
        accountDeletionStatus
      ),
      resultField(
        "users",
        userConnectionType,
        firstArgument :: afterArgument :: userRoleArgument :: userStatusArgument :: Nil,
        complexity = Some(connectionComplexity)
      )(users),
      resultField(
        "jobs",
        jobConnectionType,
        firstArgument :: afterArgument :: cityArgument :: skillsArgument :: createdAfterArgument :: searchIdArgument :: Nil,
        complexity = Some(connectionComplexity)
      )(jobs),
      resultField(
        "nearbyJobs",
        nearbyJobsResultsType,
        nearbyCenterArgument :: radiusKmArgument :: nearbyFilterArgument :: firstArgument :: afterArgument :: Nil,
        complexity = Some(expensivePageComplexity)
      )(nearbyJobs),
      resultField(
        "jobDiscoveryFacets",
        jobDiscoveryFacetsType,
        nearbyFilterArgument :: optionalNearbyCenterArgument :: optionalRadiusKmArgument :: Nil,
        complexity = Some(facetComplexity)
      )(jobDiscoveryFacets),
      resultField(
        "semanticJobSearch",
        rankedJobResultsType,
        queryArgument :: jobFilterArgument :: firstArgument :: searchIdArgument :: Nil,
        complexity = Some(expensivePageComplexity)
      )(semanticJobSearch),
      resultField(
        "recommendedJobs",
        rankedJobResultsType,
        firstArgument :: searchIdArgument :: Nil,
        complexity = Some(expensivePageComplexity)
      )(recommendedJobs),
      resultField(
        "candidateMatches",
        rankedCandidateResultsType,
        jobIdArgument :: candidateSearchQueryArgument :: candidateMatchFilterArgument :: firstArgument :: searchIdArgument :: Nil,
        complexity = Some(expensivePageComplexity)
      )(candidateMatches),
      resultField("job", OptionType(jobType), idArgument :: Nil)(job(_).map(Some(_))),
      resultField(
        "myJobs",
        jobConnectionType,
        firstArgument :: afterArgument :: jobStatusArgument :: Nil,
        complexity = Some(connectionComplexity)
      )(myJobs),
      resultField(
        "myApplications",
        applicationConnectionType,
        firstArgument :: afterArgument :: applicationStatusArgument :: Nil,
        complexity = Some(connectionComplexity)
      )(myApplications),
      resultField(
        "jobApplications",
        applicationConnectionType,
        jobIdArgument :: firstArgument :: afterArgument :: applicationStatusArgument :: Nil,
        complexity = Some(connectionComplexity)
      )(jobApplications),
      resultField(
        "applicationHistory",
        applicationEventConnectionType,
        applicationIdArgument :: firstArgument :: afterArgument :: Nil,
        complexity = Some(connectionComplexity)
      )(applicationHistory)
    )
  )

  lazy val mutationType: ObjectType[RequestContext, Unit] = ObjectType(
    "Mutation",
    fields[RequestContext, Unit](
      resultField("scheduleInterview", scheduleInterviewResultType, scheduleInterviewInputArgument :: Nil)(
        scheduleInterview
      ),
      resultField("repairInterviewWorkflow", repairInterviewResultType, repairInterviewInputArgument :: Nil)(
        repairInterviewWorkflow
      ),
      resultField("submitApplication", submitApplicationResultType, submitApplicationInputArgument :: Nil)(
        submitApplication
      ),
      resultField("createJob", createJobResultType, createJobInputArgument :: Nil)(createJob),
      resultField("updateJob", updateJobResultType, updateJobInputArgument :: Nil)(updateJob),
      resultField("publishJob", publishJobResultType, jobActionInputArgument :: Nil)(context =>
        changeJob(context, _.publishJob)
      ),
      resultField("closeJob", closeJobResultType, jobActionInputArgument :: Nil)(context =>
        changeJob(context, _.closeJob)
      ),
      resultField("acceptApplication", acceptApplicationResultType, applicationActionInputArgument :: Nil)(context =>
        applicationStatusAction(context, ApplicationStatus.Accepted)
      ),
      resultField("moveApplicationToInterview", interviewApplicationResultType, applicationActionInputArgument :: Nil)(
        context => applicationStatusAction(context, ApplicationStatus.Interview)
      ),
      resultField("hireApplication", hireApplicationResultType, applicationActionInputArgument :: Nil)(context =>
        applicationStatusAction(context, ApplicationStatus.Hired)
      ),
      resultField("rejectApplication", rejectApplicationResultType, rejectApplicationInputArgument :: Nil)(
        rejectApplication
      ),
      resultField("declineApplication", declineApplicationResultType, declineApplicationInputArgument :: Nil)(
        declineApplication
      ),
      resultField("signUp", signUpResultType, signUpInputArgument :: Nil)(signUp),
      resultField("login", loginResultType, loginInputArgument :: Nil)(login),
      resultField("bootstrapAdmin", bootstrapAdminResultType, bootstrapAdminInputArgument :: Nil)(bootstrapAdmin),
      resultField("updateMyProfile", updateMyProfileResultType, updateProfileInputArgument :: Nil)(updateMyProfile),
      resultField("deleteMyAccount", deleteMyAccountResultType, deleteMyAccountInputArgument :: Nil)(deleteMyAccount),
      resultField("recordJobView", recordJobViewResultType, recordJobViewInputArgument :: Nil)(recordJobView),
      resultField(
        "recordSearchResultClick",
        recordSearchResultClickResultType,
        recordSearchResultClickInputArgument :: Nil
      )(recordSearchResultClick)
    )
  )

  lazy val schema: Schema[RequestContext, Unit] = Schema(queryType, Some(mutationType))
}
