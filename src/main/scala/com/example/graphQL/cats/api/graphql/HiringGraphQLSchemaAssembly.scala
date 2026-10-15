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
import com.example.graphQL.cats.api.graphql.HiringGraphQLDsl.{ioField, resultField}
import com.example.graphQL.cats.domain.model.ApplicationStatus
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.GraphQLFailure
import sangria.execution.{QueryReducer, ExecutionPath}
import sangria.ast
import sangria.execution.deferred.DeferredResolver
import sangria.schema.*

private[graphql] object HiringGraphQLSchemaAssembly {
  private val MaxQueryDepth = 16

  private val MaxQueryComplexity = com.example.graphQL.cats.config.GraphQLConfig.MaxQueryComplexity.toDouble

  // Each interview action weighs this much, so one request (complexity limit 1000) carries at most a handful of them
  // however they are aliased; the per-actor allowance then counts each of those actions individually.
  private val InterviewActionComplexity = 100d

  private def interviewActionField[In](name: String, input: Argument[In])(
      resolve: Context[RequestContext, Unit] => HiringGraphQLResult[
        HiringGraphQLModel.MutationOutcome[com.example.graphQL.cats.domain.workflow.InterviewWorkflow]
      ]
  ): Field[RequestContext, Unit] =
    resultField(
      name,
      interviewActionResultType,
      input :: Nil,
      complexity = Some((_, _, child) => InterviewActionComplexity + child)
    )(resolve)
  private val connectionComplexity: (RequestContext, Args, Double) => Double =
    (_, args, child) => 1d + args.arg(firstArgument).max(0) * child
  private val expensivePageComplexity: (RequestContext, Args, Double) => Double =
    (_, args, child) => 100d + args.arg(firstArgument).max(0) * (1d + child)
  // A lexical-query facet request runs `$search` plus an aggregation, so it costs at least a semanticJobSearch page.
  // 25 is the page-size equivalent: 100 + 25 * (1 + child) is at least a semanticJobSearch(first: 25) page, close to
  // the default page, and with the typical facet selection stays well under MaxQueryComplexity (1000).
  private val LexicalFacetPageEquivalent = 25d
  private val facetComplexity: (RequestContext, Args, Double) => Double =
    (_, args, child) =>
      if (args.arg(optionalFacetQueryArgument).isDefined) 100d + LexicalFacetPageEquivalent * (1d + child)
      else 100d + child
  private val expensiveRoots =
    Set(
      "nearbyJobs",
      "jobDiscoveryFacets",
      "semanticJobSearch",
      "recommendedJobs",
      "candidateMatches",
      "embeddingCoverage"
    )
  // Each embedding coverage root starts bounded but full scans; two roots exceed MaxQueryComplexity.
  private val coverageComplexity: (RequestContext, Args, Double) => Double =
    (_, _, child) => 600d + child
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
  // Rejects any `first` outside PageSize.Min..Max before execution, so a negative value cannot lower the complexity
  // computed for sibling fields. Runs ahead of the complexity reducer; resolvers keep their own check as well.
  private val pageSizeBounds = new QueryReducer[RequestContext, RequestContext] {
    type Acc = Option[GraphQLFailure]
    val initial = None
    def reduceAlternatives(values: Seq[Option[GraphQLFailure]]): Option[GraphQLFailure] = values.flatten.headOption
    def reduceField[Val](
        fieldAcc: Option[GraphQLFailure],
        childrenAcc: Option[GraphQLFailure],
        path: ExecutionPath,
        context: RequestContext,
        astFields: Vector[ast.Field],
        parentType: ObjectType[RequestContext, Val],
        field: Field[RequestContext, Val],
        argumentValuesFn: QueryReducer.ArgumentValuesFn
    ): Option[GraphQLFailure] =
      fieldAcc.orElse(childrenAcc).orElse {
        if (field.arguments.exists(_.name == firstArgument.name))
          argumentValuesFn(path, field.arguments, astFields.head.arguments).toOption
            .flatMap(args => HiringGraphQLResolverSupport.pageSize(args.arg(firstArgument)).left.toOption)
        else None
      }
    def reduceScalar[T](path: ExecutionPath, context: RequestContext, tpe: ScalarType[T]): Option[GraphQLFailure] = None
    def reduceEnum[T](path: ExecutionPath, context: RequestContext, tpe: EnumType[T]): Option[GraphQLFailure] = None
    def reduceCtx(
        failure: Option[GraphQLFailure],
        context: RequestContext
    ): ReduceAction[RequestContext, RequestContext] =
      // Sangria reducers reject through the framework throwable boundary before any resolver starts.
      failure.fold[ReduceAction[RequestContext, RequestContext]](context)(rejected =>
        throw context.effectAdapter.pageSizeRejected(rejected)
      )
  }
  // Introspection is cheap to request and expensive to answer, and aliases repeat it freely; each `__schema` or
  // `__type` selection therefore weighs as much as a few ordinary pages, so a handful per request reaches the cap.
  private val IntrospectionComplexity = 200d
  private val IntrospectionFields = Set("__schema", "__type")

  // Sangria's `measureComplexity` plus the introspection weight above; the handler decides what to do with the total.
  private def weightedComplexity(
      handle: (Double, RequestContext) => ReduceAction[RequestContext, RequestContext]
  ): QueryReducer[RequestContext, RequestContext] =
    new QueryReducer[RequestContext, RequestContext] {
      type Acc = Double
      val initial = 0d
      def reduceAlternatives(values: Seq[Double]): Double = values.foldLeft(0d)(math.max)
      def reduceField[Val](
          fieldAcc: Double,
          childrenAcc: Double,
          path: ExecutionPath,
          context: RequestContext,
          astFields: Vector[ast.Field],
          parentType: ObjectType[RequestContext, Val],
          field: Field[RequestContext, Val],
          argumentValuesFn: QueryReducer.ArgumentValuesFn
      ): Double = {
        val own = field.complexity match {
          case Some(estimate) =>
            argumentValuesFn(path, field.arguments, astFields.head.arguments).toOption
              .fold(1d + childrenAcc)(args => estimate(context, args, childrenAcc))
          case None => 1d + childrenAcc
        }
        val weight = if (IntrospectionFields.contains(field.name)) IntrospectionComplexity else 0d
        fieldAcc + own + weight
      }
      def reduceScalar[T](path: ExecutionPath, context: RequestContext, tpe: ScalarType[T]): Double = 0d
      def reduceEnum[T](path: ExecutionPath, context: RequestContext, tpe: EnumType[T]): Double = 0d
      def reduceCtx(total: Double, context: RequestContext): ReduceAction[RequestContext, RequestContext] =
        handle(total, context)
    }

  private val complexityCap = weightedComplexity { (total, context) =>
    // Sangria reducers reject through the framework throwable boundary before any resolver starts.
    if (total > MaxQueryComplexity) throw context.effectAdapter.complexityRejected(MaxQueryComplexity) else context
  }

  // Charges the computed complexity to the caller's cost budget. It runs after the structural and complexity
  // rejections (those queries are refused anyway) and before execution, so no resolver work precedes the decision.
  private val costBudget = weightedComplexity { (complexity, context) =>
    // The charge is an in-memory step, so the reducer needs no Future interop; refusal goes through the same
    // framework throwable boundary as the other reducers.
    context
      .chargeCostBudget(budgetCost(complexity))
      .fold(refused => throw context.effectAdapter.costBudgetRejected(refused.retryAfterSeconds), _ => context)
  }
  private def budgetCost(complexity: Double): Int =
    math.min(Int.MaxValue.toDouble, math.max(1d, math.ceil(complexity))).toInt

  lazy val queryReducers: List[QueryReducer[RequestContext, ?]] = List(
    pageSizeBounds,
    expensiveRootBudget,
    QueryReducer.rejectMaxDepth[RequestContext](MaxQueryDepth),
    complexityCap,
    costBudget
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
      resultField(
        "embeddingCoverage",
        embeddingCoverageReportType,
        expectedModelArgument :: Nil,
        complexity = Some(coverageComplexity)
      )(embeddingCoverage),
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
        optionalFacetQueryArgument :: nearbyFilterArgument :: optionalNearbyCenterArgument :: optionalRadiusKmArgument :: Nil,
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
      interviewActionField("cancelInterview", interviewActionInputArgument)(cancelInterview),
      interviewActionField("requestInterviewReschedule", interviewActionInputArgument)(requestInterviewReschedule),
      interviewActionField("dismissInterviewRescheduleRequest", interviewActionInputArgument)(
        dismissInterviewRescheduleRequest
      ),
      interviewActionField("proposeInterviewReschedule", proposeInterviewRescheduleInputArgument)(
        proposeInterviewReschedule
      ),
      interviewActionField("withdrawInterviewReschedule", interviewActionInputArgument)(withdrawInterviewReschedule),
      interviewActionField("acceptInterviewReschedule", interviewActionInputArgument)(acceptInterviewReschedule),
      interviewActionField("declineInterviewReschedule", interviewActionInputArgument)(declineInterviewReschedule),
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
