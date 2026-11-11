package com.example.graphQL.cats.api.graphql

import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.{firstArgument, optionalFacetQueryArgument}
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.GraphQLFailure
import com.example.graphQL.cats.domain.pagination.PageSize
import sangria.ast
import sangria.execution.{ExecutionPath, QueryReducer}
import sangria.schema.*

/** Admission of a query before any resolver runs: the structural limits, the complexity weights that price every list
  * field (a nested connection is charged page size times its children, like a root one), the expensive-root budget, and
  * the charge to the caller's cost budget.
  */
private[graphql] object QueryAdmission {
  val MaxQueryDepth = 16

  val MaxQueryComplexity = com.example.graphQL.cats.config.GraphQLConfig.MaxQueryComplexity.toDouble

  // Each interview action weighs this much, so one request (complexity limit 1000) carries at most a handful of them
  // however they are aliased; the per-actor allowance then counts each of those actions individually.
  val InterviewActionComplexity = 100d

  def connectionPrice(first: Int, child: Double): Double = 1d + first.max(0) * child
  val connectionComplexity: (RequestContext, Args, Double) => Double =
    (_, args, child) => connectionPrice(args.arg(firstArgument), child)

  /** The fallback charge for one extra search of a recovered `jobs` page when the field's priced complexity was not
    * recorded: the page at its cheapest selection (child cost 1), a lower bound of what the field was charged.
    */
  def relaxedSearchCost(first: Int): Int = budgetCost(connectionPrice(first, 1d))
  // A nested list runs one authorized read per parent (no keyed batching yet), so it costs a base amount on top of
  // the page: 5 + first * children. With the cheapest selection a request carries about 99 parents per 1000 units.
  val NestedListBaseComplexity = 5d
  val nestedConnectionComplexity: (RequestContext, Args, Double) => Double =
    (_, args, child) => NestedListBaseComplexity + args.arg(firstArgument).max(0) * child
  // A single nested object (`Application.interview`) is also one authorized read per parent, so it carries the same base.
  val nestedFieldComplexity: (RequestContext, Args, Double) => Double =
    (_, _, child) => NestedListBaseComplexity + child
  val expensivePageComplexity: (RequestContext, Args, Double) => Double =
    (_, args, child) => 100d + args.arg(firstArgument).max(0) * (1d + child)
  // A lexical-query facet request runs `$search` plus an aggregation, so it costs at least a semanticJobSearch page.
  // 25 is the page-size equivalent: 100 + 25 * (1 + child) is at least a semanticJobSearch(first: 25) page, close to
  // the default page, and with the typical facet selection stays well under MaxQueryComplexity (1000).
  val LexicalFacetPageEquivalent = 25d
  val facetComplexity: (RequestContext, Args, Double) => Double =
    (_, args, child) =>
      if (args.arg(optionalFacetQueryArgument).isDefined) 100d + LexicalFacetPageEquivalent * (1d + child)
      else 100d + child
  // Appended to the description of each expensive discovery root, so clients can price a query before sending it.
  val CostNote =
    " Cost: 100 + first * (1 + cost of the selected subfields)"
  val ExpensiveRootCaps =
    " Caps per request: total complexity at most " + MaxQueryComplexity.toInt + ", nesting depth at most " +
      MaxQueryDepth + ", `first` between " + PageSize.Min + " and " + PageSize.Max +
      ", and a configured maximum number of expensive discovery roots (default 4). The cost is charged to the " +
      "caller's cost budget; the response reports it in `extensions.cost` when the caller has its own allowance."
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
  val coverageComplexity: (RequestContext, Args, Double) => Double =
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
        if (parentType.name == "Query" && field.name == RecoverableRoot)
          context.recordRootFieldCost(path, budgetCost(own))
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
    val requested = budgetCost(complexity)
    context
      .chargeCostBudget(requested)
      .fold(
        refused =>
          throw context.effectAdapter.costBudgetRejected(
            refused.retryAfterSeconds,
            CostReceipt.from(requested, refused.grant)
          ),
        _ => context
      )
  }
  // The root field whose empty page may be recovered by extra searches, each charged what the field itself was charged.
  private val RecoverableRoot = "jobs"
  private def budgetCost(complexity: Double): Int =
    math.min(Int.MaxValue.toDouble, math.max(1d, math.ceil(complexity))).toInt

  lazy val queryReducers: List[QueryReducer[RequestContext, ?]] = List(
    pageSizeBounds,
    expensiveRootBudget,
    QueryReducer.rejectMaxDepth[RequestContext](MaxQueryDepth),
    complexityCap,
    costBudget
  )
}
