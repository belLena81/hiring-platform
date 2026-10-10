package com.example.graphQL.cats.api.graphql

import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.AnalyticsReportPayload
import com.example.graphQL.cats.api.graphql.HiringGraphQLResolverSupport.*
import com.example.graphQL.cats.service.AnalyticsPeriod
import com.example.graphQL.cats.service.search.EmbeddingCoverageReport
import sangria.schema.Context

private[graphql] object HiringGraphQLReportingResolvers {
  def analyticsReport(context: Context[RequestContext, Unit]): HiringGraphQLResult[AnalyticsReportPayload] =
    authenticated(context) { case (actor, hiring) =>
      val period = AnalyticsPeriod(context.arg(analyticsFromArgument), context.arg(analyticsToArgument))
      raiseOnUseCaseError(hiring.analyticsReporting.report(actor, period)).map(AnalyticsReportPayload.apply)
    }

  /** Authorization lives in the service; the resolver only derives the actor and forwards the optional model. */
  def embeddingCoverage(context: Context[RequestContext, Unit]): HiringGraphQLResult[EmbeddingCoverageReport] =
    authenticated(context) { case (actor, hiring) =>
      raiseOnUseCaseError(hiring.embeddingCoverage.report(actor, context.arg(expectedModelArgument)))
    }
}
