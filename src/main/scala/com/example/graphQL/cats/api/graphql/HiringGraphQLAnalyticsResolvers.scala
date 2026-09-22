package com.example.graphQL.cats.api.graphql

import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.AnalyticsReportPayload
import com.example.graphQL.cats.api.graphql.HiringGraphQLResolverSupport.*
import com.example.graphQL.cats.service.AnalyticsPeriod
import sangria.schema.Context

private[graphql] object HiringGraphQLAnalyticsResolvers {
  def analyticsReport(context: Context[RequestContext, Unit]) =
    authenticated(context) { case (actor, hiring) =>
      val period = AnalyticsPeriod(context.arg(analyticsFromArgument), context.arg(analyticsToArgument))
      raiseOnUseCaseError(hiring.analyticsReporting.report(actor, period)).map(AnalyticsReportPayload.apply)
    }
}
