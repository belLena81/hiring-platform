package com.example.graphQL.cats.api.graphql

import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLResolverSupport.*
import com.example.graphQL.cats.service.search.EmbeddingCoverageReport
import sangria.schema.Context

private[graphql] object HiringGraphQLEmbeddingCoverageResolvers {

  /** Authorization lives in the service; the resolver only derives the actor and forwards the optional model. */
  def embeddingCoverage(context: Context[RequestContext, Unit]): HiringGraphQLResult[EmbeddingCoverageReport] =
    authenticated(context) { case (actor, hiring) =>
      raiseOnUseCaseError(hiring.embeddingCoverage.report(actor, context.arg(expectedModelArgument)))
    }
}
