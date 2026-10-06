package com.example.graphQL.cats.api.graphql

import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.api.graphql.HiringGraphQLDsl.resultFetcher
import com.example.graphQL.cats.service.read.*
import sangria.execution.deferred.{Fetcher, HasId}

private[graphql] object HiringGraphQLFetchers {
  given HasId[RelatedUser, UserRelationKey] = HasId(_.key)
  given HasId[RelatedJob, JobRelationKey] = HasId(_.key)
  given HasId[EmailVisibility, UserId] = HasId(_.userId)

  lazy val usersFetcher: Fetcher[RequestContext, RelatedUser, RelatedUser, UserRelationKey] =
    resultFetcher[RelatedUser, UserRelationKey]((context, keys) => context.relatedUsers(keys.toList))
  lazy val jobsFetcher: Fetcher[RequestContext, RelatedJob, RelatedJob, JobRelationKey] =
    resultFetcher[RelatedJob, JobRelationKey]((context, keys) => context.relatedJobs(keys.toList))
  lazy val emailVisibilityFetcher: Fetcher[RequestContext, EmailVisibility, EmailVisibility, UserId] =
    resultFetcher[EmailVisibility, UserId]((context, ids) => context.visibleEmailUsers(ids.toList))
}
