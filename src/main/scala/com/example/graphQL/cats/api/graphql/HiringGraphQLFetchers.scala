package com.example.graphQL.cats.api.graphql

import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.{Job, User}
import com.example.graphQL.cats.api.graphql.HiringGraphQLDsl.ioFetcher
import sangria.execution.deferred.{Fetcher, HasId}

private[graphql] object HiringGraphQLFetchers {
  given HasId[User, UserId] = HasId(_.id)
  given HasId[Job, JobId] = HasId(_.id)
  given HasId[EmailVisibility, UserId] = HasId(_.userId)

  lazy val usersFetcher: Fetcher[RequestContext, User, User, UserId] =
    ioFetcher[User, UserId]((context, ids) => context.users(ids.toList))
  lazy val jobsFetcher: Fetcher[RequestContext, Job, Job, JobId] =
    ioFetcher[Job, JobId]((context, ids) => context.jobs(ids.toList))
  lazy val emailVisibilityFetcher: Fetcher[RequestContext, EmailVisibility, EmailVisibility, UserId] =
    ioFetcher[EmailVisibility, UserId]((context, ids) => context.visibleEmailUsers(ids.toList))
}
