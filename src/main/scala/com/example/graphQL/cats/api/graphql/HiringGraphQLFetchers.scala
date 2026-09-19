package com.example.graphQL.cats.api.graphql

import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.{Job, User}
import sangria.execution.deferred.{Fetcher, HasId}

private[graphql] object HiringGraphQLFetchers {
  given HasId[User, UserId] = HasId(_.id)
  given HasId[Job, JobId] = HasId(_.id)
  given HasId[EmailVisibility, UserId] = HasId(_.userId)

  lazy val usersFetcher: Fetcher[RequestContext, User, User, UserId] =
    Fetcher.caching[RequestContext, User, UserId] { (context, ids) =>
      context.unsafeToFuture(context.users(ids.toList))
    }
  lazy val jobsFetcher: Fetcher[RequestContext, Job, Job, JobId] =
    Fetcher.caching[RequestContext, Job, JobId] { (context, ids) =>
      context.unsafeToFuture(context.jobs(ids.toList))
    }
  lazy val emailVisibilityFetcher: Fetcher[RequestContext, EmailVisibility, EmailVisibility, UserId] =
    Fetcher.caching[RequestContext, EmailVisibility, UserId] { (context, ids) =>
      context.unsafeToFuture(context.visibleEmailUsers(ids.toList))
    }
}
