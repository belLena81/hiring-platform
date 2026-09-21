package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import sangria.execution.deferred.{Fetcher, HasId}
import sangria.schema.{Args, Argument, Context, Field, OutputType}

private[graphql] object HiringGraphQLDsl {
  def ioField[Val, Res](
      name: String,
      fieldType: OutputType[Res],
      arguments: List[Argument[?]] = Nil,
      complexity: Option[(RequestContext, Args, Double) => Double] = None
  )(resolve: Context[RequestContext, Val] => IO[Res]): Field[RequestContext, Val] =
    Field(name, fieldType, arguments = arguments, complexity = complexity,
      resolve = context => context.ctx.unsafeFieldToFuture(name, resolve(context)))

  def ioFetcher[Res, Id](fetch: (RequestContext, Seq[Id]) => IO[Seq[Res]])(using HasId[Res, Id])
      : Fetcher[RequestContext, Res, Res, Id] =
    Fetcher.caching[RequestContext, Res, Id]((context, ids) => context.unsafeToFuture(fetch(context, ids)))
}
