package com.example.graphQL.cats.api.graphql

import cats.data.EitherT
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
    Field(
      name,
      fieldType,
      arguments = arguments,
      complexity = complexity,
      resolve = context => context.ctx.effectAdapter.fieldFuture(context.ctx, name, EitherT.liftF(resolve(context)))
    )

  def resultField[Val, Res](
      name: String,
      fieldType: OutputType[Res],
      arguments: List[Argument[?]] = Nil,
      complexity: Option[(RequestContext, Args, Double) => Double] = None
  )(resolve: Context[RequestContext, Val] => HiringGraphQLResult[Res]): Field[RequestContext, Val] =
    Field(
      name,
      fieldType,
      arguments = arguments,
      complexity = complexity,
      resolve = context => context.ctx.effectAdapter.fieldFuture(context.ctx, name, resolve(context))
    )

  def resultFetcher[Res, Id](fetch: (RequestContext, Seq[Id]) => HiringGraphQLResult[List[Res]])(using
      HasId[Res, Id]
  ): Fetcher[RequestContext, Res, Res, Id] =
    Fetcher.caching[RequestContext, Res, Id]((context, ids) =>
      context.effectAdapter.resolverFuture(context, fetch(context, ids).map(_.toSeq))
    )
}
