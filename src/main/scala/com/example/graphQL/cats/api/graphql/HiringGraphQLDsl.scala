package com.example.graphQL.cats.api.graphql

import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.{FacetsOutcome, MutationOutcome, SearchOutcome}
import sangria.execution.deferred.{Fetcher, FetcherConfig, HasId}
import sangria.schema.{Args, Argument, Context, EnumType, EnumValue, Field, ObjectType, OutputType, UnionType}

import com.example.graphQL.cats.domain.pagination.PageSize
import java.util.Locale

private[graphql] object HiringGraphQLDsl {

  /** A root field. The description is mandatory, so no public operation can be added without documentation. */
  def resultField[Val, Res](
      name: String,
      fieldType: OutputType[Res],
      description: String,
      arguments: List[Argument[?]] = Nil,
      complexity: Option[(RequestContext, Args, Double) => Double] = None
  )(resolve: Context[RequestContext, Val] => HiringGraphQLResult[Res]): Field[RequestContext, Val] =
    Field(
      name,
      fieldType,
      description = Some(description),
      arguments = arguments,
      complexity = complexity,
      resolve = context => context.ctx.effectAdapter.fieldFuture(context.ctx, name, resolve(context))
    )

  /** An enum whose wire names are the SCREAMING_SNAKE_CASE of the case names. `describe` is a total function over the
    * domain type, so a new case does not compile (when it matches exhaustively) until it is documented.
    */
  def describedEnum[A](name: String, description: String, values: List[A])(describe: A => String): EnumType[A] =
    EnumType(
      name,
      description = Some(description),
      values = values.map(value => EnumValue(wireName(value), description = Some(describe(value)), value = value))
    )

  /** An enum over explicit wire names and values. */
  def describedNames[A](name: String, description: String, values: List[(String, A, String)]): EnumType[A] =
    EnumType(
      name,
      description = Some(description),
      values = values.map { case (wire, value, text) => EnumValue(wire, description = Some(text), value = value) }
    )

  def wireName(value: Any): String =
    value.toString.replaceAll("([a-z])([A-Z])", "$1_$2").toUpperCase(Locale.ROOT)

  /** A documented result union of a mutation: the success type, or a typed validation or domain error. */
  def outcomeUnion[A](
      name: String,
      description: String,
      success: ObjectType[RequestContext, A],
      validation: ObjectType[RequestContext, HiringGraphQLModel.ValidationError],
      domain: ObjectType[RequestContext, HiringGraphQLModel.DomainError]
  ): OutputType[MutationOutcome[A]] =
    UnionType[RequestContext](name, Some(description), List(success, validation, domain))
      .mapValue[MutationOutcome[A]](identity)

  /** A documented result union of an AI search root: the ranked results, or a typed unavailability, validation or
    * domain refusal.
    */
  def searchOutcomeUnion[A](
      name: String,
      description: String,
      success: ObjectType[RequestContext, A],
      unavailable: ObjectType[RequestContext, HiringGraphQLModel.SearchUnavailable],
      validation: ObjectType[RequestContext, HiringGraphQLModel.ValidationError],
      domain: ObjectType[RequestContext, HiringGraphQLModel.DomainError]
  ): OutputType[SearchOutcome[A]] =
    UnionType[RequestContext](name, Some(description), List(success, unavailable, validation, domain))
      .mapValue[SearchOutcome[A]](identity)

  /** The facets union has no domain-refusal member. */
  def facetsOutcomeUnion[A](
      name: String,
      description: String,
      success: ObjectType[RequestContext, A],
      unavailable: ObjectType[RequestContext, HiringGraphQLModel.SearchUnavailable],
      validation: ObjectType[RequestContext, HiringGraphQLModel.ValidationError]
  ): OutputType[FacetsOutcome[A]] =
    UnionType[RequestContext](name, Some(description), List(success, unavailable, validation))
      .mapValue[FacetsOutcome[A]](identity)

  /** The most keys one repository call serves; it matches the largest page, so a full page of related entities is one
    * call and a larger batch is split instead of growing without bound.
    */
  val FetcherMaxBatchSize: Int = PageSize.Max

  def resultFetcher[Res, Id](fetch: (RequestContext, Seq[Id]) => HiringGraphQLResult[List[Res]])(using
      HasId[Res, Id]
  ): Fetcher[RequestContext, Res, Res, Id] =
    Fetcher.caching[RequestContext, Res, Id](
      (context, ids) => context.effectAdapter.resolverFuture(context, fetch(context, ids).map(_.toSeq)),
      FetcherConfig.caching.maxBatchSize(FetcherMaxBatchSize)
    )
}
