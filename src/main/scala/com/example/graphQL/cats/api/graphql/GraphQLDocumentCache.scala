package com.example.graphQL.cats.api.graphql

import cats.effect.{IO, Resource}
import com.github.benmanes.caffeine.cache.{Cache, Caffeine}
import sangria.ast.Document
import sangria.parser.QueryParser
import sangria.validation.QueryValidator

import java.util.concurrent.TimeUnit

final case class GraphQLDocument(document: Document, queryValidator: QueryValidator, cached: Boolean)

final class GraphQLDocumentCache private (cache: Cache[String, Document]) {
  def document(query: String): IO[Either[HiringGraphQLSchema.Failure, GraphQLDocument]] =
    IO.delay(Option(cache.getIfPresent(query))).map {
      case Some(value) => Right(GraphQLDocument(value, QueryValidator.empty, cached = true))
      case None => QueryParser.parse(query).toEither.left.map(_ => HiringGraphQLSchema.Failure.InvalidQuery)
        .map(value => GraphQLDocument(value, QueryValidator.default, cached = false))
    }

  def store(query: String, document: Document): IO[Unit] = IO.delay(cache.put(query, document))
}

object GraphQLDocumentCache {
  private val MaximumEntries = 256L
  private val ExpirySeconds = 60L

  def resource: Resource[IO, GraphQLDocumentCache] =
    Resource.pure(new GraphQLDocumentCache(
      Caffeine.newBuilder().maximumSize(MaximumEntries).expireAfterWrite(ExpirySeconds, TimeUnit.SECONDS).build[String, Document]()))
}
