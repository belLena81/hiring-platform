package com.example.graphQL.cats.api.graphql

import cats.data.EitherT
import cats.effect.{IO, Ref}
import io.circe.Json
import munit.CatsEffectSuite
import sangria.execution.Executor
import sangria.execution.deferred.{DeferredResolver, HasId}
import sangria.marshalling.circe.*
import sangria.parser.QueryParser
import sangria.schema.*

/** AC-14: related-entity fetchers split a large batch of keys instead of passing it whole to the repository. */
final class HiringGraphQLFetcherBatchSpec extends CatsEffectSuite {
  private final case class Item(id: Int)
  private given HasId[Item, Int] = HasId(_.id)

  private def run(keys: Int): IO[(Json, Int)] =
    for {
      calls <- Ref.of[IO, Int](0)
      fetcher = HiringGraphQLDsl.resultFetcher[Item, Int]((_, ids) =>
        EitherT.liftF[IO, HiringGraphQLFailure, List[Item]](calls.update(_ + 1).as(ids.toList.map(Item(_))))
      )
      itemType = ObjectType("Item", fields[RequestContext, Item](Field("id", IntType, resolve = _.value.id)))
      queryType = ObjectType(
        "Query",
        fields[RequestContext, Unit](
          Field(
            "item",
            OptionType(itemType),
            arguments = Argument("id", IntType) :: Nil,
            resolve = context => fetcher.deferOpt(context.arg[Int]("id"))
          )
        )
      )
      query = (1 to keys).map(index => s"i$index: item(id: $index) { id }").mkString("{ ", " ", " }")
      document = QueryParser.parse(query).get
      executionContext <- IO.executionContext
      result <- TestGraphQLSupport.context().use { context =>
        given scala.concurrent.ExecutionContext = executionContext
        IO.fromFuture(
          IO(
            Executor.execute(
              Schema(queryType),
              document,
              context,
              variables = Json.obj(),
              deferredResolver = DeferredResolver.fetchers(fetcher)
            )
          )
        )
      }
      count <- calls.get
    } yield (result, count)

  test("AC-14.1 a batch of exactly the maximum size is one repository call") {
    run(HiringGraphQLDsl.FetcherMaxBatchSize).map { case (_, count) => assertEquals(count, 1) }
  }

  test("AC-14.2 one key more than the maximum is two repository calls with the same results") {
    val keys = HiringGraphQLDsl.FetcherMaxBatchSize + 1
    run(keys).map { case (json, count) =>
      assertEquals(count, 2)
      val ids = (1 to keys).map(index => json.hcursor.downField("data").downField(s"i$index").get[Int]("id"))
      assertEquals(ids.toList, (1 to keys).toList.map(Right(_)))
    }
  }
}
