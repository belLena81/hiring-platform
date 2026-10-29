package com.example.graphQL.cats.acceptance

import cats.effect.{IO, Resource}
import io.circe.Json
import io.circe.parser.parse
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration

/** One GraphQL response as the HTTP client saw it: status code, raw body and the parsed JSON body. */
final case class GraphQLResponse(status: Int, raw: String, body: Json) {
  def errors: Vector[Json] = body.hcursor.downField("errors").focus.flatMap(_.asArray).getOrElse(Vector.empty)
  def errorCodes: Vector[String] = errors.flatMap(_.hcursor.downField("extensions").get[String]("code").toOption)
  def data(field: String): Json = body.hcursor.downField("data").downField(field).focus.getOrElse(Json.Null)
}

/** A JSON-over-HTTP GraphQL client for the real API; the bearer token is optional so anonymous calls are expressible.
  */
final class GraphQLClient private (baseUrl: String, http: HttpClient) {
  def execute(query: String, variables: Json = Json.obj(), token: Option[String] = None): IO[GraphQLResponse] =
    IO.fromCompletableFuture(
      IO {
        val payload = Json.obj("query" -> Json.fromString(query), "variables" -> variables).noSpaces
        val builder = HttpRequest
          .newBuilder(URI.create(baseUrl + "/graphql"))
          .timeout(Duration.ofSeconds(20))
          .header("Content-Type", "application/json")
        val authorised = token.fold(builder)(value => builder.header("Authorization", "Bearer " + value))
        http.sendAsync(
          authorised.POST(HttpRequest.BodyPublishers.ofString(payload)).build(),
          HttpResponse.BodyHandlers.ofString()
        )
      }
    ).map(response =>
      GraphQLResponse(response.statusCode(), response.body(), parse(response.body()).getOrElse(Json.Null))
    )
}

object GraphQLClient {
  def resource(baseUrl: String): Resource[IO, GraphQLClient] =
    Resource
      .eval(
        IO.delay(
          HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(Duration.ofSeconds(5)).build()
        )
      )
      .map(new GraphQLClient(baseUrl, _))
}
