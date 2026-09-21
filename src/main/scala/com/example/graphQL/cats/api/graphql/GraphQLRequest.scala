package com.example.graphQL.cats.api.graphql

import io.circe.{Decoder, Json, JsonObject}
import sangria.ast.Document
import sangria.parser.QueryParser

final case class GraphQLRequest(document: Document, variables: Json, operationName: Option[String])

object GraphQLRequest {
  private final case class Raw(query: String, variables: Option[JsonObject], operationName: Option[String]) derives Decoder

  given Decoder[GraphQLRequest] = Decoder[Raw].emap { raw =>
    QueryParser.parse(raw.query).toEither.left.map(_ => "Invalid GraphQL query")
      .map(document => GraphQLRequest(document, raw.variables.fold(Json.obj())(Json.fromJsonObject), raw.operationName))
  }
}
