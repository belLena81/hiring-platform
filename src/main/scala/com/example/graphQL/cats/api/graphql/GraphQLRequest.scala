package com.example.graphQL.cats.api.graphql

import io.circe.{Decoder, Json, JsonObject}

final case class GraphQLRequest(query: String, variables: Json, operationName: Option[String])

object GraphQLRequest {
  private final case class Raw(query: String, variables: Option[JsonObject], operationName: Option[String])
      derives Decoder

  given Decoder[GraphQLRequest] = Decoder[Raw].map(raw =>
    GraphQLRequest(raw.query, raw.variables.fold(Json.obj())(Json.fromJsonObject), raw.operationName)
  )
}
