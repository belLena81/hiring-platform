package com.example.graphQL.cats.api.graphql

import io.circe.Json
import io.circe.parser.parse
import io.circe.{Decoder, DecodingFailure}
import sangria.ast.Document
import sangria.parser.QueryParser

final case class GraphQLRequest(document: Document, variables: Json, operationName: Option[String])

object GraphQLRequest {
  given Decoder[GraphQLRequest] = Decoder.instance { json =>
    for {
      envelope <- json.value.asObject.toRight(DecodingFailure("GraphQL request must be a JSON object", Nil))
      query <- envelope("query").flatMap(_.asString).toRight(DecodingFailure("GraphQL query is required", Nil))
      variables <- envelope("variables").filterNot(_.isNull) match {
        case None => Right(Json.obj())
        case Some(value) => value.asObject.map(_ => value).toRight(DecodingFailure("GraphQL variables must be an object", Nil))
      }
      operationName <- envelope("operationName").filterNot(_.isNull) match {
        case None => Right(None)
        case Some(value) => value.asString.map(Some(_)).toRight(DecodingFailure("GraphQL operationName must be a string", Nil))
      }
      document <- QueryParser.parse(query).toEither.left.map(_ => DecodingFailure("Invalid GraphQL query", Nil))
    } yield GraphQLRequest(document, variables, operationName)
  }

  def parseBody(body: String): Option[GraphQLRequest] =
    for {
      json <- parse(body).toOption
      envelope <- json.asObject
      query <- envelope("query").flatMap(_.asString)
      variables <- envelope("variables").filterNot(_.isNull) match {
        case None => Some(Json.obj())
        case Some(value) => Option.when(value.isObject)(value)
      }
      operationName <- envelope("operationName").filterNot(_.isNull) match {
        case None => Some(None)
        case Some(value) => value.asString.map(Some(_))
      }
      document <- QueryParser.parse(query).toOption
    } yield GraphQLRequest(document, variables, operationName)
}
