package com.example.graphQL.cats.api.graphql

import io.circe.Json
import io.circe.parser.parse
import sangria.ast.Document
import sangria.parser.QueryParser

final case class GraphQLRequest(document: Document, variables: Json, operationName: Option[String])

object GraphQLRequest {
  def parseBody(body: String): Option[GraphQLRequest] =
    for {
      _ <- Option.when(InputBudget.lexical(body, graphql = false)) (())
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
      _ <- Option.when(InputBudget.lexical(query, graphql = true)) (())
      document <- QueryParser.parse(query).toOption
      _ <- Option.when(InputBudget.document(document, operationName)) (())
    } yield GraphQLRequest(document, variables, operationName)
}
