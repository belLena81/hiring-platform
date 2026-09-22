package com.example.graphQL.cats.shared

import org.http4s.Uri

object HiringHttpPaths {
  val Health = "/health"
  val Ready = "/ready"
  val GraphQL = "/graphql"
  val Schema = "/schema.graphql"

  val HealthPath: Uri.Path = Uri.Path.Root / "health"
  val ReadyPath: Uri.Path = Uri.Path.Root / "ready"
  val GraphQLPath: Uri.Path = Uri.Path.Root / "graphql"
  val SchemaPath: Uri.Path = Uri.Path.Root / "schema.graphql"

  val public: Set[String] = Set(Health, Ready, GraphQL, Schema)
}
