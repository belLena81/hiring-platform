package com.example.graphQL.cats.shared

import org.http4s.Uri

object HiringHttpPaths {
  val Health = "/health"
  val Ready = "/ready"
  val GraphQL = "/graphql"
  val Schema = "/schema.graphql"

  val HealthPath: Uri.Path = Uri.Path.unsafeFromString(Health)
  val ReadyPath: Uri.Path = Uri.Path.unsafeFromString(Ready)
  val GraphQLPath: Uri.Path = Uri.Path.unsafeFromString(GraphQL)

  val public: Set[String] = Set(Health, Ready, GraphQL, Schema)
}
