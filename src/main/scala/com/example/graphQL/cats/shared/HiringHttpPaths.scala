package com.example.graphQL.cats.shared

object HiringHttpPaths {
  val Health = "/health"
  val Ready = "/ready"
  val GraphQL = "/graphql"
  val Schema = "/schema.graphql"

  val public: Set[String] = Set(Health, Ready, GraphQL, Schema)
}
