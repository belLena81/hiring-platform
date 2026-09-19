package com.example.graphQL.cats.service

/** Compatibility alias while repository failures move to the port boundary. */
type RepositoryError = com.example.graphQL.cats.repository.protocol.RepositoryError

object RepositoryError {
  export com.example.graphQL.cats.repository.protocol.RepositoryError.*
}
