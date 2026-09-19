package com.example.graphQL.cats.repository.protocol

/** Failures an operational repository can report without exposing driver details. */
enum RepositoryError {
  case DuplicateApplication
  case Conflict
  case Unavailable
}
