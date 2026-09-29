package com.example.graphQL.cats.service

/** Failures an operational repository can report without exposing driver details. */
enum RepositoryError {
  case DuplicateApplication
  case Conflict
  case InvalidStoredData
  case MissingWriteResult
  case MissingStoredResult
  case Unavailable
}
