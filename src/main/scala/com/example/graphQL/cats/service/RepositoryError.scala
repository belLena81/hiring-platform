package com.example.graphQL.cats.service

/** Failures an operational repository can report without exposing driver details. */
enum RepositoryError {
  case DuplicateApplication
  case AuthorityRevoked
  case Conflict
  case InvalidEvent
  case InvalidStoredData
  case MissingStoredResult
  case Unavailable
}
