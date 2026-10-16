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

  /** The search deployment is reachable but refused or cut the query: its time budget expired or its admission breaker
    * rejected it. Retrying the same expensive query elsewhere cannot help, so callers must not degrade to another
    * retrieval path.
    */
  case Overloaded

  /** Only the vector retrieval of a hybrid query failed while its lexical retrieval answered, so the lexical branch
    * alone may serve the request. A generic `Unavailable` never carries this meaning: it can be the whole store, and a
    * lexical query against the same store would fail the same way.
    */
  case VectorSearchUnavailable
}
