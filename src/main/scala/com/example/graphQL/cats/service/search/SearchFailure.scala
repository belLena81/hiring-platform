package com.example.graphQL.cats.service.search

import com.example.graphQL.cats.service.{RepositoryError, SearchError, UseCaseError}

/** How search reads a use-case failure. `UseCaseF.translate` is the one repository-to-use-case translation (it already
  * turns the repository's lone-vector-branch failure into `SearchError.VectorSearchUnavailable`); these views add only
  * the search-specific reading and never re-match on `RepositoryError` elsewhere in the search services.
  */
object SearchFailure {

  extension (error: UseCaseError) {

    /** The lexical-fallback reason when exactly this failure is degradable: an unavailable query embedding provider, or
      * a vector search that failed while the lexical branch answered. Everything else, including a whole-store
      * `Unavailable`, a time-budget or admission `Overloaded` refusal, and corrupt stored data, is a failure: a lexical
      * query against the same store would fail the same way or repeat the same load.
      */
    def degradation: Option[DegradedSearch] =
      error match {
        case UseCaseError.Search(reason @ (SearchError.ProviderUnavailable | SearchError.VectorSearchUnavailable)) =>
          Some(DegradedSearch(reason))
        case _ => None
      }

    /** The failure a search caller sees once retrieval has been judged non-degradable. A revoked authority
      * (`Forbidden`) and a write conflict keep their meaning; every other store fault (`Unavailable`, `Overloaded`,
      * corrupt stored data, ...) is sanitized to an unavailable search so the API never reveals the store's state. The
      * degradable cases were already separated by [[degradation]], from the unmapped failure.
      */
    def asSearchFailure: UseCaseError =
      error match {
        case UseCaseError.Repository(RepositoryError.Conflict) => error
        case UseCaseError.Repository(_) => UseCaseError.Search(SearchError.VectorSearchUnavailable)
        case other                      => other
      }
  }
}
