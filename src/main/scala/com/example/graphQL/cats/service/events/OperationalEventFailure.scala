package com.example.graphQL.cats.service.events

import com.example.graphQL.cats.service.{RepositoryError, UseCaseError}

/** The one reading of an operational event contract violation by a use case. A service builds its own events, so a
  * violation is an internal fault, never a client error: every case is the repository's `InvalidEvent`, which the API
  * boundary reports as a sanitized unavailable store.
  */
object OperationalEventFailure {

  extension (error: OperationalEventContractError) {
    def asUseCaseError: UseCaseError =
      error match {
        case OperationalEventContractError.MalformedEnvelope | OperationalEventContractError.InvalidPayload |
            OperationalEventContractError.AggregateMismatch | OperationalEventContractError.EnvelopeTooLarge =>
          UseCaseError.Repository(RepositoryError.InvalidEvent)
      }
  }
}
