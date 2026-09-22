package com.example.graphQL.cats.domain.error

import com.example.graphQL.cats.domain.model.ApplicationStatus
import com.example.graphQL.cats.domain.model.JobStatus

enum DomainError {
  case CandidateRequired
  case RecruiterRequired
  case JobMustBeOpen
  case DuplicateApplication
  case SearchSessionPending
  case SearchSessionUnavailable
  case Forbidden
  case NotFound(entity: String)
  case InvalidJobTransition(from: JobStatus, to: JobStatus)
  case InvalidInitialJobStatus(status: JobStatus)
  case InvalidStatusTransition(from: ApplicationStatus, to: ApplicationStatus)
  case RejectionFeedbackRequired
  case DeclineReasonRequired
}

enum DomainValidationError {
  case BlankField(field: String)
  case EmptyCollection(field: String)
  case InvalidNumber(field: String, minimum: Int, maximum: Int, actual: Int)
  case TextTooLong(field: String, maximum: Int, actual: Int)
  case ByteLengthExceeded(field: String, maximum: Int, actual: Int)
  case TooManyValues(field: String, maximum: Int, actual: Int)
}
