package com.example.graphQL.cats.service.application

import cats.data.EitherT
import com.example.graphQL.cats.domain.workflow.InterviewWorkflowError
import com.example.graphQL.cats.service.UseCaseError

/** Why a cancel or reschedule action was refused. Authorization, visibility and storage failures keep the shared use
  * case errors; the interview rules (stale revision, expired proposal, started interview, ...) keep their own ADT so
  * the API boundary can give each a stable code.
  */
enum InterviewActionError {
  case UseCase(error: UseCaseError)
  case Workflow(error: InterviewWorkflowError)
}

type InterviewActionF[F[_], A] = EitherT[F, InterviewActionError, A]
