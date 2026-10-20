package com.example.graphQL.cats.service

import cats.MonadThrow
import cats.syntax.all.*
import com.example.graphQL.cats.service.Diagnostics.emit
import com.example.graphQL.cats.service.port.RepositoryF

/** Reports a typed repository failure through diagnostics instead of dropping it. */
object RepositoryReporting {

  extension [F[_], A](step: RepositoryF[F, A]) {

    /** Runs the step. A typed failure is reported once as `event`, with its safe error code, and replaced by `default`;
      * the caller decides what a failed step means (retry, `false`, nothing to do) without matching on `Either`.
      */
    def orReport(diagnostics: Diagnostics[F], event: LogEvent)(default: => A)(using F: MonadThrow[F]): F[A] =
      step.valueOrF(error => diagnostics.emit(event, fields = LogFields.repositoryFailure(error)).as(default))
  }
}
