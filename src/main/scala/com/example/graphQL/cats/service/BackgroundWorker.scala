package com.example.graphQL.cats.service

import cats.effect.{IO, Outcome, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.service.Diagnostics.*

/** Resource-owned background fibers whose outcome is observed instead of silently dropped.
  *
  * An errored worker is reported once with its name and the standard failure fields; cancellation on release is normal
  * shutdown and an orderly completion is silent. Restart loops belong inside `work`, not here.
  */
object BackgroundWorker {
  def resource(name: String, diagnostics: Diagnostics)(work: IO[Unit]): Resource[IO, Unit] =
    observed(name, diagnostics)(work).background.void

  private[service] def observed(name: String, diagnostics: Diagnostics)(work: IO[Unit]): IO[Unit] =
    work.guaranteeCase {
      case Outcome.Errored(error) =>
        diagnostics.emit(LogEvent.RuntimeFailed, fields = LogFields.failure(error) + (LogField.Worker -> name))
      case Outcome.Succeeded(_) | Outcome.Canceled() => IO.unit
    }
}
