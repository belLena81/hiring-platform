package com.example.hiring.analytics.adapter.local

import com.example.hiring.analytics.errors.AnalyticsError

import cats.effect.kernel.Async
import cats.effect.syntax.all.*
import cats.syntax.all.*
import fs2.Stream
import fs2.io.process.{ProcessBuilder, Processes}
import fs2.text

import scala.concurrent.duration.*

/** Runs bounded host commands for local analytics operator proofs. */
private[analytics] object LocalProcess {
  final case class Result(exitCode: Int, output: String)

  def run[F[_]: Async](
      args: Vector[String],
      timeoutMessage: String,
      launchFailureMessage: String,
      timeout: FiniteDuration = 30.seconds
  ): F[Result] =
    if (args.isEmpty)
      Async[F].raiseError(AnalyticsError.InvalidConfiguration("local command is empty"))
    else {
      val process = ProcessBuilder(args.head, args.tail*).withRedirectErrorStream(true)
      val completed = process.spawn[F](using Processes.forAsync[F]).use { running =>
        val closeInput = Stream.empty.covary[F].through(running.stdin).compile.drain
        val output = running.stdout.through(text.utf8.decode).compile.string.map(_.trim)
        closeInput *> (output, running.exitValue).tupled
      }
      completed
        .map { case (output, exitCode) => Result(exitCode, output) }
        .timeoutTo(
          timeout,
          Async[F].raiseError(AnalyticsError.InvalidConfiguration(timeoutMessage))
        )
        .adaptError {
          case error: AnalyticsError => error
          case _                     => AnalyticsError.InvalidConfiguration(launchFailureMessage)
        }
    }
}
