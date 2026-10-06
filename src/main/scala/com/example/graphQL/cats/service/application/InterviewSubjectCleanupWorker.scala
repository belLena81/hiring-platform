package com.example.graphQL.cats.service.application

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.workflow.{
  InterviewSubjectCleanup,
  InterviewCleanupCommand,
  InterviewCleanupObservation
}
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField, LogFields}
import com.example.graphQL.cats.service.Diagnostics.*
import java.time.Instant
import scala.concurrent.duration.*

/** Interprets capability-specific cleanup commands; only durable observations advance the pure policy. */
final class InterviewSubjectCleanupWorker(
    repository: InterviewSubjectCleanupRepository,
    fencer: InterviewPublisherFencer,
    capture: IO[Vector[InterviewRetentionBarrier]],
    passed: Vector[InterviewRetentionBarrier] => IO[Boolean],
    diagnostics: Diagnostics,
    currentTime: IO[Instant] = IO.realTimeInstant
) {
  private def boundary[A](effect: IO[A]): RepositoryIO[A] =
    RepositoryIO.fromIOEither(effect.attempt.map(_.leftMap(_ => RepositoryError.Unavailable)))

  def runOnce(cursor: Option[InterviewCleanupCursor]): RepositoryIO[InterviewCleanupProgress] =
    RepositoryIO.lift(currentTime).flatMap(repository.pendingPage(cursor, _)).flatMap { page =>
      RepositoryIO.lift(
        page.entries
          .foldLeftM(Option.empty[RepositoryError]) { (firstFailure, subject) =>
            processObserved(subject).map(failure => firstFailure.orElse(failure))
          }
          .map(InterviewCleanupProgress(page.next, _))
      )
    }

  private def processObserved(subject: Either[RepositoryError, InterviewSubjectCleanup]): IO[Option[RepositoryError]] =
    subject.fold(error => RepositoryIO.fromEither[Unit](Left(error)), process).value.attempt.flatMap {
      case Right(Right(_))    => IO.pure(None)
      case Right(Left(error)) =>
        diagnostics
          .emit(LogEvent.RuntimeFailed, fields = Map(LogField.SpanName -> "interviewCleanup.process"))
          .as(Some(error))
      case Left(error) =>
        diagnostics
          .emit(
            LogEvent.RuntimeFailed,
            fields = LogFields.failure(error) + (LogField.SpanName -> "interviewCleanup.process")
          )
          .as(Some(RepositoryError.Unavailable))
    }

  private def process(current: InterviewSubjectCleanup): RepositoryIO[Unit] = {
    def advance(observation: InterviewCleanupObservation): RepositoryIO[Unit] =
      for {
        now <- RepositoryIO.lift(currentTime)
        next <- RepositoryIO.fromEither(
          InterviewSubjectCleanup.decide(current, observation, now).leftMap(_ => RepositoryError.InvalidStoredData)
        )
        _ <- repository.transition(current, next)
      } yield ()

    InterviewSubjectCleanup.command(current) match {
      case InterviewCleanupCommand.FenceProducers(ids) =>
        fencer.fence(ids) *> advance(InterviewCleanupObservation.ProducersFenced)
      case InterviewCleanupCommand.PurgeMongo =>
        repository.purge(current.subjectId) *> advance(InterviewCleanupObservation.MongoPurged)
      case InterviewCleanupCommand.CaptureBarriers =>
        boundary(capture).flatMap(barriers => advance(InterviewCleanupObservation.BarriersCaptured(barriers)))
      case InterviewCleanupCommand.AwaitRetention(barriers) =>
        boundary(passed(barriers)).flatMap {
          case false => RepositoryIO.fromEither(Right(()))
          case true  =>
            repository.purge(current.subjectId) *> repository.absent(current.subjectId).flatMap {
              case false => RepositoryIO.fromEither(Left(RepositoryError.Conflict))
              case true  => advance(InterviewCleanupObservation.RetentionPassedAndMongoAbsent)
            }
        }
      case InterviewCleanupCommand.Finished => RepositoryIO.fromEither(Right(()))
    }
  }

  def resource: Resource[IO, Unit] = Resource
    .make(
      fs2.Stream
        .unfoldEval(Option.empty[InterviewCleanupCursor]) { cursor =>
          runOnce(cursor).value.attempt.flatMap {
            case Right(Right(progress)) => IO.pure(Some(((), progress.next)))
            case Right(Left(_))         => diagnostics.emit(LogEvent.RuntimeFailed).as(Some(((), cursor)))
            case Left(error)            =>
              diagnostics
                .emit(LogEvent.RuntimeFailed, fields = LogFields.failure(error))
                .as(Some(((), cursor)))
          }
        }
        .metered(1.second)
        .compile
        .drain
        .start
    )(_.cancel)
    .void
}
