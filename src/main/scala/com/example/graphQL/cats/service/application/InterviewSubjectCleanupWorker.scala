package com.example.graphQL.cats.service.application

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.workflow.{
  InterviewSubjectCleanup,
  InterviewCleanupCommand,
  InterviewCleanupObservation
}
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogFields}
import com.example.graphQL.cats.service.Diagnostics.*
import scala.concurrent.duration.*

/** Interprets capability-specific cleanup commands; only durable observations advance the pure policy. */
final class InterviewSubjectCleanupWorker(
    repository: InterviewSubjectCleanupRepository,
    fencer: InterviewPublisherFencer,
    capture: IO[Vector[InterviewRetentionBarrier]],
    passed: Vector[InterviewRetentionBarrier] => IO[Boolean],
    diagnostics: Diagnostics
) {
  private def boundary[A](effect: IO[A]): RepositoryIO[A] =
    RepositoryIO.fromIOEither(effect.attempt.map(_.leftMap(_ => RepositoryError.Unavailable)))

  def runOnce: RepositoryIO[Unit] = repository.pending.flatMap(_.traverse_(process))

  private def process(current: InterviewSubjectCleanup): RepositoryIO[Unit] = {
    def advance(observation: InterviewCleanupObservation): RepositoryIO[Unit] =
      for {
        now <- RepositoryIO.lift(IO.realTimeInstant)
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
        .repeatEval(
          runOnce.value
            .flatMap {
              case Right(_) => IO.unit
              case Left(_)  => diagnostics.emit(LogEvent.RuntimeFailed)
            }
            .handleErrorWith(error => diagnostics.emit(LogEvent.RuntimeFailed, fields = LogFields.failure(error)))
        )
        .metered(1.second)
        .compile
        .drain
        .start
    )(_.cancel)
    .void
}
