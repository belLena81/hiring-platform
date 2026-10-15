package com.example.graphQL.cats.service.application

import cats.Applicative
import cats.effect.{Resource, Temporal}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.example.graphQL.cats.domain.workflow.{
  InterviewSubjectCleanup,
  InterviewCleanupCommand,
  InterviewCleanupObservation,
  InterviewTopicPair
}
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField, LogFields}
import com.example.graphQL.cats.service.Diagnostics.*
import java.time.Instant
import scala.concurrent.duration.*

/** Interprets capability-specific cleanup commands; only durable observations advance the pure policy. */
final class InterviewSubjectCleanupWorker[F[_]](using F: Temporal[F])(
    repository: InterviewSubjectCleanupRepository[F],
    fencer: InterviewPublisherFencer[F],
    capture: F[Vector[InterviewRetentionBarrier]],
    passed: Vector[InterviewRetentionBarrier] => F[Boolean],
    diagnostics: Diagnostics[F],
    currentTime: F[Instant] = F.realTimeInstant,
    topics: InterviewTopicPair = InterviewTopicPair.Default
) {
  private def boundary[A](effect: F[A]): RepositoryF[F, A] =
    RepositoryF.fromFEither(effect.attempt.map(_.leftMap(_ => RepositoryError.Unavailable)))

  def runOnce(cursor: Option[InterviewCleanupCursor]): RepositoryF[F, InterviewCleanupProgress] =
    RepositoryF.lift(currentTime).flatMap(repository.pendingPage(cursor, _)).flatMap { page =>
      RepositoryF.lift(
        page.entries
          .foldLeftM(Option.empty[RepositoryError]) { (firstFailure, subject) =>
            processObserved(subject).map(failure => firstFailure.orElse(failure))
          }
          .map(InterviewCleanupProgress(page.next, _))
      )
    }

  private def processObserved(subject: Either[RepositoryError, InterviewSubjectCleanup]): F[Option[RepositoryError]] =
    subject.fold(error => RepositoryF.fromEither[F, Unit](Left(error)), process).value.attempt.flatMap {
      case Right(Right(_))    => Applicative[F].pure(None)
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

  private def process(current: InterviewSubjectCleanup): RepositoryF[F, Unit] = {
    def advance(observation: InterviewCleanupObservation): RepositoryF[F, Unit] =
      for {
        now <- RepositoryF.lift(currentTime)
        next <- RepositoryF.fromEither[F, InterviewSubjectCleanup](
          InterviewSubjectCleanup
            .decide(current, observation, now, topics)
            .leftMap(_ => RepositoryError.InvalidStoredData)
        )
        _ <- repository.transition(current, next)
      } yield ()

    InterviewSubjectCleanup.command(current) match {
      case InterviewCleanupCommand.FenceProducers(_) =>
        repository.producerBatch(current.subjectId).flatMap {
          case ids if ids.isEmpty => advance(InterviewCleanupObservation.ProducersFenced)
          case ids                =>
            fencer.fence(ids) *> RepositoryF
              .lift(currentTime)
              .flatMap(now => repository.markProducersFenced(current.subjectId, ids, now)) *>
              repository.producerBatch(current.subjectId).flatMap { remaining =>
                if (remaining.isEmpty) advance(InterviewCleanupObservation.ProducersFenced)
                else RepositoryF.pure[F, Unit](())
              }
        }
      case InterviewCleanupCommand.PurgeMongo =>
        repository.purge(current.subjectId) *> advance(InterviewCleanupObservation.MongoPurged)
      case InterviewCleanupCommand.CaptureBarriers =>
        boundary(capture).flatMap(barriers => advance(InterviewCleanupObservation.BarriersCaptured(barriers)))
      case InterviewCleanupCommand.AwaitRetention(barriers) =>
        boundary(passed(barriers)).flatMap {
          case false => RepositoryF.pure[F, Unit](())
          case true  =>
            repository.purge(current.subjectId) *> repository.absent(current.subjectId).flatMap {
              case false => RepositoryF.raise[F, Unit](RepositoryError.Conflict)
              case true  => advance(InterviewCleanupObservation.RetentionPassedAndMongoAbsent)
            }
        }
      case InterviewCleanupCommand.Finished => RepositoryF.pure[F, Unit](())
    }
  }

  def resource: Resource[F, Unit] = Resource
    .make(
      fs2.Stream
        .unfoldEval(Option.empty[InterviewCleanupCursor]) { cursor =>
          runOnce(cursor).value.attempt.flatMap {
            case Right(Right(progress)) => Applicative[F].pure(Some(((), progress.next)))
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
