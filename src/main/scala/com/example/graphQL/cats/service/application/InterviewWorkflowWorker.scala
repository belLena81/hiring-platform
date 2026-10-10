package com.example.graphQL.cats.service.application

import cats.effect.{Clock, IO, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField, RepositoryError}
import scala.concurrent.duration.*

enum InterviewPublicationPass {
  case Full, Idle, Deferred
}

object InterviewWorkflowWorker {

  /** A full bounded pass yields and continues; idle or deferred work waits before polling again. */
  private[application] def publicationLoop(
      publish: IO[InterviewPublicationPass],
      pollInterval: FiniteDuration
  ): fs2.Stream[IO, Unit] = fs2.Stream.repeatEval(publish.flatMap {
    case InterviewPublicationPass.Full => IO.cede
    case _                             => IO.sleep(pollInterval)
  })
}

final case class InterviewWorkerSettings(
    workerId: String,
    pollInterval: FiniteDuration,
    claimLease: FiniteDuration,
    providerTimeout: FiniteDuration,
    maxAttempts: Int,
    initialBackoff: FiniteDuration,
    maxBackoff: FiniteDuration,
    replayWindow: FiniteDuration = 7.days,
    clockSkewTolerance: FiniteDuration = 5.seconds,
    publicationBatchSize: Int = 16
)

/** Reports a failed repository call under its operation name; callers decide what a failure means for the message. */
private[application] final class InterviewWorkerReporter(diagnostics: Diagnostics) {
  def report(operation: String): IO[Unit] =
    diagnostics.emit(LogEvent.MongoRepositoryFailed, fields = Map(LogField.SpanName -> operation))

  def observe[A](operation: RepositoryIO[A], name: String): IO[Option[A]] =
    operation.value.flatMap {
      case Right(value) => IO.pure(Some(value))
      case Left(_)      => report(name).as(None)
    }

  /** `true` when the repository accepted the call; a failure is reported and answers `false`. */
  def succeeded(operation: RepositoryIO[?], name: String): IO[Boolean] = observe(operation, name).map(_.isDefined)
}

/** Resource ownership makes shutdown cancel the publisher before transport and database release. The worker is the
  * single entry point; publishing, message admission and provider execution live in their own collaborators.
  */
final class InterviewWorkflowWorker(
    repository: InterviewWorkflowRepository,
    calendar: InterviewCalendarProvider,
    notifications: InterviewNotificationProvider,
    settings: InterviewWorkerSettings,
    diagnostics: Diagnostics,
    currentTime: IO[java.time.Instant] = Clock[IO].realTimeInstant
) {
  private val reporter = new InterviewWorkerReporter(diagnostics)
  private val publication = new InterviewPublication(repository, settings, reporter, currentTime)
  private val receiver = new InterviewMessageReceiver(
    repository,
    settings,
    reporter,
    currentTime,
    new InterviewProviderExecutor(repository, calendar, notifications, currentTime)
  )

  def publisher(transport: InterviewTransport): Resource[IO, Unit] =
    Resource
      .make(publicationStream(transport).compile.drain.start)(_.cancel)
      .void

  def publicationStream(transport: InterviewTransport): fs2.Stream[IO, Unit] =
    InterviewWorkflowWorker.publicationLoop(
      publishDue(transport).handleErrorWith {
        case fenced: InterviewProducerGenerationFenced => IO.raiseError(fenced)
        case _ => reporter.report("interviewWorkflow.publisher").as(InterviewPublicationPass.Deferred)
      },
      settings.pollInterval
    )

  def publishDue(transport: InterviewTransport): IO[InterviewPublicationPass] = publication.publishDue(transport)

  /** Succeeds once the message is durably handled; anything else fails, so its offset is never committed. */
  def receiveCommand(message: InterviewMessage): RepositoryIO[Unit] = durably(receiver.receiveCommand(message))

  def receiveResult(message: InterviewMessage): RepositoryIO[Unit] = durably(receiver.receiveResult(message))

  private def durably(handled: IO[Boolean]): RepositoryIO[Unit] =
    RepositoryIO.fromIOEither(handled.map(Either.cond(_, (), RepositoryError.Unavailable)))
}
