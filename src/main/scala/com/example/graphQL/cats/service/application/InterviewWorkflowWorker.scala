package com.example.graphQL.cats.service.application

import cats.{Applicative, MonadThrow}
import cats.effect.{Resource, Temporal}
import cats.syntax.all.*
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.{BackgroundWorker, Diagnostics, LogEvent, LogField, RepositoryError}
import scala.concurrent.duration.*

enum InterviewPublicationPass {
  case Full, Idle, Deferred
}

object InterviewWorkflowWorker {

  /** A full bounded pass yields and continues; idle or deferred work waits before polling again. */
  private[application] def publicationLoop[F[_]: Temporal](
      publish: F[InterviewPublicationPass],
      pollInterval: FiniteDuration
  ): fs2.Stream[F, Unit] = fs2.Stream.repeatEval(publish.flatMap {
    case InterviewPublicationPass.Full => Temporal[F].cede
    case _                             => Temporal[F].sleep(pollInterval)
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
// MonadThrow, not Monad: `Diagnostics.emit` swallows sink failures through ApplicativeError.
private[application] final class InterviewWorkerReporter[F[_]: MonadThrow](diagnostics: Diagnostics[F]) {
  def report(operation: String): F[Unit] =
    diagnostics.emit(LogEvent.MongoRepositoryFailed, fields = Map(LogField.SpanName -> operation))

  def observe[A](operation: RepositoryF[F, A], name: String): F[Option[A]] =
    operation.value.flatMap {
      case Right(value) => Applicative[F].pure(Some(value))
      case Left(_)      => report(name).as(None)
    }

  /** `true` when the repository accepted the call; a failure is reported and answers `false`. */
  def succeeded(operation: RepositoryF[F, ?], name: String): F[Boolean] = observe(operation, name).map(_.isDefined)
}

/** Resource ownership makes shutdown cancel the publisher before transport and database release. The worker is the
  * single entry point; publishing, message admission and provider execution live in their own collaborators.
  */
final class InterviewWorkflowWorker[F[_]](using F: Temporal[F])(
    repository: InterviewWorkflowRepository[F],
    calendar: InterviewCalendarProvider[F],
    notifications: InterviewNotificationProvider[F],
    settings: InterviewWorkerSettings,
    diagnostics: Diagnostics[F],
    currentTime: F[java.time.Instant] = F.realTimeInstant
) {
  private val reporter = new InterviewWorkerReporter[F](diagnostics)
  private val publication = new InterviewPublication[F](repository, settings, reporter, currentTime)
  private val receiver = new InterviewMessageReceiver[F](
    repository,
    settings,
    reporter,
    currentTime,
    new InterviewProviderExecutor[F](repository, calendar, notifications, currentTime)
  )

  def publisher(transport: InterviewTransport[F]): Resource[F, Unit] =
    BackgroundWorker.resource("interview-publisher", diagnostics)(publicationStream(transport).compile.drain)

  def publicationStream(transport: InterviewTransport[F]): fs2.Stream[F, Unit] =
    InterviewWorkflowWorker.publicationLoop(
      publishDue(transport).handleErrorWith {
        case fenced: InterviewProducerGenerationFenced => Temporal[F].raiseError(fenced)
        case _ => reporter.report("interviewWorkflow.publisher").as(InterviewPublicationPass.Deferred)
      },
      settings.pollInterval
    )

  def publishDue(transport: InterviewTransport[F]): F[InterviewPublicationPass] = publication.publishDue(transport)

  /** Succeeds once the message is durably handled; anything else fails, so its offset is never committed. */
  def receiveCommand(message: InterviewMessage): RepositoryF[F, Unit] = durably(receiver.receiveCommand(message))

  def receiveResult(message: InterviewMessage): RepositoryF[F, Unit] = durably(receiver.receiveResult(message))

  private def durably(handled: F[Boolean]): RepositoryF[F, Unit] =
    RepositoryF.fromFEither(handled.map(Either.cond(_, (), RepositoryError.Unavailable)))
}
