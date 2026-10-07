package com.example.graphQL.cats.runtime

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.config.KafkaConfig
import com.example.graphQL.cats.infrastructure.kafka.*
import com.example.graphQL.cats.repository.mongo.{MongoInterviewSubjectCleanup, MongoInterviewWorkflowRepository}
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogFields}
import com.example.graphQL.cats.service.Diagnostics.*
import com.example.graphQL.cats.service.application.{
  InterviewWorkflowWorker,
  InterviewWorkerSettings,
  InterviewSubjectCleanupWorker
}
import com.example.graphQL.cats.service.port.*
import fs2.Stream
import scala.concurrent.duration.*

/** Each publisher generation owns its initialized producers; consumers retain independent offset lifetimes. */
private[runtime] object InterviewSchedulingRuntime {
  def resource(
      config: KafkaConfig,
      repository: MongoInterviewWorkflowRepository,
      cleanup: MongoInterviewSubjectCleanup,
      diagnostics: Diagnostics
  ): Resource[IO, Unit] = {
    val settings = config.interview
    if (!settings.enabled) Resource.unit[IO]
    else
      (
        settings.orchestratorUsername,
        settings.orchestratorPassword,
        settings.workerUsername,
        settings.workerPassword,
        settings.fencerUsername,
        settings.fencerPassword
      ) match {
        case (
              Some(orchestratorUsername),
              Some(orchestratorPassword),
              Some(workerUsername),
              Some(workerPassword),
              Some(fencerUsername),
              Some(fencerPassword)
            ) =>
          val worker = new InterviewWorkflowWorker(
            repository,
            FakeInterviewCalendarProvider.durable(repository),
            FakeInterviewNotificationProvider.durable(repository),
            InterviewWorkerSettings(
              "interview-local",
              settings.publicationPollIntervalMs.millis,
              settings.claimSeconds.seconds,
              settings.providerTimeoutSeconds.seconds,
              settings.maxAttempts,
              settings.retryBaseSeconds.seconds,
              settings.retryCapSeconds.seconds,
              settings.replayRetentionSeconds.seconds,
              settings.clockSkewToleranceMillis.millis,
              settings.publicationBatchSize
            ),
            diagnostics = diagnostics
          )
          val commandConfig = InterviewKafkaConfig(
            config.bootstrapServers,
            orchestratorUsername,
            orchestratorPassword,
            config.saslSecurityProtocol,
            worker = false,
            partitionConcurrency = settings.partitionConcurrency,
            topics = settings.topics,
            workerGroup = settings.workerGroup,
            orchestratorGroup = settings.orchestratorGroup
          )
          val resultConfig = InterviewKafkaConfig(
            config.bootstrapServers,
            workerUsername,
            workerPassword,
            config.saslSecurityProtocol,
            worker = true,
            partitionConcurrency = settings.partitionConcurrency,
            topics = settings.topics,
            workerGroup = settings.workerGroup,
            orchestratorGroup = settings.orchestratorGroup
          )
          def receive(handler: InterviewMessage => IO[Boolean])(value: Either[String, InterviewMessage]): IO[Boolean] =
            value.fold(
              identity => IO.realTimeInstant.flatMap(at => repository.quarantine(identity, at).value.map(_.isRight)),
              handler
            )
          def generation: Stream[IO, Unit] = Stream
            .resource(
              (
                InterviewKafkaRuntime.publisherResource(commandConfig, diagnostics),
                InterviewKafkaRuntime.publisherResource(resultConfig, diagnostics)
              ).tupled
            )
            .flatMap { case (commands, results) =>
              val routing = new InterviewTransport {
                private def owner(message: InterviewMessage): InterviewTransport =
                  if (message.result.isDefined) results else commands
                def generationFor(message: InterviewMessage): InterviewPublisherGeneration =
                  owner(message).generationFor(message)
                def publish(message: InterviewMessage): IO[Unit] = owner(message).publish(message)
              }
              worker.publicationStream(routing)
            }
            .handleErrorWith { error =>
              Stream.eval(diagnostics.emit(LogEvent.RuntimeFailed, fields = LogFields.failure(error))) ++ Stream
                .sleep_[IO](settings.retryBaseSeconds.seconds)
            }
          for {
            _ <- InterviewKafkaRuntime.consumerResource(commandConfig, diagnostics)(receive(worker.receiveResult))
            _ <- InterviewKafkaRuntime.consumerResource(resultConfig, diagnostics)(receive(worker.receiveCommand))
            retention <- InterviewKafkaRetention.resource(
              config.bootstrapServers,
              orchestratorUsername,
              orchestratorPassword,
              config.saslSecurityProtocol,
              settings.topics
            )
            fencer <- InterviewProducerFencer.resource(
              config.bootstrapServers,
              fencerUsername,
              fencerPassword,
              config.saslSecurityProtocol,
              diagnostics
            )
            _ <- Resource.make(Stream.suspend(generation).repeat.compile.drain.start)(_.cancel)
            _ <- new InterviewSubjectCleanupWorker(
              cleanup,
              fencer,
              retention.capture,
              retention.passed,
              diagnostics,
              topics = settings.topics
            ).resource
          } yield ()
        case _ => Resource.eval(IO.raiseError(new IllegalStateException("interview credentials were not validated")))
      }
  }
}
