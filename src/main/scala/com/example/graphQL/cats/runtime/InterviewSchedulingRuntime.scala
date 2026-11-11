package com.example.graphQL.cats.runtime

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.config.KafkaConfig
import com.example.graphQL.cats.infrastructure.kafka.*
import com.example.graphQL.cats.infrastructure.telemetry.KafkaClientMetrics
import com.example.graphQL.cats.repository.mongo.{MongoInterviewSubjectCleanup, MongoInterviewWorkflowRepository}
import com.example.graphQL.cats.service.{
  BackgroundWorker,
  Diagnostics,
  LogEvent,
  LogFields,
  ReadinessComponent,
  WorkerAlive
}
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
  private val PublicationWorker = "interview-publication"

  def resource(
      config: KafkaConfig,
      repository: MongoInterviewWorkflowRepository,
      cleanup: MongoInterviewSubjectCleanup,
      diagnostics: Diagnostics[IO],
      alive: ReadinessComponent => IO[WorkerAlive[IO]],
      metrics: KafkaClientMetrics = KafkaClientMetrics.noop
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
          InterviewNotificationProviderSelection
            .resource(settings.notifications, repository, diagnostics, SesProviderWiring.notAvailableInThisBuild)
            .flatMap { notifications =>
              val worker = new InterviewWorkflowWorker[IO](
                repository,
                LedgerInterviewCalendarProvider.durable(repository),
                notifications,
                InterviewWorkerSettings(
                  settings.workerId.value,
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
                orchestratorGroup = settings.orchestratorGroup,
                restartMaxDelaySeconds = config.restartMaxDelaySeconds,
                workerId = settings.workerId,
                mechanism = config.saslMechanism,
                maxPollRecords = settings.maxPollRecords,
                maxPollIntervalSeconds = settings.maxPollIntervalSeconds,
                commitBatchSize = settings.commitBatchSize,
                commitBatchIntervalMillis = settings.commitBatchIntervalMillis,
                claimSeconds = settings.claimSeconds
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
                orchestratorGroup = settings.orchestratorGroup,
                restartMaxDelaySeconds = config.restartMaxDelaySeconds,
                workerId = settings.workerId,
                mechanism = config.saslMechanism,
                maxPollRecords = settings.maxPollRecords,
                maxPollIntervalSeconds = settings.maxPollIntervalSeconds,
                commitBatchSize = settings.commitBatchSize,
                commitBatchIntervalMillis = settings.commitBatchIntervalMillis,
                claimSeconds = settings.claimSeconds
              )
              def receive(
                  handler: InterviewMessage => RepositoryF[IO, Unit]
              )(value: Either[String, InterviewMessage]): RepositoryF[IO, Unit] =
                value.fold(
                  identity => RepositoryF.lift(IO.realTimeInstant).flatMap(repository.quarantine(identity, _)),
                  handler
                )
              def generation: Stream[IO, Unit] = Stream
                .resource(
                  (
                    InterviewKafkaRuntime.publisherResource(commandConfig, diagnostics, metrics = metrics),
                    InterviewKafkaRuntime.publisherResource(resultConfig, diagnostics, metrics = metrics)
                  ).tupled
                )
                .flatMap { case (commands, results) =>
                  val routing = new InterviewTransport[IO] {
                    private def owner(message: InterviewMessage): InterviewTransport[IO] =
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
                orchestratorAlive <- Resource.eval(alive(ReadinessComponent.InterviewOrchestratorConsumer))
                workerAlive <- Resource.eval(alive(ReadinessComponent.InterviewWorkerConsumer))
                publicationAlive <- Resource.eval(alive(ReadinessComponent.InterviewPublication))
                cleanupAlive <- Resource.eval(alive(ReadinessComponent.InterviewSubjectCleanup))
                _ <- InterviewKafkaRuntime.consumerResource(commandConfig, diagnostics, orchestratorAlive, metrics)(
                  receive(worker.receiveResult)
                )
                _ <- InterviewKafkaRuntime.consumerResource(resultConfig, diagnostics, workerAlive, metrics)(
                  receive(worker.receiveCommand)
                )
                retention = InterviewKafkaRetention(
                  config.bootstrapServers,
                  orchestratorUsername,
                  orchestratorPassword,
                  config.saslSecurityProtocol,
                  config.saslMechanism,
                  settings.topics
                )
                fencer <- InterviewProducerFencer.resource(
                  config.bootstrapServers,
                  fencerUsername,
                  fencerPassword,
                  config.saslSecurityProtocol,
                  config.saslMechanism,
                  diagnostics
                )
                _ <- BackgroundWorker.resource(PublicationWorker, diagnostics)(
                  Stream.suspend(generation.evalTap(_ => publicationAlive.beat)).repeat.compile.drain
                )
                _ <- new InterviewSubjectCleanupWorker[IO](
                  cleanup,
                  fencer,
                  retention.capture,
                  retention.passed,
                  diagnostics,
                  topics = settings.topics,
                  alive = cleanupAlive.beat
                ).resource
              } yield ()
            }
        case _ => Resource.eval(IO.raiseError(new IllegalStateException("interview credentials were not validated")))
      }
  }
}
