package com.example.graphQL.cats.runtime

import com.example.graphQL.cats.service.port.{InterviewMessage, InterviewTransport}

import cats.effect.{IO, Resource}
import com.example.graphQL.cats.config.KafkaConfig
import com.example.graphQL.cats.infrastructure.kafka.*
import com.example.graphQL.cats.repository.mongo.{MongoInterviewSubjectCleanup, MongoInterviewWorkflowRepository}
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.application.{InterviewWorkflowWorker, InterviewWorkerSettings}
import com.example.graphQL.cats.service.port.{FakeInterviewCalendarProvider, FakeInterviewNotificationProvider}
import org.bson.Document
import scala.concurrent.duration.*

/** Local scheduling shares Mongo clients but has separate Kafka service principals and resource ownership. */
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
        settings.workerPassword
      ) match {
        case (Some(orchestratorUsername), Some(orchestratorPassword), Some(workerUsername), Some(workerPassword)) =>
          val worker = new InterviewWorkflowWorker(
            repository,
            FakeInterviewCalendarProvider.durable(repository),
            FakeInterviewNotificationProvider.durable(repository),
            InterviewWorkerSettings(
              "interview-local",
              1.second,
              settings.claimSeconds.seconds,
              settings.providerTimeoutSeconds.seconds,
              settings.maxAttempts,
              settings.retryBaseSeconds.seconds,
              settings.retryCapSeconds.seconds,
              settings.replayRetentionSeconds.seconds
            )
          )
          for {
            orchestrator <- InterviewKafkaRuntime.resource(
              InterviewKafkaConfig(
                config.bootstrapServers,
                orchestratorUsername,
                orchestratorPassword,
                config.saslSecurityProtocol,
                false
              ),
              diagnostics
            ) {
              case Right(message) => worker.receiveResult(message)
              case Left(identity) =>
                IO.realTimeInstant.flatMap(at => repository.quarantine(identity, at).value.map(_.isRight))
            }
            transport <- InterviewKafkaRuntime.resource(
              InterviewKafkaConfig(
                config.bootstrapServers,
                workerUsername,
                workerPassword,
                config.saslSecurityProtocol,
                true
              ),
              diagnostics
            ) {
              case Right(message) => worker.receiveCommand(message)
              case Left(identity) =>
                IO.realTimeInstant.flatMap(at => repository.quarantine(identity, at).value.map(_.isRight))
            }
            retention <- InterviewKafkaRetention.resource(
              config.bootstrapServers,
              orchestratorUsername,
              orchestratorPassword,
              config.saslSecurityProtocol
            )
            routing = new InterviewTransport {
              def publish(message: InterviewMessage): IO[Unit] =
                if (message.result.isDefined) transport.publish(message) else orchestrator.publish(message)
            }
            _ <- worker.publisher(routing)
            // A claim, a timed provider call and an acknowledged Kafka send must all drain before barrier capture.
            _ <- cleanup.resource(
              (settings.claimSeconds.toLong + settings.providerTimeoutSeconds.toLong + 30L).seconds,
              retention.capture.map(
                _.toList.map(value =>
                  new Document("topic", value.topic)
                    .append("partition", Int.box(value.partition))
                    .append("endOffset", Long.box(value.endOffset))
                )
              ),
              rows =>
                retention.passed(
                  rows
                    .map(row =>
                      InterviewRetentionBarrier(
                        row.getString("topic"),
                        row.getInteger("partition").intValue(),
                        row.getLong("endOffset").longValue()
                      )
                    )
                    .toVector
                ),
              diagnostics
            )
          } yield ()
        case _ => Resource.eval(IO.raiseError(new IllegalArgumentException("interview Kafka credentials unavailable")))
      }
  }
}
