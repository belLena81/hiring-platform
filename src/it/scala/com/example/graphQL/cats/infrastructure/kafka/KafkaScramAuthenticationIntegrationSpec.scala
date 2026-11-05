package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.{IO, Resource}
import com.example.graphQL.cats.config.{KafkaSaslMechanism, KafkaSaslSecurityProtocol}
import com.example.graphQL.cats.repository.mongo.KafkaIntegrationSuite
import org.apache.kafka.clients.admin.Admin

import java.time.Duration
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*

/** KFH-06.1: the isolated broker's client listeners accept SCRAM-SHA-512 and refuse PLAIN. */
final class KafkaScramAuthenticationIntegrationSpec extends KafkaIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 2.minutes

  private def admin(mechanism: KafkaSaslMechanism): Resource[IO, Admin] =
    Resource.make(
      IO.blocking(
        Admin.create(
          KafkaClientSettings.clientProperties(
            kafkaNamespace.manifest.kafkaBootstrap,
            Some("hiring_publisher_v2"),
            Some(kafkaNamespace.manifest.publisherPassword),
            KafkaSaslSecurityProtocol.Plaintext,
            mechanism
          )
        )
      )
    )(client => IO.blocking(client.close(Duration.ofSeconds(2))))

  private def describe(mechanism: KafkaSaslMechanism): IO[Either[Throwable, String]] =
    admin(mechanism).use(client => IO.blocking(client.describeCluster().clusterId().get(8L, TimeUnit.SECONDS)).attempt)

  test("KFH-06.1 a SCRAM client authenticates and a PLAIN client against the same listener does not") {
    assume(kafkaEvidenceEnabled, "BLOCKED: isolated Kafka credentials absent")
    for {
      scram <- describe(KafkaSaslMechanism.ScramSha512)
      plain <- describe(KafkaSaslMechanism.Plain)
    } yield {
      assert(scram.isRight, clue(scram))
      assert(plain.isLeft, clue(plain))
      assert(
        plain.left.exists(error =>
          Iterator
            .iterate[Throwable](error)(_.getCause)
            .takeWhile(_ != null)
            .exists(_.isInstanceOf[org.apache.kafka.common.errors.AuthenticationException])
        ),
        clue(plain)
      )
    }
  }
}
