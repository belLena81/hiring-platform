package com.example.hiring.analytics

import cats.effect.unsafe.implicits.global
import com.example.hiring.testing.LocalTestServices
import com.example.hiring.testing.LocalTestServices.TestSaslMechanism
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.common.errors.{AuthenticationException, SaslAuthenticationException}

import java.util.concurrent.TimeUnit
import scala.util.Try

/** KFH-06.1: the isolated broker's client listener authenticates SCRAM-SHA-512 only. The internal-listener half of the
  * check (a client principal refused over PLAIN on kafka:9092) runs inside the Compose network in the `initialize`
  * service of compose.test.yaml, because that listener is not published to the host.
  */
class KafkaScramAuthenticationIntegrationSpec extends munit.FunSuite {
  private def manifest: LocalTestServices.Manifest =
    LocalTestServices.manifest
      .unsafeRunSync()
      .getOrElse(throw new IllegalStateException("HIRING_TEST_MANIFEST is required for Kafka access"))

  private def clusterId(value: LocalTestServices.Manifest, user: String, password: String, m: TestSaslMechanism) = {
    val admin = Admin.create(LocalTestServices.adminProperties(value, user, password, m))
    try admin.describeCluster().clusterId().get(15L, TimeUnit.SECONDS)
    finally admin.close()
  }

  private def authenticationFailure(error: Throwable): Boolean =
    Iterator.iterate(error)(_.getCause).takeWhile(_ != null).exists(_.isInstanceOf[AuthenticationException])

  test("KFH-06.1 a SCRAM-configured client authenticates on the external listener") {
    val value = manifest
    assertEquals(
      clusterId(value, "analytics_reader", value.readerPassword, TestSaslMechanism.ScramSha512),
      value.kafkaClusterId
    )
  }

  test("KFH-06.1 a PLAIN-configured client is refused on the external listener") {
    val value = manifest
    val failure = Try(clusterId(value, "analytics_reader", value.readerPassword, TestSaslMechanism.Plain)).failed
    assert(failure.toOption.exists(authenticationFailure), s"expected an authentication failure: $failure")
  }

  test("KFH-06.1 a SCRAM client with a wrong password is refused") {
    val value = manifest
    val failure =
      Try(clusterId(value, "analytics_reader", value.readerPassword + "x", TestSaslMechanism.ScramSha512)).failed

    assert(
      failure.toOption.exists(error =>
        Iterator.iterate(error)(_.getCause).takeWhile(_ != null).exists(_.isInstanceOf[SaslAuthenticationException])
      ),
      s"expected a SASL authentication failure: $failure"
    )
  }
}
