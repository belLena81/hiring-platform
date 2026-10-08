package com.example.hiring.analytics

import cats.effect.IO
import com.example.hiring.analytics.config.*
import java.net.InetAddress

class KafkaConnectionPreflightSpec extends munit.CatsEffectSuite {
  private val connection =
    KafkaConnection("localhost:9092", securityProtocol = KafkaSecurityProtocol.SaslPlaintext, allowPlaintext = true)
  test("DNS executes only when the preflight effect runs and rejects mixed address sets") {
    var calls = 0
    val check = KafkaConnection.preflightUsing[IO](
      connection,
      _ => {
        calls += 1
        Some(
          Vector(InetAddress.getByAddress(Array[Byte](127, 0, 0, 1)), InetAddress.getByAddress(Array[Byte](8, 8, 8, 8)))
        )
      }
    )
    assert(KafkaConnection.validate(connection).isValid)
    assertEquals(calls, 0)
    check.attempt.map { result => assert(result.isLeft); assertEquals(calls, 1) }
  }
  test("Compose endpoint bypasses DNS while loopback requires successful resolution") {
    for {
      _ <- KafkaConnection
        .preflightUsing[IO](connection.copy(bootstrapServers = "kafka:9092"), _ => throw new AssertionError("DNS"))
      failed <- KafkaConnection.preflightUsing[IO](connection, _ => None).attempt
    } yield assert(failed.isLeft)
  }
}
