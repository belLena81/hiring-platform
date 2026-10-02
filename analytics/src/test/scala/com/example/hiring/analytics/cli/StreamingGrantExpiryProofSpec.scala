package com.example.hiring.analytics.cli

import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.config.KafkaConnection

final class StreamingGrantExpiryProofSpec extends munit.FunSuite {
  test("expiry identity metadata uses only the configured reader connection") {
    val reader = KafkaConnection("reader-broker:9093", Some("expiry-reader"), Some("synthetic-reader-credential"))
    val properties = StreamingGrantExpiryProofMain
      .identityAdminProperties(reader)
      .fold(
        error => throw new AssertionError(error),
        identity
      )
    assertEquals(properties.getProperty("bootstrap.servers"), reader.bootstrapServers)
    assertEquals(properties.getProperty("security.protocol"), "SASL_SSL")
    assert(properties.getProperty("sasl.jaas.config").contains("username=\"expiry-reader\""))
    Vector("key.serializer", "value.serializer", "acks", "transactional.id", "enable.idempotence")
      .foreach(key => assertEquals(properties.getProperty(key), null))
  }

  test("invalid reader credentials fail before expiry identity lookup") {
    val reader = KafkaConnection("reader-broker:9093", Some("expiry-reader"), None)
    assert(StreamingGrantExpiryProofMain.identityAdminProperties(reader).isLeft)
  }

  test("expiry observations recognize only typed production timer and activation gate errors") {
    assert(
      StreamingGrantExpiryProofMain.isExpiryFailure(
        AnalyticsError.InvalidConfiguration("analytics streaming activation grant expired")
      )
    )
    assert(
      StreamingGrantExpiryProofMain.isExpiryFailure(
        AnalyticsError.InvalidConfiguration(
          "analytics streaming activation is absent, malformed, or does not match this runtime"
        )
      )
    )
    Vector[Throwable](
      AnalyticsError.InvalidConfiguration(StreamingCheckpointStartupProofMain.CheckpointFailure),
      AnalyticsError.InvalidConfiguration("analytics configuration is invalid"),
      new IllegalStateException("analytics streaming activation grant expired")
    ).foreach(error => assert(!StreamingGrantExpiryProofMain.isExpiryFailure(error)))
  }
}
