package com.example.graphQL.cats.infrastructure.kafka

import com.example.graphQL.cats.config.{KafkaSaslMechanism, KafkaSaslSecurityProtocol}
import com.example.hiring.testing.LocalTestServices
import io.circe.Json
import io.circe.parser.parse
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.security.JaasContext

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.*

class KafkaClientSettingsSpec extends munit.FunSuite {
  private val contract: Json = {
    val relative = Path.of("test-support", "src", "main", "resources", "kafka-client-settings-contract.json")
    val start = Path.of(System.getProperty("user.dir")).toAbsolutePath
    val found = Iterator
      .iterate(Option(start))(_.flatMap(dir => Option(dir.getParent)))
      .flatten
      .map(_.resolve(relative))
      .find(Files.isRegularFile(_))
    parse(Files.readString(found.getOrElse(fail("Kafka client settings contract fixture is missing")))).toOption.get
  }

  private def protocol(name: String) =
    if (name == "SASL_SSL") KafkaSaslSecurityProtocol.Tls else KafkaSaslSecurityProtocol.Plaintext

  private def expected(json: Json): Map[String, String] = json.as[Map[String, String]].toOption.get

  private def cases(name: String): Vector[io.circe.HCursor] =
    contract.hcursor.downField(name).values.get.toVector.map(_.hcursor)

  test("credentialed settings match the shared contract and round-trip through the Kafka JAAS parser") {
    cases("credentialed").foreach { c =>
      val user = c.get[String]("username").toOption.get
      val secret = c.get[String]("password").toOption.get
      val want = expected(c.downField("expected").focus.get)
      val got = KafkaClientSettings.security(
        Some(user),
        Some(secret),
        protocol(c.get[String]("protocol").toOption.get),
        KafkaSaslMechanism.ScramSha512
      )
      assertEquals(got, want, c.get[String]("name").toOption.get)
      assertEquals(LocalTestServices.jaasConfig(user, secret), want("sasl.jaas.config"))
      val entry = JaasContext
        .loadClientContext(java.util.Map.of("sasl.jaas.config", new Password(got("sasl.jaas.config"))))
        .configurationEntries()
        .asScala
        .head
      assertEquals(entry.getOptions.get("username").toString, user)
      assertEquals(entry.getOptions.get("password").toString, secret)
    }
  }

  test("absent credentials produce an explicit PLAINTEXT transport as the shared contract states") {
    cases("anonymous").foreach { c =>
      val want = expected(c.downField("expectedRoot").focus.get)
      val selected = protocol(c.get[String]("protocol").toOption.get)
      assertEquals(KafkaClientSettings.security(None, None, selected, KafkaSaslMechanism.ScramSha512), want)
      assertEquals(
        KafkaClientSettings.security(Some("only-user"), None, selected, KafkaSaslMechanism.ScramSha512),
        want
      )
    }
  }

  test("producer settings carry the reliability literals and transport exactly once") {
    val properties = KafkaClientSettings
      .producer(
        "127.0.0.1:9092",
        Some("u"),
        Some("p"),
        KafkaSaslSecurityProtocol.Plaintext,
        65536,
        KafkaSaslMechanism.Plain,
        35
      )
      .properties
    assertEquals(properties("acks"), "all")
    assertEquals(properties("enable.idempotence"), "true")
    assertEquals(properties("max.request.size"), "65536")
    assertEquals(properties("delivery.timeout.ms"), "30000")
    assertEquals(properties("request.timeout.ms"), "10000")
    assertEquals(properties("security.protocol"), "SASL_PLAINTEXT")
    assertEquals(properties("bootstrap.servers"), "127.0.0.1:9092")
    assertEquals(properties("compression.type"), "lz4")
    assertEquals(properties("linger.ms"), "5")
  }

  private def producerProperties(leaseSeconds: Int) =
    KafkaClientSettings
      .producer(
        "127.0.0.1:9092",
        None,
        None,
        KafkaSaslSecurityProtocol.Tls,
        65536,
        KafkaSaslMechanism.ScramSha512,
        leaseSeconds
      )
      .properties

  test("KFH-08.2 the producer delivery and transaction timeouts are the lease less 5 s, so a wave ends inside it") {
    val properties = producerProperties(30)
    assertEquals(properties("transaction.timeout.ms"), "25000")
    assertEquals(properties("delivery.timeout.ms"), "25000")
    assertEquals(properties("request.timeout.ms"), "10000")
  }

  test("KFH-08.2 a lease at the 15 s floor keeps the request timeout below the delivery timeout") {
    val properties = producerProperties(15)
    assertEquals(properties("transaction.timeout.ms"), "10000")
    assertEquals(properties("delivery.timeout.ms"), "10000")
    assert(properties("request.timeout.ms").toInt < properties("delivery.timeout.ms").toInt)
  }

  test("KFH-08.2 a lease below the floor never yields a transaction timeout under 10 s") {
    assertEquals(producerProperties(3)("transaction.timeout.ms"), "10000")
  }

  test("KFH-08.2 the transaction timeout never exceeds the broker default maximum, at the 905 s lease bound") {
    assertEquals(KafkaClientSettings.BrokerDefaultTransactionMaxTimeoutMillis, 900000)
    assertEquals(producerProperties(905)("transaction.timeout.ms"), "900000")
    assertEquals(producerProperties(905)("delivery.timeout.ms"), "900000")
    assertEquals(producerProperties(904)("transaction.timeout.ms"), "899000")
    // Defensive: a lease beyond configuration validation is capped for the transaction, while delivery stays derived.
    assertEquals(producerProperties(3600)("transaction.timeout.ms"), "900000")
    assertEquals(producerProperties(3600)("delivery.timeout.ms"), "3595000")
  }

  test("a Kafka runtime configuration never prints its password") {
    val secret = "s3cret-value"
    val text = InterviewKafkaConfig(
      "127.0.0.1:9092",
      "orchestrator",
      secret,
      KafkaSaslSecurityProtocol.Tls,
      worker = false
    ).toString
    assert(!text.contains(secret), text)
    assert(text.contains("[REDACTED]"))
    val interview = com.example.graphQL.cats.config
      .InterviewRuntimeConfig(
        orchestratorPassword = Some(secret),
        workerPassword = Some(secret),
        fencerPassword = Some(secret)
      )
      .toString
    assert(!interview.contains(secret), interview)
    assert(interview.contains("[REDACTED]"))
  }

  test("SCRAM-SHA-512 emits its mechanism and login module, PLAIN emits its own") {
    val scram = KafkaClientSettings.security(
      Some("u"),
      Some("p"),
      KafkaSaslSecurityProtocol.Tls,
      KafkaSaslMechanism.ScramSha512
    )
    assertEquals(scram("sasl.mechanism"), "SCRAM-SHA-512")
    assertEquals(
      scram("sasl.jaas.config"),
      "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"u\" password=\"p\";"
    )
    val plain = KafkaClientSettings.security(
      Some("u"),
      Some("p"),
      KafkaSaslSecurityProtocol.Plaintext,
      KafkaSaslMechanism.Plain
    )
    assertEquals(plain("sasl.mechanism"), "PLAIN")
    assert(plain("sasl.jaas.config").startsWith("org.apache.kafka.common.security.plain.PlainLoginModule required"))
  }

  test("every client builder carries the configured mechanism") {
    val scram = KafkaSaslMechanism.ScramSha512
    val tls = KafkaSaslSecurityProtocol.Tls
    assertEquals(
      KafkaClientSettings.consumer("b:9093", Some("u"), Some("p"), tls, scram).properties("sasl.mechanism"),
      "SCRAM-SHA-512"
    )
    assertEquals(
      KafkaClientSettings.clientProperties("b:9093", Some("u"), Some("p"), tls, scram).getProperty("sasl.mechanism"),
      "SCRAM-SHA-512"
    )
    assertEquals(
      KafkaClientSettings.admin("b:9093", Some("u"), Some("p"), tls, scram).properties("sasl.mechanism"),
      "SCRAM-SHA-512"
    )
  }

  test("consumer settings without credentials are explicitly PLAINTEXT") {
    val properties =
      KafkaClientSettings
        .consumer("127.0.0.1:9092", None, None, KafkaSaslSecurityProtocol.Tls, KafkaSaslMechanism.ScramSha512)
        .properties
    assertEquals(properties("security.protocol"), "PLAINTEXT")
  }

  test("blocking clients get one bounded timeout and the shared transport security") {
    val properties = KafkaClientSettings.clientProperties(
      "broker:9092",
      Some("user"),
      Some("secret"),
      KafkaSaslSecurityProtocol.Plaintext,
      KafkaSaslMechanism.Plain
    )
    assertEquals(properties.getProperty("bootstrap.servers"), "broker:9092")
    assertEquals(properties.getProperty("request.timeout.ms"), "10000")
    assertEquals(properties.getProperty("default.api.timeout.ms"), "10000")
    assertEquals(
      KafkaClientSettings
        .security(Some("user"), Some("secret"), KafkaSaslSecurityProtocol.Plaintext, KafkaSaslMechanism.Plain)
        .toSet,
      properties.asScala.view.filterKeys(_.startsWith("s")).toMap.toSet
    )
  }

  private def committed(maxPollRecords: Int, maxPollIntervalSeconds: Int) =
    KafkaClientSettings.committedConsumer(
      "broker:9092",
      None,
      None,
      KafkaSaslSecurityProtocol.Tls,
      "g",
      KafkaSaslMechanism.ScramSha512,
      maxPollRecords,
      maxPollIntervalSeconds
    )

  test("KFH-08.1 consumer settings carry max.poll.records and max.poll.interval.ms from configuration") {
    val properties = committed(maxPollRecords = 37, maxPollIntervalSeconds = 120).properties
    assertEquals(properties("max.poll.records"), "37")
    assertEquals(properties("max.poll.interval.ms"), "120000")
  }

  test(
    "DDC-02.12 the committed consumer resets to earliest, reads committed records only and never auto-commits"
  ) {
    val settings = committed(maxPollRecords = 100, maxPollIntervalSeconds = 300)
    assertEquals(
      List("group.id", "enable.auto.commit", "auto.offset.reset", "isolation.level")
        .map(key => key -> settings.properties(key))
        .toMap,
      Map(
        "group.id" -> "g",
        "enable.auto.commit" -> "false",
        "auto.offset.reset" -> "earliest",
        "isolation.level" -> "read_committed"
      )
    )
  }
}
