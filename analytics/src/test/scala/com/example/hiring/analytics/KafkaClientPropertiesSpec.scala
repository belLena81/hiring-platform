package com.example.hiring.analytics

import com.example.hiring.analytics.adapter.kafka.KafkaClientProperties
import com.example.hiring.analytics.config.{KafkaConnection, KafkaSecurityProtocol}
import com.example.hiring.analytics.adapter.spark.KafkaOffsetRangeSource
import com.example.hiring.analytics.errors.AnalyticsError
import io.circe.parser.parse

import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.security.JaasContext

class KafkaClientPropertiesSpec extends munit.FunSuite {
  private val contract = {
    val relative =
      java.nio.file.Path.of("test-support", "src", "main", "resources", "kafka-client-settings-contract.json")
    val start = java.nio.file.Path.of(System.getProperty("user.dir")).toAbsolutePath
    val found = Iterator
      .iterate(Option(start))(_.flatMap(dir => Option(dir.getParent)))
      .flatten
      .map(_.resolve(relative))
      .find(java.nio.file.Files.isRegularFile(_))
    parse(
      java.nio.file.Files.readString(found.getOrElse(fail("Kafka client settings contract fixture is missing")))
    ).toOption.get
  }

  private def contractCases(name: String) = contract.hcursor.downField(name).values.get.toVector.map(_.hcursor)

  private def protocol(name: String) =
    if (name == "SASL_SSL") KafkaSecurityProtocol.SaslSsl else KafkaSecurityProtocol.SaslPlaintext

  test("credentialed and anonymous properties match the contract shared with the application build") {
    contractCases("credentialed").foreach { c =>
      val user = c.get[String]("username").toOption.get
      val secret = c.get[String]("password").toOption.get
      val selected = protocol(c.get[String]("protocol").toOption.get)
      val connection =
        KafkaConnection("localhost:9092", Some(user), Some(secret), selected, allowPlaintext = true)
      val want = c.downField("expected").focus.get.as[Map[String, String]].toOption.get
      assertEquals(KafkaClientProperties.clientProperties(connection), Right(want), c.get[String]("name").toOption.get)
      assertEquals(com.example.hiring.testing.LocalTestServices.jaasConfig(user, secret), want("sasl.jaas.config"))
    }
    contractCases("anonymous").foreach { c =>
      val connection =
        KafkaConnection("localhost:9092", None, None, protocol(c.get[String]("protocol").toOption.get), true)
      val want = c.downField("expectedAnalytics").focus.get.as[Map[String, String]].toOption.get
      assertEquals(KafkaClientProperties.clientProperties(connection), Right(want), c.get[String]("name").toOption.get)
    }
  }

  test("admin and consumer properties keep the exact keys and values of the former hand-built copies") {
    val connection = KafkaConnection("broker.example:9093", Some("reader"), Some("credential"))
    val client = KafkaClientProperties.clientProperties(connection).toOption.get
    val des = "org.apache.kafka.common.serialization.ByteArrayDeserializer"
    val host = "bootstrap.servers" -> "broker.example:9093"
    val timeout = "default.api.timeout.ms" -> "10000"
    val consumer =
      Map(host, timeout, "key.deserializer" -> des, "value.deserializer" -> des, "enable.auto.commit" -> "false")
    def want(base: Map[String, String], extra: (String, String)) = Right(base + extra ++ client)
    assertEquals(
      KafkaClientProperties.adminProperties(connection),
      want(Map(host, timeout), "request.timeout.ms" -> "10000")
    )
    assertEquals(
      KafkaClientProperties.readCommittedConsumerProperties(connection),
      want(consumer, "isolation.level" -> "read_committed")
    )
    assertEquals(
      KafkaClientProperties.retentionConsumerProperties(connection),
      want(consumer, "group.id" -> "hiring-analytics-erasure")
    )
    assertEquals(KafkaClientProperties.asJava(Map("a" -> "1")).getProperty("a"), "1")
  }

  test("JAAS connector options round-trip quoted, slashed, and control characters") {
    val username = "reader\"\\line\nuser"
    val password = "secret\"\\tab\tvalue"
    val connection = KafkaConnection("localhost:9092", Some(username), Some(password))
    val jaas = KafkaClientProperties.clientProperties(connection).toOption.get.apply("sasl.jaas.config")
    val parsed = JaasContext.loadClientContext(java.util.Map.of("sasl.jaas.config", new Password(jaas)))
    val options = parsed.configurationEntries().get(0).getOptions

    assertEquals(options.get("username").toString, username)
    assertEquals(options.get("password").toString, password)
  }

  test("connector options retain the current secure protocol and mechanism") {
    val connection = KafkaConnection("broker.example:9093", Some("reader"), Some("credential"))
    val options = KafkaClientProperties.clientProperties(connection).toOption.get
    assertEquals(
      options,
      Map(
        "security.protocol" -> "SASL_SSL",
        "sasl.mechanism" -> "PLAIN",
        "sasl.jaas.config" ->
          "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"reader\" password=\"credential\";"
      )
    )
    assertEquals(
      KafkaClientProperties.sparkOptions(connection).toOption.get,
      Map(
        "kafka.security.protocol" -> "SASL_SSL",
        "kafka.sasl.mechanism" -> "PLAIN",
        "kafka.sasl.jaas.config" ->
          "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"reader\" password=\"credential\";",
        "kafka.group.id" -> "hiring-analytics-batch",
        "kafka.isolation.level" -> "read_committed"
      )
    )
  }

  test("connections without SASL credentials still set the secure protocol explicitly") {
    assertEquals(
      KafkaClientProperties.clientProperties(KafkaConnection("broker.example:9093")).toOption.get,
      Map("security.protocol" -> "SASL_SSL")
    )
  }

  test("Kafka assignment and offset JSON round-trip unusual topics in sorted partition order") {
    val topic = "topic\"\\\n\u0001"
    val ranges = Vector(
      TestPartitionOffsetRange.unsafe(topic, 3, 12L, 15L),
      TestPartitionOffsetRange.unsafe(topic, 1, 4L, 9L),
      TestPartitionOffsetRange.unsafe("a-topic", 2, 7L, 10L)
    )
    val assignments = parse(KafkaOffsetRangeSource.assignJson(ranges)).toOption.get
    val offsets = parse(KafkaOffsetRangeSource.offsetJson(ranges, _.startOffset)).toOption.get

    assertEquals(assignments.hcursor.get[Vector[Int]](topic), Right(Vector(1, 3)))
    assertEquals(assignments.hcursor.get[Vector[Int]]("a-topic"), Right(Vector(2)))
    assertEquals(assignments.asObject.toList.flatMap(_.keys), List("a-topic", topic))
    assertEquals(offsets.hcursor.downField(topic).get[Long]("1"), Right(4L))
    assertEquals(offsets.hcursor.downField(topic).get[Long]("3"), Right(12L))
  }

  test("invalid connector settings do not reveal supplied credentials") {
    val username = "private-user"
    val password = "private-password"
    val connection = KafkaConnection(
      "broker.example:9093",
      Some(username),
      Some(password),
      securityProtocol = KafkaSecurityProtocol.SaslPlaintext,
      allowPlaintext = false
    )
    val problem = KafkaConnection.validate(connection).toEither.left.toOption.get.toNonEmptyList.toList.mkString(" ")

    assertEquals(
      KafkaClientProperties.clientProperties(connection),
      Left(AnalyticsError.InvalidConfiguration("Kafka connection settings are invalid"))
    )
    assertEquals(
      KafkaClientProperties.sparkOptions(connection),
      Left(AnalyticsError.InvalidConfiguration("Kafka connection settings are invalid"))
    )

    assert(!problem.contains(username))
    assert(!problem.contains(password))
    assert(!connection.toString.contains(username))
    assert(!connection.toString.contains(password))
  }
}
