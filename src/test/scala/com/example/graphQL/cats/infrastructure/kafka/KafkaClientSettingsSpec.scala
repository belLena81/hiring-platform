package com.example.graphQL.cats.infrastructure.kafka

import com.example.graphQL.cats.config.KafkaSaslSecurityProtocol
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
      val got = KafkaClientSettings.security(Some(user), Some(secret), protocol(c.get[String]("protocol").toOption.get))
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
      assertEquals(KafkaClientSettings.security(None, None, selected), want)
      assertEquals(KafkaClientSettings.security(Some("only-user"), None, selected), want)
    }
  }

  test("producer settings carry the reliability literals and transport exactly once") {
    val properties = KafkaClientSettings
      .producer("127.0.0.1:9092", Some("u"), Some("p"), KafkaSaslSecurityProtocol.Plaintext, 65536)
      .properties
    assertEquals(properties("acks"), "all")
    assertEquals(properties("enable.idempotence"), "true")
    assertEquals(properties("max.request.size"), "65536")
    assertEquals(properties("delivery.timeout.ms"), "30000")
    assertEquals(properties("request.timeout.ms"), "10000")
    assertEquals(properties("security.protocol"), "SASL_PLAINTEXT")
    assertEquals(properties("bootstrap.servers"), "127.0.0.1:9092")
  }

  test("consumer settings without credentials are explicitly PLAINTEXT") {
    val properties =
      KafkaClientSettings.consumer("127.0.0.1:9092", None, None, KafkaSaslSecurityProtocol.Tls).properties
    assertEquals(properties("security.protocol"), "PLAINTEXT")
  }
}
