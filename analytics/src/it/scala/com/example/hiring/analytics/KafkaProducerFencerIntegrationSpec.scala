package com.example.hiring.analytics
import com.example.hiring.analytics.service.keyretirement.*
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.errors.*
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.config.*
import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.adapter.mongo.*
import com.example.hiring.analytics.adapter.kafka.*
import com.example.hiring.analytics.adapter.local.*
import com.example.hiring.analytics.service.erasure.*

import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.adapter.mongo.*

import cats.effect.unsafe.implicits.global
import cats.effect.IO
import munit.FunSuite
import org.apache.kafka.clients.admin.{Admin, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.errors.{InvalidProducerEpochException, ProducerFencedException}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.SparkSession
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName

import java.time.Duration
import java.nio.file.Files
import java.util.Properties
import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

class KafkaProducerFencerIntegrationSpec extends FunSuite {
  override val munitTimeout = scala.concurrent.duration.FiniteDuration(3, scala.concurrent.duration.MINUTES)

  private val image = DockerImageName
    .parse(
      "apache/kafka:3.9.2"
    )
    .asCompatibleSubstituteFor("apache/kafka")

  private val brokerUsername = "broker"
  private val brokerPassword = "broker-test-secret"
  private val publisherUsername = "hiring_publisher_v2"
  private val publisherPassword = "publisher-test-secret"
  private val readerUsername = "analytics_reader"
  private val readerPassword = "reader-test-secret"
  private val fencerUsername = "analytics_fencer"
  private val fencerPassword = "fencer-test-secret"

  private val brokerJaasConfig =
    s"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$brokerUsername\" " +
      s"password=\"$brokerPassword\" user_$brokerUsername=\"$brokerPassword\" " +
      s"user_$publisherUsername=\"$publisherPassword\" user_$readerUsername=\"$readerPassword\" " +
      s"user_$fencerUsername=\"$fencerPassword\";"

  private def kafkaContainer: KafkaContainer =
    new KafkaContainer(image)
      .withEnv(
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
        "BROKER:SASL_PLAINTEXT,PLAINTEXT:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT"
      )
      .withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
      .withEnv("KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL", "PLAIN")
      .withEnv("KAFKA_SASL_JAAS_CONFIG", brokerJaasConfig)
      .withEnv("KAFKA_LISTENER_NAME_BROKER_PLAIN_SASL_JAAS_CONFIG", brokerJaasConfig)
      .withEnv("KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG", brokerJaasConfig)

  private def clientProperties(connection: KafkaConnection): Properties =
    KafkaClientProperties
      .clientProperties(connection)
      .fold(
        error => throw new AssertionError(s"Invalid Kafka fixture connection: $error"),
        properties => {
          val result = new Properties()
          properties.foreach { case (key, value) => result.setProperty(key, value) }
          result
        }
      )

  private def producerSettings(bootstrapServers: String, transactionalId: String): Properties = {
    val properties = clientProperties(localKafkaConnection(bootstrapServers, publisherUsername, publisherPassword))
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId)
    properties.put(ProducerConfig.ACKS_CONFIG, "all")
    properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "15000")
    properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "20000")
    properties
  }

  private def fenced(error: Throwable): Boolean =
    Iterator.iterate(error)(_.getCause).takeWhile(_ != null).exists {
      case _: ProducerFencedException       => true
      case _: InvalidProducerEpochException => true
      case _                                => false
    }

  private def applicationCreated(eventId: String): String =
    s"""{"eventId":"$eventId","eventType":"APPLICATION_CREATED","occurredAt":"2026-09-24T12:00:00Z","aggregateType":"Application","aggregateId":"application-$eventId","actorId":"candidate-$eventId","payload":{"applicationId":"application-$eventId","candidateId":"candidate-$eventId","jobId":"job-$eventId","newStatus":"Accepted"}}"""

  private def localKafkaConnection(bootstrapServers: String, username: String, password: String): KafkaConnection =
    KafkaConnection(
      bootstrapServers,
      saslUsername = Some(username),
      saslPassword = Some(password),
      securityProtocol = KafkaSecurityProtocol.SaslPlaintext,
      allowPlaintext = true
    )

  test("the production fencer fences an open transaction and read_committed hides its record") {
    val kafka = kafkaContainer
    kafka.start()
    val topic = "fencer-it-" + UUID.randomUUID().toString
    val transactionalId = "hiring-publisher-" + UUID.randomUUID().toString
    val adminProperties = clientProperties(
      localKafkaConnection(kafka.getBootstrapServers, fencerUsername, fencerPassword)
    )
    adminProperties.put("bootstrap.servers", kafka.getBootstrapServers)
    val admin = Admin.create(adminProperties)
    val staleProducer = new KafkaProducer[String, String](producerSettings(kafka.getBootstrapServers, transactionalId))
    var currentProducer: KafkaProducer[String, String] = null
    var spark: SparkSession = null
    try {
      admin
        .createTopics(List(new NewTopic(topic, 1, 1.toShort)).asJava)
        .all()
        .get(30, java.util.concurrent.TimeUnit.SECONDS)
      staleProducer.initTransactions()
      staleProducer.beginTransaction()
      staleProducer
        .send(new ProducerRecord(topic, "subject", applicationCreated("aborted-before-fence")))
        .get(30, java.util.concurrent.TimeUnit.SECONDS)

      val connection = localKafkaConnection(kafka.getBootstrapServers, readerUsername, readerPassword)
      val openTransactionManifest = AnalyticsRunManifest
        .validated(
          "open-transaction-it-" + UUID.randomUUID().toString,
          Vector(PartitionOffsetRange.unsafe(topic, 0, 0L, 1L))
        )
        .toEither
        .fold(errors => throw new AssertionError(errors.toString), identity)
      val openTransactionAvailability = KafkaOffsetRangeSource
        .verifyAvailable[IO](connection, openTransactionManifest, AnalyticsBatchTestSupport.driverExecution)
        .attempt
        .unsafeRunSync()
      assert(
        openTransactionAvailability.swap.toOption.exists(_.isInstanceOf[AnalyticsError.MissingOffsetRange]),
        clues(openTransactionAvailability)
      )

      KafkaProducerFencer[IO](AnalyticsBatchTestSupport.driverExecution)
        .fence(
          localKafkaConnection(kafka.getBootstrapServers, fencerUsername, fencerPassword),
          Vector(transactionalId)
        )
        .unsafeRunSync()

      val staleCommit = try {
        staleProducer.commitTransaction()
        None
      } catch {
        case NonFatal(error) => Some(error)
      }
      assert(staleCommit.exists(fenced), clues(staleCommit))
      val staleSend = try {
        staleProducer
          .send(new ProducerRecord(topic, "subject", applicationCreated("stale-after-fence")))
          .get(30, java.util.concurrent.TimeUnit.SECONDS)
        None
      } catch {
        case NonFatal(error) => Some(error)
      }
      assert(staleSend.exists(fenced), clues(staleSend))

      currentProducer = new KafkaProducer[String, String](producerSettings(kafka.getBootstrapServers, transactionalId))
      currentProducer.initTransactions()
      currentProducer.beginTransaction()
      currentProducer
        .send(new ProducerRecord(topic, "subject", applicationCreated("committed-after-fence")))
        .get(30, java.util.concurrent.TimeUnit.SECONDS)
      currentProducer.commitTransaction()

      spark = SparkSession
        .builder()
        .appName("hiring-analytics-kafka-fencing-it")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
      val barrier = KafkaRetentionAdapter
        .capture[IO](connection, topic, AnalyticsBatchTestSupport.driverExecution)
        .unsafeRunSync()
      val endOffset = barrier.partitions.head.endOffsetExclusive
      val manifest = AnalyticsRunManifest
        .validated(
          "fencer-it-" + UUID.randomUUID().toString,
          Vector(PartitionOffsetRange.unsafe(topic, 0, 0L, endOffset))
        )
        .toEither
        .fold(errors => throw new AssertionError(errors.toString), identity)
      val paths =
        AnalyticsLakehousePaths.unsafe(
          Files.createTempDirectory("analytics-read-committed").toUri.toString.stripSuffix("/")
        )
      val markers = AnalyticsSubjectPrivacy.emptyMarkers(spark.range(0L).toDF())
      val publication = AnalyticsBatchTestSupport
        .newBatch(
          paths,
          AnalyticsTestSubjectPseudonymizer.fromSecret(
            "analytics-kafka-fencing-secret".padTo(32, 'x').getBytes("UTF-8")
          ),
          DataFrameDeletionMarkerSource(markers)
        )
        .run(
          spark,
          new KafkaOffsetRangeSource(
            connection,
            com.example.hiring.analytics.adapter.spark.SparkBlockingExecution
              .forTests[IO](scala.concurrent.ExecutionContext.parasitic),
            com.example.hiring.analytics.adapter.spark.SparkBlockingExecution
              .forTests[IO](scala.concurrent.ExecutionContext.parasitic)
          ),
          manifest
        )
        .unsafeRunSync()
      assertEquals(publication.bronzeRecords, 1L)
      assertEquals(publication.validRecords, 1L)
      assertEquals(publication.quarantinedRecords, 0L)
    } finally {
      if (spark != null) spark.stop()
      if (currentProducer != null) currentProducer.close(Duration.ofSeconds(5))
      staleProducer.close(Duration.ofSeconds(5))
      admin.close(Duration.ofSeconds(5))
      kafka.stop()
    }
  }
}
