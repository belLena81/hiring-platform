package com.example.hiring.analytics

import cats.effect.{Deferred, IO, Resource}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.example.hiring.analytics.adapter.kafka.KafkaClientProperties
import com.example.hiring.analytics.adapter.mongo.MongoAnalyticsStreamingRegistry
import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.config.{AnalyticsStreamingSettings, KafkaConnection, KafkaSecurityProtocol}
import com.example.hiring.analytics.domain.{AnalyticsTopic, StreamingActivationIdentity, StreamingBatchId}
import com.example.hiring.analytics.service.streaming.StreamingActivationGate
import com.example.hiring.analytics.HiringAnalyticsRecoveryTestSupport.*
import munit.CatsEffectSuite
import org.apache.kafka.clients.admin.{Admin, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName

import java.nio.file.{Files, Path}
import java.time.Instant
import java.util.Properties
import java.util.UUID
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Actual Kafka/Spark callback and Mongo mutex ownership. Grant enforcement has separate authenticated proofs. */
final class HiringAnalyticsStreamingCancellationIntegrationSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes

  private def awaitJob(started: CountDownLatch): IO[Unit] =
    IO.blocking(started.await(45L, TimeUnit.SECONDS))
      .flatMap(ready => IO(assert(ready, "callback Spark job did not start")))

  private def kafka: Resource[IO, KafkaContainer] = {
    val jaas =
      "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"callback_test\" " +
        "password=\"synthetic-callback-password\" user_callback_test=\"synthetic-callback-password\";"
    Resource
      .make(IO.blocking {
        new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.2"))
          .withEnv(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "BROKER:SASL_PLAINTEXT,PLAINTEXT:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT"
          )
          .withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
          .withEnv("KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL", "PLAIN")
          .withEnv("KAFKA_SASL_JAAS_CONFIG", jaas)
          .withEnv("KAFKA_LISTENER_NAME_BROKER_PLAIN_SASL_JAAS_CONFIG", jaas)
          .withEnv("KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG", jaas)
      })(broker => IO.blocking(broker.stop()))
      .evalTap(broker => IO.blocking(broker.start()))
  }

  private def properties(connection: KafkaConnection): IO[Properties] =
    IO.fromEither(KafkaClientProperties.clientProperties(connection)).map { values =>
      val result = new Properties()
      values.foreach { case (key, value) => result.setProperty(key, value) }
      result.setProperty("bootstrap.servers", connection.bootstrapServers)
      result
    }

  private def initialize(connection: KafkaConnection, topic: String): IO[(String, String)] =
    properties(connection).flatMap { clientSettings =>
      Resource
        .make(IO.blocking(Admin.create(clientSettings)))(admin =>
          IO.blocking(admin.close(java.time.Duration.ofSeconds(5)))
        )
        .use { admin =>
          for {
            identity <- IO.blocking {
              admin.createTopics(List(new NewTopic(topic, 1, 1.toShort)).asJava).all().get(20L, TimeUnit.SECONDS)
              val clusterId = admin.describeCluster().clusterId().get(20L, TimeUnit.SECONDS)
              val topicId = admin
                .describeTopics(List(topic).asJava)
                .allTopicNames()
                .get(20L, TimeUnit.SECONDS)
                .get(topic)
                .topicId()
                .toString
              (clusterId, topicId)
            }
            _ <- Resource
              .make(IO.blocking {
                val producerSettings = new Properties()
                producerSettings.putAll(clientSettings)
                producerSettings
                  .setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
                producerSettings
                  .setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
                producerSettings.setProperty(ProducerConfig.ACKS_CONFIG, "all")
                new KafkaProducer[String, String](producerSettings)
              })(producer => IO.blocking(producer.close(java.time.Duration.ofSeconds(5))))
              .use(producer =>
                IO.blocking(
                  producer
                    .send(new ProducerRecord(topic, "synthetic", "callback-cancellation-fixture"))
                    .get(20L, TimeUnit.SECONDS)
                ).void
              )
          } yield identity
        }
    }

  private def settings(checkpoint: Path): IO[AnalyticsStreamingSettings] = IO.fromEither(
    AnalyticsStreamingSettings.fromHocon(s"""analytics.streaming {
      stream-id = "hiring-callback-cancellation"
      activation-grant-id = "synthetic-callback-grant"
      checkpoint-location = "${checkpoint.toUri}"
      trigger-interval = 1 second
      max-offsets-per-trigger = 1
      maximum-replay-records = 1
      maintenance-interval = 60 seconds
      progress-retention = 7 days
      initial-offsets = [{ partition = 0, offset = 0 }]
    }""")
  )

  test(
    "active Spark callback cancellation releases query, contended maintenance, driver and Mongo ownership without acknowledging input"
  ) {
    (resource, kafka).tupled.use { case (runtime, broker) =>
      for {
        harness <- runtime.harness("callback-cancellation")
        connection = KafkaConnection(
          broker.getBootstrapServers,
          Some("callback_test"),
          Some("synthetic-callback-password"),
          KafkaSecurityProtocol.SaslPlaintext,
          allowPlaintext = true
        )
        sourceIds <- initialize(connection, harness.topic)
        checkpoint = runtime.root.resolve(".local/data/analytics/checkpoints/callback-" + UUID.randomUUID().toString)
        configured <- settings(checkpoint)
        expectedIdentity <- IO.fromEither(
          configured.activationIdentity(sourceIds._1, sourceIds._2, harness.topic, harness.paths.root)
        )
        callbackBatch <- Deferred[IO, StreamingBatchId]
        callbackFinalized <- Deferred[IO, Boolean]
        maintenanceAttempted <- Deferred[IO, Unit]
        maintenanceReleased <- Deferred[IO, Unit]
        maintenanceAcquired <- Deferred[IO, Boolean]
        started = new CountDownLatch(1)
        driverExited = new AtomicBoolean(false)
        listener = new SparkListener {
          override def onJobStart(event: SparkListenerJobStart): Unit =
            if (Option(event.properties).exists(_.getProperty("hiring.callback.cancellation") == "true"))
              started.countDown()
        }
        gate = new StreamingActivationGate[IO] {
          def requireAuthorized(identity: StreamingActivationIdentity, grantId: String): IO[Instant] =
            IO {
              assertEquals(identity, expectedIdentity)
              assertEquals(grantId, "synthetic-callback-grant")
              Instant.now().plusSeconds(180L)
            }
        }
        registry = new MongoAnalyticsStreamingRegistry[IO](harness.database, harness.streams)
        stream = new SparkHiringAnalyticsStream[IO](
          runtime.spark,
          runtime.execution,
          runtime.driver,
          connection,
          AnalyticsTopic.from(harness.topic).toOption.get,
          configured,
          gate,
          registry,
          harness.lock,
          harness.paths.root,
          new DeltaStreamingCheckpointAcknowledgement[IO](harness.journal),
          (_, batchId, _, _, authorize) =>
            authorize *> callbackBatch.complete(batchId).void *> runtime
              .execution {
                val context = runtime.spark.sparkContext
                context.setLocalProperty("hiring.callback.cancellation", "true")
                try
                  context
                    .parallelize(Vector(1L), 1)
                    .mapPartitions(HiringAnalyticsCallbackCancellationTask.pause)
                    .count()
                finally {
                  context.setLocalProperty("hiring.callback.cancellation", null)
                  driverExited.set(true)
                }
              }
              .void
              .guarantee(IO(driverExited.get()).flatMap(callbackFinalized.complete).void),
          () => IO.pure(sourceIds),
          maintenance = Some((_, _, _, _) => {
            val work = awaitJob(started) *> maintenanceAttempted.complete(()).void *>
              harness.lock
                .resource(harness.paths.root)
                .use(_ => IO(driverExited.get()).flatMap(maintenanceAcquired.complete).void) *> IO.never[Unit]
            work.background.map(join => join.flatMap(_.embedNever)).onFinalize(maintenanceReleased.complete(()).void)
          })
        )
        _ <- Resource
          .make(runtime.driver(runtime.spark.sparkContext.addSparkListener(listener)))(_ =>
            runtime.driver(runtime.spark.sparkContext.removeSparkListener(listener))
          )
          .use { _ =>
            Resource.make(stream.resource.use(_ => IO.unit).start)(_.cancel).use { running =>
              for {
                batchId <- callbackBatch.get.timeout(45.seconds)
                _ <- awaitJob(started)
                _ <- maintenanceAttempted.get.timeout(5.seconds)
                before <- harness.sync(_.getCollection("analytics_lakehouse_mutexes").countDocuments())
                _ <- IO(assertEquals(before, 2L, "stream owner and active callback must own distinct durable mutexes"))
                prematurelyAcquired <- maintenanceAcquired.get.map(Option(_)).timeoutTo(200.millis, IO.pure(None))
                _ <- IO(
                  assertEquals(prematurelyAcquired, None, "maintenance entered while the callback owned its mutex")
                )
                _ <- running.cancel.timeout(10.seconds)
                finalizedAfterExit <- callbackFinalized.get.timeout(2.seconds)
                _ <- maintenanceReleased.get.timeout(2.seconds)
                acquiredAfterExit <- maintenanceAcquired.tryGet
                active <- runtime.driver(runtime.spark.streams.active.length)
                probe <- runtime.driver(runtime.spark.range(1L).count())
                _ <- harness.lock.resource(harness.paths.root).use(_ => IO.unit).timeout(5.seconds)
                remaining <- harness.sync(_.getCollection("analytics_lakehouse_mutexes").countDocuments())
                committed <- IO.blocking(Files.exists(checkpoint.resolve("commits").resolve(batchId.value.toString)))
              } yield {
                assert(finalizedAfterExit, "callback finalizer ran before the actual driver action exited")
                assert(acquiredAfterExit.forall(identity), "maintenance acquired before driver exit")
                assertEquals(active, 0)
                assertEquals(probe, 1L)
                assertEquals(remaining, 0L)
                assert(!committed, "canceled input received a Spark commit record")
              }
            }
          }
      } yield ()
    }
  }
}

/** Spark executor closure carries no test framework, latch, credentials or operational payload. */
private[analytics] object HiringAnalyticsCallbackCancellationTask {
  def pause(input: Iterator[Long]): Iterator[Long] = {
    Thread.sleep(60000L)
    input
  }
}
