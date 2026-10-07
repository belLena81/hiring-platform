package com.example.hiring.testing

import cats.effect.{IO, IOApp, Resource}
import cats.syntax.all.*
import org.apache.kafka.clients.admin.{Admin, AlterConfigOp, ConfigEntry, OffsetSpec}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.serialization.StringSerializer
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Accelerated real physical retention on exact temporary topics, without changing shared broker policy. */
object KafkaRetentionProof extends IOApp.Simple {
  def run: IO[Unit] = KafkaTestNamespace.resource.use { optional =>
    IO.fromOption(optional)(new IllegalArgumentException("An isolated test service manifest is required")).flatMap {
      namespace =>
        val settings =
          LocalTestServices.adminProperties(namespace.manifest, "broker", namespace.manifest.brokerPassword)
        val _ = settings.put("key.serializer", classOf[StringSerializer].getName)
        val _ = settings.put("value.serializer", classOf[StringSerializer].getName)
        (
          Resource.make(IO.blocking(Admin.create(settings)))(admin => IO.blocking(admin.close())),
          Resource.make(IO.blocking(new KafkaProducer[String, String](settings)))(producer =>
            IO.blocking(producer.close())
          )
        ).tupled.use { case (admin, producer) =>
          Vector(namespace.commands, namespace.results).traverse_ { topic =>
            val partition = new TopicPartition(topic, 0)
            def send(value: String): IO[Unit] = IO.blocking {
              val _ = producer
                .send(new ProducerRecord(topic, "retention-probe", value))
                .get(10L, java.util.concurrent.TimeUnit.SECONDS)
            }
            def offset(spec: OffsetSpec): IO[Long] = IO.blocking {
              admin
                .listOffsets(Map(partition -> spec).asJava)
                .partitionResult(partition)
                .get(10L, java.util.concurrent.TimeUnit.SECONDS)
                .offset()
            }
            def awaitPassed(barrier: Long, remaining: Int): IO[Long] = offset(OffsetSpec.earliest()).flatMap { first =>
              if (first >= barrier) IO.pure(first)
              else if (remaining > 0) IO.sleep(1.second) *> send("segment-roll") *> awaitPassed(barrier, remaining - 1)
              else IO.raiseError(new AssertionError("Physical Kafka retention barrier did not pass"))
            }
            for {
              _ <- IO.blocking {
                val target = new ConfigResource(ConfigResource.Type.TOPIC, topic)
                val operations =
                  Vector("retention.ms" -> "1500", "segment.ms" -> "1000", "file.delete.delay.ms" -> "1000")
                    .map { case (key, value) =>
                      new AlterConfigOp(new ConfigEntry(key, value), AlterConfigOp.OpType.SET)
                    }
                val _ = admin
                  .incrementalAlterConfigs(Map(target -> operations.asJava).asJava)
                  .all()
                  .get(10L, java.util.concurrent.TimeUnit.SECONDS)
              }
              _ <- send("before") *> IO.sleep(2.seconds) *> send("after")
              barrier <- offset(OffsetSpec.latest())
              _ <- IO.sleep(2.seconds) *> send("roll")
              beginning <- awaitPassed(barrier, 30)
              _ <- IO.println(
                s"Physical Kafka retention PASS topic=$topic partition=0 barrier=$barrier beginning=$beginning"
              )
            } yield ()
          }
        }
    }
  }
}
