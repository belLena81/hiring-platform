package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.{Deferred, IO}
import fs2.kafka.{KafkaByteProducer, ProducerSettings, Serializer, TransactionalProducerSettings}
import fs2.kafka.producer.MkProducer
import munit.CatsEffectSuite
import org.apache.kafka.clients.producer.MockProducer
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*

final class InterviewKafkaProducerResourceSpec extends CatsEffectSuite {
  private final class Client(rejectInitialization: Boolean) extends MockProducer[Array[Byte], Array[Byte]]() {
    val closes = new AtomicInteger(0)
    override def initTransactions(): Unit =
      if (rejectInitialization) throw new IllegalStateException("test initialization failure")
      else super.initTransactions()
    override def close(timeout: Duration): Unit = {
      val _ = closes.incrementAndGet()
      super.close(timeout)
    }
  }

  private def settings = TransactionalProducerSettings(
    "hiring-interview-orchestrator-test",
    ProducerSettings(Serializer[IO, String], Serializer[IO, Array[Byte]]).withCloseTimeout(1.second)
  )

  private def factory(client: Client): MkProducer[IO] = new MkProducer[IO] {
    def apply[G[_]](value: ProducerSettings[G, ?, ?]): IO[KafkaByteProducer] = IO.pure(client)
  }

  test("failed transactional initialization closes the allocated producer") {
    IO(new Client(true)).flatMap { client =>
      InterviewKafkaRuntime.initializedProducer(settings, factory(client)).use(_ => IO.unit).attempt.map { result =>
        assert(result.isLeft)
        assert(client.closed())
        assertEquals(client.closes.get(), 1)
      }
    }
  }

  test("successful acquisition transfers closing ownership to the transactional resource") {
    IO(new Client(false)).flatMap { client =>
      InterviewKafkaRuntime
        .initializedProducer(settings, factory(client))
        .use { _ =>
          IO(assertEquals(client.closes.get(), 0))
        }
        .map { _ =>
          assert(client.closed())
          assertEquals(client.closes.get(), 1)
        }
    }
  }

  test("cancellation releases the initialized interview producer exactly once") {
    for {
      client <- IO(new Client(false))
      acquired <- Deferred[IO, Unit]
      fiber <- InterviewKafkaRuntime
        .initializedProducer(settings, factory(client))
        .use(_ => acquired.complete(()).void *> IO.never)
        .start
      _ <- acquired.get
      _ <- fiber.cancel
    } yield {
      assert(client.closed())
      assertEquals(client.closes.get(), 1)
    }
  }
}
