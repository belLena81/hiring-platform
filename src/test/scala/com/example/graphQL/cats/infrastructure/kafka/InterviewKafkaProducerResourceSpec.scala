package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.{Deferred, IO, Ref}
import cats.syntax.all.*
import fs2.kafka.{KafkaByteProducer, ProducerSettings, Serializer, TransactionalProducerSettings}
import fs2.kafka.producer.MkProducer
import munit.CatsEffectSuite
import org.apache.kafka.clients.producer.MockProducer
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.concurrent.duration.*

final class InterviewKafkaProducerResourceSpec extends CatsEffectSuite {
  private final class Client(
      rejectInitialization: Boolean,
      initialization: Option[(CountDownLatch, CountDownLatch)] = None
  ) extends MockProducer[Array[Byte], Array[Byte]]() {
    val closes = new AtomicInteger(0)
    override def initTransactions(): Unit = {
      initialization.foreach { case (started, release) =>
        started.countDown()
        if (!release.await(5L, TimeUnit.SECONDS)) throw new AssertionError("blocked initialization was not released")
      }
      if (rejectInitialization) throw new IllegalStateException("test initialization failure")
      else super.initTransactions()
    }
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

  test("shared operational acquisition closes every failed generation exactly once") {
    List.fill(5)(()).traverse_ { _ =>
      IO(new Client(true)).flatMap { client =>
        GuardedTransactionalProducer.resource(settings, factory(client)).use(_ => IO.unit).attempt.map { result =>
          assert(result.isLeft)
          assertEquals(client.closes.get(), 1)
        }
      }
    }
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

  test("cancellation during blocked transactional initialization closes exactly once without entering use") {
    val started = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    (for {
      client <- IO(new Client(false, Some(started -> release)))
      acquired <- Ref.of[IO, Boolean](false)
      cancellationRequested <- Deferred[IO, Unit]
      fiber <- GuardedTransactionalProducer
        .resource(settings, factory(client))
        .use(_ => acquired.set(true) *> IO.never[Unit])
        .start
      entered <- IO.blocking(started.await(5L, TimeUnit.SECONDS))
      _ <- IO(assert(entered, "initialization must be in flight before cancellation"))
      cancellation <- (cancellationRequested.complete(()).void *> fiber.cancel).start
      _ <- cancellationRequested.get *> IO.cede
      _ <- IO.blocking(release.countDown())
      _ <- cancellation.joinWithNever
      outcome <- fiber.join
      used <- acquired.get
    } yield {
      assert(outcome.isCanceled)
      assert(!used)
      assert(client.closed())
      assertEquals(client.closes.get(), 1)
    }).guarantee(IO.blocking(release.countDown()))
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
