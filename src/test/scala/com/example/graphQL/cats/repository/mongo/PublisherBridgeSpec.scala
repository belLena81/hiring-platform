package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import munit.CatsEffectSuite
import org.reactivestreams.{Publisher, Subscriber, Subscription}

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.duration.*

class PublisherBridgeSpec extends CatsEffectSuite {
  private final class Controlled extends Publisher[Int] {
    val subscriber = new AtomicReference[Subscriber[? >: Int]]()
    val registered = new CountDownLatch(1)
    val cancellations = new AtomicInteger()
    val requests = new AtomicInteger()
    val subscription: Subscription = new Subscription {
      override def request(count: Long): Unit = { val _ = requests.addAndGet(count.toInt) }
      override def cancel(): Unit = { val _ = cancellations.incrementAndGet() }
    }
    override def subscribe(incoming: Subscriber[? >: Int]): Unit = {
      subscriber.set(incoming)
      registered.countDown()
    }
    def awaitRegistration: IO[Unit] = IO.blocking(assert(registered.await(3, TimeUnit.SECONDS)))
    def attach: IO[Unit] = IO.delay(subscriber.get().onSubscribe(subscription))
  }

  test("cancellation cancels an attached subscription exactly once") {
    val publisher = new Controlled
    for {
      fiber <- PublisherBridge.first(publisher).start
      _ <- publisher.awaitRegistration
      _ <- publisher.attach
      _ <- fiber.cancel
      _ <- IO(assertEquals(publisher.cancellations.get(), 1))
    } yield ()
  }

  test("registration arriving after cancellation is cancelled without demand") {
    val publisher = new Controlled
    for {
      fiber <- PublisherBridge.first(publisher).start
      _ <- publisher.awaitRegistration
      _ <- fiber.cancel
      _ <- publisher.attach
      _ <- IO {
        assertEquals(publisher.cancellations.get(), 1)
        assertEquals(publisher.requests.get(), 0)
      }
    } yield ()
  }

  test("first value cancels upstream and ignores late terminal signals") {
    val publisher = new Controlled
    for {
      fiber <- PublisherBridge.first(publisher).start
      _ <- publisher.awaitRegistration
      _ <- publisher.attach
      _ <- IO {
        publisher.subscriber.get().onNext(42)
        publisher.subscriber.get().onComplete()
      }
      result <- fiber.joinWithNever
      _ <- IO {
        assertEquals(result, Some(42))
        assertEquals(publisher.cancellations.get(), 1)
        assertEquals(publisher.requests.get(), 1)
      }
    } yield ()
  }

  test("upstream errors and synchronous registration failures remain errors") {
    val failure = new IllegalStateException("synthetic")
    val publisher = new Controlled
    for {
      fiber <- PublisherBridge.first(publisher).attempt.start
      _ <- publisher.awaitRegistration
      _ <- publisher.attach
      _ <- IO(publisher.subscriber.get().onError(failure))
      result <- fiber.joinWithNever
      thrown <- PublisherBridge.first[Int](throw failure).attempt
      _ <- IO {
        assertEquals(result, Left(failure))
        assertEquals(thrown, Left(failure))
        assertEquals(publisher.cancellations.get(), 1)
      }
    } yield ()
  }

  test("cancellation interrupts blocked registration and cancels its late subscription") {
    val entered = new CountDownLatch(1)
    val cancelled = new AtomicInteger()
    val publisher: Publisher[Int] = subscriber => {
      entered.countDown()
      try new CountDownLatch(1).await()
      finally subscriber.onSubscribe(new Subscription {
        override def request(count: Long): Unit = ()
        override def cancel(): Unit = { val _ = cancelled.incrementAndGet() }
      })
    }
    for {
      fiber <- PublisherBridge.first(publisher).start
      _ <- IO.blocking(assert(entered.await(3, TimeUnit.SECONDS)))
      _ <- fiber.cancel.timeout(2.seconds)
      _ <- IO(assertEquals(cancelled.get(), 1))
    } yield ()
  }

  test("empty completion returns None") {
    PublisherBridge.first[Int](subscriber => subscriber.onComplete()).map(result => assertEquals(result, None))
  }

  test("bounded collection completes only after probing past its declared limit") {
    val publisher = new Controlled
    for {
      fiber <- PublisherBridge.collectWithin(publisher, maximum = 2).start
      _ <- publisher.awaitRegistration
      _ <- publisher.attach
      _ <- IO {
        publisher.subscriber.get().onNext(1)
        publisher.subscriber.get().onNext(2)
        publisher.subscriber.get().onComplete()
      }
      result <- fiber.joinWithNever
      _ <- IO {
        assertEquals(result, List(1, 2))
        assertEquals(publisher.requests.get(), 3)
        assertEquals(publisher.cancellations.get(), 1)
      }
    } yield ()
  }

  test("bounded collection fails and cancels instead of returning a partial result") {
    val publisher = new Controlled
    for {
      fiber <- PublisherBridge.collectWithin(publisher, maximum = 2).attempt.start
      _ <- publisher.awaitRegistration
      _ <- publisher.attach
      _ <- IO {
        publisher.subscriber.get().onNext(1)
        publisher.subscriber.get().onNext(2)
        publisher.subscriber.get().onNext(3)
      }
      result <- fiber.joinWithNever
      _ <- IO {
        assertEquals(result, Left(PublisherBridge.CollectionLimitExceeded(2)))
        assertEquals(publisher.requests.get(), 3)
        assertEquals(publisher.cancellations.get(), 1)
      }
    } yield ()
  }

  test("bounded collection rejects a non-positive cap before subscribing") {
    val subscribed = new AtomicInteger()
    val publisher: Publisher[Int] = _ => { val _ = subscribed.incrementAndGet() }
    PublisherBridge.collectWithin(publisher, maximum = 0).attempt.map { result =>
      assert(result.left.exists(_.isInstanceOf[IllegalArgumentException]))
      assertEquals(subscribed.get(), 0)
    }
  }

  test("success joins and interrupts registration even when subscribe has not returned") {
    val exited = new CountDownLatch(1)
    val publisher: Publisher[Int] = subscriber => {
      subscriber.onNext(7)
      try new CountDownLatch(1).await()
      finally exited.countDown()
    }
    PublisherBridge.first(publisher).timeout(2.seconds).map { result =>
      assertEquals(result, Some(7))
      assertEquals(exited.getCount, 0L)
    }
  }
}
