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

import cats.effect.{Clock, IO}
import cats.data.EitherT
import cats.effect.unsafe.implicits.global
import cats.effect.syntax.all.*
import com.example.hiring.analytics.adapter.mongo.MongoPublisherStream
import com.mongodb.MongoException
import mongo4cats.client.ClientSession
import mongo4cats.models.client.TransactionOptions
import munit.FunSuite
import org.reactivestreams.{Publisher, Subscriber, Subscription}

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import scala.concurrent.duration.*

final class MongoPublisherStreamSpec extends FunSuite {
  private def completedPublisher: Publisher[Void] = new Publisher[Void] {
    override def subscribe(subscriber: Subscriber[? >: Void]): Unit = {
      subscriber.onSubscribe(new Subscription {
        override def request(count: Long): Unit = subscriber.onComplete()
        override def cancel(): Unit = ()
      })
    }
  }

  private def failedPublisher(error: Throwable): Publisher[Void] = new Publisher[Void] {
    override def subscribe(subscriber: Subscriber[? >: Void]): Unit = {
      subscriber.onSubscribe(new Subscription {
        override def request(count: Long): Unit = subscriber.onError(error)
        override def cancel(): Unit = ()
      })
    }
  }

  private def cutoffClock(reads: AtomicInteger): Clock[IO] = new Clock[IO] {
    override val applicative: cats.Applicative[IO] = summon[cats.Applicative[IO]]
    override def realTime: IO[FiniteDuration] = IO.pure(0.seconds)
    override def monotonic: IO[FiniteDuration] = IO.delay {
      if (reads.incrementAndGet() == 1) 0.seconds else 121.seconds
    }
  }

  private def sessionProxy(handler: (String, Array[Object]) => Object): ClientSession[IO] =
    new ClientSession[IO] {
      override def underlying: com.mongodb.reactivestreams.client.ClientSession = null
      override def hasActiveTransaction: Boolean =
        handler("hasActiveTransaction", Array.empty[Object]).asInstanceOf[java.lang.Boolean].booleanValue()

      private def invoke(name: String): IO[Unit] = IO.defer {
        handler(name, Array.empty[Object]) match {
          case publisher: Publisher[?] =>
            fs2.interop.reactivestreams
              .fromPublisher[IO, Void](publisher.asInstanceOf[Publisher[Void]], 1)
              .compile
              .drain
          case _ => IO.unit
        }
      }

      override def startTransaction(options: TransactionOptions): IO[Unit] = invoke("startTransaction")
      override def commitTransaction: IO[Unit] = invoke("commitTransaction")
      override def abortTransaction: IO[Unit] = invoke("abortTransaction")
    }

  test("Mongo transaction deadline clock is read when the returned IO runs") {
    val reads = new AtomicInteger(0)
    val clock = new Clock[IO] {
      override val applicative: cats.Applicative[IO] = summon[cats.Applicative[IO]]
      override def realTime: IO[FiniteDuration] = IO.pure(0.seconds)
      override def monotonic: IO[FiniteDuration] = IO.delay {
        reads.incrementAndGet()
        0.seconds
      }
    }
    val session = sessionProxy { (method, _) =>
      method match {
        case "hasActiveTransaction" => java.lang.Boolean.FALSE
        case "startTransaction"     => throw new IllegalStateException("test session stop")
        case _                      => null
      }
    }
    val transaction = AnalyticsTestOperationalConfig.streams
      .transaction(session)(EitherT.liftF[IO, AnalyticsError, Int](IO.pure(1)))(clock)
      .rethrowT
    assertEquals(reads.get(), 0)
    transaction.attempt.unsafeRunSync()
    assertEquals(reads.get(), 1)
  }

  test("transient transaction retries stop after the injected deadline") {
    val reads = new AtomicInteger(0)
    val starts = new AtomicInteger(0)
    val transient = new MongoException("transient")
    transient.addLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL)
    val session = sessionProxy { (method, _) =>
      method match {
        case "startTransaction" =>
          starts.incrementAndGet()
          throw transient
        case "hasActiveTransaction" => java.lang.Boolean.FALSE
        case _                      => null
      }
    }

    val result =
      AnalyticsTestOperationalConfig.streams
        .transaction(session)(EitherT.liftF[IO, AnalyticsError, Int](IO.pure(1)))(cutoffClock(reads))
        .rethrowT
        .attempt
        .unsafeRunSync()

    assert(result.isLeft)
    assertEquals(starts.get(), 1)
    assertEquals(reads.get(), 2)
  }

  test("unknown commit retries stop after the injected deadline") {
    val reads = new AtomicInteger(0)
    val commits = new AtomicInteger(0)
    val unknownCommit = new MongoException("unknown commit")
    unknownCommit.addLabel(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)
    val session = sessionProxy { (method, _) =>
      method match {
        case "startTransaction"  => null
        case "commitTransaction" =>
          commits.incrementAndGet()
          failedPublisher(unknownCommit)
        case "hasActiveTransaction" => java.lang.Boolean.FALSE
        case _                      => null
      }
    }

    val result =
      AnalyticsTestOperationalConfig.streams
        .transaction(session)(EitherT.liftF[IO, AnalyticsError, Int](IO.pure(1)))(cutoffClock(reads))
        .rethrowT
        .attempt
        .unsafeRunSync()

    assert(result.isLeft)
    assertEquals(commits.get(), 1)
    assertEquals(reads.get(), 2)
  }

  test("typed analytics errors abort and propagate without adapter rewrapping") {
    val aborts = new AtomicInteger(0)
    val session = sessionProxy { (method, _) =>
      method match {
        case "startTransaction"     => null
        case "hasActiveTransaction" => java.lang.Boolean.TRUE
        case "abortTransaction"     =>
          aborts.incrementAndGet()
          completedPublisher
        case _ => null
      }
    }
    val expected = AnalyticsError.InvalidConfiguration("expected analytics failure")

    val result = AnalyticsTestOperationalConfig.streams
      .transaction(session)(EitherT.liftF[IO, AnalyticsError, Int](IO.raiseError(expected)))
      .rethrowT
      .attempt
      .unsafeRunSync()

    assertEquals(result.swap.toOption, Some(expected))
    assertEquals(aborts.get(), 1)
  }

  test("typed transaction failures abort without committing and remain in EitherT") {
    val aborts = new AtomicInteger(0)
    val commits = new AtomicInteger(0)
    val session = sessionProxy { (method, _) =>
      method match {
        case "startTransaction"     => null
        case "hasActiveTransaction" => java.lang.Boolean.TRUE
        case "abortTransaction"     =>
          aborts.incrementAndGet()
          completedPublisher
        case "commitTransaction" =>
          commits.incrementAndGet()
          completedPublisher
        case _ => null
      }
    }
    val expected = AnalyticsError.InvalidConfiguration("expected typed failure")

    val result = AnalyticsTestOperationalConfig.streams
      .transaction(session)(EitherT.leftT[IO, Int](expected))
      .value
      .unsafeRunSync()

    assertEquals(result, Left(expected))
    assertEquals(aborts.get(), 1)
    assertEquals(commits.get(), 0)
  }

  test("publisher creation is lazy, demand is bounded, and take cancellation reaches the subscription") {
    val publisherEvaluations = new AtomicInteger(0)
    val subscriptions = new AtomicInteger(0)
    val requested = new AtomicLong(0L)
    val cancellations = new AtomicInteger(0)

    val publisher = new Publisher[Int] {
      override def subscribe(subscriber: Subscriber[? >: Int]): Unit = {
        subscriptions.incrementAndGet()
        val cancelled = new AtomicBoolean(false)
        val demand = new AtomicLong(0L)
        val started = new AtomicBoolean(false)
        val nextValue = new AtomicInteger(0)
        subscriber.onSubscribe(new Subscription {
          override def request(count: Long): Unit = {
            requested.addAndGet(count)
            demand.addAndGet(count)
            if (started.compareAndSet(false, true)) {
              val producer = new Thread(() => {
                while (!cancelled.get()) {
                  if (demand.getAndUpdate(value => math.max(0L, value - 1L)) > 0L)
                    subscriber.onNext(nextValue.incrementAndGet())
                  else Thread.sleep(1L)
                }
              })
              producer.setDaemon(true)
              producer.start()
            }
          }

          override def cancel(): Unit = {
            cancelled.set(true)
            cancellations.incrementAndGet()
          }
        })
      }
    }

    val stream = AnalyticsTestOperationalConfig.streams.stream[IO, Int] {
      publisherEvaluations.incrementAndGet()
      publisher
    }

    assertEquals(publisherEvaluations.get(), 0)
    assertEquals(subscriptions.get(), 0)

    val values = stream.take(1).compile.toList.timeout(3.seconds).unsafeRunSync()

    assertEquals(values, List(1))
    assertEquals(publisherEvaluations.get(), 1)
    assertEquals(subscriptions.get(), 1)
    assert(requested.get() > 0L, "FS2 did not request publisher elements")
    assert(requested.get() <= 256L, s"publisher demand exceeded the configured bound: ${requested.get()}")
    assertEquals(cancellations.get(), 1)
  }
}
