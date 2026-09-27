package com.example.hiring.analytics

import cats.effect.unsafe.implicits.global
import cats.effect.syntax.all.*
import com.example.hiring.analytics.mongo.MongoPublisherStream
import munit.FunSuite
import org.reactivestreams.{Publisher, Subscriber, Subscription}

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import scala.concurrent.duration.*

final class MongoPublisherStreamSpec extends FunSuite {
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

    val stream = MongoPublisherStream.stream {
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
