package com.example.graphQL.cats.infrastructure.mongo

import cats.effect.{IO, Resource}
import org.reactivestreams.{Publisher, Subscriber, Subscription}

private[mongo] object PublisherBridge {
  def first[A](publisher: => Publisher[A]): IO[Option[A]] =
    Resource.make(IO.delay(new FirstSubscriber[A]))(subscriber => IO.delay(subscriber.cancel())).use { subscriber =>
      IO.interruptibleMany(publisher.subscribe(subscriber)).attempt.flatMap {
        case Left(error) => IO.delay(subscriber.onError(error))
        case Right(_) => IO.unit
      }.background.use(_ => subscriber.result.guarantee(IO.delay(subscriber.cancel())))
    }

  private final class FirstSubscriber[A] extends Subscriber[A] {
    private var subscription: Option[Subscription] = None
    private var finished = false
    private var outcome: Option[Either[Throwable, Option[A]]] = None
    private var callback: Option[Either[Throwable, Option[A]] => Unit] = None

    val result: IO[Option[A]] = IO.async { complete =>
      IO.delay {
        synchronized {
          callback = Some(complete)
          outcome.foreach(complete)
        }
        Some(IO.delay(cancel()))
      }
    }

    def cancel(): Unit = synchronized {
      finished = true
      subscription.foreach(_.cancel())
      subscription = None
    }

    override def onSubscribe(incoming: Subscription): Unit = synchronized {
      if (finished || subscription.nonEmpty) incoming.cancel()
      else {
        subscription = Some(incoming)
        incoming.request(1L)
      }
    }

    private def finish(value: Either[Throwable, Option[A]]): Unit = synchronized {
      if (!finished) {
        cancel()
        outcome = Some(value)
        callback.foreach(_(value))
      }
    }

    override def onNext(value: A): Unit = finish(Right(Some(value)))
    override def onError(error: Throwable): Unit = finish(Left(error))
    override def onComplete(): Unit = finish(Right(None))
  }
}
