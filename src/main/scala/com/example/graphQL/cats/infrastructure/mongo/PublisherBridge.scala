package com.example.graphQL.cats.infrastructure.mongo

import cats.effect.{IO, Resource}
import org.reactivestreams.{Publisher, Subscriber, Subscription}

private[mongo] object PublisherBridge {
  def all[A](publisher: => Publisher[A]): IO[List[A]] =
    subscribe(publisher, new CollectingSubscriber[A])

  def first[A](publisher: => Publisher[A]): IO[Option[A]] =
    subscribe(publisher, new FirstSubscriber[A])

  private def subscribe[A, B](publisher: => Publisher[A], subscriber: => BridgeSubscriber[A, B]): IO[B] =
    Resource.make(IO.delay(subscriber))(active => IO.delay(active.cancel())).use { active =>
      IO.interruptibleMany(publisher.subscribe(active)).attempt.flatMap {
        case Left(error) => IO.delay(active.onError(error))
        case Right(_) => IO.unit
      }.background.use(_ => active.result.guarantee(IO.delay(active.cancel())))
    }

  private trait BridgeSubscriber[A, B] extends Subscriber[A] {
    def result: IO[B]
    def cancel(): Unit
  }

  private abstract class BaseSubscriber[A, B](requestSize: Long) extends BridgeSubscriber[A, B] {
    private var subscription: Option[Subscription] = None
    private var finished = false
    private var outcome: Option[Either[Throwable, B]] = None
    private var callback: Option[Either[Throwable, B] => Unit] = None

    final val result: IO[B] = IO.async { complete =>
      IO.delay {
        synchronized {
          callback = Some(complete)
          outcome.foreach(complete)
        }
        Some(IO.delay(cancel()))
      }
    }

    final def cancel(): Unit = synchronized {
      finished = true
      subscription.foreach(_.cancel())
      subscription = None
    }

    final override def onSubscribe(incoming: Subscription): Unit = synchronized {
      if (finished || subscription.nonEmpty) incoming.cancel()
      else {
        subscription = Some(incoming)
        incoming.request(requestSize)
      }
    }

    protected final def finish(value: => Either[Throwable, B]): Unit = synchronized {
      if (!finished) {
        cancel()
        outcome = Some(value)
        callback.foreach(_(value))
      }
    }

    protected final def whenActive(action: => Unit): Unit = synchronized {
      if (!finished) action
    }

    final override def onError(error: Throwable): Unit = finish(Left(error))
  }

  private final class FirstSubscriber[A] extends BaseSubscriber[A, Option[A]](1L) {
    override def onNext(value: A): Unit = finish(Right(Some(value)))
    override def onComplete(): Unit = finish(Right(None))
  }

  private final class CollectingSubscriber[A] extends BaseSubscriber[A, List[A]](Long.MaxValue) {
    private var values = Vector.empty[A]

    override def onNext(value: A): Unit =
      whenActive {
        values = values :+ value
      }

    override def onComplete(): Unit = finish(Right(values.toList))
  }
}
