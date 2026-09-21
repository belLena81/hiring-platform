package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import fs2.interop.reactivestreams.*
import org.reactivestreams.Publisher

private[mongo] object PublisherBridge {
  final case class CollectionLimitExceeded(maximum: Int)
      extends RuntimeException(s"reactive collection exceeded its maximum of $maximum elements")

  private val CollectionBufferSize = 32

  def collectWithin[A](publisher: => Publisher[A], maximum: Int): IO[List[A]] =
    IO.raiseWhen(maximum < 1)(new IllegalArgumentException("maximum must be positive")) *>
      IO.delay(publisher.toStreamBuffered[IO](bufferSize = CollectionBufferSize)).flatMap { stream =>
        stream
          .take(maximum.toLong + 1L)
          .compile
          .toList
          .flatMap { values =>
            if (values.size > maximum) IO.raiseError(CollectionLimitExceeded(maximum))
            else IO.pure(values)
          }
      }

  def first[A](publisher: => Publisher[A]): IO[Option[A]] =
    IO.delay(publisher.toStreamBuffered[IO](bufferSize = 1)).flatMap(_.head.compile.last)
}
