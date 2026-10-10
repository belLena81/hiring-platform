package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import fs2.Stream

private[mongo] object MongoBoundedResults {

  /** Reads at most `maximum` values, failing with `exceeded` when the source holds more. */
  def collect[A](stream: Stream[IO, A], maximum: Int, exceeded: => Throwable): IO[List[A]] =
    stream.take(maximum.toLong + 1L).compile.toList.flatMap { values =>
      if (values.size > maximum) IO.raiseError(exceeded) else IO.pure(values)
    }
}
