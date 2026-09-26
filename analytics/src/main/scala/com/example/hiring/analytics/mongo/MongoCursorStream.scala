package com.example.hiring.analytics.mongo

import cats.effect.{IO, Resource}
import fs2.Stream
import com.mongodb.client.MongoCursor

/** Resource-owned, blocking-safe traversal for the synchronous MongoDB driver. */
private[analytics] object MongoCursorStream {
  def apply[A](cursor: => MongoCursor[A]): Stream[IO, A] =
    Stream
      .resource(Resource.make(IO.blocking(cursor))(value => IO.blocking(value.close())))
      .flatMap(value => Stream.repeatEval(IO.blocking(if (value.hasNext) Some(value.next()) else None)).unNoneTerminate)
}
