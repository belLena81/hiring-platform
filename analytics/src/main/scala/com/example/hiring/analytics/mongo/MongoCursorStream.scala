package com.example.hiring.analytics.mongo

import fs2.Stream
import org.reactivestreams.Publisher

/** Lazy, bounded, cancellation-aware traversal for Mongo Reactive Streams publishers. */
private[analytics] object MongoCursorStream {
  def apply[A](publisher: => Publisher[A]): Stream[cats.effect.IO, A] =
    MongoPublisherStream.stream(publisher)
}
