package com.example.hiring.analytics.adapter.mongo
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

import cats.effect.Async
import fs2.Stream
import org.reactivestreams.Publisher

/** Lazy, bounded, cancellation-aware traversal for Mongo Reactive Streams publishers. */
private[analytics] object MongoCursorStream {
  def apply[F[_]: Async, A](publisher: => Publisher[A]): Stream[F, A] =
    MongoPublisherStream.stream[F, A](publisher)
}
