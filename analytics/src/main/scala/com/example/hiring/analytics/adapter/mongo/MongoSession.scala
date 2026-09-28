package com.example.hiring.analytics.adapter.mongo

import cats.effect.{Async, Resource}
import com.mongodb.reactivestreams.client.{ClientSession, MongoClient}

/** Owns an official Reactive Streams driver session with cancellation-safe effect cleanup. */
private[analytics] object MongoSession {
  def resource[F[_]: Async](client: MongoClient): Resource[F, ClientSession] =
    Resource.make(MongoPublisherStream.one(client.startSession()))(session => Async[F].delay(session.close()))
}
