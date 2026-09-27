package com.example.hiring.analytics.mongo

import cats.effect.{IO, Resource}
import com.mongodb.reactivestreams.client.{ClientSession, MongoClient}

/** Owns an official Reactive Streams driver session with cancellation-safe effect cleanup. */
private[analytics] object MongoSession {
  def resource(client: MongoClient): Resource[IO, ClientSession] =
    Resource.make(MongoPublisherStream.one(client.startSession()))(session => IO.delay(session.close()))
}
