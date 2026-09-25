package com.example.hiring.analytics.mongo

import cats.effect.{IO, Resource}
import com.mongodb.client.{ClientSession, MongoClient}

/** Owns a blocking official-driver session with cancellation-safe effect cleanup. */
private[analytics] object MongoSession {
  def resource(client: MongoClient): Resource[IO, ClientSession] =
    Resource.make(IO.blocking(client.startSession()))(session => IO.blocking(session.close()))
}
