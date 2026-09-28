package com.example.hiring.analytics.adapter.mongo

import cats.effect.{Async, Resource}
import mongo4cats.client.{ClientSession, MongoClient}

/** Owns an official Reactive Streams driver session with cancellation-safe effect cleanup. */
private[analytics] object MongoSession {
  def resource[F[_]: Async](client: MongoClient[F], streams: MongoPublisherStream): Resource[F, ClientSession[F]] =
    client.startSession
}
