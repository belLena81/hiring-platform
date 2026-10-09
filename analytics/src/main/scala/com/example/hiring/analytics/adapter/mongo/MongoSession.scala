package com.example.hiring.analytics.adapter.mongo

import cats.effect.Resource
import mongo4cats.client.{ClientSession, MongoClient}

/** Owns an official Reactive Streams driver session with cancellation-safe effect cleanup. */
private[analytics] object MongoSession {
  def resource[F[_]](client: MongoClient[F]): Resource[F, ClientSession[F]] =
    client.startSession
}
