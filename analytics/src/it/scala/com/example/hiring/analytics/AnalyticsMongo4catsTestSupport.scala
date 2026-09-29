package com.example.hiring.analytics

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import mongo4cats.client.MongoClient
import mongo4cats.database.MongoDatabase

private[analytics] object AnalyticsMongo4catsTestSupport {
  def client(uri: String): MongoClient[IO] =
    MongoClient.fromConnectionString[IO](uri).allocated.unsafeRunSync()._1

  def client(settings: com.mongodb.MongoClientSettings): MongoClient[IO] =
    MongoClient
      .create[IO](mongo4cats.models.client.MongoClientSettings.builderFrom(settings).build())
      .allocated
      .unsafeRunSync()
      ._1

  def database(client: MongoClient[IO], name: String): MongoDatabase[IO] =
    client.getDatabase(name).unsafeRunSync()

  def close(client: MongoClient[IO]): Unit = client.underlying.close()
}
