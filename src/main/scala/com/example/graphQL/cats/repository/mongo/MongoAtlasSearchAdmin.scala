package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import fs2.interop.reactivestreams.*
import mongo4cats.database.MongoDatabase
import com.mongodb.client.model.SearchIndexModel
import org.bson.Document

import scala.jdk.CollectionConverters.*

/** Narrow raw-driver seam for Atlas Search administration unsupported by mongo4cats. */
private[mongo] object MongoAtlasSearchAdmin {
  private val PublisherBufferSize = 32

  def createIndex(database: MongoDatabase[IO], collectionName: String, model: SearchIndexModel): IO[Unit] =
    IO.delay(
      database.underlying
        .getCollection(collectionName, classOf[Document])
        .createSearchIndexes(List(model).asJava)
    ).flatMap(_.toStreamBuffered[IO](bufferSize = 1).compile.drain)

  def listIndexes(database: MongoDatabase[IO], collectionName: String, maximum: Int): IO[List[Document]] =
    IO.delay(
      database.underlying
        .getCollection(collectionName, classOf[Document])
        .aggregate(List(new Document("$listSearchIndexes", new Document())).asJava)
    ).flatMap(_.toStreamBuffered[IO](bufferSize = PublisherBufferSize).take(maximum.toLong + 1L).compile.toList)
      .flatMap { indexes =>
        if (indexes.size > maximum)
          IO.raiseError(new IllegalStateException(s"Atlas Search index list exceeded its maximum of $maximum"))
        else IO.pure(indexes)
      }
}
