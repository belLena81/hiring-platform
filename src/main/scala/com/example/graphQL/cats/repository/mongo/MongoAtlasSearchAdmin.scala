package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import fs2.interop.reactivestreams.*
import mongo4cats.database.MongoDatabase
import com.mongodb.ReadConcern
import com.mongodb.client.model.SearchIndexModel
import com.mongodb.reactivestreams.client.{MongoCollection as JMongoCollection, MongoDatabase as JMongoDatabase}
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

  /** `$listSearchIndexes` is rejected under any read concern but `local` (code 72, InvalidOptions: "cannot run with a
    * readConcern other than 'local'"), and the application client defaults to `majority` (DDR-05). The listing reads
    * only index metadata from the search process, never business data, so `local` is pinned on this one handle.
    */
  private[mongo] def searchMetadataCollection(
      database: JMongoDatabase,
      collectionName: String
  ): JMongoCollection[Document] =
    database.getCollection(collectionName, classOf[Document]).withReadConcern(ReadConcern.LOCAL)

  def listIndexes(database: MongoDatabase[IO], collectionName: String, maximum: Int): IO[List[Document]] =
    IO.delay(
      searchMetadataCollection(database.underlying, collectionName)
        .aggregate(List(new Document("$listSearchIndexes", new Document())).asJava)
    ).flatMap(publisher =>
      MongoBoundedResults.collect(
        publisher.toStreamBuffered[IO](bufferSize = PublisherBufferSize),
        maximum,
        MongoSetupError(s"Atlas Search index list exceeded its maximum of $maximum")
      )
    )
}
