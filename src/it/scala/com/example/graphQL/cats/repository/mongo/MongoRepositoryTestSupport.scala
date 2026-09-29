package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.flatMap.*
import cats.syntax.functor.*
import com.example.graphQL.cats.service.RepositoryError
import mongo4cats.client.ClientSession
import mongo4cats.collection.MongoCollection
import mongo4cats.database.MongoDatabase
import mongo4cats.codecs.CodecRegistry
import fs2.Stream
import com.mongodb.client.result.{DeleteResult, InsertOneResult}
import com.mongodb.client.model.InsertOneOptions
import org.bson.{BsonDocument, Document => JavaDocument}
import org.bson.conversions.Bson

private[graphQL] object MongoRepositoryTestSupport {
  val noTransaction: MongoTransactionRunner = new MongoTransactionRunner {
    override def run[A](
        operation: Option[ClientSession[IO]] => IO[Either[RepositoryError, A]]
    ): IO[Either[RepositoryError, A]] = operation(None)
  }

  /** Acquire a mongo4cats-owned collection while retaining the Java BSON document type used by exact-shape fixtures. */
  def collection(database: MongoDatabase[IO], name: String): IO[MongoCollection[IO, JavaDocument]] =
    database.getCollection[mongo4cats.bson.Document](name, CodecRegistry.Default).map(_.as[JavaDocument])

  def findOne(database: MongoDatabase[IO], name: String, filter: Bson): IO[Option[JavaDocument]] =
    collection(database, name).flatMap(_.find(filter).first)

  def insertOne(database: MongoDatabase[IO], name: String, document: JavaDocument): IO[Unit] =
    collection(database, name).flatMap(_.insertOne(document)).void

  def count(database: MongoDatabase[IO], name: String, filter: Bson = new BsonDocument()): IO[Long] =
    collection(database, name).flatMap(_.count(filter))

  /** Identity boundary for operations that already use mongo4cats effects. */
  def first[A](effect: IO[A]): IO[Option[A]] = effect.map(Some(_))
  def first(query: FixtureFind): IO[Option[JavaDocument]] = query.first

  def collectWithin[A](values: IO[Iterable[A]], maximum: Int): IO[List[A]] =
    values.flatMap { items =>
      val result = items.iterator.take(maximum + 1).toList
      if (result.size > maximum) IO.raiseError(new IllegalStateException(s"Mongo fixture exceeded $maximum rows"))
      else IO.pure(result)
    }

  def collectWithin[A](values: Stream[IO, A], maximum: Int): IO[List[A]] =
    values.take(maximum.toLong + 1L).compile.toList.flatMap { result =>
      if (result.size > maximum) IO.raiseError(new IllegalStateException(s"Mongo fixture exceeded $maximum rows"))
      else IO.pure(result)
    }

  def collectWithin(query: FixtureFind, maximum: Int): IO[List[JavaDocument]] =
    collectWithin(query.stream, maximum)

  final case class FixtureFind private[MongoRepositoryTestSupport] (
      collection: IO[MongoCollection[IO, JavaDocument]],
      filter: Bson,
      maximumRows: Option[Int] = None
  ) {
    def first: IO[Option[JavaDocument]] = collection.flatMap { coll =>
      val query = maximumRows match {
        case Some(size) => coll.find(filter).limit(size)
        case None       => coll.find(filter)
      }
      query.first
    }
    def limit(size: Int): FixtureFind = copy(maximumRows = Some(size))
    def stream: Stream[IO, JavaDocument] = Stream.eval(collection).flatMap { coll =>
      val query = maximumRows match {
        case Some(size) => coll.find(filter).limit(size)
        case None       => coll.find(filter)
      }
      query.stream
    }
  }

  extension (collectionIO: IO[MongoCollection[IO, mongo4cats.bson.Document]]) {
    private def rawCollection: IO[MongoCollection[IO, JavaDocument]] = collectionIO.map(_.as[JavaDocument])

    def find(filter: Bson): FixtureFind = FixtureFind(rawCollection, filter)
    def insertOne(document: JavaDocument): IO[InsertOneResult] = rawCollection.flatMap(_.insertOne(document))
    def insertOne(document: JavaDocument, options: InsertOneOptions): IO[InsertOneResult] =
      rawCollection.flatMap(_.insertOne(document, options))
    def deleteOne(filter: Bson): IO[DeleteResult] = rawCollection.flatMap(_.deleteOne(filter))
    def countDocuments(): IO[Long] = rawCollection.flatMap(_.count)
    def listIndexes(): IO[Iterable[JavaDocument]] = rawCollection.flatMap(_.listIndexes[JavaDocument])
  }
}
