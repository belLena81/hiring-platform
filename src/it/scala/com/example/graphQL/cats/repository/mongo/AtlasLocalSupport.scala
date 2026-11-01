package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.mongodb.MongoClientSettings
import com.mongodb.event.{CommandListener, CommandStartedEvent}
import mongo4cats.client.MongoClient
import mongo4cats.database.MongoDatabase
import org.bson.Document
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters.*

/** Shared plumbing for the Atlas Local gates: a client that records aggregate command shapes, and a nonce database that
  * is dropped on success, failure and cancellation. Callers authorize the URI through `DisposableAtlas`.
  */
private[mongo] object AtlasLocalSupport {

  /** Records, per aggregate command, its database, target collection and the leading stage names. No values are kept,
    * so no query text, vector or document content is retained.
    */
  final class RecordedCommands extends CommandListener {
    final case class Aggregate(database: String, collection: String, stages: List[String])
    private val started = new ConcurrentLinkedQueue[Aggregate]()
    private val finds = new ConcurrentLinkedQueue[Aggregate]()
    private val RetrievalRoots = Set("$vectorSearch", "$search", "$rankFusion", "$scoreFusion")

    override def commandStarted(event: CommandStartedEvent): Unit = {
      if (event.getCommandName == "find") {
        val collection = Option(event.getCommand.get("find")).filter(_.isString).map(_.asString.getValue).getOrElse("")
        val _ = finds.add(Aggregate(event.getDatabaseName, collection, List("find")))
      }
      if (event.getCommandName == "aggregate") {
        val command = event.getCommand
        val collection =
          Option(command.get("aggregate")).filter(_.isString).map(_.asString.getValue).getOrElse("")
        val stages = Option(command.get("pipeline"))
          .filter(_.isArray)
          .toList
          .flatMap(_.asArray.getValues.asScala.toList)
          .flatMap(value => Option.when(value.isDocument && !value.asDocument.isEmpty)(value.asDocument.getFirstKey))
        val _ = started.add(Aggregate(event.getDatabaseName, collection, stages))
      }
    }

    def clear(): Unit = { started.clear(); finds.clear() }

    /** Number of `find` commands on a collection since the last `clear`. */
    def findCount(database: String, collection: String): Int =
      finds.asScala.count(value => value.database == database && value.collection == collection)

    /** Every aggregate on a collection since the last `clear`, whatever its first stage. */
    def aggregates(database: String, collection: String): List[Aggregate] =
      started.asScala.toList.filter(value => value.database == database && value.collection == collection)

    /** Aggregates whose first stage is a retrieval root: the number of search round trips, whatever their branch. */
    def retrievals(database: String, collection: String): List[Aggregate] =
      started.asScala.toList.filter(value =>
        value.database == database && value.collection == collection && value.stages.headOption.exists(RetrievalRoots)
      )
  }

  def client(uri: String, listener: Option[CommandListener]): Resource[IO, MongoClient[IO]] = {
    val base = MongoDatabaseProbe.effectiveSettings(uri)
    val settings = listener
      .fold(MongoClientSettings.builder(base))(value => MongoClientSettings.builder(base).addCommandListener(value))
      .build()
    MongoClient.create[IO](settings)
  }

  def nonceName(prefix: String): String = s"${prefix}${UUID.randomUUID().toString.replace("-", "")}"

  def nonceDatabase(client: MongoClient[IO], name: String): Resource[IO, MongoDatabase[IO]] =
    Resource.make(client.getDatabase(name))(db =>
      MongoAccessEvaluationSupport.command(db, new Document("dropDatabase", 1)).void
    )

  /** Names and current definitions of the search indexes of both owned collections, for before/after comparison. */
  def searchIndexDefinitions(db: MongoDatabase[IO]): IO[List[(String, String)]] = {
    List(MongoCollections.Jobs, MongoCollections.Users)
      .traverse(MongoAtlasSearchAdmin.listIndexes(db, _, 100))
      .map(
        _.flatten
          .map { index =>
            val definition = Option(index.get("latestDefinition", classOf[Document]))
              .orElse(Option(index.get("definition", classOf[Document])))
            index.getString("name") -> definition.fold("unavailable")(_.toJson)
          }
          .sortBy(_._1)
      )
  }

  /** The fields account deletion removes (`MongoUserRepository` tombstone), so a fixture is exactly what production
    * leaves behind and the strict users validator accepts it.
    */
  private val erasedOnDeletion =
    List("passwordHash", "profile", "embedding", "embeddingMeta", "recruiterProfile", "email", "emailCanonical")

  /** A stored candidate or recruiter document rewritten as the Deleted tombstone production writes. */
  def tombstoned(document: Document, at: java.time.Instant): Document = {
    erasedOnDeletion.foreach(field => document.remove(field))
    document.put("accountStatus", "Deleted")
    document.put("deletedAt", java.util.Date.from(at))
    document
  }

  /** The same tombstone as an update of a live document, with the revision advance production applies. */
  def tombstoneUpdate(at: java.time.Instant): org.bson.conversions.Bson = {
    import com.mongodb.client.model.Updates
    Updates.combine(
      (List(
        Updates.set("accountStatus", "Deleted"),
        Updates.set("deletedAt", java.util.Date.from(at)),
        Updates.inc("version", 1L)
      ) ++ erasedOnDeletion.map(Updates.unset))*
    )
  }
}
