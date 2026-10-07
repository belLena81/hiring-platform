package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Resource}
import com.mongodb.MongoClientSettings
import com.mongodb.event.{CommandFailedEvent, CommandListener, CommandStartedEvent, CommandSucceededEvent}
import fs2.interop.reactivestreams.*
import io.circe.Json
import mongo4cats.client.MongoClient
import mongo4cats.database.MongoDatabase
import org.bson.{BsonDocument, Document}
import com.example.hiring.testing.LocalTestServices
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters.*

/** Test-only ownership and synchronous driver-listener interop for disposable access evidence. */
private[mongo] object MongoAccessEvaluationSupport {
  final class Commands(database: String) extends CommandListener {
    private val observed = new ConcurrentLinkedQueue[BsonDocument]()
    override def commandStarted(event: CommandStartedEvent): Unit =
      if (
        (event.getDatabaseName == database && Set(
          "find",
          "aggregate",
          "findAndModify",
          "update",
          "delete",
          "insert",
          "getMore"
        ).contains(event.getCommandName)) ||
        Set("commitTransaction", "abortTransaction").contains(event.getCommandName)
      ) {
        val _ = observed.add(event.getCommand.clone())
      }
    override def commandSucceeded(event: CommandSucceededEvent): Unit = ()
    override def commandFailed(event: CommandFailedEvent): Unit = ()
    def clear: IO[Unit] = IO.delay(observed.clear())
    def snapshot: IO[List[BsonDocument]] = IO.delay(observed.iterator().asScala.toList)
  }

  final case class Fixture(
      client: MongoClient[IO],
      database: MongoDatabase[IO],
      commands: Commands,
      uri: String,
      sampleResources: IO[Json]
  )

  def endpoint(dedicated: Boolean = false): Resource[IO, LocalTestServices.MongoEndpoint] =
    LocalTestServices.mongoEndpoint(dedicated, testCommands = dedicated)

  // Retained for standalone runners; integration suites use their suite-owned endpoint.
  def resource: Resource[IO, Fixture] = endpoint().flatMap(resource)

  def resource(endpoint: LocalTestServices.MongoEndpoint): Resource[IO, Fixture] =
    clientFixture(endpoint.uri, endpoint.sampleResources)

  /** A separately owned disposable database/client on the same container, with its own command listener. */
  def isolatedFixture(fixture: Fixture): Resource[IO, Fixture] =
    clientFixture(fixture.uri, fixture.sampleResources)

  private def clientFixture(uri: String, resources: IO[Json]): Resource[IO, Fixture] =
    for {
      name <- Resource.eval(IO.randomUUID.map(id => "hiring_test_" + id.toString.replace("-", "")))
      commands <- Resource.eval(IO.delay(new Commands(name)))
      settings = MongoClientSettings
        .builder(MongoDatabaseProbe.effectiveSettings(uri))
        .addCommandListener(commands)
        .build()
      client <- MongoClient.create[IO](settings)
      database <- LocalTestServices.databaseNamed(client, name)
    } yield Fixture(client, database, commands, uri, resources)

  def command(database: MongoDatabase[IO], command: Document): IO[Document] =
    IO.delay(database.underlying.runCommand(command, classOf[Document]))
      .flatMap(_.toStreamBuffered[IO](1).compile.lastOrError)

  def deterministicId(value: String): UUID =
    UUID.nameUUIDFromBytes(s"20261005:$value".getBytes(java.nio.charset.StandardCharsets.UTF_8))
}
