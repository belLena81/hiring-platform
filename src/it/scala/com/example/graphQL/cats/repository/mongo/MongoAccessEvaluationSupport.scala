package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Resource}
import com.mongodb.MongoClientSettings
import com.mongodb.event.{CommandFailedEvent, CommandListener, CommandStartedEvent, CommandSucceededEvent}
import fs2.interop.reactivestreams.*
import mongo4cats.client.MongoClient
import mongo4cats.database.MongoDatabase
import org.bson.{BsonDocument, Document}
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Test-only ownership and synchronous driver-listener interop for disposable access evidence. */
private[mongo] object MongoAccessEvaluationSupport {
  private val image = "mongo:8.0.32-noble@sha256:01354084d2ae665d2e79b79b0cdc50c2c0c98873618912d9a2c8c9cb5c3d24e6"
  private final class ReplicaSet extends GenericContainer[ReplicaSet](DockerImageName.parse(image))

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

  final case class Fixture(client: MongoClient[IO], database: MongoDatabase[IO], commands: Commands)

  private def replicaSet: Resource[IO, ReplicaSet] =
    Resource.make(IO.blocking {
      val instance = new ReplicaSet
      val _ = instance
        .withExposedPorts(27017)
        .withCommand("mongod", "--bind_ip_all", "--replSet", "rs0", "--setParameter", "enableTestCommands=1")
        .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(90)))
      try {
        instance.start()
        val result = instance.execInContainer(
          "mongosh",
          "--quiet",
          "--eval",
          "rs.initiate({_id:'rs0',members:[{_id:0,host:'localhost:27017'}]})"
        )
        if (result.getExitCode != 0) throw new AssertionError("Replica-set initiation failed")
        instance
      } catch {
        case error: Throwable => instance.stop(); throw error
      }
    })(value => IO.blocking(value.stop()))

  private def awaitPrimary(instance: ReplicaSet, attempts: Int = 60): IO[Unit] =
    IO.blocking(instance.execInContainer("mongosh", "--quiet", "--eval", "db.hello().isWritablePrimary")).flatMap {
      result =>
        if (result.getExitCode == 0 && result.getStdout.trim == "true") IO.unit
        else if (attempts > 0) IO.sleep(250.millis) *> awaitPrimary(instance, attempts - 1)
        else IO.raiseError(new AssertionError("Disposable Mongo did not elect a primary"))
    }

  def resource: Resource[IO, Fixture] =
    for {
      instance <- replicaSet
      _ <- Resource.eval(awaitPrimary(instance))
      name <- Resource.eval(IO.randomUUID.map(id => s"mongodb_access_evaluation_${id.toString.replace("-", "")}"))
      commands <- Resource.eval(IO.delay(new Commands(name)))
      uri = s"mongodb://${instance.getHost}:${instance.getMappedPort(27017)}/?replicaSet=rs0&directConnection=true"
      settings = MongoClientSettings
        .builder(MongoDatabaseProbe.effectiveSettings(uri))
        .addCommandListener(commands)
        .build()
      client <- MongoClient.create[IO](settings)
      database <- Resource.make(client.getDatabase(name))(db => command(db, new Document("dropDatabase", 1)).void)
    } yield Fixture(client, database, commands)

  def command(database: MongoDatabase[IO], command: Document): IO[Document] =
    IO.delay(database.underlying.runCommand(command, classOf[Document]))
      .flatMap(_.toStreamBuffered[IO](1).compile.lastOrError)

  def deterministicId(value: String): UUID =
    UUID.nameUUIDFromBytes(s"20261005:$value".getBytes(java.nio.charset.StandardCharsets.UTF_8))
}
