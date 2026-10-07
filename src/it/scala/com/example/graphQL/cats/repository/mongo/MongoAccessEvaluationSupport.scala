package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Resource}
import com.mongodb.MongoClientSettings
import com.mongodb.event.{CommandFailedEvent, CommandListener, CommandStartedEvent, CommandSucceededEvent}
import fs2.interop.reactivestreams.*
import io.circe.Json
import mongo4cats.client.MongoClient
import mongo4cats.database.MongoDatabase
import org.bson.{BsonArray, BsonDocument, BsonNumber, Document}
import com.example.hiring.testing.LocalTestServices
import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}
import java.util.concurrent.atomic.AtomicLong
import scala.jdk.CollectionConverters.*

/** Test-only ownership and synchronous driver-listener interop for disposable access evidence. */
private[mongo] object MongoAccessEvaluationSupport {
  final class Commands(database: String) extends CommandListener {
    private val observed = new ConcurrentLinkedQueue[BsonDocument]()
    private val requests = new ConcurrentHashMap[Integer, String]()
    private val returnedOutboxRows = new AtomicLong()
    private val outboxFindResponses = new AtomicLong()
    private val outboxGetMoreResponses = new AtomicLong()
    private val matchedOutboxClaims = new AtomicLong()
    private val emptyOutboxClaims = new AtomicLong()
    private val writeConflictErrors = new AtomicLong()
    private val duplicateKeyErrors = new AtomicLong()
    private val counters = List(
      returnedOutboxRows,
      outboxFindResponses,
      outboxGetMoreResponses,
      matchedOutboxClaims,
      emptyOutboxClaims,
      writeConflictErrors,
      duplicateKeyErrors
    )

    private def captureRequest(event: CommandStartedEvent): Unit = {
      val command = event.getCommand
      def collection(field: String): Boolean = Option(command.get(field)).exists(value =>
        value.isString && value.asString().getValue == MongoCollections.EventOutbox
      )
      val kind =
        if (event.getDatabaseName != database) None
        else if (collection("find")) Some("outboxFind")
        else if (collection("findAndModify")) Some("outboxClaim")
        else if (command.containsKey("getMore") && collection("collection")) Some("outboxGetMore")
        else None
      kind.foreach(value => { val _ = requests.put(event.getRequestId, value) })
    }

    private def countError(code: Int): Unit = {
      if (code == 112) { val _ = writeConflictErrors.incrementAndGet() }
      if (code == 11000) { val _ = duplicateKeyErrors.incrementAndGet() }
    }
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
        captureRequest(event)
      }
    override def commandSucceeded(event: CommandSucceededEvent): Unit = {
      val response = event.getResponse
      Option(requests.remove(event.getRequestId)).foreach {
        case "outboxFind" | "outboxGetMore" =>
          val cursor = Option(response.get("cursor")).collect { case value: BsonDocument => value }
          val rows = cursor.toList
            .flatMap(value =>
              List("firstBatch", "nextBatch")
                .flatMap(field => Option(value.get(field)).collect { case batch: BsonArray => batch.size().toLong })
            )
            .sum
          val _ = returnedOutboxRows.addAndGet(rows)
          if (cursor.exists(_.containsKey("nextBatch"))) { val _ = outboxGetMoreResponses.incrementAndGet() }
          else { val _ = outboxFindResponses.incrementAndGet() }
        case "outboxClaim" =>
          if (Option(response.get("value")).exists(value => !value.isNull)) {
            val _ = matchedOutboxClaims.incrementAndGet()
          } else { val _ = emptyOutboxClaims.incrementAndGet() }
        case _ => ()
      }
      Option(response.get("writeErrors")).collect { case values: BsonArray => values }.foreach { values =>
        values.getValues.asScala.foreach {
          case value: BsonDocument =>
            Option(value.get("code"))
              .collect { case code: BsonNumber => code.intValue() }
              .foreach(countError)
          case _ => ()
        }
      }
    }
    override def commandFailed(event: CommandFailedEvent): Unit = {
      val _ = requests.remove(event.getRequestId)
      event.getThrowable match {
        case error: com.mongodb.MongoException => countError(error.getCode)
        case _                                 => ()
      }
    }
    def clear: IO[Unit] = IO.delay {
      observed.clear()
      counters.foreach(_.set(0L))
    }

    /** Returned rows include every cursor batch; examined work is reported separately by native explain. */
    def responseCounts: IO[Json] = IO.delay(
      Json.obj(
        "returnedOutboxRows" -> Json.fromLong(returnedOutboxRows.get()),
        "outboxFindResponses" -> Json.fromLong(outboxFindResponses.get()),
        "outboxGetMoreResponses" -> Json.fromLong(outboxGetMoreResponses.get()),
        "matchedOutboxClaimResponses" -> Json.fromLong(matchedOutboxClaims.get()),
        "emptyOutboxClaimResponses" -> Json.fromLong(emptyOutboxClaims.get()),
        "mongoWriteConflictErrors" -> Json.fromLong(writeConflictErrors.get()),
        "mongoDuplicateKeyErrors" -> Json.fromLong(duplicateKeyErrors.get())
      )
    )
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
