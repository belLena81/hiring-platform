package com.example.graphQL.cats.repository.mongo

import cats.syntax.all.*
import com.example.graphQL.cats.service.port.{InterviewCleanupCursor, RepositoryError}
import com.mongodb.MongoClientSettings
import org.bson.{BsonArray, BsonDateTime, BsonDocument, BsonString, BsonValue, Document}
import org.bson.conversions.Bson
import org.bson.json.{JsonMode, JsonWriterSettings}
import java.time.Instant
import scala.jdk.CollectionConverters.*

/** Raw BSON identities remain private to Mongo; $literal protects expression-shaped stored keys. */
private[mongo] object MongoInterviewCleanupSweepCodec {
  val PageSize = 32
  val ActiveIndex = "interview_cleanup_active_identity"
  val ActiveStates: Vector[String] = Vector("Pending", "ProducersFenced", "MongoPurged", "AwaitingRetention")
  def activeFilter: BsonDocument = new BsonDocument(
    "state",
    new BsonDocument("$in", new BsonArray(ActiveStates.map(new BsonString(_)).toList.asJava))
  )
  private val CursorJson = JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build()

  final case class Sweep(afterId: Option[BsonValue], throughId: BsonValue, startedAt: Instant)

  private def literal(value: BsonValue): BsonDocument = new BsonDocument("$literal", value)
  private def expression(operator: String, operands: BsonValue*): BsonDocument =
    new BsonDocument(operator, new BsonArray(operands.toList.asJava))

  def eligible(startedAt: Instant): BsonDocument = {
    val requested = new BsonString("$requestedAt")
    val scalarDate = expression("$eq", new BsonDocument("$type", requested), new BsonString("date"))
    val beforeStart = expression("$lte", requested, literal(new BsonDateTime(startedAt.toEpochMilli)))
    activeFilter
      .append("$expr", expression("$or", expression("$not", scalarDate), beforeStart))
  }

  def pageFilter(sweep: Sweep): Bson = {
    val key = new BsonString("$_id")
    val bounds = Vector(expression("$lte", key, literal(sweep.throughId))) ++
      sweep.afterId.toVector.map(after => expression("$gt", key, literal(after)))
    expression("$and", eligible(sweep.startedAt), new BsonDocument("$expr", expression("$and", bounds*)))
  }

  def identity(row: Document): Either[RepositoryError, BsonValue] =
    Either
      .catchNonFatal(row.toBsonDocument(classOf[Document], MongoClientSettings.getDefaultCodecRegistry))
      .leftMap(_ => RepositoryError.InvalidStoredData)
      .flatMap(value => Option(value.get("_id")).toRight(RepositoryError.InvalidStoredData))

  /** Unlike query comparison type bracketing, expression ordering traverses every stored identity type. */
  def afterFilter(after: Option[BsonValue]): Bson = after.fold[Bson](new BsonDocument()) { value =>
    new BsonDocument("$expr", expression("$gt", new BsonString("$_id"), literal(value)))
  }

  def encode(sweep: Sweep, after: BsonValue): InterviewCleanupCursor =
    InterviewCleanupCursor.fromEncoded(
      new BsonDocument("afterId", after)
        .append("throughId", sweep.throughId)
        .append("startedAt", new BsonDateTime(sweep.startedAt.toEpochMilli))
        .toJson(CursorJson)
    )

  def decode(cursor: InterviewCleanupCursor): Either[RepositoryError, Sweep] =
    Either
      .catchNonFatal(BsonDocument.parse(cursor.encoded))
      .leftMap(_ => RepositoryError.InvalidStoredData)
      .flatMap { value =>
        for {
          after <- Option(value.get("afterId")).toRight(RepositoryError.InvalidStoredData)
          through <- Option(value.get("throughId")).toRight(RepositoryError.InvalidStoredData)
          started <- Option(value.get("startedAt"))
            .collect { case date: BsonDateTime =>
              Instant.ofEpochMilli(date.getValue)
            }
            .toRight(RepositoryError.InvalidStoredData)
        } yield Sweep(Some(after), through, started)
      }
}
