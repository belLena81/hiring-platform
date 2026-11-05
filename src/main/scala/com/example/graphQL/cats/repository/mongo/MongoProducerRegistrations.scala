package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.service.port.{ProducerGeneration, ProducerGenerationId, RepositoryF}
import com.example.graphQL.cats.service.RepositoryError
import mongo4cats.client.ClientSession
import mongo4cats.database.MongoDatabase
import org.bson.{BsonArray, BsonDocument, BsonInt64, BsonString, BsonValue}
import org.bson.conversions.Bson
import scala.jdk.CollectionConverters.*
import com.mongodb.client.model.{Sorts, UpdateOptions}
import java.util.UUID
import scala.concurrent.duration.*

/** Attributable generations are independent rows keyed by subject, stable transactional id and generation id; only
  * broker-confirmed fencing permits expiry, and fencing a generation never touches a later generation of the same
  * transactional id.
  */
private[mongo] object MongoProducerRegistrations {
  val Collection = "producer_registrations"
  val ClaimCursorsCollection = "outbox_claim_cursors"
  val RegisteredAt = "registeredAt"
  val BatchSize = 64
  val RetentionSeconds = 8.days.toSeconds

  def register(
      database: MongoDatabase[IO],
      session: Option[ClientSession[IO]],
      subject: String,
      generation: ProducerGeneration,
      kind: String
  ): RepositoryF[IO, Unit] =
    RepositoryF
      .lift(
        MongoSessionOperations.updateOnePipeline(
          Mongo4catsCollections.documents(database, Collection),
          session,
          MongoFilter.eq(MongoFields.Id, rowId(subject, generation)),
          activeRegistration(subject, generation, kind),
          new UpdateOptions().upsert(true)
        )
      )
      .void

  /** [[register]] for several subjects in one bulk write, in the given order. */
  def registerAll(
      database: MongoDatabase[IO],
      session: Option[ClientSession[IO]],
      subjects: List[String],
      generation: ProducerGeneration,
      kind: String
  ): RepositoryF[IO, Unit] =
    RepositoryF
      .lift(
        MongoSessionOperations.pipelineEach(
          Mongo4catsCollections.documents(database, Collection),
          session,
          subjects.map(subject =>
            PipelineUpdate(
              MongoFilter.eq(MongoFields.Id, rowId(subject, generation)),
              activeRegistration(subject, generation, kind),
              upsert = true
            )
          )
        )
      )

  /** The row identity; an existing row of either state is never rewritten, so a Fenced generation stays dead. */
  def rowId(subject: String, generation: ProducerGeneration): String =
    s"$subject:${generation.transactionalId}:${generation.generationId.value}"

  /** An upsert pipeline that writes each field only while it is absent, so an existing row of either state is never
    * rewritten (a Fenced generation stays dead). `registeredAt` is the database clock (`$$NOW`), the same clock that
    * [[MongoProducerGenerationMaintenance]] takes its retirement cut-off from, so a skewed application clock can
    * neither fence a live successor nor leave a stale row Active.
    */
  private def activeRegistration(subject: String, generation: ProducerGeneration, kind: String): Seq[Bson] = {
    def literal(value: String): BsonValue = new BsonDocument("$literal", new BsonString(value))
    def keep(field: String, value: BsonValue): (String, BsonValue) =
      field -> new BsonDocument("$ifNull", new BsonArray(List[BsonValue](new BsonString("$" + field), value).asJava))
    MongoLeaseQueue.setStage(
      keep(MongoFields.SubjectId, literal(subject)),
      keep(MongoFields.TransactionalId, literal(generation.transactionalId)),
      keep(MongoFields.GenerationId, literal(generation.generationId.value.toString)),
      keep(MongoFields.Kind, literal(kind)),
      keep(MongoFields.State, literal(ProducerRegistrationValues.Active)),
      keep(RegisteredAt, new BsonString("$$NOW"))
    )
  }

  /** The Fenced transition, stamped by the database clock like `registeredAt`: `fencedAt = $$NOW` and `expiresAt =
    * $$NOW + 8 days`, both dates. Callers filter on state Active, so a repeated mark cannot extend the expiry.
    */
  val fencedStage: Seq[Bson] = MongoLeaseQueue.setStage(
    MongoFields.State -> new BsonDocument("$literal", new BsonString(ProducerRegistrationValues.Fenced)),
    MongoFields.FencedAt -> new BsonString("$$NOW"),
    "expiresAt" -> new BsonDocument(
      "$add",
      new BsonArray(List[BsonValue](new BsonString("$$NOW"), new BsonInt64(RetentionSeconds * 1000)).asJava)
    )
  )

  def batch(database: MongoDatabase[IO], subject: String, kind: String): RepositoryF[IO, Vector[ProducerGeneration]] =
    RepositoryF
      .lift(
        Mongo4catsCollections
          .documents(database, Collection)
          .flatMap(
            _.find(
              MongoFilter
                .and(
                  MongoFilter.eq(MongoFields.SubjectId, subject),
                  MongoFilter.eq(MongoFields.Kind, kind),
                  MongoFilter.eq(MongoFields.State, ProducerRegistrationValues.Active)
                )
                .bson
            ).sort(Sorts.ascending(MongoFields.Id)).limit(BatchSize).all
          )
      )
      .subflatMap(_.toVector.traverse(generationOf))

  /** The generation a stored row describes; a row missing either part is invalid stored data. */
  def generationOf(row: org.bson.Document): Either[RepositoryError, ProducerGeneration] =
    for {
      id <- Option(row.get(MongoFields.TransactionalId))
        .collect { case id: String => id }
        .toRight(RepositoryError.InvalidStoredData)
      generation <- Option(row.get(MongoFields.GenerationId))
        .collect { case value: String => value }
        .flatMap(value => scala.util.Try(UUID.fromString(value)).toOption.filter(_.toString == value))
        .toRight(RepositoryError.InvalidStoredData)
    } yield ProducerGeneration(id, ProducerGenerationId(generation))

  /** Called only after the broker confirms the transactional ids of the listed generations are fenced. Only those
    * generations are marked: a later generation of the same transactional id stays Active. Repeated marking is safe.
    */
  def markFenced(
      database: MongoDatabase[IO],
      subject: String,
      kind: String,
      generations: Vector[ProducerGeneration]
  ): RepositoryF[IO, Unit] =
    if (generations.isEmpty || generations.size > BatchSize) MongoF.raise(RepositoryError.InvalidStoredData)
    else
      RepositoryF
        .lift(
          MongoSessionOperations.updateManyPipeline(
            Mongo4catsCollections.documents(database, Collection),
            None,
            MongoFilter.and(
              MongoFilter.eq(MongoFields.SubjectId, subject),
              MongoFilter.eq(MongoFields.Kind, kind),
              MongoFilter.in(MongoFields.Id, generations.map(rowId(subject, _)).toList),
              MongoFilter.eq(MongoFields.State, ProducerRegistrationValues.Active)
            ),
            fencedStage
          )
        )
        .void
}
