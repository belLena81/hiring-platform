package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.shared.Parsing
import com.mongodb.client.model.{Filters, Indexes, Projections, UpdateOptions, Updates}
import org.bson.Document
import scala.jdk.CollectionConverters.*

/** `036_producer_generation_registrations`: a producer registration is keyed by one producer lifetime, not by its
  * transactional id.
  *
  * Writers now use a stable transactional id (`<role prefix><worker id>`) and a fresh generation id per lifetime. A row
  * is `_id = "<subject>:<transactionalId>:<generationId>"` with a `generationId` field, so a replacement generation of
  * the same transactional id registers beside a `Fenced` earlier generation instead of colliding with it for the
  * retention period, and fencing one generation never marks a later one. `Fenced` now means that generation is dead
  * forever; the transactional id lives on.
  *
  * Contract-then-verify, pre-MVP direct cutover. Rows written by `012` have the legacy transactional id
  * `<prefix><uuid>` and `_id = "<subject>:<transactionalId>"`. Each is rewritten under the new key with the UUID suffix
  * of its transactional id as `generationId`; the transactional id, kind, state and every date are preserved (a legacy
  * id therefore stays attributable and is retired by fencing like any other). Rows move in bounded `_id` batches: the
  * successor row is inserted with `$setOnInsert` only (a row that already exists is never overwritten) and the legacy
  * row is deleted afterwards, so a crash between the two repeats harmlessly and the pending predicate (`generationId`
  * absent) shrinks monotonically without a ledger checkpoint. After the scan every row is audited with `$nor` against
  * the successor validator (failing closed naming the collection only), the validator and the successor generation
  * index are installed, and the audit repeats to close the window between audit and install. The index
  * `producer_registration_generation_state` changes key from `(transactionalId, state, _id)` to `(transactionalId,
  * state, registeredAt, _id)` (retirement filters Active rows of one id by `registeredAt` and pages in that order); the
  * exact legacy definition is dropped, any other definition fails closed. `registeredAt` of new rows is stamped from
  * the database clock, the clock retirement cuts by.
  *
  * The completed proof is read-only: the successor validator is installed unchanged, the index definition holds, and no
  * row lacks a `generationId`; a regression (an earlier binary writing through a bypassed validator) reopens the step.
  *
  * Recovery is forward-only. Stop every publisher and erasure worker running an earlier binary before upgrading: it
  * writes legacy-keyed rows the successor validator rejects, and it fences by transactional id. Application rollback
  * does not restore legacy keys; the legacy transactional id remains stored on every migrated row.
  */
private[mongo] object MongoProducerGenerationRegistrationMigrations {
  private val Id: MigrationId = MigrationIds.ProducerGenerationRegistrations
  val MigrationId: String = Id.value

  private val spec = MongoHiringIndexSetup.producerRegistrationGenerationStateIndex
  private val indexName = spec.options.getName
  private val UuidLength = 36

  /** The registry validator: `012` plus the required UUID `generationId`; both states, no other change. */
  def registrationValidator: Document = {
    import MongoValidatorSchemas.*
    val schema = MongoProducerRegistrationMigrations.registrationValidatorV1
      .get("$and", classOf[java.util.List[Document]])
      .get(0)
      .get("$jsonSchema", classOf[Document])
    val properties = schema.get("properties", classOf[Document])
    val required = new java.util.ArrayList[String](schema.getList("required", classOf[String]))
    required.add(MongoFields.GenerationId)
    val successor = new Document("bsonType", "object")
      .append("required", required)
      .append(
        "properties",
        new Document(properties).append(MongoFields.GenerationId, stringUuid)
      )
    val original = MongoProducerRegistrationMigrations.registrationValidatorV1
      .get("$and", classOf[java.util.List[Document]])
      .get(1)
    new Document("$and", List(new Document("$jsonSchema", successor), original).asJava)
  }

  /** The `012`-era key; the only definition `dropLegacyIndex` replaces and `verifyCompleted` reopens on. */
  private val legacyKeys = new Document(MongoFields.TransactionalId, Int.box(1))
    .append(MongoFields.State, Int.box(1))
    .append(MongoFields.Id, Int.box(1))

  private def pending: org.bson.conversions.Bson = Filters.exists(MongoFields.GenerationId, false)

  private def page(run: MigrationRun): IO[List[Document]] =
    MongoBoundedResults.collect(
      run.database
        .getCollection(MongoProducerRegistrations.Collection)
        .find(pending)
        .sort(Indexes.ascending(MongoFields.Id))
        .limit(MongoBatchScan.BatchSize)
        .boundedStream(32),
      MongoBatchScan.BatchSize,
      MigrationError.StepFailed(Id, s"setup read exceeded its maximum of ${MongoBatchScan.BatchSize} elements")
    )

  private def text(run: MigrationRun, row: Document, field: String): IO[String] =
    Option(row.get(field)).collect { case value: String if value.nonEmpty => value } match {
      case Some(value) => IO.pure(value)
      case None        => run.fail[String]("found an invalid producer registration")
    }

  /** The UUID suffix of a legacy `<prefix><uuid>` transactional id. */
  private def legacyGeneration(id: String): Option[String] = {
    val suffix = id.takeRight(UuidLength)
    Option
      .when(id.length > UuidLength && id.charAt(id.length - UuidLength - 1) == '-')(suffix)
      .filter(value => Parsing.parseUuid(value).exists(_.toString == value))
  }

  private def rewrite(run: MigrationRun, row: Document): IO[Unit] =
    for {
      oldId <- text(run, row, MongoFields.Id)
      subject <- text(run, row, MongoFields.SubjectId)
      transactionalId <- text(run, row, MongoFields.TransactionalId)
      generation <- legacyGeneration(transactionalId).fold(run.fail[String]("found an invalid producer identity"))(
        IO.pure
      )
      _ <- IO.raiseUnless(oldId == s"$subject:$transactionalId")(
        MigrationError.StepFailed(Id, "found an invalid producer registration key")
      )
      newId = s"$subject:$transactionalId:$generation"
      fields = row.entrySet().asScala.toList.filterNot(_.getKey == MongoFields.Id)
      registrations = run.database.getCollection(MongoProducerRegistrations.Collection)
      inserted <- registrations.updateOne(
        Filters.eq(MongoFields.Id, newId),
        Updates.combine(
          (Updates.setOnInsert(MongoFields.GenerationId, generation) :: fields.map(entry =>
            Updates.setOnInsert(entry.getKey, entry.getValue)
          )).asJava
        ),
        new UpdateOptions().upsert(true)
      )
      _ <- IO.raiseUnless(inserted.wasAcknowledged())(MigrationError.UnacknowledgedWrite(Id))
      removed <- registrations.deleteOne(Filters.eq(MongoFields.Id, oldId))
      _ <- IO.raiseUnless(removed.wasAcknowledged())(MigrationError.UnacknowledgedWrite(Id))
    } yield ()

  private def rewriteAll(run: MigrationRun): IO[Unit] =
    page(run).flatMap(rows => IO.unlessA(rows.isEmpty)(rows.traverse_(rewrite(run, _)) *> rewriteAll(run)))

  private def audit(run: MigrationRun): IO[Unit] =
    run.database
      .getCollection(MongoProducerRegistrations.Collection)
      .find(new Document("$nor", List(registrationValidator).asJava))
      .limit(1)
      .first
      .flatMap(invalid =>
        IO.whenA(invalid.nonEmpty)(
          run.fail[Unit](s"found invalid stored data in ${MongoProducerRegistrations.Collection}")
        )
      )

  private def listIndexes(database: MongoHiringSetup.SetupDatabase): IO[List[Document]] =
    database
      .getCollection(MongoProducerRegistrations.Collection)
      .listIndexes[Document]
      .map(_.toList)

  /** Drops only the exact `012`-era definition so the successor index can be created under the same name. */
  private def dropLegacyIndex(run: MigrationRun): IO[Unit] = {
    listIndexes(run.database).flatMap { indexes =>
      indexes.find(_.getString("name") == indexName) match {
        case Some(index) if index.get("key", classOf[Document]) == legacyKeys =>
          run.database
            .runCommand(
              new Document("dropIndexes", MongoProducerRegistrations.Collection).append("index", indexName)
            )
            .void
        case _ => IO.unit
      }
    }
  }

  private def verifyCompleted(database: MongoHiringSetup.SetupDatabase): IO[CompletedProof] =
    MongoHiringValidators.assertStrictValidators(
      database,
      List(MongoProducerRegistrations.Collection -> registrationValidator)
    ) *>
      listIndexes(database).flatMap { existing =>
        existing.find(_.getString("name") == indexName) match {
          case None => IO.pure(CompletedProof.Reopen)
          // An earlier draft of this step (or an old binary) left the legacy key: reopen so the cutover repairs it.
          case Some(index) if index.get("key", classOf[Document]) == legacyKeys => IO.pure(CompletedProof.Reopen)
          case Some(index)                                                      =>
            MongoHiringIndexSetup
              .definitionMismatch(spec, index)
              .traverse_(reason =>
                IO.raiseError[Unit](MigrationError.IndexMismatch(spec.collection, indexName, reason))
              )
              .flatMap(_ =>
                database
                  .getCollection(MongoProducerRegistrations.Collection)
                  .find(pending)
                  .projection(Projections.include(MongoFields.Id))
                  .limit(1)
                  .first
                  .map(row => if (row.isEmpty) CompletedProof.Trusted else CompletedProof.Reopen)
              )
        }
      }

  private def cutover(run: MigrationRun): IO[Unit] =
    for {
      _ <- MongoMigrationLedger.requireComplete(run.database, List(MigrationIds.AttributableProducerRegistrations))
      _ <- rewriteAll(run)
      _ <- audit(run)
      _ <- MongoHiringValidators.install(run.database, MongoProducerRegistrations.Collection, registrationValidator)
      _ <- dropLegacyIndex(run)
      _ <- MongoHiringIndexSetup.ensure(run.database.underlying, List(spec))
      _ <- audit(run)
      proof <- verifyCompleted(run.database)
      _ <- IO.raiseUnless(proof == CompletedProof.Trusted)(
        MigrationError.StepFailed(Id, "proof was not established")
      )
    } yield ()

  val step: MongoMigrationStep = MongoMigrationStep(Id, cutover, verifyCompleted)
}
