package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.mongodb.MongoCommandException
import com.mongodb.client.model.{Filters, Indexes}
import org.bson.Document

/** `023_consumer_receipt_identity`: the receipt `_id` is the identity, so its duplicate index is dropped.
  *
  * Every receipt is written with `_id = consumerGroup + ":" + eventId`, which the primary key already keeps unique, so
  * the second unique index `(consumerGroup, eventId)` only costs storage and write time. The step first verifies that
  * rule for every stored receipt in bounded `_id` batches with a checkpoint (a receipt that breaks it fails the step
  * closed, because dropping the index would then lose a uniqueness guarantee), then drops the index. An absent index or
  * namespace is the expected outcome on a fresh database or a restart. Duplicate delivery is still reported as seen:
  * the insert fails on `_id` with the same duplicate-key error.
  *
  * The completed proof is read-only: the index must be absent, and a reappearance reopens the step. The index is no
  * longer an ordinary specification in `MongoHiringIndexSetup`, so nothing recreates it.
  *
  * Recovery is forward-only. The earlier binary does not require the index to exist at startup, but it would recreate
  * it; stop it before upgrading. Rolling the application back does not restore the index, which is harmless.
  */
private[mongo] object MongoConsumerReceiptIdentityMigrations {
  private val Id: MigrationId = MigrationIds.ConsumerReceiptIdentity
  val MigrationId: String = Id.value

  /** The superseded index, kept here only so this step can find and remove it. */
  val LegacyIndexName = "consumer_receipts_group_event"

  private def receipts(database: MongoHiringSetup.SetupDatabase) =
    database.getCollection(MongoCollections.ConsumerReceipts)

  private def page(run: MigrationRun, after: Option[String]): IO[List[Document]] =
    MongoBoundedResults.collect(
      receipts(run.database)
        .find(after.fold(Filters.empty())(id => Filters.gt(MongoFields.Id, id)))
        .sort(Indexes.ascending(MongoFields.Id))
        .limit(MongoBatchScan.BatchSize)
        .boundedStream(32),
      MongoBatchScan.BatchSize,
      MigrationError.StepFailed(Id, s"setup read exceeded its maximum of ${MongoBatchScan.BatchSize} elements")
    )

  private def verifyIdentity(run: MigrationRun, row: Document): IO[Unit] = {
    val identity = (
      Option(row.getString(MongoFields.Id)),
      Option(row.getString(MongoFields.ConsumerGroup)),
      Option(row.getString(MongoFields.EventId))
    )
    identity match {
      case (Some(id), Some(group), Some(event)) if id == s"$group:$event" => IO.unit
      case _ => run.fail("found a receipt whose identity is not its consumer group and event")
    }
  }

  private def recoverMissing[A](default: A)(effect: IO[A]): IO[A] =
    effect.recoverWith {
      case error: MongoCommandException if Set(26, 27).contains(error.getErrorCode) =>
        IO.pure(default)
    }

  private def indexPresent(database: MongoHiringSetup.SetupDatabase): IO[Boolean] =
    recoverMissing(false)(
      receipts(database).listIndexes[Document].map(_.exists(_.getString("name") == LegacyIndexName))
    )

  private def cutover(run: MigrationRun): IO[Unit] =
    run.scanByStringId(MongoBatchScan.BatchSize, "invalid receipt key")(page(run, _))(
      _.traverse_(verifyIdentity(run, _))
    ) *>
      recoverMissing(())(
        run.database
          .runCommand(new Document("dropIndexes", MongoCollections.ConsumerReceipts).append("index", LegacyIndexName))
          .void
      ) *>
      indexPresent(run.database).flatMap(present =>
        IO.raiseWhen(present)(MigrationError.StepFailed(Id, "index remains"))
      )

  private def verifyCompleted(database: MongoHiringSetup.SetupDatabase): IO[CompletedProof] =
    indexPresent(database).map(present => if (present) CompletedProof.Reopen else CompletedProof.Trusted)

  val step: MongoMigrationStep = MongoMigrationStep(Id, cutover, verifyCompleted)
}
