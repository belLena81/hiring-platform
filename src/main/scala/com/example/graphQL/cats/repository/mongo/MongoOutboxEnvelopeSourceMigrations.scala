package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.mongodb.client.model.{Filters, Indexes, Projections, UpdateOptions, Updates}
import org.bson.Document
import org.bson.conversions.Bson
import scala.jdk.CollectionConverters.*

/** `026_outbox_envelope_source`: `envelopeBytes` is the only stored copy of an operational event's body.
  *
  * Contract-then-verify. Writers stop storing the `payload` JSON string on `event_outbox` rows and on the event
  * embedded in `search_session_work`, and `MongoHiringCodecs.readOperationalEvent` decodes `envelopeBytes` (the
  * published wire bytes) instead. The top-level outbox fields that queries and analytics read (`state`, `availableAt`,
  * `subjectIds`, `eventType`, ...) are kept. This step `$unset`s the stored `payload`.
  *
  * Each target scans its own collection in bounded `_id` batches with the ledger checkpoint `<target ordinal>:<last
  * _id>`, so a restart resumes at the target and key where it stopped; every update repeats the pending predicate, so
  * two processes (or a replay) converge. After the scan the rows are audited with `$nor` against the successor
  * validators (the first violating collection fails the step, naming the collection only), the validators are
  * installed, and the audit repeats to close the window between audit and install. A writer still storing `payload`
  * after a first full scan triggers one rescan from the start (without moving the checkpoint) before the step fails.
  *
  * The completed proof is read-only. The successor validators must be installed unchanged (drift fails closed naming
  * the collection and is never overwritten), and no `payload` may remain; a regression (an earlier binary writing
  * through a bypassed validator) reopens the step. The `payload` scans are unindexed and bounded by the collection size
  * (accepted deviation, see `docs/mongodb-design.md`).
  *
  * Migration `003` still reads outbox rows through `readOperationalEvent`; it decodes `envelopeBytes` and ignores a
  * `payload` that has not been removed yet, and it runs only while the proof of `013` is untrusted, so a database that
  * completed `013` never reruns it. The proofs of `013` and `025` accept the successor validators installed here.
  *
  * Recovery is forward-only. Stop every writer running an earlier binary before upgrading: it stores `payload`, which
  * the successor validators reject. Application rollback does not restore the removed field; the event body remains
  * recoverable from `envelopeBytes`.
  */
private[mongo] object MongoOutboxEnvelopeSourceMigrations {
  private val Id: MigrationId = MigrationIds.OutboxEnvelopeSource
  val MigrationId: String = Id.value

  private final case class Target(collection: String, path: String) {
    val pending: Bson = Filters.exists(path, true)
  }

  /** The position is the checkpoint ordinal: never reorder, and only append. */
  private val targets: Vector[Target] = Vector(
    Target(MongoCollections.EventOutbox, MongoFields.Payload),
    Target(MongoCollections.SearchSessionWork, s"${MongoFields.Event}.${MongoFields.Payload}")
  )

  /** What this step installs for one collection, and the later generations its completed proof also accepts. */
  private final case class Successor(collection: String, installed: Document, later: List[Document] = Nil) {
    def accepted: List[Document] = installed :: later
  }

  /** Installed (and exactly proven) by this step; `event_outbox` is also installed at every start. A later step
    * (`031_search_session_work_single_copy`) replaces the search-session-work definition again; once that definition is
    * in place it is the one audited, and it is never overwritten by this step.
    */
  private val validators: List[Successor] = List(
    Successor(MongoCollections.EventOutbox, MongoHiringValidators.outboxValidator),
    Successor(
      MongoCollections.SearchSessionWork,
      MongoOperationalValidators.searchSessionWorkValidatorThrough026,
      List(MongoOperationalValidators.searchSessionWorkValidator)
    )
  )

  private val CheckpointPattern = raw"(\d{2}):(.+)".r

  private def checkpointOf(ordinal: Int, id: String): String = f"$ordinal%02d:$id"

  private def page(run: MigrationRun, target: Target, after: Option[String]): IO[List[Document]] =
    MongoBoundedResults.collect(
      run.database
        .getCollection(target.collection)
        .find(after.fold(target.pending)(id => Filters.and(target.pending, Filters.gt(MongoFields.Id, id))))
        .projection(Projections.include(MongoFields.Id))
        .sort(Indexes.ascending(MongoFields.Id))
        .limit(MongoBatchScan.BatchSize)
        .boundedStream(32),
      MongoBatchScan.BatchSize,
      MigrationError.StepFailed(Id, s"setup read exceeded its maximum of ${MongoBatchScan.BatchSize} elements")
    )

  private def key(run: MigrationRun, row: Document): IO[String] =
    Option(row.get(MongoFields.Id)).collect { case value: String if value.nonEmpty => value } match {
      case Some(value) => IO.pure(value)
      case None        => run.fail[String]("found an invalid document key")
    }

  private def resume(run: MigrationRun): IO[Option[(Int, String)]] =
    run.textCheckpoint.flatMap {
      case None                                 => IO.pure(None)
      case Some(CheckpointPattern(ordinal, id)) =>
        val index = ordinal.toInt
        if (index < targets.size) IO.pure(Some(index -> id)) else run.fail("unsupported checkpoint")
      case Some(_) => run.fail("unsupported checkpoint")
    }

  private def scan(run: MigrationRun, from: Option[(Int, String)], checkpoint: Boolean): IO[Unit] =
    targets.zipWithIndex.traverse_ { case (target, ordinal) =>
      val skipped = from.exists { case (start, _) => ordinal < start }
      val after = from.collect { case (start, id) if start == ordinal => id }
      IO.unlessA(skipped)(
        MongoBatchScan.byId(MongoBatchScan.BatchSize, after)(page(run, target, _))(key(run, _)) { rows =>
          // An empty page has nothing to unset and issues no write.
          rows.traverse(key(run, _)).flatMap { ids =>
            IO.unlessA(ids.isEmpty)(
              run.database
                .getCollection(target.collection)
                .updateMany(
                  Filters.and(Filters.in(MongoFields.Id, ids.asJava), target.pending),
                  Updates.unset(target.path),
                  // The earlier search-session-work validator requires the field being removed; the audit below checks
                  // the rewritten rows against the successor validators.
                  new UpdateOptions().bypassDocumentValidation(true)
                ) *>
                IO.whenA(checkpoint)(ids.lastOption.traverse_(id => run.advance(checkpointOf(ordinal, id))))
            )
          }
        }
      )
    }

  private def pendingTarget(database: MongoHiringSetup.SetupDatabase): IO[Option[Target]] =
    targets.toList.findM(target =>
      database.getCollection(target.collection).find(target.pending).limit(1).first.map(_.nonEmpty)
    )

  /** The definition of the collection that is in force: a later generation already in place, otherwise this step's. */
  private def inForce(database: MongoHiringSetup.SetupDatabase, successor: Successor): IO[(Document, Boolean)] =
    MongoHiringValidators.collectionOptions(database, List(successor.collection)).map { options =>
      successor.later
        .find(MongoHiringValidators.strictValidatorMatches(options.get(successor.collection), _))
        .fold(successor.installed -> false)(_ -> true)
    }

  /** Fails with the first collection holding a row its successor validator rejects; the error names only the
    * collection.
    */
  private def audit(run: MigrationRun): IO[Unit] =
    validators.traverse_ { successor =>
      inForce(run.database, successor).flatMap { case (validator, _) =>
        run.database
          .getCollection(successor.collection)
          .find(new Document("$nor", List(validator).asJava))
          .limit(1)
          .first
          .flatMap(invalid =>
            IO.whenA(invalid.nonEmpty)(run.fail[Unit](s"found invalid stored data in ${successor.collection}"))
          )
      }
    }

  private def unsetPayloads(run: MigrationRun): IO[Unit] =
    resume(run).flatMap(scan(run, _, checkpoint = true)) *>
      pendingTarget(run.database).flatMap {
        case None    => IO.unit
        case Some(_) =>
          scan(run, None, checkpoint = false) *> pendingTarget(run.database).flatMap {
            case None         => IO.unit
            case Some(target) => run.fail(s"rows to repair remain in ${target.collection}")
          }
      }

  private def verifyCompleted(database: MongoHiringSetup.SetupDatabase): IO[CompletedProof] =
    validators.traverse_(successor =>
      MongoHiringValidators.assertAnyStrictValidator(database, successor.collection, successor.accepted)
    ) *>
      pendingTarget(database).map(_.fold[CompletedProof](CompletedProof.Trusted)(_ => CompletedProof.Reopen))

  private def cutover(run: MigrationRun): IO[Unit] =
    for {
      _ <- MongoMigrationLedger.requireComplete(run.database, MigrationIds.OperationalCollectionValidators)
      _ <- unsetPayloads(run)
      _ <- audit(run)
      _ <- validators.traverse_ { successor =>
        inForce(run.database, successor).flatMap { case (_, laterInPlace) =>
          IO.unlessA(laterInPlace)(
            MongoHiringValidators.install(run.database, successor.collection, successor.installed)
          )
        }
      }
      _ <- audit(run)
    } yield ()

  val step: MongoMigrationStep = MongoMigrationStep(Id, cutover, verifyCompleted)
}
