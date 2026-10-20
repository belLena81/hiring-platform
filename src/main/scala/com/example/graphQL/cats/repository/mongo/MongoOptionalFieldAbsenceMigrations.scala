package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.mongodb.client.model.{Filters, Indexes, Projections, Updates}
import org.bson.{BsonType, Document}
import org.bson.conversions.Bson
import scala.jdk.CollectionConverters.*

/** `024_optional_field_absence`: an absent value is omitted, never stored as `null`, and every user has `updatedAt`.
  *
  * Contract-then-verify. Writers omit `leaseOwner`/`leaseToken` (outbox), `query`/`model` (search sessions and the
  * session embedded in search-session work) and `auditActorId` (inbox receipts) instead of storing `null`, stop writing
  * the unread `subjectIds` copy on interview workflows and notification receipts, and write `users.updatedAt` on insert
  * (equal to `createdAt`). This step repairs stored rows to the same shape: `$unset` of each stored `null` or unread
  * `subjectIds`, and `updatedAt = createdAt` where it is missing. Hiring receipts keep `subjectIds` (the erasure purge
  * reads them); only workflows and notification receipts are cleared.
  *
  * Every target scans its own collection in bounded `_id` batches, and the ledger checkpoint is
  * `<target ordinal>:<last _id>`, so a restart resumes at the target and key where it stopped. Each update repeats the
  * pending predicate, so two processes (or a replay) converge. A user without a `createdAt` date fails the step closed
  * with the collection name only.
  *
  * The completed proof is read-only: no target may have a pending row; a regression (an earlier binary still writing)
  * reopens the step, and one rescan from the start (without moving the checkpoint) repairs rows that sort below the
  * saved checkpoint. The proof reads one document per target on each start; the scans are unindexed and bounded by the
  * collection size (accepted deviation, see `docs/mongodb-design.md`). Migration `025` installs the validators that
  * make the same regressions impossible for users, search sessions, inbox receipts and notification receipts.
  *
  * Recovery is forward-only. Stop every writer running an earlier binary before upgrading (it stores `null` again and
  * would not write `users.updatedAt`); the reopened proof repairs what it wrote on the next start. Application rollback
  * does not restore the removed fields.
  */
private[mongo] object MongoOptionalFieldAbsenceMigrations {
  private val Id: MigrationId = MigrationIds.OptionalFieldAbsence
  val MigrationId: String = Id.value

  private val AuditActorId = "auditActorId"

  private enum Repair {
    case Unset(path: String)
    case UpdatedAtFromCreatedAt
  }

  private final case class Target(collection: String, pending: Bson, repair: Repair)

  private def storedNull(path: String): Bson = Filters.`type`(path, BsonType.NULL)

  private def unsetNull(collection: String, path: String): Target =
    Target(collection, storedNull(path), Repair.Unset(path))

  private def unsetPresent(collection: String, path: String): Target =
    Target(collection, Filters.exists(path, true), Repair.Unset(path))

  private val sessionQuery = s"${MongoFields.Session}.${MongoFields.Query}"
  private val sessionModel = s"${MongoFields.Session}.${MongoFields.Model}"

  /** The position is the checkpoint ordinal: never reorder, and only append. */
  private val targets: Vector[Target] = Vector(
    unsetNull(MongoCollections.EventOutbox, MongoFields.LeaseOwner),
    unsetNull(MongoCollections.EventOutbox, MongoFields.LeaseToken),
    unsetNull(MongoCollections.SearchSessions, MongoFields.Query),
    unsetNull(MongoCollections.SearchSessions, MongoFields.Model),
    unsetNull(MongoCollections.SearchSessionWork, sessionQuery),
    unsetNull(MongoCollections.SearchSessionWork, sessionModel),
    unsetNull(MongoCollections.InterviewWorkflowInbox, AuditActorId),
    unsetPresent(MongoCollections.InterviewWorkflows, MongoFields.SubjectIds),
    unsetPresent(MongoCollections.InterviewNotificationReceipts, MongoFields.SubjectIds),
    Target(MongoCollections.Users, Filters.exists(MongoFields.UpdatedAt, false), Repair.UpdatedAtFromCreatedAt)
  )

  private val CheckpointPattern = raw"(\d{2}):(.+)".r

  private def checkpointOf(ordinal: Int, id: String): String = f"$ordinal%02d:$id"

  private def collection(database: MongoHiringSetup.SetupDatabase, target: Target) =
    database.getCollection(target.collection)

  private def page(run: MigrationRun, target: Target, after: Option[String]): IO[List[Document]] =
    MongoBoundedResults.collect(
      collection(run.database, target)
        .find(after.fold(target.pending)(id => Filters.and(target.pending, Filters.gt(MongoFields.Id, id))))
        .projection(Projections.include(MongoFields.Id, MongoFields.CreatedAt))
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

  private def repair(run: MigrationRun, target: Target, rows: List[Document]): IO[Unit] =
    target.repair match {
      case Repair.Unset(path) =>
        rows.traverse(key(run, _)).flatMap { ids =>
          collection(run.database, target)
            .updateMany(Filters.and(Filters.in(MongoFields.Id, ids.asJava), target.pending), Updates.unset(path))
            .void
        }
      case Repair.UpdatedAtFromCreatedAt =>
        rows.traverse_ { row =>
          key(run, row).flatMap { id =>
            row.get(MongoFields.CreatedAt) match {
              case createdAt: java.util.Date =>
                collection(run.database, target)
                  .updateOne(
                    Filters.and(Filters.eq(MongoFields.Id, id), target.pending),
                    Updates.set(MongoFields.UpdatedAt, createdAt)
                  )
                  .void
              case _ => run.fail(s"found a document without createdAt in ${target.collection}")
            }
          }
        }
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
          repair(run, target, rows) *>
            IO.whenA(checkpoint)(
              rows.lastOption.traverse_(key(run, _).flatMap(id => run.advance(checkpointOf(ordinal, id))))
            )
        }
      )
    }

  private def pendingTarget(database: MongoHiringSetup.SetupDatabase): IO[Option[Target]] =
    targets.toList.findM(target => collection(database, target).find(target.pending).limit(1).first.map(_.nonEmpty))

  private def verifyCompleted(database: MongoHiringSetup.SetupDatabase): IO[CompletedProof] =
    pendingTarget(database).map(_.fold[CompletedProof](CompletedProof.Trusted)(_ => CompletedProof.Reopen))

  private def backfill(run: MigrationRun): IO[Unit] =
    resume(run).flatMap(scan(run, _, checkpoint = true)) *>
      pendingTarget(run.database).flatMap {
        case None    => IO.unit
        case Some(_) =>
          scan(run, None, checkpoint = false) *> pendingTarget(run.database).flatMap {
            case None         => IO.unit
            case Some(target) => run.fail(s"rows to repair remain in ${target.collection}")
          }
      }

  val step: MongoMigrationStep = MongoMigrationStep(Id, backfill, verifyCompleted)
}
