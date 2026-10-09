package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.mongodb.MongoCommandException
import org.bson.Document

/** `017_interview_ledger_collections`: moves the interview calendar and notification ledgers from their original
  * development-era collection names to their business names, preserving every document.
  *
  * Each pair is renamed only when the old collection exists and the new one does not. If both exist the step fails
  * closed with a typed error, because merging two ledgers cannot be decided automatically. Indexes keep their old names
  * through a rename, so old-named indexes are dropped afterwards and the index setup recreates them under the new names
  * and verifies their definitions. The step is idempotent: a concurrent or restarted run observes the finished rename
  * and only repeats the (idempotent) index cleanup.
  */
private[mongo] object MongoInterviewLedgerCollectionMigrations {
  private val Id: MigrationId = MigrationIds.InterviewLedgerCollections
  val MigrationId: String = Id.value

  /** Original names, kept here only as the source of this migration. */
  val Renames: List[(String, String)] = List(
    "fake_interview_calendar_reservations" -> MongoCollections.InterviewCalendarReservations,
    "fake_interview_calendar_participant_locks" -> MongoCollections.InterviewCalendarParticipantLocks,
    "fake_interview_notification_receipts" -> MongoCollections.InterviewNotificationReceipts
  )
  val LegacyNames: List[String] = Renames.map(_._1)
  private val LegacyIndexPrefix = "fake_"

  private def existing(database: MongoHiringSetup.SetupDatabase, names: List[String]): IO[Set[String]] =
    MongoHiringValidators.collectionOptions(database, names).map(_.keySet)

  private def renameOne(database: MongoHiringSetup.SetupDatabase, oldName: String, newName: String): IO[Unit] =
    existing(database, List(oldName, newName)).flatMap { present =>
      (present(oldName), present(newName)) match {
        case (false, _)    => IO.unit
        case (true, true)  => IO.raiseError(MigrationError.CollectionRenameConflict(oldName, newName))
        case (true, false) =>
          database.underlying.getCollection[Document](oldName, mongo4cats.codecs.CodecRegistry.Default).flatMap {
            collection =>
              collection
                .renameCollection(mongo4cats.models.collection.MongoNamespace(database.underlying.name, newName))
                .void
                // A concurrent runner may win the rename; the outcome is then verified, never assumed.
                .recoverWith {
                  case error: MongoCommandException if Set(26, 48)(error.getErrorCode) =>
                    existing(database, List(oldName, newName)).flatMap { after =>
                      IO.raiseUnless(!after(oldName) && after(newName))(error)
                    }
                }
          }
      }
    }

  private def dropLegacyIndexes(database: MongoHiringSetup.SetupDatabase, collection: String): IO[Unit] =
    existing(database, List(collection)).flatMap { present =>
      IO.whenA(present(collection)) {
        database
          .getCollection(collection)
          .listIndexes[Document]
          .flatMap(
            _.toList
              .map(_.getString("name"))
              .filter(_.startsWith(LegacyIndexPrefix))
              .traverse_ { name =>
                database
                  .runCommand(new Document("dropIndexes", collection).append("index", name))
                  .void
                  .recoverWith { case error: MongoCommandException if error.getErrorCode == 27 => IO.unit }
              }
          )
      }
    }

  private def cutover(run: MigrationRun): IO[Unit] =
    Renames.traverse_ { case (oldName, newName) =>
      renameOne(run.database, oldName, newName) *> dropLegacyIndexes(run.database, newName)
    }

  /** The proof is trusted while no legacy collection exists; a reappearing one reopens the step (and fails closed when
    * its target also exists).
    */
  private def verifyCompleted(database: MongoHiringSetup.SetupDatabase): IO[CompletedProof] =
    existing(database, LegacyNames).map(found => if (found.isEmpty) CompletedProof.Trusted else CompletedProof.Reopen)

  val step: MongoMigrationStep = MongoMigrationStep(Id, cutover, verifyCompleted)
}
