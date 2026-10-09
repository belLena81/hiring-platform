package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.workflow.InterviewWorkflowPhase
import com.mongodb.MongoCommandException
import com.mongodb.client.model.{Filters, Indexes}
import org.bson.{BsonType, Document}
import scala.jdk.CollectionConverters.*

/** `018_interview_cancellation_reschedule`: lets the interview workflow store cancellation and reschedule intents.
  *
  * It expands the strict command validator with the new intents, makes the calendar ledger's reservation identity
  * unique per workflow and reserve key (replacing identity by workflow alone), and verifies the due-command index that
  * proposal expiry relies on. Existing documents are verified, never rewritten: absent lifecycle fields decode to
  * defaults. The step is restartable (bounded checkpointed scan, idempotent installs) and safe under concurrent starts.
  * A completed proof is trusted only while the exact validator and known index definitions still hold.
  */
private[mongo] object MongoInterviewLifecycleMigrations {
  private val Id: MigrationId = MigrationIds.InterviewCancellationReschedule
  val MigrationId: String = Id.value
  private val BatchSize = 500
  private val CoveredProofs = List(
    MigrationIds.InterviewWorkflowStorage,
    MigrationIds.InterviewLedgerCollections,
    MigrationIds.HiringWorkflowIntegrity
  )

  private val reservations = MongoCollections.InterviewCalendarReservations
  private val requiredIndexes = List(
    MongoHiringIndexSetup.interviewReservationIdentityIndex,
    MongoHiringIndexSetup.interviewCommandDueIndex
  )

  /** Every index the reservation store may carry; anything else could re-introduce workflow-only identity. */
  private val knownReservationIndexes: Set[String] = Set(
    "_id_",
    MongoIndexNames.InterviewCalendarParticipants,
    MongoIndexNames.InterviewCalendarRelease,
    MongoIndexNames.InterviewCalendarReservationIdentity,
    MongoIndexNames.completedEvidenceExpiry(reservations)
  )

  private def commandsValidator: List[(String, Document)] =
    List(MongoCollections.InterviewWorkflowCommands -> MongoWorkflowIntegrityMigrations.commandValidator)

  private def listIndexes(database: MongoHiringSetup.SetupDatabase, collection: String): IO[List[Document]] =
    database
      .getCollection(collection)
      .listIndexes[Document]
      .map(_.toList)
      .recoverWith { case error: MongoCommandException if error.getErrorCode == 26 => IO.pure(Nil) }

  /** A reservation without both identity fields would collide on the unique index as a null pair. */
  private def verifyReservationIdentity(run: MigrationRun): IO[Unit] =
    run.database
      .getCollection(reservations)
      .count(
        Filters.nor(
          Filters.and(
            Filters.`type`(MongoFields.WorkflowId, BsonType.STRING),
            Filters.`type`("reserveKey", BsonType.STRING)
          )
        ),
        new com.mongodb.client.model.CountOptions()
      )
      .flatMap(count => IO.whenA(count != 0L)(run.fail("a calendar reservation lacks its workflow or reserve key")))

  private def rejectUnknownReservationIndexes(database: MongoHiringSetup.SetupDatabase): IO[Unit] =
    listIndexes(database, reservations).flatMap(
      _.map(_.getString("name")).filterNot(knownReservationIndexes).headOption.traverse_ { name =>
        IO.raiseError[Unit](MigrationError.IndexMismatch(reservations, name, "unknown index definition"))
      }
    )

  /** Missing required indexes reopen the proof (they are recreated); a changed definition is drift and fails closed. */
  private def indexProof(database: MongoHiringSetup.SetupDatabase): IO[CompletedProof] =
    rejectUnknownReservationIndexes(database) *>
      requiredIndexes
        .traverse { spec =>
          listIndexes(database, spec.collection).flatMap { existing =>
            existing.find(_.getString("name") == spec.options.getName) match {
              case None        => IO.pure(CompletedProof.Reopen)
              case Some(index) =>
                MongoHiringIndexSetup
                  .definitionMismatch(spec, index)
                  .traverse_(reason =>
                    IO.raiseError[Unit](MigrationError.IndexMismatch(spec.collection, spec.options.getName, reason))
                  )
                  .as(CompletedProof.Trusted)
            }
          }
        }
        .map(proofs => if (proofs.contains(CompletedProof.Reopen)) CompletedProof.Reopen else CompletedProof.Trusted)

  private def verifyCompleted(database: MongoHiringSetup.SetupDatabase): IO[CompletedProof] =
    MongoHiringValidators.assertStrictValidators(database, commandsValidator) *>
      MongoMigrationLedger.requireComplete(database, CoveredProofs) *> indexProof(database)

  /** Workflows keep decoding after the cutover: known phase and a consistent set of lifecycle fields. */
  private def verifyWorkflows(run: MigrationRun): IO[Unit] = {
    val workflows = run.database.getCollection(MongoCollections.InterviewWorkflows)
    def scan(after: Option[String]): IO[Unit] =
      workflows
        .find(
          after.fold(Filters.eq("documentType", "workflow"))(id =>
            Filters.and(Filters.eq("documentType", "workflow"), Filters.gt(MongoFields.Id, id))
          )
        )
        .sort(Indexes.ascending(MongoFields.Id))
        .limit(BatchSize)
        .boundedStream(32)
        .compile
        .toList
        .flatMap { batch =>
          batch.traverse_ { row =>
            val valid = for {
              phaseName <- Option(row.get("phase")).collect { case value: String => value }
              phase <- InterviewWorkflowPhase.values.find(_.toString == phaseName)
              _ <- MongoInterviewWorkflowLifecycleCodec.decode(row, phase).toOption
            } yield ()
            IO.whenA(valid.isEmpty)(run.fail("a stored workflow is not decodable by the active contract"))
          } *> batch.lastOption.traverse_ { last =>
            Option(last.getString(MongoFields.Id)).fold(run.fail[Unit]("invalid workflow key"))(run.advance)
          } *> IO.whenA(batch.size == BatchSize)(scan(batch.lastOption.map(_.getString(MongoFields.Id))))
        }
    run.textCheckpoint.flatMap(scan)
  }

  private def verifyStoredCommands(run: MigrationRun): IO[Unit] =
    commandsValidator.traverse_ { case (name, validator) =>
      run.database.getCollection(name).find(new Document("$nor", List(validator).asJava)).limit(1).first.flatMap {
        case None    => IO.unit
        case Some(_) => run.fail(s"cutover found invalid stored data in $name")
      }
    }

  private def cutover(run: MigrationRun): IO[Unit] =
    for {
      _ <- MongoMigrationLedger.requireComplete(run.database, CoveredProofs)
      _ <- verifyReservationIdentity(run)
      _ <- rejectUnknownReservationIndexes(run.database)
      _ <- verifyWorkflows(run)
      _ <- MongoHiringIndexSetup.ensure(run.database.underlying, requiredIndexes)
      _ <- commandsValidator.traverse_ { case (name, validator) =>
        MongoHiringValidators.install(run.database, name, validator)
      }
      // collMod is not retroactive: every stored command must satisfy the extended validator before completion.
      _ <- verifyStoredCommands(run)
      proof <- verifyCompleted(run.database)
      _ <- IO.raiseUnless(proof == CompletedProof.Trusted)(
        MigrationError.StepFailed(run.id, "proof was not established")
      )
    } yield ()

  val step: MongoMigrationStep = MongoMigrationStep(Id, cutover, verifyCompleted)
}
