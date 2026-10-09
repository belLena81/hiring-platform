package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.workflow.InterviewTopicPair
import com.mongodb.client.model.Sorts
import org.bson.{BsonValue, Document}
import scala.jdk.CollectionConverters.*

/** One audited baseline plus strict store validation replaces repeated cleanup full scans on restart. */
private[mongo] object MongoInterviewCleanupIntegrityMigrations {
  private val Id: MigrationId = MigrationIds.InterviewCleanupIntegrity

  /** Ledger literal kept as `String` for existing callers; the typed identity is
    * `MigrationIds.InterviewCleanupIntegrity`.
    */
  val MigrationId: String = Id.value
  private val BatchSize = 500
  private val CoveredProofs =
    List(MigrationIds.InterviewPublicationFencing, MigrationIds.AttributableProducerRegistrations)

  private def requireValidTopics(topics: InterviewTopicPair): IO[Unit] =
    IO.raiseUnless(topics.valid)(MigrationError.StepFailed(Id, "invalid interview cleanup topic identities"))

  /** A completed proof is trusted only with the exact installed validator for these topics and its covered proofs. */
  private def verifyCompleted(
      database: MongoHiringSetup.SetupDatabase,
      topics: InterviewTopicPair
  ): IO[CompletedProof] =
    MongoHiringValidators.assertStrictValidators(
      database,
      List(MongoCollections.InterviewSubjectCleanup -> MongoInterviewCleanupValidator.definition(topics))
    ) *> MongoMigrationLedger.requireComplete(database, CoveredProofs).as(CompletedProof.Trusted)

  private def audit(run: MigrationRun, topics: InterviewTopicPair): IO[Unit] = {
    val queue = run.database.getCollection(MongoCollections.InterviewSubjectCleanup)
    val validator = MongoInterviewCleanupValidator.definition(topics)
    def scan(after: Option[BsonValue]): IO[Unit] =
      queue
        .find(MongoInterviewCleanupSweepCodec.afterFilter(after))
        .sort(Sorts.ascending(MongoFields.Id))
        .hint("_id_")
        .limit(BatchSize)
        .all
        .flatMap { rows =>
          rows.toList.traverse_(row =>
            IO.fromEither(
              MongoInterviewCleanupCodec
                .decodeCurrent(row, topics)
                .leftMap(_ => MigrationError.StepFailed(run.id, "cutover rejected stored cleanup evidence"))
            ).void
          ) *> rows.lastOption.traverse_ { last =>
            IO.fromEither(
              MongoInterviewCleanupSweepCodec
                .identity(last)
                .leftMap(_ => MigrationError.StepFailed(run.id, "cleanup identity is absent"))
            ).flatMap { identity =>
              run.advance(last.get(MongoFields.Id)) *>
                (if (rows.size == BatchSize) IO.defer(scan(Some(identity))) else IO.unit)
            }
          }
        }
    for {
      _ <- MongoMigrationLedger.requireComplete(run.database, CoveredProofs)
      after <- IO.fromEither(run.checkpoint.traverse { value =>
        MongoInterviewCleanupSweepCodec
          .identity(new Document(MongoFields.Id, value))
          .leftMap(_ => MigrationError.LedgerCorrupt(run.id, "unsupported cleanup identity checkpoint"))
      })
      _ <- MongoHiringValidators.install(run.database, MongoCollections.InterviewSubjectCleanup, validator)
      _ <- scan(after)
      // collMod does not retroactively validate old rows; check the full native predicate before proof completion.
      invalid <- queue.find(new Document("$nor", List(validator).asJava)).limit(1).first
      _ <- IO.raiseUnless(invalid.isEmpty)(MigrationError.StepFailed(run.id, "stored cleanup verification failed"))
      _ <- verifyCompleted(run.database, topics)
    } yield ()
  }

  def step(topics: InterviewTopicPair): MongoMigrationStep =
    MongoMigrationStep(Id, audit(_, topics), verifyCompleted(_, topics))

  def trusted(database: MongoHiringSetup.SetupDatabase, topics: InterviewTopicPair): IO[Boolean] =
    requireValidTopics(topics) *> MongoMigrationRunner.trusted(database, step(topics))

  def initialize(database: MongoHiringSetup.SetupDatabase, topics: InterviewTopicPair): IO[Unit] =
    requireValidTopics(topics) *> MongoMigrationRunner.run(database, step(topics))
}
