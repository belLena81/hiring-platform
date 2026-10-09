package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import mongo4cats.database.MongoDatabase
import mongo4cats.collection.MongoCollection
import mongo4cats.codecs.CodecRegistry
import mongo4cats.models.database.CreateCollectionOptions
import com.mongodb.{MongoCommandException, ReadPreference}
import org.bson.{BsonDocument, Document}
import org.bson.conversions.Bson
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.domain.workflow.InterviewTopicPair

final case class AtlasSearchIndexConfig(
    jobVectorIndex: String,
    candidateVectorIndex: String,
    jobLexicalIndex: String,
    candidateLexicalIndex: String,
    dimension: Int,
    readyTimeoutMillis: Int,
    pollIntervalMillis: Int
)

/** Creates the pre-MVP Mongo shape and only removes prior hiring data when explicitly requested. */
object MongoHiringSetup {
  private[mongo] type SetupCollection = MongoCollection[IO, Document]
  private[mongo] final case class SetupDatabase(
      underlying: MongoDatabase[IO],
      collections: Map[String, SetupCollection]
  ) {
    def getCollection(name: String): SetupCollection = collections(name)
    def createCollection(name: String): IO[Unit] = underlying.createCollection(name, CreateCollectionOptions())

    /** Idempotent creation: an already existing namespace (code 48) is the expected restart outcome. */
    def ensureCollection(name: String): IO[Unit] = createCollection(name).recoverWith {
      case error: MongoCommandException if error.getErrorCode == 48 => IO.unit
    }
    def runCommand(command: Bson): IO[BsonDocument] =
      underlying.runCommand(command, ReadPreference.primary()).map(_.toBsonDocument)
    def dropCollection(name: String): IO[Unit] = getCollection(name).drop
  }

  private[mongo] def setupDatabase(database: MongoDatabase[IO]): IO[SetupDatabase] =
    MongoHiringMigrations.ownedCollections.toList
      .traverse(name => database.getCollection[Document](name, CodecRegistry.Default).map(name -> _))
      .map(values => SetupDatabase(database, values.toMap))

  // TODO(RF-06): forwarding aliases for integration specs owned by concurrent slices; use MongoIndexNames directly.
  val JobsLocationPointIndex: String = MongoIndexNames.JobsLocationPoint
  val EventOutboxSubjectIdsIndex: String = MongoIndexNames.EventOutboxSubjectIds
  val InterviewWorkflowInboxIdentityIndex: String = MongoIndexNames.InterviewWorkflowInboxIdentity

  def initialize(database: MongoDatabase[IO], diagnostics: Diagnostics): IO[Unit] =
    initialize(database, None, resetOnStart = false, diagnostics = diagnostics)
  def initialize(database: MongoDatabase[IO], diagnostics: Diagnostics, topics: InterviewTopicPair): IO[Unit] =
    initialize(database, None, resetOnStart = false, diagnostics = diagnostics, topics = topics)
  def initialize(
      database: MongoDatabase[IO],
      atlas: Option[AtlasSearchIndexConfig],
      diagnostics: Diagnostics
  ): IO[Unit] =
    initialize(database, atlas, resetOnStart = false, diagnostics = diagnostics)

  /** Ordered setup: ledger migrations, strict validators, verified indexes, optional Atlas search provisioning. The
    * residence proof is checked before the user validator is installed so completed-proof drift is never overwritten.
    */
  def initialize(
      database: MongoDatabase[IO],
      atlas: Option[AtlasSearchIndexConfig],
      resetOnStart: Boolean,
      diagnostics: Diagnostics,
      topics: InterviewTopicPair = InterviewTopicPair.Default
  ): IO[Unit] =
    setupDatabase(database).flatMap { setup =>
      List(
        IO.unlessA(resetOnStart)(MongoCandidateResidenceIntegrityMigrations.verifyCompleted(setup)),
        MongoHiringMigrations.initialize(setup, resetOnStart, diagnostics, topics),
        MongoHiringValidators.createUserValidator(setup),
        MongoCandidateResidenceIntegrityMigrations.initialize(setup),
        MongoHiringValidators.createJobValidator(setup),
        MongoHiringMigrations.verifyJobGeoPoints(setup),
        MongoHiringIndexSetup.create(database),
        MongoHiringValidators.createOutboxValidator(setup),
        atlas.traverse_(MongoAtlasSearchSetup.provision(setup, _))
      ).sequence_
    }
}
