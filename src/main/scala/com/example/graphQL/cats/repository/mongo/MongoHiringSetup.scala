package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import mongo4cats.database.MongoDatabase
import mongo4cats.collection.MongoCollection
import mongo4cats.codecs.CodecRegistry
import mongo4cats.models.database.CreateCollectionOptions
import com.mongodb.ReadPreference
import org.bson.Document
import org.bson.conversions.Bson

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
    def runCommand(command: Bson): IO[Unit] = underlying.runCommand(command, ReadPreference.primary()).void
    def dropCollection(name: String): IO[Unit] = getCollection(name).drop
  }

  private def setupDatabase(database: MongoDatabase[IO]): IO[SetupDatabase] =
    MongoHiringMigrations.ownedCollections.toList
      .traverse(name => database.getCollection[Document](name, CodecRegistry.Default).map(name -> _))
      .map(values => SetupDatabase(database, values.toMap))

  val EmbeddingWorkAvailableIndex = "embedding_work_available_lease"
  val UsersEmailIndex = "users_emailCanonical_unique"
  val UsersNameIndex = "users_nameCanonical_unique"
  val UsersStatusCreatedIndex = "users_accountStatus_created_id"
  val UsersRoleStatusCreatedIndex = "users_role_accountStatus_created_id"
  val UsersAdminSingletonIndex = "users_adminSingleton_unique"
  val ApplicationsCandidateJobIndex = "applications_candidate_job_unique"
  val JobsRecruiterStatusCreatedIndex = "jobs_recruiter_status_created_id"
  val JobsRecruiterCreatedIndex = "jobs_recruiter_created_id"
  val ApplicationsCandidateStatusCreatedIndex = "applications_candidate_status_created_id"
  val ApplicationsCandidateCreatedIndex = "applications_candidate_created_id"
  val ApplicationsJobStatusCreatedIndex = "applications_job_status_created_id"
  val ApplicationsJobCreatedIndex = "applications_job_created_id"
  val ApplicationEventsApplicationCreatedIndex = "application_events_application_created_id"
  val JobsCreatedIndex = "jobs_created_id"
  val JobsOpenCreatedIndex = "jobs_open_created_id"
  val JobsOpenCityCreatedIndex = "jobs_open_city_created_id"
  val JobsEmbeddingMetaIndex = "jobs_embedding_meta_filters"
  val UsersEmbeddingMetaIndex = "users_embedding_meta_filters"
  val EventOutboxClaimIndex = "event_outbox_claim"
  val EventOutboxPublishedRetentionIndex = "event_outbox_published_retention"
  val EventOutboxSubjectIdsIndex = "event_outbox_subject_ids"
  val OutboxSubjectFenceLeaseIndex = "outbox_subject_fences_lease_until"
  val SearchSessionsActorIndex = "search_sessions_actor_created"
  val SearchSessionsExpiryIndex = "search_sessions_expiry"
  val SearchSessionWorkClaimIndex = "search_session_work_claim"
  val SearchSessionWorkRetentionIndex = "search_session_work_retention"
  val ConsumerReceiptsIdIndex = "consumer_receipts_group_event"
  val ConsumerReceiptsExpiryIndex = "consumer_receipts_expiry"
  val MutationReceiptsKeyIndex = "mutation_receipts_operation_scope_key"
  val MutationReceiptsExpiryIndex = "mutation_receipts_expiry"
  val EventQuarantineOffsetIndex = "event_quarantine_offset"
  val EventQuarantineExpiryIndex = "event_quarantine_expiry"
  val AnalyticsErasureRequestStateIndex = "analytics_erasure_requests_state_requested"
  val AnalyticsReportPublishedIndex = "analytics_report_snapshots_published_as_of"
  val AnalyticsReportExpiryIndex = "analytics_report_snapshots_expiry"
  val AnalyticsReportRunExpiryIndex = "analytics_report_runs_expiry"

  def initialize(database: MongoDatabase[IO]): IO[Unit] = initialize(database, None, resetOnStart = false)
  def initialize(database: MongoDatabase[IO], atlas: Option[AtlasSearchIndexConfig]): IO[Unit] =
    initialize(database, atlas, resetOnStart = false)
  def initialize(database: MongoDatabase[IO], atlas: Option[AtlasSearchIndexConfig], resetOnStart: Boolean): IO[Unit] =
    setupDatabase(database).flatMap { setup =>
      MongoHiringMigrations.initialize(setup, resetOnStart) *>
        MongoHiringValidators.createUserValidator(database) *>
        MongoHiringValidators.createJobValidator(database) *>
        MongoHiringIndexSetup.create(database) *>
        MongoHiringValidators.createOutboxValidator(database) *>
        atlas.traverse_(MongoAtlasSearchSetup.provision(setup, _))
    }

}
