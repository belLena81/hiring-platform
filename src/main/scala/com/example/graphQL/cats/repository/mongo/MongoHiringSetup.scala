package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.mongodb.client.model.{Filters, IndexOptions, Indexes, SearchIndexModel, SearchIndexType, UpdateOptions, Updates}
import com.mongodb.MongoCommandException
import com.mongodb.reactivestreams.client.{MongoCollection, MongoDatabase}
import org.bson.Document
import org.bson.conversions.Bson
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.*

final case class AtlasSearchIndexConfig(
    jobVectorIndex: String, candidateVectorIndex: String, jobLexicalIndex: String,
    dimension: Int, readyTimeoutMillis: Int, pollIntervalMillis: Int
)

/** Creates the pre-MVP Mongo shape and only removes prior hiring data when explicitly requested. */
object MongoHiringSetup {
  private val CollectionLimit = 128
  private val ownedCollections = Set(
    "users", "jobs", "applications", "application_events", "account_registry", "embedding_work",
    "event_outbox", "search_sessions", "search_session_work", "consumer_receipts", "mutation_receipts", "event_quarantine", "hiring_migration_ledger"
  )

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

  def initialize(database: MongoDatabase): IO[Unit] = initialize(database, None, resetOnStart = false)
  def initialize(database: MongoDatabase, atlas: Option[AtlasSearchIndexConfig]): IO[Unit] = initialize(database, atlas, resetOnStart = false)
  def initialize(database: MongoDatabase, atlas: Option[AtlasSearchIndexConfig], resetOnStart: Boolean): IO[Unit] =
    Option.when(resetOnStart)(resetOwnedCollections(database)).getOrElse(IO.unit) *>
      createAccountRegistry(database) *> createUserValidator(database) *> createIndexes(database) *>
      atlas.traverse_(provisionAtlasIndexes(database, _))

  private def resetOwnedCollections(database: MongoDatabase): IO[Unit] =
    PublisherBridge.collectWithin(database.listCollectionNames(), CollectionLimit).flatMap { names =>
      names.filter(ownedCollections).traverse_(name => PublisherBridge.first(database.getCollection(name).drop()).void)
    }

  private def createAccountRegistry(database: MongoDatabase): IO[Unit] =
    PublisherBridge.first(database.getCollection("account_registry").updateOne(
      Filters.eq("_id", "user-account-registry"),
      Updates.combine(Updates.setOnInsert("_id", "user-account-registry"), Updates.setOnInsert("state", "Uninitialized")),
      new UpdateOptions().upsert(true)
    )).void

  private def createIndexes(database: MongoDatabase): IO[Unit] = List(
    index(database.getCollection("users"), Indexes.ascending("emailCanonical"), new IndexOptions().name(UsersEmailIndex).unique(true).sparse(true)),
    index(database.getCollection("users"), Indexes.ascending("nameCanonical"), new IndexOptions().name(UsersNameIndex).unique(true)),
    index(database.getCollection("users"), Indexes.compoundIndex(Indexes.ascending("accountStatus"), Indexes.descending("createdAt", "_id")), new IndexOptions().name(UsersStatusCreatedIndex)),
    index(database.getCollection("users"), Indexes.compoundIndex(Indexes.ascending("role", "accountStatus"), Indexes.descending("createdAt", "_id")), new IndexOptions().name(UsersRoleStatusCreatedIndex)),
    index(database.getCollection("users"), Indexes.ascending("adminSingletonKey"), new IndexOptions().name(UsersAdminSingletonIndex).unique(true).partialFilterExpression(Filters.eq("role", "Admin"))),
    index(database.getCollection("users"), Indexes.ascending("embeddingMeta.model", "role"), new IndexOptions().name(UsersEmbeddingMetaIndex)),
    index(database.getCollection("jobs"), Indexes.compoundIndex(Indexes.ascending("recruiterId", "status"), Indexes.descending("createdAt", "_id")), new IndexOptions().name(JobsRecruiterStatusCreatedIndex)),
    index(database.getCollection("jobs"), Indexes.compoundIndex(Indexes.ascending("recruiterId"), Indexes.descending("createdAt", "_id")), new IndexOptions().name(JobsRecruiterCreatedIndex)),
    index(database.getCollection("jobs"), Indexes.descending("createdAt", "_id"), new IndexOptions().name(JobsCreatedIndex)),
    index(database.getCollection("jobs"), Indexes.compoundIndex(Indexes.ascending("status"), Indexes.descending("createdAt", "_id")), new IndexOptions().name(JobsOpenCreatedIndex)),
    index(database.getCollection("jobs"), Indexes.compoundIndex(Indexes.ascending("status", "location.city"), Indexes.descending("createdAt", "_id")), new IndexOptions().name(JobsOpenCityCreatedIndex)),
    index(database.getCollection("jobs"), Indexes.ascending("embeddingMeta.model", "status", "location.city", "recruiterId"), new IndexOptions().name(JobsEmbeddingMetaIndex)),
    index(database.getCollection("applications"), Indexes.ascending("candidateId", "jobId"), new IndexOptions().name(ApplicationsCandidateJobIndex).unique(true)),
    index(database.getCollection("applications"), Indexes.compoundIndex(Indexes.ascending("candidateId", "status"), Indexes.descending("createdAt", "_id")), new IndexOptions().name(ApplicationsCandidateStatusCreatedIndex)),
    index(database.getCollection("applications"), Indexes.compoundIndex(Indexes.ascending("candidateId"), Indexes.descending("createdAt", "_id")), new IndexOptions().name(ApplicationsCandidateCreatedIndex)),
    index(database.getCollection("applications"), Indexes.compoundIndex(Indexes.ascending("jobId", "status"), Indexes.descending("createdAt", "_id")), new IndexOptions().name(ApplicationsJobStatusCreatedIndex)),
    index(database.getCollection("applications"), Indexes.compoundIndex(Indexes.ascending("jobId"), Indexes.descending("createdAt", "_id")), new IndexOptions().name(ApplicationsJobCreatedIndex)),
    index(database.getCollection("application_events"), Indexes.compoundIndex(Indexes.ascending("applicationId"), Indexes.descending("occurredAt", "_id")), new IndexOptions().name(ApplicationEventsApplicationCreatedIndex)),
    index(database.getCollection("embedding_work"), Indexes.ascending("state", "availableAt", "leaseUntil"), new IndexOptions().name(EmbeddingWorkAvailableIndex)),
    index(database.getCollection("event_outbox"), Indexes.ascending("state", "availableAt", "leaseUntil", "occurredAt", "_id"), new IndexOptions().name(EventOutboxClaimIndex)),
    index(database.getCollection("event_outbox"), Indexes.ascending("retentionExpiresAt"), new IndexOptions().name(EventOutboxPublishedRetentionIndex).expireAfter(0L, TimeUnit.SECONDS).partialFilterExpression(Filters.eq("state", "Published"))),
    index(database.getCollection("search_sessions"), Indexes.compoundIndex(Indexes.ascending("actorId"), Indexes.descending("occurredAt", "_id")), new IndexOptions().name(SearchSessionsActorIndex)),
    index(database.getCollection("search_sessions"), Indexes.ascending("expiresAt"), new IndexOptions().name(SearchSessionsExpiryIndex).expireAfter(0L, TimeUnit.SECONDS)),
    index(database.getCollection("search_session_work"), Indexes.ascending("state", "availableAt", "leaseUntil", "createdAt"), new IndexOptions().name(SearchSessionWorkClaimIndex)),
    index(database.getCollection("search_session_work"), Indexes.ascending("retentionExpiresAt"), new IndexOptions().name(SearchSessionWorkRetentionIndex).expireAfter(0L, TimeUnit.SECONDS).partialFilterExpression(Filters.eq("state", "Failed"))),
    index(database.getCollection("consumer_receipts"), Indexes.ascending("consumerGroup", "eventId"), new IndexOptions().name(ConsumerReceiptsIdIndex).unique(true)),
    index(database.getCollection("consumer_receipts"), Indexes.ascending("expiresAt"), new IndexOptions().name(ConsumerReceiptsExpiryIndex).expireAfter(0L, TimeUnit.SECONDS)),
    index(database.getCollection("mutation_receipts"), Indexes.ascending("operation", "actorScope", "idempotencyKey"), new IndexOptions().name(MutationReceiptsKeyIndex).unique(true)),
    index(database.getCollection("mutation_receipts"), Indexes.ascending("expiresAt"), new IndexOptions().name(MutationReceiptsExpiryIndex).expireAfter(0L, TimeUnit.SECONDS)),
    index(database.getCollection("event_quarantine"), Indexes.ascending("topic", "partition", "offset"), new IndexOptions().name(EventQuarantineOffsetIndex).unique(true)),
    index(database.getCollection("event_quarantine"), Indexes.ascending("expiresAt"), new IndexOptions().name(EventQuarantineExpiryIndex).expireAfter(0L, TimeUnit.SECONDS))
  ).sequence_.void

  private def createUserValidator(database: MongoDatabase): IO[Unit] = {
    def active(role: String, required: String) = new Document("required", List(required).asJava).append("properties", new Document()
      .append("role", new Document("enum", List(role).asJava)).append("accountStatus", new Document("enum", List("Active").asJava)))
    val admin = active("Admin", "adminSingletonKey")
    admin.get("properties", classOf[Document]).append("adminSingletonKey", new Document("enum", List("singleton-admin").asJava))
    val schema = new Document("$jsonSchema", new Document("bsonType", "object").append("required", List("role", "accountStatus").asJava)
      .append("oneOf", List(active("Candidate", "profile"), active("Recruiter", "profile"), admin,
        new Document("properties", new Document("accountStatus", new Document("enum", List("Deleted").asJava)))).asJava))
    PublisherBridge.first(database.createCollection("users")).void.recoverWith {
      case error: MongoCommandException if error.getErrorCode == 48 => IO.unit
    } *> PublisherBridge.first(database.runCommand(new Document("collMod", "users").append("validator", schema)
      .append("validationLevel", "strict").append("validationAction", "error"))).void
  }

  private def index(collection: MongoCollection[Document], keys: Bson, options: IndexOptions): IO[Unit] =
    PublisherBridge.first(collection.createIndex(keys, options)).void

  private def provisionAtlasIndexes(database: MongoDatabase, config: AtlasSearchIndexConfig): IO[Unit] = {
    def vectorDefinition(filters: List[String]): Document = new Document("fields", (new Document("type", "vector")
      .append("path", "embedding").append("numDimensions", Int.box(config.dimension)).append("similarity", "cosine") ::
      filters.map(path => new Document("type", "filter").append("path", path))).asJava)
    val lexicalDefinition = new Document("mappings", new Document("dynamic", false).append("fields", new Document()
      .append("title", new Document("type", "string")).append("description", new Document("type", "string"))
      .append("requirements", new Document("type", "string")).append("skills", new Document("type", "string"))))
    def create(collection: MongoCollection[Document], model: SearchIndexModel): IO[Unit] =
      PublisherBridge.first(collection.createSearchIndexes(List(model).asJava)).void
    List(
      create(database.getCollection("jobs"), new SearchIndexModel(config.jobVectorIndex,
        vectorDefinition(List("status", "location.city", "skills", "createdAt", "recruiterId", "embeddingMeta.model")), SearchIndexType.vectorSearch())),
      create(database.getCollection("users"), new SearchIndexModel(config.candidateVectorIndex,
        vectorDefinition(List("role", "embeddingMeta.model")), SearchIndexType.vectorSearch())),
      create(database.getCollection("jobs"), new SearchIndexModel(config.jobLexicalIndex, lexicalDefinition, SearchIndexType.search()))
    ).sequence_.void
  }
}
