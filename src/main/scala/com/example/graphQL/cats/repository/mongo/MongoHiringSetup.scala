package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.mongodb.client.model.{Filters, IndexOptions, Indexes, SearchIndexModel, SearchIndexType, UpdateOptions, Updates}
import com.mongodb.reactivestreams.client.MongoDatabase
import org.bson.Document
import java.time.Instant
import java.util.Date
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

final case class AtlasSearchIndexConfig(
    jobVectorIndex: String,
    candidateVectorIndex: String,
    jobLexicalIndex: String,
    dimension: Int,
    readyTimeoutMillis: Int,
    pollIntervalMillis: Int
)

object MongoHiringSetup {
  val UsersEmailIndex = "users_emailCanonical_unique"
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
  val HiringDomainMongoMigrationId = "phase-2-domain-mongodb-v1"
  val HiringGraphQLSearchIndexMigrationId = "hiring-graphql-search-indexes-v1"
  val HiringAdminJobListingIndexMigrationId = "hiring-admin-job-listing-indexes-v1"
  val HiringVectorSearchMigrationId = "hiring-vector-search-v1"
  val HiringAtlasSearchIndexMigrationId = "hiring-atlas-search-indexes-v1"

  def initialize(database: MongoDatabase): IO[Unit] =
    initialize(database, None)

  def initialize(database: MongoDatabase, atlas: Option[AtlasSearchIndexConfig]): IO[Unit] =
    ordinaryIndexes(database).sequence_.flatMap { _ =>
      val atlasSetup = atlas.fold(IO.unit)(config => provisionAtlasIndexes(database, config))
      atlasSetup *> recordMigrations(database, atlas.isDefined)
    }

  private def ordinaryIndexes(database: MongoDatabase): List[IO[Unit]] = List(
      createIndex(database.getCollection("users"),
        Indexes.ascending("emailCanonical"), new IndexOptions().name(UsersEmailIndex).unique(true)),
      createIndex(database.getCollection("users"),
        Indexes.ascending("adminSingletonKey"),
        new IndexOptions().name(UsersAdminSingletonIndex).unique(true)
          .partialFilterExpression(Filters.eq("role", "Admin"))),
      createIndex(database.getCollection("applications"),
        Indexes.ascending("candidateId", "jobId"), new IndexOptions().name(ApplicationsCandidateJobIndex).unique(true)),
      createIndex(database.getCollection("jobs"),
        Indexes.compoundIndex(Indexes.ascending("recruiterId", "status"), Indexes.descending("createdAt", "_id")),
        new IndexOptions().name(JobsRecruiterStatusCreatedIndex)),
      createIndex(database.getCollection("jobs"),
        Indexes.compoundIndex(Indexes.ascending("recruiterId"), Indexes.descending("createdAt", "_id")),
        new IndexOptions().name(JobsRecruiterCreatedIndex)),
      createIndex(database.getCollection("applications"),
        Indexes.compoundIndex(Indexes.ascending("candidateId", "status"), Indexes.descending("createdAt", "_id")),
        new IndexOptions().name(ApplicationsCandidateStatusCreatedIndex)),
      createIndex(database.getCollection("applications"),
        Indexes.compoundIndex(Indexes.ascending("candidateId"), Indexes.descending("createdAt", "_id")),
        new IndexOptions().name(ApplicationsCandidateCreatedIndex)),
      createIndex(database.getCollection("applications"),
        Indexes.compoundIndex(Indexes.ascending("jobId", "status"), Indexes.descending("createdAt", "_id")),
        new IndexOptions().name(ApplicationsJobStatusCreatedIndex)),
      createIndex(database.getCollection("applications"),
        Indexes.compoundIndex(Indexes.ascending("jobId"), Indexes.descending("createdAt", "_id")),
        new IndexOptions().name(ApplicationsJobCreatedIndex)),
      createIndex(database.getCollection("application_events"),
        Indexes.compoundIndex(Indexes.ascending("applicationId"), Indexes.descending("occurredAt", "_id")),
        new IndexOptions().name(ApplicationEventsApplicationCreatedIndex)),
      createIndex(database.getCollection("jobs"),
        Indexes.descending("createdAt", "_id"),
        new IndexOptions().name(JobsCreatedIndex)),
      createIndex(database.getCollection("jobs"),
        Indexes.compoundIndex(Indexes.ascending("status"), Indexes.descending("createdAt", "_id")),
        new IndexOptions().name(JobsOpenCreatedIndex)),
      createIndex(database.getCollection("jobs"),
        Indexes.compoundIndex(Indexes.ascending("status", "location.city"), Indexes.descending("createdAt", "_id")),
        new IndexOptions().name(JobsOpenCityCreatedIndex)),
      createIndex(database.getCollection("jobs"),
        Indexes.ascending("embeddingMeta.model", "embeddingMeta.version", "status", "location.city", "recruiterId"),
        new IndexOptions().name(JobsEmbeddingMetaIndex)),
      createIndex(database.getCollection("users"),
        Indexes.ascending("embeddingMeta.model", "embeddingMeta.version", "role"),
        new IndexOptions().name(UsersEmbeddingMetaIndex))
    )

  private def recordMigrations(database: MongoDatabase, atlasEnabled: Boolean): IO[Unit] = {
    val migrations = database.getCollection("schema_migrations")
    val base = recordMigration(migrations, HiringDomainMongoMigrationId, "Hiring domain MongoDB collections and indexes") *>
      recordMigration(migrations, HiringGraphQLSearchIndexMigrationId, "Hiring GraphQL job search indexes") *>
      recordMigration(migrations, HiringAdminJobListingIndexMigrationId, "Hiring Admin job listing indexes") *>
      recordMigration(migrations, HiringVectorSearchMigrationId, "Hiring Vector Search metadata indexes")
    Option.when(atlasEnabled)(recordMigration(migrations, HiringAtlasSearchIndexMigrationId,
      "Hiring Atlas vector and lexical search indexes")).fold(base)(base *> _)
  }

  private def provisionAtlasIndexes(database: MongoDatabase, config: AtlasSearchIndexConfig): IO[Unit] = {
    val jobs = database.getCollection("jobs")
    val users = database.getCollection("users")
    List(
      ensureSearchIndex(jobs, new SearchIndexModel(config.jobVectorIndex, vectorDefinition(config, jobFilterFields), SearchIndexType.vectorSearch()), config),
      ensureSearchIndex(users, new SearchIndexModel(config.candidateVectorIndex, vectorDefinition(config, candidateFilterFields), SearchIndexType.vectorSearch()), config),
      ensureSearchIndex(jobs, new SearchIndexModel(config.jobLexicalIndex, lexicalDefinition, SearchIndexType.search()), config)
    ).sequence_.void
  }

  private val jobFilterFields = List("status", "location.city", "skills", "createdAt", "recruiterId",
    "embeddingMeta.model", "embeddingMeta.version")
  private val candidateFilterFields = List("role", "embeddingMeta.model", "embeddingMeta.version")

  private def vectorDefinition(config: AtlasSearchIndexConfig, filters: List[String]): Document =
    new Document("fields", (new Document("type", "vector")
      .append("path", "embedding")
      .append("numDimensions", java.lang.Integer.valueOf(config.dimension))
      .append("similarity", "cosine") :: filters.map(path => new Document("type", "filter").append("path", path))).asJava)

  private val lexicalDefinition: Document =
    new Document("mappings", new Document("dynamic", false).append("fields", new Document()
      .append("title", new Document("type", "string"))
      .append("description", new Document("type", "string"))
      .append("requirements", new Document("type", "string"))
      .append("skills", new Document("type", "string"))))

  private def ensureSearchIndex(
      collection: com.mongodb.reactivestreams.client.MongoCollection[Document],
      model: SearchIndexModel,
      config: AtlasSearchIndexConfig
  ): IO[Unit] = {
    val name = model.getName
    val expectedType = model.getType.toBsonValue.asString().getValue
    PublisherBridge.first(collection.listSearchIndexes().name(name).first()).flatMap {
      case None =>
        createSearchIndex(collection, model) *> awaitReady(collection, name, expectedType, model.getDefinition, config)
      case Some(existing) if searchIndexMatches(existing, expectedType, model.getDefinition) =>
        awaitReady(collection, name, expectedType, model.getDefinition, config)
      case Some(existing) if existing.getString("type", "search") != expectedType =>
        PublisherBridge.first(collection.dropSearchIndex(name)).void *>
          createSearchIndex(collection, model) *>
          awaitReady(collection, name, expectedType, model.getDefinition, config)
      case Some(_) =>
        PublisherBridge.first(collection.updateSearchIndex(name, model.getDefinition)).void *>
          awaitReady(collection, name, expectedType, model.getDefinition, config)
    }
  }

  private def createSearchIndex(
      collection: com.mongodb.reactivestreams.client.MongoCollection[Document],
      model: SearchIndexModel
  ): IO[Unit] = PublisherBridge.first(collection.createSearchIndexes(List(model).asJava)).void

  private def searchIndexMatches(existing: Document, expectedType: String, expectedDefinition: org.bson.conversions.Bson): Boolean =
    existing.getString("type", "search") == expectedType &&
      Option(existing.get("latestDefinition", classOf[Document])).exists(_ == expectedDefinition.asInstanceOf[Document])

  private def awaitReady(
      collection: com.mongodb.reactivestreams.client.MongoCollection[Document],
      name: String,
      expectedType: String,
      expectedDefinition: org.bson.conversions.Bson,
      config: AtlasSearchIndexConfig
  ): IO[Unit] = {
    val timeout = config.readyTimeoutMillis.millis
    def loop(start: FiniteDuration): IO[Unit] =
      PublisherBridge.first(collection.listSearchIndexes().name(name).first()).flatMap {
        case Some(index) if searchIndexMatches(index, expectedType, expectedDefinition) &&
            index.getString("status", "").equalsIgnoreCase("READY") && index.getBoolean("queryable", false) =>
          IO.unit
        case Some(index) if index.getString("status", "").equalsIgnoreCase("FAILED") =>
          IO.raiseError(new IllegalStateException(s"Atlas search index '$name' failed to build"))
        case _ =>
          IO.monotonic.flatMap { now =>
            if (now - start >= timeout) IO.raiseError(new IllegalStateException(s"Atlas search index '$name' was not ready"))
            else IO.sleep(config.pollIntervalMillis.millis) *> loop(start)
          }
      }
    IO.monotonic.flatMap(loop)
  }

  private def recordMigration(
      migrations: com.mongodb.reactivestreams.client.MongoCollection[Document],
      id: String,
      description: String
  ): IO[Unit] =
    PublisherBridge.first(migrations.updateOne(
      Filters.eq("_id", id),
      Updates.combine(
        Updates.setOnInsert("_id", id),
        Updates.setOnInsert("schemaVersion", 1),
        Updates.setOnInsert("appliedAt", Date.from(Instant.now())),
        Updates.setOnInsert("description", description),
        Updates.setOnInsert("checksum", id)
      ),
      new UpdateOptions().upsert(true)
    )).void

  private def createIndex(
      collection: com.mongodb.reactivestreams.client.MongoCollection[Document],
      keys: org.bson.conversions.Bson,
      options: IndexOptions
  ): IO[Unit] =
    PublisherBridge.first(collection.createIndex(keys, options)).void
}
