package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.AccountName
import com.mongodb.client.model.{Filters, IndexOptions, Indexes, SearchIndexModel, SearchIndexType, UpdateOptions, Updates}
import com.mongodb.reactivestreams.client.MongoDatabase
import org.bson.Document
import org.bson.conversions.Bson
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
  val HiringDomainMongoMigrationId = "phase-2-domain-mongodb-v1"
  val HiringGraphQLSearchIndexMigrationId = "hiring-graphql-search-indexes-v1"
  val HiringAdminJobListingIndexMigrationId = "hiring-admin-job-listing-indexes-v1"
  val HiringVectorSearchMigrationId = "hiring-vector-search-v1"
  val HiringAtlasSearchIndexMigrationId = "hiring-atlas-search-indexes-v1"
  val UserNameCanonicalMigrationId = "user-name-canonical-v1"
  val UserAccountMigrationId = "user-account-management-v1"
  val UserProfileOneOfMigrationId = "user-profile-one-of-v1"
  val UserEmailSparseIndexMigrationId = "user-email-canonical-sparse-v1"

  def initialize(database: MongoDatabase): IO[Unit] =
    initialize(database, None)

  def initialize(database: MongoDatabase, atlas: Option[AtlasSearchIndexConfig]): IO[Unit] =
    backfillLegacyNames(database) *>
      migrateUserProfiles(database) *>
      replaceEmailIndex(database) *>
      (ordinaryIndexes(database) :+ ensureAccountRegistry(database)).sequence_ *>
      ensureUserProfileValidator(database) *>
      recordMigrations(database, atlas.isDefined) *>
      atlas.fold(IO.unit)(config => provisionAtlasIndexes(database, config))

  private def ordinaryIndexes(database: MongoDatabase): List[IO[Unit]] = List(
      createIndex(database.getCollection("users"),
        Indexes.ascending("nameCanonical"), new IndexOptions().name(UsersNameIndex).unique(true)),
      createIndex(database.getCollection("users"),
        Indexes.compoundIndex(Indexes.ascending("accountStatus"), Indexes.descending("createdAt", "_id")),
        new IndexOptions().name(UsersStatusCreatedIndex)),
      createIndex(database.getCollection("users"),
        Indexes.compoundIndex(Indexes.ascending("role", "accountStatus"), Indexes.descending("createdAt", "_id")),
        new IndexOptions().name(UsersRoleStatusCreatedIndex)),
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
      *>
      recordMigration(migrations, UserNameCanonicalMigrationId, "Backfill canonical account names before the unique index")
      *>
      recordMigration(migrations, UserAccountMigrationId, "User account credentials, lifecycle, and query indexes")
      *>
      recordMigration(migrations, UserProfileOneOfMigrationId, "Normalize active user profiles to one role-specific MongoDB profile")
      *>
      recordMigration(migrations, UserEmailSparseIndexMigrationId, "Replace the legacy email index with a sparse unique index")
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

  private def replaceEmailIndex(database: MongoDatabase): IO[Unit] = {
    val users = database.getCollection("users")
    PublisherBridge.all(users.listIndexes()).flatMap { indexes =>
      indexes.find(_.getString("name") == UsersEmailIndex) match {
        case Some(index) if index.getBoolean("unique", false) && index.getBoolean("sparse", false) =>
          IO.unit
        case Some(_) =>
          PublisherBridge.first(users.dropIndex(UsersEmailIndex)).void *>
            createIndex(users, Indexes.ascending("emailCanonical"),
              new IndexOptions().name(UsersEmailIndex).unique(true).sparse(true))
        case None =>
          createIndex(users, Indexes.ascending("emailCanonical"),
            new IndexOptions().name(UsersEmailIndex).unique(true).sparse(true))
      }
    }
  }

  private def migrateUserProfiles(database: MongoDatabase): IO[Unit] = {
    val users = database.getCollection("users")
    PublisherBridge.all(users.find()).flatMap { documents =>
      documents.traverse(profileMigrationUpdates).flatMap { plans =>
        documents.zip(plans).traverse_ { case (document, updates) =>
          if (updates.isEmpty) IO.unit
          else PublisherBridge.first(users.updateOne(
            Filters.eq("_id", document.getString("_id")),
            Updates.combine(updates*)
          )).void
        }
      }
    }
  }

  private def profileMigrationUpdates(document: Document): IO[List[Bson]] =
    IO.fromEither {
      val role = Option(document.getString("role"))
        .toRight(new IllegalStateException("users collection contains an account without a role"))
      val status = Option(document.getString("accountStatus")).getOrElse("Active")
      val profile = Option(document.get("profile", classOf[Document]))
      val legacyRecruiter = Option(document.get("recruiterProfile", classOf[Document]))

      role.flatMap {
        case "Admin" if status == "Active" =>
          if (Option(document.getString("adminSingletonKey")).contains("singleton-admin") &&
              profile.isEmpty && legacyRecruiter.isEmpty)
            Right(List(Updates.set("schemaVersion", 3)))
          else Left(new IllegalStateException("active Admin must be the singleton account without a profile"))

        case "Candidate" if status == "Active" =>
          if (legacyRecruiter.nonEmpty)
            Left(new IllegalStateException("Candidate account contains a recruiter profile"))
          else profile match {
            case None => Left(new IllegalStateException("active Candidate account is missing its profile"))
            case Some(value) if Option(value.getString("kind")).exists(_ != "Candidate") =>
              Left(new IllegalStateException("Candidate account contains a non-Candidate profile"))
            case Some(_) =>
              Right(List(Updates.set("profile.kind", "Candidate"), Updates.set("schemaVersion", 3)))
          }

        case "Recruiter" if status == "Active" =>
          profile match {
            case Some(_) if legacyRecruiter.nonEmpty =>
              Left(new IllegalStateException("Recruiter account contains duplicate profiles"))
            case Some(value) if Option(value.getString("kind")).exists(_ != "Recruiter") =>
              Left(new IllegalStateException("Recruiter account contains a non-Recruiter profile"))
            case Some(_) =>
              Right(List(Updates.set("profile.kind", "Recruiter"), Updates.set("schemaVersion", 3)))
            case None => legacyRecruiter match {
              case None => Left(new IllegalStateException("active Recruiter account is missing its profile"))
              case Some(value) =>
                val migrated = new Document()
                migrated.putAll(value)
                migrated.put("kind", "Recruiter")
                Right(List(
                  Updates.set("profile", migrated),
                  Updates.unset("recruiterProfile"),
                  Updates.set("schemaVersion", 3)
                ))
            }
          }

        case _ if status == "Deleted" =>
          Right(List(
            Updates.unset("profile"),
            Updates.unset("recruiterProfile"),
            Updates.set("schemaVersion", 3)
          ))

        case other =>
          Left(new IllegalStateException(s"unsupported user role '$other' in active account"))
      }
    }

  private def ensureUserProfileValidator(database: MongoDatabase): IO[Unit] = {
    val candidateProfile = new Document("bsonType", "object")
      .append("required", List("kind", "skills").asJava)
      .append("properties", new Document("kind", new Document("enum", List("Candidate").asJava))
        .append("skills", new Document("bsonType", "array").append("minItems", 1)))
    val recruiterProfile = new Document("bsonType", "object")
      .append("required", List("kind", "organizationName").asJava)
      .append("properties", new Document("kind", new Document("enum", List("Recruiter").asJava))
        .append("organizationName", new Document("bsonType", "string")))
    val noProfile = new Document("anyOf", List(
      new Document("required", List("profile").asJava),
      new Document("required", List("recruiterProfile").asJava)
    ).asJava)
    val deleted = new Document("properties", new Document("accountStatus", new Document("enum", List("Deleted").asJava)))
      .append("not", noProfile)
    val admin = new Document("required", List("adminSingletonKey").asJava)
      .append("properties", new Document("accountStatus", new Document("enum", List("Active").asJava))
        .append("role", new Document("enum", List("Admin").asJava))
        .append("adminSingletonKey", new Document("enum", List("singleton-admin").asJava)))
      .append("not", noProfile)
    val candidate = new Document("required", List("profile").asJava)
      .append("properties", new Document("accountStatus", new Document("enum", List("Active").asJava))
        .append("role", new Document("enum", List("Candidate").asJava))
        .append("profile", candidateProfile))
      .append("not", new Document("anyOf", List(
        new Document("required", List("recruiterProfile").asJava),
        new Document("required", List("adminSingletonKey").asJava)
      ).asJava))
    val recruiter = new Document("required", List("profile").asJava)
      .append("properties", new Document("accountStatus", new Document("enum", List("Active").asJava))
        .append("role", new Document("enum", List("Recruiter").asJava))
        .append("profile", recruiterProfile))
      .append("not", new Document("anyOf", List(
        new Document("required", List("recruiterProfile").asJava),
        new Document("required", List("adminSingletonKey").asJava)
      ).asJava))
    val schema = new Document("bsonType", "object")
      .append("required", List("role", "accountStatus").asJava)
      .append("oneOf", List(deleted, admin, candidate, recruiter).asJava)
    val command = new Document("collMod", "users")
      .append("validator", new Document("$jsonSchema", schema))
      .append("validationLevel", "strict")
      .append("validationAction", "error")
    PublisherBridge.first(database.runCommand(command)).void
  }

  private def backfillLegacyNames(database: MongoDatabase): IO[Unit] = {
    val users = database.getCollection("users")
    PublisherBridge.all(users.find()).flatMap { documents =>
      val resolved = documents.traverse { document =>
        Option(document.getString("name")).filter(_.trim.nonEmpty) match {
          case Some(name) => Right(document -> AccountName.canonical(name))
          case None => Left(new IllegalStateException("users collection contains an account without a valid name"))
        }
      }
      resolved match {
        case Left(error) => IO.raiseError(error)
        case Right(values) =>
          val collisions = values.groupBy(_._2).collect { case (canonical, records) if records.size > 1 => canonical }.toList
          if (collisions.nonEmpty)
            IO.raiseError(new IllegalStateException("users collection contains duplicate canonical account names"))
          else values.filterNot { case (document, _) => document.containsKey("nameCanonical") }.traverse_ {
            case (document, canonical) =>
              PublisherBridge.first(users.updateOne(
                Filters.eq("_id", document.get("_id")),
                Updates.set("nameCanonical", canonical)
              )).void
          }
      }
    }
  }

  private def ensureAccountRegistry(database: MongoDatabase): IO[Unit] =
    PublisherBridge.first(database.getCollection("account_registry").updateOne(
      Filters.eq("_id", "user-account-registry"),
      Updates.combine(
        Updates.setOnInsert("_id", "user-account-registry"),
        Updates.setOnInsert("schemaVersion", 1),
        Updates.setOnInsert("state", "Uninitialized")
      ),
      new UpdateOptions().upsert(true)
    )).void
}
