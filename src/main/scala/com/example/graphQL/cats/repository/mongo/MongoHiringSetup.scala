package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.AccountName
import com.mongodb.MongoCommandException
import com.mongodb.client.model.{Filters, FindOneAndUpdateOptions, IndexOptions, Indexes, ReturnDocument, SearchIndexModel, SearchIndexType, Sorts, UpdateOptions, Updates}
import com.mongodb.reactivestreams.client.MongoDatabase
import org.bson.Document
import org.bson.conversions.Bson
import java.util.Date
import java.util.UUID
import java.security.MessageDigest
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
  private val UserMigrationBatchSize = 100
  private val MigrationLease = 5.minutes
  val HiringMigrationLedger = "hiring_migration_ledger"
  val HiringUserSetupMigrationId = "hiring-user-setup-v2"
  val EmbeddingWorkMigrationId = "hiring-embedding-work-v1"
  private val HiringUserSetupDescriptor = "canonical-name-account-status-profile-email-indexes-validator-atlas-v2"
  private val HiringUserSetupChecksum = sha256(HiringUserSetupDescriptor)
  private val EmbeddingWorkDescriptor = "durable-embedding-work-claim-index-v1"
  private val EmbeddingWorkChecksum = sha256(EmbeddingWorkDescriptor)
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
  val HiringDomainMongoMigrationId = "phase-2-domain-mongodb-v1"
  val HiringGraphQLSearchIndexMigrationId = "hiring-graphql-search-indexes-v1"
  val HiringAdminJobListingIndexMigrationId = "hiring-admin-job-listing-indexes-v1"
  val HiringVectorSearchMigrationId = "hiring-vector-search-v1"
  val HiringAtlasSearchIndexMigrationId = "hiring-atlas-search-indexes-v1"
  val UserNameCanonicalMigrationId = "user-name-canonical-v1"
  val UserAccountMigrationId = "user-account-management-v1"
  val UserAccountStatusMigrationId = "user-account-status-v1"
  val UserProfileOneOfMigrationId = "user-profile-one-of-v1"
  val UserEmailSparseIndexMigrationId = "user-email-canonical-sparse-v1"

  def initialize(database: MongoDatabase): IO[Unit] =
    initialize(database, None)

  def initialize(database: MongoDatabase, atlas: Option[AtlasSearchIndexConfig]): IO[Unit] =
    runHiringUserSetupMigration(database, atlas) *> runEmbeddingWorkMigration(database)

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

  private def runHiringUserSetupMigration(database: MongoDatabase, atlas: Option[AtlasSearchIndexConfig]): IO[Unit] = {
    val ledger = database.getCollection(HiringMigrationLedger)
    IO(UUID.randomUUID().toString).flatMap { owner =>
      for {
        now <- IO.realTimeInstant
        _ <- ensureLedgerRecord(ledger, HiringUserSetupMigrationId, HiringUserSetupDescriptor, HiringUserSetupChecksum, now)
        record <- PublisherBridge.first(ledger.find(Filters.eq("_id", HiringUserSetupMigrationId)))
          .flatMap(_.liftTo[IO](new IllegalStateException("hiring migration ledger record was not created")))
        _ <- validateLedgerRecord(record, HiringUserSetupMigrationId, HiringUserSetupDescriptor, HiringUserSetupChecksum)
        claimed <- claimMigration(ledger, HiringUserSetupMigrationId, HiringUserSetupChecksum, owner, now)
        current <- claimed.fold(PublisherBridge.first(ledger.find(Filters.eq("_id", HiringUserSetupMigrationId)))
          .flatMap(_.liftTo[IO](new IllegalStateException("hiring migration ledger record disappeared"))))(IO.pure)
        _ <- claimed match {
          case None if current.getString("status") == "Applied" =>
            verifyAppliedHiringUserSetup(database, atlas)
          case None => IO.raiseError(new IllegalStateException(
            s"hiring migration '$HiringUserSetupMigrationId' is already applying; wait for its lease to expire before recovery"
          ))
          case Some(_) =>
            ensureUsersCollection(database) *>
              migrateUsers(database.getCollection("users"), ledger, owner) *>
              replaceEmailIndex(database) *>
              (ordinaryIndexes(database) :+ ensureAccountRegistry(database)).sequence_ *>
              ensureUserProfileValidator(database) *>
              atlas.fold(IO.unit)(provisionAtlasIndexes(database, _)) *>
              verifyHiringUserSetup(database) *>
              markMigrationApplied(ledger, HiringUserSetupMigrationId, HiringUserSetupChecksum, owner)
        }
      } yield ()
    }
  }

  private def verifyAppliedHiringUserSetup(database: MongoDatabase, atlas: Option[AtlasSearchIndexConfig]): IO[Unit] =
    ensureUsersCollection(database) *>
      replaceEmailIndex(database) *>
      (ordinaryIndexes(database) :+ ensureAccountRegistry(database)).sequence_ *>
      ensureUserProfileValidator(database) *>
      verifyHiringUserSetup(database) *>
      atlas.fold(IO.unit)(provisionAtlasIndexes(database, _))

  private def runEmbeddingWorkMigration(database: MongoDatabase): IO[Unit] = {
    val ledger = database.getCollection(HiringMigrationLedger)
    IO(UUID.randomUUID().toString).flatMap { owner =>
      for {
        now <- IO.realTimeInstant
        _ <- ensureLedgerRecord(ledger, EmbeddingWorkMigrationId, EmbeddingWorkDescriptor, EmbeddingWorkChecksum, now)
        record <- PublisherBridge.first(ledger.find(Filters.eq("_id", EmbeddingWorkMigrationId)))
          .flatMap(_.liftTo[IO](new IllegalStateException("embedding work migration ledger record was not created")))
        _ <- validateLedgerRecord(record, EmbeddingWorkMigrationId, EmbeddingWorkDescriptor, EmbeddingWorkChecksum)
        claimed <- claimMigration(ledger, EmbeddingWorkMigrationId, EmbeddingWorkChecksum, owner, now)
        current <- claimed.fold(PublisherBridge.first(ledger.find(Filters.eq("_id", EmbeddingWorkMigrationId)))
          .flatMap(_.liftTo[IO](new IllegalStateException("embedding work migration ledger record disappeared"))))(IO.pure)
        _ <- claimed match {
          case None if current.getString("status") == "Applied" => verifyEmbeddingWorkIndexes(database)
          case None => IO.raiseError(new IllegalStateException(
            s"hiring migration '$EmbeddingWorkMigrationId' is already applying; wait for its lease to expire before recovery"
          ))
          case Some(_) =>
            ensureEmbeddingWorkIndexes(database) *> verifyEmbeddingWorkIndexes(database) *>
              markMigrationApplied(ledger, EmbeddingWorkMigrationId, EmbeddingWorkChecksum, owner)
        }
      } yield ()
    }
  }

  private def ensureLedgerRecord(
      ledger: com.mongodb.reactivestreams.client.MongoCollection[Document],
      id: String,
      descriptor: String,
      checksum: String,
      now: java.time.Instant
  ): IO[Unit] =
    PublisherBridge.first(ledger.updateOne(
      Filters.eq("_id", id),
      Updates.combine(
        Updates.setOnInsert("_id", id),
        Updates.setOnInsert("schemaVersion", 2),
        Updates.setOnInsert("descriptor", descriptor),
        Updates.setOnInsert("checksum", checksum),
        Updates.setOnInsert("status", "Pending"),
        Updates.setOnInsert("createdAt", Date.from(now))
      ),
      new UpdateOptions().upsert(true)
    )).void

  private def validateLedgerRecord(record: Document, id: String, descriptor: String, checksum: String): IO[Unit] =
    if (record.getString("checksum") == checksum && record.getString("descriptor") == descriptor)
      IO.unit
    else IO.raiseError(new IllegalStateException(
      s"hiring migration '$id' has an immutable descriptor or checksum mismatch"
    ))

  private def claimMigration(
      ledger: com.mongodb.reactivestreams.client.MongoCollection[Document],
      id: String,
      checksum: String,
      owner: String,
      now: java.time.Instant
  ): IO[Option[Document]] =
    PublisherBridge.first(ledger.findOneAndUpdate(
      Filters.and(
        Filters.eq("_id", id),
        Filters.eq("checksum", checksum),
        Filters.or(
          Filters.eq("status", "Pending"),
          Filters.and(Filters.eq("status", "Applying"), Filters.lte("leaseUntil", Date.from(now)))
        )
      ),
      Updates.combine(
        Updates.set("status", "Applying"),
        Updates.set("owner", owner),
        Updates.set("leaseUntil", Date.from(now.plusMillis(MigrationLease.toMillis))),
        Updates.set("startedAt", Date.from(now))
      ),
      new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER)
    ))

  private def persistCheckpoint(
      ledger: com.mongodb.reactivestreams.client.MongoCollection[Document],
      owner: String,
      lastProcessedId: String
  ): IO[Unit] =
    IO.realTimeInstant.flatMap { now =>
      PublisherBridge.first(ledger.updateOne(
        Filters.and(
          Filters.eq("_id", HiringUserSetupMigrationId),
          Filters.eq("checksum", HiringUserSetupChecksum),
          Filters.eq("status", "Applying"),
          Filters.eq("owner", owner)
        ),
        Updates.combine(
          Updates.set("lastProcessedId", lastProcessedId),
          Updates.set("leaseUntil", Date.from(now.plusMillis(MigrationLease.toMillis))),
          Updates.set("updatedAt", Date.from(now))
        )
      )).flatMap { result =>
        if (result.exists(_.getMatchedCount == 1)) IO.unit
        else IO.raiseError(new IllegalStateException(s"hiring migration '$HiringUserSetupMigrationId' lost its lease"))
      }
    }

  private def markMigrationApplied(
      ledger: com.mongodb.reactivestreams.client.MongoCollection[Document],
      id: String,
      checksum: String,
      owner: String
  ): IO[Unit] =
    IO.realTimeInstant.flatMap { now =>
      PublisherBridge.first(ledger.updateOne(
        Filters.and(
          Filters.eq("_id", id),
          Filters.eq("checksum", checksum),
          Filters.eq("status", "Applying"),
          Filters.eq("owner", owner)
        ),
        Updates.combine(
          Updates.set("status", "Applied"),
          Updates.set("appliedAt", Date.from(now)),
          Updates.unset("owner"),
          Updates.unset("leaseUntil")
        )
      )).flatMap { result =>
        if (result.exists(_.getMatchedCount == 1)) IO.unit
        else IO.raiseError(new IllegalStateException(s"hiring migration '$id' lost its lease before verification"))
      }
    }

  private def verifyHiringUserSetup(database: MongoDatabase): IO[Unit] = {
    val users = database.getCollection("users")
    val expectedKeys = new Document("emailCanonical", 1)
    PublisherBridge.all(users.listIndexes()).flatMap { indexes =>
      val names = indexes.map(_.getString("name")).toSet
      val required = Set(UsersEmailIndex, UsersNameIndex, UsersStatusCreatedIndex, UsersRoleStatusCreatedIndex,
        UsersAdminSingletonIndex, UsersEmbeddingMetaIndex)
      indexes.find(_.getString("name") == UsersEmailIndex) match {
        case Some(index) if Option(index.get("key", classOf[Document])).contains(expectedKeys) &&
            index.getBoolean("unique", false) && index.getBoolean("sparse", false) && required.subsetOf(names) => IO.unit
        case _ => IO.raiseError(new IllegalStateException("users security index verification failed"))
      }
    } *> verifyIndexes(database.getCollection("applications"), Set(
      ApplicationsCandidateJobIndex, ApplicationsCandidateStatusCreatedIndex, ApplicationsCandidateCreatedIndex,
      ApplicationsJobStatusCreatedIndex, ApplicationsJobCreatedIndex
    )) *> verifyIndexes(database.getCollection("jobs"), Set(
      JobsRecruiterStatusCreatedIndex, JobsRecruiterCreatedIndex, JobsCreatedIndex, JobsOpenCreatedIndex,
      JobsOpenCityCreatedIndex, JobsEmbeddingMetaIndex
    )) *> verifyIndexes(database.getCollection("application_events"), Set(ApplicationEventsApplicationCreatedIndex)) *>
      verifyAccountRegistry(database) *> verifyUserProfileValidator(database)
  }

  private def verifyIndexes(
      collection: com.mongodb.reactivestreams.client.MongoCollection[Document],
      required: Set[String]
  ): IO[Unit] =
    PublisherBridge.all(collection.listIndexes()).flatMap { indexes =>
      if (required.subsetOf(indexes.map(_.getString("name")).toSet)) IO.unit
      else IO.raiseError(new IllegalStateException(s"${collection.getNamespace.getCollectionName} index verification failed"))
    }

  private def verifyAccountRegistry(database: MongoDatabase): IO[Unit] =
    PublisherBridge.first(database.getCollection("account_registry").find(Filters.eq("_id", "user-account-registry"))).flatMap {
      case Some(record) if Set("Initialized", "Uninitialized").contains(record.getString("state")) => IO.unit
      case _ => IO.raiseError(new IllegalStateException("account registry verification failed"))
    }

  private def verifyUserProfileValidator(database: MongoDatabase): IO[Unit] =
    PublisherBridge.first(database.listCollections().filter(Filters.eq("name", "users")).first()).flatMap {
      case Some(collection) if Option(collection.get("options", classOf[Document]))
            .flatMap(options => Option(options.get("validator", classOf[Document]))).nonEmpty => IO.unit
      case _ => IO.raiseError(new IllegalStateException("users profile validator verification failed"))
    }

  private def ensureEmbeddingWorkIndexes(database: MongoDatabase): IO[Unit] =
    createIndex(
      database.getCollection("embedding_work"),
      Indexes.ascending("state", "availableAt", "leaseUntil"),
      new IndexOptions().name(EmbeddingWorkAvailableIndex)
    )

  private def verifyEmbeddingWorkIndexes(database: MongoDatabase): IO[Unit] =
    PublisherBridge.all(database.getCollection("embedding_work").listIndexes()).flatMap { indexes =>
      indexes.find(_.getString("name") == EmbeddingWorkAvailableIndex) match {
        case Some(index) if Option(index.get("key", classOf[Document])).contains(
              new Document("state", 1).append("availableAt", 1).append("leaseUntil", 1)
            ) => IO.unit
        case _ => IO.raiseError(new IllegalStateException("embedding work claim index verification failed"))
      }
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
        IO.raiseError(new IllegalStateException(
          s"Atlas search index '$name' has type '${existing.getString("type", "search")}', expected '$expectedType'; " +
            "create a new configured index name and perform an explicit cutover migration"
        ))
      case Some(_) =>
        IO.raiseError(new IllegalStateException(
          s"Atlas search index '$name' has an incompatible definition; " +
            "create a new configured index name and perform an explicit cutover migration"
        ))
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

  private def createIndex(
      collection: com.mongodb.reactivestreams.client.MongoCollection[Document],
      keys: org.bson.conversions.Bson,
      options: IndexOptions
  ): IO[Unit] =
    PublisherBridge.first(collection.createIndex(keys, options)).void.handleErrorWith {
      case error: MongoCommandException if options.getName == UsersNameIndex && error.getErrorCode == 11000 =>
        IO.raiseError(new IllegalStateException("users collection contains duplicate canonical account names", error))
      case error => IO.raiseError(error)
    }

  private def ensureUsersCollection(database: MongoDatabase): IO[Unit] =
    PublisherBridge.first(database.createCollection("users")).void.handleErrorWith {
      case error: MongoCommandException if error.getErrorCode == 48 => IO.unit
    }

  private def replaceEmailIndex(database: MongoDatabase): IO[Unit] = {
    val users = database.getCollection("users")
    val expectedKeys = new Document("emailCanonical", 1)
    PublisherBridge.all(users.listIndexes()).flatMap { indexes =>
      indexes.find(_.getString("name") == UsersEmailIndex) match {
        case Some(index) if Option(index.get("key", classOf[Document])).contains(expectedKeys) &&
            index.getBoolean("unique", false) && index.getBoolean("sparse", false) =>
          IO.unit
        case Some(_) =>
          IO.raiseError(new IllegalStateException(
            s"users index '$UsersEmailIndex' is incompatible; create a replacement index and perform an explicit cutover migration"
          ))
        case None =>
          createIndex(users, Indexes.ascending("emailCanonical"),
            new IndexOptions().name(UsersEmailIndex).unique(true).sparse(true))
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

  private def userMigrationBatch(
      users: com.mongodb.reactivestreams.client.MongoCollection[Document],
      after: Option[String]
  ): IO[List[Document]] =
    PublisherBridge.all(users.find(after.fold[org.bson.conversions.Bson](new Document())(Filters.gt("_id", _)))
      .sort(Sorts.ascending("_id"))
      .limit(UserMigrationBatchSize)
      .batchSize(UserMigrationBatchSize))

  private def migrateUsers(
      users: com.mongodb.reactivestreams.client.MongoCollection[Document],
      ledger: com.mongodb.reactivestreams.client.MongoCollection[Document],
      owner: String
  ): IO[Unit] = {
    def update(document: Document): IO[Unit] =
      userMigrationUpdates(document).flatMap {
        case Nil => IO.unit
        case updates => PublisherBridge.first(users.updateOne(
          Filters.eq("_id", document.get("_id")),
          Updates.combine(updates*)
        )).void
      }

    def migrateAfter(lastId: Option[String]): IO[Unit] =
      userMigrationBatch(users, lastId).flatMap { documents =>
        documents.traverse_(update) *>
          documents.lastOption.fold(IO.unit) { document =>
            persistCheckpoint(ledger, owner, document.getString("_id")) *>
              migrateAfter(Option(document.getString("_id")))
          }
      }
    PublisherBridge.first(ledger.find(Filters.eq("_id", HiringUserSetupMigrationId))).flatMap {
      case Some(record) => migrateAfter(Option(record.getString("lastProcessedId")))
      case None => IO.raiseError(new IllegalStateException("hiring migration ledger record disappeared"))
    }.handleErrorWith {
      case error: MongoCommandException if error.getErrorCode == 11000 =>
        IO.raiseError(new IllegalStateException("users collection contains duplicate canonical account names", error))
      case error => IO.raiseError(error)
    }
  }

  private def userMigrationUpdates(document: Document): IO[List[Bson]] =
    for {
      profileUpdates <- profileMigrationUpdates(document)
      nameUpdates <- IO.fromOption(Option(document.getString("name")).filter(_.trim.nonEmpty))(
        new IllegalStateException("users collection contains an account without a valid name")
      ).map { name =>
        Option.when(!document.containsKey("nameCanonical"))(Updates.set("nameCanonical", AccountName.canonical(name))).toList
      }
    } yield {
      val statusUpdates = Option.when(!document.containsKey("accountStatus"))(Updates.set("accountStatus", "Active")).toList
      nameUpdates ++ statusUpdates ++ profileUpdates
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

  private def sha256(value: String): String =
    MessageDigest.getInstance("SHA-256").digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8)).map("%02x".format(_)).mkString
}
