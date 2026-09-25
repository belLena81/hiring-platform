package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.UserRole
import com.mongodb.client.model.{
  Filters,
  IndexOptions,
  Indexes,
  SearchIndexModel,
  SearchIndexType,
  UpdateOptions,
  Updates
}
import com.mongodb.MongoCommandException
import com.mongodb.reactivestreams.client.{MongoCollection, MongoDatabase}
import org.bson.Document
import org.bson.BsonType
import org.bson.conversions.Bson
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

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
  private val CollectionLimit = 128
  private val RevisionMigrationId = "001_user_job_revisions"
  private val RevisionMigrationBatchSize = 500
  private val CandidateProfileMigrationId = "002_candidate_search_profile_verification"
  private val OutboxSubjectReferencesMigrationId = "003_event_outbox_subject_references"
  private val AnalyticsReportControlMigrationId = "004_analytics_report_control"
  private val AnalyticsDeletionReceiptMigrationId = "005_analytics_deletion_receipts"
  private val ownedCollections = Set(
    "users",
    "jobs",
    "applications",
    "application_events",
    "account_registry",
    "embedding_work",
    "event_outbox",
    "outbox_subject_fences",
    "search_sessions",
    "search_session_work",
    "consumer_receipts",
    "mutation_receipts",
    "event_quarantine",
    "analytics_erasure_requests",
    "analytics_erasure_completions",
    "analytics_erasure_delta_files",
    "analytics_worker_heartbeats",
    "analytics_report_snapshots",
    "analytics_report_control",
    "analytics_report_runs",
    "hiring_migration_ledger"
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

  def initialize(database: MongoDatabase): IO[Unit] = initialize(database, None, resetOnStart = false)
  def initialize(database: MongoDatabase, atlas: Option[AtlasSearchIndexConfig]): IO[Unit] =
    initialize(database, atlas, resetOnStart = false)
  def initialize(database: MongoDatabase, atlas: Option[AtlasSearchIndexConfig], resetOnStart: Boolean): IO[Unit] =
    Option.when(resetOnStart)(resetOwnedCollections(database)).getOrElse(IO.unit) *>
      migrateAggregateVersions(database) *> verifyCandidateSearchProfiles(database) *>
      migrateOutboxSubjectReferences(database) *> migrateAnalyticsReportControl(database) *>
      migrateAnalyticsDeletionReceipts(database) *>
      createAccountRegistry(database) *> createUserValidator(database) *>
      createJobValidator(database) *> createIndexes(database) *> createOutboxValidator(database) *>
      atlas.traverse_(provisionAtlasIndexes(database, _))

  private def createOutboxValidator(database: MongoDatabase): IO[Unit] = {
    val subjectIds = new Document("bsonType", "array")
      .append("minItems", 1)
      .append("uniqueItems", true)
      .append("items", new Document("bsonType", "string"))
    val version = new Document("bsonType", "int").append("enum", List(1).asJava)
    val schema = new Document("bsonType", "object")
      .append("required", List("subjectIds", "subjectRefsVersion").asJava)
      .append("properties", new Document("subjectIds", subjectIds).append("subjectRefsVersion", version))
    val command = new Document("collMod", "event_outbox")
      .append("validator", new Document("$jsonSchema", schema))
      .append("validationLevel", "strict")
      .append("validationAction", "error")
    PublisherBridge.first(database.runCommand(command)).void
  }

  private def migrateAggregateVersions(database: MongoDatabase): IO[Unit] = {
    val ledger = database.getCollection("hiring_migration_ledger")
    val migration = Filters.eq("_id", RevisionMigrationId)
    val state = PublisherBridge.first(ledger.find(migration)).map(_.map(_.getString("state")))
    val started = Updates.combine(
      Updates.setOnInsert("_id", RevisionMigrationId),
      Updates.set("version", 1L),
      Updates.set("state", "Running")
    )
    state.flatMap {
      case Some("Complete") => IO.unit
      case _                =>
        PublisherBridge
          .first(ledger.updateOne(migration, started, new UpdateOptions().upsert(true)))
          .void *> List(database.getCollection("users"), database.getCollection("jobs")).traverse_(backfillVersions) *>
          verifyAggregateVersions(database) *>
          PublisherBridge
            .first(
              ledger.updateOne(
                migration,
                Updates.combine(Updates.set("version", 1L), Updates.set("state", "Complete"))
              )
            )
            .void
    }
  }

  private def verifyCandidateSearchProfiles(database: MongoDatabase): IO[Unit] = {
    val ledger = database.getCollection("hiring_migration_ledger")
    val users = database.getCollection("users")
    val migration = Filters.eq("_id", CandidateProfileMigrationId)
    PublisherBridge.first(ledger.find(migration)).flatMap {
      case Some(document) if document.getString("state") == "Complete" => IO.unit
      case existing                                                    =>
        val checkpoint = existing.flatMap(value => Option(value.getString("lastId")))
        PublisherBridge
          .first(
            ledger.updateOne(
              migration,
              Updates.combine(
                Updates.setOnInsert("_id", CandidateProfileMigrationId),
                Updates.set("version", 1L),
                Updates.set("state", "Running")
              ),
              new UpdateOptions().upsert(true)
            )
          )
          .void *> verifyCandidateProfileBatches(users, ledger, migration, checkpoint)
    }
  }

  /** Backfills the internal subject index needed to fence/purge events without changing the Kafka envelope. */
  private def migrateOutboxSubjectReferences(database: MongoDatabase): IO[Unit] = {
    val ledger = database.getCollection("hiring_migration_ledger")
    val outbox = database.getCollection("event_outbox")
    val migration = Filters.eq("_id", OutboxSubjectReferencesMigrationId)
    val unverifiedSubjects = Filters.ne("subjectRefsVersion", 1)

    def verifyAndComplete: IO[Unit] = PublisherBridge.first(outbox.countDocuments(unverifiedSubjects)).flatMap {
      case Some(count) if count.longValue() == 0L =>
        PublisherBridge
          .first(
            ledger.updateOne(
              migration,
              Updates.combine(
                Updates.set("version", 1L),
                Updates.set("state", "Complete"),
                Updates.unset("lastId")
              )
            )
          )
          .void
      case Some(_) => backfillNextBatch
      case None => IO.raiseError(new IllegalStateException("Outbox subject-reference verification returned no count"))
    }

    def backfillNextBatch: IO[Unit] =
      PublisherBridge
        .collectWithin(
          outbox.find(unverifiedSubjects).sort(Indexes.ascending("_id")).limit(RevisionMigrationBatchSize),
          RevisionMigrationBatchSize
        )
        .flatMap { batch =>
          batch.traverse_(backfillOutboxSubjectReferences(database, outbox, ledger, migration, _)) *>
            (if (batch.size == RevisionMigrationBatchSize) backfillNextBatch else verifyAndComplete)
        }

    PublisherBridge.first(ledger.find(migration)).flatMap {
      case Some(document) if document.getString("state") == "Complete" =>
        PublisherBridge.first(outbox.countDocuments(unverifiedSubjects)).flatMap {
          case Some(count) if count.longValue() == 0L => IO.unit
          case Some(_)                                =>
            PublisherBridge
              .first(
                ledger.updateOne(
                  migration,
                  Updates.combine(Updates.set("state", "Running"), Updates.set("version", 1L))
                )
              )
              .void *> backfillNextBatch
          case None =>
            IO.raiseError(new IllegalStateException("Outbox subject-reference verification returned no count"))
        }
      case _ =>
        PublisherBridge
          .first(
            ledger.updateOne(
              migration,
              Updates.combine(
                Updates.setOnInsert("_id", OutboxSubjectReferencesMigrationId),
                Updates.set("version", 1L),
                Updates.set("state", "Running")
              ),
              new UpdateOptions().upsert(true)
            )
          )
          .void *> backfillNextBatch
    }
  }

  private def backfillOutboxSubjectReferences(
      database: MongoDatabase,
      outbox: MongoCollection[Document],
      ledger: MongoCollection[Document],
      migration: Bson,
      document: Document
  ): IO[Unit] = {
    val id = Option(document.getString("_id"))
    val event = MongoHiringCodecs.readOperationalEvent(document).toEither.leftMap(_ => "undecodable event")

    (id, event) match {
      case (Some(eventId), Right(value)) =>
        outboxCandidateIds(database, value).flatMap {
          case Left(reason) =>
            IO.raiseError(
              new IllegalStateException(
                s"Outbox subject-reference migration found $reason at event $eventId"
              )
            )
          case Right(candidates) =>
            val subjectIds = (value.actorId.value.toString :: candidates).distinct.sorted
            val values = subjectIds.asJava
            PublisherBridge
              .first(
                outbox.updateOne(
                  Filters.eq("_id", eventId),
                  Updates.combine(
                    Updates.set("subjectIds", values),
                    Updates.set("subjectRefsVersion", 1)
                  )
                )
              )
              .flatMap {
                case Some(result) if result.getMatchedCount == 1L =>
                  PublisherBridge.first(ledger.updateOne(migration, Updates.set("lastId", eventId))).void
                case Some(_) =>
                  IO.raiseError(
                    new IllegalStateException(s"Outbox subject-reference migration could not update event $eventId")
                  )
                case None =>
                  IO.raiseError(
                    new IllegalStateException("Outbox subject-reference migration returned no update result")
                  )
              }
        }
      case (Some(eventId), Left(reason)) =>
        IO.raiseError(new IllegalStateException(s"Outbox subject-reference migration found $reason at event $eventId"))
      case _ =>
        IO.raiseError(new IllegalStateException("Outbox subject-reference migration found an invalid event key"))
    }
  }

  /** Seeds a durable report generation and visibility record independently of the expiring snapshot payload. */
  private def migrateAnalyticsReportControl(database: MongoDatabase): IO[Unit] = {
    val ledger = database.getCollection("hiring_migration_ledger")
    val snapshots = database.getCollection("analytics_report_snapshots")
    val controls = database.getCollection("analytics_report_control")
    val migration = Filters.eq("_id", AnalyticsReportControlMigrationId)

    def verify: IO[Unit] =
      PublisherBridge.first(controls.find(Filters.eq("_id", "analytics-report"))).flatMap {
        case Some(document)
            if Option(document.get("generation", classOf[java.lang.Long])).isDefined &&
              Option(document.get("nextRevision", classOf[java.lang.Long])).isDefined &&
              Option(document.get("lastPublishedRevision", classOf[java.lang.Long])).isDefined &&
              Set("Unpublished", "Hidden", "Published").contains(document.getString("state")) =>
          IO.unit
        case _ => IO.raiseError(new IllegalStateException("Analytics report control migration verification failed"))
      }

    PublisherBridge.first(ledger.find(migration)).flatMap {
      case Some(document) if document.getString("state") == "Complete" => verify
      case _                                                           =>
        PublisherBridge
          .first(
            ledger.updateOne(
              migration,
              Updates.combine(
                Updates.setOnInsert("_id", AnalyticsReportControlMigrationId),
                Updates.set("version", 1L),
                Updates.set("state", "Running")
              ),
              new UpdateOptions().upsert(true)
            )
          )
          .void *> PublisherBridge.first(snapshots.find(Filters.eq("_id", "current"))).flatMap { legacy =>
          val visible = legacy.exists(snapshot =>
            snapshot.getString("state") == "Published" &&
              Option(snapshot.getDate("expiresAt")).exists(_.after(new java.util.Date()))
          )
          val addLegacyMetadata =
            PublisherBridge
              .first(
                snapshots.updateOne(
                  Filters.and(Filters.eq("_id", "current"), Filters.exists("generation", false)),
                  Updates.combine(
                    Updates.set("generation", 0L),
                    Updates.set("revision", 0L),
                    Updates.set("runId", "legacy")
                  )
                )
              )
              .void
          val initialControl = Updates.combine(
            Updates.setOnInsert("_id", "analytics-report"),
            Updates.setOnInsert("generation", 0L),
            Updates.setOnInsert("state", if (visible) "Published" else "Unpublished"),
            Updates.setOnInsert("nextRevision", 0L),
            Updates.setOnInsert("lastPublishedRevision", 0L),
            Updates.setOnInsert("lastRunId", if (visible) "legacy" else "")
          )
          addLegacyMetadata *> PublisherBridge
            .first(
              controls
                .updateOne(Filters.eq("_id", "analytics-report"), initialControl, new UpdateOptions().upsert(true))
            )
            .void *> verify *> PublisherBridge
            .first(
              ledger.updateOne(
                migration,
                Updates.combine(Updates.set("version", 1L), Updates.set("state", "Complete"))
              )
            )
            .void
        }
    }
  }

  private def outboxCandidateIds(
      database: MongoDatabase,
      event: com.example.graphQL.cats.shared.events.OperationalEventEnvelope
  ): IO[Either[String, List[String]]] = {
    import com.example.graphQL.cats.shared.events.{OperationalAggregateType, OperationalEventType}

    val expectedAggregate = event.eventType match {
      case OperationalEventType.JOB_CREATED | OperationalEventType.JOB_UPDATED | OperationalEventType.JOB_CLOSED =>
        OperationalAggregateType.Job
      case OperationalEventType.APPLICATION_CREATED | OperationalEventType.APPLICATION_STATUS_CHANGED |
          OperationalEventType.CANDIDATE_HIRED =>
        OperationalAggregateType.Application
      case OperationalEventType.JOB_VIEWED | OperationalEventType.SEARCH_PERFORMED |
          OperationalEventType.SEARCH_RESULT_CLICKED =>
        OperationalAggregateType.Search
    }

    if (event.aggregateType != expectedAggregate) IO.pure(Left("an event with an incompatible aggregate type"))
    else
      event.eventType match {
        case OperationalEventType.APPLICATION_CREATED | OperationalEventType.APPLICATION_STATUS_CHANGED |
            OperationalEventType.CANDIDATE_HIRED =>
          IO.pure(
            event.payload.hcursor
              .get[String]("candidateId")
              .toOption
              .flatMap(value => scala.util.Try(java.util.UUID.fromString(value)).toOption)
              .map(value => List(value.toString))
              .toRight("an application event without a valid candidateId")
          )
        case OperationalEventType.SEARCH_PERFORMED =>
          candidateIdsFromSearchPerformed(event.payload)
        case OperationalEventType.SEARCH_RESULT_CLICKED =>
          candidateIdFromSearchClick(database, event)
        case _ => IO.pure(Right(Nil))
      }
  }

  private def candidateIdsFromSearchPerformed(
      payload: io.circe.Json
  ): IO[Either[String, List[String]]] = {
    val cursor = payload.hcursor
    val searchKind = cursor.get[String]("searchKind").toOption
    val results = cursor.get[List[io.circe.Json]]("results").toOption
    val isCandidateSearch = searchKind.contains("candidateMatches")
    val isJobSearch = Set("semanticJobSearch", "recommendedJobs", "jobs").exists(searchKind.contains)
    if (!isCandidateSearch && !isJobSearch) IO.pure(Left("a search event with an unknown or missing searchKind"))
    else {
      val parsedResults = results
        .toRight("a search event without results")
        .flatMap(_.traverse { result =>
          result.hcursor
            .get[String]("resultId")
            .toOption
            .flatMap(value => scala.util.Try(java.util.UUID.fromString(value)).toOption)
            .map(_.toString)
            .toRight("a search event with an invalid resultId")
        })
      IO.pure(parsedResults.map(ids => if (isCandidateSearch) ids else Nil))
    }
  }

  private def candidateIdFromSearchClick(
      database: MongoDatabase,
      event: com.example.graphQL.cats.shared.events.OperationalEventEnvelope
  ): IO[Either[String, List[String]]] = {
    val payload = event.payload.hcursor
    val searchId = payload
      .get[String]("searchId")
      .toOption
      .flatMap(value => scala.util.Try(java.util.UUID.fromString(value)).toOption)
    val resultId = payload.get[String]("resultId").toOption
    (searchId, resultId) match {
      case (Some(id), Some(result)) =>
        PublisherBridge
          .first(database.getCollection("search_sessions").find(Filters.eq("_id", id.toString)))
          .map {
            case None           => Left("a search click without its retained search session")
            case Some(document) =>
              MongoHiringCodecs
                .readSearchSession(document)
                .toEither
                .leftMap(_ => "an undecodable search session")
                .flatMap {
                  case session if session.actorId != event.actorId => Left("a search click with a mismatched actor")
                  case session if !session.results.exists(_.resultId == result) =>
                    Left("a search click without a matching result")
                  case session if session.searchKind == "candidateMatches" =>
                    scala.util
                      .Try(java.util.UUID.fromString(result))
                      .toEither
                      .leftMap(_ => "a candidate search click with an invalid resultId")
                      .map(value => List(value.toString))
                  case session if Set("semanticJobSearch", "recommendedJobs", "jobs").contains(session.searchKind) =>
                    Right(Nil)
                  case _ => Left("a search click with an unknown searchKind")
                }
          }
          .handleError(_ => Left("an unavailable retained search session"))
      case _ => IO.pure(Left("a search click without valid searchId or resultId"))
    }
  }

  private def verifyCandidateProfileBatches(
      users: MongoCollection[Document],
      ledger: MongoCollection[Document],
      migration: Bson,
      checkpoint: Option[String]
  ): IO[Unit] = {
    val cursor = checkpoint.fold(Filters.empty())(id => Filters.gt("_id", id))
    PublisherBridge
      .collectWithin(
        users.find(cursor).sort(Indexes.ascending("_id")).limit(RevisionMigrationBatchSize),
        RevisionMigrationBatchSize
      )
      .flatMap { batch =>
        batch.traverse_(user => validateCandidateSearchProfile(user) *> ensureCanonicalCandidateSkills(users, user)) *>
          batch.lastOption.traverse_ { last =>
            Option(last.getString("_id")).fold(
              IO.raiseError[Unit](new IllegalStateException("Invalid user migration key"))
            )(id => PublisherBridge.first(ledger.updateOne(migration, Updates.set("lastId", id))).void)
          } *> (if (batch.size == RevisionMigrationBatchSize)
                  verifyCandidateProfileBatches(users, ledger, migration, batch.lastOption.map(_.getString("_id")))
                else
                  PublisherBridge
                    .first(
                      ledger.updateOne(
                        migration,
                        Updates.combine(Updates.set("state", "Complete"), Updates.unset("lastId"))
                      )
                    )
                    .void)
      }
  }

  private def validateCandidateSearchProfile(user: Document): IO[Unit] = {
    val profile = user.get("profile") match {
      case value: Document => Some(value)
      case _               => None
    }
    def invalid(field: String): IO[Unit] =
      IO.raiseError(new IllegalStateException(s"Candidate profile migration found malformed $field"))
    def optionalFieldValid(document: Document, field: String)(valid: Any => Boolean): Boolean =
      !document.containsKey(field) || valid(document.get(field))
    def canonical(value: String): String = value.trim.toLowerCase(java.util.Locale.ROOT)
    def validResidence(value: Any): Boolean = value match {
      case residence: Document =>
        val country = Option(residence.get("country")).collect { case text: String => text }
        val countryCanonical = Option(residence.get("countryCanonical")).collect { case text: String => text }
        val city = Option(residence.get("city")).collect { case text: String => text }
        val cityCanonical = Option(residence.get("cityCanonical")).collect { case text: String => text }
        def validText(text: String): Boolean = text.trim.nonEmpty && text.length <= 256
        country.exists(validText) && countryCanonical.contains(country.map(canonical).getOrElse("")) &&
        optionalFieldValid(residence, "city")(_ => city.exists(validText)) &&
        optionalFieldValid(residence, "cityCanonical")(_ =>
          city.exists(value => cityCanonical.contains(canonical(value)))
        )
      case _ => false
    }
    val residenceValid = profile.forall(value => optionalFieldValid(value, "currentResidence")(validResidence))
    val availabilityValid = profile.forall(value =>
      optionalFieldValid(value, "availabilityStatus") {
        case status: String => Set("AVAILABLE_NOW", "UNAVAILABLE").contains(status)
        case _              => false
      }
    )
    val consentValid = profile.forall(value =>
      optionalFieldValid(value, "recruiterSearchOptIn") {
        case _: java.lang.Boolean => true
        case _                    => false
      }
    )
    if (!residenceValid) invalid("currentResidence")
    else if (!availabilityValid) invalid("availabilityStatus")
    else if (!consentValid) invalid("recruiterSearchOptIn")
    else IO.unit
  }

  private def ensureCanonicalCandidateSkills(users: MongoCollection[Document], user: Document): IO[Unit] = {
    val isCandidate = user.getString("role") == "Candidate"
    val profile = user.get("profile") match {
      case value: Document => Some(value)
      case _               => None
    }
    val skills = profile.flatMap(value => Option(value.get("skills"))) match {
      case Some(values: java.util.List[?]) => values.asScala.toList.collect { case value: String => value }
      case _                               => Nil
    }
    if (!isCandidate) IO.unit
    else updateCanonicalSkills(users, user, skills, retries = 3)
  }

  private def updateCanonicalSkills(
      users: MongoCollection[Document],
      user: Document,
      skills: List[String],
      retries: Int
  ): IO[Unit] = {
    val canonical = skills.map(_.trim.toLowerCase(java.util.Locale.ROOT)).sorted.asJava
    val id = user.getString("_id")
    val version = user.getLong("version")
    val expectedSkills = skills.asJava
    val unchanged = Filters.and(
      Filters.eq("_id", id),
      Filters.eq("version", version),
      Filters.eq("role", UserRole.Candidate.toString),
      Filters.eq("profile.skills", expectedSkills)
    )
    PublisherBridge.first(users.updateOne(unchanged, Updates.set("profile.skillsCanonical", canonical))).flatMap {
      case Some(result) if result.getMatchedCount == 1L => IO.unit
      case Some(_) if retries > 0                       =>
        PublisherBridge.first(users.find(Filters.eq("_id", id))).flatMap {
          case Some(latest) =>
            val latestProfile = latest.get("profile") match {
              case profile: Document => Some(profile)
              case _                 => None
            }
            val latestSkills = latestProfile.flatMap(value => Option(value.get("skills"))) match {
              case Some(values: java.util.List[?]) => values.asScala.toList.collect { case value: String => value }
              case _                               => Nil
            }
            val sidecar = latestProfile.flatMap(value => Option(value.get("skillsCanonical")))
            if (sidecar.contains(canonical)) IO.unit
            else updateCanonicalSkills(users, latest, latestSkills, retries - 1)
          case None => IO.unit
        }
      case Some(_) => IO.raiseError(new IllegalStateException("Candidate skill migration raced with profile updates"))
      case None    => IO.raiseError(new IllegalStateException("Candidate skill migration returned no update result"))
    }
  }

  private def backfillVersions(collection: MongoCollection[Document]): IO[Unit] = {
    val missingVersion = Filters.exists("version", false)
    def nextBatch: IO[Unit] =
      PublisherBridge
        .collectWithin(
          collection.find(missingVersion).sort(Indexes.ascending("_id")).limit(RevisionMigrationBatchSize),
          RevisionMigrationBatchSize
        )
        .flatMap { documents =>
          val ids = documents.flatMap(document => Option(document.getString("_id")))
          if (documents.isEmpty) IO.unit
          else if (ids.size != documents.size)
            IO.raiseError(new IllegalStateException("Mongo revision backfill found a document without a string _id"))
          else backfillVersionBatch(collection, ids) *> nextBatch
        }
    nextBatch
  }

  private[mongo] def backfillVersionBatch(collection: MongoCollection[Document], ids: List[String]): IO[Unit] =
    PublisherBridge
      .first(
        collection.updateMany(
          Filters.and(Filters.in("_id", ids*), Filters.exists("version", false)),
          Updates.set("version", Long.box(0L))
        )
      )
      .flatMap {
        // Another startup may have migrated some of these documents first. The missing-version
        // predicate keeps a concurrent aggregate write from being reset to revision zero.
        case Some(result) if result.getMatchedCount <= ids.size.toLong => IO.unit
        case Some(_)                                                   =>
          IO.raiseError(new IllegalStateException("Mongo revision backfill updated an unexpected row count"))
        case None => IO.raiseError(new IllegalStateException("Mongo revision backfill returned no update result"))
      }

  private def verifyAggregateVersions(database: MongoDatabase): IO[Unit] =
    List(database.getCollection("users"), database.getCollection("jobs")).traverse_ { collection =>
      val invalidVersion = Filters.or(
        Filters.exists("version", false),
        Filters.lt("version", 0L),
        Filters.not(Filters.`type`("version", BsonType.INT64))
      )
      PublisherBridge.first(collection.countDocuments(invalidVersion)).flatMap {
        case Some(count) if count.longValue() == 0L => IO.unit
        case Some(_) => IO.raiseError(new IllegalStateException("Mongo contains an invalid aggregate revision"))
        case None    => IO.raiseError(new IllegalStateException("Mongo revision verification returned no count"))
      }
    }

  private def resetOwnedCollections(database: MongoDatabase): IO[Unit] =
    PublisherBridge.collectWithin(database.listCollectionNames(), CollectionLimit).flatMap { names =>
      names.filter(ownedCollections).traverse_(name => PublisherBridge.first(database.getCollection(name).drop()).void)
    }

  private def createAccountRegistry(database: MongoDatabase): IO[Unit] =
    PublisherBridge
      .first(
        database
          .getCollection("account_registry")
          .updateOne(
            Filters.eq("_id", "user-account-registry"),
            Updates.combine(
              Updates.setOnInsert("_id", "user-account-registry"),
              Updates.setOnInsert("state", "Uninitialized")
            ),
            new UpdateOptions().upsert(true)
          )
      )
      .void

  private def migrateAnalyticsDeletionReceipts(database: MongoDatabase): IO[Unit] = {
    val ledger = database.getCollection("hiring_migration_ledger")
    val migration = Filters.eq("_id", AnalyticsDeletionReceiptMigrationId)
    PublisherBridge.first(ledger.find(migration)).flatMap {
      case Some(document) if document.getString("state") == "Complete" => IO.unit
      case _ =>
        val started = Updates.combine(
          Updates.setOnInsert("_id", AnalyticsDeletionReceiptMigrationId),
          Updates.set("version", 1L),
          Updates.set("state", "Running")
        )
        PublisherBridge.first(ledger.updateOne(migration, started, new UpdateOptions().upsert(true))).void *>
          List(
            index(
              database.getCollection("analytics_erasure_requests"),
              Indexes.ascending("receiptId"),
              new IndexOptions().name("analytics_erasure_request_receipt_unique").unique(true)
                .partialFilterExpression(Filters.exists("receiptId", true))
            ),
            index(
              database.getCollection("analytics_erasure_completions"),
              Indexes.ascending("receiptId"),
              new IndexOptions().name("analytics_erasure_completion_receipt_unique").unique(true)
                .partialFilterExpression(Filters.exists("receiptId", true))
            )
          ).sequence_.void *>
          PublisherBridge.first(ledger.updateOne(migration, Updates.set("state", "Complete"))).void
    }
  }

  private def createIndexes(database: MongoDatabase): IO[Unit] = List(
    index(
      database.getCollection("users"),
      Indexes.ascending("emailCanonical"),
      new IndexOptions().name(UsersEmailIndex).unique(true).sparse(true)
    ),
    index(
      database.getCollection("users"),
      Indexes.ascending("nameCanonical"),
      new IndexOptions().name(UsersNameIndex).unique(true)
    ),
    index(
      database.getCollection("users"),
      Indexes.compoundIndex(Indexes.ascending("accountStatus"), Indexes.descending("createdAt", "_id")),
      new IndexOptions().name(UsersStatusCreatedIndex)
    ),
    index(
      database.getCollection("users"),
      Indexes.compoundIndex(Indexes.ascending("role", "accountStatus"), Indexes.descending("createdAt", "_id")),
      new IndexOptions().name(UsersRoleStatusCreatedIndex)
    ),
    index(
      database.getCollection("users"),
      Indexes.ascending("adminSingletonKey"),
      new IndexOptions()
        .name(UsersAdminSingletonIndex)
        .unique(true)
        .partialFilterExpression(Filters.eq("role", "Admin"))
    ),
    index(
      database.getCollection("users"),
      Indexes.ascending("embeddingMeta.model", "role"),
      new IndexOptions().name(UsersEmbeddingMetaIndex)
    ),
    index(
      database.getCollection("jobs"),
      Indexes.compoundIndex(Indexes.ascending("recruiterId", "status"), Indexes.descending("createdAt", "_id")),
      new IndexOptions().name(JobsRecruiterStatusCreatedIndex)
    ),
    index(
      database.getCollection("jobs"),
      Indexes.compoundIndex(Indexes.ascending("recruiterId"), Indexes.descending("createdAt", "_id")),
      new IndexOptions().name(JobsRecruiterCreatedIndex)
    ),
    index(
      database.getCollection("jobs"),
      Indexes.descending("createdAt", "_id"),
      new IndexOptions().name(JobsCreatedIndex)
    ),
    index(
      database.getCollection("jobs"),
      Indexes.compoundIndex(Indexes.ascending("status"), Indexes.descending("createdAt", "_id")),
      new IndexOptions().name(JobsOpenCreatedIndex)
    ),
    index(
      database.getCollection("jobs"),
      Indexes.compoundIndex(Indexes.ascending("status", "location.city"), Indexes.descending("createdAt", "_id")),
      new IndexOptions().name(JobsOpenCityCreatedIndex)
    ),
    index(
      database.getCollection("jobs"),
      Indexes.ascending("embeddingMeta.model", "status", "location.city", "recruiterId"),
      new IndexOptions().name(JobsEmbeddingMetaIndex)
    ),
    index(
      database.getCollection("applications"),
      Indexes.ascending("candidateId", "jobId"),
      new IndexOptions().name(ApplicationsCandidateJobIndex).unique(true)
    ),
    index(
      database.getCollection("applications"),
      Indexes.compoundIndex(Indexes.ascending("candidateId", "status"), Indexes.descending("createdAt", "_id")),
      new IndexOptions().name(ApplicationsCandidateStatusCreatedIndex)
    ),
    index(
      database.getCollection("applications"),
      Indexes.compoundIndex(Indexes.ascending("candidateId"), Indexes.descending("createdAt", "_id")),
      new IndexOptions().name(ApplicationsCandidateCreatedIndex)
    ),
    index(
      database.getCollection("applications"),
      Indexes.compoundIndex(Indexes.ascending("jobId", "status"), Indexes.descending("createdAt", "_id")),
      new IndexOptions().name(ApplicationsJobStatusCreatedIndex)
    ),
    index(
      database.getCollection("applications"),
      Indexes.compoundIndex(Indexes.ascending("jobId"), Indexes.descending("createdAt", "_id")),
      new IndexOptions().name(ApplicationsJobCreatedIndex)
    ),
    index(
      database.getCollection("application_events"),
      Indexes.compoundIndex(Indexes.ascending("applicationId"), Indexes.descending("occurredAt", "_id")),
      new IndexOptions().name(ApplicationEventsApplicationCreatedIndex)
    ),
    index(
      database.getCollection("embedding_work"),
      Indexes.ascending("state", "availableAt", "leaseUntil"),
      new IndexOptions().name(EmbeddingWorkAvailableIndex)
    ),
    index(
      database.getCollection("event_outbox"),
      Indexes.ascending("state", "availableAt", "leaseUntil", "occurredAt", "_id"),
      new IndexOptions().name(EventOutboxClaimIndex)
    ),
    index(
      database.getCollection("event_outbox"),
      Indexes.ascending("retentionExpiresAt"),
      new IndexOptions()
        .name(EventOutboxPublishedRetentionIndex)
        .expireAfter(0L, TimeUnit.SECONDS)
        .partialFilterExpression(Filters.eq("state", "Published"))
    ),
    index(
      database.getCollection("event_outbox"),
      Indexes.ascending("subjectIds"),
      new IndexOptions().name(EventOutboxSubjectIdsIndex)
    ),
    index(
      database.getCollection("outbox_subject_fences"),
      Indexes.ascending("deleted", "leaseUntil"),
      new IndexOptions().name(OutboxSubjectFenceLeaseIndex)
    ),
    index(
      database.getCollection("search_sessions"),
      Indexes.compoundIndex(Indexes.ascending("actorId"), Indexes.descending("occurredAt", "_id")),
      new IndexOptions().name(SearchSessionsActorIndex)
    ),
    index(
      database.getCollection("search_sessions"),
      Indexes.ascending("expiresAt"),
      new IndexOptions().name(SearchSessionsExpiryIndex).expireAfter(0L, TimeUnit.SECONDS)
    ),
    index(
      database.getCollection("search_session_work"),
      Indexes.ascending("state", "availableAt", "leaseUntil", "createdAt"),
      new IndexOptions().name(SearchSessionWorkClaimIndex)
    ),
    index(
      database.getCollection("search_session_work"),
      Indexes.ascending("retentionExpiresAt"),
      new IndexOptions()
        .name(SearchSessionWorkRetentionIndex)
        .expireAfter(0L, TimeUnit.SECONDS)
        .partialFilterExpression(Filters.eq("state", "Failed"))
    ),
    index(
      database.getCollection("consumer_receipts"),
      Indexes.ascending("consumerGroup", "eventId"),
      new IndexOptions().name(ConsumerReceiptsIdIndex).unique(true)
    ),
    index(
      database.getCollection("consumer_receipts"),
      Indexes.ascending("expiresAt"),
      new IndexOptions().name(ConsumerReceiptsExpiryIndex).expireAfter(0L, TimeUnit.SECONDS)
    ),
    index(
      database.getCollection("mutation_receipts"),
      Indexes.ascending("operation", "actorScope", "idempotencyKey"),
      new IndexOptions().name(MutationReceiptsKeyIndex).unique(true)
    ),
    index(
      database.getCollection("mutation_receipts"),
      Indexes.ascending("expiresAt"),
      new IndexOptions().name(MutationReceiptsExpiryIndex).expireAfter(0L, TimeUnit.SECONDS)
    ),
    index(
      database.getCollection("event_quarantine"),
      Indexes.ascending("topic", "partition", "offset"),
      new IndexOptions().name(EventQuarantineOffsetIndex).unique(true)
    ),
    index(
      database.getCollection("event_quarantine"),
      Indexes.ascending("expiresAt"),
      new IndexOptions().name(EventQuarantineExpiryIndex).expireAfter(0L, TimeUnit.SECONDS)
    ),
    index(
      database.getCollection("analytics_erasure_requests"),
      Indexes.ascending("state", "requestedAt"),
      new IndexOptions().name(AnalyticsErasureRequestStateIndex)
    ),
    index(
      database.getCollection("analytics_erasure_requests"),
      Indexes.ascending("receiptId"),
      new IndexOptions().name("analytics_erasure_request_receipt_unique").unique(true)
        .partialFilterExpression(Filters.exists("receiptId", true))
    ),
    index(
      database.getCollection("analytics_erasure_completions"),
      Indexes.ascending("receiptId"),
      new IndexOptions().name("analytics_erasure_completion_receipt_unique").unique(true)
        .partialFilterExpression(Filters.exists("receiptId", true))
    ),
    index(
      database.getCollection("analytics_erasure_requests"),
      Indexes.ascending("expiresAt"),
      new IndexOptions().name("analytics_erasure_request_expiry").expireAfter(0L, TimeUnit.SECONDS)
    ),
    index(
      database.getCollection("analytics_worker_heartbeats"),
      Indexes.ascending("leaseUntil"),
      new IndexOptions().name("analytics_worker_heartbeat_expiry").expireAfter(0L, TimeUnit.SECONDS)
    ),
    index(
      database.getCollection("analytics_report_snapshots"),
      Indexes.compoundIndex(Indexes.ascending("state"), Indexes.descending("asOf")),
      new IndexOptions().name(AnalyticsReportPublishedIndex)
    ),
    index(
      database.getCollection("analytics_report_snapshots"),
      Indexes.ascending("expiresAt"),
      new IndexOptions().name(AnalyticsReportExpiryIndex).expireAfter(0L, TimeUnit.SECONDS)
    ),
    index(
      database.getCollection("analytics_report_runs"),
      Indexes.ascending("expiresAt"),
      new IndexOptions().name(AnalyticsReportRunExpiryIndex).expireAfter(0L, TimeUnit.SECONDS)
    )
  ).sequence_.void

  private def createUserValidator(database: MongoDatabase): IO[Unit] = {
    def active(role: String, required: String) = new Document("required", List(required).asJava).append(
      "properties",
      new Document()
        .append("role", new Document("enum", List(role).asJava))
        .append("accountStatus", new Document("enum", List("Active").asJava))
    )
    val candidateProfile = new Document("bsonType", "object").append(
      "properties",
      new Document()
        .append("recruiterSearchOptIn", new Document("bsonType", "bool"))
        .append("availabilityStatus", new Document("enum", List("AVAILABLE_NOW", "UNAVAILABLE").asJava))
        .append(
          "currentResidence",
          new Document("bsonType", "object")
            .append("required", List("country", "countryCanonical").asJava)
            .append(
              "properties",
              new Document()
                .append("country", new Document("bsonType", "string").append("minLength", 1).append("maxLength", 256))
                .append("city", new Document("bsonType", "string").append("minLength", 1).append("maxLength", 256))
                .append(
                  "countryCanonical",
                  new Document("bsonType", "string").append("minLength", 1).append("maxLength", 256)
                )
                .append(
                  "cityCanonical",
                  new Document("bsonType", "string").append("minLength", 1).append("maxLength", 256)
                )
            )
        )
    )
    val candidate = active("Candidate", "profile")
    candidate.get("properties", classOf[Document]).append("profile", candidateProfile)
    val admin = active("Admin", "adminSingletonKey")
    admin
      .get("properties", classOf[Document])
      .append("adminSingletonKey", new Document("enum", List("singleton-admin").asJava))
    val schema = new Document(
      "$jsonSchema",
      new Document("bsonType", "object")
        .append("required", List("role", "accountStatus", "version").asJava)
        .append("properties", new Document("version", new Document("bsonType", "long").append("minimum", 0L)))
        .append(
          "oneOf",
          List(
            candidate,
            active("Recruiter", "profile"),
            admin,
            new Document("properties", new Document("accountStatus", new Document("enum", List("Deleted").asJava)))
          ).asJava
        )
    )
    PublisherBridge.first(database.createCollection("users")).void.recoverWith {
      case error: MongoCommandException if error.getErrorCode == 48 => IO.unit
    } *> PublisherBridge
      .first(
        database.runCommand(
          new Document("collMod", "users")
            .append("validator", schema)
            .append("validationLevel", "strict")
            .append("validationAction", "error")
        )
      )
      .void
  }

  private def createJobValidator(database: MongoDatabase): IO[Unit] = {
    val schema = new Document(
      "$jsonSchema",
      new Document("bsonType", "object")
        .append("required", List("version").asJava)
        .append("properties", new Document("version", new Document("bsonType", "long").append("minimum", 0L)))
    )
    PublisherBridge.first(database.createCollection("jobs")).void.recoverWith {
      case error: MongoCommandException if error.getErrorCode == 48 => IO.unit
    } *> PublisherBridge
      .first(
        database.runCommand(
          new Document("collMod", "jobs")
            .append("validator", schema)
            .append("validationLevel", "strict")
            .append("validationAction", "error")
        )
      )
      .void
  }

  private def index(collection: MongoCollection[Document], keys: Bson, options: IndexOptions): IO[Unit] =
    PublisherBridge.first(collection.createIndex(keys, options)).void

  private def provisionAtlasIndexes(database: MongoDatabase, config: AtlasSearchIndexConfig): IO[Unit] = {
    def vectorDefinition(filters: List[String]): Document = new Document(
      "fields",
      (new Document("type", "vector")
        .append("path", "embedding")
        .append("numDimensions", Int.box(config.dimension))
        .append("similarity", "cosine") ::
        filters.map(path => new Document("type", "filter").append("path", path))).asJava
    )
    val lexicalDefinition = new Document(
      "mappings",
      new Document("dynamic", false).append(
        "fields",
        new Document()
          .append("title", new Document("type", "string"))
          .append("description", new Document("type", "string"))
          .append("requirements", new Document("type", "string"))
          .append("skills", new Document("type", "string"))
      )
    )
    def create(collection: MongoCollection[Document], model: SearchIndexModel): IO[Unit] =
      PublisherBridge.first(collection.createSearchIndexes(List(model).asJava)).void
    List(
      create(
        database.getCollection("jobs"),
        new SearchIndexModel(
          config.jobVectorIndex,
          vectorDefinition(
            List("status", "location.city", "skills", "createdAt", "recruiterId", "embeddingMeta.model")
          ),
          SearchIndexType.vectorSearch()
        )
      ),
      create(
        database.getCollection("users"),
        new SearchIndexModel(
          config.candidateVectorIndex,
          vectorDefinition(
            List(
              "role",
              "accountStatus",
              "embeddingMeta.model",
              "profile.skillsCanonical",
              "profile.recruiterSearchOptIn",
              "profile.currentResidence.countryCanonical",
              "profile.currentResidence.cityCanonical",
              "profile.availabilityStatus"
            )
          ),
          SearchIndexType.vectorSearch()
        )
      ),
      create(
        database.getCollection("users"),
        new SearchIndexModel(
          config.candidateLexicalIndex,
          new Document(
            "mappings",
            new Document("dynamic", false).append(
              "fields",
              new Document(
                "profile",
                new Document("type", "document").append(
                  "fields",
                  new Document()
                    .append("skills", new Document("type", "string"))
                    .append("experienceSummary", new Document("type", "string"))
                )
              )
            )
          ),
          SearchIndexType.search()
        )
      ),
      create(
        database.getCollection("jobs"),
        new SearchIndexModel(config.jobLexicalIndex, lexicalDefinition, SearchIndexType.search())
      )
    ).sequence_.void *> awaitCandidateSearchIndexes(database, config)
  }

  private def awaitCandidateSearchIndexes(database: MongoDatabase, config: AtlasSearchIndexConfig): IO[Unit] = {
    val candidateVectorFilters = Set(
      "role",
      "accountStatus",
      "embeddingMeta.model",
      "profile.skillsCanonical",
      "profile.recruiterSearchOptIn",
      "profile.currentResidence.countryCanonical",
      "profile.currentResidence.cityCanonical",
      "profile.availabilityStatus"
    )
    val deadline = IO.monotonic.map(_ + config.readyTimeoutMillis.millis)
    def indexDocuments(collection: MongoCollection[Document]): IO[List[Document]] =
      PublisherBridge.collectWithin(
        collection.aggregate(List(new Document("$listSearchIndexes", new Document())).asJava),
        100
      )
    def validVector(index: Document): Boolean = {
      val definition = Option(index.get("latestDefinition", classOf[Document]))
        .orElse(Option(index.get("definition", classOf[Document])))
      val fields = definition
        .flatMap(value => Option(value.get("fields", classOf[java.util.List[Document]])))
        .toList
        .flatMap(_.asScala)
      val filters =
        fields.filter(_.getString("type") == "filter").flatMap(field => Option(field.getString("path"))).toSet
      val vector = fields.filter(_.getString("type") == "vector")
      candidateVectorFilters.subsetOf(filters) && vector.size == 1 && vector.headOption.exists(field =>
        field.getString("path") == "embedding" &&
          Option(field.getInteger("numDimensions")).contains(config.dimension) &&
          field.getString("similarity") == "cosine"
      )
    }
    def validLexical(index: Document): Boolean = {
      val definition = Option(index.get("latestDefinition", classOf[Document]))
        .orElse(Option(index.get("definition", classOf[Document])))
      val mappings = definition.flatMap(value => Option(value.get("mappings", classOf[Document])))
      val profile = mappings.flatMap(value =>
        Option(value.get("fields", classOf[Document])).flatMap(fields =>
          Option(fields.get("profile", classOf[Document]))
        )
      )
      val profileFields = profile.flatMap(value => Option(value.get("fields", classOf[Document])))
      def stringField(fields: Document, name: String): Boolean =
        Option(fields.get(name, classOf[Document])).exists(_.getString("type") == "string")
      mappings.exists(_.getBoolean("dynamic", true) == false) &&
      profile.exists(_.getString("type") == "document") &&
      profileFields.exists(fields => stringField(fields, "skills") && stringField(fields, "experienceSummary"))
    }
    def await(name: String, collection: MongoCollection[Document], validate: Document => Boolean): IO[Unit] =
      deadline.flatMap { until =>
        def poll: IO[Unit] = indexDocuments(collection).flatMap { indexes =>
          indexes.find(_.getString("name") == name) match {
            case Some(index) if !validate(index) =>
              IO.raiseError(new IllegalStateException(s"Atlas Search index '$name' has an incompatible definition"))
            case Some(index) if index.getBoolean("queryable", java.lang.Boolean.FALSE).booleanValue() => IO.unit
            case _                                                                                    =>
              IO.monotonic.flatMap(now =>
                if (now >= until)
                  IO.raiseError(new IllegalStateException(s"Atlas Search index '$name' is not queryable"))
                else IO.sleep(config.pollIntervalMillis.millis) *> poll
              )
          }
        }
        poll
      }
    await(config.candidateVectorIndex, database.getCollection("users"), validVector) *>
      await(config.candidateLexicalIndex, database.getCollection("users"), validLexical)
  }
}
