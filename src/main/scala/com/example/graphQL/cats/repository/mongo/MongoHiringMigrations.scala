package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import io.circe.Json
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogFields}
import com.example.graphQL.cats.service.Diagnostics.*
import com.example.graphQL.cats.shared.*
import com.mongodb.*
import com.mongodb.client.model.*
import org.bson.*
import org.bson.conversions.*
import scala.jdk.CollectionConverters.*

private[mongo] object MongoHiringMigrations {
  import MongoHiringSetup.{SetupCollection, SetupDatabase}

  private val RevisionMigrationId = "001_user_job_revisions"
  private val RevisionMigrationBatchSize = 500
  private val CandidateProfileMigrationId = "002_candidate_search_profile_verification"
  private val OutboxSubjectReferencesMigrationId = "003_event_outbox_subject_references"
  private val AnalyticsReportControlMigrationId = "004_analytics_report_control"
  private val AnalyticsDeletionReceiptMigrationId = "005_analytics_deletion_receipts"

  val ownedCollections: Set[String] = Set(
    MongoCollections.Users,
    MongoCollections.Jobs,
    MongoCollections.Applications,
    MongoCollections.ApplicationEvents,
    MongoCollections.AccountRegistry,
    MongoCollections.EmbeddingWork,
    MongoCollections.EventOutbox,
    MongoCollections.OutboxSubjectFences,
    MongoCollections.SearchSessions,
    MongoCollections.SearchSessionWork,
    MongoCollections.ConsumerReceipts,
    MongoCollections.MutationReceipts,
    MongoCollections.EventQuarantine,
    MongoCollections.AnalyticsErasureRequests,
    MongoCollections.AnalyticsErasureCompletions,
    MongoCollections.AnalyticsErasureDeltaFiles,
    MongoCollections.AnalyticsWorkerHeartbeats,
    MongoCollections.AnalyticsReportSnapshots,
    MongoCollections.AnalyticsReportControl,
    MongoCollections.AnalyticsReportRuns,
    MongoCollections.HiringMigrationLedger
  )

  private final case class SetupCollectionLimitExceeded(maximum: Int)
      extends RuntimeException(s"setup read exceeded its maximum of $maximum elements")

  private def collectWithin[A](values: fs2.Stream[IO, A], maximum: Int): IO[List[A]] =
    values.take(maximum.toLong + 1L).compile.toList.flatMap { found =>
      if (found.size > maximum) IO.raiseError(SetupCollectionLimitExceeded(maximum))
      else IO.pure(found)
    }

  private def countDocuments(collection: SetupCollection, filter: Bson): IO[Long] =
    collection.count(filter, new CountOptions())

  private def index(collection: SetupCollection, keys: Bson, options: IndexOptions): IO[Unit] =
    collection.createIndex(keys, options).void

  def initialize(database: SetupDatabase, resetOnStart: Boolean, diagnostics: Diagnostics): IO[Unit] =
    Option.when(resetOnStart)(resetOwnedCollections(database)).getOrElse(IO.unit) *>
      migrateAggregateVersions(database) *> verifyCandidateSearchProfiles(database) *>
      migrateOutboxSubjectReferences(database, diagnostics) *> migrateAnalyticsReportControl(database) *>
      migrateAnalyticsDeletionReceipts(database) *> createAccountRegistry(database)

  private def migrateAggregateVersions(database: SetupDatabase): IO[Unit] = {
    val ledger = database.getCollection(MongoCollections.HiringMigrationLedger)
    val migration = Filters.eq(MongoFields.Id, RevisionMigrationId)
    val state = ledger.find(migration).first.map(_.map(_.getString(MongoFields.State)))
    val started = Updates.combine(
      Updates.setOnInsert(MongoFields.Id, RevisionMigrationId),
      Updates.set(MongoFields.Version, 1L),
      Updates.set(MongoFields.State, "Running")
    )
    state.flatMap {
      case Some("Complete") => IO.unit
      case _                =>
        ledger.updateOne(migration, started, new UpdateOptions().upsert(true)).void *> List(
          database.getCollection(MongoCollections.Users),
          database.getCollection(MongoCollections.Jobs)
        ).traverse_(backfillVersions) *>
          verifyAggregateVersions(database) *>
          ledger
            .updateOne(
              migration,
              Updates.combine(Updates.set(MongoFields.Version, 1L), Updates.set(MongoFields.State, "Complete"))
            )
            .void
    }
  }

  private def verifyCandidateSearchProfiles(database: SetupDatabase): IO[Unit] = {
    val ledger = database.getCollection(MongoCollections.HiringMigrationLedger)
    val users = database.getCollection(MongoCollections.Users)
    val migration = Filters.eq(MongoFields.Id, CandidateProfileMigrationId)
    ledger.find(migration).first.flatMap {
      case Some(document) if document.getString(MongoFields.State) == "Complete" => IO.unit
      case existing                                                              =>
        val checkpoint = existing.flatMap(value => Option(value.getString(MongoFields.LastId)))
        ledger
          .updateOne(
            migration,
            Updates.combine(
              Updates.setOnInsert(MongoFields.Id, CandidateProfileMigrationId),
              Updates.set(MongoFields.Version, 1L),
              Updates.set(MongoFields.State, "Running")
            ),
            new UpdateOptions().upsert(true)
          )
          .void *> verifyCandidateProfileBatches(users, ledger, migration, checkpoint)
    }
  }

  /** Backfills the internal subject index needed to fence/purge events without changing the Kafka envelope. */
  private def migrateOutboxSubjectReferences(database: SetupDatabase, diagnostics: Diagnostics): IO[Unit] = {
    val ledger = database.getCollection(MongoCollections.HiringMigrationLedger)
    val outbox = database.getCollection(MongoCollections.EventOutbox)
    val migration = Filters.eq(MongoFields.Id, OutboxSubjectReferencesMigrationId)
    val unverifiedSubjects = Filters.ne(MongoFields.SubjectRefsVersion, 1)

    def verifyAndComplete: IO[Unit] = countDocuments(outbox, unverifiedSubjects).flatMap {
      case count if count.longValue() == 0L =>
        ledger
          .updateOne(
            migration,
            Updates.combine(
              Updates.set(MongoFields.Version, 1L),
              Updates.set(MongoFields.State, "Complete"),
              Updates.unset(MongoFields.LastId)
            )
          )
          .void
      case _ => backfillNextBatch
    }

    def backfillNextBatch: IO[Unit] =
      collectWithin(
        outbox
          .find(unverifiedSubjects)
          .sort(Indexes.ascending(MongoFields.Id))
          .limit(RevisionMigrationBatchSize)
          .boundedStream(32),
        RevisionMigrationBatchSize
      )
        .flatMap { batch =>
          batch.traverse_(backfillOutboxSubjectReferences(database, outbox, ledger, migration, _, diagnostics)) *>
            (if (batch.size == RevisionMigrationBatchSize) backfillNextBatch else verifyAndComplete)
        }

    ledger.find(migration).first.flatMap {
      case Some(document) if document.getString(MongoFields.State) == "Complete" =>
        countDocuments(outbox, unverifiedSubjects).flatMap {
          case count if count.longValue() == 0L => IO.unit
          case _                                =>
            ledger
              .updateOne(
                migration,
                Updates.combine(Updates.set(MongoFields.State, "Running"), Updates.set(MongoFields.Version, 1L))
              )
              .void *> backfillNextBatch
        }
      case _ =>
        ledger
          .updateOne(
            migration,
            Updates.combine(
              Updates.setOnInsert(MongoFields.Id, OutboxSubjectReferencesMigrationId),
              Updates.set(MongoFields.Version, 1L),
              Updates.set(MongoFields.State, "Running")
            ),
            new UpdateOptions().upsert(true)
          )
          .void *> backfillNextBatch
    }
  }

  private def backfillOutboxSubjectReferences(
      database: SetupDatabase,
      outbox: SetupCollection,
      ledger: SetupCollection,
      migration: Bson,
      document: Document,
      diagnostics: Diagnostics
  ): IO[Unit] = {
    val id = Option(document.getString(MongoFields.Id))
    val event = MongoHiringCodecs.readOperationalEvent(document).toEither.leftMap(_ => "undecodable event")

    (id, event) match {
      case (Some(eventId), Right(value)) =>
        outboxCandidateIds(database, value, diagnostics).flatMap {
          case Left(reason) =>
            IO.raiseError(
              new IllegalStateException(
                s"Outbox subject-reference migration found $reason at event $eventId"
              )
            )
          case Right(candidates) =>
            val subjectIds = (value.actorId.value.toString :: candidates).distinct.sorted
            val values = subjectIds.asJava
            outbox
              .updateOne(
                Filters.eq(MongoFields.Id, eventId),
                Updates.combine(
                  Updates.set(MongoFields.SubjectIds, values),
                  Updates.set(MongoFields.SubjectRefsVersion, 1)
                )
              )
              .flatMap {
                case result if result.getMatchedCount == 1L =>
                  ledger.updateOne(migration, Updates.set(MongoFields.LastId, eventId)).void
                case _ =>
                  IO.raiseError(
                    new IllegalStateException(s"Outbox subject-reference migration could not update event $eventId")
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
  private def migrateAnalyticsReportControl(database: SetupDatabase): IO[Unit] = {
    val ledger = database.getCollection(MongoCollections.HiringMigrationLedger)
    val snapshots = database.getCollection(MongoCollections.AnalyticsReportSnapshots)
    val controls = database.getCollection(MongoCollections.AnalyticsReportControl)
    val migration = Filters.eq(MongoFields.Id, AnalyticsReportControlMigrationId)

    def verify: IO[Unit] =
      controls.find(Filters.eq(MongoFields.Id, "analytics-report")).first.flatMap {
        case Some(document)
            if Option(document.get(MongoFields.Generation, classOf[java.lang.Long])).isDefined &&
              Option(document.get(MongoFields.NextRevision, classOf[java.lang.Long])).isDefined &&
              Option(document.get(MongoFields.LastPublishedRevision, classOf[java.lang.Long])).isDefined &&
              Set("Unpublished", "Hidden", "Published").contains(document.getString(MongoFields.State)) =>
          IO.unit
        case _ => IO.raiseError(new IllegalStateException("Analytics report control migration verification failed"))
      }

    ledger.find(migration).first.flatMap {
      case Some(document) if document.getString(MongoFields.State) == "Complete" => verify
      case _                                                                     =>
        ledger
          .updateOne(
            migration,
            Updates.combine(
              Updates.setOnInsert(MongoFields.Id, AnalyticsReportControlMigrationId),
              Updates.set(MongoFields.Version, 1L),
              Updates.set(MongoFields.State, "Running")
            ),
            new UpdateOptions().upsert(true)
          )
          .void *> snapshots.find(Filters.eq(MongoFields.Id, "current")).first.flatMap { legacy =>
          val visible = legacy.exists(snapshot =>
            snapshot.getString(MongoFields.State) == "Published" &&
              Option(snapshot.getDate(MongoFields.ExpiresAt)).exists(_.after(new java.util.Date()))
          )
          val addLegacyMetadata =
            snapshots
              .updateOne(
                Filters.and(Filters.eq(MongoFields.Id, "current"), Filters.exists(MongoFields.Generation, false)),
                Updates.combine(
                  Updates.set(MongoFields.Generation, 0L),
                  Updates.set(MongoFields.Revision, 0L),
                  Updates.set(MongoFields.RunId, "legacy")
                )
              )
              .void
          val initialControl = Updates.combine(
            Updates.setOnInsert(MongoFields.Id, "analytics-report"),
            Updates.setOnInsert(MongoFields.Generation, 0L),
            Updates.setOnInsert(MongoFields.State, if (visible) "Published" else "Unpublished"),
            Updates.setOnInsert(MongoFields.NextRevision, 0L),
            Updates.setOnInsert(MongoFields.LastPublishedRevision, 0L),
            Updates.setOnInsert(MongoFields.LastRunId, if (visible) "legacy" else "")
          )
          addLegacyMetadata *> controls
            .updateOne(Filters.eq(MongoFields.Id, "analytics-report"), initialControl, new UpdateOptions().upsert(true))
            .void *> verify *> ledger
            .updateOne(
              migration,
              Updates.combine(Updates.set(MongoFields.Version, 1L), Updates.set(MongoFields.State, "Complete"))
            )
            .void
        }
    }
  }

  private def outboxCandidateIds(
      database: SetupDatabase,
      event: com.example.graphQL.cats.service.events.OperationalEventEnvelope,
      diagnostics: Diagnostics
  ): IO[Either[String, List[String]]] = {
    import com.example.graphQL.cats.service.events.{OperationalAggregateType, OperationalEventType}

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
              .get[String](MongoFields.CandidateId)
              .toOption
              .flatMap(value => Parsing.parseUuid(value).toOption)
              .map(value => List(value.toString))
              .toRight("an application event without a valid candidateId")
          )
        case OperationalEventType.SEARCH_PERFORMED =>
          candidateIdsFromSearchPerformed(event.payload)
        case OperationalEventType.SEARCH_RESULT_CLICKED =>
          candidateIdFromSearchClick(database, event, diagnostics)
        case _ => IO.pure(Right(Nil))
      }
  }

  private def candidateIdsFromSearchPerformed(
      payload: Json
  ): IO[Either[String, List[String]]] = {
    val cursor = payload.hcursor
    val searchKind = cursor.get[String](MongoFields.SearchKind).toOption
    val results = cursor.get[List[Json]](MongoFields.Results).toOption
    val isCandidateSearch = searchKind.contains("candidateMatches")
    val isJobSearch = Set("semanticJobSearch", "recommendedJobs", MongoCollections.Jobs).exists(searchKind.contains)
    if (!isCandidateSearch && !isJobSearch) IO.pure(Left("a search event with an unknown or missing searchKind"))
    else {
      val parsedResults = results
        .toRight("a search event without results")
        .flatMap(_.traverse { result =>
          result.hcursor
            .get[String](MongoFields.ResultId)
            .toOption
            .flatMap(value => Parsing.parseUuid(value).toOption)
            .map(_.toString)
            .toRight("a search event with an invalid resultId")
        })
      IO.pure(parsedResults.map(ids => if (isCandidateSearch) ids else Nil))
    }
  }

  private def candidateIdFromSearchClick(
      database: SetupDatabase,
      event: com.example.graphQL.cats.service.events.OperationalEventEnvelope,
      diagnostics: Diagnostics
  ): IO[Either[String, List[String]]] =
    verifyRetainedSearchSessionForClick(
      event,
      id =>
        database
          .getCollection(MongoCollections.SearchSessions)
          .find(Filters.eq(MongoFields.Id, id.toString))
          .first,
      diagnostics
    )

  private[mongo] def verifyRetainedSearchSessionForClick(
      event: com.example.graphQL.cats.service.events.OperationalEventEnvelope,
      findSession: java.util.UUID => IO[Option[Document]],
      diagnostics: Diagnostics
  ): IO[Either[String, List[String]]] = {
    val payload = event.payload.hcursor
    val searchId = payload
      .get[String]("searchId")
      .toOption
      .flatMap(value => Parsing.parseUuid(value).toOption)
    val resultId = payload.get[String](MongoFields.ResultId).toOption
    (searchId, resultId) match {
      case (Some(id), Some(result)) =>
        findSession(id)
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
                    Parsing
                      .parseUuid(result)
                      .leftMap(_ => "a candidate search click with an invalid resultId")
                      .map(value => List(value.toString))
                  case session
                      if Set("semanticJobSearch", "recommendedJobs", MongoCollections.Jobs).contains(
                        session.searchKind
                      ) =>
                    Right(Nil)
                  case _ => Left("a search click with an unknown searchKind")
                }
          }
          .handleErrorWith(error =>
            diagnostics
              .emit(LogEvent.SearchSessionVerificationFailed, fields = LogFields.failure(error))
              .as(Left("an unavailable retained search session"))
          )
      case _ => IO.pure(Left("a search click without valid searchId or resultId"))
    }
  }

  private def verifyCandidateProfileBatches(
      users: SetupCollection,
      ledger: SetupCollection,
      migration: Bson,
      checkpoint: Option[String]
  ): IO[Unit] = {
    val cursor = checkpoint.fold(Filters.empty())(id => Filters.gt(MongoFields.Id, id))
    collectWithin(
      users.find(cursor).sort(Indexes.ascending(MongoFields.Id)).limit(RevisionMigrationBatchSize).boundedStream(32),
      RevisionMigrationBatchSize
    )
      .flatMap { batch =>
        batch.traverse_(user => validateCandidateSearchProfile(user) *> ensureCanonicalCandidateSkills(users, user)) *>
          batch.lastOption.traverse_ { last =>
            Option(last.getString(MongoFields.Id)).fold(
              IO.raiseError[Unit](new IllegalStateException("Invalid user migration key"))
            )(id => ledger.updateOne(migration, Updates.set(MongoFields.LastId, id)).void)
          } *> (if (batch.size == RevisionMigrationBatchSize)
                  verifyCandidateProfileBatches(
                    users,
                    ledger,
                    migration,
                    batch.lastOption.map(_.getString(MongoFields.Id))
                  )
                else
                  ledger
                    .updateOne(
                      migration,
                      Updates.combine(Updates.set(MongoFields.State, "Complete"), Updates.unset(MongoFields.LastId))
                    )
                    .void)
      }
  }

  private def validateCandidateSearchProfile(user: Document): IO[Unit] = {
    val profile = user.get(MongoFields.Profile) match {
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
        val country = Option(residence.get(MongoFields.Country)).collect { case text: String => text }
        val countryCanonical = Option(residence.get(MongoFields.CountryCanonical)).collect { case text: String => text }
        val city = Option(residence.get(MongoFields.City)).collect { case text: String => text }
        val cityCanonical = Option(residence.get(MongoFields.CityCanonical)).collect { case text: String => text }
        def validText(text: String): Boolean = text.trim.nonEmpty && text.length <= 256
        country.exists(validText) && countryCanonical.contains(country.map(canonical).getOrElse("")) &&
        optionalFieldValid(residence, MongoFields.City)(_ => city.exists(validText)) &&
        optionalFieldValid(residence, MongoFields.CityCanonical)(_ =>
          city.exists(value => cityCanonical.contains(canonical(value)))
        )
      case _ => false
    }
    val residenceValid =
      profile.forall(value => optionalFieldValid(value, MongoFields.CurrentResidence)(validResidence))
    val availabilityValid = profile.forall(value =>
      optionalFieldValid(value, MongoFields.AvailabilityStatus) {
        case status: String => Set("AVAILABLE_NOW", "UNAVAILABLE").contains(status)
        case _              => false
      }
    )
    val consentValid = profile.forall(value =>
      optionalFieldValid(value, MongoFields.RecruiterSearchOptIn) {
        case _: java.lang.Boolean => true
        case _                    => false
      }
    )
    if (!residenceValid) invalid(MongoFields.CurrentResidence)
    else if (!availabilityValid) invalid(MongoFields.AvailabilityStatus)
    else if (!consentValid) invalid(MongoFields.RecruiterSearchOptIn)
    else IO.unit
  }

  private def ensureCanonicalCandidateSkills(users: SetupCollection, user: Document): IO[Unit] = {
    val isCandidate = user.getString(MongoFields.Role) == "Candidate"
    val profile = user.get(MongoFields.Profile) match {
      case value: Document => Some(value)
      case _               => None
    }
    val skills = profile.flatMap(value => Option(value.get(MongoFields.Skills))) match {
      case Some(values: java.util.List[?]) => values.asScala.toList.collect { case value: String => value }
      case _                               => Nil
    }
    if (!isCandidate) IO.unit
    else updateCanonicalSkills(users, user, skills, retries = 3)
  }

  private def updateCanonicalSkills(
      users: SetupCollection,
      user: Document,
      skills: List[String],
      retries: Int
  ): IO[Unit] = {
    val canonical = skills.map(_.trim.toLowerCase(java.util.Locale.ROOT)).sorted.asJava
    val id = user.getString(MongoFields.Id)
    val version = user.getLong(MongoFields.Version)
    val expectedSkills = skills.asJava
    val unchanged = Filters.and(
      Filters.eq(MongoFields.Id, id),
      Filters.eq(MongoFields.Version, version),
      Filters.eq(MongoFields.Role, UserRole.Candidate.toString),
      Filters.eq(MongoFields.ProfileSkills, expectedSkills)
    )
    users.updateOne(unchanged, Updates.set(MongoFields.ProfileSkillsCanonical, canonical)).flatMap {
      case result if result.getMatchedCount == 1L => IO.unit
      case _ if retries > 0                       =>
        users.find(Filters.eq(MongoFields.Id, id)).first.flatMap {
          case Some(latest) =>
            val latestProfile = latest.get(MongoFields.Profile) match {
              case profile: Document => Some(profile)
              case _                 => None
            }
            val latestSkills = latestProfile.flatMap(value => Option(value.get(MongoFields.Skills))) match {
              case Some(values: java.util.List[?]) => values.asScala.toList.collect { case value: String => value }
              case _                               => Nil
            }
            val sidecar = latestProfile.flatMap(value => Option(value.get(MongoFields.SkillsCanonical)))
            if (sidecar.contains(canonical)) IO.unit
            else updateCanonicalSkills(users, latest, latestSkills, retries - 1)
          case None => IO.unit
        }
      case _ => IO.raiseError(new IllegalStateException("Candidate skill migration raced with profile updates"))
    }
  }

  private def backfillVersions(collection: SetupCollection): IO[Unit] = {
    val missingVersion = Filters.exists(MongoFields.Version, false)
    def nextBatch: IO[Unit] =
      collectWithin(
        collection
          .find(missingVersion)
          .sort(Indexes.ascending(MongoFields.Id))
          .limit(RevisionMigrationBatchSize)
          .boundedStream(32),
        RevisionMigrationBatchSize
      )
        .flatMap { documents =>
          val ids = documents.flatMap(document => Option(document.getString(MongoFields.Id)))
          if (documents.isEmpty) IO.unit
          else if (ids.size != documents.size)
            IO.raiseError(new IllegalStateException("Mongo revision backfill found a document without a string _id"))
          else backfillVersionBatch(collection, ids) *> nextBatch
        }
    nextBatch
  }

  private[mongo] def backfillVersionBatch(collection: SetupCollection, ids: List[String]): IO[Unit] =
    collection
      .updateMany(
        Filters.and(Filters.in(MongoFields.Id, ids*), Filters.exists(MongoFields.Version, false)),
        Updates.set(MongoFields.Version, Long.box(0L))
      )
      .flatMap {
        // Another startup may have migrated some of these documents first. The missing-version
        // predicate keeps a concurrent aggregate write from being reset to revision zero.
        case result if result.getMatchedCount <= ids.size.toLong => IO.unit
        case _                                                   =>
          IO.raiseError(new IllegalStateException("Mongo revision backfill updated an unexpected row count"))
      }

  private def verifyAggregateVersions(database: SetupDatabase): IO[Unit] =
    List(database.getCollection(MongoCollections.Users), database.getCollection(MongoCollections.Jobs)).traverse_ {
      collection =>
        val invalidVersion = Filters.or(
          Filters.exists(MongoFields.Version, false),
          Filters.lt(MongoFields.Version, 0L),
          Filters.not(Filters.`type`(MongoFields.Version, BsonType.INT64))
        )
        countDocuments(collection, invalidVersion).flatMap {
          case count if count.longValue() == 0L => IO.unit
          case _ => IO.raiseError(new IllegalStateException("Mongo contains an invalid aggregate revision"))
        }
    }

  private def resetOwnedCollections(database: SetupDatabase): IO[Unit] =
    ownedCollections.toList.traverse_(database.dropCollection)

  private def createAccountRegistry(database: SetupDatabase): IO[Unit] =
    database
      .getCollection(MongoCollections.AccountRegistry)
      .updateOne(
        Filters.eq(MongoFields.Id, "user-account-registry"),
        Updates.combine(
          Updates.setOnInsert(MongoFields.Id, "user-account-registry"),
          Updates.setOnInsert(MongoFields.State, "Uninitialized")
        ),
        new UpdateOptions().upsert(true)
      )
      .void

  private def migrateAnalyticsDeletionReceipts(database: SetupDatabase): IO[Unit] = {
    val ledger = database.getCollection(MongoCollections.HiringMigrationLedger)
    val migration = Filters.eq(MongoFields.Id, AnalyticsDeletionReceiptMigrationId)
    ledger.find(migration).first.flatMap {
      case Some(document) if document.getString(MongoFields.State) == "Complete" => IO.unit
      case _                                                                     =>
        val started = Updates.combine(
          Updates.setOnInsert(MongoFields.Id, AnalyticsDeletionReceiptMigrationId),
          Updates.set(MongoFields.Version, 1L),
          Updates.set(MongoFields.State, "Running")
        )
        ledger.updateOne(migration, started, new UpdateOptions().upsert(true)).void *>
          List(
            index(
              database.getCollection(MongoCollections.AnalyticsErasureRequests),
              Indexes.ascending(MongoFields.ReceiptId),
              new IndexOptions()
                .name("analytics_erasure_request_receipt_unique")
                .unique(true)
                .partialFilterExpression(Filters.exists(MongoFields.ReceiptId, true))
            ),
            index(
              database.getCollection(MongoCollections.AnalyticsErasureCompletions),
              Indexes.ascending(MongoFields.ReceiptId),
              new IndexOptions()
                .name("analytics_erasure_completion_receipt_unique")
                .unique(true)
                .partialFilterExpression(Filters.exists(MongoFields.ReceiptId, true))
            )
          ).sequence_.void *>
          ledger.updateOne(migration, Updates.set(MongoFields.State, "Complete")).void
    }
  }

}
