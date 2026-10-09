package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import io.circe.Json
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.workflow.InterviewTopicPair
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogFields}
import com.example.graphQL.cats.service.Diagnostics.*
import com.example.graphQL.cats.shared.*
import com.mongodb.*
import com.mongodb.client.model.*
import org.bson.*
import org.bson.conversions.*
import scala.jdk.CollectionConverters.*

/** Ordered hiring migrations 001–009 plus the shared ledger-tracked plan; later cutovers live in their own objects. */
private[mongo] object MongoHiringMigrations {
  import MongoHiringSetup.{SetupCollection, SetupDatabase}

  private val BatchSize = 500
  private val InterviewWorkflowCollections = List(
    MongoCollections.InterviewWorkflows,
    MongoCollections.InterviewWorkflowCommands,
    MongoCollections.InterviewWorkflowInbox,
    MongoCollections.InterviewCalendarReservations,
    MongoCollections.InterviewCalendarParticipantLocks,
    MongoCollections.InterviewNotificationReceipts
  )

  val ownedCollections: Set[String] = Set(
    MongoCollections.Users,
    MongoCollections.Jobs,
    MongoCollections.Applications,
    MongoCollections.ApplicationEvents,
    MongoCollections.AccountRegistry,
    MongoCollections.EmbeddingWork,
    MongoCollections.EventOutbox,
    MongoCollections.OutboxSubjectFences,
    MongoProducerRegistrations.Collection,
    MongoProducerRegistrations.ClaimCursorsCollection,
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
    MongoCollections.HiringMigrationLedger,
    MongoCollections.InterviewSubjectCleanup,
    MongoCollections.InterviewWorkflows,
    MongoCollections.InterviewWorkflowCommands,
    MongoCollections.InterviewWorkflowInbox,
    MongoCollections.InterviewCalendarReservations,
    MongoCollections.InterviewCalendarParticipantLocks,
    MongoCollections.InterviewNotificationReceipts
  )

  private def collectWithin[A](run: MigrationRun, values: fs2.Stream[IO, A], maximum: Int): IO[List[A]] =
    values.take(maximum.toLong + 1L).compile.toList.flatMap { found =>
      if (found.size > maximum) run.fail(s"setup read exceeded its maximum of $maximum elements")
      else IO.pure(found)
    }

  private def countDocuments(collection: SetupCollection, filter: Bson): IO[Long] =
    collection.count(filter, new CountOptions())

  private def boundedBatch(collection: SetupCollection, filter: Bson) =
    collection.find(filter).sort(Indexes.ascending(MongoFields.Id)).limit(BatchSize).boundedStream(32)

  /** The ledger plan in application order. Completed 013/015 proofs retire the 003/010 and 011 scans they cover. */
  def initialize(
      database: SetupDatabase,
      resetOnStart: Boolean,
      diagnostics: Diagnostics,
      topics: InterviewTopicPair = InterviewTopicPair.Default
  ): IO[Unit] =
    IO.whenA(resetOnStart)(resetOwnedCollections(database)) *>
      (
        MongoWorkflowIntegrityMigrations.trusted(database),
        MongoInterviewCleanupIntegrityMigrations.trusted(database, topics)
      ).tupled.flatMap { (workflowTrusted, cleanupTrusted) =>
        def run(step: MongoMigrationStep): IO[Unit] = MongoMigrationRunner.run(database, step)
        List(
          run(userJobRevisions),
          run(candidateSearchProfiles),
          IO.unlessA(workflowTrusted)(run(outboxSubjectReferences(diagnostics))),
          run(analyticsReportControl),
          run(analyticsDeletionReceipts),
          run(interviewWorkflowStorage),
          run(MongoInterviewLedgerCollectionMigrations.step),
          run(interviewSubjectCleanup),
          run(interviewInboxIdentity),
          // TODO(RF-06): 010 keeps its own ledger handling until the interview workflow session releases the file.
          IO.unlessA(workflowTrusted)(MongoInterviewWorkflowMigrations.initialize(database)),
          IO.unlessA(cleanupTrusted)(run(MongoInterviewCleanupMigrations.step)),
          run(MongoProducerRegistrationMigrations.step),
          run(MongoWorkflowIntegrityMigrations.step),
          run(MongoDeletedAccountEmbeddingMigrations.step),
          run(MongoInterviewCleanupIntegrityMigrations.step(topics)),
          run(MongoInterviewLifecycleMigrations.step),
          run(MongoInterviewRequestReceiptMigrations.step),
          createAccountRegistry(database)
        ).sequence_
      }

  /** Retire only the known unfiltered inbox index; quarantine/hiring receipts must not share a null identity. */
  private val interviewInboxIdentity = MongoMigrationStep(
    MigrationIds.InterviewInboxIdentity,
    run => {
      val collection = run.database.getCollection(MongoCollections.InterviewWorkflowInbox)
      val indexName = MongoIndexNames.InterviewWorkflowInboxIdentity
      collection.listIndexes[Document].flatMap { indexes =>
        indexes.find(_.getString("name") == indexName) match {
          case Some(index) if !index.containsKey("partialFilterExpression") =>
            val keys = index.get("key", classOf[Document])
            val expected = new Document(MongoFields.WorkflowId, Int.box(1)).append(MongoFields.MessageId, Int.box(1))
            if (keys != expected || !index.getBoolean("unique", false))
              IO.raiseError(
                MigrationError.IndexMismatch(
                  MongoCollections.InterviewWorkflowInbox,
                  indexName,
                  "unexpected legacy definition"
                )
              )
            else
              run.database
                .runCommand(
                  new Document("dropIndexes", MongoCollections.InterviewWorkflowInbox)
                    .append("index", indexName)
                )
                .void
                .recoverWith { case error: MongoCommandException if error.getErrorCode == 27 => IO.unit }
          case _ => IO.unit
        }
      }
    }
  )

  private val interviewSubjectCleanup = MongoMigrationStep(
    MigrationIds.InterviewSubjectCleanup,
    run => run.database.ensureCollection(MongoCollections.InterviewSubjectCleanup)
  )

  /** Introduces empty durable-work and local ledger-provider stores. There is no legacy payload to backfill; each
    * collection creation is idempotent. Index setup runs after the ledger plan and verifies each required definition on
    * every startup.
    */
  private val interviewWorkflowStorage = MongoMigrationStep(
    MigrationIds.InterviewWorkflowStorage,
    run => InterviewWorkflowCollections.traverse_(run.database.ensureCollection)
  )

  /** Verifies existing optional GeoJSON points after the strict collection validator is installed. Missing points are
    * valid and are deliberately left untouched. The scan is bounded and resumes after the last verified job ID; new
    * writes are protected by the validator before this runs.
    */
  private[mongo] def verifyJobGeoPoints(database: SetupDatabase): IO[Unit] =
    MongoMigrationRunner.run(database, jobGeoPoints)

  private val jobGeoPoints = MongoMigrationStep(
    MigrationIds.JobGeoPoints,
    run => {
      val jobs = run.database.getCollection(MongoCollections.Jobs)
      val pointExists = Filters.exists(MongoFields.LocationPoint, true)
      def scan(checkpoint: Option[String]): IO[Unit] = {
        val afterCheckpoint =
          checkpoint.fold(pointExists)(id => Filters.and(pointExists, Filters.gt(MongoFields.Id, id)))
        collectWithin(run, boundedBatch(jobs, afterCheckpoint), BatchSize).flatMap { batch =>
          batch.traverse_ { job =>
            val id = Option(job.getString(MongoFields.Id))
            val point = Option(job.get(MongoFields.Location, classOf[Document]))
              .flatMap(location => Option(location.get(MongoFields.Point, classOf[Document])))
            (id, point) match {
              case (Some(_), Some(value)) if isValidJobGeoPoint(value) => IO.unit
              case (Some(jobId), _) => run.fail(s"found an invalid point at job $jobId")
              case _                => run.fail("found an invalid job key")
            }
          } *> batch.lastOption.traverse_ { last =>
            Option(last.getString(MongoFields.Id)).fold(run.fail[Unit]("invalid job key"))(run.advance)
          } *> (if (batch.size == BatchSize) scan(batch.lastOption.map(_.getString(MongoFields.Id))) else IO.unit)
        }
      }
      run.textCheckpoint.flatMap(scan)
    }
  )

  private[mongo] def isValidJobGeoPoint(point: Document): Boolean = {
    val coordinates = Option(point.get(MongoFields.Coordinates)) collect { case values: java.util.List[?] =>
      values.asScala.toList
    }
    val longitudeLatitude = coordinates.filter(_.size == 2).flatMap {
      case List(longitude: Number, latitude: Number) =>
        val lon = longitude.doubleValue()
        val lat = latitude.doubleValue()
        Option.when(lon.isFinite && lat.isFinite && lon >= -180d && lon <= 180d && lat >= -90d && lat <= 90d)(())
      case _ => None
    }
    point.get(MongoFields.Type) == "Point" && longitudeLatitude.isDefined
  }

  private val userJobRevisions = MongoMigrationStep(
    MigrationIds.UserJobRevisions,
    run =>
      List(
        run.database.getCollection(MongoCollections.Users),
        run.database.getCollection(MongoCollections.Jobs)
      ).traverse_(backfillVersions(run, _)) *> verifyAggregateVersions(run)
  )

  private val candidateSearchProfiles = MongoMigrationStep(
    MigrationIds.CandidateSearchProfileVerification,
    run => run.textCheckpoint.flatMap(verifyCandidateProfileBatches(run, _))
  )

  /** Backfills the internal subject index needed to fence/purge events without changing the Kafka envelope. A completed
    * proof is reopened when unverified events reappear.
    */
  private def outboxSubjectReferences(diagnostics: Diagnostics): MongoMigrationStep = {
    val unverifiedSubjects = Filters.ne(MongoFields.SubjectRefsVersion, 1)
    def outbox(database: SetupDatabase) = database.getCollection(MongoCollections.EventOutbox)
    def backfill(run: MigrationRun): IO[Unit] =
      collectWithin(run, boundedBatch(outbox(run.database), unverifiedSubjects), BatchSize).flatMap { batch =>
        batch.traverse_(backfillOutboxSubjectReferences(run, _, diagnostics)) *>
          (if (batch.size == BatchSize) backfill(run)
           else
             countDocuments(outbox(run.database), unverifiedSubjects).flatMap {
               case count if count.longValue() == 0L => IO.unit
               case _                                => backfill(run)
             })
      }
    MongoMigrationStep(
      MigrationIds.EventOutboxSubjectReferences,
      backfill,
      database =>
        countDocuments(outbox(database), unverifiedSubjects).map {
          case count if count.longValue() == 0L => CompletedProof.Trusted
          case _                                => CompletedProof.Reopen
        }
    )
  }

  private def backfillOutboxSubjectReferences(
      run: MigrationRun,
      document: Document,
      diagnostics: Diagnostics
  ): IO[Unit] = {
    val outbox = run.database.getCollection(MongoCollections.EventOutbox)
    val id = Option(document.getString(MongoFields.Id))
    val event = MongoHiringCodecs.readOperationalEvent(document).toEither.leftMap(_ => "undecodable event")

    (id, event) match {
      case (Some(eventId), Right(value)) =>
        outboxCandidateIds(run.database, value, diagnostics).flatMap {
          case Left(reason)      => run.fail(s"found $reason at event $eventId")
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
                case result if result.getMatchedCount == 1L => run.advance(eventId)
                case _                                      => run.fail(s"could not update event $eventId")
              }
        }
      case (Some(eventId), Left(reason)) => run.fail(s"found $reason at event $eventId")
      case _                             => run.fail("found an invalid event key")
    }
  }

  /** Seeds a durable report generation and visibility record independently of the expiring snapshot payload. */
  private val analyticsReportControl: MongoMigrationStep = {
    def verify(run: MigrationRun): IO[Unit] =
      MongoSessionOperations
        .findById(
          IO.pure(run.database.getCollection(MongoCollections.AnalyticsReportControl)),
          None,
          "analytics-report"
        )
        .flatMap {
          case Some(document)
              if Option(document.get(MongoFields.Generation, classOf[java.lang.Long])).isDefined &&
                Option(document.get(MongoFields.NextRevision, classOf[java.lang.Long])).isDefined &&
                Option(document.get(MongoFields.LastPublishedRevision, classOf[java.lang.Long])).isDefined &&
                Set("Unpublished", "Hidden", "Published").contains(document.getString(MongoFields.State)) =>
            IO.unit
          case _ => run.fail("verification failed")
        }
    def seed(run: MigrationRun): IO[Unit] = {
      val snapshots = run.database.getCollection(MongoCollections.AnalyticsReportSnapshots)
      val controls = run.database.getCollection(MongoCollections.AnalyticsReportControl)
      MongoSessionOperations.findById(IO.pure(snapshots), None, "current").flatMap { legacy =>
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
          .void *> verify(run)
      }
    }
    MongoMigrationStep(
      MigrationIds.AnalyticsReportControl,
      seed,
      database => verify(MigrationRun(database, MigrationIds.AnalyticsReportControl, None)).as(CompletedProof.Trusted)
    )
  }

  /** The receipt uniqueness indexes are declared once in `MongoHiringIndexSetup`; this step only introduces them. */
  private val analyticsDeletionReceipts = MongoMigrationStep(
    MigrationIds.AnalyticsDeletionReceipts,
    // One spec at a time: `ensure` groups by collection (unordered), and the applied command order is kept as at HEAD.
    run =>
      MongoHiringIndexSetup.analyticsErasureReceiptIndexes.traverse_(spec =>
        MongoHiringIndexSetup.ensure(run.database.underlying, List(spec))
      )
  )

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
        MongoSessionOperations.findById(
          IO.pure(database.getCollection(MongoCollections.SearchSessions)),
          None,
          id.toString
        ),
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

  private def verifyCandidateProfileBatches(run: MigrationRun, checkpoint: Option[String]): IO[Unit] = {
    val users = run.database.getCollection(MongoCollections.Users)
    val cursor = checkpoint.fold(Filters.empty())(id => Filters.gt(MongoFields.Id, id))
    collectWithin(run, boundedBatch(users, cursor), BatchSize).flatMap { batch =>
      batch.traverse_(user => validateCandidateSearchProfile(run, user) *> ensureCanonicalCandidateSkills(run, user)) *>
        batch.lastOption.traverse_ { last =>
          Option(last.getString(MongoFields.Id)).fold(run.fail[Unit]("invalid user key"))(run.advance)
        } *> (if (batch.size == BatchSize)
                verifyCandidateProfileBatches(run, batch.lastOption.map(_.getString(MongoFields.Id)))
              else IO.unit)
    }
  }

  private def validateCandidateSearchProfile(run: MigrationRun, user: Document): IO[Unit] = {
    val profile = user.get(MongoFields.Profile) match {
      case value: Document => Some(value)
      case _               => None
    }
    def invalid(field: String): IO[Unit] = run.fail(s"found malformed $field")
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

  private def ensureCanonicalCandidateSkills(run: MigrationRun, user: Document): IO[Unit] = {
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
    else updateCanonicalSkills(run, user, skills, retries = 3)
  }

  private def updateCanonicalSkills(
      run: MigrationRun,
      user: Document,
      skills: List[String],
      retries: Int
  ): IO[Unit] = {
    val users = run.database.getCollection(MongoCollections.Users)
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
        MongoSessionOperations.findById(IO.pure(users), None, id).flatMap {
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
            else updateCanonicalSkills(run, latest, latestSkills, retries - 1)
          case None => IO.unit
        }
      case _ => run.fail("candidate skill migration raced with profile updates")
    }
  }

  private def backfillVersions(run: MigrationRun, collection: SetupCollection): IO[Unit] = {
    val missingVersion = Filters.exists(MongoFields.Version, false)
    def nextBatch: IO[Unit] =
      collectWithin(run, boundedBatch(collection, missingVersion), BatchSize).flatMap { documents =>
        val ids = documents.flatMap(document => Option(document.getString(MongoFields.Id)))
        if (documents.isEmpty) IO.unit
        else if (ids.size != documents.size) run.fail("revision backfill found a document without a string _id")
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
          IO.raiseError(
            MigrationError
              .StepFailed(MigrationIds.UserJobRevisions, "revision backfill updated an unexpected row count")
          )
      }

  private def verifyAggregateVersions(run: MigrationRun): IO[Unit] =
    List(
      run.database.getCollection(MongoCollections.Users),
      run.database.getCollection(MongoCollections.Jobs)
    ).traverse_ { collection =>
      val invalidVersion = Filters.or(
        Filters.exists(MongoFields.Version, false),
        Filters.lt(MongoFields.Version, 0L),
        Filters.not(Filters.`type`(MongoFields.Version, BsonType.INT64))
      )
      countDocuments(collection, invalidVersion).flatMap {
        case count if count.longValue() == 0L => IO.unit
        case _                                => run.fail("an invalid aggregate revision remains")
      }
    }

  private def resetOwnedCollections(database: SetupDatabase): IO[Unit] =
    ownedCollections.toList.traverse_(database.dropCollection) *>
      // A stale original-name ledger must not be renamed back into a freshly reset installation.
      MongoInterviewLedgerCollectionMigrations.LegacyNames.traverse_(name =>
        database
          .runCommand(new org.bson.Document("drop", name))
          .void
          .recoverWith { case error: MongoCommandException if error.getErrorCode == 26 => IO.unit }
      )

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
}
