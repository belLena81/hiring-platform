package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.*
import com.example.graphQL.cats.domain.pagination.*
import com.example.graphQL.cats.service.{ActorContext, Diagnostics, RepositoryError}
import com.example.graphQL.cats.service.auth.ActorAuthorization
import com.example.graphQL.cats.service.events.{OperationalEvents, OperationalEventType}
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.read.{HiringReadScope, JobRelationKey, UserRelationKey}
import com.example.graphQL.cats.service.search.{
  CandidateMatchFilters,
  JobSearchFilter,
  ValidatedCandidateMatchFilters,
  VectorSearchQuery
}
import io.circe.Json
import io.circe.parser.parse
import org.bson.{BsonDocument, BsonValue, Document}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.Instant
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** A bounded baseline of production adapter calls, separate from performance acceptance. */
final class MongoOperationalAccessEvaluationIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes
  private val support = MongoAccessEvaluationSupport
  private val now = Instant.parse("2026-10-05T12:00:00Z")
  private val size = PageSize.fromInt(7).toOption.getOrElse(fail("invalid fixture page size"))
  private val page = JobPageRequest(None, None, size)
  private val applicationsPage = ApplicationPageRequest(None, None, size)
  private val historyPage = ApplicationEventPageRequest(None, size)
  private val repetitions = 100
  private val warmupRequests = 8
  private val workRecordsPerPhase = 128

  private def user(index: Int, role: UserRole): User =
    User(
      UserId(support.deterministicId(s"user:$index")),
      None,
      s"Synthetic $role $index",
      role,
      role match {
        case UserRole.Candidate =>
          Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), Some("Synthetic experience"), None)))
        case UserRole.Recruiter => Some(UserProfile.Recruiter(RecruiterProfile("Synthetic organization", None)))
        case UserRole.Admin     => None
      },
      now,
      adminSingleton = role == UserRole.Admin
    )

  private val candidates = (0 until 32).toList.map(user(_, UserRole.Candidate))
  private val recruiters = List(user(32, UserRole.Recruiter), user(33, UserRole.Recruiter))
  private val admin = user(34, UserRole.Admin)
  private val jobs = (0 until 128).toList.map { index =>
    Job(
      JobId(support.deterministicId(s"job:$index")),
      recruiters(index % 2).id,
      s"Synthetic Scala job $index",
      "Synthetic job description",
      List("Bounded fixture"),
      if (index % 3 == 0) Set("Scala", "MongoDB") else Set("Scala"),
      Location("Cyprus", if (index % 2 == 0) "Nicosia" else "Limassol", remote = false),
      if (index % 4 == 3) JobStatus.Closed else JobStatus.Open,
      now.minusSeconds((index / 8).toLong),
      now
    )
  }
  private val applications = (0 until 128).toList.map { index =>
    Application(
      ApplicationId(support.deterministicId(s"application:$index")),
      candidates(index % 32).id,
      jobs(index / 8).id,
      ApplicationStatus.Created,
      now.minusSeconds((index / 8).toLong),
      now
    )
  }
  private val events = applications.flatMap { application =>
    (0 until 3).toList.map { index =>
      ApplicationEvent(
        ApplicationEventId(support.deterministicId(s"event:${application.id}:$index")),
        application.id,
        None,
        ApplicationStatus.Created,
        application.candidateId,
        now.minusSeconds(index.toLong),
        None,
        None
      )
    }
  }

  private final class ObservedRepositoryFailure(val error: RepositoryError)
      extends RuntimeException("Repository failure during synthetic baseline")

  private def successful[A](value: RepositoryIO[A]): IO[A] = value.value.flatMap {
    case Right(result) => IO.pure(result)
    case Left(error)   => IO.raiseError(new ObservedRepositoryFailure(error))
  }

  private final case class Capability(
      name: String,
      actor: String,
      run: IO[Int],
      prepare: IO[Unit] = IO.unit,
      minimumReturned: Int = 0,
      operation: String = "read",
      reconcile: List[Observation] => IO[Json] = _ => IO.pure(Json.Null)
  )
  private final case class Observation(latencyMillis: Double, returned: Int, error: Option[String])

  test("production operational access records bounded commands, plans and reproducible local baselines") {
    mongoResource.use { fixture =>
      val database = fixture.database
      val users = new MongoUserRepository(
        database,
        MongoRepositoryTestSupport.noTransaction,
        MongoEmbeddingWorkEnqueuer.disabled,
        Diagnostics.noop
      )
      val jobRepository = MongoJobRepository.transactional(
        database,
        fixture.client,
        MongoEmbeddingWorkEnqueuer.disabled,
        Diagnostics.noop
      )
      val applicationRepository = MongoApplicationRepository.transactional(database, fixture.client, Diagnostics.noop)
      val search = new MongoSemanticSearchRepository(
        database,
        "job-vector",
        "candidate-vector",
        "job-lexical",
        "candidate-lexical",
        100,
        diagnostics = Diagnostics.noop
      )
      val authorization = ActorAuthorization(users)
      def scope(value: User): HiringReadScope =
        HiringReadScope
          .validated(ActorContext(value.id, value.role), value, authorization)
          .toOption
          .getOrElse(fail("invalid fixture actor"))
      val candidateScope = scope(candidates.head)
      val recruiterScope = scope(recruiters.head)
      val adminScope = scope(admin)
      val capabilities = List(
        Capability(
          "jobDiscoveryBroad",
          "Candidate",
          successful(jobRepository.findOpen(JobSearchFilter(None, Set.empty, None), page)).map(_.size)
        ),
        Capability(
          "jobDiscoverySelective",
          "Candidate",
          successful(
            jobRepository.findOpen(JobSearchFilter(Some("Nicosia"), Set("MongoDB"), Some(now.minusSeconds(5))), page)
          ).map(_.size)
        ),
        Capability(
          "jobDiscoveryEmpty",
          "Candidate",
          successful(jobRepository.findOpen(JobSearchFilter(Some("Absent fixture city"), Set.empty, None), page))
            .map(_.size)
        ),
        Capability(
          "jobDiscoveryCityOnly",
          "Candidate",
          successful(jobRepository.findOpen(JobSearchFilter(Some("Nicosia"), Set.empty, None), page)).map(_.size)
        ),
        Capability(
          "jobDiscoverySkillsOnly",
          "Candidate",
          successful(jobRepository.findOpen(JobSearchFilter(None, Set("MongoDB"), None), page)).map(_.size)
        ),
        Capability(
          "jobDiscoveryDateOnly",
          "Candidate",
          successful(jobRepository.findOpen(JobSearchFilter(None, Set.empty, Some(now.minusSeconds(5))), page))
            .map(_.size)
        ),
        Capability(
          "jobDiscoveryLaterPage",
          "Candidate",
          successful(
            jobRepository.findOpen(
              JobSearchFilter(None, Set.empty, None),
              page.copy(cursor = Some(JobCursor(jobs(8).createdAt, jobs(8).id)))
            )
          ).map(_.size)
        ),
        Capability(
          "recruiterJobsBroad",
          "Recruiter",
          successful(jobRepository.findByRecruiter(recruiters.head.id, page)).map(_.size)
        ),
        Capability(
          "recruiterJobsFiltered",
          "Recruiter",
          successful(jobRepository.findByRecruiter(recruiters.head.id, page.copy(status = Some(JobStatus.Open))))
            .map(_.size)
        ),
        Capability("adminJobs", "Admin", successful(jobRepository.findAll(page)).map(_.size)),
        Capability(
          "candidateApplications",
          "Candidate",
          successful(applicationRepository.findByCandidate(candidateScope, applicationsPage)).map(_.size)
        ),
        Capability(
          "candidateApplicationsEmpty",
          "Candidate",
          successful(
            applicationRepository.findByCandidate(
              candidateScope,
              applicationsPage.copy(status = Some(ApplicationStatus.Hired))
            )
          ).map(_.size)
        ),
        Capability(
          "candidateApplicationsFiltered",
          "Candidate",
          successful(
            applicationRepository.findByCandidate(
              candidateScope,
              applicationsPage.copy(status = Some(ApplicationStatus.Created))
            )
          ).map(_.size)
        ),
        Capability(
          "candidateApplicationsLaterPage",
          "Candidate",
          successful(
            applicationRepository.findByCandidate(
              candidateScope,
              applicationsPage.copy(cursor = Some(ApplicationCursor(applications(32).createdAt, applications(32).id)))
            )
          ).map(_.size)
        ),
        Capability(
          "recruiterApplications",
          "Recruiter",
          successful(applicationRepository.findByJob(recruiterScope, jobs.head.id, applicationsPage)).map(_.size)
        ),
        Capability(
          "recruiterApplicationsForbidden",
          "Recruiter",
          successful(applicationRepository.findByJob(recruiterScope, jobs(1).id, applicationsPage)).map(_.size)
        ),
        Capability(
          "recruiterApplicationsFiltered",
          "Recruiter",
          successful(
            applicationRepository.findByJob(
              recruiterScope,
              jobs.head.id,
              applicationsPage.copy(status = Some(ApplicationStatus.Created))
            )
          ).map(_.size)
        ),
        Capability(
          "recruiterApplicationsEmpty",
          "Recruiter",
          successful(
            applicationRepository.findByJob(
              recruiterScope,
              jobs.head.id,
              applicationsPage.copy(status = Some(ApplicationStatus.Hired))
            )
          ).map(_.size)
        ),
        Capability(
          "recruiterApplicationsLaterPage",
          "Recruiter",
          successful(
            applicationRepository.findByJob(
              recruiterScope,
              jobs.head.id,
              applicationsPage.copy(cursor = Some(ApplicationCursor(applications.head.createdAt, applications.head.id)))
            )
          ).map(_.size)
        ),
        Capability(
          "adminApplications",
          "Admin",
          successful(applicationRepository.findByJob(adminScope, jobs.head.id, applicationsPage)).map(_.size)
        ),
        Capability(
          "candidateHistory",
          "Candidate",
          successful(applicationRepository.history(candidateScope, applications.head.id, historyPage)).map(_.size)
        ),
        Capability(
          "recruiterHistory",
          "Recruiter",
          successful(applicationRepository.history(recruiterScope, applications.head.id, historyPage)).map(_.size)
        ),
        Capability(
          "candidateHistoryLaterPage",
          "Candidate",
          successful(
            applicationRepository.history(
              candidateScope,
              applications.head.id,
              historyPage.copy(cursor = Some(ApplicationEventCursor(events(1).occurredAt, events(1).id)))
            )
          ).map(_.size)
        ),
        Capability(
          "adminAccountsBroad",
          "Admin",
          successful(users.listAccounts(UserPageRequest(AccountStatus.Active, None, None, size))).map(_.size)
        ),
        Capability(
          "adminAccountsSelective",
          "Admin",
          successful(users.listAccounts(UserPageRequest(AccountStatus.Active, Some(UserRole.Candidate), None, size)))
            .map(_.size)
        ),
        Capability(
          "nestedApplicationJobs",
          "Candidate",
          successful(
            jobRepository.relatedJobs(
              candidateScope,
              applications
                .filter(_.candidateId == candidates.head.id)
                .map(value => JobRelationKey(value.id, value.jobId))
            )
          ).map(_.size)
        ),
        Capability(
          "nestedJobRecruiters",
          "Candidate",
          successful(
            users.relatedUsers(
              candidateScope,
              jobs.take(7).map(value => UserRelationKey.JobRecruiter(value.id, value.recruiterId))
            )
          ).map(_.size)
        ),
        Capability(
          "nestedApplicationCandidates",
          "Recruiter",
          successful(
            users.relatedUsers(
              recruiterScope,
              applications.take(7).map(value => UserRelationKey.ApplicationCandidate(value.id, value.candidateId))
            )
          ).map(_.size)
        ),
        Capability(
          "jobEligibilityProjection",
          "Candidate",
          successful(search.jobEligibility(jobs.take(7).map(_.id))).map(_.size)
        ),
        Capability(
          "candidateEligibilityProjection",
          "Recruiter",
          successful(search.candidateEligibility(candidates.take(7).map(_.id))).map(_.size)
        )
      )
      for {
        _ <- (candidates ++ recruiters :+ admin).traverse_(value => successful(users.insert(value)))
        _ <- jobs.traverse_(value =>
          MongoRepositoryTestSupport.insertOne(database, MongoCollections.Jobs, MongoHiringCodecs.job(value))
        )
        _ <- applications.traverse_(value =>
          MongoRepositoryTestSupport.insertOne(
            database,
            MongoCollections.Applications,
            MongoHiringCodecs.application(value)
          )
        )
        _ <- events.traverse_(value =>
          MongoRepositoryTestSupport.insertOne(
            database,
            MongoCollections.ApplicationEvents,
            MongoHiringCodecs.event(value)
          )
        )
        buildStarted <- IO.monotonic
        _ <- MongoHiringIndexSetup.create(database)
        buildEnded <- IO.monotonic
        _ <- MongoHiringSetup.initialize(database, Diagnostics.noop)
        revision <- IO.blocking(scala.sys.process.Process(Seq("git", "rev-parse", "HEAD")).!!.trim)
        digest <- sourceDigest
        version <- support.command(database, new Document("buildInfo", 1)).map(_.getString("version"))
        storage <- collectionStorage(fixture)
        reports <- List(1, 8)
          .traverse(concurrency => capabilities.traverse(capability => observe(fixture, capability, concurrency)))
          .map(_.flatten)
        writes <- writeWorkloads(fixture)
        report = Json.obj(
          "seed" -> Json.fromLong(20261005L),
          "jobs" -> Json.fromInt(jobs.size),
          "candidates" -> Json.fromInt(candidates.size),
          "recruiters" -> Json.fromInt(recruiters.size),
          "singletonAdmins" -> Json.fromInt(1),
          "applications" -> Json.fromInt(applications.size),
          "historyRecords" -> Json.fromInt(events.size),
          "embeddingWorkRecords" -> Json.fromInt(0),
          "outboxRecords" -> Json.fromInt(0),
          "pageSize" -> Json.fromInt(7),
          "queriesPerCapability" -> Json.fromInt(repetitions),
          "mongoVersion" -> Json.fromString(version),
          "sourceHead" -> Json.fromString(revision),
          "sourceState" -> Json.fromString(
            "Current working-tree source; HEAD identifies baseline ancestry, not an exact clean build"
          ),
          "sourceFilesSha256" -> Json.fromString(digest),
          "timestampUtc" -> Json.fromString(Instant.now().toString),
          "indexSetupMillis" -> Json.fromLong((buildEnded - buildStarted).toMillis),
          "collectionStorage" -> Json.fromValues(storage),
          "environment" -> Json.fromString(
            "Disposable local Mongo replica set; synthetic fixtures; first pass followed by repeated workload, no server cache control"
          ),
          "cpuMemoryTelemetry" -> Json.fromString(
            "Per-workload boundary cgroup samples. CPU delta is approximate; memory samples are not interval maxima."
          ),
          "vectorIndexBytes" -> Json.Null,
          "embeddingProviderRequests" -> Json.fromInt(0),
          "acceptance" -> Json.fromString(
            "Measurements only; agreed local limits exist but no optimization or paired regression comparison has been adopted"
          ),
          "optimizationPolicy" -> Json.obj(
            "status" -> Json.fromString(
              "User agreed local write/storage ceilings; acceptance requires a paired comparison"
            ),
            "maximumWriteP95RegressionPercent" -> Json.fromInt(10),
            "maximumOrdinaryIndexBytesGrowthPercent" -> Json.fromInt(25),
            "concurrency" -> Json.arr(Json.fromInt(1), Json.fromInt(8)),
            "maximumUnexpectedErrors" -> Json.fromInt(0),
            "comparison" -> Json.fromString(
              "Paired identical corpus/load/index-build conditions; insert growth is not index overhead"
            )
          ),
          "capabilities" -> Json.fromValues(reports),
          "writeWorkloads" -> writes
        )
        _ <- write(report)
      } yield {
        assertEquals(reports.size, capabilities.size * 2)
        val writeReports =
          writes.hcursor.get[Vector[Json]]("capabilities").toOption.getOrElse(fail("missing write reports"))
        (reports ++ writeReports).foreach { row =>
          val cursor = row.hcursor
          assertEquals(cursor.get[Int]("errors"), Right(0), cursor.get[String]("capability").toString)
          assertEquals(cursor.get[Int]("firstPassErrors"), Right(0))
          assertEquals(cursor.get[Int]("warmupErrors"), Right(0))
          val minimum = cursor.get[Int]("minimumReturnedPerRequest").toOption.getOrElse(fail("missing operation bound"))
          List("returnedPerRequest", "firstPassReturned", "warmupReturned").foreach { field =>
            assert(cursor.get[Vector[Int]](field).exists(_.forall(value => value >= minimum && value <= size.value)))
          }
          assertEquals(cursor.get[Int]("sampleCount"), Right(repetitions))
          assert(cursor.get[Vector[Int]]("returnedPerRequest").exists(_.size == repetitions))
          assert(cursor.get[Vector[Int]]("firstPassReturned").exists(_.size == 1))
          assert(cursor.get[Vector[Int]]("warmupReturned").exists(_.size == warmupRequests))
          if (cursor.get[String]("operation").contains("claimAndPublish")) {
            List("firstPassReconciliation", "warmupReconciliation", "measuredReconciliation").foreach { field =>
              val reconciliation = cursor.downField(field)
              assertEquals(reconciliation.get[Boolean]("positiveProgress"), Right(true))
              assertEquals(reconciliation.get[Boolean]("publishedMatchesCompleted"), Right(true))
              assertEquals(reconciliation.get[Boolean]("retryableMatchesRemaining"), Right(true))
              assertEquals(reconciliation.get[Int]("totalRecords"), Right(workRecordsPerPhase))
              assertEquals(reconciliation.get[Int]("inFlightRecords"), Right(0))
              assertEquals(reconciliation.get[Int]("activeSubjectLeases"), Right(0))
            }
          }
        }
        assert(reports.forall(_.hcursor.get[Vector[Int]]("returnedPerRequest").exists(_.size == repetitions)))
        assert(reports.forall(_.hcursor.get[Vector[Int]]("returnedPerRequest").exists(_.forall(_ <= size.value))))
      }
    }
  }

  test("explain evidence preserves metric locations and separates selected rejected and executed indexes") {
    val tree = Json.obj(
      "queryPlanner" -> Json.obj(
        "winningPlan" -> Json.obj(
          "stage" -> Json.fromString("FETCH"),
          "inputStage" -> Json
            .obj("stage" -> Json.fromString("IXSCAN"), "indexName" -> Json.fromString("selected_index"))
        ),
        "rejectedPlans" -> Json.arr(
          Json.obj("stage" -> Json.fromString("IXSCAN"), "indexName" -> Json.fromString("rejected_index"))
        )
      ),
      "executionStats" -> Json.obj(
        "nReturned" -> Json.fromInt(7),
        "totalDocsExamined" -> Json.fromInt(9),
        "executionStages" -> Json.obj(
          "stage" -> Json.fromString("IXSCAN"),
          "indexName" -> Json.fromString("selected_index"),
          "keysExamined" -> Json.fromInt(11),
          "nReturned" -> Json.fromInt(7)
        )
      ),
      "stages" -> Json.arr(
        Json.obj(
          "$lookup" -> Json.obj("from" -> Json.fromString("users")),
          "totalDocsExamined" -> Json.fromInt(3),
          "indexesUsed" -> Json.arr(Json.fromString("related_index")),
          "nReturned" -> Json.fromInt(7)
        )
      )
    )
    val plans = collectPlannerPlans(tree)
    assertEquals(plans.size, 1)
    assertEquals(plans.head.hcursor.get[String]("winningPlanPath"), Right("$.queryPlanner.winningPlan"))
    assertEquals(plans.head.hcursor.get[Vector[String]]("winningIndexNames"), Right(Vector("selected_index")))
    assertEquals(plans.head.hcursor.get[Vector[String]]("rejectedIndexNames"), Right(Vector("rejected_index")))
    val metrics = collectMetrics(tree)
    assertEquals(
      metrics.flatMap(_.hcursor.get[String]("path").toOption).toSet,
      Set("$.executionStats", "$.executionStats.executionStages", "$.stages[0]")
    )
    val lookupMetrics =
      metrics.find(_.hcursor.get[String]("path").contains("$.stages[0]")).getOrElse(fail("missing lookup metrics"))
    assertEquals(lookupMetrics.hcursor.get[String]("pipelineOperator"), Right("$lookup"))
    assertEquals(lookupMetrics.hcursor.downField("metrics").get[Int]("totalDocsExamined"), Right(3))
    val executed = collectExecutionIndexes(tree)
    assertEquals(
      executed.flatMap(_.hcursor.get[Vector[String]]("indexNames").toOption).flatten.toSet,
      Set("selected_index", "related_index")
    )
    val scan = collectPlannerPlans(
      Json.obj(
        "queryPlanner" -> Json.obj(
          "winningPlan" -> Json.obj("stage" -> Json.fromString("COLLSCAN")),
          "rejectedPlans" -> Json.arr()
        )
      )
    )
    assertEquals(scan.head.hcursor.get[Vector[String]]("winningIndexNames"), Right(Vector.empty[String]))
    assertEquals(scan.head.hcursor.get[Vector[String]]("winningStages"), Right(Vector("COLLSCAN")))
  }

  test("production candidate predicates execute the true false absent consent truth table on Mongo") {
    mongoResource.use { fixture =>
      val model = "synthetic-model"
      val optedIn = CandidateProfile(
        Set("Scala"),
        Some("Synthetic experience"),
        Some("private-fixture-resume"),
        Some(CandidateResidence("Cyprus", Some("Nicosia"))),
        Some(CandidateAvailabilityStatus.AVAILABLE_NOW),
        recruiterSearchOptIn = true
      )
      val variations = List(
        ("true-matching", optedIn, true, model, AccountStatus.Active),
        (
          "false-missing-private",
          optedIn.copy(recruiterSearchOptIn = false, currentResidence = None, availabilityStatus = None),
          true,
          model,
          AccountStatus.Active
        ),
        (
          "absent-missing-private",
          optedIn.copy(recruiterSearchOptIn = false, currentResidence = None, availabilityStatus = None),
          false,
          model,
          AccountStatus.Active
        ),
        (
          "true-missing-private",
          optedIn.copy(currentResidence = None, availabilityStatus = None),
          true,
          model,
          AccountStatus.Active
        ),
        (
          "true-wrong-city",
          optedIn.copy(currentResidence = Some(CandidateResidence("Cyprus", Some("Limassol")))),
          true,
          model,
          AccountStatus.Active
        ),
        ("inactive", optedIn, true, model, AccountStatus.Deleted),
        ("wrong-model", optedIn, true, "other-model", AccountStatus.Active),
        ("missing-skills", optedIn.copy(skills = Set("Java")), true, model, AccountStatus.Active)
      )
      val values = variations.zipWithIndex.map { case ((name, profile, consentPresent, currentModel, status), index) =>
        val value = user(100 + index, UserRole.Candidate).copy(
          name = name,
          profile = Some(UserProfile.Candidate(profile)),
          embedding = Some(
            EntityEmbedding(
              List(0.1f, 0.2f),
              EmbeddingMeta(
                currentModel,
                com.example.graphQL.cats.shared.crypto.SourceHash.sha256(SearchableText.candidate(profile)),
                now
              )
            )
          ),
          accountStatus = status
        )
        val document = MongoHiringCodecs.user(value)
        if (!consentPresent) { val _ = document.get("profile", classOf[Document]).remove("recruiterSearchOptIn") }
        (value, document)
      }
      val search = new MongoSemanticSearchRepository(
        fixture.database,
        "job-vector",
        "candidate-vector",
        "job-lexical",
        "candidate-lexical",
        100,
        diagnostics = Diagnostics.noop
      )
      val query = VectorSearchQuery(
        List(0.1f, 0.2f),
        None,
        JobSearchFilter(None, Set.empty, None),
        size,
        SearchMode.VECTOR,
        model,
        support.deterministicId("consent-query"),
        candidateFilters = ValidatedCandidateMatchFilters
          .from(CandidateMatchFilters(List("scala"), Some("cyprus"), Some("nicosia"), Some("AVAILABLE_NOW")))
          .fold(errors => fail(errors.toString), identity)
      )
      for {
        _ <- values.traverse_ { case (_, document) =>
          MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.Users, document)
        }
        collection <- MongoRepositoryTestSupport.collection(fixture.database, MongoCollections.Users)
        selected <- collection.find(search.candidateFilter(query, includeEmbeddingModel = true)).stream.compile.toList
        projected <- successful(search.candidateEligibility(values.map(_._1.id)))
        empty <- successful(search.candidateEligibility(Nil))
        overflow <- search
          .candidateEligibility((0 to 100).toList.map(index => UserId(support.deterministicId(s"overflow:$index"))))
          .value
      } yield {
        assertEquals(
          selected.map(_.getString("name")).toSet,
          Set("true-matching", "false-missing-private", "absent-missing-private")
        )
        assertEquals(projected.size, values.size)
        assert(projected.flatMap(_.profile).forall(_.resumeRef.isEmpty))
        assertEquals(empty, Nil)
        assert(overflow.isLeft)
      }
    }
  }

  private def collectionStorage(fixture: support.Fixture): IO[List[Json]] =
    MongoHiringMigrations.ownedCollections.toList.traverse { name =>
      support
        .command(fixture.database, new Document("collStats", name))
        .map(value =>
          Json.obj(
            "collection" -> Json.fromString(name),
            "documents" -> Json.fromLong(value.get("count").asInstanceOf[Number].longValue),
            "collectionIndexBytes" -> Json.fromLong(value.get("totalIndexSize").asInstanceOf[Number].longValue),
            "collectionStorageBytes" -> Json.fromLong(value.get("storageSize").asInstanceOf[Number].longValue)
          )
        )
        .handleError(error =>
          Json.obj(
            "collection" -> Json.fromString(name),
            "status" -> Json.fromString(s"Unavailable: ${error.getClass.getSimpleName}")
          )
        )
    }

  private def clearCollection(fixture: support.Fixture, name: String): IO[Unit] =
    support
      .command(
        fixture.database,
        new Document("delete", name).append(
          "deletes",
          List(new Document("q", new Document()).append("limit", 0)).asJava
        )
      )
      .void

  private def writeWorkloads(readFixture: support.Fixture): IO[Json] =
    support.isolatedFixture(readFixture).use { fixture =>
      (cats.effect.Ref.of[IO, Int](0), IO.realTimeInstant).tupled.flatMap { case (sequence, measuredAt) =>
        val work = new MongoEmbeddingWorkRepository(fixture.database, Diagnostics.noop)
        val outbox =
          MongoOperationalEventOutboxRepository.transactional(fixture.database, fixture.client, Diagnostics.noop)
        val users = new MongoUserRepository(
          fixture.database,
          MongoRepositoryTestSupport.noTransaction,
          MongoEmbeddingWorkEnqueuer.disabled,
          Diagnostics.noop
        )
        val jobRepository = MongoJobRepository.transactional(fixture.database, fixture.client, work, Diagnostics.noop)
        // Distinct and shared subjects exercise the same production claim contract.
        // claim maps expected lease conflicts to None, so an empty attempt is legitimate.
        // Actual progress and durable post-phase reconciliation are required independently.
        val publisherActors = (0 until workRecordsPerPhase).toList.map(index => user(1000 + index, UserRole.Candidate))
        val prepareWork = clearCollection(fixture, MongoCollections.EmbeddingWork) *>
          jobs.traverse_(job =>
            successful(work.enqueue(EmbeddingWorkKey(EmbeddingWorkKind.Job, job.id.value.toString), now))
          )
        def prepareOutbox(sharedSubject: Boolean): IO[Unit] =
          clearCollection(fixture, MongoCollections.EventOutbox) *>
            clearCollection(fixture, MongoCollections.OutboxSubjectFences) *>
            jobs.zipWithIndex.traverse_ { case (job, index) =>
              val event = OperationalEvents.jobViewed(
                support.deterministicId(s"publisher-event:$index"),
                job.id,
                publisherActors(if (sharedSubject) 0 else index).id,
                None,
                None,
                now
              )
              MongoHiringCodecs
                .outboxRecord(event, now)
                .fold(
                  error => IO.raiseError(new AssertionError(error)),
                  document =>
                    MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.EventOutbox, document)
                )
            }
        def publicationCount(query: Document): IO[Int] =
          support
            .command(
              fixture.database,
              new Document("count", MongoCollections.EventOutbox).append("query", query)
            )
            .map(_.get("n").asInstanceOf[Number].intValue)
        val reconcilePublication: List[Observation] => IO[Json] = observations =>
          for {
            published <- publicationCount(new Document(MongoFields.State, "Published"))
            retryable <- publicationCount(new Document(MongoFields.State, "Retryable"))
            inFlight <- publicationCount(new Document(MongoFields.State, "InFlight"))
            total <- publicationCount(new Document())
            leases <- support
              .command(
                fixture.database,
                new Document("count", MongoCollections.OutboxSubjectFences).append(
                  "query",
                  new Document(MongoFields.LeaseToken, new Document("$exists", true))
                )
              )
              .map(_.get("n").asInstanceOf[Number].intValue)
            completed = observations.map(_.returned).sum
          } yield Json.obj(
            "completedOperations" -> Json.fromInt(completed),
            "publishedRecords" -> Json.fromInt(published),
            "retryableRecords" -> Json.fromInt(retryable),
            "inFlightRecords" -> Json.fromInt(inFlight),
            "totalRecords" -> Json.fromInt(total),
            "activeSubjectLeases" -> Json.fromInt(leases),
            "positiveProgress" -> Json.fromBoolean(completed > 0),
            "publishedMatchesCompleted" -> Json.fromBoolean(published == completed),
            "retryableMatchesRemaining" -> Json.fromBoolean(retryable == workRecordsPerPhase - completed),
            "claimContract" -> Json.fromString(
              "MongoOperationalEventOutboxRepository.claim maps expected lease Conflict to an empty claim; empty attempts are counted separately and never counted as completed publications."
            )
          )
        val prepareJobWrites = List(MongoCollections.Jobs, MongoCollections.EmbeddingWork, MongoCollections.EventOutbox)
          .traverse_(clearCollection(fixture, _)) *> sequence.set(0)
        val createJob = for {
          index <- sequence.getAndUpdate(_ + 1)
          id = support.deterministicId(s"created-job:$index")
          eventId = support.deterministicId(s"created-job-event:$index")
          value = jobs.head.copy(id = JobId(id))
          event = OperationalEvents
            .jobEvent(OperationalEventType.JOB_CREATED, eventId, value, value.recruiterId, now)
            .fold(error => fail(error.toString), identity)
          _ <- successful(jobRepository.createWithEvents(value, now, List(event), MutationWriteContext.directWrite))
        } yield 1
        val claimAndPublish =
          successful(
            outbox.claim(
              "synthetic-publisher",
              "hiring-publisher-" + support.deterministicId("synthetic-publisher-generation").toString,
              now,
              now.plusSeconds(60),
              1
            )
          )
            .flatMap(values =>
              values
                .traverse_(value =>
                  // Anchor synthetic retention beyond this run so TTL cannot invalidate reconciliation.
                  successful(
                    outbox.markPublished(value.event.eventId, value.leaseToken, now, measuredAt.plusSeconds(86400))
                  )
                )
                .as(values.size)
            )
        val capabilities = List(
          Capability(
            "embeddingClaimComplete",
            "Worker",
            successful(work.claim("synthetic-worker", now, now.plusSeconds(60)))
              .flatMap(_.fold(IO.pure(0))(claim => successful(work.complete(claim)).as(1))),
            prepareWork,
            minimumReturned = 1,
            operation = "claimAndComplete"
          ),
          Capability(
            "outboxClaimPublish",
            "Publisher",
            claimAndPublish,
            prepareOutbox(sharedSubject = false),
            operation = "claimAndPublish",
            reconcile = reconcilePublication
          ),
          Capability(
            "outboxClaimPublishSharedSubject",
            "Publisher",
            claimAndPublish,
            prepareOutbox(sharedSubject = true),
            operation = "claimAndPublish",
            reconcile = reconcilePublication
          ),
          Capability(
            "jobCreateWithEmbeddingWorkAndEvent",
            "Recruiter",
            createJob,
            prepareJobWrites,
            minimumReturned = 1,
            operation = "transactionalWrite"
          )
        )
        for {
          _ <- (publisherActors ++ recruiters).traverse_(value => successful(users.insert(value)))
          _ <- jobs.traverse_(value =>
            MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.Jobs, MongoHiringCodecs.job(value))
          )
          started <- IO.monotonic
          _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
          ended <- IO.monotonic
          reports <- capabilities
            .traverse(capability => List(1, 8).traverse(observe(fixture, capability, _)))
            .map(_.flatten)
        } yield Json.obj(
          "isolation" -> Json
            .fromString("Separate owned database/client on the same disposable container; no read-corpus writes"),
          "publicationSubjects" -> Json.fromString(
            "Distinct-subject scenario: 128 active candidates. Shared-subject scenario: one active candidate for all 128 events. Jobs remain distinct in both."
          ),
          "publishedRetention" -> Json
            .fromString("Anchored 24 hours beyond run start to prevent synthetic TTL depletion during reconciliation"),
          "recordsReplenishedPerPhase" -> Json.fromInt(workRecordsPerPhase),
          "setupMillis" -> Json.fromLong((ended - started).toMillis),
          "storageInterpretation" -> Json.fromString(
            "Before/after collection samples reflect this workload's document and state changes; not a paired index-overhead comparison"
          ),
          "capabilities" -> Json.fromValues(reports)
        )
      }
    }

  private def runObservations(capability: Capability, count: Int, concurrency: Int): IO[List[Observation]] =
    (0 until count).toList
      .grouped(concurrency)
      .toList
      .traverse(batch =>
        batch.parTraverse(_ =>
          capability.run.attempt.timed.map { case (elapsed, result) =>
            val error = result.left.toOption.map {
              case failure: ObservedRepositoryFailure => failure.error.toString
              case failure                            => failure.getClass.getSimpleName
            }
            Observation(elapsed.toNanos.toDouble / 1000000d, result.toOption.getOrElse(0), error)
          }
        )
      )
      .map(_.flatten)

  private def observe(fixture: support.Fixture, capability: Capability, concurrency: Int): IO[Json] =
    for {
      _ <- capability.prepare
      firstPass <- runObservations(capability, 1, 1)
      firstPassReconciliation <- capability.reconcile(firstPass)
      _ <- capability.prepare
      warmup <- runObservations(capability, warmupRequests, concurrency)
      warmupReconciliation <- capability.reconcile(warmup)
      _ <- capability.prepare
      storageBefore <- if (capability.operation != "read") collectionStorage(fixture) else IO.pure(Nil)
      resourcesBefore <- fixture.sampleResources
      _ <- fixture.commands.clear
      started <- IO.monotonic
      observations <- runObservations(capability, repetitions, concurrency)
      ended <- IO.monotonic
      commands <- fixture.commands.snapshot
      resourcesAfter <- fixture.sampleResources
      measuredReconciliation <- capability.reconcile(observations)
      storageAfter <- if (capability.operation != "read") collectionStorage(fixture) else IO.pure(Nil)
      shapes = commands.groupBy(command => sanitizeCommand(command).noSpaces).toList.sortBy(_._1)
      explains <- shapes.traverse { case (_, samples) => explain(fixture.database, samples.head) }
      totalMillis = (ended - started).toNanos.toDouble / 1000000d
      completedLatencies = observations.filter(value => value.error.isEmpty && value.returned > 0).map(_.latencyMillis)
    } yield Json.obj(
      "capability" -> Json.fromString(capability.name),
      "actor" -> Json.fromString(capability.actor),
      "operation" -> Json.fromString(capability.operation),
      "concurrency" -> Json.fromInt(concurrency),
      "p50Millis" -> Json.fromDoubleOrNull(percentile(observations.map(_.latencyMillis), 0.50)),
      "p95Millis" -> Json.fromDoubleOrNull(percentile(observations.map(_.latencyMillis), 0.95)),
      "p99Millis" -> Json.fromDoubleOrNull(percentile(observations.map(_.latencyMillis), 0.99)),
      "completionSampleCount" -> Json.fromInt(if (capability.operation == "read") 0 else completedLatencies.size),
      "completionP50Millis" -> completionPercentile(capability, completedLatencies, 0.50),
      "completionP95Millis" -> completionPercentile(capability, completedLatencies, 0.95),
      "completionP99Millis" -> completionPercentile(capability, completedLatencies, 0.99),
      "completionTimingInterpretation" -> Json.fromString(
        "Non-read operations with returned count >0 and no error; completion sample count can be smaller than attempt count. Null for reads or no completed writes. Write regression compares completion p95 under paired identical workload/subject distribution."
      ),
      "firstPassMillis" -> Json.fromDoubleOrNull(firstPass.headOption.fold(0d)(_.latencyMillis)),
      "firstPassErrors" -> Json.fromInt(firstPass.count(_.error.nonEmpty)),
      "firstPassErrorCategories" -> Json.fromValues(firstPass.flatMap(_.error).map(Json.fromString)),
      "firstWarmedRequestMillis" -> Json.fromDoubleOrNull(observations.headOption.fold(0d)(_.latencyMillis)),
      "throughputRequestsPerSecond" -> Json.fromDoubleOrNull(repetitions.toDouble * 1000d / totalMillis),
      "completedOperations" -> Json.fromInt(
        if (capability.operation == "read") observations.count(_.error.isEmpty) else observations.map(_.returned).sum
      ),
      "completedOperationsPerSecond" -> Json.fromDoubleOrNull(
        (if (capability.operation == "read") observations.count(_.error.isEmpty)
         else observations.map(_.returned).sum).toDouble * 1000d / totalMillis
      ),
      "emptyOperations" -> Json.fromInt(observations.count(value => value.error.isEmpty && value.returned == 0)),
      "elapsedMillis" -> Json.fromDoubleOrNull(totalMillis),
      "sampleCount" -> Json.fromInt(repetitions),
      "warmupRequests" -> Json.fromInt(warmupRequests),
      "warmupErrors" -> Json.fromInt(warmup.count(_.error.nonEmpty)),
      "warmupErrorCategories" -> Json.fromValues(warmup.flatMap(_.error).map(Json.fromString)),
      "warmupP50Millis" -> Json.fromDoubleOrNull(percentile(warmup.map(_.latencyMillis), 0.50)),
      "errors" -> Json.fromInt(observations.count(_.error.nonEmpty)),
      "returnedPerRequest" -> Json.fromValues(observations.map(value => Json.fromInt(value.returned))),
      "errorCategories" -> Json.fromValues(observations.flatMap(_.error).map(Json.fromString)),
      "minimumReturnedPerRequest" -> Json.fromInt(capability.minimumReturned),
      "firstPassReturned" -> Json.fromValues(firstPass.map(value => Json.fromInt(value.returned))),
      "warmupReturned" -> Json.fromValues(warmup.map(value => Json.fromInt(value.returned))),
      "firstPassReconciliation" -> firstPassReconciliation,
      "warmupReconciliation" -> warmupReconciliation,
      "measuredReconciliation" -> measuredReconciliation,
      "resourceSamples" -> resourceSamples(resourcesBefore, resourcesAfter),
      "collectionStorageBefore" -> Json.fromValues(storageBefore),
      "collectionStorageAfter" -> Json.fromValues(storageAfter),
      "timingScope" -> Json.fromString(
        "Production adapter calls including retries/cursor decoding; transactional job writes also include deterministic fixture allocation. Preparation, boundary probes, storage queries and explains are excluded."
      ),
      "firstPassInterpretation" -> Json.fromString(
        "First invocation for this capability/concurrency, before its dedicated warmup; not a cold-cache claim. Read corpus is shared between concurrency runs."
      ),
      "documentHydration" -> Json.fromString(
        if (capability.name.endsWith("EligibilityProjection"))
          "Selective authoritative projection; embedding vectors excluded; candidate email, name and resume references excluded"
        else if (capability.operation != "read")
          "Full bounded work or event records; publication updates and leases retained"
        else
          "Full operational entity documents; actor/parent selection stays in Mongo; GraphQL field authorization is separate"
      ),
      "workerAcceptance" -> Json.fromString(
        "Contention errors are observations, not suppressed or a performance acceptance pass"
      ),
      "commandCount" -> Json.fromInt(commands.size),
      "commandsPerRequest" -> Json.fromDoubleOrNull(commands.size.toDouble / repetitions),
      "commandShapes" -> Json.fromValues(shapes.map { case (_, samples) =>
        Json.obj("count" -> Json.fromInt(samples.size), "shape" -> sanitizeCommand(samples.head))
      }),
      "explainTiming" -> Json.fromString(
        "After workload; stats reflect then-current data. Write explains select without applying writes."
      ),
      "executionStats" -> Json.fromValues(explains)
    )

  private def completionPercentile(capability: Capability, values: List[Double], fraction: Double): Json =
    if (capability.operation == "read" || values.isEmpty) Json.Null
    else Json.fromDoubleOrNull(percentile(values, fraction))

  private def resourceSamples(before: Json, after: Json): Json = {
    val cpuDelta = for {
      start <- before.hcursor.get[Long]("cpuUsageMicros").toOption
      end <- after.hcursor.get[Long]("cpuUsageMicros").toOption
      if end >= start
    } yield end - start
    val elapsed = for {
      start <- before.hcursor.get[Long]("monotonicNanos").toOption
      end <- after.hcursor.get[Long]("monotonicNanos").toOption
      if end > start
    } yield end - start
    val averageCores = for { delta <- cpuDelta; nanos <- elapsed } yield delta.toDouble * 1000d / nanos.toDouble
    Json.obj(
      "samples" -> Json.arr(before, after),
      "cpuUsageMicrosDelta" -> cpuDelta.fold(Json.Null)(Json.fromLong),
      "approximateAverageCpuCores" -> averageCores.fold(Json.Null)(Json.fromDoubleOrNull),
      "limitations" -> Json.fromString(
        "Two boundary samples only; memory maxima between samples are unknown. CPU interval includes probe overhead and container background work. No attribution to individual requests or host isolation."
      )
    )
  }

  private def percentile(values: List[Double], fraction: Double): Double = {
    val ordered = values.sorted
    ordered.lift(math.max(0, math.ceil(ordered.size * fraction).toInt - 1)).getOrElse(0d)
  }

  private def sanitize(value: BsonValue): Json =
    if (value.isDocument)
      Json.fromFields(value.asDocument.entrySet().asScala.toList.map(entry => entry.getKey -> sanitize(entry.getValue)))
    else if (value.isArray) Json.fromValues(value.asArray.getValues.asScala.map(sanitize))
    else if (value.isString)
      Json.fromString(
        if (
          value.asString.getValue.startsWith("$") || MongoHiringMigrations.ownedCollections
            .contains(value.asString.getValue) ||
          Set(
            "Candidate",
            "Recruiter",
            "Admin",
            "Open",
            "Closed",
            "Active",
            "Deleted",
            "Ready",
            "Retry",
            "Processing",
            "Retryable",
            "InFlight"
          ).contains(value.asString.getValue)
        ) value.asString.getValue
        else "[VALUE]"
      )
    else if (value.isInt32) Json.fromInt(value.asInt32.getValue)
    else if (value.isInt64) Json.fromLong(value.asInt64.getValue)
    else if (value.isBoolean) Json.fromBoolean(value.asBoolean.getValue)
    else Json.fromString("[VALUE]")

  private def sanitizeCommand(command: BsonDocument): Json = {
    val fields = Set(
      "find",
      "aggregate",
      "findAndModify",
      "update",
      "delete",
      "insert",
      "getMore",
      "collection",
      "commitTransaction",
      "abortTransaction",
      "filter",
      "query",
      "sort",
      "limit",
      "projection",
      "pipeline",
      "updates",
      "deletes",
      "cursor",
      "batchSize",
      "upsert",
      "new"
    )
    Json.fromFields(command.entrySet().asScala.toList.filter(entry => fields.contains(entry.getKey)).map { entry =>
      val value =
        if (
          Set("find", "aggregate", "findAndModify", "update", "delete", "insert")
            .contains(entry.getKey) && entry.getValue.isString
        )
          Json.fromString(entry.getValue.asString.getValue)
        else sanitize(entry.getValue)
      entry.getKey -> value
    })
  }

  private def explain(database: mongo4cats.database.MongoDatabase[IO], observed: BsonDocument): IO[Json] = {
    val commandName = observed.getFirstKey
    if (!Set("find", "aggregate", "findAndModify").contains(commandName))
      IO.pure(
        Json.obj(
          "command" -> Json.fromString(commandName),
          "status" -> Json.fromString("No executionStats requested for this write/control command")
        )
      )
    else {
      val command = Document.parse(observed.toJson)
      List("$db", "lsid", "txnNumber", "startTransaction", "autocommit", "readConcern", "writeConcern", "maxTimeMS")
        .foreach(command.remove)
      support.command(database, new Document("explain", command).append("verbosity", "executionStats")).attempt.map {
        case Left(error) =>
          Json.obj(
            "command" -> Json.fromString(commandName),
            "status" -> Json.fromString(s"Unavailable: ${error.getClass.getSimpleName}")
          )
        case Right(result) =>
          val tree = parse(result.toJson).toOption.getOrElse(Json.Null)
          Json.obj(
            "command" -> Json.fromString(commandName),
            "metrics" -> Json.fromValues(collectMetrics(tree)),
            "plannerPlans" -> Json.fromValues(collectPlannerPlans(tree)),
            "executionIndexReferences" -> Json.fromValues(collectExecutionIndexes(tree))
          )
      }
    }
  }

  private def children(json: Json, path: String): List[(String, Json)] =
    json.asObject.fold(json.asArray.toList.flatten.zipWithIndex.map { case (value, index) =>
      s"$path[$index]" -> value
    }) { obj =>
      obj.toList.map { case (name, value) => s"$path.$name" -> value }
    }

  private def collectMetrics(json: Json, path: String = "$"): List[Json] = {
    val fields =
      Set(
        "nReturned",
        "totalKeysExamined",
        "totalDocsExamined",
        "executionTimeMillis",
        "executionTimeMillisEstimate",
        "keysExamined",
        "docsExamined"
      )
    val current = json.asObject.toList.flatMap { obj =>
      val metrics = obj.toList.filter(entry => fields.contains(entry._1) && entry._2.isNumber)
      Option
        .when(metrics.nonEmpty)(
          Json.obj(
            "path" -> Json.fromString(path),
            "stage" -> obj("stage").filter(_.isString).getOrElse(Json.Null),
            "pipelineOperator" -> obj.keys.find(_.startsWith("$")).fold(Json.Null)(Json.fromString),
            "metrics" -> Json.fromFields(metrics)
          )
        )
        .toList
    }
    current ++ children(json, path).flatMap { case (nextPath, value) => collectMetrics(value, nextPath) }
  }

  private def collectIndexes(json: Json): List[String] =
    json.asObject.fold(json.asArray.toList.flatten.toList.flatMap(collectIndexes)) { obj =>
      obj("indexName").flatMap(_.asString).toList ++ obj.values.toList.flatMap(collectIndexes)
    }

  private def collectStages(json: Json): List[String] =
    json.asObject.fold(json.asArray.toList.flatten.toList.flatMap(collectStages)) { obj =>
      obj("stage").flatMap(_.asString).toList ++ obj.values.toList.flatMap(collectStages)
    }

  private def collectPlannerPlans(json: Json, path: String = "$"): List[Json] = {
    val current = json.asObject.toList.flatMap { obj =>
      obj("winningPlan").toList.map { winning =>
        val rejected = obj("rejectedPlans").getOrElse(Json.arr())
        Json.obj(
          "path" -> Json.fromString(path),
          "winningPlanPath" -> Json.fromString(s"$path.winningPlan"),
          "winningIndexNames" -> Json.fromValues(collectIndexes(winning).distinct.sorted.map(Json.fromString)),
          "winningStages" -> Json.fromValues(collectStages(winning).distinct.sorted.map(Json.fromString)),
          "rejectedPlansPath" -> Json.fromString(s"$path.rejectedPlans"),
          "rejectedIndexNames" -> Json.fromValues(collectIndexes(rejected).distinct.sorted.map(Json.fromString)),
          "rejectedStages" -> Json.fromValues(collectStages(rejected).distinct.sorted.map(Json.fromString))
        )
      }
    }
    current ++ children(json, path).flatMap { case (nextPath, value) => collectPlannerPlans(value, nextPath) }
  }

  private def collectExecutionIndexes(json: Json, path: String = "$"): List[Json] = {
    val current = json.asObject.toList.flatMap { obj =>
      val names = obj("indexesUsed").flatMap(_.asArray).toList.flatten.flatMap(_.asString).toList ++
        Option.when(path.contains(".executionStats"))(obj("indexName").flatMap(_.asString).toList).toList.flatten
      Option
        .when(names.nonEmpty)(
          Json.obj(
            "path" -> Json.fromString(path),
            "stage" -> obj("stage").filter(_.isString).getOrElse(Json.Null),
            "indexNames" -> Json.fromValues(names.distinct.sorted.map(Json.fromString))
          )
        )
        .toList
    }
    current ++ children(json, path).flatMap { case (nextPath, value) => collectExecutionIndexes(value, nextPath) }
  }

  private def write(report: Json): IO[Unit] = IO.blocking {
    val directory = Paths.get(".local/data/mongodb-access-evaluation")
    val _ = Files.createDirectories(directory)
    val _ =
      Files.writeString(
        directory.resolve("operational-access-measurements.json"),
        report.spaces2,
        StandardCharsets.UTF_8
      )
    ()
  }

  private def sourceDigest: IO[String] = IO.blocking {
    val digest = java.security.MessageDigest.getInstance("SHA-256")
    List("src/main", "src/test", "src/it").foreach { root =>
      val stream = Files.walk(Paths.get(root))
      try {
        stream
          .iterator()
          .asScala
          .filter(path => Files.isRegularFile(path) && path.toString.endsWith(".scala"))
          .toList
          .sortBy(_.toString)
          .foreach { path =>
            digest.update(path.toString.getBytes(StandardCharsets.UTF_8))
            digest.update(0.toByte)
            digest.update(Files.readAllBytes(path))
          }
      } finally stream.close()
    }
    digest.digest().map(value => f"${value & 0xff}%02x").mkString
  }
}
