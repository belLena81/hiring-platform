package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.*
import com.example.graphQL.cats.domain.pagination.*
import com.example.graphQL.cats.service.{ActorContext, Diagnostics, RepositoryError}
import com.example.graphQL.cats.service.auth.ActorAuthorization
import com.example.graphQL.cats.service.events.{
  OperationalAggregateType,
  OperationalEventEnvelope,
  OperationalEventType
}
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.read.{HiringReadScope, JobRelationKey, UserRelationKey}
import com.example.graphQL.cats.service.search.{CandidateMatchFilters, JobSearchFilter, VectorSearchQuery}
import io.circe.Json
import io.circe.parser.parse
import munit.CatsEffectSuite
import org.bson.{BsonDocument, BsonValue, Document}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.Instant
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** A bounded baseline of production adapter calls, separate from performance acceptance. */
final class MongoOperationalAccessEvaluationIntegrationSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes
  private val support = MongoAccessEvaluationSupport
  private val now = Instant.parse("2026-10-05T12:00:00Z")
  private val size = PageSize.fromInt(7).toOption.getOrElse(fail("invalid fixture page size"))
  private val page = JobPageRequest(None, None, size)
  private val applicationsPage = ApplicationPageRequest(None, None, size)
  private val historyPage = ApplicationEventPageRequest(None, size)
  private val repetitions = 20

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

  private final case class Capability(name: String, actor: String, run: IO[Int])
  private final case class Observation(latencyMillis: Double, returned: Int, error: Option[String])

  test("production operational access records bounded commands, plans and reproducible local baselines") {
    support.resource.use { fixture =>
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
      val work = new MongoEmbeddingWorkRepository(database, Diagnostics.noop)
      val outbox = MongoOperationalEventOutboxRepository.transactional(database, fixture.client, Diagnostics.noop)
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
        ),
        Capability(
          "embeddingClaimComplete",
          "Worker",
          successful(work.claim("synthetic-worker", now, now.plusSeconds(60)))
            .flatMap(_.fold(IO.pure(0))(claim => successful(work.complete(claim)).as(1)))
        ),
        Capability(
          "outboxClaimPublish",
          "Publisher",
          successful(outbox.claim("synthetic-publisher", "synthetic-transaction", now, now.plusSeconds(60), 1)).flatMap(
            values =>
              values
                .traverse_(value =>
                  successful(outbox.markPublished(value.event.eventId, value.leaseToken, now, now.plusSeconds(86400)))
                )
                .as(values.size)
          )
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
        _ <- (0 until 64).toList.traverse_(index =>
          successful(work.enqueue(EmbeddingWorkKey(EmbeddingWorkKind.Job, jobs(index).id.value.toString), now))
        )
        _ <- (0 until 64).toList.traverse_ { index =>
          val event = OperationalEventEnvelope(
            support.deterministicId(s"outbox:$index"),
            OperationalEventType.JOB_VIEWED,
            now,
            OperationalAggregateType.Job,
            jobs(index).id.value.toString,
            candidates(index % 32).id,
            Json.obj("jobId" -> Json.fromString(jobs(index).id.value.toString))
          )
          MongoHiringCodecs
            .outboxRecord(event, now)
            .fold(
              error => IO.raiseError(new AssertionError(error)),
              document => MongoRepositoryTestSupport.insertOne(database, MongoCollections.EventOutbox, document)
            )
        }
        buildStarted <- IO.monotonic
        _ <- MongoHiringIndexSetup.create(database)
        buildEnded <- IO.monotonic
        _ <- MongoHiringSetup.initialize(database, Diagnostics.noop)
        revision <- IO.blocking(scala.sys.process.Process(Seq("git", "rev-parse", "HEAD")).!!.trim)
        digest <- sourceDigest
        version <- support.command(database, new Document("buildInfo", 1)).map(_.getString("version"))
        storage <- MongoHiringMigrations.ownedCollections.toList.traverse { name =>
          support
            .command(database, new Document("collStats", name))
            .map(value =>
              Json.obj(
                "collection" -> Json.fromString(name),
                "collectionIndexBytes" -> Json.fromLong(value.get("totalIndexSize").asInstanceOf[Number].longValue)
              )
            )
            .handleError(error =>
              Json.obj(
                "collection" -> Json.fromString(name),
                "collectionIndexBytes" -> Json.Null,
                "status" -> Json.fromString(s"Unavailable: ${error.getClass.getSimpleName}")
              )
            )
        }
        reports <- List(1, 8)
          .traverse(concurrency => capabilities.traverse(capability => observe(fixture, capability, concurrency)))
          .map(_.flatten)
        report = Json.obj(
          "seed" -> Json.fromLong(20261005L),
          "jobs" -> Json.fromInt(jobs.size),
          "candidates" -> Json.fromInt(candidates.size),
          "recruiters" -> Json.fromInt(recruiters.size),
          "singletonAdmins" -> Json.fromInt(1),
          "applications" -> Json.fromInt(applications.size),
          "historyRecords" -> Json.fromInt(events.size),
          "embeddingWorkRecords" -> Json.fromInt(64),
          "outboxRecords" -> Json.fromInt(64),
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
          "cpuMemoryTelemetry" -> Json.fromString("Unavailable; no server resource sampler configured"),
          "vectorIndexBytes" -> Json.Null,
          "embeddingProviderRequests" -> Json.fromInt(0),
          "acceptance" -> Json.fromString("Baseline only; no numerical performance target or optimization winner"),
          "capabilities" -> Json.fromValues(reports)
        )
        _ <- write(report)
      } yield {
        assertEquals(reports.size, capabilities.size * 2)
        assert(
          reports
            .filterNot(_.hcursor.get[String]("actor").exists(Set("Worker", "Publisher")))
            .forall(_.hcursor.get[Int]("errors").contains(0))
        )
        assert(reports.forall(_.hcursor.get[Vector[Int]]("returnedPerRequest").exists(_.size == repetitions)))
        assert(reports.forall(_.hcursor.get[Vector[Int]]("returnedPerRequest").exists(_.forall(_ <= size.value))))
      }
    }
  }

  test("production candidate predicates execute the true false absent consent truth table on Mongo") {
    support.resource.use { fixture =>
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
        candidateFilters = CandidateMatchFilters(List("scala"), Some("cyprus"), Some("nicosia"), Some("AVAILABLE_NOW"))
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

  private def observe(fixture: support.Fixture, capability: Capability, concurrency: Int): IO[Json] =
    for {
      _ <- fixture.commands.clear
      started <- IO.monotonic
      observations <- (0 until repetitions).toList
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
      ended <- IO.monotonic
      commands <- fixture.commands.snapshot
      shapes = commands.groupBy(command => sanitizeCommand(command).noSpaces).toList.sortBy(_._1)
      explains <- shapes.traverse { case (_, samples) => explain(fixture.database, samples.head) }
      totalMillis = (ended - started).toNanos.toDouble / 1000000d
    } yield Json.obj(
      "capability" -> Json.fromString(capability.name),
      "actor" -> Json.fromString(capability.actor),
      "concurrency" -> Json.fromInt(concurrency),
      "p50Millis" -> Json.fromDoubleOrNull(percentile(observations.map(_.latencyMillis), 0.50)),
      "p95Millis" -> Json.fromDoubleOrNull(percentile(observations.map(_.latencyMillis), 0.95)),
      "p99Millis" -> Json.fromDoubleOrNull(percentile(observations.map(_.latencyMillis), 0.99)),
      "firstRequestMillis" -> Json.fromDoubleOrNull(observations.headOption.fold(0d)(_.latencyMillis)),
      "throughputRequestsPerSecond" -> Json.fromDoubleOrNull(repetitions.toDouble * 1000d / totalMillis),
      "elapsedMillis" -> Json.fromDoubleOrNull(totalMillis),
      "sampleCount" -> Json.fromInt(repetitions),
      "warmupRequests" -> Json.fromInt(0),
      "errors" -> Json.fromInt(observations.count(_.error.nonEmpty)),
      "returnedPerRequest" -> Json.fromValues(observations.map(value => Json.fromInt(value.returned))),
      "errorCategories" -> Json.fromValues(observations.flatMap(_.error).map(Json.fromString)),
      "documentHydration" -> Json.fromString(
        if (capability.name.endsWith("EligibilityProjection"))
          "Selective authoritative projection; embedding vectors excluded; candidate email, name and resume references excluded"
        else if (Set("Worker", "Publisher").contains(capability.actor))
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
            "winningAndRejectedIndexNames" -> Json.fromValues(collectIndexes(tree).distinct.sorted.map(Json.fromString))
          )
      }
    }
  }

  private def collectMetrics(json: Json): List[Json] = {
    val fields =
      Set("nReturned", "totalKeysExamined", "totalDocsExamined", "executionTimeMillis", "keysExamined", "docsExamined")
    json.asObject.fold(json.asArray.toList.flatten.toList.flatMap(collectMetrics)) { obj =>
      val metrics = obj.toList.filter(entry => fields.contains(entry._1) && entry._2.isNumber)
      Option.when(metrics.nonEmpty)(Json.fromFields(metrics)).toList ++ obj.values.toList.flatMap(collectMetrics)
    }
  }

  private def collectIndexes(json: Json): List[String] =
    json.asObject.fold(json.asArray.toList.flatten.toList.flatMap(collectIndexes)) { obj =>
      obj("indexName").flatMap(_.asString).toList ++ obj.values.toList.flatMap(collectIndexes)
    }

  private def write(report: Json): IO[Unit] = IO.blocking {
    val directory = Paths.get(".local/data/mongodb-access-evaluation")
    val _ = Files.createDirectories(directory)
    val _ =
      Files.writeString(directory.resolve("operational-access-baseline.json"), report.spaces2, StandardCharsets.UTF_8)
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
