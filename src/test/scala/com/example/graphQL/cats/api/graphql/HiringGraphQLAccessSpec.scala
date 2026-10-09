package com.example.graphQL.cats.api.graphql

import com.example.graphQL.cats.FixedTestClock

import com.example.graphQL.cats.AccountValueFixtures.email
import cats.effect.IO
import cats.effect.Ref
import cats.syntax.all.*
import com.example.graphQL.cats.api.graphql.{GraphQLRequest, HiringGraphQLSchema, HiringGraphQLServices}
import com.example.graphQL.cats.service.{
  ActorContext,
  AnalyticsError,
  AnalyticsReportSnapshot,
  AnalyticsReportingUseCases,
  EmbeddingCoverageService,
  EmbeddingCoverageUseCases,
  HiringReadService,
  ProbeResult,
  RepositoryError,
  UseCaseError
}
import com.example.graphQL.cats.service.port.{
  EmbeddingError,
  EmbeddingInput,
  EmbeddingService,
  EmbeddingVector,
  RepositoryIO,
  SemanticSearchRepository
}
import com.example.graphQL.cats.service.ServiceFixtures.{InMemoryApplications, InMemoryJobs, InMemoryUsers}
import com.example.graphQL.cats.service.protocol.{
  AccountProfileInput,
  AccountUseCases,
  IdempotencyRequest,
  LoginInput,
  SignUpInput,
  UseCaseIO
}
import com.example.graphQL.cats.service.search.SemanticSearchService
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.service.search.{RankedJob, VectorSearchQuery, JobRetrievalHit, CandidateRetrievalHit}
import com.example.graphQL.cats.shared.crypto.SourceHash
import io.circe.Json
import munit.CatsEffectSuite

import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.{Base64, UUID}

final class HiringGraphQLAccessSpec extends CatsEffectSuite {
  private val now = Instant.parse("2026-09-17T08:00:00Z")
  private val candidateId = UserId(UUID.fromString("10000000-0000-0000-0000-000000000001"))
  private val recruiterId = UserId(UUID.fromString("10000000-0000-0000-0000-000000000002"))
  private val adminId = UserId(UUID.fromString("10000000-0000-0000-0000-000000000006"))
  private val jobId = JobId(UUID.fromString("10000000-0000-0000-0000-000000000003"))
  private val closedJobId = JobId(UUID.fromString("10000000-0000-0000-0000-000000000004"))
  private val applicationId = ApplicationId(UUID.fromString("10000000-0000-0000-0000-000000000005"))

  private val candidate = User(
    candidateId,
    Some(email("candidate@example.com")),
    "Candidate",
    UserRole.Candidate,
    Some(
      UserProfile.Candidate(
        CandidateProfile(
          Set("Scala"),
          None,
          None,
          Some(CandidateResidence("Cyprus", Some("Nicosia"))),
          Some(CandidateAvailabilityStatus.AVAILABLE_NOW),
          recruiterSearchOptIn = true
        )
      )
    ),
    now
  )
  private val recruiter = User(
    recruiterId,
    Some(email("recruiter@example.com")),
    "Recruiter",
    UserRole.Recruiter,
    Some(UserProfile.Recruiter(RecruiterProfile("Acme", None))),
    now
  )
  private val admin =
    User(adminId, Some(email("admin@example.com")), "Admin", UserRole.Admin, None, now, adminSingleton = true)
  private val openJob = job(jobId, JobStatus.Open)
  private val closedJob = job(closedJobId, JobStatus.Closed)
  private val application = Application.create(applicationId, candidateId, jobId, now)

  private def errorCode(json: Json): Either[io.circe.Error, String] =
    json.hcursor.downField("errors").downArray.downField("extensions").get[String]("code")

  test("hiring mutation without ActorContext returns typed unauthorized payload") {
    val query =
      s"""mutation {
         |  submitApplication(input: { idempotencyKey: "00000000-0000-0000-0000-000000000001", jobId: "${jobId.value}" }) {
         |    __typename
         |  }
         |}""".stripMargin

    execute(query, None).map { json =>
      assertEquals(errorCode(json), Right("UNAUTHORIZED"))
    }
  }

  test("account resolvers share the centralized unauthorized error") {
    val me =
      """query { me { id } }"""
    val users =
      """query { users(first: 10) { edges { node { id } } __typename } }"""
    val update =
      """mutation { updateMyProfile(input: { idempotencyKey: "00000000-0000-0000-0000-000000000001", skills: ["Scala"] }) { __typename } }"""
    val delete =
      """mutation { deleteMyAccount(input: { idempotencyKey: "00000000-0000-0000-0000-000000000001" }) { __typename ... on DeletionReceipt { receiptId status } } }"""

    (execute(me, None), execute(users, None), execute(update, None), execute(delete, None)).mapN {
      (meJson, usersJson, updateJson, deleteJson) =>
        assertEquals(errorCode(meJson), Right("UNAUTHORIZED"))
        assertEquals(errorCode(usersJson), Right("UNAUTHORIZED"))
        assertEquals(errorCode(updateJson), Right("UNAUTHORIZED"))
        assertEquals(errorCode(deleteJson), Right("UNAUTHORIZED"))
    }
  }

  test("deletion receipt status requires an authenticated actor") {
    val query =
      """query { accountDeletionStatus(receiptId: "00000000-0000-0000-0000-000000000001") }"""

    (execute(query, None), execute(query, Some(ActorContext(candidateId, UserRole.Candidate)))).mapN {
      (anonymous, authenticated) =>
        assertEquals(errorCode(anonymous), Right("UNAUTHORIZED"))
        assertEquals(
          authenticated.hcursor.downField("data").get[String]("accountDeletionStatus"),
          Right("NOT_FOUND")
        )
    }
  }

  test("analytics report requires authentication") {
    execute(
      """query { analyticsReport(from: "2026-09-16T00:00:00Z", to: "2026-09-17T00:00:00Z") { asOf } }""",
      None
    ).map { json =>
      assertEquals(errorCode(json), Right("UNAUTHORIZED"))
    }
  }

  test("analytics report failures use a sanitized analytics error") {
    val reporting = new AnalyticsReportingUseCases {
      override def report(
          actor: ActorContext,
          period: com.example.graphQL.cats.service.AnalyticsPeriod
      ): UseCaseIO[com.example.graphQL.cats.service.AnalyticsReportSnapshot] =
        UseCaseIO.left(UseCaseError.Analytics(AnalyticsError.ReportsUnavailable))
    }
    val query =
      """query { analyticsReport(from: "2026-09-16T00:00:00Z", to: "2026-09-17T00:00:00Z") { asOf } }"""

    executeWithUsers(query, Some(ActorContext(adminId, UserRole.Admin)), List(admin), analyticsReporting = reporting)
      .map { json =>
        assertEquals(errorCode(json), Right("ANALYTICS_UNAVAILABLE"))
        assertEquals(
          json.hcursor.downField("errors").downArray.get[String]("message"),
          Right("Analytics reports are unavailable")
        )
      }
  }

  test("an active Admin can read a published report with its asOf timestamp") {
    val asOf = Instant.parse("2026-09-17T08:00:00Z")
    val snapshot = AnalyticsReportSnapshot(asOf, Nil, None, Nil)
    val reporting = new AnalyticsReportingUseCases {
      override def report(
          actor: ActorContext,
          period: com.example.graphQL.cats.service.AnalyticsPeriod
      ): UseCaseIO[AnalyticsReportSnapshot] = UseCaseIO.pure(snapshot)
    }
    val query =
      """query { analyticsReport(from: "2026-09-16T00:00:00Z", to: "2026-09-17T00:00:00Z") { asOf } }"""

    executeWithUsers(query, Some(ActorContext(adminId, UserRole.Admin)), List(admin), analyticsReporting = reporting)
      .map { json =>
        assertEquals(
          json.hcursor.downField("data").downField("analyticsReport").get[String]("asOf"),
          Right(asOf.toString)
        )
      }
  }

  private val embeddingCoverageQuery =
    """query { embeddingCoverage(expectedModel: "model-a") {
      |  asOf expectedModel queueTruncated oldestQueuedWorkAgeSeconds oldestNotCurrentAgeSeconds lagEntityKinds
      |  kinds { kind searchableCount scannedCount truncated coverageShare }
      |  cells { kind freshness repairState failureReason count }
      |  observedModels { kind model count }
      |  lagSeconds { p50Seconds p95Seconds p99Seconds sampleCount }
      |  checks { check status offendingCount }
      |} }""".stripMargin

  private def coverageService(
      users: com.example.graphQL.cats.service.port.UserRepository
  ): EmbeddingCoverageUseCases = {
    import com.example.graphQL.cats.service.search.*
    val tally = EmbeddingCoverageTally.empty.add(
      EmbeddingCoverageEntity(
        com.example.graphQL.cats.service.port.EmbeddingWorkKind.Job,
        Some(EmbeddingMeta("model-a", "h", now)),
        "h",
        Some(now),
        None
      ),
      Some("model-a"),
      now
    )
    val repository = new com.example.graphQL.cats.service.port.EmbeddingCoverageRepository {
      override def observe(request: EmbeddingCoverageScanRequest): RepositoryIO[EmbeddingCoverageObservation] =
        RepositoryIO.fromEither(
          Right(
            EmbeddingCoverageObservation(
              tally,
              List(
                EmbeddingCoverageKindObservation(com.example.graphQL.cats.service.port.EmbeddingWorkKind.Job, 1L, false)
              ),
              EmbeddingQueueObservation(false, 0L, None)
            )
          )
        )
    }
    EmbeddingCoverageService.live(
      users,
      repository,
      scala.concurrent.duration.DurationInt(5).minutes,
      clock = FixedTestClock.at(now)
    )
  }

  test("embedding coverage requires authentication") {
    execute(embeddingCoverageQuery, None).map(json => assertEquals(errorCode(json), Right("UNAUTHORIZED")))
  }

  test("embedding coverage is denied to Candidates and Recruiters by the service") {
    List(
      ActorContext(candidateId, UserRole.Candidate),
      ActorContext(recruiterId, UserRole.Recruiter)
    ).traverse { actor =>
      executeWithUsers(
        embeddingCoverageQuery,
        Some(actor),
        List(admin, candidate, recruiter),
        embeddingCoverage = coverageService
      )
    }.map(_.foreach(json => assertEquals(errorCode(json), Right("UNAUTHORIZED"))))
  }

  test("embedding coverage returns only aggregate fields to an active Admin") {
    executeWithUsers(
      embeddingCoverageQuery,
      Some(ActorContext(adminId, UserRole.Admin)),
      List(admin),
      embeddingCoverage = coverageService
    ).map { json =>
      val report = json.hcursor.downField("data").downField("embeddingCoverage")
      assertEquals(report.get[String]("asOf"), Right(now.toString))
      assertEquals(report.downField("cells").downArray.get[String]("freshness"), Right("CURRENT"))
      assertEquals(report.downField("cells").downArray.get[String]("repairState"), Right("NO_QUEUED_WORK"))
      assertEquals(report.downField("cells").downArray.get[String]("kind"), Right("JOB"))
      assertEquals(
        report.downField("checks").values.map(_.map(_.hcursor.get[String]("status"))).map(_.toList),
        Some(List(Right("PASSED"), Right("PASSED")))
      )
      assertEquals(report.get[List[String]]("lagEntityKinds"), Right(List("JOB")))
      assertEquals(json.hcursor.downField("errors").succeeded, false)
    }
  }

  test("embedding coverage is typed unavailable when vector search is disabled and rejects a blank model") {
    for {
      disabled <- executeWithUsers(
        embeddingCoverageQuery,
        Some(ActorContext(adminId, UserRole.Admin)),
        List(admin),
        embeddingCoverage = users => EmbeddingCoverageService.vectorSearchDisabled(users)
      )
      blank <- executeWithUsers(
        """query { embeddingCoverage(expectedModel: " ") { asOf } }""",
        Some(ActorContext(adminId, UserRole.Admin)),
        List(admin),
        embeddingCoverage = coverageService
      )
    } yield {
      assertEquals(errorCode(disabled), Right("VECTOR_SEARCH_UNAVAILABLE"))
      assertEquals(errorCode(blank), Right("VALIDATION_FAILED"))
    }
  }

  test("embedding coverage roots are expensive: aliased scans are rejected before any repository call") {
    val adminActor = Some(ActorContext(adminId, UserRole.Admin))
    for {
      calls <- Ref.of[IO, Int](0)
      repository = new com.example.graphQL.cats.service.port.EmbeddingCoverageRepository {
        override def observe(
            request: com.example.graphQL.cats.service.search.EmbeddingCoverageScanRequest
        ): RepositoryIO[com.example.graphQL.cats.service.search.EmbeddingCoverageObservation] =
          RepositoryIO.lift(
            calls
              .update(_ + 1)
              .as(
                com.example.graphQL.cats.service.search.EmbeddingCoverageObservation(
                  com.example.graphQL.cats.service.search.EmbeddingCoverageTally.empty,
                  Nil,
                  com.example.graphQL.cats.service.search.EmbeddingQueueObservation(false, 0L, None)
                )
              )
          )
      }
      service = (users: com.example.graphQL.cats.service.port.UserRepository) =>
        EmbeddingCoverageService.live(
          users,
          repository,
          scala.concurrent.duration.DurationInt(5).minutes,
          clock = FixedTestClock.at(now)
        )
      aliased = (1 to 500).map(n => s"a$n: embeddingCoverage { asOf }").mkString("query { ", " ", " }")
      twoRoots = "query { a: embeddingCoverage { asOf } b: embeddingCoverage { asOf } }"
      rejectedMany <- executeEitherWithUsers(aliased, adminActor, List(admin), embeddingCoverage = service)
      rejectedTwo <- executeEitherWithUsers(twoRoots, adminActor, List(admin), embeddingCoverage = service)
      rejectedCount <- calls.get
      single <- executeEitherWithUsers(
        "query { a: embeddingCoverage { asOf } }",
        adminActor,
        List(admin),
        embeddingCoverage = service
      )
      singleCount <- calls.get
    } yield {
      assertEquals(rejectedMany, Left(HiringGraphQLSchema.Failure.InvalidQuery))
      assertEquals(rejectedTwo, Left(HiringGraphQLSchema.Failure.InvalidQuery))
      assertEquals(rejectedCount, 0)
      assert(single.exists(_.hcursor.downField("data").downField("a").succeeded))
      assertEquals(singleCount, 1)
    }
  }

  test("an unwired embedding coverage capability denies even an Admin without leaking availability") {
    executeWithUsers(
      embeddingCoverageQuery,
      Some(ActorContext(adminId, UserRole.Admin)),
      List(admin)
    ).map(json => assertEquals(errorCode(json), Right("UNAUTHORIZED")))
  }

  test("embedding coverage rejects an oversized expectedModel at the GraphQL boundary") {
    executeWithUsers(
      s"""query { embeddingCoverage(expectedModel: "${"m" * 129}") { asOf } }""",
      Some(ActorContext(adminId, UserRole.Admin)),
      List(admin),
      embeddingCoverage = coverageService
    ).map(json => assertEquals(errorCode(json), Right("VALIDATION_FAILED")))
  }

  test("anonymous Admin bootstrap is absent from the public schema") {
    val query = "mutation { bootstrapAdmin(input: {name: \"Admin\", password: \"password-password\"}) { __typename } }"
    for {
      request <- parseRequest(query)
      result <- TestGraphQLSupport
        .context(IO.pure(ProbeResult.Ready), None)
        .use(TestGraphQLSupport.parseAndExecute(request, _))
    } yield assertEquals(result, Left(HiringGraphQLSchema.Failure.InvalidQuery))
  }

  test("public account mutations are unavailable until hiring setup is ready") {
    val signUp =
      """mutation { signUp(input: { idempotencyKey: "00000000-0000-0000-0000-000000000001", name: "Candidate", role: CANDIDATE, password: "password-password", skills: ["Scala"] }) { __typename } }"""
    val login =
      """mutation { login(input: { idempotencyKey: "00000000-0000-0000-0000-000000000001", name: "Candidate", password: "password-password" }) { __typename } }"""
    for {
      calls <- Ref.of[IO, Int](0)
      service = new PublicAccountService(calls)
      results <- List(signUp, login).traverse(query =>
        executeWithUsers(
          query,
          None,
          List(candidate, recruiter),
          service,
          hiringReady = IO.pure(ProbeResult.Unavailable)
        )
      )
      count <- calls.get
    } yield {
      results.foreach { json =>
        assertEquals(errorCode(json), Right("SERVICE_NOT_READY"))
      }
      assertEquals(count, 0)
    }
  }

  test("application connection rejects a job cursor") {
    given CursorCodec.CursorKey = TestGraphQLSupport.cursorKey
    IO.realTimeInstant.flatMap { cursorNow =>
      val cursor =
        CursorCodec.encode(com.example.graphQL.cats.domain.pagination.JobCursor(cursorNow, jobId), cursorNow)
      val query =
        s"""query {
           |  myApplications(first: 10, after: "$cursor") {
           |    edges { node { id } }
           | __typename
           |  }
           |}""".stripMargin

      execute(query, Some(ActorContext(candidateId, UserRole.Candidate))).map { json =>
        assertEquals(errorCode(json), Right("WRONG_CURSOR_KIND"))
      }
    }
  }

  test("application connection rejects an expired cursor") {
    given CursorCodec.CursorKey = TestGraphQLSupport.cursorKey
    IO.realTimeInstant.flatMap { current =>
      val issuedAt = current.minusSeconds(TestGraphQLSupport.cursorKey.ttlSeconds + 1)
      val cursor = CursorCodec.encode(com.example.graphQL.cats.domain.pagination.JobCursor(current, jobId), issuedAt)
      val query =
        s"""query {
           |  myApplications(first: 10, after: "$cursor") {
           |    edges { node { id } }
           |  }
           |}""".stripMargin

      execute(query, Some(ActorContext(candidateId, UserRole.Candidate))).map { json =>
        assertEquals(errorCode(json), Right("INVALID_CURSOR"))
      }
    }
  }

  test("job search without ActorContext returns typed unauthorized connection") {
    val query =
      """query {
        |  jobs(first: 10) {
        |    edges { node { id } }
        | __typename
        |  }
        |}""".stripMargin

    execute(query, None).map { json =>
      assertEquals(errorCode(json), Right("UNAUTHORIZED"))
    }
  }

  test("job query preserves unauthorized and not-found failures in typed payloads") {
    val existing =
      s"""query {
         |  job(id: "${jobId.value}") {
         |    id
         | __typename
         |  }
         |}""".stripMargin
    val missingId = JobId(UUID.fromString("10000000-0000-0000-0000-000000000099"))
    val missing =
      s"""query {
         |  job(id: "${missingId.value}") {
         |    id
         | __typename
         |  }
         |}""".stripMargin

    (execute(existing, None), execute(missing, Some(ActorContext(candidateId, UserRole.Candidate)))).mapN {
      (unauthorizedJson, missingJson) =>
        assertEquals(errorCode(unauthorizedJson), Right("UNAUTHORIZED"))

        assertEquals(errorCode(missingJson), Right("NOT_FOUND"))
    }
  }

  test("me query preserves unauthenticated failure in typed payload") {
    val query =
      """query {
        |  me {
        |    id
        | __typename
        |  }
        |}""".stripMargin

    execute(query, None).map { json =>
      assertEquals(errorCode(json), Right("UNAUTHORIZED"))
    }
  }

  test("nested user email is projected by service authorization") {
    val query =
      """query {
        |  myApplications(first: 10) {
        |    edges { node { candidate { email } job { recruiter { email } } } }
        | __typename
        |  }
        |}""".stripMargin

    execute(query, Some(ActorContext(candidateId, UserRole.Candidate))).map { json =>
      val candidateEmail = json.hcursor
        .downField("data")
        .downField("myApplications")
        .downField("edges")
        .downArray
        .downField("node")
        .downField("candidate")
        .get[String]("email")
      val recruiterEmail = json.hcursor
        .downField("data")
        .downField("myApplications")
        .downField("edges")
        .downArray
        .downField("node")
        .downField("job")
        .downField("recruiter")
        .get[Option[String]]("email")
      assertEquals(candidateEmail, Right("candidate@example.com"))
      assertEquals(recruiterEmail, Right(None))
    }
  }

  test("candidate private matching fields are visible to their owner and hidden from recruiters") {
    val ownerQuery =
      """query { myApplications(first: 10) { edges { node { candidate { profile { ... on CandidateProfile { currentResidence { country city } availabilityStatus recruiterSearchOptIn } } } } } } }"""
    val recruiterQuery =
      s"""query { jobApplications(jobId: "${jobId.value}", first: 10) { edges { node { candidate { profile { ... on CandidateProfile { currentResidence { country } availabilityStatus recruiterSearchOptIn } } } } } } }"""
    (
      execute(ownerQuery, Some(ActorContext(candidateId, UserRole.Candidate))),
      execute(recruiterQuery, Some(ActorContext(recruiterId, UserRole.Recruiter)))
    ).mapN { (owner, recruiterView) =>
      val ownerProfile = owner.hcursor
        .downField("data")
        .downField("myApplications")
        .downField("edges")
        .downArray
        .downField("node")
        .downField("candidate")
        .downField("profile")
      assert(ownerProfile.succeeded, owner.noSpaces)
      assertEquals(ownerProfile.downField("currentResidence").get[String]("country"), Right("Cyprus"))
      assertEquals(ownerProfile.get[String]("availabilityStatus"), Right("AVAILABLE_NOW"))
      assertEquals(ownerProfile.get[Boolean]("recruiterSearchOptIn"), Right(true))
      val privateView = recruiterView.hcursor
        .downField("data")
        .downField("jobApplications")
        .downField("edges")
        .downArray
        .downField("node")
        .downField("candidate")
        .downField("profile")
      assertEquals(privateView.get[Option[Json]]("currentResidence"), Right(None))
      assertEquals(privateView.get[Option[Json]]("availabilityStatus"), Right(None))
      assertEquals(privateView.get[Boolean]("recruiterSearchOptIn"), Right(false))
    }
  }

  private val candidateProfileSelection =
    "profile { ... on CandidateProfile { skills currentResidence { country } availabilityStatus recruiterSearchOptIn } }"

  private def jobApplicationCandidateProfile(json: Json) =
    json.hcursor
      .downField("data")
      .downField("jobApplications")
      .downField("edges")
      .downArray
      .downField("node")
      .downField("candidate")
      .downField("profile")

  test("an admin sees another candidate's public profile with owner-only matching fields masked") {
    val query =
      s"""query { jobApplications(jobId: "${jobId.value}", first: 10) { edges { node { candidate { $candidateProfileSelection } } } } }"""
    executeWithUsers(query, Some(ActorContext(adminId, UserRole.Admin)), List(candidate, recruiter, admin)).map {
      json =>
        val profile = jobApplicationCandidateProfile(json)
        assert(profile.succeeded, json.noSpaces)
        assertEquals(profile.get[List[String]]("skills"), Right(List("Scala")))
        assertEquals(profile.get[Option[Json]]("currentResidence"), Right(None))
        assertEquals(profile.get[Option[Json]]("availabilityStatus"), Right(None))
        assertEquals(profile.get[Boolean]("recruiterSearchOptIn"), Right(false))
    }
  }

  test("a different candidate cannot reach another candidate's profile or its owner-only fields") {
    val otherId = UserId(UUID.fromString("10000000-0000-0000-0000-000000000007"))
    val other = candidate.copy(id = otherId, email = Some(email("other@example.com")), name = "Other")
    val query =
      s"""query { jobApplications(jobId: "${jobId.value}", first: 10) { edges { node { candidate { $candidateProfileSelection } } } } }"""
    executeWithUsers(query, Some(ActorContext(otherId, UserRole.Candidate)), List(candidate, recruiter, other)).map {
      json =>
        assert(json.hcursor.downField("errors").succeeded, json.noSpaces)
        assert(!jobApplicationCandidateProfile(json).succeeded, json.noSpaces)
        assert(!json.noSpaces.contains("Cyprus") && !json.noSpaces.contains("AVAILABLE_NOW"), json.noSpaces)
    }
  }

  test("an unauthenticated payload errors on each owner-only profile field and still resolves skills alone") {
    def signUp(selection: String) =
      s"""mutation { signUp(input: { idempotencyKey: "00000000-0000-0000-0000-000000000001", name: "Candidate", role: CANDIDATE, password: "password-password", skills: ["Scala"] }) { ... on AuthSuccess { user { profile { ... on CandidateProfile { $selection } } } } } }"""
    val privateFields = List("currentResidence", "availabilityStatus", "recruiterSearchOptIn")
    (
      executeWithUsers(
        signUp("skills currentResidence { country } availabilityStatus recruiterSearchOptIn"),
        None,
        List(candidate, recruiter),
        SignUpReturningCandidateService
      ),
      executeWithUsers(signUp("skills"), None, List(candidate, recruiter), SignUpReturningCandidateService)
    ).mapN { (all, skillsOnly) =>
      // The non-null recruiterSearchOptIn error nulls the nullable profile, as before the service-owned rule.
      assertEquals(
        all.hcursor.downField("data").downField("signUp").downField("user").get[Option[Json]]("profile"),
        Right(None)
      )
      val errors = all.hcursor.downField("errors").as[List[Json]].getOrElse(Nil)
      assertEquals(
        errors.flatMap(_.hcursor.downField("path").as[List[String]].toOption.flatMap(_.lastOption)),
        privateFields
      )
      assert(errors.forall(_.hcursor.downField("extensions").get[String]("code") == Right("UNAUTHORIZED")))
      assertEquals(
        skillsOnly.hcursor
          .downField("data")
          .downField("signUp")
          .downField("user")
          .downField("profile")
          .get[List[String]]("skills"),
        Right(List("Scala"))
      )
      assert(skillsOnly.hcursor.downField("errors").failed, skillsOnly.noSpaces)
    }
  }

  test("job connection rejects a cursor with malformed fields as a typed error") {
    val cursor = encodeCursor(
      Json.obj(
        "kind" -> Json.fromString("job"),
        "createdAt" -> Json.fromString("not-an-instant"),
        "occurredAt" -> Json.Null,
        "id" -> Json.fromString(jobId.value.toString)
      )
    )
    val query =
      s"""query {
         |  myJobs(first: 10, after: "$cursor") {
         |    edges { node { id } }
         | __typename
         |  }
         |}""".stripMargin

    execute(query, Some(ActorContext(recruiterId, UserRole.Recruiter))).map { json =>
      assertEquals(errorCode(json), Right("INVALID_CURSOR"))
    }
  }

  test("injected hiring context searches only open jobs and preserves nested recruiter batching boundary") {
    val query =
      """query {
        |  jobs(first: 10, city: "Kyiv", skills: ["Scala"]) {
        |    edges {
        |      cursor
        |      node {
        |        id
        |        title
        |        recruiter { id name }
        |      }
        |    }
        | __typename
        |  }
        |}""".stripMargin

    execute(query, Some(ActorContext(candidateId, UserRole.Candidate))).map { json =>
      val edges = json.hcursor.downField("data").downField("jobs").downField("edges").focus.getOrElse(Json.Null)
      assertEquals(edges.asArray.map(_.size), Some(1))
      assertEquals(edges.hcursor.downArray.downField("node").get[String]("id"), Right(jobId.value.toString))
      assertEquals(
        edges.hcursor.downArray.downField("node").downField("recruiter").get[String]("id"),
        Right(recruiterId.value.toString)
      )
      assert(!json.hcursor.downField("errors").succeeded)
    }
  }

  test("User profile resolves through the role-specific union") {
    val query =
      """query {
        |  jobs(first: 10) {
        |    edges {
        |      node {
        |        recruiter {
        |          profile {
        |            __typename
        |            ... on RecruiterProfile { organizationName }
        |          }
        |        }
        |      }
        |    }
        |    pageInfo { endCursor }
        | __typename
        |  }
        |}""".stripMargin

    execute(query, Some(ActorContext(candidateId, UserRole.Candidate))).map { json =>
      val profile = json.hcursor
        .downField("data")
        .downField("jobs")
        .downField("edges")
        .downArray
        .downField("node")
        .downField("recruiter")
        .downField("profile")
      assertEquals(profile.get[String]("__typename"), Right("RecruiterProfile"))
      assertEquals(profile.get[String]("organizationName"), Right("Acme"))
    }
  }

  test("job search rejects malformed createdAfter instead of widening results") {
    val query =
      """query {
        |  jobs(first: 10, createdAfter: "not-an-instant") {
        |    edges { node { id } }
        | __typename
        |  }
        |}""".stripMargin

    for {
      request <- parseRequest(query)
      context <- TestGraphQLSupport
        .context(IO.pure(ProbeResult.Ready), Some(ActorContext(candidateId, UserRole.Candidate)))
        .allocated
      result <- TestGraphQLSupport.parseAndExecute(request, context._1).guarantee(context._2)
    } yield assertEquals(result, Left(HiringGraphQLSchema.Failure.InvalidQuery))
  }

  test("myJobs resolves stored actor before recruiter ownership lookup") {
    val query =
      """query {
        |  myJobs(first: 10) {
        |    edges { node { id } }
        | __typename
        |  }
        |}""".stripMargin

    execute(query, Some(ActorContext(recruiterId, UserRole.Candidate))).map { json =>
      assertEquals(errorCode(json), Right("FORBIDDEN"))
    }
  }

  test("myJobs lists recruiter-owned jobs for singleton admin") {
    val query =
      """query {
        |  myJobs(first: 10) {
        |    edges { node { id } }
        | __typename
        |  }
        |}""".stripMargin

    executeWithUsers(query, Some(ActorContext(adminId, UserRole.Admin)), List(candidate, recruiter, admin)).map {
      json =>
        val jobs = json.hcursor.downField("data").downField("myJobs")
        val ids = jobs
          .downField("edges")
          .focus
          .flatMap(_.asArray)
          .getOrElse(Vector.empty)
          .flatMap(_.hcursor.downField("node").get[String]("id").toOption)
          .toSet
        assertEquals(ids, Set(jobId.value.toString, closedJobId.value.toString))
        assert(!json.hcursor.downField("errors").succeeded)
    }
  }

  test("Admin profile update is rejected before constructing a candidate profile") {
    for {
      updateCalls <- Ref.of[IO, Int](0)
      accountService = new RecordingAccountService(updateCalls)
      query =
        """mutation {
          |  updateMyProfile(input: { idempotencyKey: "00000000-0000-0000-0000-000000000001", skills: ["Scala"] }) {
          |    __typename
          | __typename
          |  }
          |}""".stripMargin
      json <- executeWithUsers(query, Some(ActorContext(adminId, UserRole.Admin)), List(admin), accountService)
      calls <- updateCalls.get
    } yield {
      assertEquals(
        json.hcursor.downField("data").downField("updateMyProfile").get[String]("__typename"),
        Right("DomainError")
      )
      assertEquals(calls, 0)
    }
  }

  test("Recruiter profile update delegates semantic validation to the account service") {
    for {
      updateCalls <- Ref.of[IO, Int](0)
      accountService = new RecordingAccountService(updateCalls)
      query =
        """mutation {
          |  updateMyProfile(input: { idempotencyKey: "00000000-0000-0000-0000-000000000001", jobTitle: "Hiring Lead" }) {
          |    __typename
          | __typename
          |  }
          |}""".stripMargin
      json <- executeWithUsers(
        query,
        Some(ActorContext(recruiterId, UserRole.Recruiter)),
        List(recruiter),
        accountService
      )
      calls <- updateCalls.get
    } yield {
      val payload = json.hcursor.downField("data").downField("updateMyProfile")
      assertEquals(payload.get[String]("__typename"), Right("ValidationError"))
      assertEquals(calls, 1)
    }
  }

  test("injected candidate context lists only the candidate applications") {
    val query =
      """query {
        |  myApplications(first: 10) {
        |    edges {
        |      cursor
        |      node {
        |        id
        |        status
        |        job { id title }
        |      }
        |    }
        |    pageInfo { endCursor }
        | __typename
        |  }
        |}""".stripMargin

    execute(query, Some(ActorContext(candidateId, UserRole.Candidate))).map { json =>
      val applications = json.hcursor.downField("data").downField("myApplications")
      val edge = applications.downField("edges").downArray
      val node = edge.downField("node")
      assertEquals(node.get[String]("id"), Right(applicationId.value.toString))
      assertEquals(node.get[String]("status"), Right("CREATED"))
      assertEquals(node.downField("job").get[String]("id"), Right(jobId.value.toString))
      assertEquals(applications.downField("pageInfo").get[String]("endCursor"), edge.get[String]("cursor"))
    }
  }

  test("recruiter job application listing forwards the requested job id") {
    val query =
      s"""query {
         |  jobApplications(jobId: "${jobId.value}", first: 10) {
         |    edges { node { id job { id } } }
         | __typename
         |  }
         |}""".stripMargin

    execute(query, Some(ActorContext(recruiterId, UserRole.Recruiter))).map { json =>
      val applications = json.hcursor.downField("data").downField("jobApplications")
      val node = applications.downField("edges").downArray.downField("node")
      assertEquals(node.get[String]("id"), Right(applicationId.value.toString))
      assertEquals(node.downField("job").get[String]("id"), Right(jobId.value.toString))
      assert(!json.hcursor.downField("errors").succeeded)
    }
  }

  test("application job recruiter nesting preloads recruiter users in one batch") {
    val secondRecruiterId = UserId(UUID.fromString("10000000-0000-0000-0000-000000000007"))
    val secondJobId = JobId(UUID.fromString("10000000-0000-0000-0000-000000000008"))
    val secondApplicationId = ApplicationId(UUID.fromString("10000000-0000-0000-0000-000000000009"))
    val secondRecruiter = User(
      secondRecruiterId,
      Some(email("recruiter2@example.com")),
      "Recruiter 2",
      UserRole.Recruiter,
      Some(UserProfile.Recruiter(RecruiterProfile("Acme 2", None))),
      now
    )
    val secondJob = openJob.copy(id = secondJobId, recruiterId = secondRecruiterId, title = "Platform Engineer")
    val secondApplication = Application.create(secondApplicationId, candidateId, secondJobId, now.minusSeconds(60))
    val query =
      """query {
        |  myApplications(first: 10) {
        |    edges {
        |      node {
        |        candidate { id }
        |        job { id recruiter { id } }
        |      }
        |    }
        | __typename
        |  }
        |}""".stripMargin

    for {
      usersRef <- Ref.of[IO, Map[UserId, User]](
        List(candidate, recruiter, secondRecruiter).map(user => user.id -> user).toMap
      )
      userBatches <- Ref.of[IO, Vector[List[UserId]]](Vector.empty)
      jobsRef <- Ref.of[IO, Map[JobId, Job]](List(openJob, secondJob).map(job => job.id -> job).toMap)
      applicationsRef <- Ref.of[IO, Map[ApplicationId, Application]](
        List(application, secondApplication).map(application => application.id -> application).toMap
      )
      eventsRef <- Ref.of[IO, Vector[ApplicationEvent]](Vector.empty)
      nextCreateError <- Ref.of[IO, Option[com.example.graphQL.cats.service.RepositoryError]](None)
      users = RecordingUsers(usersRef, userBatches, jobsRef, applicationsRef)
      jobs = InMemoryJobs(
        jobsRef,
        relationLookup =
          Some(com.example.graphQL.cats.service.ServiceFixtures.jobRelations(usersRef, jobsRef, applicationsRef))
      )
      applications = InMemoryApplications(
        applicationsRef,
        eventsRef,
        nextCreateError,
        jobLookup = id => jobsRef.get.map(_.get(id))
      )
      services = HiringGraphQLServices(
        HiringReadService(users, jobs, applications),
        com.example.graphQL.cats.service.TestHiringServices.job(users, jobs),
        com.example.graphQL.cats.service.TestHiringServices.applications(users, jobs, applications),
        TestGraphQLSupport.cursorKey,
        TestGraphQLSupport.accountService,
        TestGraphQLSupport.interactions,
        TestGraphQLSupport.searchSessions
      )
      request <- parseRequest(query)
      result <- TestGraphQLSupport
        .context(IO.pure(ProbeResult.Ready), Some(ActorContext(candidateId, UserRole.Candidate)), services)
        .use(TestGraphQLSupport.parseAndExecute(request, _))
      batches <- userBatches.get
    } yield {
      result.fold(failure => fail(failure.toString), _ => ())
      assert(batches.exists(_.toSet == Set(recruiterId, secondRecruiterId)))
      assert(!batches.exists(batch => batch.size == 1 && Set(recruiterId, secondRecruiterId).contains(batch.head)))
    }
  }

  test("createJob input publishes skills under the domain field name") {
    val query =
      """mutation {
        |  createJob(input: { idempotencyKey: "00000000-0000-0000-0000-000000000001",
        |    title: "Staff Scala Developer"
        |    description: "Build platform services"
        |    requirements: ["Scala"]
        |    skills: ["Scala", "Cats Effect"]
        |    country: "Cyprus"
        |    city: "Nicosia"
        |    remote: true
        |  }) {
        |    __typename
        |    ... on Job { title skills }
        |  }
        |}""".stripMargin

    execute(query, Some(ActorContext(recruiterId, UserRole.Recruiter))).map { json =>
      val payload = json.hcursor.downField("data").downField("createJob")
      assertEquals(payload.get[String]("title"), Right("Staff Scala Developer"))
      assertEquals(
        payload.downField("skills").focus.flatMap(_.asArray).map(_.flatMap(_.asString).toList),
        Some(List("Cats Effect", "Scala"))
      )
      assert(!json.hcursor.downField("errors").succeeded)
    }
  }

  test("createJob validation failures use the named validation error case") {
    val query =
      """mutation {
        |  createJob(input: { idempotencyKey: "00000000-0000-0000-0000-000000000001",
        |    title: "Staff Scala Developer"
        |    description: "Build platform services"
        |    requirements: ["Scala"]
        |    skills: ["Scala"]
        |    country: ""
        |    remote: true
        |  }) {
        |    __typename
        | __typename
        |  }
        |}""".stripMargin

    execute(query, Some(ActorContext(recruiterId, UserRole.Recruiter))).map { json =>
      val payload = json.hcursor.downField("data").downField("createJob")
      assertEquals(payload.get[String]("__typename"), Right("ValidationError"))
    }
  }

  test("submitApplication duplicate and closed-job failures stay in typed payloads") {
    val duplicate =
      s"""mutation {
         |  submitApplication(input: { idempotencyKey: "00000000-0000-0000-0000-000000000001", jobId: "${jobId.value}" }) {
         |    __typename
         |    ... on Application { id status }
         |  }
         |}""".stripMargin
    val closed =
      s"""mutation {
         |  submitApplication(input: { idempotencyKey: "00000000-0000-0000-0000-000000000001", jobId: "${closedJobId.value}" }) {
         |    __typename
         |    ... on Application { id status }
         |  }
         |}""".stripMargin

    (
      execute(duplicate, Some(ActorContext(candidateId, UserRole.Candidate))),
      execute(closed, Some(ActorContext(candidateId, UserRole.Candidate)))
    ).mapN { (duplicateJson, closedJson) =>
      assertEquals(
        duplicateJson.hcursor.downField("data").downField("submitApplication").get[String]("__typename"),
        Right("DomainError")
      )
      assertEquals(
        closedJson.hcursor.downField("data").downField("submitApplication").get[String]("__typename"),
        Right("DomainError")
      )
    }
  }

  test("application status mutation invalid transition stays in typed payloads") {
    val query =
      s"""mutation {
         |  hireApplication(input: { idempotencyKey: "00000000-0000-0000-0000-000000000001", applicationId: "${applicationId.value}" }) {
         |    __typename
         |    ... on Application { id status }
         |  }
         |}""".stripMargin

    execute(query, Some(ActorContext(recruiterId, UserRole.Recruiter))).map { json =>
      val payload = json.hcursor.downField("data").downField("hireApplication")
      assertEquals(payload.get[String]("__typename"), Right("DomainError"))
    }
  }

  test("candidate application history access resolves stored actor before ownership") {
    val query =
      s"""query {
         |  applicationHistory(applicationId: "${applicationId.value}", first: 10) {
         |    edges { node { id } }
         | __typename
         |  }
         |}""".stripMargin

    executeWithUsers(query, Some(ActorContext(candidateId, UserRole.Candidate)), List(recruiter)).map { json =>
      assertEquals(errorCode(json), Right("UNAUTHORIZED"))
    }
  }

  test("application rejection uses reject input object and returns typed payload errors") {
    val query =
      s"""mutation {
         |  rejectApplication(input: { idempotencyKey: "00000000-0000-0000-0000-000000000001", applicationId: "${applicationId.value}", feedback: "Not enough Scala" }) {
         |    __typename
         |    ... on Application { id status }
         |  }
         |}""".stripMargin

    execute(query, Some(ActorContext(recruiterId, UserRole.Recruiter))).map { json =>
      val payload = json.hcursor.downField("data").downField("rejectApplication")
      assertEquals(payload.get[String]("id"), Right(applicationId.value.toString))
      assertEquals(payload.get[String]("status"), Right("REJECTED"))
      assert(!json.hcursor.downField("errors").succeeded)
    }
  }

  test("application rejection and decline validation failures stay in typed payloads") {
    val reject =
      s"""mutation {
         |  rejectApplication(input: { idempotencyKey: "00000000-0000-0000-0000-000000000001", applicationId: "${applicationId.value}", feedback: " " }) {
         |    __typename
         | __typename
         |  }
         |}""".stripMargin
    val decline =
      s"""mutation {
         |  declineApplication(input: { idempotencyKey: "00000000-0000-0000-0000-000000000001", applicationId: "${applicationId.value}", reason: "" }) {
         |    __typename
         | __typename
         |  }
         |}""".stripMargin

    (
      execute(reject, Some(ActorContext(recruiterId, UserRole.Recruiter))),
      execute(decline, Some(ActorContext(recruiterId, UserRole.Recruiter)))
    ).mapN { (rejectJson, declineJson) =>
      val rejectPayload = rejectJson.hcursor.downField("data").downField("rejectApplication")
      val declinePayload = declineJson.hcursor.downField("data").downField("declineApplication")
      assertEquals(rejectPayload.get[String]("__typename"), Right("DomainError"))
      assertEquals(declinePayload.get[String]("__typename"), Right("DomainError"))
    }
  }

  test("VHS-AC02 semantic job search returns ranked jobs with observable model metadata") {
    val query =
      """query {
        |  semanticJobSearch(query: "scala backend", filter: { city: "Kyiv", skills: ["Scala"] }, first: 5) {
        |    results {
        |      job { id recruiter { id } }
        |      score
        |      searchMode
        |      model
        |      searchId
        |    }
        | __typename
        |  }
        |}""".stripMargin

    executeWithSemanticSearch(query, Some(ActorContext(candidateId, UserRole.Candidate))).map { json =>
      val payload = json.hcursor.downField("data").downField("semanticJobSearch")
      assertEquals(
        payload.downField("results").downArray.downField("job").get[String]("id"),
        Right(jobId.value.toString)
      )
      assertEquals(payload.downField("results").downArray.get[String]("searchMode"), Right("HYBRID"))
      assertEquals(payload.downField("results").downArray.get[String]("model"), Right("voyage-4-lite"))
      assert(!json.hcursor.downField("errors").succeeded)
    }
  }

  test("VHS-AC04 candidateMatches projection does not expose email or resumeRef") {
    val query =
      s"""query {
         |  candidateMatches(jobId: "${jobId.value}", first: 5) {
         |    results {
         |      candidate { id email profile { resumeRef } }
         |    }
         |  }
         |}""".stripMargin

    for {
      request <- parseRequest(query)
      result <- TestGraphQLSupport
        .context(IO.pure(ProbeResult.Ready))
        .use(TestGraphQLSupport.parseAndExecute(request, _))
    } yield assertEquals(result, Left(HiringGraphQLSchema.Failure.InvalidQuery))
  }

  test("VHS-AC04 documented candidateMatches profile fragment remains executable") {
    val query =
      s"""query CandidateMatches {
         |  candidateMatches(jobId: "${jobId.value}", first: 5) {
         |    results {
         |      candidate {
         |        id
         |        name
         |        profile {
         |          ... on CandidateMatchProfile { skills }
         |        }
         |      }
         |    }
         | __typename
         |  }
         |}""".stripMargin

    executeWithSemanticSearch(query, Some(ActorContext(recruiterId, UserRole.Recruiter)), staleSearchJob = true).map {
      json =>
        assertEquals(errorCode(json), Right("STALE_EMBEDDING"))
    }
  }

  test("signup canonical-name conflicts return a generic registration failure") {
    val query =
      """mutation {
        |  signUp(input: { idempotencyKey: "00000000-0000-0000-0000-000000000001", name: "Candidate", role: CANDIDATE, password: "password-password", skills: ["Scala"] }) {
        |    __typename
        | __typename
        |  }
        |}""".stripMargin

    executeWithUsers(query, None, List(candidate, recruiter), NameTakenAccountService).map { json =>
      assertEquals(json.hcursor.downField("data").downField("signUp").get[String]("__typename"), Right("DomainError"))
      assert(!json.noSpaces.contains("NAME_TAKEN"))
    }
  }

  test("variable mutation inputs decode custom scalar input objects") {
    val submit =
      """mutation Submit($input: SubmitApplicationInput!) {
        |  submitApplication(input: $input) {
        |    __typename
        | __typename
        |  }
        |}""".stripMargin
    val updateAndReject =
      """mutation RecruiterActions($update: UpdateJobInput!, $reject: RejectApplicationInput!) {
        |  updateJob(input: $update) {
        |    __typename
        | __typename
        |  }
        |  rejectApplication(input: $reject) {
        |    __typename
        | __typename
        |  }
        |}""".stripMargin
    val signup =
      """mutation Signup($input: SignUpInput!) {
        |  signUp(input: $input) {
        |    __typename
        | __typename
        |  }
        |}""".stripMargin

    val submitVariables = Json.obj(
      "input" -> Json.obj(
        "idempotencyKey" -> Json.fromString("00000000-0000-0000-0000-000000000001"),
        "jobId" -> Json.fromString(jobId.value.toString)
      )
    )
    val recruiterVariables = Json.obj(
      "update" -> Json.obj(
        "idempotencyKey" -> Json.fromString("00000000-0000-0000-0000-000000000001"),
        "id" -> Json.fromString(jobId.value.toString),
        "patch" -> Json.obj(
          "title" -> Json.fromString("Principal Scala Developer"),
          "description" -> Json.fromString("Build reliable services"),
          "requirements" -> Json.arr(Json.fromString("Scala")),
          "skills" -> Json.arr(Json.fromString("Scala"), Json.fromString("Cats Effect")),
          "country" -> Json.fromString("Cyprus"),
          "city" -> Json.fromString("Nicosia"),
          "remote" -> Json.True
        )
      ),
      "reject" -> Json.obj(
        "idempotencyKey" -> Json.fromString("00000000-0000-0000-0000-000000000001"),
        "applicationId" -> Json.fromString(applicationId.value.toString),
        "feedback" -> Json.fromString("Not enough Scala")
      )
    )
    val signupVariables = Json.obj(
      "input" -> Json.obj(
        "idempotencyKey" -> Json.fromString("00000000-0000-0000-0000-000000000001"),
        "name" -> Json.fromString("Candidate"),
        "role" -> Json.fromString("CANDIDATE"),
        "password" -> Json.fromString("password-password"),
        "skills" -> Json.arr(Json.fromString("Scala"))
      )
    )

    (
      executeWithUsers(
        submit,
        Some(ActorContext(candidateId, UserRole.Candidate)),
        List(candidate, recruiter),
        variables = submitVariables
      ),
      executeWithUsers(
        updateAndReject,
        Some(ActorContext(recruiterId, UserRole.Recruiter)),
        List(candidate, recruiter),
        variables = recruiterVariables
      ),
      executeWithUsers(signup, None, List(candidate, recruiter), NameTakenAccountService, signupVariables)
    ).mapN { (submitJson, recruiterJson, signupJson) =>
      assertEquals(
        submitJson.hcursor.downField("data").downField("submitApplication").get[String]("__typename"),
        Right("DomainError")
      )

      val updatePayload = recruiterJson.hcursor.downField("data").downField("updateJob")
      assertEquals(updatePayload.get[String]("__typename"), Right("Job"))

      val rejectPayload = recruiterJson.hcursor.downField("data").downField("rejectApplication")
      assertEquals(rejectPayload.get[String]("__typename"), Right("Application"))

      assertEquals(
        signupJson.hcursor.downField("data").downField("signUp").get[String]("__typename"),
        Right("DomainError")
      )
    }
  }

  private def execute(query: String, actor: Option[ActorContext]): IO[Json] = {
    executeWithUsers(query, actor, List(candidate, recruiter))
  }

  private def parseRequest(query: String, variables: Json = Json.obj()): IO[GraphQLRequest] =
    IO.fromEither(
      Json
        .obj("query" -> Json.fromString(query), "variables" -> variables)
        .as[GraphQLRequest]
        .leftMap(error => new IllegalArgumentException("Invalid GraphQL test request", error))
    )

  private def executeWithSemanticSearch(
      query: String,
      actor: Option[ActorContext],
      staleSearchJob: Boolean = false
  ): IO[Json] = {
    val sourceHash = if (staleSearchJob) "hash" else SourceHash.sha256(SearchableText.job(openJob))
    val meta = EmbeddingMeta("voyage-4-lite", sourceHash, now)
    val embeddedJob = openJob.copy(embedding = Some(EntityEmbedding(List(0.1f, 0.2f), meta)))
    for {
      usersRef <- Ref.of[IO, Map[UserId, User]](List(candidate, recruiter).map(user => user.id -> user).toMap)
      jobsRef <- Ref.of[IO, Map[JobId, Job]](Map(embeddedJob.id -> embeddedJob))
      applicationsRef <- Ref.of[IO, Map[ApplicationId, Application]](Map.empty)
      eventsRef <- Ref.of[IO, Vector[ApplicationEvent]](Vector.empty)
      nextCreateError <- Ref.of[IO, Option[RepositoryError]](None)
      users = InMemoryUsers(
        usersRef,
        Some(com.example.graphQL.cats.service.ServiceFixtures.userRelations(usersRef, jobsRef, applicationsRef))
      )
      jobs = InMemoryJobs(
        jobsRef,
        relationLookup =
          Some(com.example.graphQL.cats.service.ServiceFixtures.jobRelations(usersRef, jobsRef, applicationsRef))
      )
      applications = InMemoryApplications(
        applicationsRef,
        eventsRef,
        nextCreateError,
        jobLookup = id => jobsRef.get.map(_.get(id))
      )
      searchService = SemanticSearchService(
        users,
        jobs,
        FakeEmbeddingService(Right(EmbeddingVector(List(0.1f, 0.2f), "voyage-4-lite", 2))),
        FakeSemanticSearchRepository(
          List(
            RankedJob(
              embeddedJob,
              0.98,
              SearchMode.HYBRID,
              meta,
              UUID.fromString("10000000-0000-0000-0000-000000000099")
            )
          )
        ),
        embeddingModel = "voyage-4-lite"
      )
      services = HiringGraphQLServices(
        HiringReadService(users, jobs, applications),
        com.example.graphQL.cats.service.TestHiringServices.job(users, jobs),
        com.example.graphQL.cats.service.TestHiringServices.applications(users, jobs, applications),
        TestGraphQLSupport.cursorKey,
        TestGraphQLSupport.accountService,
        TestGraphQLSupport.interactions,
        TestGraphQLSupport.searchSessions,
        Some(searchService)
      )
      request <- parseRequest(query)
      result <- TestGraphQLSupport
        .context(IO.pure(ProbeResult.Ready), actor, services)
        .use(TestGraphQLSupport.parseAndExecute(request, _))
    } yield result.fold(failure => fail(failure.toString), identity)
  }

  private def executeWithUsers(
      query: String,
      actor: Option[ActorContext],
      users: List[User],
      accountService: AccountUseCases = TestGraphQLSupport.accountService,
      variables: Json = Json.obj(),
      hiringReady: IO[ProbeResult] = IO.pure(ProbeResult.Ready),
      analyticsReporting: AnalyticsReportingUseCases = AnalyticsReportingUseCases.unavailable,
      embeddingCoverage: com.example.graphQL.cats.service.port.UserRepository => EmbeddingCoverageUseCases = _ =>
        EmbeddingCoverageUseCases.denyAll
  ): IO[Json] =
    executeEitherWithUsers(
      query,
      actor,
      users,
      accountService,
      variables,
      hiringReady,
      analyticsReporting,
      embeddingCoverage
    )
      .map(_.fold(failure => fail(failure.toString), identity))

  private def executeEitherWithUsers(
      query: String,
      actor: Option[ActorContext],
      users: List[User],
      accountService: AccountUseCases = TestGraphQLSupport.accountService,
      variables: Json = Json.obj(),
      hiringReady: IO[ProbeResult] = IO.pure(ProbeResult.Ready),
      analyticsReporting: AnalyticsReportingUseCases = AnalyticsReportingUseCases.unavailable,
      embeddingCoverage: com.example.graphQL.cats.service.port.UserRepository => EmbeddingCoverageUseCases
  ): IO[Either[HiringGraphQLSchema.Failure, Json]] = {
    for {
      usersRef <- Ref.of[IO, Map[UserId, User]](users.map(user => user.id -> user).toMap)
      jobsRef <- Ref.of[IO, Map[JobId, Job]](List(openJob, closedJob).map(job => job.id -> job).toMap)
      applicationsRef <- Ref.of[IO, Map[ApplicationId, Application]](Map(application.id -> application))
      eventsRef <- Ref.of[IO, Vector[ApplicationEvent]](Vector.empty)
      nextCreateError <- Ref.of[IO, Option[com.example.graphQL.cats.service.RepositoryError]](None)
      users = InMemoryUsers(
        usersRef,
        Some(com.example.graphQL.cats.service.ServiceFixtures.userRelations(usersRef, jobsRef, applicationsRef))
      )
      jobs = InMemoryJobs(
        jobsRef,
        relationLookup =
          Some(com.example.graphQL.cats.service.ServiceFixtures.jobRelations(usersRef, jobsRef, applicationsRef))
      )
      applications = InMemoryApplications(
        applicationsRef,
        eventsRef,
        nextCreateError,
        jobLookup = id => jobsRef.get.map(_.get(id))
      )
      services = HiringGraphQLServices(
        HiringReadService(users, jobs, applications),
        com.example.graphQL.cats.service.TestHiringServices.job(users, jobs),
        com.example.graphQL.cats.service.TestHiringServices.applications(users, jobs, applications),
        TestGraphQLSupport.cursorKey,
        accountService = accountService,
        interactionService = TestGraphQLSupport.interactions,
        searchSessions = TestGraphQLSupport.searchSessions,
        analyticsReporting = analyticsReporting,
        embeddingCoverage = embeddingCoverage(users)
      )
      request <- parseRequest(query, variables)
      result <- TestGraphQLSupport
        .context(IO.pure(ProbeResult.Ready), actor, services, hiringReady)
        .use(TestGraphQLSupport.parseAndExecute(request, _))
    } yield result
  }

  private def encodeCursor(json: Json): String =
    Base64.getUrlEncoder.withoutPadding().encodeToString(json.noSpaces.getBytes(StandardCharsets.UTF_8))

  private final class RecordingUsers(
      ref: Ref[IO, Map[UserId, User]],
      batches: Ref[IO, Vector[List[UserId]]],
      jobs: Ref[IO, Map[JobId, Job]],
      applications: Ref[IO, Map[ApplicationId, Application]]
  ) extends com.example.graphQL.cats.service.ServiceFixtures.VersionedUserRepositoryTestAdapter {
    override def relatedUsers(
        scope: com.example.graphQL.cats.service.read.HiringReadScope,
        keys: List[com.example.graphQL.cats.service.read.UserRelationKey]
    ) =
      com.example.graphQL.cats.service.port.RepositoryIO.lift(
        com.example.graphQL.cats.service.ServiceFixtures
          .userRelations(ref, jobs, applications)(scope, keys)
          .flatTap(values => batches.update(_ :+ values.map(_.value.id)))
      )

    override def find(id: UserId): RepositoryIO[Option[User]] =
      com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(ref.get.map(_.get(id)).map(Right(_)))

    override def findMany(ids: List[UserId]): RepositoryIO[List[User]] =
      com.example.graphQL.cats.service.port.RepositoryIO
        .fromIOEither(batches.update(_ :+ ids) *> ref.get.map(users => Right(ids.distinct.flatMap(users.get))))

    override def updateEmbedding(
        id: UserId,
        embedding: com.example.graphQL.cats.domain.model.EntityEmbedding
    ): RepositoryIO[Unit] = com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(ref.modify { users =>
      users.get(id) match {
        case Some(user) => (users + (id -> user.copy(embedding = Some(embedding))), Right(()))
        case None       => (users, Left(RepositoryError.Conflict))
      }
    })
  }

  private final case class FakeEmbeddingService(result: Either[EmbeddingError, EmbeddingVector])
      extends EmbeddingService {
    override def embed(input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]] =
      IO.pure(result)
  }

  private final case class FakeSemanticSearchRepository(jobs: List[RankedJob]) extends SemanticSearchRepository {
    override def authorizedJobEligibility(
        scope: com.example.graphQL.cats.service.read.HiringReadScope,
        ids: List[com.example.graphQL.cats.domain.model.Identifiers.JobId],
        expected: Option[com.example.graphQL.cats.service.search.CandidateSearchEligibility]
    ) =
      if (scope.role == UserRole.Candidate) jobEligibility(ids)
      else com.example.graphQL.cats.service.port.RepositoryIO.fromEither(Right(Nil))
    override def authorizedCandidateEligibility(
        scope: com.example.graphQL.cats.service.read.HiringReadScope,
        expected: com.example.graphQL.cats.service.search.JobSearchEligibility,
        ids: List[com.example.graphQL.cats.domain.model.Identifiers.UserId]
    ) =
      com.example.graphQL.cats.service.port.RepositoryIO
        .fromEither(Right(List.empty[com.example.graphQL.cats.service.search.CandidateSearchEligibility]))

    override def jobEligibility(ids: List[com.example.graphQL.cats.domain.model.Identifiers.JobId]) =
      com.example.graphQL.cats.service.port.RepositoryIO.fromEither(
        Right(
          jobs
            .filter(hit => ids.contains(hit.job.id))
            .map(hit => com.example.graphQL.cats.service.search.JobSearchEligibility.fromJob(hit.job))
        )
      )
    override def candidateEligibility(ids: List[com.example.graphQL.cats.domain.model.Identifiers.UserId]) =
      com.example.graphQL.cats.service.port.RepositoryIO
        .fromEither(Right(List.empty[com.example.graphQL.cats.service.search.CandidateSearchEligibility]))

    override def searchJobs(query: VectorSearchQuery): RepositoryIO[List[JobRetrievalHit]] =
      com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(IO.pure(Right(jobs.map(_.retrieval))))

    override def recommendedJobs(query: VectorSearchQuery): RepositoryIO[List[JobRetrievalHit]] =
      com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(IO.pure(Right(jobs.map(_.retrieval))))

    override def candidateMatches(query: VectorSearchQuery): RepositoryIO[List[CandidateRetrievalHit]] =
      com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(IO.pure(Right(Nil)))
  }

  private final class RecordingAccountService(updateCalls: Ref[IO, Int]) extends AccountUseCases {
    private val unsupported: UseCaseError =
      UseCaseError.Account(com.example.graphQL.cats.service.AccountError.ProfileUnsupportedForRole)

    override def signUp(request: IdempotencyRequest, input: SignUpInput): UseCaseIO[(User, AccountToken)] =
      UseCaseIO.left(unsupported)

    override def login(request: IdempotencyRequest, input: LoginInput): UseCaseIO[(User, AccountToken)] =
      UseCaseIO.left(unsupported)

    override def me(actor: ActorContext): UseCaseIO[User] =
      UseCaseIO.left(unsupported)

    override def updateMyProfile(
        request: IdempotencyRequest,
        actor: ActorContext,
        input: AccountProfileInput
    ): UseCaseIO[User] =
      UseCaseIO.fromIO(
        updateCalls.update(_ + 1) *> IO.pure(
          UserProfile
            .validateFor(actor.role, Some(input.profile))
            .toEither
            .leftMap(UseCaseError.ValidationFailed.apply)
            .map(_ => recruiter)
        )
      )

    override def deleteMyAccount(request: IdempotencyRequest, actor: ActorContext): UseCaseIO[String] =
      UseCaseIO.left(unsupported)

    override def accountDeletionStatus(actor: ActorContext, receiptId: String): UseCaseIO[AccountDeletionStatus] =
      UseCaseIO.pure(AccountDeletionStatus.NotFound)

    override def listUsers(actor: ActorContext, page: UserPageRequest): UseCaseIO[List[User]] =
      UseCaseIO.left(unsupported)
  }

  private object SignUpReturningCandidateService extends AccountUseCases {
    private def unsupported[A]: UseCaseIO[A] =
      UseCaseIO.left(UseCaseError.Account(com.example.graphQL.cats.service.AccountError.ProfileUnsupportedForRole))
    override def signUp(request: IdempotencyRequest, input: SignUpInput): UseCaseIO[(User, AccountToken)] =
      UseCaseIO.pure(candidate -> AccountToken("token", now.plusSeconds(60)))
    override def login(request: IdempotencyRequest, input: LoginInput): UseCaseIO[(User, AccountToken)] = unsupported
    override def me(actor: ActorContext): UseCaseIO[User] = unsupported
    override def updateMyProfile(
        request: IdempotencyRequest,
        actor: ActorContext,
        input: AccountProfileInput
    ): UseCaseIO[User] = unsupported
    override def deleteMyAccount(request: IdempotencyRequest, actor: ActorContext): UseCaseIO[String] = unsupported
    override def accountDeletionStatus(actor: ActorContext, receiptId: String): UseCaseIO[AccountDeletionStatus] =
      UseCaseIO.pure(AccountDeletionStatus.NotFound)
    override def listUsers(actor: ActorContext, page: UserPageRequest): UseCaseIO[List[User]] = unsupported
  }

  private final class PublicAccountService(calls: Ref[IO, Int]) extends AccountUseCases {
    private def unavailable[A]: UseCaseIO[A] =
      UseCaseIO.left(UseCaseError.Availability(com.example.graphQL.cats.service.AvailabilityError.ServiceNotReady))

    override def signUp(request: IdempotencyRequest, input: SignUpInput): UseCaseIO[(User, AccountToken)] =
      UseCaseIO.liftIO(calls.update(_ + 1)) *> unavailable

    override def login(request: IdempotencyRequest, input: LoginInput): UseCaseIO[(User, AccountToken)] =
      UseCaseIO.liftIO(calls.update(_ + 1)) *> unavailable

    override def me(actor: ActorContext): UseCaseIO[User] = unavailable
    override def updateMyProfile(
        request: IdempotencyRequest,
        actor: ActorContext,
        input: AccountProfileInput
    ): UseCaseIO[User] = unavailable
    override def deleteMyAccount(request: IdempotencyRequest, actor: ActorContext): UseCaseIO[String] = unavailable
    override def accountDeletionStatus(actor: ActorContext, receiptId: String): UseCaseIO[AccountDeletionStatus] =
      UseCaseIO.pure(AccountDeletionStatus.NotFound)
    override def listUsers(actor: ActorContext, page: UserPageRequest): UseCaseIO[List[User]] = unavailable
  }

  private object NameTakenAccountService extends AccountUseCases {
    private val unsupported: UseCaseError =
      UseCaseError.Account(com.example.graphQL.cats.service.AccountError.ProfileUnsupportedForRole)

    override def signUp(request: IdempotencyRequest, input: SignUpInput): UseCaseIO[(User, AccountToken)] =
      UseCaseIO.left(UseCaseError.Account(com.example.graphQL.cats.service.AccountError.NameTaken))

    override def login(request: IdempotencyRequest, input: LoginInput): UseCaseIO[(User, AccountToken)] =
      UseCaseIO.left(unsupported)

    override def me(actor: ActorContext): UseCaseIO[User] =
      UseCaseIO.left(unsupported)

    override def updateMyProfile(
        request: IdempotencyRequest,
        actor: ActorContext,
        input: AccountProfileInput
    ): UseCaseIO[User] =
      UseCaseIO.left(unsupported)

    override def deleteMyAccount(request: IdempotencyRequest, actor: ActorContext): UseCaseIO[String] =
      UseCaseIO.left(unsupported)

    override def accountDeletionStatus(actor: ActorContext, receiptId: String): UseCaseIO[AccountDeletionStatus] =
      UseCaseIO.pure(AccountDeletionStatus.NotFound)

    override def listUsers(actor: ActorContext, page: UserPageRequest): UseCaseIO[List[User]] =
      UseCaseIO.left(unsupported)
  }

  private def job(id: JobId, status: JobStatus): Job =
    Job(
      id,
      recruiterId,
      "Senior Scala Developer",
      "Build backend services",
      List("Scala"),
      Set("Scala", "Cats Effect"),
      Location("Ukraine", "Kyiv", remote = true),
      status,
      now,
      now
    )
}
