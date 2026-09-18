package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import cats.effect.Ref
import cats.syntax.all.*
import com.example.graphQL.cats.api.graphql.{CursorCodec, GraphQLRequest, HiringGraphQLSchema, HiringGraphQLServices, RequestContext}
import com.example.graphQL.cats.service.{ActorContext, HiringReadService, ProbeResult, UseCaseError}
import com.example.graphQL.cats.repository.protocol.{EmbeddingError, EmbeddingInput, EmbeddingService, EmbeddingVector, SemanticSearchRepository, UserRepository}
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.ServiceFixtures.{InMemoryApplications, InMemoryJobs, InMemoryUsers}
import com.example.graphQL.cats.service.application.ApplicationService
import com.example.graphQL.cats.service.job.JobService
import com.example.graphQL.cats.service.protocol.{AccountProfileInput, AccountUseCases, BootstrapAdminInput, LoginInput, SignUpInput}
import com.example.graphQL.cats.service.search.SemanticSearchService
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.shared.search.{RankedCandidate, RankedJob, VectorSearchQuery}
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

  private val candidate = User(candidateId, Some("candidate@example.com"), "Candidate", UserRole.Candidate,
    Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None))), now)
  private val recruiter = User(recruiterId, Some("recruiter@example.com"), "Recruiter", UserRole.Recruiter,
    Some(UserProfile.Recruiter(RecruiterProfile("Acme", None))), now)
  private val admin = User(adminId, Some("admin@example.com"), "Admin", UserRole.Admin, None, now, adminSingleton = true)
  private val openJob = job(jobId, JobStatus.Open)
  private val closedJob = job(closedJobId, JobStatus.Closed)
  private val application = Application.create(applicationId, candidateId, jobId, now)

  test("hiring mutation without ActorContext returns typed unauthorized payload") {
    val query =
      s"""mutation {
         |  submitApplication(input: { jobId: "${jobId.value}" }) {
         |    application { id }
         |    errors { code message }
         |  }
         |}""".stripMargin

    execute(query, None).map { json =>
      assertEquals(json.hcursor.downField("data").downField("submitApplication").downField("errors").downArray.get[String]("code"),
        Right("UNAUTHORIZED"))
      assert(!json.hcursor.downField("errors").succeeded)
    }
  }

  test("application connection rejects a job cursor") {
    val cursor = CursorCodec.encodeJob(com.example.graphQL.cats.shared.pagination.JobCursor(now, jobId))
    val query =
      s"""query {
         |  myApplications(first: 10, after: "$cursor") {
         |    edges { node { id } }
         |    errors { code message }
         |  }
         |}""".stripMargin

    execute(query, Some(ActorContext(candidateId, UserRole.Candidate))).map { json =>
      val applications = json.hcursor.downField("data").downField("myApplications")
      assertEquals(applications.downField("edges").focus.flatMap(_.asArray).map(_.size), Some(0))
      assertEquals(applications.downField("errors").downArray.get[String]("code"), Right("INVALID_CURSOR"))
      assert(!json.hcursor.downField("errors").succeeded)
    }
  }

  test("job search without ActorContext returns typed unauthorized connection") {
    val query =
      """query {
        |  jobs(first: 10) {
        |    edges { node { id } }
        |    errors { code message }
        |  }
        |}""".stripMargin

    execute(query, None).map { json =>
      val jobs = json.hcursor.downField("data").downField("jobs")
      assertEquals(jobs.downField("edges").focus.flatMap(_.asArray).map(_.size), Some(0))
      assertEquals(jobs.downField("errors").downArray.get[String]("code"), Right("UNAUTHORIZED"))
      assert(!json.hcursor.downField("errors").succeeded)
    }
  }

  test("nested user email is projected by service authorization") {
    val query =
      """query {
        |  myApplications(first: 10) {
        |    edges { node { candidate { email } job { recruiter { email } } } }
        |    errors { code }
        |  }
        |}""".stripMargin

    execute(query, Some(ActorContext(candidateId, UserRole.Candidate))).map { json =>
      val candidateEmail = json.hcursor.downField("data").downField("myApplications").downField("edges").downArray
        .downField("node").downField("candidate").get[String]("email")
      val recruiterEmail = json.hcursor.downField("data").downField("myApplications").downField("edges").downArray
        .downField("node").downField("job").downField("recruiter").get[Option[String]]("email")
      assertEquals(candidateEmail, Right("candidate@example.com"))
      assertEquals(recruiterEmail, Right(None))
    }
  }

  test("job connection rejects a cursor with malformed fields as a typed error") {
    val cursor = encodeCursor(Json.obj(
      "kind" -> Json.fromString("job"),
      "createdAt" -> Json.fromString("not-an-instant"),
      "occurredAt" -> Json.Null,
      "id" -> Json.fromString(jobId.value.toString)
    ))
    val query =
      s"""query {
         |  myJobs(first: 10, after: "$cursor") {
         |    edges { node { id } }
         |    errors { code message }
         |  }
         |}""".stripMargin

    execute(query, Some(ActorContext(recruiterId, UserRole.Recruiter))).map { json =>
      val jobs = json.hcursor.downField("data").downField("myJobs")
      assertEquals(jobs.downField("edges").focus.flatMap(_.asArray).map(_.size), Some(0))
      assertEquals(jobs.downField("errors").downArray.get[String]("code"), Right("INVALID_CURSOR"))
      assert(!json.hcursor.downField("errors").succeeded)
    }
  }

  test("injected hiring context searches only open jobs and preserves nested recruiter batching boundary") {
    val query =
      """query {
        |  jobs(first: 10, city: "Kyiv", skills: ["Scala"]) {
        |    edges {
        |      node {
        |        id
        |        title
        |        recruiter { id name }
        |      }
        |    }
        |    errors { code }
        |  }
        |}""".stripMargin

    execute(query, Some(ActorContext(candidateId, UserRole.Candidate))).map { json =>
      val edges = json.hcursor.downField("data").downField("jobs").downField("edges").focus.getOrElse(Json.Null)
      assertEquals(edges.asArray.map(_.size), Some(1))
      assertEquals(edges.hcursor.downArray.downField("node").get[String]("id"), Right(jobId.value.toString))
      assertEquals(edges.hcursor.downArray.downField("node").downField("recruiter").get[String]("id"), Right(recruiterId.value.toString))
      assertEquals(json.hcursor.downField("data").downField("jobs").downField("errors").focus.flatMap(_.asArray).map(_.size), Some(0))
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
        |    errors { code }
        |  }
        |}""".stripMargin

    execute(query, Some(ActorContext(candidateId, UserRole.Candidate))).map { json =>
      val profile = json.hcursor.downField("data").downField("jobs").downField("edges").downArray
        .downField("node").downField("recruiter").downField("profile")
      assertEquals(profile.get[String]("__typename"), Right("RecruiterProfile"))
      assertEquals(profile.get[String]("organizationName"), Right("Acme"))
    }
  }

  test("job search rejects malformed createdAfter instead of widening results") {
    val query =
      """query {
        |  jobs(first: 10, createdAfter: "not-an-instant") {
        |    edges { node { id } }
        |    errors { code message }
        |  }
        |}""".stripMargin

    for {
      request <- IO.fromOption(GraphQLRequest.parseBody(Json.obj("query" -> Json.fromString(query)).noSpaces))(
        new IllegalArgumentException("Invalid GraphQL test request"))
      context <- RequestContext.resource(IO.pure(ProbeResult.Ready), Some(ActorContext(candidateId, UserRole.Candidate)), None).allocated
      result <- HiringGraphQLSchema.executeInContext(request, context._1).guarantee(context._2)
    } yield assertEquals(result, Left(HiringGraphQLSchema.Failure.InvalidQuery))
  }

  test("myJobs resolves stored actor before recruiter ownership lookup") {
    val query =
      """query {
        |  myJobs(first: 10) {
        |    edges { node { id } }
        |    errors { code message }
        |  }
        |}""".stripMargin

    execute(query, Some(ActorContext(recruiterId, UserRole.Candidate))).map { json =>
      val jobs = json.hcursor.downField("data").downField("myJobs")
      assertEquals(jobs.downField("edges").focus.flatMap(_.asArray).map(_.size), Some(0))
      assertEquals(jobs.downField("errors").downArray.get[String]("code"), Right("FORBIDDEN"))
      assert(!json.hcursor.downField("errors").succeeded)
    }
  }

  test("myJobs lists recruiter-owned jobs for singleton admin") {
    val query =
      """query {
        |  myJobs(first: 10) {
        |    edges { node { id } }
        |    errors { code message }
        |  }
        |}""".stripMargin

    executeWithUsers(query, Some(ActorContext(adminId, UserRole.Admin)), List(candidate, recruiter, admin)).map { json =>
      val jobs = json.hcursor.downField("data").downField("myJobs")
      val ids = jobs.downField("edges").focus.flatMap(_.asArray).getOrElse(Vector.empty)
        .flatMap(_.hcursor.downField("node").get[String]("id").toOption)
        .toSet
      assertEquals(ids, Set(jobId.value.toString, closedJobId.value.toString))
      assertEquals(jobs.downField("errors").focus.flatMap(_.asArray).map(_.size), Some(0))
      assert(!json.hcursor.downField("errors").succeeded)
    }
  }

  test("Admin profile update is rejected before constructing a candidate profile") {
    for {
      updateCalls <- Ref.of[IO, Int](0)
      accountService = new RecordingAccountService(updateCalls)
      query =
        """mutation {
          |  updateMyProfile(input: { skills: ["Scala"] }) {
          |    user { profile { __typename } }
          |    errors { code }
          |  }
          |}""".stripMargin
      json <- executeWithUsers(query, Some(ActorContext(adminId, UserRole.Admin)), List(admin), Some(accountService))
      calls <- updateCalls.get
    } yield {
      val payload = json.hcursor.downField("data").downField("updateMyProfile")
      assert(payload.downField("user").focus.contains(Json.Null))
      assertEquals(payload.downField("errors").downArray.get[String]("code"), Right("PROFILE_UNSUPPORTED_FOR_ROLE"))
      assertEquals(calls, 0)
    }
  }

  test("injected candidate context lists only the candidate applications") {
    val query =
      """query {
        |  myApplications(first: 10) {
        |    edges {
        |      node {
        |        id
        |        status
        |        job { id title }
        |      }
        |    }
        |    errors { code }
        |  }
        |}""".stripMargin

    execute(query, Some(ActorContext(candidateId, UserRole.Candidate))).map { json =>
      val node = json.hcursor.downField("data").downField("myApplications").downField("edges").downArray.downField("node")
      assertEquals(node.get[String]("id"), Right(applicationId.value.toString))
      assertEquals(node.get[String]("status"), Right("Created"))
      assertEquals(node.downField("job").get[String]("id"), Right(jobId.value.toString))
    }
  }

  test("application job recruiter nesting preloads recruiter users in one batch") {
    val secondRecruiterId = UserId(UUID.fromString("10000000-0000-0000-0000-000000000007"))
    val secondJobId = JobId(UUID.fromString("10000000-0000-0000-0000-000000000008"))
    val secondApplicationId = ApplicationId(UUID.fromString("10000000-0000-0000-0000-000000000009"))
    val secondRecruiter = User(secondRecruiterId, Some("recruiter2@example.com"), "Recruiter 2", UserRole.Recruiter,
      Some(UserProfile.Recruiter(RecruiterProfile("Acme 2", None))), now)
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
        |    errors { code }
        |  }
        |}""".stripMargin

    for {
      usersRef <- Ref.of[IO, Map[UserId, User]](
        List(candidate, recruiter, secondRecruiter).map(user => user.id -> user).toMap)
      userBatches <- Ref.of[IO, Vector[List[UserId]]](Vector.empty)
      jobsRef <- Ref.of[IO, Map[JobId, Job]](List(openJob, secondJob).map(job => job.id -> job).toMap)
      applicationsRef <- Ref.of[IO, Map[ApplicationId, Application]](
        List(application, secondApplication).map(application => application.id -> application).toMap)
      eventsRef <- Ref.of[IO, Vector[ApplicationEvent]](Vector.empty)
      nextCreateError <- Ref.of[IO, Option[com.example.graphQL.cats.service.RepositoryError]](None)
      users = RecordingUsers(usersRef, userBatches)
      jobs = InMemoryJobs(jobsRef)
      applications = InMemoryApplications(applicationsRef, eventsRef, nextCreateError)
      services = HiringGraphQLServices(HiringReadService[IO](users, jobs, applications), JobService[IO](users, jobs), ApplicationService[IO](users, jobs, applications))
      request <- IO.fromOption(GraphQLRequest.parseBody(Json.obj("query" -> Json.fromString(query)).noSpaces))(
        new IllegalArgumentException("Invalid GraphQL test request"))
      result <- RequestContext.resource(IO.pure(ProbeResult.Ready), Some(ActorContext(candidateId, UserRole.Candidate)), Some(services))
        .use(HiringGraphQLSchema.executeInContext(request, _))
      batches <- userBatches.get
    } yield {
      val json = result.fold(failure => fail(failure.toString), identity)
      val applications = json.hcursor.downField("data").downField("myApplications")
      assertEquals(applications.downField("errors").focus.flatMap(_.asArray).map(_.size), Some(0))
      assert(batches.exists(_.toSet == Set(recruiterId, secondRecruiterId)))
      assert(!batches.exists(batch => batch.size == 1 && Set(recruiterId, secondRecruiterId).contains(batch.head)))
    }
  }

  test("createJob input publishes skills under the domain field name") {
    val query =
      """mutation {
        |  createJob(input: {
        |    title: "Staff Scala Developer"
        |    description: "Build platform services"
        |    requirements: ["Scala"]
        |    skills: ["Scala", "Cats Effect"]
        |    country: "Cyprus"
        |    city: "Nicosia"
        |    remote: true
        |  }) {
        |    job { title skills }
        |    errors { code }
        |  }
        |}""".stripMargin

    execute(query, Some(ActorContext(recruiterId, UserRole.Recruiter))).map { json =>
      val payload = json.hcursor.downField("data").downField("createJob")
      assertEquals(payload.downField("job").get[String]("title"), Right("Staff Scala Developer"))
      assertEquals(payload.downField("job").downField("skills").focus.flatMap(_.asArray).map(_.flatMap(_.asString).toList),
        Some(List("Cats Effect", "Scala")))
      assertEquals(payload.downField("errors").focus.flatMap(_.asArray).map(_.size), Some(0))
      assert(!json.hcursor.downField("errors").succeeded)
    }
  }

  test("createJob validation failures use the named validation error case") {
    val query =
      """mutation {
        |  createJob(input: {
        |    title: "Staff Scala Developer"
        |    description: "Build platform services"
        |    requirements: ["Scala"]
        |    skills: ["Scala"]
        |    country: ""
        |    remote: true
        |  }) {
        |    job { id }
        |    errors { code message }
        |  }
        |}""".stripMargin

    execute(query, Some(ActorContext(recruiterId, UserRole.Recruiter))).map { json =>
      val payload = json.hcursor.downField("data").downField("createJob")
      assertEquals(payload.downField("job").focus, Some(Json.Null))
      assertEquals(payload.downField("errors").downArray.get[String]("code"), Right("VALIDATION_FAILED"))
      assertEquals(payload.downField("errors").downArray.get[String]("message"), Right("country is required, city is required"))
      assert(!json.hcursor.downField("errors").succeeded)
    }
  }

  test("submitApplication duplicate and closed-job failures stay in typed payloads") {
    val duplicate =
      s"""mutation {
         |  submitApplication(input: { jobId: "${jobId.value}" }) {
         |    application { id }
         |    errors { code message }
         |  }
         |}""".stripMargin
    val closed =
      s"""mutation {
         |  submitApplication(input: { jobId: "${closedJobId.value}" }) {
         |    application { id }
         |    errors { code message }
         |  }
         |}""".stripMargin

    (execute(duplicate, Some(ActorContext(candidateId, UserRole.Candidate))),
      execute(closed, Some(ActorContext(candidateId, UserRole.Candidate)))).mapN { (duplicateJson, closedJson) =>
      val duplicatePayload = duplicateJson.hcursor.downField("data").downField("submitApplication")
      val closedPayload = closedJson.hcursor.downField("data").downField("submitApplication")
      assertEquals(duplicatePayload.downField("application").focus, Some(Json.Null))
      assertEquals(duplicatePayload.downField("errors").downArray.get[String]("code"), Right("DUPLICATE_APPLICATION"))
      assert(!duplicateJson.hcursor.downField("errors").succeeded)
      assertEquals(closedPayload.downField("application").focus, Some(Json.Null))
      assertEquals(closedPayload.downField("errors").downArray.get[String]("code"), Right("JOB_MUST_BE_OPEN"))
      assert(!closedJson.hcursor.downField("errors").succeeded)
    }
  }

  test("application status mutation invalid transition stays in typed payloads") {
    val query =
      s"""mutation {
         |  hireApplication(input: { applicationId: "${applicationId.value}" }) {
         |    application { id }
         |    errors { code message }
         |  }
         |}""".stripMargin

    execute(query, Some(ActorContext(recruiterId, UserRole.Recruiter))).map { json =>
      val payload = json.hcursor.downField("data").downField("hireApplication")
      assertEquals(payload.downField("application").focus, Some(Json.Null))
      assertEquals(payload.downField("errors").downArray.get[String]("code"), Right("INVALID_STATUS_TRANSITION"))
      assert(!json.hcursor.downField("errors").succeeded)
    }
  }

  test("candidate application history access resolves stored actor before ownership") {
    val query =
      s"""query {
         |  applicationHistory(applicationId: "${applicationId.value}", first: 10) {
         |    edges { node { id } }
         |    errors { code message }
         |  }
         |}""".stripMargin

    executeWithUsers(query, Some(ActorContext(candidateId, UserRole.Candidate)), List(recruiter)).map { json =>
      val history = json.hcursor.downField("data").downField("applicationHistory")
      assertEquals(history.downField("edges").focus.flatMap(_.asArray).map(_.size), Some(0))
      assertEquals(history.downField("errors").downArray.get[String]("code"), Right("UNAUTHORIZED"))
      assert(!json.hcursor.downField("errors").succeeded)
    }
  }

  test("application rejection uses reject input object and returns typed payload errors") {
    val query =
      s"""mutation {
         |  rejectApplication(input: { applicationId: "${applicationId.value}", feedback: "Not enough Scala" }) {
         |    application { id status }
         |    errors { code message }
         |  }
         |}""".stripMargin

    execute(query, Some(ActorContext(recruiterId, UserRole.Recruiter))).map { json =>
      val payload = json.hcursor.downField("data").downField("rejectApplication")
      assertEquals(payload.downField("application").get[String]("id"), Right(applicationId.value.toString))
      assertEquals(payload.downField("application").get[String]("status"), Right("Rejected"))
      assertEquals(payload.downField("errors").focus.flatMap(_.asArray).map(_.size), Some(0))
      assert(!json.hcursor.downField("errors").succeeded)
    }
  }

  test("application rejection and decline validation failures stay in typed payloads") {
    val reject =
      s"""mutation {
         |  rejectApplication(input: { applicationId: "${applicationId.value}", feedback: " " }) {
         |    application { id }
         |    errors { code message }
         |  }
         |}""".stripMargin
    val decline =
      s"""mutation {
         |  declineApplication(input: { applicationId: "${applicationId.value}", reason: "" }) {
         |    application { id }
         |    errors { code message }
         |  }
         |}""".stripMargin

    (execute(reject, Some(ActorContext(recruiterId, UserRole.Recruiter))),
      execute(decline, Some(ActorContext(recruiterId, UserRole.Recruiter)))).mapN { (rejectJson, declineJson) =>
      val rejectPayload = rejectJson.hcursor.downField("data").downField("rejectApplication")
      val declinePayload = declineJson.hcursor.downField("data").downField("declineApplication")
      assertEquals(rejectPayload.downField("application").focus, Some(Json.Null))
      assertEquals(rejectPayload.downField("errors").downArray.get[String]("code"), Right("REJECTION_FEEDBACK_REQUIRED"))
      assert(!rejectJson.hcursor.downField("errors").succeeded)
      assertEquals(declinePayload.downField("application").focus, Some(Json.Null))
      assertEquals(declinePayload.downField("errors").downArray.get[String]("code"), Right("DECLINE_REASON_REQUIRED"))
      assert(!declineJson.hcursor.downField("errors").succeeded)
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
        |      version
        |      searchId
        |    }
        |    errors { code message }
        |  }
        |}""".stripMargin

    executeWithSemanticSearch(query, Some(ActorContext(candidateId, UserRole.Candidate))).map { json =>
      val payload = json.hcursor.downField("data").downField("semanticJobSearch")
      assertEquals(payload.downField("errors").focus.flatMap(_.asArray).map(_.size), Some(0))
      assertEquals(payload.downField("results").downArray.downField("job").get[String]("id"), Right(jobId.value.toString))
      assertEquals(payload.downField("results").downArray.get[String]("searchMode"), Right("HYBRID"))
      assertEquals(payload.downField("results").downArray.get[String]("model"), Right("voyage-4-lite"))
      assertEquals(payload.downField("results").downArray.get[Int]("version"), Right(1))
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
      request <- IO.fromOption(GraphQLRequest.parseBody(Json.obj("query" -> Json.fromString(query)).noSpaces))(
        new IllegalArgumentException("Invalid GraphQL test request"))
      result <- RequestContext.resource(IO.pure(ProbeResult.Ready)).use(HiringGraphQLSchema.executeInContext(request, _))
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
         |    errors { code }
         |  }
         |}""".stripMargin

    executeWithSemanticSearch(query, Some(ActorContext(recruiterId, UserRole.Recruiter))).map { json =>
      val payload = json.hcursor.downField("data").downField("candidateMatches")
      assertEquals(payload.downField("errors").downArray.get[String]("code"), Right("STALE_EMBEDDING"))
      assert(!json.hcursor.downField("errors").succeeded)
    }
  }

  private def execute(query: String, actor: Option[ActorContext]): IO[Json] = {
    executeWithUsers(query, actor, List(candidate, recruiter))
  }

  private def executeWithSemanticSearch(query: String, actor: Option[ActorContext]): IO[Json] = {
    val meta = EmbeddingMeta("voyage-4-lite", 1, "hash", now)
    val embeddedJob = openJob.copy(embedding = Some(EntityEmbedding(List(0.1f, 0.2f), meta)))
    for {
      usersRef <- Ref.of[IO, Map[UserId, User]](List(candidate, recruiter).map(user => user.id -> user).toMap)
      jobsRef <- Ref.of[IO, Map[JobId, Job]](Map(embeddedJob.id -> embeddedJob))
      applicationsRef <- Ref.of[IO, Map[ApplicationId, Application]](Map.empty)
      eventsRef <- Ref.of[IO, Vector[ApplicationEvent]](Vector.empty)
      nextCreateError <- Ref.of[IO, Option[RepositoryError]](None)
      users = InMemoryUsers(usersRef)
      jobs = InMemoryJobs(jobsRef)
      applications = InMemoryApplications(applicationsRef, eventsRef, nextCreateError)
      searchService = SemanticSearchService[IO](
        users,
        jobs,
        FakeEmbeddingService(Right(EmbeddingVector(List(0.1f, 0.2f), "voyage-4-lite", 2))),
        FakeSemanticSearchRepository(List(RankedJob(
          embeddedJob,
          0.98,
          SearchMode.HYBRID,
          meta,
          UUID.fromString("10000000-0000-0000-0000-000000000099")
        ))),
        embeddingModel = "voyage-4-lite",
        embeddingVersion = 1
      )
      services = HiringGraphQLServices(HiringReadService[IO](users, jobs, applications), JobService[IO](users, jobs), ApplicationService[IO](users, jobs, applications),
        Some(searchService))
      request <- IO.fromOption(GraphQLRequest.parseBody(Json.obj("query" -> Json.fromString(query)).noSpaces))(
        new IllegalArgumentException("Invalid GraphQL test request"))
      result <- RequestContext.resource(IO.pure(ProbeResult.Ready), actor, Some(services)).use(HiringGraphQLSchema.executeInContext(request, _))
    } yield result.fold(failure => fail(failure.toString), identity)
  }

  private def executeWithUsers(
      query: String,
      actor: Option[ActorContext],
      users: List[User],
      accountService: Option[AccountUseCases[IO]] = None
  ): IO[Json] = {
    for {
      usersRef <- Ref.of[IO, Map[UserId, User]](users.map(user => user.id -> user).toMap)
      jobsRef <- Ref.of[IO, Map[JobId, Job]](List(openJob, closedJob).map(job => job.id -> job).toMap)
      applicationsRef <- Ref.of[IO, Map[ApplicationId, Application]](Map(application.id -> application))
      eventsRef <- Ref.of[IO, Vector[ApplicationEvent]](Vector.empty)
      nextCreateError <- Ref.of[IO, Option[com.example.graphQL.cats.service.RepositoryError]](None)
      users = InMemoryUsers(usersRef)
      jobs = InMemoryJobs(jobsRef)
      applications = InMemoryApplications(applicationsRef, eventsRef, nextCreateError)
      services = HiringGraphQLServices(HiringReadService[IO](users, jobs, applications), JobService[IO](users, jobs), ApplicationService[IO](users, jobs, applications), accountService = accountService)
      request <- IO.fromOption(GraphQLRequest.parseBody(Json.obj("query" -> Json.fromString(query)).noSpaces))(
        new IllegalArgumentException("Invalid GraphQL test request"))
      result <- RequestContext.resource(IO.pure(ProbeResult.Ready), actor, Some(services)).use(HiringGraphQLSchema.executeInContext(request, _))
    } yield result.fold(failure => fail(failure.toString), identity)
  }

  private def encodeCursor(json: Json): String =
    Base64.getUrlEncoder.withoutPadding().encodeToString(json.noSpaces.getBytes(StandardCharsets.UTF_8))

  private final class RecordingUsers(
      ref: Ref[IO, Map[UserId, User]],
      batches: Ref[IO, Vector[List[UserId]]]
  ) extends UserRepository[IO] {
    override def find(id: UserId): IO[Option[User]] =
      ref.get.map(_.get(id))

    override def findMany(ids: List[UserId]): IO[List[User]] =
      batches.update(_ :+ ids) *> ref.get.map(users => ids.distinct.flatMap(users.get))

    override def updateEmbedding(
        id: UserId,
        embedding: com.example.graphQL.cats.domain.model.EntityEmbedding
    ): IO[Either[RepositoryError, Unit]] =
      ref.modify { users =>
        users.get(id) match {
          case Some(user) => (users + (id -> user.copy(embedding = Some(embedding))), Right(()))
          case None => (users, Left(RepositoryError.Conflict))
        }
      }
  }

  private final case class FakeEmbeddingService(result: Either[EmbeddingError, EmbeddingVector]) extends EmbeddingService[IO] {
    override def embed(input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]] =
      IO.pure(result)
  }

  private final case class FakeSemanticSearchRepository(jobs: List[RankedJob]) extends SemanticSearchRepository[IO] {
    override def searchJobs(query: VectorSearchQuery): IO[Either[RepositoryError, List[RankedJob]]] =
      IO.pure(Right(jobs))

    override def recommendedJobs(query: VectorSearchQuery): IO[Either[RepositoryError, List[RankedJob]]] =
      IO.pure(Right(jobs))

    override def candidateMatches(query: VectorSearchQuery): IO[Either[RepositoryError, List[RankedCandidate]]] =
      IO.pure(Right(Nil))
  }

  private final class RecordingAccountService(updateCalls: Ref[IO, Int]) extends AccountUseCases[IO] {
    private val unsupported: UseCaseError = UseCaseError.account(com.example.graphQL.cats.service.AccountError.ProfileUnsupportedForRole)

    override def signUp(input: SignUpInput, now: Instant, userId: UserId): IO[Either[UseCaseError, (User, AccountToken)]] =
      IO.pure(Left(unsupported))

    override def bootstrapAdmin(input: BootstrapAdminInput, now: Instant, userId: UserId): IO[Either[UseCaseError, (User, AccountToken)]] =
      IO.pure(Left(unsupported))

    override def login(input: LoginInput, now: Instant): IO[Either[UseCaseError, (User, AccountToken)]] =
      IO.pure(Left(unsupported))

    override def me(actor: ActorContext): IO[Either[UseCaseError, User]] =
      IO.pure(Left(unsupported))

    override def updateMyProfile(actor: ActorContext, input: AccountProfileInput): IO[Either[UseCaseError, User]] =
      updateCalls.update(_ + 1).as(Left(unsupported))

    override def deleteMyAccount(actor: ActorContext, now: Instant): IO[Either[UseCaseError, Unit]] =
      IO.pure(Left(unsupported))

    override def listUsers(actor: ActorContext, page: UserPageRequest): IO[Either[UseCaseError, List[User]]] =
      IO.pure(Left(unsupported))
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
