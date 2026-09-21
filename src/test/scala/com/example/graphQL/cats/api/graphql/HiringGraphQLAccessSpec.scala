package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import cats.effect.Ref
import cats.syntax.all.*
import com.example.graphQL.cats.api.graphql.{GraphQLRequest, HiringGraphQLSchema, HiringGraphQLServices}
import com.example.graphQL.cats.service.{ActorContext, HiringReadService, ProbeResult, UseCaseError}
import com.example.graphQL.cats.repository.protocol.{EmbeddingError, EmbeddingInput, EmbeddingService, EmbeddingVector, SearchSessionRepository, SemanticSearchRepository, UserRepository}
import com.example.graphQL.cats.repository.protocol.RepositoryError
import com.example.graphQL.cats.service.ServiceFixtures.{InMemoryApplications, InMemoryJobs, InMemoryUsers}
import com.example.graphQL.cats.service.application.ApplicationService
import com.example.graphQL.cats.service.job.JobService
import com.example.graphQL.cats.service.protocol.{AccountProfileInput, AccountUseCases, BootstrapAdminInput, LoginInput, SignUpInput}
import com.example.graphQL.cats.service.search.SemanticSearchService
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.shared.search.{RankedCandidate, RankedJob, VectorSearchQuery}
import com.example.graphQL.cats.shared.events.SearchSession
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

  private def errorCode(json: Json): Either[io.circe.Error, String] =
    json.hcursor.downField("errors").downArray.downField("extensions").get[String]("code")

  test("hiring mutation without ActorContext returns typed unauthorized payload") {
    val query =
      s"""mutation {
         |  submitApplication(input: { jobId: "${jobId.value}" }) {
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
      """mutation { updateMyProfile(input: { skills: ["Scala"] }) { __typename } }"""
    val delete =
      """mutation { deleteMyAccount { __typename ... on DeletionSuccess { deleted } } }"""

    (execute(me, None), execute(users, None), execute(update, None), execute(delete, None)).mapN {
      (meJson, usersJson, updateJson, deleteJson) =>
        assertEquals(errorCode(meJson), Right("UNAUTHORIZED"))
        assertEquals(errorCode(usersJson), Right("UNAUTHORIZED"))
        assertEquals(errorCode(updateJson), Right("UNAUTHORIZED"))
        assertEquals(errorCode(deleteJson), Right("UNAUTHORIZED"))
    }
  }

  test("public account mutations are unavailable until hiring setup is ready") {
    val signUp =
      """mutation { signUp(input: { name: "Candidate", role: CANDIDATE, password: "password-password", skills: ["Scala"] }) { __typename } }"""
    val bootstrap =
      """mutation { bootstrapAdmin(input: { name: "Admin", password: "password-password" }) { __typename } }"""
    val login =
      """mutation { login(input: { name: "Candidate", password: "password-password" }) { __typename } }"""
    for {
      calls <- Ref.of[IO, Int](0)
      service = new PublicAccountService(calls)
      results <- List(signUp, bootstrap, login).traverse(query =>
        executeWithUsers(query, None, List(candidate, recruiter), service, hiringReady = IO.pure(ProbeResult.Unavailable)))
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
    val cursor = CursorCodec.encode(com.example.graphQL.cats.shared.pagination.JobCursor(now, jobId))
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
      assertEquals(edges.hcursor.downArray.downField("node").downField("recruiter").get[String]("id"), Right(recruiterId.value.toString))
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
        | __typename
        |  }
        |}""".stripMargin

    for {
      request <- parseRequest(query)
      context <- TestGraphQLSupport.context(IO.pure(ProbeResult.Ready), Some(ActorContext(candidateId, UserRole.Candidate))).allocated
      result <- HiringGraphQLSchema.executeInContext(request, context._1).guarantee(context._2)
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

    executeWithUsers(query, Some(ActorContext(adminId, UserRole.Admin)), List(candidate, recruiter, admin)).map { json =>
      val jobs = json.hcursor.downField("data").downField("myJobs")
      val ids = jobs.downField("edges").focus.flatMap(_.asArray).getOrElse(Vector.empty)
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
          |  updateMyProfile(input: { skills: ["Scala"] }) {
          |    __typename
          | __typename
          |  }
          |}""".stripMargin
      json <- executeWithUsers(query, Some(ActorContext(adminId, UserRole.Admin)), List(admin), accountService)
      calls <- updateCalls.get
    } yield {
      assertEquals(errorCode(json), Right("PROFILE_UNSUPPORTED_FOR_ROLE"))
      assertEquals(calls, 0)
    }
  }

  test("Recruiter profile update delegates semantic validation to the account service") {
    for {
      updateCalls <- Ref.of[IO, Int](0)
      accountService = new RecordingAccountService(updateCalls)
      query =
        """mutation {
          |  updateMyProfile(input: { jobTitle: "Hiring Lead" }) {
          |    __typename
          | __typename
          |  }
          |}""".stripMargin
      json <- executeWithUsers(query, Some(ActorContext(recruiterId, UserRole.Recruiter)), List(recruiter), accountService)
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
        | __typename
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
      nextCreateError <- Ref.of[IO, Option[com.example.graphQL.cats.repository.protocol.RepositoryError]](None)
      users = RecordingUsers(usersRef, userBatches)
      jobs = InMemoryJobs(jobsRef)
      applications = InMemoryApplications(applicationsRef, eventsRef, nextCreateError)
      services = HiringGraphQLServices(HiringReadService(users, jobs, applications), JobService(users, jobs), ApplicationService(users, jobs, applications), TestGraphQLSupport.cursorKey, TestGraphQLSupport.accountService)
      request <- parseRequest(query)
      result <- TestGraphQLSupport.context(IO.pure(ProbeResult.Ready), Some(ActorContext(candidateId, UserRole.Candidate)), services)
        .use(HiringGraphQLSchema.executeInContext(request, _))
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
        |  createJob(input: {
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
      assertEquals(payload.downField("skills").focus.flatMap(_.asArray).map(_.flatMap(_.asString).toList),
        Some(List("Cats Effect", "Scala")))
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
         |  submitApplication(input: { jobId: "${jobId.value}" }) {
         |    __typename
         |    ... on Application { id status }
         |  }
         |}""".stripMargin
    val closed =
      s"""mutation {
         |  submitApplication(input: { jobId: "${closedJobId.value}" }) {
         |    __typename
         |    ... on Application { id status }
         |  }
         |}""".stripMargin

    (execute(duplicate, Some(ActorContext(candidateId, UserRole.Candidate))),
      execute(closed, Some(ActorContext(candidateId, UserRole.Candidate)))).mapN { (duplicateJson, closedJson) =>
      assertEquals(errorCode(duplicateJson), Right("DUPLICATE_APPLICATION"))
      assertEquals(closedJson.hcursor.downField("data").downField("submitApplication").get[String]("__typename"), Right("DomainError"))
    }
  }

  test("application status mutation invalid transition stays in typed payloads") {
    val query =
      s"""mutation {
         |  hireApplication(input: { applicationId: "${applicationId.value}" }) {
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
         |  rejectApplication(input: { applicationId: "${applicationId.value}", feedback: "Not enough Scala" }) {
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
         |  rejectApplication(input: { applicationId: "${applicationId.value}", feedback: " " }) {
         |    __typename
         | __typename
         |  }
         |}""".stripMargin
    val decline =
      s"""mutation {
         |  declineApplication(input: { applicationId: "${applicationId.value}", reason: "" }) {
         |    __typename
         | __typename
         |  }
         |}""".stripMargin

    (execute(reject, Some(ActorContext(recruiterId, UserRole.Recruiter))),
      execute(decline, Some(ActorContext(recruiterId, UserRole.Recruiter)))).mapN { (rejectJson, declineJson) =>
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
      assertEquals(payload.downField("results").downArray.downField("job").get[String]("id"), Right(jobId.value.toString))
      assertEquals(payload.downField("results").downArray.get[String]("searchMode"), Right("HYBRID"))
      assertEquals(payload.downField("results").downArray.get[String]("model"), Right("voyage-4-lite"))
      assert(!json.hcursor.downField("errors").succeeded)
    }
  }

  test("search results survive best-effort search-session persistence failures") {
    val semanticQuery =
      """query { semanticJobSearch(query: "scala", first: 5) { results { job { id } } __typename } }"""
    val jobsQuery =
      """query { jobs(first: 5) { edges { node { id } } __typename } }"""
    for {
      semantic <- executeWithSemanticSearch(semanticQuery, Some(ActorContext(candidateId, UserRole.Candidate)), FailingSearchSessions)
      jobs <- executeWithUsers(jobsQuery, Some(ActorContext(candidateId, UserRole.Candidate)), List(candidate, recruiter), searchSessions = FailingSearchSessions)
    } yield {
      assertEquals(semantic.hcursor.downField("data").downField("semanticJobSearch").downField("results").downArray.downField("job").get[String]("id"), Right(jobId.value.toString))
      assert(!semantic.hcursor.downField("errors").succeeded)
      assertEquals(jobs.hcursor.downField("data").downField("jobs").downField("edges").downArray.downField("node").get[String]("id"), Right(jobId.value.toString))
      assert(!jobs.hcursor.downField("errors").succeeded)
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
      result <- TestGraphQLSupport.context(IO.pure(ProbeResult.Ready)).use(HiringGraphQLSchema.executeInContext(request, _))
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

    executeWithSemanticSearch(query, Some(ActorContext(recruiterId, UserRole.Recruiter))).map { json =>
      assertEquals(errorCode(json), Right("STALE_EMBEDDING"))
    }
  }

  test("signup canonical-name conflicts return a generic registration failure") {
    val query =
      """mutation {
        |  signUp(input: { name: "Candidate", role: CANDIDATE, password: "password-password", skills: ["Scala"] }) {
        |    __typename
        | __typename
        |  }
        |}""".stripMargin

    executeWithUsers(query, None, List(candidate, recruiter), NameTakenAccountService).map { json =>
      assertEquals(errorCode(json), Right("REGISTRATION_FAILED"))
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

    val submitVariables = Json.obj("input" -> Json.obj("jobId" -> Json.fromString(jobId.value.toString)))
    val recruiterVariables = Json.obj(
      "update" -> Json.obj(
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
        "applicationId" -> Json.fromString(applicationId.value.toString),
        "feedback" -> Json.fromString("Not enough Scala")
      )
    )
    val signupVariables = Json.obj("input" -> Json.obj(
      "name" -> Json.fromString("Candidate"),
      "role" -> Json.fromString("CANDIDATE"),
      "password" -> Json.fromString("password-password"),
      "skills" -> Json.arr(Json.fromString("Scala"))
    ))

    (executeWithUsers(submit, Some(ActorContext(candidateId, UserRole.Candidate)), List(candidate, recruiter),
      variables = submitVariables),
      executeWithUsers(updateAndReject, Some(ActorContext(recruiterId, UserRole.Recruiter)), List(candidate, recruiter),
        variables = recruiterVariables),
      executeWithUsers(signup, None, List(candidate, recruiter), NameTakenAccountService, signupVariables)).mapN {
      (submitJson, recruiterJson, signupJson) =>
        assertEquals(errorCode(submitJson), Right("DUPLICATE_APPLICATION"))

        val updatePayload = recruiterJson.hcursor.downField("data").downField("updateJob")
        assertEquals(updatePayload.get[String]("__typename"), Right("Job"))

        val rejectPayload = recruiterJson.hcursor.downField("data").downField("rejectApplication")
        assertEquals(rejectPayload.get[String]("__typename"), Right("Application"))

        assertEquals(errorCode(signupJson), Right("REGISTRATION_FAILED"))
    }
  }

  private def execute(query: String, actor: Option[ActorContext]): IO[Json] = {
    executeWithUsers(query, actor, List(candidate, recruiter))
  }

  private def parseRequest(query: String, variables: Json = Json.obj()): IO[GraphQLRequest] =
    IO.fromEither(Json.obj("query" -> Json.fromString(query), "variables" -> variables).as[GraphQLRequest]
      .leftMap(error => new IllegalArgumentException("Invalid GraphQL test request", error)))

  private def executeWithSemanticSearch(query: String, actor: Option[ActorContext]): IO[Json] = {
    executeWithSemanticSearch(query, actor, SearchSessionRepository.noop[IO])
  }

  private def executeWithSemanticSearch(
      query: String,
      actor: Option[ActorContext],
      searchSessions: SearchSessionRepository[IO]
  ): IO[Json] = {
    val meta = EmbeddingMeta("voyage-4-lite", "hash", now)
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
      searchService = SemanticSearchService(
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
        embeddingModel = "voyage-4-lite"
      )
      services = HiringGraphQLServices(HiringReadService(users, jobs, applications), JobService(users, jobs), ApplicationService(users, jobs, applications),
        TestGraphQLSupport.cursorKey,
        TestGraphQLSupport.accountService,
        Some(searchService),
        searchSessions = searchSessions)
      request <- parseRequest(query)
      result <- TestGraphQLSupport.context(IO.pure(ProbeResult.Ready), actor, services).use(HiringGraphQLSchema.executeInContext(request, _))
    } yield result.fold(failure => fail(failure.toString), identity)
  }

  private def executeWithUsers(
      query: String,
      actor: Option[ActorContext],
      users: List[User],
      accountService: AccountUseCases = TestGraphQLSupport.accountService,
      variables: Json = Json.obj(),
      hiringReady: IO[ProbeResult] = IO.pure(ProbeResult.Ready),
      searchSessions: SearchSessionRepository[IO] = SearchSessionRepository.noop[IO]
  ): IO[Json] = {
    for {
      usersRef <- Ref.of[IO, Map[UserId, User]](users.map(user => user.id -> user).toMap)
      jobsRef <- Ref.of[IO, Map[JobId, Job]](List(openJob, closedJob).map(job => job.id -> job).toMap)
      applicationsRef <- Ref.of[IO, Map[ApplicationId, Application]](Map(application.id -> application))
      eventsRef <- Ref.of[IO, Vector[ApplicationEvent]](Vector.empty)
      nextCreateError <- Ref.of[IO, Option[com.example.graphQL.cats.repository.protocol.RepositoryError]](None)
      users = InMemoryUsers(usersRef)
      jobs = InMemoryJobs(jobsRef)
      applications = InMemoryApplications(applicationsRef, eventsRef, nextCreateError)
      services = HiringGraphQLServices(HiringReadService(users, jobs, applications), JobService(users, jobs), ApplicationService(users, jobs, applications), TestGraphQLSupport.cursorKey, accountService = accountService, searchSessions = searchSessions)
      request <- parseRequest(query, variables)
      result <- TestGraphQLSupport.context(IO.pure(ProbeResult.Ready), actor, services, hiringReady).use(HiringGraphQLSchema.executeInContext(request, _))
    } yield result.fold(failure => fail(failure.toString), identity)
  }

  private def encodeCursor(json: Json): String =
    Base64.getUrlEncoder.withoutPadding().encodeToString(json.noSpaces.getBytes(StandardCharsets.UTF_8))

  private final class RecordingUsers(
      ref: Ref[IO, Map[UserId, User]],
      batches: Ref[IO, Vector[List[UserId]]]
  ) extends UserRepository[IO] {
    override def find(id: UserId): IO[Either[RepositoryError, Option[User]]] =
      ref.get.map(_.get(id)).map(Right(_))

    override def findMany(ids: List[UserId]): IO[Either[RepositoryError, List[User]]] =
      batches.update(_ :+ ids) *> ref.get.map(users => Right(ids.distinct.flatMap(users.get)))

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

  private object FailingSearchSessions extends SearchSessionRepository[IO] {
    override def save(session: SearchSession, event: com.example.graphQL.cats.shared.events.OperationalEventEnvelope): IO[Either[RepositoryError, Unit]] =
      IO.pure(Left(RepositoryError.Unavailable))
    override def find(id: UUID): IO[Either[RepositoryError, Option[SearchSession]]] =
      IO.pure(Right(None))
    override def recordInteraction(event: com.example.graphQL.cats.shared.events.OperationalEventEnvelope): IO[Either[RepositoryError, Boolean]] =
      IO.pure(Right(true))
  }

  private final class RecordingAccountService(updateCalls: Ref[IO, Int]) extends AccountUseCases {
    private val unsupported: UseCaseError = UseCaseError.Account(com.example.graphQL.cats.service.AccountError.ProfileUnsupportedForRole)

    override def signUp(input: SignUpInput, now: Instant, userId: UserId): IO[Either[UseCaseError, (User, AccountToken)]] =
      IO.pure(Left(unsupported))

    override def bootstrapAdmin(input: BootstrapAdminInput, now: Instant, userId: UserId): IO[Either[UseCaseError, (User, AccountToken)]] =
      IO.pure(Left(unsupported))

    override def login(input: LoginInput, now: Instant): IO[Either[UseCaseError, (User, AccountToken)]] =
      IO.pure(Left(unsupported))

    override def me(actor: ActorContext): IO[Either[UseCaseError, User]] =
      IO.pure(Left(unsupported))

    override def updateMyProfile(actor: ActorContext, input: AccountProfileInput, now: Instant): IO[Either[UseCaseError, User]] =
      updateCalls.update(_ + 1) *> IO.pure(
        UserProfile.validateFor(actor.role, Some(input.profile)).toEither
          .leftMap(UseCaseError.ValidationFailed.apply)
          .map(_ => recruiter)
      )

    override def deleteMyAccount(actor: ActorContext, now: Instant): IO[Either[UseCaseError, Unit]] =
      IO.pure(Left(unsupported))

    override def listUsers(actor: ActorContext, page: UserPageRequest): IO[Either[UseCaseError, List[User]]] =
      IO.pure(Left(unsupported))
  }

  private final class PublicAccountService(calls: Ref[IO, Int]) extends AccountUseCases {
    private val unavailable = Left(UseCaseError.Availability(com.example.graphQL.cats.service.AvailabilityError.ServiceNotReady))

    override def signUp(input: SignUpInput, now: Instant, userId: UserId): IO[Either[UseCaseError, (User, AccountToken)]] =
      calls.update(_ + 1).as(unavailable)

    override def bootstrapAdmin(input: BootstrapAdminInput, now: Instant, userId: UserId): IO[Either[UseCaseError, (User, AccountToken)]] =
      calls.update(_ + 1).as(unavailable)

    override def login(input: LoginInput, now: Instant): IO[Either[UseCaseError, (User, AccountToken)]] =
      calls.update(_ + 1).as(unavailable)

    override def me(actor: ActorContext): IO[Either[UseCaseError, User]] = IO.pure(unavailable)
    override def updateMyProfile(actor: ActorContext, input: AccountProfileInput, now: Instant): IO[Either[UseCaseError, User]] = IO.pure(unavailable)
    override def deleteMyAccount(actor: ActorContext, now: Instant): IO[Either[UseCaseError, Unit]] = IO.pure(unavailable)
    override def listUsers(actor: ActorContext, page: UserPageRequest): IO[Either[UseCaseError, List[User]]] = IO.pure(unavailable)
  }

  private object NameTakenAccountService extends AccountUseCases {
    private val unsupported: UseCaseError = UseCaseError.Account(com.example.graphQL.cats.service.AccountError.ProfileUnsupportedForRole)

    override def signUp(input: SignUpInput, now: Instant, userId: UserId): IO[Either[UseCaseError, (User, AccountToken)]] =
      IO.pure(Left(UseCaseError.Account(com.example.graphQL.cats.service.AccountError.NameTaken)))

    override def bootstrapAdmin(input: BootstrapAdminInput, now: Instant, userId: UserId): IO[Either[UseCaseError, (User, AccountToken)]] =
      IO.pure(Left(unsupported))

    override def login(input: LoginInput, now: Instant): IO[Either[UseCaseError, (User, AccountToken)]] =
      IO.pure(Left(unsupported))

    override def me(actor: ActorContext): IO[Either[UseCaseError, User]] =
      IO.pure(Left(unsupported))

    override def updateMyProfile(actor: ActorContext, input: AccountProfileInput, now: Instant): IO[Either[UseCaseError, User]] =
      IO.pure(Left(unsupported))

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
