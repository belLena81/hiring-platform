package com.example.graphQL.cats.api.graphql

import cats.data.EitherT
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.pagination.ApplicationEventPageRequest
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.{
  ActorContext,
  HiringReadService,
  RepositoryError,
  ServiceFixtures,
  TestHiringServices
}
import com.example.graphQL.cats.service.ServiceFixtures.{InMemoryApplications, InMemoryJobs, InMemoryUsers}
import com.example.graphQL.cats.service.application.{InMemoryInterviewWorkflowRepository, InterviewSchedulingService}
import com.example.graphQL.cats.service.protocol.{AccountUseCases, HiringReadModel}
import com.example.graphQL.cats.service.read.{JobRelationKey, UserRelationKey}
import io.circe.{ACursor, Json}
import java.util.UUID
import munit.CatsEffectSuite
import sangria.parser.QueryParser
import sangria.validation.QueryValidator
import scala.concurrent.duration.*

/** AC-07: the graph is walkable (job to applications to candidate and events, viewer to applications, workflow to
  * application) through the same use cases and authorization as the root queries, with batched relations, bounded
  * nesting and the same refusals for direct-ID, nested and batch access.
  */
final class HiringGraphQLGraphAccessSpec extends CatsEffectSuite {
  import ServiceFixtures.{admin, adminId, candidate, candidateId, jobId, now, openJob, recruiter, recruiterId}

  private def uuid(n: Long) = new UUID(0L, n)
  private val benId = UserId(uuid(101))
  private val carlaId = UserId(uuid(102))
  private val otherRecruiterId = UserId(uuid(103))
  private val otherJobId = JobId(uuid(104))
  private val workflowId = InterviewWorkflowId(uuid(20))
  private val ana = ActorContext(candidateId, UserRole.Candidate)
  private val ben = ActorContext(benId, UserRole.Candidate)
  private val rita = ActorContext(recruiterId, UserRole.Recruiter)
  private val rob = ActorContext(otherRecruiterId, UserRole.Recruiter)
  private val root = ActorContext(adminId, UserRole.Admin)

  private val anaApplication = ApplicationId(uuid(1001))
  private val benApplication = ApplicationId(uuid(1002))
  private val carlaApplication = ApplicationId(uuid(1003))
  private val anaOtherJobApplication = ApplicationId(uuid(1004))

  private val benUser = candidate.copy(id = benId, name = "Ben")
  private val carlaUser = candidate.copy(id = carlaId, name = "Carla")
  private val robUser = recruiter.copy(id = otherRecruiterId, name = "Rob")
  private val otherJob = openJob.copy(id = otherJobId, recruiterId = otherRecruiterId, title = "Rob's job")

  private def applicationOf(id: ApplicationId, who: UserId, job: JobId, createdAt: java.time.Instant) =
    Application.create(id, who, job, createdAt)
  private val applications: List[Application] = List(
    applicationOf(anaApplication, candidateId, jobId, now.plusSeconds(10)),
    applicationOf(benApplication, benId, jobId, now.plusSeconds(20)),
    applicationOf(carlaApplication, carlaId, jobId, now.plusSeconds(30)),
    applicationOf(anaOtherJobApplication, candidateId, otherJobId, now.plusSeconds(40))
  )
  private def event(n: Long, application: ApplicationId, actor: UserId, status: ApplicationStatus) =
    ApplicationEvent(
      ApplicationEventId(uuid(2000 + n)),
      application,
      None,
      status,
      actor,
      now.plusSeconds(n),
      None,
      None,
      UserRole.Candidate
    )
  private val events: Vector[ApplicationEvent] = Vector(
    event(1, anaApplication, candidateId, ApplicationStatus.Created),
    event(2, anaApplication, recruiterId, ApplicationStatus.Accepted),
    event(3, benApplication, benId, ApplicationStatus.Created)
  )

  private def workflow(recruiter: UserId): InterviewWorkflow =
    InterviewWorkflow(
      workflowId,
      anaApplication,
      candidateId,
      recruiter,
      InterviewInterval(now.plusSeconds(172800), now.plusSeconds(176400)),
      now.plusSeconds(300),
      uuid(21),
      1L,
      InterviewWorkflowPhase.Completed,
      Set(InterviewParticipant.Candidate, InterviewParticipant.Recruiter),
      recruiter,
      initiatedByRole = com.example.graphQL.cats.domain.model.UserRole.Recruiter
    )

  private enum Call { case Users(keys: Int); case Jobs(keys: Int) }

  private final case class Env(services: HiringGraphQLServices, calls: Ref[IO, Vector[Call]])

  private def env(interviewRecruiter: UserId = recruiterId): IO[Env] =
    for {
      usersRef <- Ref.of[IO, Map[UserId, User]](
        List(candidate, benUser, carlaUser, recruiter, robUser, admin).map(user => user.id -> user).toMap
      )
      jobsRef <- Ref.of[IO, Map[JobId, Job]](List(openJob, otherJob).map(job => job.id -> job).toMap)
      applicationsRef <- Ref.of[IO, Map[ApplicationId, Application]](applications.map(a => a.id -> a).toMap)
      eventsRef <- Ref.of[IO, Vector[ApplicationEvent]](events)
      errors <- Ref.of[IO, Option[RepositoryError]](None)
      calls <- Ref.of[IO, Vector[Call]](Vector.empty)
      workflows <- InMemoryInterviewWorkflowRepository.create(workflow(interviewRecruiter), ApplicationStatus.Interview)
      users = new InMemoryUsers(usersRef, Some(ServiceFixtures.userRelations(usersRef, jobsRef, applicationsRef)))
      jobs = new InMemoryJobs(
        jobsRef,
        relationLookup = Some(ServiceFixtures.jobRelations(usersRef, jobsRef, applicationsRef))
      )
      applicationRepository = new InMemoryApplications(
        applicationsRef,
        eventsRef,
        errors,
        jobLookup = id => jobsRef.get.map(_.get(id))
      )
      interview = new InterviewSchedulingService[IO](
        users,
        jobs,
        applicationRepository,
        workflows,
        com.example.graphQL.cats.service.mutation.TestIdempotency.noop,
        5.minutes,
        IO.pure(now)
      )
      readModel = new CountingReadModel(HiringReadService(users, jobs, applicationRepository), calls)
      account = new AccountUseCases[IO] {
        private val base = TestGraphQLSupport.accountService
        def signUp(
            r: com.example.graphQL.cats.service.protocol.IdempotencyRequest,
            i: com.example.graphQL.cats.service.protocol.SignUpInput
        ) = base.signUp(r, i)
        def login(
            r: com.example.graphQL.cats.service.protocol.IdempotencyRequest,
            i: com.example.graphQL.cats.service.protocol.LoginInput
        ) = base.login(r, i)
        def me(actor: ActorContext) = EitherT.liftF(usersRef.get.map(_(actor.userId)))
        def updateMyProfile(
            r: com.example.graphQL.cats.service.protocol.IdempotencyRequest,
            actor: ActorContext,
            i: com.example.graphQL.cats.service.protocol.AccountProfileInput
        ) = base.updateMyProfile(r, actor, i)
        def deleteMyAccount(r: com.example.graphQL.cats.service.protocol.IdempotencyRequest, actor: ActorContext) =
          base.deleteMyAccount(r, actor)
        def accountDeletionStatus(actor: ActorContext, receiptId: String) = base.accountDeletionStatus(actor, receiptId)
        def listUsers(actor: ActorContext, page: UserPageRequest) = base.listUsers(actor, page)
      }
    } yield Env(
      HiringGraphQLServices(
        readModel,
        TestHiringServices.job(users, jobs),
        TestHiringServices.applications(users, jobs, applicationRepository),
        TestGraphQLSupport.cursorKey,
        account,
        TestGraphQLSupport.interactions,
        TestGraphQLSupport.searchSessions,
        interviewScheduling = Some(interview)
      ),
      calls
    )

  private final class CountingReadModel(delegate: HiringReadModel[IO], calls: Ref[IO, Vector[Call]])
      extends HiringReadModel[IO] {
    def viewer(actor: ActorContext) = delegate.viewer(actor)
    def canViewUserEmails(actor: ActorContext, userIds: List[UserId]) = delegate.canViewUserEmails(actor, userIds)
    def jobApplicationCounts(
        actor: ActorContext,
        jobIds: List[com.example.graphQL.cats.domain.model.Identifiers.JobId]
    ) =
      delegate.jobApplicationCounts(actor, jobIds)
    def application(id: ApplicationId) = delegate.application(id)
    def canViewApplication(actor: ActorContext, applicationId: ApplicationId) =
      delegate.canViewApplication(actor, applicationId)
    def relatedUsers(actor: ActorContext, keys: List[UserRelationKey]) =
      EitherT.liftF[IO, com.example.graphQL.cats.service.UseCaseError, Unit](
        calls.update(_ :+ Call.Users(keys.distinct.size))
      ) *> delegate.relatedUsers(actor, keys)
    def relatedJobs(actor: ActorContext, keys: List[JobRelationKey]) =
      EitherT.liftF[IO, com.example.graphQL.cats.service.UseCaseError, Unit](
        calls.update(_ :+ Call.Jobs(keys.distinct.size))
      ) *> delegate.relatedJobs(actor, keys)
    def applicationHistory(actor: ActorContext, applicationId: ApplicationId, page: ApplicationEventPageRequest) =
      delegate.applicationHistory(actor, applicationId, page)
  }

  private def execute(
      environment: Env,
      query: String,
      actor: ActorContext,
      variables: Json = Json.obj()
  ): IO[Either[HiringGraphQLSchema.Failure, Json]] =
    TestGraphQLSupport
      .context(Some(actor), environment.services)
      .use(TestGraphQLSupport.parseAndExecute(GraphQLRequest(query, variables, None), _))

  private def ok(result: Either[HiringGraphQLSchema.Failure, Json]): Json =
    result.fold(failure => fail(failure.toString), identity)

  private def errorCodes(json: Json): List[String] =
    json.hcursor
      .downField("errors")
      .values
      .toList
      .flatten
      .flatMap(_.hcursor.downField("extensions").get[String]("code").toOption)
  private def data(json: Json): ACursor = json.hcursor.downField("data")
  private def ids(cursor: ACursor): List[String] =
    cursor.downField("edges").values.toList.flatten.flatMap(_.hcursor.downField("node").get[String]("id").toOption)

  private def idOf(application: ApplicationId) = application.value.toString

  private val jobGraph =
    """query($jobId: JobID!, $first: Int!, $after: Cursor, $status: ApplicationStatus) {
      |  job(id: $jobId) {
      |    applications(first: $first, after: $after, status: $status) {
      |      edges { cursor node { id status candidate { id name } events(first: 5) { edges { node { newStatus } } } } }
      |      pageInfo { hasNextPage hasPreviousPage startCursor endCursor }
      |    }
      |  }
      |}""".stripMargin

  private def jobVars(first: Int, after: Option[String] = None) =
    Json.obj(
      "jobId" -> Json.fromString(jobId.value.toString),
      "first" -> Json.fromInt(first),
      "after" -> after.fold(Json.Null)(Json.fromString)
    )

  test("AC-07.1 a recruiter walks job to applications to candidate and events, equal to the root queries") {
    for {
      e <- env()
      nested <- execute(e, jobGraph, rita, jobVars(10)).map(ok)
      root <- execute(
        e,
        s"""query { jobApplications(jobId: "${jobId.value}", first: 10) { edges { node { id } } } }""",
        rita
      ).map(ok)
    } yield {
      assert(errorCodes(nested).isEmpty, clue(nested))
      val connection = data(nested).downField("job").downField("applications")
      assertEquals(ids(connection), ids(data(root).downField("jobApplications")))
      assertEquals(
        ids(connection).toSet,
        Set(idOf(anaApplication), idOf(benApplication), idOf(carlaApplication))
      )
      val first = connection.downField("edges").downArray.downField("node")
      assertEquals(first.downField("candidate").get[String]("name"), Right("Carla"))
      val anaNode = connection
        .downField("edges")
        .values
        .toList
        .flatten
        .map(_.hcursor.downField("node"))
        .find(
          _.get[String]("id") == Right(idOf(anaApplication))
        )
        .get
      assertEquals(
        anaNode
          .downField("events")
          .downField("edges")
          .values
          .toList
          .flatten
          .map(_.hcursor.downField("node").get[String]("newStatus")),
        List(Right("ACCEPTED"), Right("CREATED"))
      )
    }
  }

  test("AC-07.2 the candidates of a page of applications are one batched user read, not one per application") {
    for {
      e <- env()
      json <- execute(e, jobGraph, rita, jobVars(10)).map(ok)
      calls <- e.calls.get
    } yield {
      assert(errorCodes(json).isEmpty, clue(json))
      assertEquals(calls.collect { case Call.Users(keys) => keys }, Vector(3))
    }
  }

  test("AC-07.3 a recruiter who does not own the job is refused exactly as by jobApplications, with no data") {
    for {
      e <- env()
      nested <- execute(e, jobGraph, rob, jobVars(10)).map(ok)
      rootJson <- execute(
        e,
        s"""query { jobApplications(jobId: "${jobId.value}", first: 10) { edges { node { id } } } }""",
        rob
      ).map(ok)
    } yield {
      assertEquals(errorCodes(nested), errorCodes(rootJson))
      assertEquals(errorCodes(nested), List("FORBIDDEN"))
      assert(!nested.noSpaces.contains(idOf(anaApplication)), clue(nested))
      assert(!nested.noSpaces.contains("Carla"), clue(nested))
    }
  }

  test("AC-07.3 a candidate cannot list the applications of a job through the nested field") {
    for {
      e <- env()
      json <- execute(e, jobGraph, ana, jobVars(10)).map(ok)
    } yield {
      assertEquals(errorCodes(json), List("FORBIDDEN"))
      assert(!json.noSpaces.contains(idOf(benApplication)), clue(json))
    }
  }

  test("AC-07.3 Admin lists the applications of any job through the nested field") {
    for {
      e <- env()
      json <- execute(e, jobGraph, root, jobVars(10)).map(ok)
    } yield {
      assert(errorCodes(json).isEmpty, clue(json))
      assertEquals(ids(data(json).downField("job").downField("applications")).size, 3)
    }
  }

  test("AC-07.4 the nested events field shows a candidate the history of their own applications only") {
    val query =
      """query { me { applications(first: 10) { edges { node { id events(first: 10) { edges { node { id } } } } } } } }"""
    for {
      e <- env()
      asAna <- execute(e, query, ana).map(ok)
      asBen <- execute(e, query, ben).map(ok)
    } yield {
      def eventCounts(json: Json): Map[String, Int] =
        data(json)
          .downField("me")
          .downField("applications")
          .downField("edges")
          .values
          .toList
          .flatten
          .map { edge =>
            val node = edge.hcursor.downField("node")
            node
              .get[String]("id")
              .toOption
              .get -> node.downField("events").downField("edges").values.toList.flatten.size
          }
          .toMap
      assert(errorCodes(asAna).isEmpty && errorCodes(asBen).isEmpty, clue(asAna.noSpaces + asBen.noSpaces))
      assertEquals(eventCounts(asAna), Map(idOf(anaApplication) -> 2, idOf(anaOtherJobApplication) -> 0))
      assertEquals(eventCounts(asBen), Map(idOf(benApplication) -> 1))
    }
  }

  test("AC-07.4 the direct-ID history of someone else's application stays forbidden, as does the nested one") {
    for {
      e <- env()
      direct <- execute(
        e,
        s"""query { applicationHistory(applicationId: "${anaApplication.value}", first: 5) { edges { node { id } } } }""",
        ben
      ).map(ok)
      nestedAsOtherRecruiter <- execute(e, jobGraph, rob, jobVars(5)).map(ok)
    } yield {
      assertEquals(errorCodes(direct), List("FORBIDDEN"))
      assertEquals(errorCodes(nestedAsOtherRecruiter), List("FORBIDDEN"))
      assert(!direct.noSpaces.contains("ACCEPTED"), clue(direct))
    }
  }

  test("AC-07.5 me.applications equals myApplications for the calling candidate") {
    for {
      e <- env()
      nested <- execute(e, "query { me { applications(first: 10) { edges { node { id } } } } }", ana).map(ok)
      rootJson <- execute(e, "query { myApplications(first: 10) { edges { node { id } } } }", ana).map(ok)
    } yield {
      assertEquals(
        ids(data(nested).downField("me").downField("applications")),
        ids(data(rootJson).downField("myApplications"))
      )
      assertEquals(
        ids(data(nested).downField("me").downField("applications")).toSet,
        Set(idOf(anaApplication), idOf(anaOtherJobApplication))
      )
    }
  }

  test("AC-07.5 User.applications is null for any account other than the viewer, even where the user is visible") {
    val query =
      s"""query { job(id: "${jobId.value}") { applications(first: 10) { edges { node { candidate { id applications(first: 10) { edges { node { id } } } } } } } } }"""
    for {
      e <- env()
      asRecruiter <- execute(e, query, rita).map(ok)
      asAdmin <- execute(e, query, root).map(ok)
    } yield List(asRecruiter, asAdmin).foreach { json =>
      assert(errorCodes(json).isEmpty, clue(json))
      val candidates = data(json).downField("job").downField("applications").downField("edges").values.toList.flatten
      assertEquals(candidates.size, 3)
      candidates.foreach(edge =>
        assertEquals(
          edge.hcursor.downField("node").downField("candidate").downField("applications").focus,
          Some(Json.Null),
          clue(json)
        )
      )
    }
  }

  test("AC-07.5 a recruiter's own me.applications is refused like myApplications and leaves the siblings") {
    for {
      e <- env()
      json <- execute(e, "query { me { id applications(first: 5) { edges { node { id } } } } }", rita).map(ok)
    } yield {
      assertEquals(errorCodes(json), List("FORBIDDEN"))
      assertEquals(data(json).downField("me").get[String]("id"), Right(recruiterId.value.toString))
      assertEquals(data(json).downField("me").downField("applications").focus, Some(Json.Null))
    }
  }

  test("AC-07.6 InterviewWorkflow.application resolves for the candidate, the owning recruiter and Admin") {
    val query =
      s"""query { interviewWorkflow(workflowId: "${workflowId.value}") { id application { id status job { title } candidate { name } } } }"""
    for {
      e <- env()
      results <- List(ana, rita, root).traverse(actor => execute(e, query, actor).map(ok))
    } yield results.foreach { json =>
      assert(errorCodes(json).isEmpty, clue(json))
      val application = data(json).downField("interviewWorkflow").downField("application")
      assertEquals(application.get[String]("id"), Right(idOf(anaApplication)))
      assertEquals(application.downField("job").get[String]("title"), Right(openJob.title))
    }
  }

  test("AC-07.6 a workflow nobody else may see stays hidden, and an application the viewer cannot see is null") {
    val query =
      s"""query { interviewWorkflow(workflowId: "${workflowId.value}") { id application { id } } }"""
    for {
      hiddenFrom <- env()
      stranger <- execute(hiddenFrom, query, ben).map(ok)
      otherRecruiter <- execute(hiddenFrom, query, rob).map(ok)
      // The workflow names Rob as its recruiter, but he does not own the job of the application.
      misassigned <- env(interviewRecruiter = otherRecruiterId)
      robSees <- execute(misassigned, query, rob).map(ok)
    } yield {
      assertEquals(data(stranger).downField("interviewWorkflow").focus, Some(Json.Null))
      assertEquals(data(otherRecruiter).downField("interviewWorkflow").focus, Some(Json.Null))
      assert(errorCodes(stranger).isEmpty && errorCodes(otherRecruiter).isEmpty)
      assertEquals(data(robSees).downField("interviewWorkflow").get[String]("id"), Right(workflowId.value.toString))
      assertEquals(data(robSees).downField("interviewWorkflow").downField("application").focus, Some(Json.Null))
      assert(errorCodes(robSees).isEmpty, clue(robSees))
    }
  }

  private val interviewOfApplications =
    "query { myApplications(first: 10) { edges { node { id interview { id progress applicationId } } } } }"
  private def interviewOfJobApplications =
    s"""query { jobApplications(jobId: "${jobId.value}", first: 10) { edges { node { id interview { id } } } } }"""

  private def interviewIds(cursor: ACursor): Map[String, Option[String]] =
    cursor
      .downField("edges")
      .values
      .toList
      .flatten
      .map(_.hcursor.downField("node"))
      .map(node => node.get[String]("id").toOption.get -> node.downField("interview").get[String]("id").toOption)
      .toMap

  test("AC-07.12 Application.interview shows the workflow to its candidate, the owning recruiter and Admin") {
    for {
      e <- env()
      asAna <- execute(e, interviewOfApplications, ana).map(ok)
      asRita <- execute(e, interviewOfJobApplications, rita).map(ok)
      asRoot <- execute(e, interviewOfJobApplications, root).map(ok)
    } yield {
      List(asAna, asRita, asRoot).foreach(json => assert(errorCodes(json).isEmpty, clue(json)))
      val expected = Some(workflowId.value.toString)
      assertEquals(interviewIds(data(asAna).downField("myApplications"))(idOf(anaApplication)), expected)
      assertEquals(interviewIds(data(asRita).downField("jobApplications"))(idOf(anaApplication)), expected)
      assertEquals(interviewIds(data(asRoot).downField("jobApplications"))(idOf(anaApplication)), expected)
    }
  }

  test("AC-07.12 an application without an interview, and one of another participant, resolve interview to null") {
    for {
      e <- env()
      asBen <- execute(e, interviewOfApplications, ben).map(ok)
      asRita <- execute(e, interviewOfJobApplications, rita).map(ok)
    } yield {
      assertEquals(interviewIds(data(asBen).downField("myApplications")), Map(idOf(benApplication) -> None))
      val byApplication = interviewIds(data(asRita).downField("jobApplications"))
      assertEquals(byApplication(idOf(benApplication)), None)
      assertEquals(byApplication(idOf(carlaApplication)), None)
    }
  }

  test("AC-07.12 the participant predicate is enforced by the service, not only by the parent field") {
    for {
      e <- env()
      service = e.services.interviewScheduling.get
      asAna <- service.inspectForApplication(ana, anaApplication).value
      asRita <- service.inspectForApplication(rita, anaApplication).value
      asRoot <- service.inspectForApplication(root, anaApplication).value
      // Another candidate, a recruiter who is not on the workflow, and a stranger asking for an unknown application.
      asBen <- service.inspectForApplication(ben, anaApplication).value
      asRob <- service.inspectForApplication(rob, anaApplication).value
      unknown <- service.inspectForApplication(ana, ApplicationId(uuid(9999))).value
      misassigned <- env(interviewRecruiter = otherRecruiterId)
      asOwner <- misassigned.services.interviewScheduling.get.inspectForApplication(rita, anaApplication).value
    } yield {
      List(asAna, asRita, asRoot).foreach(found => assertEquals(found.map(_.map(_.id)), Right(Some(workflowId))))
      List(asBen, asRob, unknown, asOwner).foreach(found => assertEquals(found, Right(None)))
    }
  }

  test("AC-07.12 an unauthenticated caller reaches no application and so no interview") {
    for {
      e <- env()
      json <- TestGraphQLSupport
        .context(None, e.services)
        .use(TestGraphQLSupport.parseAndExecute(GraphQLRequest(interviewOfApplications, Json.obj(), None), _))
        .map(ok)
    } yield {
      assertEquals(errorCodes(json), List("UNAUTHORIZED"), clue(json))
      assert(!json.noSpaces.contains(workflowId.value.toString), clue(json))
    }
  }

  test("AC-07.7 nested connections page with the shared cursor and report where the page sits") {
    for {
      e <- env()
      firstPage <- execute(e, jobGraph, rita, jobVars(2)).map(ok)
      page = data(firstPage).downField("job").downField("applications")
      info = page.downField("pageInfo")
      endCursor = info.get[String]("endCursor").toOption.get
      secondPage <- execute(e, jobGraph, rita, jobVars(2, Some(endCursor))).map(ok)
      secondInfo = data(secondPage).downField("job").downField("applications").downField("pageInfo")
    } yield {
      assertEquals(info.get[Boolean]("hasNextPage"), Right(true))
      assertEquals(info.get[Boolean]("hasPreviousPage"), Right(false))
      assertEquals(info.get[String]("startCursor"), page.downField("edges").downArray.get[String]("cursor"))
      assertEquals(ids(data(secondPage).downField("job").downField("applications")).size, 1)
      assertEquals(secondInfo.get[Boolean]("hasNextPage"), Right(false))
      assertEquals(secondInfo.get[Boolean]("hasPreviousPage"), Right(true))
      assert(
        ids(page).intersect(ids(data(secondPage).downField("job").downField("applications"))).isEmpty,
        clue(secondPage)
      )
    }
  }

  test("AC-07.7 a cursor of another list or a tampered one is refused for the nested field") {
    for {
      e <- env()
      jobsPage <- execute(e, "query { myJobs(first: 1) { pageInfo { endCursor } } }", rita).map(ok)
      jobCursor = data(jobsPage).downField("myJobs").downField("pageInfo").get[String]("endCursor").toOption
      foreign <- execute(e, jobGraph, rita, jobVars(2, jobCursor)).map(ok)
      tampered <- execute(e, jobGraph, rita, jobVars(2, jobCursor.map(_.reverse))).map(ok)
    } yield {
      assert(jobCursor.isDefined, clue(jobsPage))
      assertEquals(errorCodes(foreign).size, 1, clue(foreign))
      assertEquals(errorCodes(tampered).size, 1, clue(tampered))
      assertNotEquals(errorCodes(foreign), List("FORBIDDEN"))
    }
  }

  test("AC-07.8 nested connections are charged page size times their children, so a deep wide query is rejected") {
    val wide =
      """query { myJobs(first: 100) { edges { node { applications(first: 100) { edges { node { id } } } } } } }"""
    val modest =
      """query { myJobs(first: 5) { edges { node { applications(first: 5) { edges { node { id events(first: 5) { edges { node { id } } } } } } } } } }"""
    val badPage =
      """query { myJobs(first: 5) { edges { node { applications(first: 101) { edges { node { id } } } } } } }"""
    for {
      e <- env()
      rejected <- execute(e, wide, rita)
      admitted <- execute(e, modest, rita)
      pageRejected <- execute(e, badPage, rita)
    } yield {
      assertEquals(rejected, Left(HiringGraphQLSchema.Failure.InvalidQuery))
      assert(admitted.isRight, clue(admitted))
      assert(errorCodes(ok(admitted)).isEmpty, clue(admitted))
      assert(pageRejected.exists(_.noSpaces.contains("VALIDATION_FAILED")), clue(pageRejected))
    }
  }

  test("AC-07.7 a cursor is bound to its list: not replayable on another job, status, application or list") {
    def page(e: Env, query: String, actor: ActorContext) = execute(e, query, actor).map(ok)
    def cursorOf(json: Json, path: ACursor => ACursor): String =
      path(data(json)).get[String]("endCursor").toOption.get
    def refusal(json: Json) = errorCodes(json)
    for {
      e <- env()
      jobPage <- page(
        e,
        s"""query { job(id: "${jobId.value}") { applications(first: 1) { pageInfo { endCursor } } } }""",
        root
      )
      jobCursor = cursorOf(jobPage, _.downField("job").downField("applications").downField("pageInfo"))
      sameJob <- page(
        e,
        s"""query { job(id: "${jobId.value}") { applications(first: 5, after: "$jobCursor") { edges { node { id } } } } }""",
        root
      )
      otherJob <- page(
        e,
        s"""query { job(id: "$otherJobId") { applications(first: 5, after: "$jobCursor") { edges { node { id } } } } }""",
        root
      )
      otherStatus <- page(
        e,
        s"""query { job(id: "${jobId.value}") { applications(first: 5, status: ACCEPTED, after: "$jobCursor") { edges { node { id } } } } }""",
        root
      )
      asRoot <- page(
        e,
        s"""query { jobApplications(jobId: "${jobId.value}", first: 5, after: "$jobCursor") { edges { node { id } } } }""",
        root
      )
      eventPage <- page(
        e,
        s"""query { applicationHistory(applicationId: "${anaApplication.value}", first: 1) { pageInfo { endCursor } } }""",
        root
      )
      eventCursor = cursorOf(eventPage, _.downField("applicationHistory").downField("pageInfo"))
      otherApplication <- page(
        e,
        s"""query { applicationHistory(applicationId: "${benApplication.value}", first: 5, after: "$eventCursor") { edges { node { id } } } }""",
        root
      )
      asNested <- page(
        e,
        s"""query { me { applications(first: 5, after: "$jobCursor") { edges { node { id } } } } }""",
        ana
      )
    } yield {
      assert(refusal(sameJob).isEmpty, clue(sameJob))
      assert(refusal(asRoot).isEmpty, clue(asRoot)) // the root query is the same list
      List(otherJob, otherStatus, otherApplication, asNested).foreach(json =>
        assertEquals(refusal(json).size, 1, clue(json))
      )
      assert(!otherJob.noSpaces.contains(idOf(anaOtherJobApplication)), clue(otherJob))
    }
  }

  test("AC-07.11 every nested path refuses an unauthenticated caller and returns no data") {
    val queries = List(
      s"""query { job(id: "${jobId.value}") { applications(first: 5) { edges { node { id events(first: 5) { edges { node { id } } } } } } } }""",
      "query { me { applications(first: 5) { edges { node { id } } } } }",
      s"""query { interviewWorkflow(workflowId: "${workflowId.value}") { application { id } } }"""
    )
    for {
      e <- env()
      results <- queries.traverse(query =>
        TestGraphQLSupport
          .context(None, e.services)
          .use(TestGraphQLSupport.parseAndExecute(GraphQLRequest(query, Json.obj(), None), _))
          .map(ok)
      )
    } yield results.foreach { json =>
      assertEquals(errorCodes(json), List("UNAUTHORIZED"), clue(json))
      assert(!json.noSpaces.contains(idOf(anaApplication)), clue(json))
    }
  }

  test("AC-07.8 a nested list is priced per parent: about 99 parents fit with the cheapest selection") {
    def nested(parents: Int) =
      s"query { myJobs(first: $parents) { edges { node { applications(first: 1) { edges { node { id } } } } } } }"
    for {
      e <- env()
      fits <- execute(e, nested(99), rita)
      over <- execute(e, nested(100), rita)
    } yield {
      assert(fits.exists(json => errorCodes(json).isEmpty), clue(fits))
      assertEquals(over, Left(HiringGraphQLSchema.Failure.InvalidQuery))
    }
  }

  test("AC-07.12 Application.interview is priced like the other per-parent reads") {
    def nested(selection: String) =
      s"query { myJobs(first: 99) { edges { node { applications(first: 1) { edges { node { $selection } } } } } } }"
    for {
      e <- env()
      plain <- execute(e, nested("id"), rita)
      withInterview <- execute(e, nested("id interview { id }"), rita)
    } yield {
      assert(plain.exists(json => errorCodes(json).isEmpty), clue(plain))
      assertEquals(withInterview, Left(HiringGraphQLSchema.Failure.InvalidQuery))
    }
  }

  test("AC-07.8 aliases and fragments of the same nested list are each charged") {
    val aliased =
      """query { myJobs(first: 10) { edges { node {
        |  a: applications(first: 30) { edges { node { id } } }
        |  b: applications(first: 30) { edges { node { id } } } } } } }""".stripMargin
    val fragmented =
      """query { myJobs(first: 10) { edges { node { ...Page ...Page2 } } } }
        |fragment Page on Job { applications(first: 30) { edges { node { id } } } }
        |fragment Page2 on Job { other: applications(first: 30) { edges { node { id } } } }""".stripMargin
    val single =
      """query { myJobs(first: 10) { edges { node { applications(first: 30) { edges { node { id } } } } } } }"""
    for {
      e <- env()
      byAlias <- execute(e, aliased, rita)
      byFragment <- execute(e, fragmented, rita)
      one <- execute(e, single, rita)
    } yield {
      assertEquals(byAlias, Left(HiringGraphQLSchema.Failure.InvalidQuery))
      assertEquals(byFragment, Left(HiringGraphQLSchema.Failure.InvalidQuery))
      assert(one.exists(json => errorCodes(json).isEmpty), clue(one))
    }
  }

  test("AC-07.8 a zero or negative first on a nested list cannot lower the cost of its siblings") {
    List("0", "-5").traverse_ { first =>
      for {
        e <- env()
        json <- execute(
          e,
          s"query { myJobs(first: 5) { edges { node { applications(first: $first) { edges { node { id } } } } } } }",
          rita
        )
      } yield assert(json.exists(_.noSpaces.contains("VALIDATION_FAILED")), clue(json))
    }
  }

  test("AC-07.9 one error fragment reads the refusals of different mutations") {
    val operation =
      """mutation($publish: JobActionInput!, $create: CreateJobInput!) {
        |  publishJob(input: $publish) { __typename ... on Job { id } ...Refusal }
        |  createJob(input: $create) { __typename ... on Job { id } ...Refusal }
        |}
        |fragment Refusal on UserError { code message }""".stripMargin
    val variables = Json.obj(
      "publish" -> Json.obj(
        "jobId" -> Json.fromString(uuid(999).toString),
        "idempotencyKey" -> Json.fromString(uuid(1).toString)
      ),
      "create" -> Json.obj(
        "idempotencyKey" -> Json.fromString(uuid(2).toString),
        "title" -> Json.fromString(""),
        "description" -> Json.fromString("D"),
        "requirements" -> Json.arr(),
        "skills" -> Json.arr(),
        "country" -> Json.fromString("UA"),
        "remote" -> Json.True
      )
    )
    for {
      e <- env()
      json <- execute(e, operation, rita, variables).map(ok)
    } yield {
      assert(errorCodes(json).isEmpty, clue(json))
      assertEquals(data(json).downField("publishJob").get[String]("__typename"), Right("DomainError"))
      assertEquals(data(json).downField("createJob").get[String]("__typename"), Right("ValidationError"))
      List("publishJob", "createJob").foreach { field =>
        val result = data(json).downField(field)
        assert(result.get[String]("code").isRight && result.get[String]("message").isRight, clue(json))
      }
    }
  }

  test("AC-07.10 the nested graph fields, the Cursor scalar and ScoredHit are in the active schema") {
    def fieldNames(typeName: String): Set[String] = HiringGraphQLSchema.schema.outputTypes(typeName) match {
      case objectType: sangria.schema.ObjectType[?, ?] => objectType.fields.map(_.name).toSet
      case other                                       => fail(s"unexpected $other")
    }
    assert(fieldNames("Job").contains("applications"))
    assert(fieldNames("Application").contains("events"))
    assert(fieldNames("User").contains("applications"))
    assert(fieldNames("InterviewWorkflow").contains("application"))
    assertEquals(fieldNames("PageInfo"), Set("hasNextPage", "hasPreviousPage", "startCursor", "endCursor"))
    assert(HiringGraphQLSchema.sdl.contains("\nscalar Cursor\n"))
    assert(HiringGraphQLSchema.sdl.contains("type RankedJob implements ScoredHit {"))
    assert(HiringGraphQLSchema.sdl.contains("type RankedCandidate implements ScoredHit {"))
    List("RankedJob", "RankedCandidate").foreach(name =>
      assert(!fieldNames(name).contains("searchId") && !fieldNames(name).contains("model"), name)
    )
    assert(!HiringGraphQLSchema.sdl.contains("after: String"))
  }

  test("AC-07.10 every operation fixture validates against the active schema") {
    val directory = java.nio.file.Path.of("src/test/resources/graphql")
    val fixtures = com.example.graphQL.cats.api.http.PersistedQueryManifestCapture.operationFixtures(directory)
    assert(fixtures.nonEmpty)
    fixtures.foreach { path =>
      val document = QueryParser
        .parse(java.nio.file.Files.readString(path))
        .fold(error => fail(s"$path: ${error.getMessage}"), identity)
      assertEquals(
        QueryValidator.default
          .validateQuery(HiringGraphQLSchema.schema, document, Map.empty, None)
          .map(_.errorMessage)
          .toList,
        Nil,
        clue(path.getFileName.toString)
      )
    }
  }
}
