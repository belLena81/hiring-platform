package com.example.graphQL.cats.api.graphql

import cats.data.EitherT
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.pagination.ApplicationEventPageRequest
import com.example.graphQL.cats.service.{
  ActorContext,
  HiringReadService,
  RepositoryError,
  ServiceFixtures,
  UseCaseError
}
import com.example.graphQL.cats.service.ServiceFixtures.{InMemoryApplications, InMemoryJobs, InMemoryUsers}
import com.example.graphQL.cats.service.protocol.HiringReadModel
import com.example.graphQL.cats.service.read.{JobRelationKey, UserRelationKey}
import io.circe.Json
import java.util.UUID
import munit.CatsEffectSuite

/** AC-14: the related-entity fetchers batch per request, split batches over the maximum size, and keep authorization,
  * email visibility and failures correct in every batch.
  */
final class HiringGraphQLRelationBatchingSpec extends CatsEffectSuite {
  import ServiceFixtures.{admin, adminId, candidate, now, openJob, recruiter, recruiterId}

  private enum Call {
    case Users(actor: ActorContext, keys: Int); case Jobs(actor: ActorContext, keys: Int);
    case Emails(actor: ActorContext, ids: Int)
  }

  /** Records every repository-facing relation call; `failUsersFrom` makes the n-th and later user batches fail. */
  private final class CountingReadModel(
      delegate: HiringReadModel[IO],
      calls: Ref[IO, Vector[Call]],
      failUsersFrom: Option[Int]
  ) extends HiringReadModel[IO] {
    def viewer(actor: ActorContext) = delegate.viewer(actor)
    def canViewUserEmails(actor: ActorContext, userIds: List[UserId]) =
      EitherT.liftF[IO, UseCaseError, Unit](calls.update(_ :+ Call.Emails(actor, userIds.distinct.size))) *>
        delegate.canViewUserEmails(actor, userIds)
    def application(id: ApplicationId) = delegate.application(id)
    def canViewApplication(actor: ActorContext, applicationId: ApplicationId) =
      delegate.canViewApplication(actor, applicationId)
    def relatedUsers(actor: ActorContext, keys: List[UserRelationKey]) =
      EitherT
        .liftF[IO, UseCaseError, Int](calls.modify { all =>
          val position = all.count(_.isInstanceOf[Call.Users]) + 1
          (all :+ Call.Users(actor, keys.distinct.size), position)
        })
        .flatMap(position =>
          if (failUsersFrom.exists(position >= _))
            EitherT.leftT[IO, List[com.example.graphQL.cats.service.read.RelatedUser]](
              UseCaseError.Repository(RepositoryError.Unavailable)
            )
          else delegate.relatedUsers(actor, keys)
        )
    def relatedJobs(actor: ActorContext, keys: List[JobRelationKey]) =
      EitherT.liftF[IO, UseCaseError, Unit](calls.update(_ :+ Call.Jobs(actor, keys.distinct.size))) *>
        delegate.relatedJobs(actor, keys)
    def applicationHistory(actor: ActorContext, applicationId: ApplicationId, page: ApplicationEventPageRequest) =
      delegate.applicationHistory(actor, applicationId, page)
  }

  private def uuid(n: Long) = new UUID(0L, n)
  private def jobWithId(n: Long): Job = openJob.copy(id = JobId(uuid(1000 + n)))
  private def candidateWithId(n: Long): User =
    candidate.copy(id = UserId(uuid(2000 + n)), name = s"Candidate $n")

  private def run(
      query: String,
      actor: ActorContext,
      candidates: List[User],
      jobs: List[Job],
      applications: List[Application],
      failUsersFrom: Option[Int] = None
  ): IO[(Json, Vector[Call])] =
    for {
      usersRef <- Ref.of[IO, Map[UserId, User]]((recruiter :: admin :: candidates).map(user => user.id -> user).toMap)
      jobsRef <- Ref.of[IO, Map[JobId, Job]](jobs.map(job => job.id -> job).toMap)
      applicationsRef <- Ref.of[IO, Map[ApplicationId, Application]](applications.map(a => a.id -> a).toMap)
      eventsRef <- Ref.of[IO, Vector[ApplicationEvent]](Vector.empty)
      errorRef <- Ref.of[IO, Option[RepositoryError]](None)
      calls <- Ref.of[IO, Vector[Call]](Vector.empty)
      users = new InMemoryUsers(
        usersRef,
        Some(ServiceFixtures.userRelations(usersRef, jobsRef, applicationsRef))
      )
      jobRepository = new InMemoryJobs(
        jobsRef,
        relationLookup = Some(ServiceFixtures.jobRelations(usersRef, jobsRef, applicationsRef))
      )
      applicationRepository = new InMemoryApplications(
        applicationsRef,
        eventsRef,
        errorRef,
        jobLookup = id => jobsRef.get.map(_.get(id))
      )
      services = HiringGraphQLServices(
        new CountingReadModel(HiringReadService(users, jobRepository, applicationRepository), calls, failUsersFrom),
        com.example.graphQL.cats.service.TestHiringServices.job(users, jobRepository),
        com.example.graphQL.cats.service.TestHiringServices.applications(users, jobRepository, applicationRepository),
        TestGraphQLSupport.cursorKey,
        TestGraphQLSupport.accountService,
        TestGraphQLSupport.interactions,
        TestGraphQLSupport.searchSessions
      )
      request = GraphQLRequest(query, Json.obj(), None)
      json <- TestGraphQLSupport
        .context(Some(actor), services)
        .use(TestGraphQLSupport.parseAndExecute(request, _))
        .map(_.fold(failure => fail(failure.toString), identity))
      recorded <- calls.get
    } yield (json, recorded)

  private def applicationsFor(candidates: List[User], job: Job, offset: Long): List[Application] =
    candidates.zipWithIndex.map { case (user, index) =>
      Application.create(ApplicationId(uuid(offset + index)), user.id, job.id, now)
    }

  test("AC-14.1 nested job of three applications is one jobsFetcher call") {
    val jobs = List(1L, 2L, 3L).map(jobWithId)
    val applications = jobs.zipWithIndex.map { case (job, index) =>
      Application.create(ApplicationId(uuid(3000L + index)), candidate.id, job.id, now)
    }
    run(
      "query { myApplications(first: 10) { edges { node { id job { id } } } } }",
      ActorContext(candidate.id, UserRole.Candidate),
      List(candidate),
      jobs,
      applications
    ).map { case (json, calls) =>
      assert(json.hcursor.downField("errors").focus.isEmpty, clue(json))
      assertEquals(calls.collect { case Call.Jobs(_, keys) => keys }, Vector(3))
      val ids = json.hcursor
        .downField("data")
        .downField("myApplications")
        .downField("edges")
        .values
        .get
        .toList
        .map(_.hcursor.downField("node").downField("job").get[String]("id"))
      assertEquals(ids.toSet, jobs.map(job => Right(job.id.value.toString)).toSet)
    }
  }

  private val selection = "edges { node { candidate { id email } } }"
  private val twoPages = s"""query {
       |  a: jobApplications(jobId: "${jobWithId(1).id.value}", first: 60) { $selection }
       |  b: jobApplications(jobId: "${jobWithId(2).id.value}", first: 60) { $selection }
       |}""".stripMargin

  private def fixture: (List[User], List[Job], List[Application]) = {
    val candidates = (1L to 120L).toList.map(candidateWithId)
    val jobs = List(jobWithId(1), jobWithId(2))
    val applications = applicationsFor(candidates.take(60), jobs.head, 5000L) ++
      applicationsFor(candidates.drop(60), jobs(1), 6000L)
    (candidates, jobs, applications)
  }

  private def emails(json: Json): List[Json] =
    List("a", "b").flatMap(alias =>
      json.hcursor
        .downField("data")
        .downField(alias)
        .downField("edges")
        .values
        .get
        .toList
        .map(_.hcursor.downField("node").downField("candidate").downField("email").focus.get)
    )

  test(
    "AC-14.3 a batch over the maximum splits, and the recruiter's authorization and hidden emails hold in every batch"
  ) {
    val (candidates, jobs, applications) = fixture
    val actor = ActorContext(recruiterId, UserRole.Recruiter)
    run(twoPages, actor, candidates, jobs, applications).map { case (json, calls) =>
      assert(json.hcursor.downField("errors").focus.isEmpty, clue(json))
      val users = calls.collect { case Call.Users(who, keys) => (who, keys) }
      val emailCalls = calls.collect { case Call.Emails(who, ids) => (who, ids) }
      assertEquals(users.map(_._2).sum, 120)
      assert(users.size >= 2 && users.forall(_._2 <= HiringGraphQLDsl.FetcherMaxBatchSize), clue(users))
      assertEquals(emailCalls.map(_._2).sum, 120)
      assert(emailCalls.size >= 2 && emailCalls.forall(_._2 <= HiringGraphQLDsl.FetcherMaxBatchSize), clue(emailCalls))
      assert(
        (users ++ emailCalls).forall(call => call._1.userId == actor.userId && call._1.role == actor.role),
        "every batch carries the authenticated actor"
      )
      assertEquals(emails(json).size, 120)
      assert(emails(json).forall(_ == Json.Null), "a recruiter never sees candidate emails, in any batch")
    }
  }

  test("AC-14.3 an Admin sees every email across all batches of the same request") {
    val (candidates, jobs, applications) = fixture
    run(twoPages, ActorContext(adminId, UserRole.Admin), candidates, jobs, applications).map { case (json, _) =>
      assert(json.hcursor.downField("errors").focus.isEmpty, clue(json))
      assertEquals(emails(json).count(_ != Json.Null), 120)
    }
  }

  test("AC-14.4 a failure in the second batch surfaces as a GraphQL error while the first batch still resolves") {
    val (candidates, jobs, applications) = fixture
    val actor = ActorContext(recruiterId, UserRole.Recruiter)
    run(twoPages, actor, candidates, jobs, applications, failUsersFrom = Some(2)).map { case (json, calls) =>
      assertEquals(calls.count(_.isInstanceOf[Call.Users]), 2)
      val codes = json.hcursor
        .downField("errors")
        .values
        .toList
        .flatten
        .map(_.hcursor.downField("extensions").get[String]("code"))
      assert(codes.nonEmpty && codes.forall(_ == Right("UNAVAILABLE")), clue(json))
      assert(json.hcursor.downField("data").focus.exists(_ != Json.Null), clue(json))
    }
  }
}
