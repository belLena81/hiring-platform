package com.example.graphQL.cats.api.graphql

import cats.effect.{IO, Ref}
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.{Application, Job, User, UserRole}
import com.example.graphQL.cats.domain.pagination.{JobPageRequest, JobProjection}
import com.example.graphQL.cats.service.{ActorContext, HiringReadService, ServiceFixtures, TestHiringServices}
import com.example.graphQL.cats.service.ServiceFixtures.{InMemoryApplications, InMemoryJobs, InMemoryUsers}
import io.circe.Json
import munit.CatsEffectSuite
import sangria.ast
import sangria.parser.QueryParser

/** GQ-02 (AC-10): the list roots read the posting text only when the selection names it. */
final class HiringGraphQLJobProjectionSpec extends CatsEffectSuite {
  import ServiceFixtures.{candidate, candidateId, openJob, recruiter, recruiterId}

  private val candidateActor = ActorContext(candidateId, UserRole.Candidate)
  private val recruiterActor = ActorContext(recruiterId, UserRole.Recruiter)

  private def rootField(query: String): (Vector[ast.Field], Map[String, ast.FragmentDefinition]) = {
    val document = QueryParser.parse(query).fold(error => fail(error.getMessage), identity)
    val operation = document.operations.values.head
    val roots = operation.selections.collect { case field: ast.Field => field }
    (roots, document.fragments)
  }

  private def projectionOf(query: String): JobProjection = {
    val (roots, fragments) = rootField(query)
    HiringGraphQLJobProjection.of(roots, fragments)
  }

  test("AC-10.1 a selection of identity and title alone is a summary") {
    assertEquals(
      projectionOf("query { jobs(first: 5) { edges { cursor node { id title location { country } } } } }"),
      JobProjection.Summary
    )
  }

  test("AC-10.2 selecting the description or the requirements anywhere under the root is full") {
    assertEquals(projectionOf("query { jobs(first: 5) { edges { node { id description } } } }"), JobProjection.Full)
    assertEquals(
      projectionOf("query { myJobs(first: 5) { edges { node { id requirements } } } }"),
      JobProjection.Full
    )
    assertEquals(
      projectionOf("query { jobs(first: 5) { edges { node { id mine: description } } } }"),
      JobProjection.Full
    )
  }

  test("AC-10.3 a fragment or inline fragment that selects the posting text is full, one that does not is a summary") {
    assertEquals(
      projectionOf(
        "query { jobs(first: 5) { edges { node { ...Posting } } } } fragment Posting on Job { id requirements }"
      ),
      JobProjection.Full
    )
    assertEquals(
      projectionOf("query { jobs(first: 5) { edges { node { ... on Job { id description } } } } }"),
      JobProjection.Full
    )
    assertEquals(
      projectionOf("query { jobs(first: 5) { edges { node { ...Card } } } } fragment Card on Job { id title }"),
      JobProjection.Summary
    )
  }

  test("AC-10.4 a nested selection that names the posting text reads more rather than less") {
    assertEquals(
      projectionOf(
        "query { myJobs(first: 5) { edges { node { id applications(first: 1) { edges { node { job { description } } } } } } } }"
      ),
      JobProjection.Full
    )
  }

  test("AC-10.5 an unknown fragment cannot be proven to leave the posting text out, so it is full") {
    val (roots, _) = rootField("query { jobs(first: 5) { edges { node { ...Missing } } } }")
    assertEquals(HiringGraphQLJobProjection.of(roots, Map.empty), JobProjection.Full)
  }

  private def requestsFor(query: String, actor: ActorContext): IO[Vector[JobPageRequest]] =
    for {
      usersRef <- Ref.of[IO, Map[UserId, User]](List(candidate, recruiter).map(user => user.id -> user).toMap)
      jobsRef <- Ref.of[IO, Map[JobId, Job]](Map(openJob.id -> openJob))
      applicationsRef <- Ref.of[IO, Map[ApplicationId, Application]](Map.empty)
      eventsRef <- Ref.of[IO, Vector[com.example.graphQL.cats.domain.model.ApplicationEvent]](Vector.empty)
      errors <- Ref.of[IO, Option[com.example.graphQL.cats.service.RepositoryError]](None)
      recorded <- Ref.of[IO, Vector[JobPageRequest]](Vector.empty)
      users = new InMemoryUsers(usersRef)
      jobs = new InMemoryJobs(jobsRef, pageRequests = Some(recorded))
      applications = new InMemoryApplications(
        applicationsRef,
        eventsRef,
        errors,
        jobLookup = id => jobsRef.get.map(_.get(id))
      )
      services = HiringGraphQLServices(
        HiringReadService(users, jobs, applications),
        TestHiringServices.job(users, jobs),
        TestHiringServices.applications(users, jobs, applications),
        TestGraphQLSupport.cursorKey,
        TestGraphQLSupport.accountService,
        TestGraphQLSupport.interactions,
        TestGraphQLSupport.searchSessions
      )
      result <- TestGraphQLSupport
        .context(Some(actor), services)
        .use(TestGraphQLSupport.parseAndExecute(GraphQLRequest(query, Json.obj(), None), _))
      _ = assert(result.exists(_.hcursor.downField("errors").failed), result.toString)
      requests <- recorded.get
    } yield requests

  test("AC-10.6 jobs { id title } asks the repository for the summary and keeps the status filter and page size") {
    requestsFor("query { jobs(first: 3) { edges { node { id title } } } }", candidateActor).map { requests =>
      assertEquals(requests.map(_.projection), Vector(JobProjection.Summary))
      assertEquals(requests.map(_.pageSize.value), Vector(3 + 1))
    }
  }

  test("AC-10.7 jobs { id description requirements } asks for the full job") {
    requestsFor("query { jobs(first: 3) { edges { node { id description requirements } } } }", candidateActor).map {
      requests => assertEquals(requests.map(_.projection), Vector(JobProjection.Full))
    }
  }

  test("AC-10.8 myJobs follows its selection the same way and keeps the status argument") {
    for {
      summary <- requestsFor("query { myJobs(first: 3, status: OPEN) { edges { node { id title } } } }", recruiterActor)
      full <- requestsFor("query { myJobs(first: 3) { edges { node { id description } } } }", recruiterActor)
    } yield {
      assertEquals(summary.map(_.projection), Vector(JobProjection.Summary))
      assertEquals(summary.flatMap(_.status.map(_.toString)), Vector("Open"))
      assertEquals(full.map(_.projection), Vector(JobProjection.Full))
    }
  }
}
