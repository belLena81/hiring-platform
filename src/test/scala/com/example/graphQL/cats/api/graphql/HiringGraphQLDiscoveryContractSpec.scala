package com.example.graphQL.cats.api.graphql

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.{AccountStatus, GeoPoint, Location, Application, ApplicationEvent}
import com.example.graphQL.cats.domain.model.Identifiers.ApplicationId
import com.example.graphQL.cats.service.{ActorContext, ProbeResult, ServiceFixtures, HiringReadService, RepositoryError}
import com.example.graphQL.cats.service.search.{JobDiscoveryTestSupport, JobSearchFilter, NearbyJobsQuery}
import io.circe.Json
import munit.CatsEffectSuite
import sangria.parser.QueryParser
import sangria.validation.QueryValidator

final class HiringGraphQLDiscoveryContractSpec extends CatsEffectSuite {
  private val center = GeoPoint(35d, 33d)
  private val candidate = ServiceFixtures.candidate
  private val actor = ActorContext(candidate.id, candidate.role)
  private val pointJob = ServiceFixtures.openJob.copy(location = Location("Cyprus", "Nicosia", false, Some(center)))

  private def execute(
      query: String,
      variables: Json,
      fixture: JobDiscoveryTestSupport.Fixture,
      authenticated: Option[ActorContext] = Some(actor)
  ): IO[Json] =
    for {
      applications <- Ref.of[IO, Map[ApplicationId, Application]](Map.empty)
      events <- Ref.of[IO, Vector[ApplicationEvent]](Vector.empty)
      createError <- Ref.of[IO, Option[RepositoryError]](None)
      readModel = new HiringReadService(
        fixture.users,
        fixture.jobs,
        new ServiceFixtures.InMemoryApplications(applications, events, createError)
      )
      result <- TestGraphQLSupport
        .context(
          IO.pure(ProbeResult.Ready),
          authenticated,
          TestGraphQLSupport.emptyServices.copy(jobService = fixture.service, readModel = readModel)
        )
        .use { context =>
          TestGraphQLSupport
            .parseAndExecute(GraphQLRequest(query, variables, None), context)
            .map(_.fold(failure => fail(failure.toString), identity))
        }
    } yield result

  private def errorCode(result: Json): Either[io.circe.DecodingFailure, String] =
    result.hcursor.downField("errors").downArray.downField("extensions").get[String]("code")

  test("five expensive aliases through fragments reject before any discovery resolver runs") {
    val query = """query {
      ...DiscoveryRoots
      fifth: jobDiscoveryFacets { truncated }
    }
    fragment DiscoveryRoots on Query {
      first: jobDiscoveryFacets { truncated }
      second: jobDiscoveryFacets { truncated }
      third: jobDiscoveryFacets { truncated }
      fourth: jobDiscoveryFacets { truncated }
    }"""
    JobDiscoveryTestSupport.fixture(List(candidate)).flatMap { fixture =>
      TestGraphQLSupport
        .context(
          IO.pure(ProbeResult.Ready),
          Some(actor),
          TestGraphQLSupport.emptyServices.copy(jobService = fixture.service)
        )
        .use { context =>
          TestGraphQLSupport.parseAndExecute(GraphQLRequest(query, Json.obj(), None), context).flatMap { result =>
            assertEquals(result, Left(HiringGraphQLSchema.Failure.InvalidQuery))
            fixture.calls.get.map(calls => assertEquals(calls, JobDiscoveryTestSupport.Calls()))
          }
        }
    }
  }

  test("request context can lower the expensive root budget without running resolvers") {
    val query = """query {
      first: jobDiscoveryFacets { truncated }
      second: jobDiscoveryFacets { truncated }
      third: jobDiscoveryFacets { truncated }
    }"""
    JobDiscoveryTestSupport.fixture(List(candidate)).flatMap { fixture =>
      RequestContextFactory.resource
        .flatMap(
          _.resource(
            RequestContextParameters(
              IO.pure(ProbeResult.Ready),
              Some(actor),
              TestGraphQLSupport.emptyServices.copy(jobService = fixture.service),
              IO.pure(ProbeResult.Ready),
              diagnostics = com.example.graphQL.cats.service.Diagnostics.noop,
              discoveryMaxRoots = 2
            )
          )
        )
        .use { context =>
          TestGraphQLSupport.parseAndExecute(GraphQLRequest(query, Json.obj(), None), context).flatMap { result =>
            assertEquals(result, Left(HiringGraphQLSchema.Failure.InvalidQuery))
            fixture.calls.get.map(calls => assertEquals(calls, JobDiscoveryTestSupport.Calls()))
          }
        }
    }
  }

  test("four expensive aliases remain allowed within depth and complexity limits") {
    val query = """query {
      first: jobDiscoveryFacets { truncated }
      second: jobDiscoveryFacets { truncated }
      third: jobDiscoveryFacets { truncated }
      fourth: jobDiscoveryFacets { truncated }
    }"""
    JobDiscoveryTestSupport.fixture(List(candidate)).flatMap { fixture =>
      execute(query, Json.obj(), fixture).flatMap { result =>
        assert(!result.hcursor.downField("errors").succeeded)
        fixture.calls.get.map(calls => assertEquals(calls.facets.size, 4))
      }
    }
  }

  test("geographic hits and exact structured facet operations validate against the active schema") {
    val query = """query {
      nearbyJobs(center: {latitude: 35.1856, longitude: 33.3823}, radiusKm: 20,
        filter: {city: "Nicosia", skills: ["Scala"]}, first: 7) {
        results { job { id location { coordinates { latitude longitude } } } distanceKm cursor }
        hasNextPage
      }
      jobDiscoveryFacets(filter: {skills: ["Scala"]}) {
        skills { value count } countries { value count } cities { value count } remote { value count } truncated
      }
      nearbyFacets: jobDiscoveryFacets(center: {latitude: 35.1856, longitude: 33.3823}, radiusKm: 20) {
        skills { value count } truncated
      }
    }"""
    val document = QueryParser.parse(query).fold(error => fail(error.getMessage), identity)
    assertEquals(
      QueryValidator.default.validateQuery(HiringGraphQLSchema.schema, document, Map.empty, None).toList,
      Nil
    )
  }

  test("discovery resolvers execute authorized hits, facets and cursor encoding with normalized filters") {
    val query = """query($filter: NearbyJobsFilter) {
      nearbyJobs(center: {latitude: 35, longitude: 33}, radiusKm: 20, first: 1, filter: $filter) {
        results { job { id } distanceKm cursor } hasNextPage
      }
      jobDiscoveryFacets(filter: $filter) { skills { value count } cities { value count } truncated }
    }"""
    val variables = Json.obj(
      "filter" -> Json.obj(
        "city" -> Json.fromString(" Nicosia "),
        "skills" -> Json.arr(Json.fromString(" Scala "), Json.fromString("Scala"), Json.fromString(" "))
      )
    )
    JobDiscoveryTestSupport.fixture(List(candidate), List(pointJob)).flatMap { fixture =>
      for {
        result <- execute(query, variables, fixture)
        calls <- fixture.calls.get
      } yield {
        assert(!result.hcursor.downField("errors").succeeded, result.noSpaces)
        val hit = result.hcursor.downField("data").downField("nearbyJobs").downField("results").downArray
        assertEquals(hit.downField("job").get[String]("id"), Right(pointJob.id.value.toString))
        assertEquals(hit.get[Double]("distanceKm"), Right(0d))
        assertEquals(result.hcursor.downField("data").downField("nearbyJobs").get[Boolean]("hasNextPage"), Right(false))
        val normalized = JobSearchFilter(Some("Nicosia"), Set("Scala"), None)
        assertEquals(calls.nearby.map(_.filter), List(normalized))
        assertEquals(calls.facets.map(_.filter), List(normalized))
        assert(
          hit
            .get[String]("cursor")
            .toOption
            .exists(cursor => NearbyJobsQuery.decodeCursor(cursor, NearbyJobsQuery(center, 20d, normalized)).isRight)
        )
        val skill = result.hcursor.downField("data").downField("jobDiscoveryFacets").downField("skills").downArray
        assertEquals(skill.get[String]("value"), Right("Scala"))
        assertEquals(skill.get[Long]("count"), Right(1L))
      }
    }
  }

  test("executed invalid geographic, page and pairing inputs never call discovery repositories") {
    val cases = List(
      "nearbyJobs(center: {latitude: 91, longitude: 33}, radiusKm: 20, first: 1) { hasNextPage }" -> "INVALID_SEARCH_FILTER",
      "nearbyJobs(center: {latitude: 35, longitude: 33}, radiusKm: 501, first: 1) { hasNextPage }" -> "INVALID_SEARCH_FILTER",
      "nearbyJobs(center: {latitude: 35, longitude: 33}, radiusKm: 0, first: 1) { hasNextPage }" -> "INVALID_SEARCH_FILTER",
      "nearbyJobs(center: {latitude: 35, longitude: 33}, radiusKm: 20, first: 0) { hasNextPage }" -> "VALIDATION_FAILED",
      "nearbyJobs(center: {latitude: 35, longitude: 33}, radiusKm: 20, first: 101) { hasNextPage }" -> "VALIDATION_FAILED",
      "jobDiscoveryFacets(center: {latitude: 35, longitude: 33}) { truncated }" -> "INVALID_FILTER",
      "jobDiscoveryFacets(radiusKm: 20) { truncated }" -> "INVALID_FILTER",
      "jobDiscoveryFacets(center: {latitude: 35, longitude: 33}, radiusKm: -1) { truncated }" -> "INVALID_SEARCH_FILTER"
    )
    JobDiscoveryTestSupport.fixture(List(candidate)).flatMap { fixture =>
      cases.traverse_ { case (field, code) =>
        execute(s"query { $field }", Json.obj(), fixture)
          .map(result => assertEquals(errorCode(result), Right(code)))
      } *> fixture.calls.get.map(calls => assertEquals(calls, JobDiscoveryTestSupport.Calls()))
    }
  }

  test("executed blank and oversized filters fail before discovery calls") {
    val invalidFilters = List(
      Json.obj("city" -> Json.fromString(" ")),
      Json.obj("city" -> Json.fromString("a" * 257)),
      Json.obj("skills" -> Json.arr(Json.fromString("a" * 257))),
      Json.obj("skills" -> Json.fromValues((1 to 101).map(index => Json.fromString(s"Skill$index"))))
    )
    val operations = List(
      "nearbyJobs(center: {latitude: 35, longitude: 33}, radiusKm: 20, first: 1, filter: $filter) { hasNextPage }",
      "jobDiscoveryFacets(filter: $filter) { truncated }"
    )
    JobDiscoveryTestSupport.fixture(List(candidate)).flatMap { fixture =>
      (invalidFilters, operations).tupled.traverse_ { case (filter, field) =>
        execute(s"query($$filter: NearbyJobsFilter) { $field }", Json.obj("filter" -> filter), fixture)
          .map(result => assertEquals(errorCode(result), Right("INVALID_SEARCH_FILTER")))
      } *> fixture.calls.get.map(calls => assertEquals(calls, JobDiscoveryTestSupport.Calls()))
    }
  }

  test("executed malformed and wrong-bound cursors preserve sanitized cursor errors") {
    val query = NearbyJobsQuery(center, 20d, JobSearchFilter(None, Set.empty, None))
    val wrongBound = NearbyJobsQuery.encodeCursor(1d, pointJob.id, query.copy(radiusKm = 21d))
    val invalidDistance = NearbyJobsQuery.encodeCursor(-1d, pointJob.id, query)
    val invalidIdentity = java.util.Base64.getUrlEncoder
      .withoutPadding()
      .encodeToString(s"1.0|invalid|${query.fingerprint}".getBytes(java.nio.charset.StandardCharsets.UTF_8))
    val operation = """query($after: String) {
      nearbyJobs(center: {latitude: 35, longitude: 33}, radiusKm: 20, first: 1, after: $after) { hasNextPage }
    }"""
    JobDiscoveryTestSupport.fixture(List(candidate)).flatMap { fixture =>
      List("%", wrongBound, invalidDistance, invalidIdentity).traverse_ { cursor =>
        execute(operation, Json.obj("after" -> Json.fromString(cursor)), fixture)
          .map(result => assertEquals(errorCode(result), Right("INVALID_CURSOR")))
      } *> fixture.calls.get.map(calls => assertEquals(calls, JobDiscoveryTestSupport.Calls()))
    }
  }

  test("executed discovery authorizes active candidates and singleton admin and denies restricted accounts") {
    val operation = "query { jobDiscoveryFacets { truncated } }"
    val active = List(candidate, ServiceFixtures.admin)
    val denied = List(ServiceFixtures.recruiter, candidate.copy(accountStatus = AccountStatus.Deleted))
    active.traverse_ { user =>
      JobDiscoveryTestSupport.fixture(List(user)).flatMap { fixture =>
        execute(operation, Json.obj(), fixture, Some(ActorContext(user.id, user.role)))
          .map(result => assert(!result.hcursor.downField("errors").succeeded))
      }
    } *> denied.traverse_ { user =>
      JobDiscoveryTestSupport.fixture(List(user)).flatMap { fixture =>
        execute(operation, Json.obj(), fixture, Some(ActorContext(user.id, user.role))).map { result =>
          assertEquals(
            errorCode(result),
            Right(if (user.accountStatus == AccountStatus.Deleted) "UNAUTHORIZED" else "FORBIDDEN")
          )
        } *>
          fixture.calls.get.map(calls => assertEquals(calls, JobDiscoveryTestSupport.Calls()))
      }
    } *> JobDiscoveryTestSupport.fixture(List(candidate)).flatMap { fixture =>
      execute(operation, Json.obj(), fixture, None)
        .map(result => assertEquals(errorCode(result), Right("UNAUTHORIZED"))) *>
        fixture.calls.get.map(calls => assertEquals(calls, JobDiscoveryTestSupport.Calls()))
    }
  }
}
