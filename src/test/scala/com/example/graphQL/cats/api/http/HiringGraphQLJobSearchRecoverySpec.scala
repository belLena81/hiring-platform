package com.example.graphQL.cats.api.http

import io.github.iltotore.iron.{autoRefine, refineUnsafe}
import cats.effect.{IO, Ref}
import com.example.graphQL.cats.api.graphql.TestGraphQLSupport
import com.example.graphQL.cats.config.CostBudgetConfig
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.{Application, ApplicationEvent, Job, User, UserRole}
import com.example.graphQL.cats.domain.pagination.JobPageRequest
import com.example.graphQL.cats.service.{
  ActorContext,
  DatabaseProbe,
  Diagnostics,
  HealthService,
  HiringReadService,
  ProbeResult,
  RepositoryError,
  ServiceFixtures,
  TestHiringServices
}
import com.example.graphQL.cats.service.ServiceFixtures.{InMemoryApplications, InMemoryJobs, InMemoryUsers}
import com.example.graphQL.cats.api.graphql.HiringGraphQLServices
import io.circe.Json
import munit.CatsEffectSuite
import org.http4s.*
import org.http4s.circe.*
import org.typelevel.ci.CIString
import scala.concurrent.duration.*

/** GDF-01 over GraphQL: `jobs` names the relaxed filters, prices the extra searches, and never relaxes silently. */
final class HiringGraphQLJobSearchRecoverySpec extends CatsEffectSuite {
  import ServiceFixtures.{candidate, candidateId, openJob, recruiter, later, now}

  private val actorHeader = CIString("X-Test-Actor")
  private val window = later.toString // the stored jobs were created at `now`, before this bound
  private val first = 2
  private val heavyNode = "id title description skills"

  private def jobWith(index: Int, skills: Set[String]): Job =
    openJob.copy(
      id = JobId(java.util.UUID.fromString(f"00000000-0000-0000-0000-0000000002$index%02d")),
      skills = skills,
      createdAt = now.plusSeconds(index.toLong)
    )

  private def query(filter: String, size: Int = first, node: String = "id"): String =
    s"""{ jobs(first: $size, filter: $filter) {
       |  relaxedFilters { filter from to }
       |  edges { cursor node { $node } }
       |  pageInfo { hasNextPage }
       |} }""".stripMargin

  private def build(
      stored: List[Job],
      maxCost: Int = 100000
  ): IO[(HttpApp[IO], Ref[IO, Vector[JobPageRequest]])] =
    for {
      usersRef <- Ref.of[IO, Map[UserId, User]](List(candidate, recruiter).map(user => user.id -> user).toMap)
      jobsRef <- Ref.of[IO, Map[JobId, Job]](stored.map(job => job.id -> job).toMap)
      applicationsRef <- Ref.of[IO, Map[ApplicationId, Application]](Map.empty)
      eventsRef <- Ref.of[IO, Vector[ApplicationEvent]](Vector.empty)
      errors <- Ref.of[IO, Option[RepositoryError]](None)
      searches <- Ref.of[IO, Vector[JobPageRequest]](Vector.empty)
      users = new InMemoryUsers(usersRef)
      jobs = new InMemoryJobs(jobsRef, pageRequests = Some(searches))
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
      built <- TestGraphQLSupport
        .dependencies(
          hiring = services,
          authenticate = request =>
            IO.pure(Right(request.headers.get(actorHeader).map(_ => ActorContext(candidateId, UserRole.Candidate)))),
          costBudget = CostBudgetConfig(60, maxCost.refineUnsafe, 100, 100)
        )
        .allocated
      probe = new DatabaseProbe[IO] {
        def check(requestId: Option[String]): IO[ProbeResult] = IO.pure(ProbeResult.Ready)
      }
      http <- new HiringApiRoutes(new HealthService(probe, Diagnostics.noop), Diagnostics.noop, built._1)
        .httpApp(HiringApiRoutes.HttpConfig(16L, 5.seconds))
    } yield (http, searches)

  private def post(text: String, signedIn: Boolean = true): Request[IO] = {
    val base =
      Request[IO](Method.POST, Uri.unsafeFromString("/graphql")).withEntity(Json.obj("query" -> Json.fromString(text)))
    if (signedIn) base.putHeaders(Header.Raw(actorHeader, "candidate")) else base
  }

  private def run(http: HttpApp[IO], text: String): IO[Json] = http(post(text)).flatMap(_.as[Json])

  private def jobsOf(body: Json) = body.hcursor.downField("data").downField("jobs")

  private def relaxed(body: Json): List[(String, String, Option[String])] =
    jobsOf(body)
      .downField("relaxedFilters")
      .as[List[Json]]
      .getOrElse(Nil)
      .map(item =>
        (item.hcursor.get[String]("filter"), item.hcursor.get[String]("from"), item.hcursor.get[Option[String]]("to"))
      )
      .collect { case (Right(f), Right(from), Right(to)) => (f, from, to) }

  private def requestedCost(body: Json): Option[Int] =
    body.hcursor.downField("extensions").downField("cost").get[Int]("requested").toOption

  private def edgeCount(body: Json): Int = jobsOf(body).downField("edges").values.fold(0)(_.size)

  test("GDF-01.1 an empty window relaxes createdAfter and the response names the filter, from and to") {
    for {
      (http, _) <- build(List(jobWith(1, Set("Scala", "Kafka"))))
      body <- run(http, query(s"""{ skills: ["Scala", "Kafka"], createdAfter: "$window" }"""))
    } yield {
      assertEquals(edgeCount(body), 1, clue(body))
      assertEquals(relaxed(body), List(("CREATED_AFTER", window, None)), clue(body))
    }
  }

  test("GDF-01.1 skills widened from all-of to any-of are named ALL_OF to ANY_OF") {
    for {
      (http, _) <- build(List(jobWith(1, Set("Scala"))))
      body <- run(http, query("""{ skills: ["Scala", "Kafka"] }"""))
    } yield assertEquals(relaxed(body), List(("SKILLS", "ALL_OF", Some("ANY_OF"))), clue(body))
  }

  test("GDF-01.2 a non-empty page carries an empty relaxedFilters and costs one search") {
    for {
      (http, searches) <- build(List(jobWith(1, Set("Scala"))))
      body <- run(http, query("""{ skills: ["Scala"] }"""))
      count <- searches.get.map(_.size)
    } yield {
      assertEquals(edgeCount(body), 1, clue(body))
      assertEquals(relaxed(body), Nil, clue(body))
      assertEquals(count, 1)
    }
  }

  test("GDF-01.3 an unrecoverable query is empty, ran two extra searches and its cost reflects them") {
    val unrecoverable = s"""{ skills: ["Scala", "Rust"], createdAfter: "$window" }"""
    val recoverableBase = s"""{ skills: ["Scala"] }"""
    for {
      (http, searches) <- build(List(jobWith(1, Set("Scala"))))
      single <- run(http, query(recoverableBase))
      recovered <- run(http, query(unrecoverable))
      count <- searches.get.map(_.size)
    } yield {
      assertEquals(edgeCount(recovered), 0, clue(recovered))
      assertEquals(relaxed(recovered), Nil, clue(recovered))
      assertEquals(count, 1 + 1 + 2, "one search for the first query, three for the recovered one")
      assertEquals(
        requestedCost(recovered).zip(requestedCost(single)).map { case (all, base) => all - base },
        requestedCost(single).map(2 * _),
        clue(recovered)
      )
    }
  }

  test("GDF-01.3 a heavier selection charges each extra search exactly what the base jobs field was charged") {
    val unrecoverable = s"""{ skills: ["Scala", "Rust"], createdAfter: "$window" }"""
    for {
      (http, _) <- build(List(jobWith(1, Set("Scala"))))
      light <- run(http, query("""{ skills: ["Scala"] }"""))
      heavy <- run(http, query("""{ skills: ["Scala"] }""", node = heavyNode))
      recovered <- run(http, query(unrecoverable, node = heavyNode))
    } yield {
      val lightCost = requestedCost(light).getOrElse(fail(s"no cost: $light"))
      val heavyCost = requestedCost(heavy).getOrElse(fail(s"no cost: $heavy"))
      assert(heavyCost > lightCost, s"$heavyCost must exceed $lightCost")
      assertEquals(requestedCost(recovered), Some(3 * heavyCost), clue(recovered))
    }
  }

  test("GDF-01.3 two aliased jobs fields that both relax are each priced on their own and the total is the sum") {
    val unrecoverable = s"""{ skills: ["Scala", "Rust"], createdAfter: "$window" }"""
    def two(node: String): String =
      s"""{
         |  a: jobs(first: $first, filter: $unrecoverable) { edges { node { id } } }
         |  b: jobs(first: $first, filter: $unrecoverable) { edges { node { $node } } }
         |}""".stripMargin
    def one(node: String): String =
      s"""{ a: jobs(first: $first, filter: { skills: ["Scala"] }) { edges { node { $node } } } }"""
    for {
      (http, searches) <- build(List(jobWith(1, Set("Scala"))))
      costA <- run(http, one("id")).map(requestedCost)
      costB <- run(http, one(heavyNode)).map(requestedCost)
      before <- searches.get.map(_.size)
      both <- run(http, two(heavyNode))
      count <- searches.get.map(_.size).map(_ - before)
    } yield {
      val a = costA.getOrElse(fail("no cost for the light field"))
      val b = costB.getOrElse(fail("no cost for the heavy field"))
      assert(b > a, s"$b must exceed $a")
      // Each field is its own base charge plus two extra searches at that field's own price.
      assertEquals(requestedCost(both), Some(3 * a + 3 * b), clue(both))
      assertEquals(count, 6, "three searches for each alias")
    }
  }

  test("GDF-01.3 a heavy selection that cannot pay for an extra search returns the empty base page and one search") {
    val unrecoverable = s"""{ skills: ["Scala", "Rust"], createdAfter: "$window" }"""
    for {
      (probeHttp, _) <- build(List(jobWith(1, Set("Scala"))))
      base <- run(probeHttp, query("""{ skills: ["Scala"] }""", node = heavyNode))
      baseCost = requestedCost(base).getOrElse(fail(s"no cost: $base"))
      // Room for the query and one extra search at the heavy price; a lower bound price would admit a second one.
      (http, searches) <- build(List(jobWith(1, Set("Scala"))), maxCost = 2 * baseCost + 1)
      body <- run(http, query(unrecoverable, node = heavyNode))
      count <- searches.get.map(_.size)
    } yield {
      assertEquals(body.hcursor.downField("errors").focus, None, clue(body))
      assertEquals(edgeCount(body), 0, clue(body))
      assertEquals(count, 2, "the base search and one admitted extra search")
    }
  }

  test("GDF-01.3 a caller whose budget cannot pay for an extra search gets the empty base page, not an error") {
    val unrecoverable = s"""{ skills: ["Scala", "Rust"], createdAfter: "$window" }"""
    for {
      // The same selection without a relaxation reports what the query itself costs.
      (probeHttp, _) <- build(List(jobWith(1, Set("Scala"))))
      base <- run(probeHttp, query("""{ skills: ["Scala"] }"""))
      baseCost = requestedCost(base).getOrElse(fail(s"no cost: $base"))
      // Room for the query itself and exactly one extra search.
      (http, searches) <- build(List(jobWith(1, Set("Scala"))), maxCost = 2 * baseCost)
      body <- run(http, query(unrecoverable))
      count <- searches.get.map(_.size)
    } yield {
      assertEquals(body.hcursor.downField("errors").focus, None, clue(body))
      assertEquals(edgeCount(body), 0, clue(body))
      assertEquals(count, 2, "the base search and one admitted extra search; the second extra search was refused")
    }
  }

  test("GDF-01.1 a relaxed page is the only page: hasNextPage is false even when more jobs match the relaxed filter") {
    val stored = (1 to 3).toList.map(index => jobWith(index, Set("Scala", "Kafka")))
    for {
      (http, _) <- build(stored)
      body <- run(http, query(s"""{ skills: ["Scala", "Kafka"], createdAfter: "$window" }""", size = 1))
    } yield {
      assertEquals(edgeCount(body), 1, clue(body))
      assertEquals(relaxed(body).map(_._1), List("CREATED_AFTER"), clue(body))
      assertEquals(jobsOf(body).downField("pageInfo").get[Boolean]("hasNextPage"), Right(false), clue(body))
    }
  }

  test("GDF-08 an unauthenticated caller is refused before any search or relaxation runs") {
    for {
      (http, searches) <- build(List(jobWith(1, Set("Scala", "Kafka"))))
      response <- http(post(query(s"""{ skills: ["Scala", "Kafka"], createdAfter: "$window" }"""), signedIn = false))
      body <- response.as[Json]
      count <- searches.get.map(_.size)
    } yield {
      assertEquals(count, 0)
      assertEquals(relaxed(body), Nil, clue(body))
      assert(body.hcursor.downField("errors").focus.isDefined, clue(body))
    }
  }
}
