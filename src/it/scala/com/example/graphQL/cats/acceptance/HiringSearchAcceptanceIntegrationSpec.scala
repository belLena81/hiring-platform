package com.example.graphQL.cats.acceptance

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.example.hiring.testing.DisposableAtlas
import com.example.hiring.testing.HiringSearchStoryData as story
import io.circe.Json
import io.circe.syntax.*
import java.nio.file.Path
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.*

/** The search part of the hiring story on Atlas Local (docs/specs/hiring-end-to-end-acceptance.md, Tier B): UC02 hybrid
  * search and its degraded fallback, UC06 recommendations and their stale refusal, UC10 candidate matches and privacy.
  *
  * The real `Main` runs as a child JVM on a nonce database of the disposable deployment authorized by `ATLAS_TEST_URI`,
  * `ATLAS_TEST_DISPOSABLE` and `ATLAS_TEST_ALLOWED_HOSTS` (the `DisposableAtlas` guard). Embeddings come from a local
  * deterministic stub reached by configuration only. Without the guard the tests are skipped as BLOCKED, never reported
  * as a pass. The expected outcomes are written in the spec section "Tier B expected outcomes", fixed before any run.
  *
  * The tests run in the order of the story and share its state. Atlas Search indexes are eventually consistent, so a
  * read is repeated for a bounded time until the visible, current documents are indexed; a hidden job or candidate in
  * any answer fails at once, because that is a visibility defect and not index lag.
  */
final class HiringSearchAcceptanceIntegrationSpec extends munit.CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 15.minutes

  private val model = "voyage-4-lite"
  private val indexLagDeadline = 120.seconds
  private val embeddingDeadline = 180.seconds

  private val stackFixture = ResourceSuiteLocalFixture(
    "hiring-search-atlas-stack",
    Resource.eval(IO.delay(DisposableAtlas.authorizedUri(sys.env))).flatMap {
      case Left(_)    => Resource.pure[IO, Option[HiringSearchAtlasStack.Running]](None)
      case Right(uri) =>
        if (DisposableAtlas.deployment(uri) != "local-container")
          Resource.eval(
            IO.raiseError[Option[HiringSearchAtlasStack.Running]](
              new IllegalStateException("The search story runs on Atlas Local (loopback) only")
            )
          )
        else {
          val workspace = sys.env.getOrElse("HIRING_TEST_WORKSPACE", sys.props("user.dir"))
          HiringSearchAtlasStack.resource(uri, Path.of(workspace)).map(Option(_))
        }
    }
  )

  override def munitFixtures: List[munit.AnyFixture[?]] = List(stackFixture)

  private def stack: HiringSearchAtlasStack.Running = {
    val value = stackFixture()
    assume(
      value.nonEmpty,
      "BLOCKED: Atlas Local is not authorized; set ATLAS_TEST_URI, ATLAS_TEST_DISPOSABLE=true and ATLAS_TEST_ALLOWED_HOSTS"
    )
    value.get
  }

  /** A story test always resolves the stack first, so without the Atlas guard it is skipped as BLOCKED. */
  private def storyTest(name: String)(body: => IO[Any])(using munit.Location): Unit =
    test(name) {
      val _ = stack
      body
    }

  private lazy val operations = new StoryOperations(stack.client)

  private val actors = TrieMap.empty[String, StoryActor]
  private val ids = TrieMap.empty[String, String] // story key (J1..J5) -> job id
  private def anaActor: StoryActor = actors(story.ana.key)
  private def ritaActor: StoryActor = actors("R1")
  private def raviActor: StoryActor = actors("R2")

  private def jobKey(id: String): String = ids.collectFirst { case (key, value) if value == id => key }.getOrElse(id)
  private def candidateKey(id: String): String =
    actors.collectFirst { case (key, actor) if actor.userId == id => key }.getOrElse(id)

  private def array(value: Json, field: String): Vector[Json] =
    value.hcursor.downField(field).focus.flatMap(_.asArray).getOrElse(Vector.empty)

  private def hitJobKeys(value: Json): Vector[String] =
    array(value, "results").flatMap(_.hcursor.downField("job").get[String]("id").toOption).map(jobKey)

  private def hitCandidateKeys(value: Json): Vector[String] =
    array(value, "results").flatMap(_.hcursor.downField("candidate").get[String]("id").toOption).map(candidateKey)

  private def scores(value: Json): Vector[Double] =
    array(value, "results").flatMap(_.hcursor.get[Double]("score").toOption)

  private def modes(value: Json): Vector[String] =
    array(value, "results").flatMap(_.hcursor.get[String]("searchMode").toOption)

  private def lowerCased(values: Iterable[String]): List[String] = values.map(_.toLowerCase).toList

  private val hiddenJobs = Set(story.j3.key, story.j5.key)

  private val coverageDocument =
    "query Coverage($model: String) { embeddingCoverage(expectedModel: $model) { queueTruncated kinds { kind searchableCount scannedCount truncated } cells { kind freshness repairState count } } }"

  private val semanticSearchDocument =
    "query Search($query: String!, $first: Int!) { semanticJobSearch(query: $query, first: $first) { __typename ... on UserError { code message } ... on RankedJobResults { searchId model degradedReason results { score searchMode matchedSkills job { id title status } } } } }"

  private val recommendationsDocument =
    "query Recommended($first: Int!) { recommendedJobs(first: $first) { __typename ... on UserError { code message } ... on RankedJobResults { searchId model degradedReason results { score searchMode matchedSkills job { id title status } } } } }"

  private val matchesDocument =
    "query Matches($jobId: JobID!, $query: String, $filter: CandidateMatchFilter, $first: Int!) { candidateMatches(jobId: $jobId, query: $query, filter: $filter, first: $first) { __typename ... on UserError { code message } ... on RankedCandidateResults { searchId results { score searchMode matchedSkills candidate { id name profile { skills experienceSummary } } } } } }"

  /** Repeats `attempt` every 2 s until `ready` holds, at most `deadline`. */
  private def eventually[A](what: String, deadline: FiniteDuration)(attempt: IO[A])(ready: A => Boolean): IO[A] = {
    def loop: IO[A] = attempt.flatMap(value => if (ready(value)) IO.pure(value) else IO.sleep(2.seconds) *> loop)
    loop.timeoutTo(deadline, IO.raiseError(new AssertionError(s"$what was not ready within $deadline")))
  }

  private def search(document: String, field: String, variables: Json, actor: StoryActor): IO[Json] =
    operations.read(document, field, variables, actor)

  private def leakGuard(keys: Vector[String], context: String): Unit =
    assert(keys.toSet.intersect(hiddenJobs).isEmpty, s"$context returned a closed or unpublished job: $keys")

  private def assertDescending(values: Vector[Double], context: String): Unit =
    assert(
      values == values.sorted(using Ordering[Double].reverse),
      s"$context are not in descending score order: $values"
    )

  storyTest("HEA-02.0 accounts, jobs J1 to J5 and their embeddings are in place before search is exercised") {
    def publishJob(seed: story.JobSeed, owner: StoryActor, close: Boolean): IO[Unit] =
      for {
        id <- operations
          .succeed(
            "createJob",
            operations.jobDocument(seed.title, seed.description, seed.requirements, seed.city, seed.skills),
            owner
          )
          .map(operations.idOf)
        _ <- operations.succeed("publishJob", Json.obj("jobId" -> id.asJson), owner)
        _ <- if (close) operations.succeed("closeJob", Json.obj("jobId" -> id.asJson), owner).void else IO.unit
        _ = ids.update(seed.key, id)
      } yield ()
    for {
      administrator <- operations.login(HiringStoryStack.AdminName, HiringStoryStack.AdminPassword, "ADMIN")
      r1 <- operations.signUpRecruiter("Rita Recruiter", "Rita Hiring GmbH")
      r2 <- operations.signUpRecruiter("Ravi Recruiter", "Ravi Hiring GmbH")
      _ = actors.update("R1", r1)
      _ = actors.update("R2", r2)
      _ <- story.candidates.traverse_ { seed =>
        operations
          .signUpCandidate(seed.name, seed.skills, seed.city, seed.experienceSummary, seed.recruiterSearchOptIn)
          .map(actors.update(seed.key, _))
      }
      _ <- publishJob(story.j1, r1, close = false)
      _ <- publishJob(story.j2, r1, close = false)
      _ <- publishJob(story.j3, r1, close = true)
      _ <- publishJob(story.j4, r2, close = false)
      // J5 stays a Draft: created, never published.
      draft <- operations.succeed(
        "createJob",
        operations.jobDocument(
          story.j5.title,
          story.j5.description,
          story.j5.requirements,
          story.j5.city,
          story.j5.skills
        ),
        r1
      )
      _ = ids.update(story.j5.key, operations.idOf(draft))
      coverage <- eventually("embedding coverage", embeddingDeadline)(
        operations.read(coverageDocument, "embeddingCoverage", Json.obj("model" -> model.asJson), administrator)
      ) { report =>
        val cells = array(report, "cells")
        cells.nonEmpty && cells.forall(_.hcursor.get[String]("freshness") == Right("CURRENT"))
      }
    } yield {
      def searchable(kind: String): Option[Long] =
        array(coverage, "kinds")
          .find(_.hcursor.get[String]("kind") == Right(kind))
          .flatMap(_.hcursor.get[Long]("searchableCount").toOption)
      assertEquals(searchable("JOB"), Some(3L), "J1, J2 and J4 are the open jobs")
      assertEquals(searchable("CANDIDATE_PROFILE"), Some(4L), "the four candidates have profiles")
      val counts = array(coverage, "cells").map(cell =>
        cell.hcursor.get[String]("kind").getOrElse("?") -> cell.hcursor.get[Long]("count").getOrElse(-1L)
      )
      assertEquals(counts.toMap, Map("JOB" -> 3L, "CANDIDATE_PROFILE" -> 4L), "every searchable entity is CURRENT")
      assert(array(coverage, "cells").forall(_.hcursor.get[String]("repairState") == Right("NO_QUEUED_WORK")))
    }
  }

  storyTest("HEA-02.1 Ana's search for 'functional JVM backend' is hybrid, ranks J1 first and never shows J3 or J5") {
    val calls = stack.stub.callCount
    val variables = Json.obj("query" -> "functional JVM backend".asJson, "first" -> 10.asJson)
    eventually("hybrid search index", indexLagDeadline)(
      search(semanticSearchDocument, "semanticJobSearch", variables, anaActor)
        .flatTap(answer => IO(leakGuard(hitJobKeys(answer), "semanticJobSearch")))
    )(answer => hitJobKeys(answer).contains(story.j1.key)).map { answer =>
      val keys = hitJobKeys(answer)
      assertEquals(keys.headOption, Some(story.j1.key), s"J1 is the best match for the query: $keys")
      assert(keys.toSet.subsetOf(Set(story.j1.key, story.j2.key, story.j4.key)), s"only open jobs: $keys")
      assert(modes(answer).nonEmpty && modes(answer).forall(_ == "HYBRID"), s"every hit is HYBRID: ${modes(answer)}")
      assertDescending(scores(answer), "fused scores")
      assertEquals(answer.hcursor.get[String]("model"), Right(model))
      assert(answer.hcursor.downField("degradedReason").focus.forall(_.isNull), "the answer is not degraded")
      assert(answer.hcursor.get[String]("searchId").exists(_.nonEmpty))
      assert(stack.stub.callCount > calls, "the query was embedded by the provider stub")
    }
  }

  storyTest("HEA-06.1 Ana's recommendations contain only open, visible jobs and put J1 first") {
    eventually("recommendations", indexLagDeadline)(
      search(recommendationsDocument, "recommendedJobs", Json.obj("first" -> 10.asJson), anaActor)
        .flatTap(answer => IO(leakGuard(hitJobKeys(answer), "recommendedJobs")))
    )(answer => hitJobKeys(answer).contains(story.j1.key)).map { answer =>
      val keys = hitJobKeys(answer)
      assertEquals(keys.headOption, Some(story.j1.key), s"Ana's profile is closest to J1: $keys")
      assert(keys.toSet.subsetOf(Set(story.j1.key, story.j2.key, story.j4.key)), s"only open jobs: $keys")
      assertDescending(scores(answer), "recommendation scores")
      val matched = array(answer, "results").headOption.flatMap(_.hcursor.get[List[String]]("matchedSkills").toOption)
      assertEquals(matched.map(lowerCased), Some(List("kafka", "scala")))
    }
  }

  storyTest("HEA-10.1 Rita's matches for J1 rank Ana first with matched skills, and reveal no private profile data") {
    val jobId = ids(story.j1.key)
    def matches(filter: Option[Json]): IO[GraphQLResponse] =
      operations.run(
        matchesDocument,
        Json.obj("jobId" -> jobId.asJson, "first" -> 10.asJson, "filter" -> filter.asJson),
        Some(ritaActor)
      )
    def answerOf(response: GraphQLResponse): Json =
      StoryOutcome.of(response, "candidateMatches") match {
        case StoryOutcome.Succeeded(value) => value
        case other                         => fail(s"candidateMatches expected success but was $other")
      }
    val everyone = story.candidates.map(_.key).toSet
    for {
      unfiltered <- eventually("candidate index", indexLagDeadline)(matches(None))(response =>
        hitCandidateKeys(answerOf(response)).toSet == everyone
      )
      karlsruhe <- matches(Some(Json.obj("city" -> "Karlsruhe".asJson)))
      hamburg <- matches(Some(Json.obj("city" -> "Hamburg".asJson)))
      nowhere <- matches(Some(Json.obj("city" -> "Atlantis".asJson)))
      probe <- operations.run(
        "query Probe($jobId: JobID!) { candidateMatches(jobId: $jobId, first: 3) { ... on RankedCandidateResults { results { candidate { email residence { city } } } } } }",
        Json.obj("jobId" -> jobId.asJson),
        Some(ritaActor)
      )
    } yield {
      val value = answerOf(unfiltered)
      val keys = hitCandidateKeys(value)
      assertEquals(keys.take(2), Vector(story.ana.key, story.dee.key), s"ranking: $keys")
      val matched = array(value, "results").map { hit =>
        candidateKey(hit.hcursor.downField("candidate").get[String]("id").getOrElse("")) ->
          lowerCased(hit.hcursor.get[List[String]]("matchedSkills").getOrElse(Nil))
      }.toMap
      assertEquals(matched.get(story.ana.key), Some(List("kafka", "scala")))
      assertEquals(matched.get(story.dee.key), Some(List("scala")))
      assertEquals(matched.get(story.bo.key), Some(Nil))
      // The recruiter sees names, skills and experience summaries, never residence, availability, email or consent.
      val raw = unfiltered.raw.toLowerCase
      List(
        "residence",
        "availability",
        "email",
        "consent",
        "optin",
        "password",
        "berlin",
        "munich",
        "karlsruhe",
        "hamburg"
      ).foreach(term => assert(!raw.contains(term), s"the match answer must not contain '$term'"))
      assert(probe.errors.nonEmpty, "asking for the email or residence of a match is refused by the schema")
      // A residence filter applies to opted-in candidates only; the opted-out Dee is retained whatever it says.
      assertEquals(hitCandidateKeys(answerOf(karlsruhe)).toSet, Set(story.cy.key, story.dee.key))
      assertEquals(hitCandidateKeys(answerOf(hamburg)).toSet, Set(story.dee.key))
      assertEquals(hitCandidateKeys(answerOf(nowhere)).toSet, Set(story.dee.key))
    }
  }

  storyTest(
    "HEA-10.2 a non-owner recruiter and a candidate are refused matches for J1, and no embedding call is made"
  ) {
    val before = stack.stub.callCount
    val variables = Json.obj("jobId" -> ids(story.j1.key).asJson, "first" -> 5.asJson, "query" -> "Scala".asJson)
    for {
      other <- operations.query(matchesDocument, "candidateMatches", variables, Some(raviActor))
      candidate <- operations.query(matchesDocument, "candidateMatches", variables, Some(anaActor))
      anonymous <- operations.run(matchesDocument, variables, None)
    } yield {
      other match {
        case StoryOutcome.Refused(code, _, _) => assertEquals(code, "NOT_FOUND", "hidden is the same as missing")
        case unexpected                       => fail(s"a non-owner recruiter must be refused, got $unexpected")
      }
      candidate match {
        case StoryOutcome.Refused(code, _, _) => assert(Set("RECRUITER_REQUIRED", "FORBIDDEN").contains(code), code)
        case unexpected                       => fail(s"a candidate must be refused, got $unexpected")
      }
      assert(anonymous.errors.nonEmpty || anonymous.status >= 400, "an anonymous caller is refused")
      assertEquals(stack.stub.callCount, before, "authorization precedes any provider call")
    }
  }

  storyTest("HEA-02.2 with the provider stopped Ana's search is a keyword result marked degraded, not an error") {
    val variables = Json.obj("query" -> "Scala backend".asJson, "first" -> 10.asJson)
    for {
      _ <- IO.blocking(stack.stub.stop())
      answer <- eventually("degraded keyword search", indexLagDeadline)(
        search(semanticSearchDocument, "semanticJobSearch", variables, anaActor)
          .flatTap(value => IO(leakGuard(hitJobKeys(value), "degraded semanticJobSearch")))
      )(value => hitJobKeys(value).contains(story.j1.key))
    } yield {
      val keys = hitJobKeys(answer)
      assertEquals(keys.headOption, Some(story.j1.key), keys.toString)
      assert(
        modes(answer).nonEmpty && modes(answer).forall(_ == "LEXICAL"),
        s"keyword relevance only: ${modes(answer)}"
      )
      assertEquals(answer.hcursor.get[String]("degradedReason"), Right("PROVIDER_UNAVAILABLE"))
      assert(answer.hcursor.downField("model").focus.exists(_.isNull), "a keyword answer names no embedding model")
    }
  }

  storyTest(
    "HEA-06.2 when Ana's profile changed after its embedding, recommendations are SearchUnavailable, not a list"
  ) {
    val seed = story.ana
    val profile = Json.obj(
      "candidate" -> Json.obj(
        "skills" -> seed.skills.asJson,
        "experienceSummary" -> (seed.experienceSummary + " and event streaming").asJson,
        "residence" -> Json.obj("country" -> "DE".asJson, "city" -> seed.city.asJson),
        "recruiterSearchOptIn" -> true.asJson
      )
    )
    for {
      _ <- operations.succeed("updateMyProfile", Json.obj("profile" -> profile), anaActor)
      outcome <- operations.query(
        recommendationsDocument,
        "recommendedJobs",
        Json.obj("first" -> 10.asJson),
        Some(anaActor)
      )
    } yield outcome match {
      case StoryOutcome.Refused(code, _, typed) =>
        assert(typed, "the refusal is the typed SearchUnavailable member, not a top-level error")
        assert(Set("STALE_EMBEDDING", "MISSING_EMBEDDING").contains(code), code)
      case succeeded => fail(s"a stale profile embedding must not produce a list, got $succeeded")
    }
  }
}
