package com.example.graphQL.cats.acceptance

import cats.effect.IO
import cats.syntax.all.*
import io.circe.Json
import io.circe.syntax.*
import java.util.UUID
import scala.collection.concurrent.TrieMap

/** HEA-14: every query and mutation of the checked contract tried as each role against the real `Main` over HTTP.
  *
  * Expected allow or forbid comes from the SDL descriptions, `docs/api.md` and `docs/use-cases.md`. A cell whose
  * documents do not say who may call is `Unspecified`: it is executed and recorded, never asserted. Search is disabled
  * on this stack (no Atlas search), so a search cell only proves that authorization did not refuse.
  */
class HiringRoleMatrixAcceptanceIntegrationSpec extends HiringStoryStackSuite {
  import HiringRoleMatrixAcceptanceIntegrationSpec.*

  override protected def interviewEnabled: Boolean = true

  private val actors = TrieMap.empty[Role, StoryActor]
  private val fixtures = TrieMap.empty[String, String]

  private def fixture(name: String): String = fixtures(name)

  private def freshJob: IO[String] =
    IO.defer(
      operations.createPublishedJob(
        actors(Role.RecruiterOwner),
        s"Matrix job ${UUID.randomUUID()}",
        "Berlin",
        List("Scala")
      )
    )

  /** A fresh Draft of the owning recruiter, for the lifecycle step that publishes it. */
  private def freshDraft: IO[String] =
    IO.defer(
      operations.createJob(actors(Role.RecruiterOwner), s"Matrix draft ${UUID.randomUUID()}", "Berlin", List("Scala"))
    )

  /** A fresh application on a fresh job of the owning recruiter, advanced to `stage` by the owner. */
  private def freshApplication(stage: Stage): IO[String] =
    IO.defer(for {
      job <- freshJob
      application <- operations.submit(actors(Role.CandidateOwner), job).map {
        case StoryOutcome.Succeeded(value) => operations.idOf(value)
        case other                         => fail(s"fixture submission failed: $other")
      }
      owner = actors(Role.RecruiterOwner)
      _ <- stage.steps.traverse_ { step =>
        operations.applicationAction(step, owner, application).flatMap {
          case StoryOutcome.Succeeded(_) => IO.unit
          case other                     => IO.raiseError(new AssertionError(s"fixture $step failed: $other"))
        }
      }
    } yield application)

  private val randomId: IO[String] = IO.delay(UUID.randomUUID().toString)

  private val later = "2099-01-01T10:00:00Z"
  private val muchLater = "2099-01-01T11:00:00Z"

  private val queries: Vector[Row] = {
    import Expect.*
    def byRole(allowed: Set[Role], otherwise: Expect): Role => Expect =
      role =>
        if (role == Role.Anonymous) Forbid(Set("UNAUTHORIZED"))
        else if (allowed.contains(role)) Allow()
        else otherwise
    def authedAllow: Role => Expect = byRole(Role.values.toSet, Allow())
    val candidates = Set(Role.CandidateOwner, Role.CandidateOther)
    val recruiters = Set(Role.RecruiterOwner, Role.RecruiterOther)
    val unavailable = Allow(Set("VECTOR_SEARCH_UNAVAILABLE"))
    val searchTargets = candidates + Role.Admin
    val unauthorized = Forbid(Set("UNAUTHORIZED"))
    val forbidden = Forbid(Set("FORBIDDEN"))
    val missing = Forbid(Set("NOT_FOUND"))
    Vector(
      Row.query(
        "interviewWorkflow",
        "query Q($id: InterviewWorkflowID!) { interviewWorkflow(workflowId: $id) { id progress } }",
        randomId.map(id => Json.obj("id" -> id.asJson)),
        authedAllow
      ),
      Row.query("me", "query Q { me { id role } }", IO.pure(Json.obj()), authedAllow),
      Row.query(
        "analyticsReport",
        "query Q($from: Instant!, $to: Instant!) { analyticsReport(from: $from, to: $to) { asOf } }",
        IO.pure(Json.obj("from" -> "2026-10-28T00:00:00Z".asJson, "to" -> "2026-10-29T00:00:00Z".asJson)),
        role => if (role == Role.Admin) Allow(Set("ANALYTICS_UNAVAILABLE", "UNAVAILABLE")) else unauthorized
      ),
      Row.query(
        "embeddingCoverage",
        "query Q { embeddingCoverage { asOf } }",
        IO.pure(Json.obj()),
        role => if (role == Role.Admin) Allow(Set("VECTOR_SEARCH_UNAVAILABLE")) else unauthorized
      ),
      Row.query(
        "accountDeletionStatus",
        "query Q($id: DeletionReceiptID!) { accountDeletionStatus(receiptId: $id) }",
        randomId.map(id => Json.obj("id" -> id.asJson)),
        authedAllow
      ),
      Row.query(
        "users",
        "query Q { users(first: 5) { edges { node { id role } } } }",
        IO.pure(Json.obj()),
        role => if (role == Role.Admin) Allow() else unauthorized
      ),
      Row.query(
        "jobs",
        "query Q { jobs(first: 5) { searchId edges { node { id } } } }",
        IO.pure(Json.obj()),
        byRole(searchTargets, Unspecified)
      ),
      Row.query(
        "nearbyJobs",
        "query Q { nearbyJobs(center: {latitude: 52.52, longitude: 13.405}, radiusKm: 50, first: 5) { edges { distanceKm node { id } } } }",
        IO.pure(Json.obj()),
        byRole(searchTargets, Unspecified)
      ),
      Row.query(
        "jobDiscoveryFacets",
        "query Q { jobDiscoveryFacets { __typename ... on UserError { code message } ... on JobDiscoveryFacets { truncated } } }",
        IO.pure(Json.obj()),
        byRole(searchTargets, forbidden)
      ),
      Row.query(
        "semanticJobSearch",
        "query Q { semanticJobSearch(query: \"scala\", first: 5) { __typename ... on UserError { code message } ... on RankedJobResults { searchId } } }",
        IO.pure(Json.obj()),
        byRole(searchTargets, Unspecified)
          .andThen { case Allow(_) => unavailable; case other => other }
      ),
      Row.query(
        "recommendedJobs",
        "query Q { recommendedJobs(first: 5) { __typename ... on UserError { code message } ... on RankedJobResults { searchId } } }",
        IO.pure(Json.obj()),
        role =>
          if (role == Role.Anonymous) unauthorized
          else if (candidates.contains(role)) unavailable
          else Unspecified
      ),
      Row.query(
        "candidateMatches",
        "query Q($jobId: JobID!) { candidateMatches(jobId: $jobId, first: 5) { __typename ... on UserError { code message } ... on RankedCandidateResults { searchId } } }",
        IO.delay(Json.obj("jobId" -> fixture("openJob").asJson)),
        role =>
          if (role == Role.Anonymous) unauthorized
          else if (role == Role.RecruiterOwner) unavailable
          else Unspecified
      ),
      Row.query(
        "job",
        "query Q($id: JobID!) { job(id: $id) { id status } }",
        IO.delay(Json.obj("id" -> fixture("openJob").asJson)),
        authedAllow
      ),
      Row.query(
        "job",
        "query Q($id: JobID!) { job(id: $id) { id status } }",
        IO.delay(Json.obj("id" -> fixture("closedJob").asJson)),
        role =>
          if (role == Role.Anonymous) unauthorized
          else if (role == Role.RecruiterOwner || role == Role.Admin) Allow()
          else missing,
        label = "job (closed)"
      ),
      Row.query(
        "job",
        "query Q($id: JobID!) { job(id: $id) { id status } }",
        IO.delay(Json.obj("id" -> fixture("draftJob").asJson)),
        role =>
          if (role == Role.Anonymous) unauthorized
          else if (role == Role.RecruiterOwner || role == Role.Admin) Allow()
          else missing,
        label = "job (draft)"
      ),
      Row.query(
        "myJobs",
        "query Q { myJobs(first: 5) { edges { node { id } } } }",
        IO.pure(Json.obj()),
        byRole(recruiters + Role.Admin, forbidden)
      ),
      Row.query(
        "myApplications",
        "query Q { myApplications(first: 5) { edges { node { id } } } }",
        IO.pure(Json.obj()),
        role =>
          if (role == Role.Anonymous) unauthorized
          else if (candidates.contains(role)) Allow()
          else if (recruiters.contains(role)) forbidden
          else Unspecified
      ),
      Row.query(
        "jobApplications",
        "query Q($jobId: JobID!) { jobApplications(jobId: $jobId, first: 5) { edges { node { id } } } }",
        IO.delay(Json.obj("jobId" -> fixture("openJob").asJson)),
        role =>
          if (role == Role.Anonymous) unauthorized
          else if (role == Role.RecruiterOwner || role == Role.Admin) Allow()
          else if (role == Role.RecruiterOther) missing
          else forbidden
      ),
      Row.query(
        "applicationHistory",
        "query Q($id: ApplicationID!) { applicationHistory(applicationId: $id, first: 5) { edges { node { id } } } }",
        IO.delay(Json.obj("id" -> fixture("application").asJson)),
        // Another candidate's application, or one on another recruiter's job, is a missing one.
        byRole(Set(Role.CandidateOwner, Role.RecruiterOwner, Role.Admin), missing)
      )
    )
  }

  private val mutations: Vector[Row] = {
    import Expect.*
    val candidates = Set(Role.CandidateOwner, Role.CandidateOther)
    val managers = Set(Role.RecruiterOwner, Role.Admin)
    val unauthorized = Forbid(Set("UNAUTHORIZED"))
    val forbidden = Forbid(Set("FORBIDDEN"))
    val missing = Forbid(Set("NOT_FOUND"))
    def whoever(allowed: Set[Role], tolerated: Set[String] = Set.empty): Role => Expect = role =>
      if (role == Role.Anonymous) unauthorized
      else if (allowed.contains(role)) Allow(tolerated)
      else forbidden
    // A role with the right may only act on what it manages: another recruiter's job or application is a missing one,
    // and a Candidate has no right to the action at all (the application actions answer another candidate as a missing application, because the id is what is probed).
    def managing(candidateOwnAnswer: Expect = forbidden, candidateOtherAnswer: Expect = forbidden): Role => Expect = {
      case Role.Anonymous                   => unauthorized
      case Role.RecruiterOwner | Role.Admin => Allow()
      case Role.RecruiterOther              => missing
      case Role.CandidateOwner              => candidateOwnAnswer
      case Role.CandidateOther              => candidateOtherAnswer
    }
    val unspecifiedExceptAnonymous: Role => Expect = role => if (role == Role.Anonymous) unauthorized else Unspecified
    // docs/api.md: a workflow the caller does not take part in is indistinguishable from a missing one (NOT_FOUND).
    val missingWorkflow: Role => Expect = role => if (role == Role.Anonymous) unauthorized else Forbid(Set("NOT_FOUND"))
    def interview(name: String): Row =
      Row.mutation(
        name,
        randomId.map(id => Json.obj("workflowId" -> id.asJson, "expectedRevision" -> 1.asJson)),
        missingWorkflow
      )
    def statusRow(
        name: String,
        stage: Stage,
        extra: Json = Json.obj(),
        expect: Role => Expect = managing(candidateOtherAnswer = missing)
    ): Row =
      Row.mutation(
        name,
        freshApplication(stage).map(id => Json.obj("applicationId" -> id.asJson).deepMerge(extra)),
        expect
      )
    Vector(
      Row.mutation(
        "scheduleInterview",
        freshApplication(Stage.Accepted).map(id =>
          Json.obj("applicationId" -> id.asJson, "startsAt" -> later.asJson, "endsAt" -> muchLater.asJson)
        ),
        role =>
          if (role == Role.Anonymous) unauthorized
          else if (role == Role.RecruiterOwner) Allow()
          else Unspecified
      ),
      Row.mutation(
        "repairInterviewWorkflow",
        randomId.map(id => Json.obj("workflowId" -> id.asJson, "expectedRevision" -> 1.asJson)),
        role => if (role != Role.Admin) unauthorized else Unspecified
      ),
      interview("cancelInterview"),
      interview("requestInterviewReschedule"),
      interview("dismissInterviewRescheduleRequest"),
      Row.mutation(
        "proposeInterviewReschedule",
        randomId.map(id =>
          Json.obj(
            "workflowId" -> id.asJson,
            "expectedRevision" -> 1.asJson,
            "startsAt" -> later.asJson,
            "endsAt" -> muchLater.asJson
          )
        ),
        missingWorkflow
      ),
      interview("withdrawInterviewReschedule"),
      interview("acceptInterviewReschedule"),
      interview("declineInterviewReschedule"),
      Row.mutation(
        "submitApplication",
        freshJob.map(id => Json.obj("jobId" -> id.asJson)),
        whoever(candidates)
      ),
      Row.mutation(
        "createJob",
        IO.pure(StoryOperationsInputs.job("Matrix created job")),
        whoever(managers + Role.RecruiterOther)
      ),
      Row.mutation(
        "updateJob",
        freshJob.map(id => Json.obj("id" -> id.asJson, "job" -> StoryOperationsInputs.job("Matrix updated job"))),
        managing()
      ),
      Row.mutation(
        "publishJob",
        freshDraft.map(id => Json.obj("jobId" -> id.asJson)),
        managing()
      ),
      Row.mutation("closeJob", freshJob.map(id => Json.obj("jobId" -> id.asJson)), managing()),
      statusRow("acceptApplication", Stage.Created),
      statusRow("moveApplicationToInterview", Stage.Accepted),
      statusRow("hireApplication", Stage.Interview),
      statusRow("rejectApplication", Stage.Created, Json.obj("feedback" -> "Matrix feedback".asJson)),
      statusRow(
        "declineApplication",
        Stage.Created,
        Json.obj("reason" -> "Matrix reason".asJson),
        // The owning candidate withdraws; the owning recruiter and Admin decline as before.
        managing(candidateOwnAnswer = Allow(), candidateOtherAnswer = missing)
      ),
      Row.publicMutation("signUp"),
      Row.publicMutation("login"),
      Row.mutation(
        "updateMyProfile",
        IO.pure(Json.obj()),
        role =>
          if (role == Role.Anonymous) unauthorized
          else if (role == Role.Admin) Forbid(Set("PROFILE_UNSUPPORTED_FOR_ROLE"))
          else Allow(),
        inputFor = Some(role => IO.pure(Json.obj("profile" -> StoryOperationsInputs.profile(role))))
      ),
      Row.mutation(
        "deleteMyAccount",
        IO.pure(Json.obj()),
        // Deletion also erases analytics data; without the analytics pipeline it answers ANALYTICS_UNAVAILABLE after the
        // authorization decision, so the effect itself is BLOCKED on this stack.
        role => if (role == Role.Anonymous) unauthorized else Allow(Set("ANALYTICS_UNAVAILABLE")),
        throwawayAccount = true
      ),
      Row.mutation(
        "recordJobView",
        IO.pure(Json.obj()),
        role =>
          if (role == Role.Anonymous) unauthorized
          else if (candidates.contains(role)) Allow()
          else Unspecified,
        inputFor =
          Some(_ => randomId.map(event => Json.obj("eventId" -> event.asJson, "jobId" -> fixture("openJob").asJson)))
      ),
      Row.mutation(
        "recordSearchResultClick",
        randomId.flatMap(event =>
          randomId.flatMap(search =>
            randomId.map(result =>
              Json.obj("eventId" -> event.asJson, "searchId" -> search.asJson, "resultId" -> result.asJson)
            )
          )
        ),
        unspecifiedExceptAnonymous
      )
    )
  }

  private def actorFor(role: Role, row: Row): IO[Option[StoryActor]] =
    if (role == Role.Anonymous) IO.pure(None)
    else if (row.throwawayAccount && role == Role.Admin) IO.pure(Some(actors(Role.Admin)))
    else if (row.throwawayAccount) {
      val name = s"Throwaway ${UUID.randomUUID()}"
      (if (Set(Role.RecruiterOwner, Role.RecruiterOther).contains(role)) operations.signUpRecruiter(name, "Matrix Org")
       else operations.signUpCandidate(name, List("Scala"))).map(Some(_))
    } else IO.pure(Some(actors(role)))

  private def execute(row: Row, role: Role): IO[Cell] = {
    // The singleton Admin must survive the run, and public operations are exercised anonymously only.
    val notExecuted = (row.throwawayAccount && role == Role.Admin) || (row.isPublic && role != Role.Anonymous)
    if (notExecuted) IO.pure(Cell(row, role, None, None, Expect.Unspecified))
    else
      for {
        actor <- actorFor(role, row)
        variables <-
          if (row.isPublic) publicInput(row.field) else row.inputFor.fold(row.variables)(_(role))
        response <- row.kind match {
          case Kind.Query    => operations.run(row.document, variables, actor)
          case Kind.Mutation => operations.send(row.field, variables, actor)
        }
      } yield Cell(row, role, Some(StoryOutcome.of(response, row.field)), Some(response), row.expect(role))
  }

  private def publicInput(field: String): IO[Json] =
    if (field == "login")
      IO.pure(
        Json.obj("name" -> HiringStoryStack.AdminName.asJson, "password" -> HiringStoryStack.AdminPassword.asJson)
      )
    else
      IO.pure(
        Json.obj(
          "name" -> s"Matrix Public ${UUID.randomUUID()}".asJson,
          "role" -> "CANDIDATE".asJson,
          "password" -> "matrix-public-password-1".asJson,
          "profile" -> StoryOperationsInputs.profile(Role.CandidateOwner)
        )
      )

  private def verdict(cell: Cell): Option[String] = (cell.outcome, cell.expect) match {
    case (None, _)                                                  => None
    case (_, Expect.Unspecified)                                    => None
    case (Some(StoryOutcome.Succeeded(_)), Expect.Allow(_))         => None
    case (Some(StoryOutcome.Refused(code, _, _)), Expect.Allow(ok)) =>
      Option.when(!ok.contains(code))(s"expected allow but refused with $code")
    case (Some(StoryOutcome.Succeeded(_)), Expect.Forbid(codes))        => Some(s"expected one of $codes but succeeded")
    case (Some(StoryOutcome.Refused(code, _, _)), Expect.Forbid(codes)) =>
      Option.when(!codes.contains(code))(s"expected one of $codes but was $code")
  }

  private val leaks =
    List("Exception", "Caused by", "\tat ", "com.example", "mongodb", "stack trace", "java.", "scala.")
  private val uuidPattern = "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}".r

  /** A refusal message is fixed text: no stack, driver text, class names or identifiers of the targeted resource. */
  private def sanitation(cell: Cell): Option[String] = (cell.outcome, cell.response) match {
    case (Some(StoryOutcome.Refused(_, _, _)), Some(response)) =>
      leaks
        .find(response.raw.contains)
        .map(marker => s"refusal body contains '$marker'")
        .orElse(uuidPattern.findFirstIn(response.raw).map(_ => "refusal body contains an identifier"))
    case _ => None
  }

  private def render(cell: Cell): String = cell.outcome match {
    case None                                   => "n/a"
    case Some(StoryOutcome.Succeeded(_))        => "ok"
    case Some(StoryOutcome.Refused(code, _, _)) => code
  }

  private def runMatrix(rows: Vector[Row], title: String): IO[Unit] =
    rows.traverse(row => Role.values.toVector.traverse(role => execute(row, role))).map { grid =>
      val header = Role.values.map(_.toString).mkString(" | ")
      println(s"HEA-14 $title matrix (roles: $header)")
      grid.foreach(cells => println(s"  ${cells.head.row.label}: ${cells.map(render).mkString(" | ")}"))
      val unspecified = grid.flatten.count(_.expect == Expect.Unspecified)
      val executed = grid.flatten.count(_.outcome.nonEmpty)
      println(s"HEA-14 $title: $executed cells executed, $unspecified without a documented expectation")
      val failures =
        grid.flatten.flatMap(cell => verdict(cell).map(message => s"${cell.row.label} as ${cell.role}: $message"))
      val leaksFound =
        grid.flatten.flatMap(cell => sanitation(cell).map(message => s"${cell.row.label} as ${cell.role}: $message"))
      assertEquals(failures, Vector.empty[String], "allow or forbid differs from the documented contract")
      assertEquals(leaksFound, Vector.empty[String], "a refusal is not sanitized")
    }

  test("HEA-14.0 fixtures: one account per role, an open, a closed and a draft job and one application") {
    for {
      admin <- adminActor
      owner <- operations.signUpRecruiter("Matrix Owner", "Owner Org")
      other <- operations.signUpRecruiter("Matrix Other Recruiter", "Other Org")
      candidateOwner <- operations.signUpCandidate("Matrix Candidate", List("Scala"))
      candidateOther <- operations.signUpCandidate("Matrix Other Candidate", List("Scala"))
      open <- operations.createPublishedJob(owner, "Matrix open job", "Berlin", List("Scala"))
      draft <- operations.createJob(owner, "Matrix draft job", "Berlin", List("Scala"))
      closing <- operations.createPublishedJob(owner, "Matrix closed job", "Berlin", List("Scala"))
      _ <- operations.succeed("closeJob", Json.obj("jobId" -> closing.asJson), owner)
      application <- operations.submitOk(candidateOwner, open)
    } yield {
      actors.put(Role.Admin, admin)
      actors.put(Role.RecruiterOwner, owner)
      actors.put(Role.RecruiterOther, other)
      actors.put(Role.CandidateOwner, candidateOwner)
      actors.put(Role.CandidateOther, candidateOther)
      fixtures.put("openJob", open)
      fixtures.put("closedJob", closing)
      fixtures.put("draftJob", draft)
      fixtures.put("application", application)
      assertEquals(actors.size, 5)
    }
  }

  test("HEA-14.1 the matrix covers every query and mutation of the checked contract") {
    val sdl = scala.io.Source.fromResource("graphql/hiring.graphql").mkString
    def fieldsOf(typeName: String): Set[String] = {
      val body = sdl.linesIterator.dropWhile(_ != s"type $typeName {").drop(1).takeWhile(_ != "}").toList
      body.flatMap(line => "^  ([a-zA-Z]\\w*)[(:]".r.findFirstMatchIn(line).map(_.group(1))).toSet
    }
    val queryFields = fieldsOf("Query")
    val mutationFields = fieldsOf("Mutation")
    assert(queryFields.nonEmpty && mutationFields.nonEmpty)
    assertEquals(queries.map(_.field).toSet, queryFields)
    assertEquals(mutations.map(_.field).toSet, mutationFields)
  }

  test("HEA-14.2 every query: allow or forbid per role matches the documented contract and refusals are sanitized") {
    runMatrix(queries, "queries")
  }

  test("HEA-14.3 every mutation: allow or forbid per role matches the documented contract and refusals are sanitized") {
    runMatrix(mutations, "mutations")
  }

  test("HEA-14.4 a garbage and a tampered token are rejected before execution with a sanitized 401") {
    // docs/api.md does not state the status for an invalid token; the observed contract is an HTTP-level 401.
    val valid = actors(Role.CandidateOwner).token
    val tampered = valid.dropRight(2) + (if (valid.endsWith("AA")) "BB" else "AA")
    List("not-a-token", tampered)
      .traverse { token =>
        // The API answers an invalid token with 401 before reading the body and drops the connection, so each request
        // gets its own connection instead of the pooled one (see GraphQLClient.executeOnFreshConnection).
        stack.running.client.executeOnFreshConnection("query Q { me { id } }", Json.obj(), Some(token))
      }
      .map(_.foreach { response =>
        assertEquals(response.status, 401)
        assertEquals(response.data("me"), Json.Null)
        assert(!leaks.exists(response.raw.contains), clues(response.raw))
        assert(uuidPattern.findFirstIn(response.raw).isEmpty, clues(response.raw))
      })
  }

  test("HEA-14.5 a real interview workflow refuses the roles the documented table excludes") {
    // docs/api.md authorization table. Only refusals are exercised, so the workflow itself stays untouched.
    val refusals: List[(String, Role, String)] = List(
      ("cancelInterview", Role.CandidateOther, "NOT_FOUND"),
      ("cancelInterview", Role.RecruiterOther, "NOT_FOUND"),
      ("requestInterviewReschedule", Role.RecruiterOwner, "FORBIDDEN"),
      ("requestInterviewReschedule", Role.Admin, "FORBIDDEN"),
      ("requestInterviewReschedule", Role.CandidateOther, "NOT_FOUND"),
      ("dismissInterviewRescheduleRequest", Role.CandidateOwner, "FORBIDDEN"),
      ("dismissInterviewRescheduleRequest", Role.RecruiterOther, "NOT_FOUND"),
      ("withdrawInterviewReschedule", Role.CandidateOwner, "FORBIDDEN"),
      ("acceptInterviewReschedule", Role.RecruiterOwner, "FORBIDDEN"),
      ("acceptInterviewReschedule", Role.Admin, "FORBIDDEN"),
      ("declineInterviewReschedule", Role.RecruiterOwner, "FORBIDDEN"),
      ("declineInterviewReschedule", Role.RecruiterOther, "NOT_FOUND")
    )
    for {
      application <- freshApplication(Stage.Accepted)
      scheduled <- operations.succeed(
        "scheduleInterview",
        Json.obj("applicationId" -> application.asJson, "startsAt" -> later.asJson, "endsAt" -> muchLater.asJson),
        actors(Role.RecruiterOwner)
      )
      workflow = operations.idOf(scheduled)
      revision = scheduled.hcursor.get[Long]("revision").fold(e => fail(e.message), identity)
      results <- refusals.traverse { case (name, role, expected) =>
        val input = Json.obj("workflowId" -> workflow.asJson, "expectedRevision" -> revision.asJson)
        val withSlot =
          if (name == "proposeInterviewReschedule")
            input.deepMerge(Json.obj("startsAt" -> later.asJson, "endsAt" -> muchLater.asJson))
          else input
        operations.mutate(name, withSlot, actors(role)).map(outcome => (name, role, expected, outcome))
      }
    } yield {
      val mismatches = results.collect {
        case (name, role, expected, outcome)
            if !outcome
              .isInstanceOf[StoryOutcome.Refused] || outcome.asInstanceOf[StoryOutcome.Refused].code != expected =>
          s"$name as $role: expected $expected but was $outcome"
      }
      assertEquals(mismatches, List.empty[String])
    }
  }
}

object HiringRoleMatrixAcceptanceIntegrationSpec {
  enum Role { case CandidateOwner, CandidateOther, RecruiterOwner, RecruiterOther, Admin, Anonymous }
  enum Kind { case Query, Mutation }
  enum Stage(val steps: List[String]) {
    case Created extends Stage(Nil)
    case Accepted extends Stage(List("acceptApplication"))
    case Interview extends Stage(List("acceptApplication", "moveApplicationToInterview"))
  }

  enum Expect {

    /** Authorization passes: success, or one of the listed business refusals. */
    case Allow(tolerated: Set[String] = Set.empty)

    /** Authorization refuses with one of the listed codes. */
    case Forbid(codes: Set[String])

    /** The documents do not say; executed and recorded, not asserted. */
    case Unspecified
  }

  final case class Row(
      field: String,
      kind: Kind,
      document: String,
      variables: IO[Json],
      expect: Role => Expect,
      label: String,
      inputFor: Option[Role => IO[Json]] = None,
      throwawayAccount: Boolean = false,
      isPublic: Boolean = false
  )

  object Row {
    def query(
        field: String,
        document: String,
        variables: IO[Json],
        expect: Role => Expect,
        label: String = ""
    ): Row = Row(field, Kind.Query, document, variables, expect, if (label.isEmpty) field else label)

    def mutation(
        field: String,
        variables: IO[Json],
        expect: Role => Expect,
        inputFor: Option[Role => IO[Json]] = None,
        throwawayAccount: Boolean = false
    ): Row =
      Row(field, Kind.Mutation, StoryDocuments.mutation(field), variables, expect, field, inputFor, throwawayAccount)

    /** `signUp` and `login` are public: only the anonymous caller is exercised. */
    def publicMutation(field: String): Row =
      Row(
        field,
        Kind.Mutation,
        StoryDocuments.mutation(field),
        IO.pure(Json.obj()),
        _ => Expect.Allow(),
        field,
        isPublic = true
      )
  }

  final case class Cell(
      row: Row,
      role: Role,
      outcome: Option[StoryOutcome],
      response: Option[GraphQLResponse],
      expect: Expect
  )

  object StoryOperationsInputs {
    def job(title: String): Json = Json.obj(
      "title" -> title.asJson,
      "description" -> s"$title: a fictional posting".asJson,
      "requirements" -> List("Two years of relevant experience").asJson,
      "skills" -> List("Scala").asJson,
      "country" -> "DE".asJson,
      "city" -> "Berlin".asJson,
      "remote" -> false.asJson
    )

    /** A complete profile of the variant that matches the role (the Admin is sent a candidate one on purpose). */
    def profile(role: Role): Json = role match {
      case Role.RecruiterOwner | Role.RecruiterOther =>
        Json.obj("recruiter" -> Json.obj("organizationName" -> "Matrix Org".asJson))
      case _ =>
        Json.obj("candidate" -> Json.obj("skills" -> List("Scala").asJson, "experienceSummary" -> "Five years".asJson))
    }
  }
}
