package com.example.graphQL.cats.api.graphql

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.example.graphQL.cats.api.admission.InterviewActionRateLimiter
import com.example.graphQL.cats.config.InterviewActionRateLimitConfig
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.{
  ActorContext,
  HiringReadService,
  ProbeResult,
  RepositoryError,
  ServiceFixtures,
  TestHiringServices
}
import com.example.graphQL.cats.service.ServiceFixtures.{InMemoryApplications, InMemoryJobs, InMemoryUsers}
import com.example.graphQL.cats.service.application.{InMemoryInterviewWorkflowRepository, InterviewSchedulingService}
import io.circe.Json
import java.time.Instant
import java.util.UUID
import munit.CatsEffectSuite
import scala.concurrent.duration.*

/** DHW-15, DHW-19, DHW-31 and DHW-34 through GraphQL: the seven cancel and reschedule mutations reach the real
  * `InterviewSchedulingService`, the authorization matrix and sanitized error codes hold at the API boundary, and the
  * per-actor allowance is taken before any storage work.
  */
final class HiringGraphQLInterviewActionSpec extends CatsEffectSuite {
  import ServiceFixtures.*

  private val otherCandidateId = UserId(new UUID(0L, 101L))
  private val otherRecruiterId = UserId(new UUID(0L, 102L))
  private val workflowId = InterviewWorkflowId(new UUID(0L, 20L))
  private val interval = InterviewInterval(now.plusSeconds(172800), now.plusSeconds(176400))
  private val replacement = InterviewInterval(now.plusSeconds(259200), now.plusSeconds(262800))
  private val bothNotified = Set(InterviewParticipant.Candidate, InterviewParticipant.Recruiter)
  private val revision = 3L

  private val candidateActor = ActorContext(candidateId, UserRole.Candidate)
  private val recruiterActor = ActorContext(recruiterId, UserRole.Recruiter)
  private val adminActor = ActorContext(adminId, UserRole.Admin)
  private val otherCandidateActor = ActorContext(otherCandidateId, UserRole.Candidate)
  private val otherRecruiterActor = ActorContext(otherRecruiterId, UserRole.Recruiter)

  private def workflowIn(
      phase: InterviewWorkflowPhase,
      customize: InterviewWorkflow => InterviewWorkflow = identity
  ): InterviewWorkflow =
    customize(
      InterviewWorkflow(
        workflowId,
        applicationId,
        candidateId,
        recruiterId,
        interval,
        now.plusSeconds(300),
        new UUID(0L, 21L),
        revision,
        phase,
        bothNotified,
        recruiterId
      )
    )

  private def proposalOpen(expiresAt: Instant = now.plusSeconds(172800)): InterviewWorkflow =
    workflowIn(
      InterviewWorkflowPhase.ProposalPending,
      _.copy(proposal = Some(InterviewRescheduleProposal(replacement, recruiterId, expiresAt)))
    )

  private def completed: InterviewWorkflow = workflowIn(InterviewWorkflowPhase.Completed)

  private enum Action(val operation: String, val field: String) {
    case Cancel extends Action("CancelInterview", "cancelInterview")
    case Request extends Action("RequestInterviewReschedule", "requestInterviewReschedule")
    case Dismiss extends Action("DismissInterviewRescheduleRequest", "dismissInterviewRescheduleRequest")
    case Propose extends Action("ProposeInterviewReschedule", "proposeInterviewReschedule")
    case Withdraw extends Action("WithdrawInterviewReschedule", "withdrawInterviewReschedule")
    case Accept extends Action("AcceptInterviewReschedule", "acceptInterviewReschedule")
    case Decline extends Action("DeclineInterviewReschedule", "declineInterviewReschedule")
  }

  private def startState(action: Action): InterviewWorkflow = action match {
    case Action.Dismiss => workflowIn(InterviewWorkflowPhase.Completed, _.copy(rescheduleRequestedAt = Some(now)))
    case Action.Withdraw | Action.Accept | Action.Decline => proposalOpen()
    case Action.Cancel | Action.Request | Action.Propose  => completed
  }

  private def operations: IO[String] = IO.blocking {
    val stream = Option(getClass.getResourceAsStream("/graphql/interview-cancellation.graphql"))
      .getOrElse(throw new IllegalArgumentException("Missing contract fixture"))
    try new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8)
    finally stream.close()
  }

  private def variables(
      action: Action,
      id: UUID = workflowId.value,
      expectedRevision: Long = revision,
      key: UUID = UUID.randomUUID(),
      proposed: InterviewInterval = replacement
  ): Json = {
    val base = List(
      "workflowId" -> Json.fromString(id.toString),
      "expectedRevision" -> Json.fromLong(expectedRevision),
      "idempotencyKey" -> Json.fromString(key.toString)
    )
    val input =
      if (action == Action.Propose)
        base ++ List(
          "startsAt" -> Json.fromString(proposed.startsAt.toString),
          "endsAt" -> Json.fromString(proposed.endsAt.toString)
        )
      else base
    Json.obj("input" -> Json.obj(input*))
  }

  private final case class Arrangement(
      services: HiringGraphQLServices,
      repository: InMemoryInterviewWorkflowRepository
  ) {
    def stored: IO[InterviewWorkflow] = repository.store.get.map(_.workflows(workflowId))
    def commands: IO[Int] = repository.store.get.map(_.commands.size)
    def status: IO[ApplicationStatus] = repository.applicationStatus.get
  }

  private def arranged(
      workflow: InterviewWorkflow,
      clock: Instant = now,
      status: ApplicationStatus = ApplicationStatus.Interview
  ): IO[Arrangement] =
    for {
      usersRef <- Ref.of[IO, Map[UserId, User]](
        Map(
          candidateId -> candidate,
          recruiterId -> recruiter,
          adminId -> admin,
          otherCandidateId -> candidate.copy(id = otherCandidateId, name = "Other candidate"),
          otherRecruiterId -> recruiter.copy(id = otherRecruiterId, name = "Other recruiter")
        )
      )
      jobsRef <- Ref.of[IO, Map[JobId, Job]](Map(jobId -> openJob))
      applicationsRef <- Ref.of[IO, Map[ApplicationId, Application]](
        Map(applicationId -> createdApplication.copy(status = status))
      )
      events <- Ref.of[IO, Vector[ApplicationEvent]](Vector.empty)
      errors <- Ref.of[IO, Option[RepositoryError]](None)
      repository <- InMemoryInterviewWorkflowRepository.create(workflow, status)
      users = new InMemoryUsers(usersRef, Some(ServiceFixtures.userRelations(usersRef, jobsRef, applicationsRef)))
      jobs = new InMemoryJobs(
        jobsRef,
        relationLookup = Some(ServiceFixtures.jobRelations(usersRef, jobsRef, applicationsRef))
      )
      applications = new InMemoryApplications(
        applicationsRef,
        events,
        errors,
        jobLookup = id => jobsRef.get.map(_.get(id))
      )
      service = new InterviewSchedulingService(users, jobs, applications, repository, 5.minutes, IO.pure(clock))
    } yield Arrangement(
      HiringGraphQLServices(
        HiringReadService(users, jobs, applications),
        TestHiringServices.job(users, jobs),
        TestHiringServices.applications(users, jobs, applications),
        TestGraphQLSupport.cursorKey,
        TestGraphQLSupport.accountService,
        TestGraphQLSupport.interactions,
        TestGraphQLSupport.searchSessions,
        interviewScheduling = Some(service)
      ),
      repository
    )

  private def unlimited: UserId => IO[Either[InterviewActionRateLimiter.RateLimited, Unit]] = _ => IO.pure(Right(()))

  private def run(
      a: Arrangement,
      query: String,
      operationName: Option[String],
      vars: Json,
      actor: Option[ActorContext],
      allowance: UserId => IO[Either[InterviewActionRateLimiter.RateLimited, Unit]] = unlimited,
      hiringReady: IO[ProbeResult] = IO.pure(ProbeResult.Ready)
  ): IO[Either[HiringGraphQLSchema.Failure, Json]] =
    IO.fromEither(
      Json
        .obj(
          "query" -> Json.fromString(query),
          "variables" -> vars,
          "operationName" -> operationName.fold(Json.Null)(Json.fromString)
        )
        .as[GraphQLRequest]
        .left
        .map(error => new IllegalArgumentException("Invalid test request", error))
    ).flatMap(request =>
      TestGraphQLSupport
        .context(IO.pure(ProbeResult.Ready), actor, a.services, hiringReady, interviewActionRateLimit = allowance)
        .use(TestGraphQLSupport.parseAndExecute(request, _))
    )

  private def act(
      a: Arrangement,
      action: Action,
      actor: Option[ActorContext],
      vars: Json,
      allowance: UserId => IO[Either[InterviewActionRateLimiter.RateLimited, Unit]] = unlimited,
      hiringReady: IO[ProbeResult] = IO.pure(ProbeResult.Ready)
  ): IO[Json] =
    operations.flatMap(query =>
      run(a, query, Some(action.operation), vars, actor, allowance, hiringReady).map(
        _.fold(failure => fail(failure.toString), identity)
      )
    )

  private def payload(json: Json, action: Action): Json =
    json.hcursor.downField("data").downField(action.field).focus.get
  private def code(json: Json, action: Action): Option[String] =
    payload(json, action).hcursor.get[String]("code").toOption
  private def errorCode(json: Json): Option[String] =
    json.hcursor.downField("errors").downArray.downField("extensions").get[String]("code").toOption

  private enum Expectation { case Allowed, Forbidden, NotFound }
  import Expectation.*

  private val matrix: List[(Action, List[(ActorContext, String, Expectation)])] = {
    def row(action: Action, own: Expectation, recruiter: Expectation, adminResult: Expectation) =
      action -> List(
        (candidateActor, "the application's candidate", own),
        (recruiterActor, "the owning recruiter", recruiter),
        (adminActor, "Admin", adminResult),
        (otherCandidateActor, "another candidate", NotFound),
        (otherRecruiterActor, "a non-owning recruiter", NotFound)
      )
    List(
      row(Action.Cancel, Allowed, Allowed, Allowed),
      row(Action.Request, Allowed, Forbidden, Forbidden),
      row(Action.Dismiss, Forbidden, Allowed, Allowed),
      row(Action.Propose, Forbidden, Allowed, Allowed),
      row(Action.Withdraw, Forbidden, Allowed, Allowed),
      row(Action.Accept, Allowed, Forbidden, Forbidden),
      row(Action.Decline, Allowed, Forbidden, Forbidden)
    )
  }

  for {
    (action, actors) <- matrix
    (actor, who, expectation) <- actors
  } test(s"DHW-15 ${action.field} through GraphQL by $who is $expectation") {
    arranged(startState(action)).flatMap { a =>
      for {
        json <- act(a, action, Some(actor), variables(action))
        after <- a.stored
        commands <- a.commands
        status <- a.status
      } yield expectation match {
        case Allowed =>
          assertEquals(payload(json, action).hcursor.get[String]("id"), Right(workflowId.value.toString), clue(json))
          assertEquals(payload(json, action).hcursor.get[Long]("revision"), Right(revision + 1), clue(json))
          assert(json.hcursor.downField("errors").focus.isEmpty, clue(json))
        case Forbidden =>
          assertEquals(code(json, action), Some("FORBIDDEN"), clue(json))
          assertEquals(payload(json, action).hcursor.get[String]("message"), Right("Forbidden"))
          assertEquals((after, commands, status), (startState(action), 0, ApplicationStatus.Interview))
        case NotFound =>
          assertEquals(code(json, action), Some("NOT_FOUND"), clue(json))
          assertEquals((after, commands, status), (startState(action), 0, ApplicationStatus.Interview))
      }
    }
  }

  test("DHW-15 an existing workflow and a missing one are indistinguishable to a stranger for every mutation") {
    Action.values.toList.traverse_ { action =>
      arranged(startState(action)).flatMap { a =>
        List(otherCandidateActor, otherRecruiterActor).traverse_ { stranger =>
          for {
            hidden <- act(a, action, Some(stranger), variables(action))
            missing <- act(a, action, Some(stranger), variables(action, id = new UUID(0L, 999L)))
            ownerMissing <- act(a, action, Some(candidateActor), variables(action, id = new UUID(0L, 999L)))
          } yield {
            assertEquals(hidden, missing, action.field)
            assertEquals(missing, ownerMissing, action.field)
            assertEquals(code(hidden, action), Some("NOT_FOUND"))
          }
        }
      }
    }
  }

  test("DHW-15 an unauthenticated request reaches no workflow and is refused with the shared unauthorized code") {
    Action.values.toList.traverse_ { action =>
      arranged(startState(action)).flatMap { a =>
        for {
          json <- act(a, action, None, variables(action))
          after <- a.stored
          commands <- a.commands
        } yield {
          assertEquals(errorCode(json), Some("UNAUTHORIZED"), clue(json))
          assertEquals((after, commands), (startState(action), 0))
        }
      }
    }
  }

  test("DHW-31 inputs carry no actor, role, proposer or initiator and cancel has no text field") {
    val action = HiringGraphQLSchema.schema.inputTypes("InterviewActionInput")
    val propose = HiringGraphQLSchema.schema.inputTypes("ProposeInterviewRescheduleInput")
    def names(tpe: sangria.schema.InputType[?]): Set[String] = tpe match {
      case input: sangria.schema.InputObjectType[?] => input.fields.map(_.name).toSet
      case other                                    => fail(s"unexpected input type $other")
    }
    assertEquals(names(action), Set("workflowId", "expectedRevision", "idempotencyKey"))
    assertEquals(names(propose), Set("workflowId", "expectedRevision", "startsAt", "endsAt", "idempotencyKey"))
  }

  test("DHW-31 a client-supplied actor, role or text is rejected before any resolver runs") {
    arranged(completed).flatMap { a =>
      List("actorId", "role", "proposedBy", "initiator", "feedback", "reason").traverse_ { extra =>
        val vars = variables(Action.Cancel).mapObject(
          _.mapValues(_.mapObject(_.add(extra, Json.fromString("injected"))))
        )
        for {
          result <- operations.flatMap(query =>
            run(a, query, Some(Action.Cancel.operation), vars, Some(candidateActor))
          )
          after <- a.stored
          commands <- a.commands
        } yield {
          assert(result.isLeft || result.exists(_.hcursor.downField("errors").focus.nonEmpty), clue((extra, result)))
          assertEquals((after, commands), (completed, 0))
        }
      }
    }
  }

  test("DHW-31 InterviewWorkflow exposes the specified fields and no participant, proposer or initiator identity") {
    val fields = HiringGraphQLSchema.schema.outputTypes("InterviewWorkflow") match {
      case objectType: sangria.schema.ObjectType[?, ?] => objectType.fields.map(_.name).toSet
      case other                                       => fail(s"unexpected type $other")
    }
    assertEquals(
      fields,
      Set(
        "id",
        "applicationId",
        "startsAt",
        "endsAt",
        "revision",
        "progress",
        "notifiedParticipants",
        "pendingStartsAt",
        "pendingEndsAt",
        "cancelledAt",
        "proposedStartsAt",
        "proposedEndsAt",
        "proposalExpiresAt",
        "rescheduleRequested"
      )
    )
  }

  test("DHW-31 the lifecycle fields read back through the query for participants and Admin, hidden from strangers") {
    val withProposal = proposalOpen()
    val read = (a: Arrangement, actor: ActorContext) =>
      operations.flatMap(query =>
        run(
          a,
          query,
          Some("InterviewLifecycle"),
          Json.obj("workflowId" -> Json.fromString(workflowId.value.toString)),
          Some(actor)
        ).map(_.fold(failure => fail(failure.toString), identity))
      )
    arranged(withProposal).flatMap { a =>
      for {
        asCandidate <- read(a, candidateActor)
        asRecruiter <- read(a, recruiterActor)
        asAdmin <- read(a, adminActor)
        asStranger <- read(a, otherCandidateActor)
        asOtherRecruiter <- read(a, otherRecruiterActor)
      } yield {
        List(asCandidate, asRecruiter, asAdmin).foreach { json =>
          val workflow = json.hcursor.downField("data").downField("interviewWorkflow")
          assertEquals(workflow.get[String]("progress"), Right("ProposalPending"), clue(json))
          assertEquals(workflow.get[String]("proposedStartsAt"), Right(replacement.startsAt.toString))
          assertEquals(workflow.get[String]("proposedEndsAt"), Right(replacement.endsAt.toString))
          assertEquals(workflow.get[String]("proposalExpiresAt"), Right(withProposal.proposal.get.expiresAt.toString))
          assertEquals(workflow.get[Boolean]("rescheduleRequested"), Right(false))
          assertEquals(workflow.downField("cancelledAt").focus, Some(Json.Null))
          assertEquals(workflow.downField("pendingStartsAt").focus, Some(Json.Null))
        }
        assertEquals(errorCode(asStranger), Some("NOT_FOUND"), clue(asStranger))
        assertEquals(errorCode(asOtherRecruiter), Some("NOT_FOUND"), clue(asOtherRecruiter))
        assertEquals(asStranger.hcursor.downField("data").focus, Some(Json.Null))
      }
    }
  }

  test("DHW-31 a request, a cancel and an accepted proposal show their state through the mutation payloads") {
    for {
      a <- arranged(completed)
      requested <- act(a, Action.Request, Some(candidateActor), variables(Action.Request))
      b <- arranged(proposalOpen())
      accepted <- act(b, Action.Accept, Some(candidateActor), variables(Action.Accept))
      c <- arranged(completed)
      cancelled <- act(c, Action.Cancel, Some(candidateActor), variables(Action.Cancel))
      history <- c.repository.store.get.map(_.history)
    } yield {
      assertEquals(payload(requested, Action.Request).hcursor.get[Boolean]("rescheduleRequested"), Right(true))
      assertEquals(payload(accepted, Action.Accept).hcursor.get[String]("progress"), Right("RescheduleHoldPending"))
      assertEquals(
        payload(accepted, Action.Accept).hcursor.get[String]("pendingStartsAt"),
        Right(replacement.startsAt.toString)
      )
      assertEquals(payload(cancelled, Action.Cancel).hcursor.get[String]("progress"), Right("CancelPending"))
      assertEquals(history.map(_._4), Vector("Interview cancelled by the candidate."))
    }
  }

  private val rules: List[(String, InterviewWorkflow, Action, Instant, ApplicationStatus, Long, String)] = List(
    (
      "a started interval",
      completed,
      Action.Cancel,
      interval.startsAt,
      ApplicationStatus.Interview,
      revision,
      "INTERVIEW_ALREADY_STARTED"
    ),
    (
      "an already cancelled workflow",
      workflowIn(InterviewWorkflowPhase.Cancelled, _.copy(cancelledAt = Some(now))),
      Action.Cancel,
      now,
      ApplicationStatus.Rejected,
      revision,
      "INTERVIEW_ALREADY_CANCELLED"
    ),
    (
      "an unsettled phase",
      workflowIn(InterviewWorkflowPhase.RescheduleHoldPending, _.copy(pendingInterval = Some(replacement))),
      Action.Propose,
      now,
      ApplicationStatus.Interview,
      revision,
      "INTERVIEW_NOT_SETTLED"
    ),
    (
      "an open proposal",
      proposalOpen(),
      Action.Propose,
      now,
      ApplicationStatus.Interview,
      revision,
      "PROPOSAL_ALREADY_OPEN"
    ),
    ("no proposal to accept", completed, Action.Accept, now, ApplicationStatus.Interview, revision, "NO_OPEN_PROPOSAL"),
    (
      "no proposal to withdraw",
      completed,
      Action.Withdraw,
      now,
      ApplicationStatus.Interview,
      revision,
      "NO_OPEN_PROPOSAL"
    ),
    (
      "an expired proposal",
      proposalOpen(now.plusSeconds(60)),
      Action.Accept,
      now.plusSeconds(60),
      ApplicationStatus.Interview,
      revision,
      "PROPOSAL_EXPIRED"
    ),
    ("a stale revision", completed, Action.Request, now, ApplicationStatus.Interview, revision - 1, "STALE_REVISION"),
    (
      "no request to dismiss",
      completed,
      Action.Dismiss,
      now,
      ApplicationStatus.Interview,
      revision,
      "NO_RESCHEDULE_REQUEST"
    ),
    (
      "a hired application",
      completed,
      Action.Cancel,
      now,
      ApplicationStatus.Hired,
      revision,
      "INVALID_STATUS_TRANSITION"
    )
  )

  rules.foreach { case (label, workflow, action, clock, status, expectedRevision, expected) =>
    test(s"DHW-19 $label is the sanitized code $expected and changes nothing") {
      val actor = action match {
        case Action.Propose | Action.Withdraw | Action.Dismiss => recruiterActor
        case _                                                 => candidateActor
      }
      arranged(workflow, clock, status).flatMap { a =>
        for {
          json <- act(a, action, Some(actor), variables(action, expectedRevision = expectedRevision))
          after <- a.stored
          commands <- a.commands
          current <- a.status
        } yield {
          assertEquals(code(json, action), Some(expected), clue(json))
          val text = payload(json, action).hcursor.get[String]("message").getOrElse("")
          assert(!text.contains(workflowId.value.toString) && !text.contains(candidateId.value.toString), clue(text))
          assertEquals((after, commands, current), (workflow, 0, status))
        }
      }
    }
  }

  test("DHW-19 an interval that ends before it starts, or equals the current one, is a typed outcome") {
    arranged(completed).flatMap { a =>
      for {
        reversed <- act(
          a,
          Action.Propose,
          Some(recruiterActor),
          variables(Action.Propose, proposed = InterviewInterval(replacement.endsAt, replacement.startsAt))
        )
        unchanged <- act(a, Action.Propose, Some(recruiterActor), variables(Action.Propose, proposed = interval))
        after <- a.stored
      } yield {
        assertEquals(code(reversed, Action.Propose), Some("INVALID_INTERVIEW_INTERVAL"), clue(reversed))
        assertEquals(code(unchanged, Action.Propose), Some("RESCHEDULE_INTERVAL_UNCHANGED"), clue(unchanged))
        assertEquals(after, completed)
      }
    }
  }

  test("DHW-31 a replayed key returns the same workflow once; a reused key for another action is a generic conflict") {
    arranged(completed).flatMap { a =>
      val key = UUID.randomUUID()
      for {
        first <- act(a, Action.Request, Some(candidateActor), variables(Action.Request, key = key))
        replay <- act(a, Action.Request, Some(candidateActor), variables(Action.Request, key = key))
        commands <- a.commands
        reused <- act(a, Action.Cancel, Some(candidateActor), variables(Action.Cancel, key = key))
        after <- a.stored
      } yield {
        assertEquals(payload(first, Action.Request).hcursor.get[Long]("revision"), Right(revision + 1))
        assertEquals(payload(replay, Action.Request), payload(first, Action.Request))
        assertEquals(commands, 1)
        assertEquals(code(reused, Action.Cancel), Some("CONFLICT"), clue(reused))
        assertEquals(after.revision, revision + 1)
      }
    }
  }

  private def aliased(count: Int, field: String = "requestInterviewReschedule"): String =
    (1 to count)
      .map(index =>
        s"""a$index: $field(input: { workflowId: "${workflowId.value}", expectedRevision: $revision, idempotencyKey: "${new UUID(
            7L,
            index.toLong
          )}" }) { ... on InterviewWorkflow { revision } ... on DomainError { code } }"""
      )
      .mkString("mutation { ", "\n", " }")

  private def limiter(attempts: Int): IO[UserId => IO[Either[InterviewActionRateLimiter.RateLimited, Unit]]] =
    InterviewActionRateLimiter.create(InterviewActionRateLimitConfig(60, attempts, 100)).map(limiter => limiter.permit)

  test("DHW-34 aliased mutations in one request each take an allowance unit and the surplus is a typed refusal") {
    for {
      a <- arranged(completed)
      permit <- limiter(2)
      result <- run(a, aliased(4), None, Json.obj(), Some(candidateActor), permit)
      json = result.fold(failure => fail(failure.toString), identity)
      data = json.hcursor.downField("data")
    } yield {
      assert(json.hcursor.downField("errors").focus.isEmpty, clue(json))
      assertEquals(data.downField("a1").get[Long]("revision"), Right(revision + 1))
      assertEquals(data.downField("a2").get[String]("code"), Right("STALE_REVISION"), clue(json))
      assertEquals(data.downField("a3").get[String]("code"), Right("RATE_LIMITED"), clue(json))
      assertEquals(data.downField("a4").get[String]("code"), Right("RATE_LIMITED"), clue(json))
      assert(!json.noSpaces.contains("attempts") && !json.noSpaces.contains("window"), clue(json))
    }
  }

  test("DHW-34 each actor has an isolated allowance across requests") {
    for {
      a <- arranged(completed)
      permit <- limiter(1)
      candidateFirst <- act(a, Action.Request, Some(candidateActor), variables(Action.Request), permit)
      candidateAgain <- act(a, Action.Request, Some(candidateActor), variables(Action.Request), permit)
      recruiterFirst <- act(
        a,
        Action.Dismiss,
        Some(recruiterActor),
        variables(Action.Dismiss, expectedRevision = revision + 1),
        permit
      )
      otherFirst <- act(a, Action.Request, Some(otherCandidateActor), variables(Action.Request), permit)
      adminFirst <- act(a, Action.Cancel, Some(adminActor), variables(Action.Cancel), permit)
    } yield {
      assertEquals(
        payload(candidateFirst, Action.Request).hcursor.get[Long]("revision"),
        Right(revision + 1),
        clue(candidateFirst)
      )
      assertEquals(code(candidateAgain, Action.Request), Some("RATE_LIMITED"), clue(candidateAgain))
      assertEquals(
        payload(recruiterFirst, Action.Dismiss).hcursor.get[Long]("revision"),
        Right(revision + 2),
        clue(recruiterFirst)
      )
      assertEquals(code(otherFirst, Action.Request), Some("NOT_FOUND"), clue(otherFirst))
      assertEquals(code(adminFirst, Action.Cancel), Some("STALE_REVISION"), clue(adminFirst))
    }
  }

  test("DHW-34 a refused call does no storage work: it is answered even when the hiring store is unavailable") {
    for {
      a <- arranged(completed)
      permit <- limiter(1)
      spent <- permit(candidateId)
      down = IO.pure(ProbeResult.Unavailable)
      refused <- act(a, Action.Request, Some(candidateActor), variables(Action.Request), permit, down)
      stranger <- act(a, Action.Request, Some(otherCandidateActor), variables(Action.Request), permit, down)
      after <- a.stored
    } yield {
      assertEquals(spent, Right(()))
      assertEquals(code(refused, Action.Request), Some("RATE_LIMITED"), clue(refused))
      // A caller with allowance left does reach the storage check, so the refusal above really came first.
      assertEquals(errorCode(stranger), Some("SERVICE_NOT_READY"), clue(stranger))
      assertEquals(after, completed)
    }
  }

  test("DHW-34 only the verified claims name the actor: a request without claims takes no allowance") {
    for {
      a <- arranged(completed)
      taken <- Ref.of[IO, Int](0)
      json <- act(a, Action.Request, None, variables(Action.Request), _ => taken.update(_ + 1).as(Right(())))
      count <- taken.get
    } yield {
      assertEquals(errorCode(json), Some("UNAUTHORIZED"))
      assertEquals(count, 0)
    }
  }

  test("DHW-34 a request that batches too many interview actions is rejected before any of them runs") {
    for {
      a <- arranged(completed)
      many <- run(a, aliased(12, "cancelInterview"), None, Json.obj(), Some(candidateActor))
      untouched <- a.stored
      few <- run(a, aliased(5, "cancelInterview"), None, Json.obj(), Some(candidateActor))
      after <- a.stored
    } yield {
      assertEquals(many, Left(HiringGraphQLSchema.Failure.InvalidQuery))
      assertEquals(untouched, completed)
      val data = few.fold(failure => fail(failure.toString), _.hcursor.downField("data"))
      assertEquals(data.downField("a1").get[Long]("revision"), Right(revision + 1), clue(few))
      assertEquals(
        (2 to 5).toList.map(n => data.downField(s"a$n").get[String]("code")),
        List.fill(4)(Right("STALE_REVISION")),
        clue(few)
      )
      assertEquals(after.phase, InterviewWorkflowPhase.CancelPending)
    }
  }
}
