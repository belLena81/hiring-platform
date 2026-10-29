package com.example.graphQL.cats.acceptance

import cats.effect.IO
import io.circe.Json
import io.circe.syntax.*
import java.util.UUID
import munit.Assertions.*

/** A signed-in account of the story. The token is the bearer credential returned by `signUp` or `login`. */
final case class StoryActor(label: String, role: String, userId: String, token: String)

/** What an operation answered, independent of whether the refusal came as a typed payload or a GraphQL error. */
sealed trait StoryOutcome
object StoryOutcome {
  final case class Succeeded(value: Json) extends StoryOutcome
  final case class Refused(code: String, message: String, typed: Boolean) extends StoryOutcome

  private val typedRefusals = Set("DomainError", "ValidationError", "SearchUnavailable")

  def of(response: GraphQLResponse, field: String): StoryOutcome =
    response.errors.headOption match {
      case Some(error) =>
        Refused(
          error.hcursor.downField("extensions").get[String]("code").getOrElse("UNCODED"),
          error.hcursor.get[String]("message").getOrElse(""),
          typed = false
        )
      case None =>
        val value = response.data(field)
        if (value.hcursor.get[String]("__typename").exists(typedRefusals)) {
          Refused(
            value.hcursor.get[String]("code").getOrElse("UNCODED"),
            value.hcursor.get[String]("message").getOrElse(""),
            typed = true
          )
        } else if (response.status >= 400) Refused("HTTP_" + response.status, "", typed = false)
        else Succeeded(value)
    }
}

/** The GraphQL operations of the checked contract, as the documents a client would send. */
object StoryDocuments {
  final case class MutationShape(inputType: String, success: String, selection: String)

  private val applicationSelection = "{ id status createdAt updatedAt }"
  private val jobSelection = "{ id title status skills createdAt updatedAt }"
  private val workflowSelection = "{ id revision progress }"

  /** Every mutation of the contract, with its input type and the selection of its success member. */
  val mutations: Map[String, MutationShape] = Map(
    "scheduleInterview" -> MutationShape("ScheduleInterviewInput", "InterviewWorkflow", workflowSelection),
    "repairInterviewWorkflow" -> MutationShape("RepairInterviewInput", "InterviewWorkflow", workflowSelection),
    "cancelInterview" -> MutationShape("InterviewActionInput", "InterviewWorkflow", workflowSelection),
    "requestInterviewReschedule" -> MutationShape("InterviewActionInput", "InterviewWorkflow", workflowSelection),
    "dismissInterviewRescheduleRequest" -> MutationShape(
      "InterviewActionInput",
      "InterviewWorkflow",
      workflowSelection
    ),
    "proposeInterviewReschedule" -> MutationShape(
      "ProposeInterviewRescheduleInput",
      "InterviewWorkflow",
      workflowSelection
    ),
    "withdrawInterviewReschedule" -> MutationShape("InterviewActionInput", "InterviewWorkflow", workflowSelection),
    "acceptInterviewReschedule" -> MutationShape("InterviewActionInput", "InterviewWorkflow", workflowSelection),
    "declineInterviewReschedule" -> MutationShape("InterviewActionInput", "InterviewWorkflow", workflowSelection),
    "submitApplication" -> MutationShape("SubmitApplicationInput", "Application", applicationSelection),
    "createJob" -> MutationShape("CreateJobInput", "Job", jobSelection),
    "updateJob" -> MutationShape("UpdateJobInput", "Job", jobSelection),
    "publishJob" -> MutationShape("JobActionInput", "Job", jobSelection),
    "closeJob" -> MutationShape("JobActionInput", "Job", jobSelection),
    "acceptApplication" -> MutationShape("ApplicationActionInput", "Application", applicationSelection),
    "moveApplicationToInterview" -> MutationShape("ApplicationActionInput", "Application", applicationSelection),
    "hireApplication" -> MutationShape("ApplicationActionInput", "Application", applicationSelection),
    "rejectApplication" -> MutationShape("RejectApplicationInput", "Application", applicationSelection),
    "declineApplication" -> MutationShape("DeclineApplicationInput", "Application", applicationSelection),
    "signUp" -> MutationShape("SignUpInput", "AuthSuccess", "{ accessToken user { id name role } }"),
    "login" -> MutationShape("LoginInput", "AuthSuccess", "{ accessToken user { id name role } }"),
    "updateMyProfile" -> MutationShape("UpdateMyProfileInput", "User", "{ id role }"),
    "deleteMyAccount" -> MutationShape("DeleteMyAccountInput", "DeletionReceipt", "{ receiptId status }"),
    "recordJobView" -> MutationShape("RecordJobViewInput", "InteractionSuccess", "{ eventId }"),
    "recordSearchResultClick" -> MutationShape("RecordSearchResultClickInput", "InteractionSuccess", "{ eventId }")
  )

  def mutation(name: String): String = {
    val shape = mutations(name)
    s"mutation Op($$input: ${shape.inputType}!) { $name(input: $$input) { __typename ... on ${shape.success} ${shape.selection} ... on UserError { code message } } }"
  }

  val me = "query Me { me { id name role status } }"

  val jobs =
    "query Jobs($filter: JobFilter, $first: Int!, $after: Cursor) { jobs(filter: $filter, first: $first, after: $after) { searchId edges { cursor node { id title status } } pageInfo { hasNextPage hasPreviousPage endCursor } } }"

  val job =
    "query JobById($id: JobID!) { job(id: $id) { id title status description skills updatedAt applicationCounts { total byStatus { status count } } } }"

  val myJobs =
    "query MyJobs($first: Int!, $after: Cursor, $status: JobStatus) { myJobs(first: $first, after: $after, status: $status) { edges { node { id title status } } pageInfo { hasNextPage endCursor } } }"

  val myApplications =
    "query MyApplications($first: Int!, $after: Cursor) { myApplications(first: $first, after: $after) { edges { node { id status job { id } } } pageInfo { hasNextPage endCursor } } }"

  val jobApplications =
    "query JobApplications($jobId: JobID!, $first: Int!, $after: Cursor, $status: ApplicationStatus) { jobApplications(jobId: $jobId, first: $first, after: $after, status: $status) { edges { node { id status candidate { id } } } pageInfo { hasNextPage endCursor } } }"

  val applicationHistory =
    "query History($applicationId: ApplicationID!, $first: Int!) { applicationHistory(applicationId: $applicationId, first: $first) { edges { node { id previousStatus newStatus actorRole occurredAt feedback reason } } pageInfo { hasNextPage endCursor } } }"
}

/** Story-level calls against the real API. Every mutation carries a fresh `idempotencyKey`. */
final class StoryOperations(client: GraphQLClient) {
  import StoryDocuments.*

  def key: String = UUID.randomUUID().toString

  def run(document: String, variables: Json, actor: Option[StoryActor]): IO[GraphQLResponse] =
    client.execute(document, variables, actor.map(_.token))

  /** Sends the named mutation with a fresh idempotency key merged into `input`. */
  def send(name: String, input: Json, actor: Option[StoryActor]): IO[GraphQLResponse] =
    sendWithKey(name, input, actor, key)

  def sendWithKey(name: String, input: Json, actor: Option[StoryActor], idempotencyKey: String): IO[GraphQLResponse] =
    run(
      mutation(name),
      Json.obj("input" -> input.deepMerge(Json.obj("idempotencyKey" -> idempotencyKey.asJson))),
      actor
    )

  def mutate(name: String, input: Json, actor: StoryActor): IO[StoryOutcome] =
    send(name, input, Some(actor)).map(StoryOutcome.of(_, name))

  def succeed(name: String, input: Json, actor: StoryActor): IO[Json] =
    mutate(name, input, actor).map {
      case StoryOutcome.Succeeded(value) => value
      case other                         => fail(s"$name expected success for ${actor.label} but was $other")
    }

  def query(document: String, field: String, variables: Json, actor: Option[StoryActor]): IO[StoryOutcome] =
    run(document, variables, actor).map(StoryOutcome.of(_, field))

  def read(document: String, field: String, variables: Json, actor: StoryActor): IO[Json] =
    query(document, field, variables, Some(actor)).map {
      case StoryOutcome.Succeeded(value) => value
      case other                         => fail(s"$field expected success for ${actor.label} but was $other")
    }

  private def authenticate(name: String, response: GraphQLResponse, label: String, role: String): StoryActor =
    StoryOutcome.of(response, name) match {
      case StoryOutcome.Succeeded(value) =>
        StoryActor(
          label,
          role,
          value.hcursor.downField("user").get[String]("id").fold(e => fail(e.message), identity),
          value.hcursor.get[String]("accessToken").fold(e => fail(e.message), identity)
        )
      case other => fail(s"$name for $label expected success but was $other")
    }

  def login(name: String, password: String, role: String): IO[StoryActor] =
    send("login", Json.obj("name" -> name.asJson, "password" -> password.asJson), None)
      .map(authenticate("login", _, name, role))

  val accountPassword = "story-account-password-1"

  def signUpCandidate(name: String, skills: List[String], city: String = "Berlin"): IO[StoryActor] =
    send(
      "signUp",
      Json.obj(
        "name" -> name.asJson,
        "role" -> "CANDIDATE".asJson,
        "password" -> accountPassword.asJson,
        "profile" -> Json.obj(
          "candidate" -> Json.obj(
            "skills" -> skills.asJson,
            "experienceSummary" -> "Five years of backend work".asJson,
            "residence" -> Json.obj("country" -> "DE".asJson, "city" -> city.asJson),
            "recruiterSearchOptIn" -> true.asJson
          )
        )
      ),
      None
    ).map(authenticate("signUp", _, name, "CANDIDATE"))

  def signUpRecruiter(name: String, organization: String): IO[StoryActor] =
    send(
      "signUp",
      Json.obj(
        "name" -> name.asJson,
        "role" -> "RECRUITER".asJson,
        "password" -> accountPassword.asJson,
        "profile" -> Json.obj("recruiter" -> Json.obj("organizationName" -> organization.asJson))
      ),
      None
    ).map(authenticate("signUp", _, name, "RECRUITER"))

  def jobInput(title: String, city: String, skills: List[String]): Json = Json.obj(
    "title" -> title.asJson,
    "description" -> s"$title: a fictional posting used by the acceptance story".asJson,
    "requirements" -> List("Two years of relevant experience").asJson,
    "skills" -> skills.asJson,
    "country" -> "DE".asJson,
    "city" -> city.asJson,
    "remote" -> false.asJson
  )

  /** `createJob` makes a Draft that only its owner can see. */
  def createJob(owner: StoryActor, title: String, city: String, skills: List[String]): IO[String] =
    succeed("createJob", jobInput(title, city, skills), owner).map(idOf)

  /** A job created and then published, so candidates can find it. */
  def createPublishedJob(owner: StoryActor, title: String, city: String, skills: List[String]): IO[String] =
    createJob(owner, title, city, skills).flatTap(id => succeed("publishJob", Json.obj("jobId" -> id.asJson), owner))

  def submit(candidate: StoryActor, jobId: String): IO[StoryOutcome] =
    mutate("submitApplication", Json.obj("jobId" -> jobId.asJson), candidate)

  def submitOk(candidate: StoryActor, jobId: String): IO[String] =
    submit(candidate, jobId).map {
      case StoryOutcome.Succeeded(value) => idOf(value)
      case other                         => fail(s"${candidate.label} expected to submit to $jobId but was $other")
    }

  def applicationAction(
      name: String,
      actor: StoryActor,
      applicationId: String,
      extra: Json = Json.obj()
  ): IO[StoryOutcome] =
    mutate(name, Json.obj("applicationId" -> applicationId.asJson).deepMerge(extra), actor)

  def history(actor: StoryActor, applicationId: String): IO[Vector[Json]] =
    read(
      applicationHistory,
      "applicationHistory",
      Json.obj("applicationId" -> applicationId.asJson, "first" -> 50.asJson),
      actor
    )
      .map(edges)

  def edges(connection: Json): Vector[Json] =
    connection.hcursor
      .downField("edges")
      .focus
      .flatMap(_.asArray)
      .getOrElse(Vector.empty)
      .flatMap(_.hcursor.downField("node").focus)

  def idOf(value: Json): String = value.hcursor.get[String]("id").fold(e => fail(e.message), identity)
}
