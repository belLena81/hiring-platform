package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.service.{AccountError, ActorContext, AuthenticationError, AvailabilityError, HealthService, ProbeResult, RepositoryError, SearchError, TraceContext, UseCaseError}
import com.example.graphQL.cats.domain.error.{DomainError, DomainValidationError}
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId}
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.service.job.{CreateJobInput, UpdateJobInput}
import com.example.graphQL.cats.service.protocol.{AccountProfileInput, AccountUseCases, BootstrapAdminInput, LoginInput, SignUpInput, JobUseCases, SearchUseCases}
import com.example.graphQL.cats.shared.pagination.*
import com.example.graphQL.cats.shared.search.{JobSearchFilter, RankedCandidate, RankedJob}
import io.circe.{Decoder, Json}
import sangria.execution.{ExceptionHandler, Executor, HandledException, QueryAnalysisError, QueryReducer}
import sangria.marshalling.circe.*
import sangria.renderer.SchemaRenderer
import sangria.schema.*

import java.time.Instant
import java.util.UUID
import java.util.Locale

object HiringGraphQLSchema {
  private val MaxQueryDepth = 16
  private val MaxQueryComplexity = 1000d
  private val QueryReducers = List(
    QueryReducer.rejectMaxDepth[RequestContext](MaxQueryDepth),
    QueryReducer.rejectComplexQueries[RequestContext](MaxQueryComplexity, (_, _) =>
      new IllegalArgumentException("Query complexity limit exceeded"))
  )

  private final case class GraphQLError(code: String, message: String)
  private final case class PageInfo(hasNextPage: Boolean, endCursor: Option[String])
  private final case class Edge[A](node: A, cursor: String)
  private final case class Connection[A](edges: List[Edge[A]], pageInfo: PageInfo, errors: List[GraphQLError] = Nil)
  private final case class JobPayload(job: Option[Job], errors: List[GraphQLError])
  private final case class ApplicationPayload(application: Option[Application], errors: List[GraphQLError])
  private final case class AccountPayload(user: Option[User], accessToken: Option[String], expiresAt: Option[String], errors: List[GraphQLError])
  private final case class UserPayload(user: Option[User], errors: List[GraphQLError])
  private final case class DeleteAccountPayload(deleted: Boolean, errors: List[GraphQLError])
  private final case class JobFilterGraphQLInput(city: Option[String], skills: Option[List[String]], createdAfter: Option[String])
  private final case class CandidateMatchProfile(skills: Set[String], experienceSummary: Option[String])
  private final case class CandidateMatchCandidate(id: String, name: String, profile: Option[CandidateMatchProfile])
  private final case class RankedJobPayload(job: Job, score: Double, searchMode: SearchMode, model: String, version: Int, searchId: String)
  private final case class RankedCandidatePayload(candidate: CandidateMatchCandidate, score: Double, searchMode: SearchMode, model: String, version: Int, searchId: String)
  private final case class RankedJobResults(results: List[RankedJobPayload], errors: List[GraphQLError])
  private final case class RankedCandidateResults(results: List[RankedCandidatePayload], errors: List[GraphQLError])
  private final case class SubmitApplicationGraphQLInput(jobId: String)
  private final case class JobGraphQLInput(
      title: String,
      description: String,
      requirements: List[String],
      skills: List[String],
      country: String,
      city: Option[String],
      remote: Boolean
  )
  private final case class UpdateJobGraphQLInput(id: String, patch: JobGraphQLInput)
  private final case class JobActionGraphQLInput(jobId: String)
  private final case class ApplicationActionGraphQLInput(applicationId: String)
  private final case class RejectApplicationGraphQLInput(applicationId: String, feedback: Option[String])
  private final case class DeclineApplicationGraphQLInput(applicationId: String, reason: Option[String])
  private final case class SignUpGraphQLInput(name: String, role: String, password: String, skills: Option[List[String]], experienceSummary: Option[String], resumeRef: Option[String], organizationName: Option[String], jobTitle: Option[String])
  private final case class BootstrapAdminGraphQLInput(name: String, password: String)
  private final case class LoginGraphQLInput(name: String, password: String)
  private final case class UpdateProfileGraphQLInput(skills: Option[List[String]], experienceSummary: Option[String], resumeRef: Option[String], organizationName: Option[String], jobTitle: Option[String])

  private given Decoder[SubmitApplicationGraphQLInput] =
    Decoder.forProduct1("jobId")(SubmitApplicationGraphQLInput.apply)
  private given Decoder[JobGraphQLInput] =
    Decoder.forProduct7("title", "description", "requirements", "skills", "country", "city", "remote")(JobGraphQLInput.apply)
  private given Decoder[UpdateJobGraphQLInput] =
    Decoder.forProduct2("id", "patch")(UpdateJobGraphQLInput.apply)
  private given Decoder[JobActionGraphQLInput] =
    Decoder.forProduct1("jobId")(JobActionGraphQLInput.apply)
  private given Decoder[ApplicationActionGraphQLInput] =
    Decoder.forProduct1("applicationId")(ApplicationActionGraphQLInput.apply)
  private given Decoder[RejectApplicationGraphQLInput] =
    Decoder.forProduct2("applicationId", "feedback")(RejectApplicationGraphQLInput.apply)
  private given Decoder[DeclineApplicationGraphQLInput] =
    Decoder.forProduct2("applicationId", "reason")(DeclineApplicationGraphQLInput.apply)
  private given Decoder[JobFilterGraphQLInput] =
    Decoder.forProduct3("city", "skills", "createdAfter")(JobFilterGraphQLInput.apply)
  private given Decoder[SignUpGraphQLInput] =
    Decoder.forProduct8("name", "role", "password", "skills", "experienceSummary", "resumeRef", "organizationName", "jobTitle")(SignUpGraphQLInput.apply)
  private given Decoder[BootstrapAdminGraphQLInput] =
    Decoder.forProduct2("name", "password")(BootstrapAdminGraphQLInput.apply)
  private given Decoder[LoginGraphQLInput] =
    Decoder.forProduct2("name", "password")(LoginGraphQLInput.apply)
  private given Decoder[UpdateProfileGraphQLInput] =
    Decoder.forProduct5("skills", "experienceSummary", "resumeRef", "organizationName", "jobTitle")(UpdateProfileGraphQLInput.apply)

  private val healthStatus = EnumType("HealthStatus", values = List(EnumValue("UP", value = "UP")))
  private val readinessStatus = EnumType("ReadinessStatus", values = List(
    EnumValue("READY", value = "READY"), EnumValue("NOT_READY", value = "NOT_READY")))
  private val jobStatus = EnumType("JobStatus", values = JobStatus.values.toList.map(status => EnumValue(status.toString, value = status)))
  private val applicationStatus =
    EnumType("ApplicationStatus", values = ApplicationStatus.values.toList.map(status => EnumValue(status.toString, value = status)))
  private val userRole = EnumType("UserRole", values = UserRole.values.toList.map(role => EnumValue(role.toString, value = role)))
  private val userStatus = EnumType("UserStatus", values = AccountStatus.values.toList.map(status => EnumValue(status.toString.toUpperCase(Locale.ROOT), value = status)))
  private val searchMode = EnumType("SearchMode", values = SearchMode.values.toList.map(mode => EnumValue(mode.toString, value = mode)))

  private val idArgument = Argument("id", IDType)
  private val jobIdArgument = Argument("jobId", IDType)
  private val queryArgument = Argument("query", StringType)
  private val applicationIdArgument = Argument("applicationId", IDType)
  private val firstArgument = Argument("first", IntType)
  private val afterArgument = Argument("after", OptionInputType(StringType))
  private val cityArgument = Argument("city", OptionInputType(StringType))
  private val skillsArgument = Argument("skills", OptionInputType(ListInputType(StringType)))
  private val createdAfterArgument = Argument("createdAfter", OptionInputType(StringType))
  private val jobStatusArgument = Argument("status", OptionInputType(jobStatus))
  private val applicationStatusArgument = Argument("status", OptionInputType(applicationStatus))
  private val userRoleArgument = Argument("role", OptionInputType(userRole))
  private val userStatusArgument = Argument("status", OptionInputType(userStatus))
  private val jobFilterInputType = InputObjectType[JobFilterGraphQLInput]("JobFilter", List(
    InputField("city", OptionInputType(StringType)),
    InputField("skills", OptionInputType(ListInputType(StringType))),
    InputField("createdAfter", OptionInputType(StringType))
  ))
  private val jobFilterArgument = Argument("filter", OptionInputType(jobFilterInputType))
  private val submitApplicationInputType = InputObjectType[SubmitApplicationGraphQLInput]("SubmitApplicationInput", List(
    InputField("jobId", IDType)
  ))
  private val jobInputType = InputObjectType[JobGraphQLInput]("JobInput", List(
    InputField("title", StringType),
    InputField("description", StringType),
    InputField("requirements", ListInputType(StringType)),
    InputField("skills", ListInputType(StringType)),
    InputField("country", StringType),
    InputField("city", OptionInputType(StringType)),
    InputField("remote", BooleanType)
  ))
  private val updateJobInputType = InputObjectType[UpdateJobGraphQLInput]("UpdateJobInput", List(
    InputField("id", IDType),
    InputField("patch", jobInputType)
  ))
  private val jobActionInputType = InputObjectType[JobActionGraphQLInput]("JobActionInput", List(
    InputField("jobId", IDType)
  ))
  private val applicationActionInputType = InputObjectType[ApplicationActionGraphQLInput]("ApplicationActionInput", List(
    InputField("applicationId", IDType)
  ))
  private val rejectApplicationInputType = InputObjectType[RejectApplicationGraphQLInput]("RejectApplicationInput", List(
    InputField("applicationId", IDType),
    InputField("feedback", OptionInputType(StringType))
  ))
  private val declineApplicationInputType = InputObjectType[DeclineApplicationGraphQLInput]("DeclineApplicationInput", List(
    InputField("applicationId", IDType),
    InputField("reason", OptionInputType(StringType))
  ))
  private val submitApplicationInputArgument = Argument("input", submitApplicationInputType)
  private val createJobInputArgument = Argument("input", jobInputType)
  private val updateJobInputArgument = Argument("input", updateJobInputType)
  private val jobActionInputArgument = Argument("input", jobActionInputType)
  private val applicationActionInputArgument = Argument("input", applicationActionInputType)
  private val rejectApplicationInputArgument = Argument("input", rejectApplicationInputType)
  private val declineApplicationInputArgument = Argument("input", declineApplicationInputType)
  private val signUpInputType = InputObjectType[SignUpGraphQLInput]("SignUpInput", List(
    InputField("name", StringType), InputField("role", userRole), InputField("password", StringType),
    InputField("skills", OptionInputType(ListInputType(StringType))), InputField("experienceSummary", OptionInputType(StringType)),
    InputField("resumeRef", OptionInputType(StringType)), InputField("organizationName", OptionInputType(StringType)),
    InputField("jobTitle", OptionInputType(StringType))))
  private val bootstrapAdminInputType = InputObjectType[BootstrapAdminGraphQLInput]("BootstrapAdminInput", List(InputField("name", StringType), InputField("password", StringType)))
  private val loginInputType = InputObjectType[LoginGraphQLInput]("LoginInput", List(InputField("name", StringType), InputField("password", StringType)))
  private val updateProfileInputType = InputObjectType[UpdateProfileGraphQLInput]("UpdateMyProfileInput", List(
    InputField("skills", OptionInputType(ListInputType(StringType))), InputField("experienceSummary", OptionInputType(StringType)),
    InputField("resumeRef", OptionInputType(StringType)), InputField("organizationName", OptionInputType(StringType)),
    InputField("jobTitle", OptionInputType(StringType))))
  private val signUpInputArgument = Argument("input", signUpInputType)
  private val bootstrapAdminInputArgument = Argument("input", bootstrapAdminInputType)
  private val loginInputArgument = Argument("input", loginInputType)
  private val updateProfileInputArgument = Argument("input", updateProfileInputType)

  private val healthType = ObjectType("Health", fields[RequestContext, Unit](
    Field("status", healthStatus, resolve = _ => "UP")))
  private val readinessType = ObjectType("Readiness", fields[RequestContext, ProbeResult](
    Field("status", readinessStatus, resolve = context =>
      if (context.value == ProbeResult.Ready) "READY" else "NOT_READY")))
  private val errorType = ObjectType("PayloadError", fields[RequestContext, GraphQLError](
    Field("code", StringType, resolve = _.value.code),
    Field("message", StringType, resolve = _.value.message)))
  private val pageInfoType = ObjectType("PageInfo", fields[RequestContext, PageInfo](
    Field("hasNextPage", BooleanType, resolve = _.value.hasNextPage),
    Field("endCursor", OptionType(StringType), resolve = _.value.endCursor)))
  private val locationType = ObjectType("Location", fields[RequestContext, Location](
    Field("country", StringType, resolve = _.value.country),
    Field("city", StringType, resolve = _.value.city),
    Field("remote", BooleanType, resolve = _.value.remote)))
  private val candidateProfileType = ObjectType("CandidateProfile", fields[RequestContext, CandidateProfile](
    Field("skills", ListType(StringType), resolve = _.value.skills.toList.sorted),
    Field("experienceSummary", OptionType(StringType), resolve = _.value.experienceSummary),
    Field("resumeRef", OptionType(StringType), resolve = _.value.resumeRef)))
  private val candidateMatchProfileType = ObjectType("CandidateMatchProfile", fields[RequestContext, CandidateMatchProfile](
    Field("skills", ListType(StringType), resolve = _.value.skills.toList.sorted),
    Field("experienceSummary", OptionType(StringType), resolve = _.value.experienceSummary)))
  private val candidateMatchCandidateType = ObjectType("CandidateMatchCandidate", fields[RequestContext, CandidateMatchCandidate](
    Field("id", IDType, resolve = _.value.id),
    Field("name", StringType, resolve = _.value.name),
    Field("profile", OptionType(candidateMatchProfileType), resolve = _.value.profile)))
  private lazy val userProfileType: OutputType[User] =
    UnionType[RequestContext]("UserProfile", List(candidateProfileType, recruiterProfileType))
      .mapValue[User](user => user.profile.orElse(user.recruiterProfile).orNull)
  private lazy val userType: ObjectType[RequestContext, User] = ObjectType("User", fields[RequestContext, User](
    Field("id", IDType, resolve = _.value.id.value.toString),
    Field("email", OptionType(StringType), resolve = _.value.email),
    Field("name", StringType, resolve = _.value.name),
    Field("role", userRole, resolve = _.value.role),
    Field("status", userStatus, resolve = _.value.accountStatus),
    Field("profile", OptionType(userProfileType), resolve = _.value),
    Field("createdAt", StringType, resolve = _.value.createdAt.toString)))
  private lazy val recruiterProfileType: ObjectType[RequestContext, RecruiterProfile] = ObjectType("RecruiterProfile", fields[RequestContext, RecruiterProfile](
      Field("organizationName", StringType, resolve = _.value.organizationName),
      Field("jobTitle", OptionType(StringType), resolve = _.value.jobTitle)))
  private lazy val jobType: ObjectType[RequestContext, Job] = ObjectType("Job", fields[RequestContext, Job](
    Field("id", IDType, resolve = _.value.id.value.toString),
    Field("title", StringType, resolve = _.value.title),
    Field("description", StringType, resolve = _.value.description),
    Field("requirements", ListType(StringType), resolve = _.value.requirements),
    Field("skills", ListType(StringType), resolve = _.value.skills.toList.sorted),
    Field("location", locationType, resolve = _.value.location),
    Field("status", jobStatus, resolve = _.value.status),
    Field("createdAt", StringType, resolve = _.value.createdAt.toString),
    Field("updatedAt", StringType, resolve = _.value.updatedAt.toString),
    Field("recruiter", OptionType(userType), resolve = context =>
      context.ctx.unsafeToFuture(context.ctx.user(context.value.recruiterId)))))
  private lazy val applicationType: ObjectType[RequestContext, Application] = ObjectType("Application", fields[RequestContext, Application](
    Field("id", IDType, resolve = _.value.id.value.toString),
    Field("status", applicationStatus, resolve = _.value.status),
    Field("createdAt", StringType, resolve = _.value.createdAt.toString),
    Field("updatedAt", StringType, resolve = _.value.updatedAt.toString),
    Field("candidate", OptionType(userType), resolve = context =>
      context.ctx.unsafeToFuture(context.ctx.user(context.value.candidateId))),
    Field("job", OptionType(jobType), resolve = context =>
      context.ctx.unsafeToFuture(context.ctx.job(context.value.jobId)))))
  private val applicationEventType = ObjectType("ApplicationEvent", fields[RequestContext, ApplicationEvent](
    Field("id", IDType, resolve = _.value.id.value.toString),
    Field("previousStatus", OptionType(applicationStatus), resolve = _.value.previousStatus),
    Field("newStatus", applicationStatus, resolve = _.value.newStatus),
    Field("actorId", IDType, resolve = _.value.actorId.value.toString),
    Field("occurredAt", StringType, resolve = _.value.occurredAt.toString),
    Field("feedback", OptionType(StringType), resolve = _.value.feedback),
    Field("reason", OptionType(StringType), resolve = _.value.reason)))

  private lazy val jobEdgeType = edgeType("JobEdge", jobType)
  private lazy val applicationEdgeType = edgeType("ApplicationEdge", applicationType)
  private lazy val applicationEventEdgeType = edgeType("ApplicationEventEdge", applicationEventType)
  private lazy val jobConnectionType = connectionType("JobConnection", jobEdgeType)
  private lazy val applicationConnectionType = connectionType("ApplicationConnection", applicationEdgeType)
  private lazy val applicationEventConnectionType = connectionType("ApplicationEventConnection", applicationEventEdgeType)
  private lazy val userEdgeType = edgeType("UserEdge", userType)
  private lazy val userConnectionType = connectionType("UserConnection", userEdgeType)
  private lazy val accountPayloadType = ObjectType("AuthPayload", fields[RequestContext, AccountPayload](
    Field("user", OptionType(userType), resolve = _.value.user), Field("accessToken", OptionType(StringType), resolve = _.value.accessToken),
    Field("expiresAt", OptionType(StringType), resolve = _.value.expiresAt), Field("errors", ListType(errorType), resolve = _.value.errors)))
  private lazy val userPayloadType = ObjectType("UserPayload", fields[RequestContext, UserPayload](
    Field("user", OptionType(userType), resolve = _.value.user), Field("errors", ListType(errorType), resolve = _.value.errors)))
  private lazy val deleteAccountPayloadType = ObjectType("DeleteAccountPayload", fields[RequestContext, DeleteAccountPayload](
    Field("deleted", BooleanType, resolve = _.value.deleted), Field("errors", ListType(errorType), resolve = _.value.errors)))
  private lazy val jobPayloadType = ObjectType("JobPayload", fields[RequestContext, JobPayload](
    Field("job", OptionType(jobType), resolve = _.value.job),
    Field("errors", ListType(errorType), resolve = _.value.errors)))
  private lazy val applicationPayloadType = ObjectType("ApplicationPayload", fields[RequestContext, ApplicationPayload](
    Field("application", OptionType(applicationType), resolve = _.value.application),
    Field("errors", ListType(errorType), resolve = _.value.errors)))
  private lazy val rankedJobType = ObjectType("RankedJob", fields[RequestContext, RankedJobPayload](
    Field("job", jobType, resolve = _.value.job),
    Field("score", FloatType, resolve = _.value.score),
    Field("searchMode", searchMode, resolve = _.value.searchMode),
    Field("model", StringType, resolve = _.value.model),
    Field("version", IntType, resolve = _.value.version),
    Field("searchId", IDType, resolve = _.value.searchId)))
  private lazy val rankedCandidateType = ObjectType("RankedCandidate", fields[RequestContext, RankedCandidatePayload](
    Field("candidate", candidateMatchCandidateType, resolve = _.value.candidate),
    Field("score", FloatType, resolve = _.value.score),
    Field("searchMode", searchMode, resolve = _.value.searchMode),
    Field("model", StringType, resolve = _.value.model),
    Field("version", IntType, resolve = _.value.version),
    Field("searchId", IDType, resolve = _.value.searchId)))
  private lazy val rankedJobResultsType = ObjectType("RankedJobResults", fields[RequestContext, RankedJobResults](
    Field("results", ListType(rankedJobType), resolve = _.value.results),
    Field("errors", ListType(errorType), resolve = _.value.errors)))
  private lazy val rankedCandidateResultsType = ObjectType("RankedCandidateResults", fields[RequestContext, RankedCandidateResults](
    Field("results", ListType(rankedCandidateType), resolve = _.value.results),
    Field("errors", ListType(errorType), resolve = _.value.errors)))

  val schema: Schema[RequestContext, Unit] = Schema(
    ObjectType("Query", fields[RequestContext, Unit](
      Field("health", healthType, resolve = _ => ()),
      Field("readiness", readinessType, resolve = context => context.ctx.readiness),
      Field("me", OptionType(userType), resolve = context => context.ctx.unsafeToFuture(accountMe(context))),
      Field("users", userConnectionType, arguments = firstArgument :: afterArgument :: userRoleArgument :: userStatusArgument :: Nil,
        resolve = context => context.ctx.unsafeToFuture(users(context))),
      Field("jobs", jobConnectionType,
        arguments = firstArgument :: afterArgument :: cityArgument :: skillsArgument :: createdAfterArgument :: Nil,
        resolve = context => context.ctx.unsafeToFuture(jobs(context))),
      Field("semanticJobSearch", rankedJobResultsType,
        arguments = queryArgument :: jobFilterArgument :: firstArgument :: Nil,
        resolve = context => context.ctx.unsafeToFuture(semanticJobSearch(context))),
      Field("recommendedJobs", rankedJobResultsType,
        arguments = firstArgument :: Nil,
        resolve = context => context.ctx.unsafeToFuture(recommendedJobs(context))),
      Field("candidateMatches", rankedCandidateResultsType,
        arguments = jobIdArgument :: firstArgument :: Nil,
        resolve = context => context.ctx.unsafeToFuture(candidateMatches(context))),
      Field("job", OptionType(jobType), arguments = idArgument :: Nil,
        resolve = context => context.ctx.unsafeToFuture(job(context))),
      Field("myJobs", jobConnectionType, arguments = firstArgument :: afterArgument :: jobStatusArgument :: Nil,
        resolve = context => context.ctx.unsafeToFuture(myJobs(context))),
      Field("myApplications", applicationConnectionType, arguments = firstArgument :: afterArgument :: applicationStatusArgument :: Nil,
        resolve = context => context.ctx.unsafeToFuture(myApplications(context))),
      Field("jobApplications", applicationConnectionType,
        arguments = jobIdArgument :: firstArgument :: afterArgument :: applicationStatusArgument :: Nil,
        resolve = context => context.ctx.unsafeToFuture(jobApplications(context))),
      Field("applicationHistory", applicationEventConnectionType,
        arguments = applicationIdArgument :: firstArgument :: afterArgument :: Nil,
        resolve = context => context.ctx.unsafeToFuture(applicationHistory(context)))
    )),
    Some(ObjectType("Mutation", fields[RequestContext, Unit](
      Field("submitApplication", applicationPayloadType, arguments = submitApplicationInputArgument :: Nil,
        resolve = context => context.ctx.unsafeToFuture(submitApplication(context))),
      Field("createJob", jobPayloadType, arguments = createJobInputArgument :: Nil,
        resolve = context => context.ctx.unsafeToFuture(createJob(context))),
      Field("updateJob", jobPayloadType, arguments = updateJobInputArgument :: Nil,
        resolve = context => context.ctx.unsafeToFuture(updateJob(context))),
      Field("publishJob", jobPayloadType, arguments = jobActionInputArgument :: Nil,
        resolve = context => context.ctx.unsafeToFuture(changeJob(context, _.publishJob))),
      Field("closeJob", jobPayloadType, arguments = jobActionInputArgument :: Nil,
        resolve = context => context.ctx.unsafeToFuture(changeJob(context, _.closeJob))),
      Field("acceptApplication", applicationPayloadType, arguments = applicationActionInputArgument :: Nil,
        resolve = context => context.ctx.unsafeToFuture(applicationStatusAction(context, ApplicationStatus.Accepted))),
      Field("moveApplicationToInterview", applicationPayloadType, arguments = applicationActionInputArgument :: Nil,
        resolve = context => context.ctx.unsafeToFuture(applicationStatusAction(context, ApplicationStatus.Interview))),
      Field("hireApplication", applicationPayloadType, arguments = applicationActionInputArgument :: Nil,
        resolve = context => context.ctx.unsafeToFuture(applicationStatusAction(context, ApplicationStatus.Hired))),
      Field("rejectApplication", applicationPayloadType, arguments = rejectApplicationInputArgument :: Nil,
        resolve = context => context.ctx.unsafeToFuture(rejectApplication(context))),
      Field("declineApplication", applicationPayloadType, arguments = declineApplicationInputArgument :: Nil,
        resolve = context => context.ctx.unsafeToFuture(declineApplication(context)))
      ,Field("signUp", accountPayloadType, arguments = signUpInputArgument :: Nil, resolve = context => context.ctx.unsafeToFuture(signUp(context)))
      ,Field("login", accountPayloadType, arguments = loginInputArgument :: Nil, resolve = context => context.ctx.unsafeToFuture(login(context)))
      ,Field("bootstrapAdmin", accountPayloadType, arguments = bootstrapAdminInputArgument :: Nil, resolve = context => context.ctx.unsafeToFuture(bootstrapAdmin(context)))
      ,Field("updateMyProfile", userPayloadType, arguments = updateProfileInputArgument :: Nil, resolve = context => context.ctx.unsafeToFuture(updateMyProfile(context)))
      ,Field("deleteMyAccount", deleteAccountPayloadType, resolve = context => context.ctx.unsafeToFuture(deleteMyAccount(context)))
    )))
  )

  val sdl: String = SchemaRenderer.renderSchema(schema) + "\n"

  enum Failure {
    case InvalidQuery, Internal
  }

  def execute(
      request: GraphQLRequest,
      service: HealthService,
      requestId: String,
      actor: Option[ActorContext] = None,
      hiring: Option[HiringGraphQLServices] = None,
      ensureHiringReady: IO[Boolean] = IO.pure(true),
      trace: Option[TraceContext] = None
  ): IO[Either[Failure, Json]] =
    RequestContext.resource(service.readiness(Some(requestId)), actor, hiring, ensureHiringReady, trace).use { context =>
      executeInContext(request, context)
    }

  private[api] def executeInContext(request: GraphQLRequest, context: RequestContext): IO[Either[Failure, Json]] =
    IO.executionContext.flatMap { implicit executionContext =>
      IO.fromFuture(IO(Executor.execute(
        schema = schema,
        queryAst = request.document,
        userContext = context,
        variables = request.variables,
        operationName = request.operationName,
        exceptionHandler = ExceptionHandler { case (_, _) => HandledException("Execution failed") },
        queryReducers = QueryReducers,
        errorsLimit = Some(1)
      ))).map { result =>
        Right(if (result.hcursor.downField("errors").succeeded)
          result.mapObject(_.add("errors", Json.arr(Json.obj("message" -> Json.fromString("Execution failed")))))
        else result)
      }.handleError {
        case _: QueryAnalysisError => Left(Failure.InvalidQuery)
        case _                     => Left(Failure.Internal)
      }
    }

  private def edgeType[A](name: String, nodeType: OutputType[A]): ObjectType[RequestContext, Edge[A]] =
    ObjectType(name, fields[RequestContext, Edge[A]](
      Field("node", nodeType, resolve = _.value.node),
      Field("cursor", StringType, resolve = _.value.cursor)))

  private def connectionType[A](name: String, edgeType: OutputType[Edge[A]]): ObjectType[RequestContext, Connection[A]] =
    ObjectType(name, fields[RequestContext, Connection[A]](
      Field("edges", ListType(edgeType), resolve = _.value.edges),
      Field("pageInfo", pageInfoType, resolve = _.value.pageInfo),
      Field("errors", ListType(errorType), resolve = _.value.errors)))

  private def jobs(context: Context[RequestContext, Unit]): IO[Connection[Job]] =
    authenticatedConnection(context) { case (actor, hiring) =>
      page(context.arg(firstArgument), context.arg(afterArgument), CursorCodec.decodeJob).flatMap {
        case Left(error) => IO.pure(graphQLErrorConnection(error))
        case Right((page, requested)) =>
          createdAfter(context.arg(createdAfterArgument)) match {
            case Left(error) => IO.pure(graphQLErrorConnection(error))
            case Right(createdAfter) =>
              val filter = JobSearchFilter(
                context.arg(cityArgument),
                context.arg(skillsArgument).fold(Set.empty[String])(_.toSet),
                createdAfter
              )
              hiring.jobService.searchOpenJobs(actor, filter, page)
                .flatTap(_.fold(_ => IO.unit, jobs => context.ctx.preloadUsers(jobs.map(_.recruiterId))))
                .map(_.fold(errorConnection[Job], jobConnection(_, requested)))
          }
      }
    }

  private def semanticJobSearch(context: Context[RequestContext, Unit]): IO[RankedJobResults] =
    authenticatedSearch(context).flatMap {
      case Left(error) => IO.pure(RankedJobResults(Nil, List(this.error(error))))
      case Right((actor, _, service)) =>
        pageSize(context.arg(firstArgument)).flatMap {
          case Left(error) => IO.pure(RankedJobResults(Nil, List(error)))
          case Right(size) =>
            jobFilter(context.arg(jobFilterArgument)) match {
              case Left(error) => IO.pure(RankedJobResults(Nil, List(error)))
              case Right(filter) =>
                IO.randomUUID.flatMap(searchId =>
                  service.semanticJobSearch(actor, context.arg(queryArgument), filter, size, searchId)
                    .flatTap(_.fold(_ => IO.unit, results => context.ctx.preloadUsers(results.map(_.job.recruiterId))))
                    .map(_.fold(error => RankedJobResults(Nil, List(this.error(error))), rankedJobResults)))
            }
        }
    }

  private def recommendedJobs(context: Context[RequestContext, Unit]): IO[RankedJobResults] =
    authenticatedSearch(context).flatMap {
      case Left(error) => IO.pure(RankedJobResults(Nil, List(this.error(error))))
      case Right((actor, _, service)) =>
        pageSize(context.arg(firstArgument)).flatMap {
          case Left(error) => IO.pure(RankedJobResults(Nil, List(error)))
          case Right(size) =>
            IO.randomUUID.flatMap(searchId =>
              service.recommendedJobs(actor, size, searchId)
                .flatTap(_.fold(_ => IO.unit, results => context.ctx.preloadUsers(results.map(_.job.recruiterId))))
                .map(_.fold(error => RankedJobResults(Nil, List(this.error(error))), rankedJobResults)))
        }
    }

  private def candidateMatches(context: Context[RequestContext, Unit]): IO[RankedCandidateResults] =
    authenticatedSearch(context).flatMap {
      case Left(error) => IO.pure(RankedCandidateResults(Nil, List(this.error(error))))
      case Right((actor, _, service)) =>
        (parseJobId(context.arg(jobIdArgument)), pageSize(context.arg(firstArgument))) match {
          case (Some(jobId), pageIO) => pageIO.flatMap {
            case Left(error) => IO.pure(RankedCandidateResults(Nil, List(error)))
            case Right(size) =>
              IO.randomUUID.flatMap(searchId =>
                service.candidateMatches(actor, jobId, size, searchId)
                  .map(_.fold(error => RankedCandidateResults(Nil, List(this.error(error))), rankedCandidateResults)))
          }
          case (None, _) => IO.pure(RankedCandidateResults(Nil, List(error(DomainError.NotFound("job")))))
        }
    }

  private def job(context: Context[RequestContext, Unit]): IO[Option[Job]] =
    authenticated(context).flatMap {
      case Left(_) => IO.pure(None)
      case Right((actor, hiring)) =>
        parseJobId(context.arg(idArgument)).fold(IO.pure(Option.empty[Job]))(id => hiring.jobService.viewJob(actor, id).map(_.toOption))
    }

  private def myJobs(context: Context[RequestContext, Unit]): IO[Connection[Job]] =
    authenticatedConnection(context) { case (actor, hiring) =>
      page(context.arg(firstArgument), context.arg(afterArgument), CursorCodec.decodeJob).flatMap {
        case Left(error) => IO.pure(graphQLErrorConnection(error))
        case Right((page, requested)) =>
          hiring.jobService.myJobs(actor, page.copy(status = context.arg(jobStatusArgument)))
            .flatTap(_.fold(_ => IO.unit, jobs => context.ctx.preloadUsers(jobs.map(_.recruiterId))))
            .map(_.fold(errorConnection[Job], jobConnection(_, requested)))
      }
    }

  private def myApplications(context: Context[RequestContext, Unit]): IO[Connection[Application]] =
    authenticatedConnection(context) { case (actor, hiring) =>
      applicationPage(context.arg(firstArgument), context.arg(afterArgument), context.arg(applicationStatusArgument)).flatMap {
        case Left(error) => IO.pure(graphQLErrorConnection(error))
        case Right((page, requested)) =>
          hiring.applicationService.myApplications(actor, page)
            .flatTap(_.fold(_ => IO.unit, applications => preloadApplications(context, applications)))
            .map(_.fold(errorConnection[Application], applicationConnection(_, requested)))
      }
    }

  private def jobApplications(context: Context[RequestContext, Unit]): IO[Connection[Application]] =
    authenticatedConnection(context) { case (actor, hiring) =>
      (parseJobId(context.arg(jobIdArgument)), applicationPage(context.arg(firstArgument), context.arg(afterArgument), context.arg(applicationStatusArgument))) match {
        case (Some(jobId), pageIO) => pageIO.flatMap {
          case Left(error) => IO.pure(graphQLErrorConnection(error))
          case Right((page, requested)) =>
            hiring.applicationService.jobApplications(actor, jobId, page)
              .flatTap(_.fold(_ => IO.unit, applications => preloadApplications(context, applications)))
              .map(_.fold(errorConnection[Application], applicationConnection(_, requested)))
        }
        case _ => IO.pure(errorConnection(DomainError.NotFound("job")))
      }
    }

  private def applicationHistory(context: Context[RequestContext, Unit]): IO[Connection[ApplicationEvent]] =
    authenticatedConnection(context) { case (actor, hiring) =>
      parseApplicationId(context.arg(applicationIdArgument)) match {
        case None => IO.pure(errorConnection(DomainError.NotFound("application")))
        case Some(applicationId) =>
          pageEvent(context.arg(firstArgument), context.arg(afterArgument)).flatMap {
            case Left(error) => IO.pure(graphQLErrorConnection(error))
            case Right((page, requested)) =>
              canViewApplication(actor, hiring, applicationId).flatMap {
                case Left(error) => IO.pure(errorConnection(error))
                case Right(()) => hiring.readModel.applicationHistory(applicationId, page).map(eventConnection(_, requested))
              }
          }
      }
    }

  private def submitApplication(context: Context[RequestContext, Unit]): IO[ApplicationPayload] =
    authenticatedPayload(ApplicationPayload(None, _))(context) { case (actor, hiring) =>
      parseJobId(context.arg(submitApplicationInputArgument).jobId) match {
        case None => IO.pure(ApplicationPayload(None, List(error(DomainError.NotFound("job")))))
        case Some(jobId) =>
          IO.realTimeInstant.flatMap(now =>
            IO.randomUUID.flatMap(applicationId =>
              IO.randomUUID.flatMap(eventId =>
                hiring.applicationService.submitApplication(actor, jobId, ApplicationId(applicationId), ApplicationEventId(eventId), now)
                  .map(applicationPayload)
              )
            )
          )
      }
    }

  private def createJob(context: Context[RequestContext, Unit]): IO[JobPayload] =
    authenticatedPayload(JobPayload(None, _))(context) { case (actor, hiring) =>
      jobInput(context.arg(createJobInputArgument), JobStatus.Open).fold(error => IO.pure(jobErrorPayload(error)), input =>
        IO.realTimeInstant.flatMap(now => IO.randomUUID.flatMap(jobId =>
          hiring.jobService.createJob(actor, input, now, JobId(jobId))
            .map(jobPayload)))
      )
    }

  private def updateJob(context: Context[RequestContext, Unit]): IO[JobPayload] =
    authenticatedPayload(JobPayload(None, _))(context) { case (actor, hiring) =>
      val input = context.arg(updateJobInputArgument)
      (parseJobId(input.id), updateInput(input.patch)) match {
        case (Some(jobId), Right(input)) =>
          IO.realTimeInstant.flatMap(now => hiring.jobService.updateJob(actor, jobId, input, now)
            .map(jobPayload))
        case (None, _) => IO.pure(JobPayload(None, List(error(DomainError.NotFound("job")))))
        case (_, Left(error)) => IO.pure(jobErrorPayload(error))
      }
    }

  private def changeJob(
      context: Context[RequestContext, Unit],
      method: JobUseCases[IO] => (ActorContext, JobId, Instant) => IO[Either[UseCaseError, Job]]
  ): IO[JobPayload] =
    authenticatedPayload(JobPayload(None, _))(context) { case (actor, hiring) =>
      parseJobId(context.arg(jobActionInputArgument).jobId) match {
        case None => IO.pure(JobPayload(None, List(error(DomainError.NotFound("job")))))
        case Some(jobId) => IO.realTimeInstant.flatMap(now =>
          method(hiring.jobService)(actor, jobId, now).map(jobPayload))
      }
    }

  private def applicationStatusAction(context: Context[RequestContext, Unit], status: ApplicationStatus): IO[ApplicationPayload] = {
    val input = context.arg(applicationActionInputArgument)
    changeApplicationStatus(context, input.applicationId, status, None, None)
  }

  private def changeApplicationStatus(
      context: Context[RequestContext, Unit],
      applicationIdValue: String,
      status: ApplicationStatus,
      feedback: Option[String],
      reason: Option[String]
  ): IO[ApplicationPayload] =
    authenticatedPayload(ApplicationPayload(None, _))(context) { case (actor, hiring) =>
      parseApplicationId(applicationIdValue) match {
        case None => IO.pure(ApplicationPayload(None, List(error(DomainError.NotFound("application")))))
        case Some(applicationId) =>
          IO.realTimeInstant.flatMap(now => IO.randomUUID.flatMap(eventId =>
            hiring.applicationService.changeStatus(actor, applicationId, status, feedback, reason, ApplicationEventId(eventId), now)
              .map(applicationPayload)))
      }
    }

  private def rejectApplication(context: Context[RequestContext, Unit]): IO[ApplicationPayload] = {
    val input = context.arg(rejectApplicationInputArgument)
    changeApplicationStatus(context, input.applicationId, ApplicationStatus.Rejected, input.feedback, None)
  }

  private def declineApplication(context: Context[RequestContext, Unit]): IO[ApplicationPayload] = {
    val input = context.arg(declineApplicationInputArgument)
    changeApplicationStatus(context, input.applicationId, ApplicationStatus.Declined, None, input.reason)
  }

  private def accountService(context: Context[RequestContext, Unit]): Either[UseCaseError, AccountUseCases[IO]] =
    context.ctx.hiring.flatMap(_.accountService).toRight(AvailabilityError.ServiceNotReady: UseCaseError)

  private def accountMe(context: Context[RequestContext, Unit]): IO[Option[User]] =
    (context.ctx.actor, accountService(context)) match {
      case (Some(actor), Right(service)) => service.me(actor).map(_.toOption)
      case _ => IO.pure(None)
    }

  private def signUp(context: Context[RequestContext, Unit]): IO[AccountPayload] = {
    val input = context.arg(signUpInputArgument)
    (for {
      role <- UserRole.values.find(_.toString.equalsIgnoreCase(input.role)).toRight[UseCaseError](AccountError.ProfileRoleMismatch)
      service <- accountService(context)
      candidateProfile = Option.when(role == UserRole.Candidate)(CandidateProfile(input.skills.getOrElse(Nil).toSet, input.experienceSummary, input.resumeRef))
      recruiterProfile = Option.when(role == UserRole.Recruiter)(RecruiterProfile(input.organizationName.getOrElse(""), input.jobTitle))
    } yield (service, SignUpInput(input.name, role, input.password, candidateProfile, recruiterProfile))).fold(
      error => IO.pure(accountErrorPayload(error)),
      { case (service, request) => IO.realTimeInstant.flatMap(now => IO.randomUUID.flatMap(id => service.signUp(request, now, Identifiers.UserId(id)))).map(accountPayload) }
    )
  }

  private def bootstrapAdmin(context: Context[RequestContext, Unit]): IO[AccountPayload] =
    accountService(context).fold(
      error => IO.pure(accountErrorPayload(error)),
      service => {
        val input = context.arg(bootstrapAdminInputArgument)
        IO.realTimeInstant.flatMap(now => IO.randomUUID.flatMap(id => service.bootstrapAdmin(BootstrapAdminInput(input.name, input.password), now, Identifiers.UserId(id)))).map(accountPayload)
      }
    )

  private def login(context: Context[RequestContext, Unit]): IO[AccountPayload] =
    accountService(context).fold(
      error => IO.pure(accountErrorPayload(error)),
      service => {
        val input = context.arg(loginInputArgument)
        IO.realTimeInstant.flatMap(now => service.login(LoginInput(input.name, input.password), now)).map(accountPayload)
      }
    )

  private def updateMyProfile(context: Context[RequestContext, Unit]): IO[UserPayload] =
    (context.ctx.actor, accountService(context)) match {
      case (Some(actor), Right(service)) =>
        val input = context.arg(updateProfileInputArgument)
        val candidate = input.skills.map(skills => CandidateProfile(skills.toSet, input.experienceSummary, input.resumeRef))
        val recruiter = input.organizationName.map(name => RecruiterProfile(name, input.jobTitle))
        service.updateMyProfile(actor, AccountProfileInput(candidate, recruiter)).map(_.fold(error => UserPayload(None, List(this.error(error))), user => UserPayload(Some(user), Nil)))
      case _ => IO.pure(UserPayload(None, List(GraphQLError("UNAUTHORIZED", "Authentication required"))))
    }

  private def deleteMyAccount(context: Context[RequestContext, Unit]): IO[DeleteAccountPayload] =
    (context.ctx.actor, accountService(context)) match {
      case (Some(actor), Right(service)) => IO.realTimeInstant.flatMap(now => service.deleteMyAccount(actor, now)).map(_.fold(error => DeleteAccountPayload(false, List(this.error(error))), _ => DeleteAccountPayload(true, Nil)))
      case _ => IO.pure(DeleteAccountPayload(false, List(GraphQLError("UNAUTHORIZED", "Authentication required"))))
    }

  private def users(context: Context[RequestContext, Unit]): IO[Connection[User]] =
    (context.ctx.actor, accountService(context)) match {
      case (Some(actor), Right(service)) =>
        val requested = context.arg(firstArgument)
        val status = context.arg(userStatusArgument).getOrElse(AccountStatus.Active)
        val role = context.arg(userRoleArgument)
        userPage(requested, context.arg(afterArgument), status, role).flatMap {
          case Left(validationError) => IO.pure(graphQLErrorConnection(validationError))
          case Right((request, pageSize)) =>
            service.listUsers(actor, request).map(_.fold(errorConnection[User], values => userConnection(values, pageSize)))
        }
      case _ => IO.pure(errorConnection(AuthenticationError.Unauthorized))
    }

  private def authenticated(context: Context[RequestContext, Unit]): IO[Either[UseCaseError, (ActorContext, HiringGraphQLServices)]] =
    (context.ctx.actor, context.ctx.hiring) match {
      case (Some(actor), Some(hiring)) =>
        context.ctx.hiringAvailable.map { ready =>
          if (ready) Right((actor, hiring))
          else Left(AvailabilityError.ServiceNotReady: UseCaseError)
        }
      case _ => IO.pure(Left(AuthenticationError.Unauthorized: UseCaseError))
    }

  private def authenticatedSearch(
      context: Context[RequestContext, Unit]
  ): IO[Either[UseCaseError, (ActorContext, HiringGraphQLServices, SearchUseCases[IO])]] =
    authenticated(context).map(_.flatMap { case (actor, hiring) =>
      hiring.semanticSearchService.map(service => (actor, hiring, service)).toRight(SearchError.VectorSearchUnavailable: UseCaseError)
    })

  private def authenticatedConnection[A](
      context: Context[RequestContext, Unit]
  )(action: (ActorContext, HiringGraphQLServices) => IO[Connection[A]]): IO[Connection[A]] =
    authenticated(context).flatMap(_.fold(error => IO.pure(errorConnection[A](error)), action.tupled))

  private def authenticatedPayload[A](
      unauthenticated: List[GraphQLError] => A
  )(context: Context[RequestContext, Unit])(
      action: (ActorContext, HiringGraphQLServices) => IO[A]
  ): IO[A] =
    authenticated(context).flatMap(_.fold(error => IO.pure(unauthenticated(List(this.error(error)))), action.tupled))

  private def page(
      first: Int,
      after: Option[String],
      decode: String => Option[JobCursor]
  ): IO[Either[GraphQLError, (JobPageRequest, Int)]] =
    cursorPage(first, after, decode)((cursor, size) => JobPageRequest(None, cursor, size))

  private def pageEvent(first: Int, after: Option[String]): IO[Either[GraphQLError, (ApplicationEventPageRequest, Int)]] =
    cursorPage(first, after, CursorCodec.decodeEvent)((cursor, size) => ApplicationEventPageRequest(cursor, size))

  private def applicationPage(
      first: Int,
      after: Option[String],
      status: Option[ApplicationStatus]
  ): IO[Either[GraphQLError, (ApplicationPageRequest, Int)]] =
    cursorPage(first, after, CursorCodec.decodeApplication)((cursor, size) => ApplicationPageRequest(status, cursor, size))

  private def userPage(
      first: Int,
      after: Option[String],
      status: AccountStatus,
      role: Option[UserRole]
  ): IO[Either[GraphQLError, (UserPageRequest, Int)]] =
    cursorPage(first, after, CursorCodec.decodeUser)((cursor, size) => UserPageRequest(status, role, cursor, size.value))

  private def cursorPage[A, B](
      first: Int,
      after: Option[String],
      decode: String => Option[A]
  )(build: (Option[A], PageSize) => B): IO[Either[GraphQLError, (B, Int)]] =
    pageSize(first).map(_.map { size =>
      build(after.flatMap(decode), PageSize.next(size)) -> size.value
    }).map(validateCursor(after, decode))

  private def pageSize(first: Int): IO[Either[GraphQLError, PageSize]] =
    IO.pure(PageSize.fromInt(first).toEither.leftMap(error))

  private def jobFilter(value: Option[JobFilterGraphQLInput]): Either[GraphQLError, JobSearchFilter] =
    value match {
      case None => Right(JobSearchFilter(None, Set.empty, None))
      case Some(filter) =>
        createdAfter(filter.createdAfter).map(createdAt =>
          JobSearchFilter(filter.city, filter.skills.fold(Set.empty[String])(_.toSet), createdAt))
    }

  private def validateCursor[A, B](
      after: Option[String],
      decode: String => Option[A]
  )(page: Either[GraphQLError, (B, Int)]): Either[GraphQLError, (B, Int)] =
    after match {
      case Some(raw) if decode(raw).isEmpty => Left(GraphQLError("INVALID_CURSOR", "Invalid cursor"))
      case _ => page
    }

  private def createdAfter(value: Option[String]): Either[GraphQLError, Option[Instant]] =
    value.traverse(raw =>
      Either.catchNonFatal(Instant.parse(raw))
        .leftMap(_ => GraphQLError("INVALID_CREATED_AFTER", "createdAfter must be an ISO-8601 instant"))
    )

  private def jobInput(input: JobGraphQLInput, status: JobStatus): Either[UseCaseError, CreateJobInput] =
    location(input).map(location => CreateJobInput(
      input.title,
      input.description,
      input.requirements,
      input.skills.toSet,
      location,
      status
    ))

  private def updateInput(input: JobGraphQLInput): Either[UseCaseError, UpdateJobInput] =
    location(input).map(location => UpdateJobInput(
      input.title,
      input.description,
      input.requirements,
      input.skills.toSet,
      location
    ))

  private def location(input: JobGraphQLInput): Either[UseCaseError, Location] =
    Location.validate(input.country, input.city.getOrElse(""), input.remote).toEither.leftMap(identity)

  private def parseJobId(value: String): Option[JobId] =
    Either.catchNonFatal(JobId(UUID.fromString(value))).toOption

  private def parseApplicationId(value: String): Option[ApplicationId] =
    Either.catchNonFatal(ApplicationId(UUID.fromString(value))).toOption

  private def canViewApplication(
      actor: ActorContext,
      hiring: HiringGraphQLServices,
      applicationId: ApplicationId
  ): IO[Either[UseCaseError, Unit]] =
    hiring.readModel.canViewApplication(actor, applicationId)

  private def preloadApplications(context: Context[RequestContext, Unit], applications: List[Application]): IO[Unit] =
    context.ctx.preloadUsers(applications.map(_.candidateId)) *>
      context.ctx.preloadJobs(applications.map(_.jobId)) *>
      applications.traverse(application => context.ctx.job(application.jobId)).flatMap(jobs =>
        context.ctx.preloadUsers(jobs.flatten.map(_.recruiterId)))

  private def jobConnection(values: List[Job], requested: Int): Connection[Job] = {
    connection(values, requested)(job => CursorCodec.encodeJob(JobCursor(job.createdAt, job.id)))
  }

  private def applicationConnection(values: List[Application], requested: Int): Connection[Application] = {
    connection(values, requested)(application => CursorCodec.encodeApplication(ApplicationCursor(application.createdAt, application.id)))
  }

  private def eventConnection(values: List[ApplicationEvent], requested: Int): Connection[ApplicationEvent] = {
    connection(values, requested)(event => CursorCodec.encodeEvent(ApplicationEventCursor(event.occurredAt, event.id)))
  }

  private def rankedJobResults(values: List[RankedJob]): RankedJobResults =
    RankedJobResults(values.map(value => RankedJobPayload(
      value.job,
      value.score,
      value.mode,
      value.meta.model,
      value.meta.version,
      value.searchId.toString
    )), Nil)

  private def rankedCandidateResults(values: List[RankedCandidate]): RankedCandidateResults =
    RankedCandidateResults(values.map(value => RankedCandidatePayload(
      CandidateMatchCandidate(
        value.candidate.id.value.toString,
        value.candidate.name,
        value.candidate.profile.map(profile => CandidateMatchProfile(profile.skills, profile.experienceSummary))
      ),
      value.score,
      value.mode,
      value.meta.model,
      value.meta.version,
      value.searchId.toString
    )), Nil)

  private def connection[A](values: List[A], requested: Int)(cursor: A => String): Connection[A] = {
    val nodes = values.take(requested)
    Connection(nodes.map(value => Edge(value, cursor(value))), PageInfo(values.size > requested, nodes.lastOption.map(cursor)))
  }

  private def errorConnection[A](error: UseCaseError): Connection[A] =
    Connection(Nil, PageInfo(false, None), List(this.error(error)))

  private def graphQLErrorConnection[A](error: GraphQLError): Connection[A] =
    Connection(Nil, PageInfo(false, None), List(error))

  private def jobPayload(result: Either[UseCaseError, Job]): JobPayload =
    result.fold(jobErrorPayload, job => JobPayload(Some(job), Nil))

  private def jobErrorPayload(error: UseCaseError): JobPayload =
    JobPayload(None, List(this.error(error)))

  private def applicationPayload(result: Either[UseCaseError, Application]): ApplicationPayload =
    result.fold(applicationErrorPayload, application => ApplicationPayload(Some(application), Nil))

  private def applicationErrorPayload(error: UseCaseError): ApplicationPayload =
    ApplicationPayload(None, List(this.error(error)))

  private def accountPayload(result: Either[UseCaseError, (User, AccountToken)]): AccountPayload =
    result.fold(accountErrorPayload, { case (user, token) => AccountPayload(Some(user), Some(token.value), Some(token.expiresAt.toString), Nil) })

  private def accountErrorPayload(error: UseCaseError): AccountPayload =
    AccountPayload(None, None, None, List(this.error(error)))

  private def userConnection(values: List[User], requested: Int): Connection[User] =
    connection(values, requested)(user => CursorCodec.encodeUser(UserCursor(user.createdAt, user.id)))

  private def error(error: UseCaseError): GraphQLError =
    error match {
      case AuthenticationError.Unauthorized => GraphQLError("UNAUTHORIZED", "Authentication required")
      case AuthenticationError.SingletonAdminViolation => GraphQLError("FORBIDDEN", "Forbidden")
      case AccountError.BootstrapRequired => GraphQLError("ADMIN_BOOTSTRAP_REQUIRED", "The first Admin must be bootstrapped")
      case AccountError.AlreadyBootstrapped => GraphQLError("ADMIN_ALREADY_BOOTSTRAPPED", "Admin bootstrap is already complete")
      case AccountError.NameTaken => GraphQLError("NAME_TAKEN", "Name is already in use")
      case AccountError.InvalidCredentials => GraphQLError("INVALID_CREDENTIALS", "Invalid credentials")
      case AccountError.DeletedAccount => GraphQLError("UNAUTHORIZED", "Authentication required")
      case AccountError.ProfileRoleMismatch => GraphQLError("PROFILE_ROLE_MISMATCH", "Profile does not match the selected role")
      case AccountError.PasswordPolicyViolation => GraphQLError("INVALID_PASSWORD", "Password does not meet policy")
      case AccountError.AccountAlreadyDeleted => GraphQLError("ACCOUNT_ALREADY_DELETED", "Account is already deleted")
      case AccountError.AdminSignupForbidden => GraphQLError("ADMIN_BOOTSTRAP_ONLY", "Admin accounts can only be created through bootstrap")
      case AvailabilityError.ServiceNotReady => GraphQLError("SERVICE_NOT_READY", "Service not ready")
      case DomainError.Forbidden => GraphQLError("FORBIDDEN", "Forbidden")
      case DomainError.NotFound(entity) => GraphQLError("NOT_FOUND", s"$entity not found")
      case DomainError.DuplicateApplication => GraphQLError("DUPLICATE_APPLICATION", "Application already exists")
      case DomainError.JobMustBeOpen => GraphQLError("JOB_MUST_BE_OPEN", "Job must be open")
      case DomainError.CandidateRequired => GraphQLError("CANDIDATE_REQUIRED", "Candidate role required")
      case DomainError.RecruiterRequired => GraphQLError("RECRUITER_REQUIRED", "Recruiter role required")
      case DomainError.InvalidJobTransition(_, _) => GraphQLError("INVALID_JOB_TRANSITION", "Invalid job transition")
      case DomainError.InvalidStatusTransition(_, _) => GraphQLError("INVALID_STATUS_TRANSITION", "Invalid application status transition")
      case DomainError.RejectionFeedbackRequired => GraphQLError("REJECTION_FEEDBACK_REQUIRED", "Rejection feedback is required")
      case DomainError.DeclineReasonRequired => GraphQLError("DECLINE_REASON_REQUIRED", "Decline reason is required")
      case RepositoryError.DuplicateApplication => GraphQLError("DUPLICATE_APPLICATION", "Application already exists")
      case RepositoryError.Conflict => GraphQLError("CONFLICT", "Conflict")
      case RepositoryError.Unavailable => GraphQLError("UNAVAILABLE", "Repository unavailable")
      case SearchError.MissingEmbedding(entity) => GraphQLError("MISSING_EMBEDDING", s"$entity embedding is missing")
      case SearchError.StaleEmbedding(entity) => GraphQLError("STALE_EMBEDDING", s"$entity embedding is stale")
      case SearchError.InputTooLarge(field, maximum) => GraphQLError("INPUT_TOO_LARGE", s"$field must be at most $maximum characters")
      case SearchError.ProviderUnavailable => GraphQLError("PROVIDER_UNAVAILABLE", "Embedding provider unavailable")
      case SearchError.VectorSearchUnavailable => GraphQLError("VECTOR_SEARCH_UNAVAILABLE", "Vector search unavailable")
      case errors: cats.data.NonEmptyList[?] =>
        val fields = errors.toList.collect { case validation: DomainValidationError => validation }
          .map(validationErrorMessage).mkString(", ")
        GraphQLError("VALIDATION_FAILED", if (fields.isEmpty) "Validation failed" else fields)
    }

  private def validationErrorMessage(error: DomainValidationError): String =
    error match {
      case DomainValidationError.BlankField(field) => s"$field is required"
      case DomainValidationError.EmptyCollection(field) => s"$field must not be empty"
      case DomainValidationError.InvalidNumber(field, minimum, maximum, _) => s"$field must be between $minimum and $maximum"
    }
}
