package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.application.{AuthenticationError, HealthService, ProbeResult, UseCaseError}
import com.example.graphQL.cats.application.port.*
import com.example.graphQL.cats.application.service.{ActorAuthorization, CreateJobInput, JobService, UpdateJobInput}
import com.example.graphQL.cats.domain.error.{DomainError, DomainValidationError}
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId}
import com.example.graphQL.cats.domain.model.*
import io.circe.{Decoder, Json}
import sangria.execution.{ExceptionHandler, Executor, HandledException, QueryAnalysisError}
import sangria.marshalling.circe.*
import sangria.renderer.SchemaRenderer
import sangria.schema.*

import java.time.Instant
import java.util.UUID

object HiringGraphQLSchema {
  private final case class GraphQLError(code: String, message: String)
  private final case class PageInfo(hasNextPage: Boolean, endCursor: Option[String])
  private final case class Edge[A](node: A, cursor: String)
  private final case class Connection[A](edges: List[Edge[A]], pageInfo: PageInfo, errors: List[GraphQLError] = Nil)
  private final case class JobPayload(job: Option[Job], errors: List[GraphQLError])
  private final case class ApplicationPayload(application: Option[Application], errors: List[GraphQLError])
  private final case class SubmitApplicationGraphQLInput(jobId: String)
  private final case class JobGraphQLInput(
      title: String,
      description: String,
      requirements: List[String],
      inputSkills: List[String],
      country: String,
      city: Option[String],
      remote: Boolean
  )
  private final case class UpdateJobGraphQLInput(id: String, patch: JobGraphQLInput)
  private final case class JobActionGraphQLInput(jobId: String)
  private final case class ApplicationActionGraphQLInput(applicationId: String)
  private final case class RejectApplicationGraphQLInput(applicationId: String, feedback: Option[String])
  private final case class DeclineApplicationGraphQLInput(applicationId: String, reason: Option[String])

  private given Decoder[SubmitApplicationGraphQLInput] =
    Decoder.forProduct1("jobId")(SubmitApplicationGraphQLInput.apply)
  private given Decoder[JobGraphQLInput] =
    Decoder.forProduct7("title", "description", "requirements", "inputSkills", "country", "city", "remote")(JobGraphQLInput.apply)
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

  private val healthStatus = EnumType("HealthStatus", values = List(EnumValue("UP", value = "UP")))
  private val readinessStatus = EnumType("ReadinessStatus", values = List(
    EnumValue("READY", value = "READY"), EnumValue("NOT_READY", value = "NOT_READY")))
  private val jobStatus = EnumType("JobStatus", values = JobStatus.values.toList.map(status => EnumValue(status.toString, value = status)))
  private val applicationStatus =
    EnumType("ApplicationStatus", values = ApplicationStatus.values.toList.map(status => EnumValue(status.toString, value = status)))
  private val userRole = EnumType("UserRole", values = UserRole.values.toList.map(role => EnumValue(role.toString, value = role)))

  private val idArgument = Argument("id", IDType)
  private val jobIdArgument = Argument("jobId", IDType)
  private val applicationIdArgument = Argument("applicationId", IDType)
  private val firstArgument = Argument("first", IntType)
  private val afterArgument = Argument("after", OptionInputType(StringType))
  private val cityArgument = Argument("city", OptionInputType(StringType))
  private val skillsArgument = Argument("skills", OptionInputType(ListInputType(StringType)))
  private val createdAfterArgument = Argument("createdAfter", OptionInputType(StringType))
  private val jobStatusArgument = Argument("status", OptionInputType(jobStatus))
  private val applicationStatusArgument = Argument("status", OptionInputType(applicationStatus))
  private val submitApplicationInputType = InputObjectType[SubmitApplicationGraphQLInput]("SubmitApplicationInput", List(
    InputField("jobId", IDType)
  ))
  private val jobInputType = InputObjectType[JobGraphQLInput]("JobInput", List(
    InputField("title", StringType),
    InputField("description", StringType),
    InputField("requirements", ListInputType(StringType)),
    InputField("inputSkills", ListInputType(StringType)),
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
  private lazy val userType: ObjectType[RequestContext, User] = ObjectType("User", fields[RequestContext, User](
    Field("id", IDType, resolve = _.value.id.value.toString),
    Field("email", StringType, resolve = _.value.email),
    Field("name", StringType, resolve = _.value.name),
    Field("role", userRole, resolve = _.value.role),
    Field("profile", OptionType(candidateProfileType), resolve = _.value.profile),
    Field("createdAt", StringType, resolve = _.value.createdAt.toString)))
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
  private lazy val jobPayloadType = ObjectType("JobPayload", fields[RequestContext, JobPayload](
    Field("job", OptionType(jobType), resolve = _.value.job),
    Field("errors", ListType(errorType), resolve = _.value.errors)))
  private lazy val applicationPayloadType = ObjectType("ApplicationPayload", fields[RequestContext, ApplicationPayload](
    Field("application", OptionType(applicationType), resolve = _.value.application),
    Field("errors", ListType(errorType), resolve = _.value.errors)))

  val schema: Schema[RequestContext, Unit] = Schema(
    ObjectType("Query", fields[RequestContext, Unit](
      Field("health", healthType, resolve = _ => ()),
      Field("readiness", readinessType, resolve = context => context.ctx.readiness),
      Field("jobs", jobConnectionType,
        arguments = firstArgument :: afterArgument :: cityArgument :: skillsArgument :: createdAfterArgument :: Nil,
        resolve = context => context.ctx.unsafeToFuture(jobs(context))),
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
    )))
  )

  val sdl: String = SchemaRenderer.renderSchema(schema) + "\n"

  enum Failure {
    case InvalidQuery, Internal
  }

  def execute(request: GraphQLRequest, service: HealthService, requestId: String): IO[Either[Failure, Json]] =
    RequestContext.resource(service.readiness(Some(requestId))).use { context =>
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
    withHiringConnection(context) { hiring =>
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
              hiring.jobs.findOpen(filter, page).flatTap(jobs => context.ctx.preloadUsers(jobs.map(_.recruiterId))).map(jobConnection(_, requested))
          }
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
          hiring.jobs.findByRecruiter(actor.userId, page.copy(status = context.arg(jobStatusArgument)))
            .flatTap(jobs => context.ctx.preloadUsers(jobs.map(_.recruiterId)))
            .map(jobConnection(_, requested))
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
                case Right(()) => hiring.applications.history(applicationId, page).map(eventConnection(_, requested))
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
                  .map(_.fold(error => ApplicationPayload(None, List(this.error(error))), application => ApplicationPayload(Some(application), Nil)))
              )
            )
          )
      }
    }

  private def createJob(context: Context[RequestContext, Unit]): IO[JobPayload] =
    authenticatedPayload(JobPayload(None, _))(context) { case (actor, hiring) =>
      jobInput(context.arg(createJobInputArgument), JobStatus.Open).fold(error => IO.pure(JobPayload(None, List(this.error(error)))), input =>
        IO.realTimeInstant.flatMap(now => IO.randomUUID.flatMap(jobId =>
          hiring.jobService.createJob(actor, input, now, JobId(jobId))
            .map(_.fold(error => JobPayload(None, List(this.error(error))), job => JobPayload(Some(job), Nil)))))
      )
    }

  private def updateJob(context: Context[RequestContext, Unit]): IO[JobPayload] =
    authenticatedPayload(JobPayload(None, _))(context) { case (actor, hiring) =>
      val input = context.arg(updateJobInputArgument)
      (parseJobId(input.id), updateInput(input.patch)) match {
        case (Some(jobId), Right(input)) =>
          IO.realTimeInstant.flatMap(now => hiring.jobService.updateJob(actor, jobId, input, now)
            .map(_.fold(error => JobPayload(None, List(this.error(error))), job => JobPayload(Some(job), Nil))))
        case (None, _) => IO.pure(JobPayload(None, List(error(DomainError.NotFound("job")))))
        case (_, Left(error)) => IO.pure(JobPayload(None, List(this.error(error))))
      }
    }

  private def changeJob(
      context: Context[RequestContext, Unit],
      method: JobService[IO] => (com.example.graphQL.cats.application.ActorContext, JobId, Instant) => IO[Either[UseCaseError, Job]]
  ): IO[JobPayload] =
    authenticatedPayload(JobPayload(None, _))(context) { case (actor, hiring) =>
      parseJobId(context.arg(jobActionInputArgument).jobId) match {
        case None => IO.pure(JobPayload(None, List(error(DomainError.NotFound("job")))))
        case Some(jobId) => IO.realTimeInstant.flatMap(now =>
          method(hiring.jobService)(actor, jobId, now).map(_.fold(error => JobPayload(None, List(this.error(error))), job => JobPayload(Some(job), Nil))))
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
              .map(_.fold(error => ApplicationPayload(None, List(this.error(error))), application => ApplicationPayload(Some(application), Nil)))))
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

  private def withHiringConnection[A](context: Context[RequestContext, Unit])(action: HiringGraphQLServices => IO[Connection[A]]): IO[Connection[A]] =
    context.ctx.hiring.fold(IO.pure(errorConnection[A](AuthenticationError.Unauthorized)))(action)

  private def authenticated(context: Context[RequestContext, Unit]): IO[Either[UseCaseError, (com.example.graphQL.cats.application.ActorContext, HiringGraphQLServices)]] =
    IO.pure((context.ctx.actor, context.ctx.hiring).mapN((_, _)).toRight(AuthenticationError.Unauthorized: UseCaseError))

  private def authenticatedConnection[A](
      context: Context[RequestContext, Unit]
  )(action: (com.example.graphQL.cats.application.ActorContext, HiringGraphQLServices) => IO[Connection[A]]): IO[Connection[A]] =
    authenticated(context).flatMap(_.fold(error => IO.pure(errorConnection[A](error)), action.tupled))

  private def authenticatedPayload[A](
      unauthenticated: List[GraphQLError] => A
  )(context: Context[RequestContext, Unit])(
      action: (com.example.graphQL.cats.application.ActorContext, HiringGraphQLServices) => IO[A]
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
      input.inputSkills.toSet,
      location,
      status
    ))

  private def updateInput(input: JobGraphQLInput): Either[UseCaseError, UpdateJobInput] =
    location(input).map(location => UpdateJobInput(
      input.title,
      input.description,
      input.requirements,
      input.inputSkills.toSet,
      location
    ))

  private def location(input: JobGraphQLInput): Either[UseCaseError, Location] =
    Location.validate(input.country, input.city.getOrElse(""), input.remote).toEither.leftMap(identity)

  private def parseJobId(value: String): Option[JobId] =
    Either.catchNonFatal(JobId(UUID.fromString(value))).toOption

  private def parseApplicationId(value: String): Option[ApplicationId] =
    Either.catchNonFatal(ApplicationId(UUID.fromString(value))).toOption

  private def canViewApplication(
      actor: com.example.graphQL.cats.application.ActorContext,
      hiring: HiringGraphQLServices,
      applicationId: ApplicationId
  ): IO[Either[UseCaseError, Unit]] =
    ActorAuthorization[IO](hiring.users).resolve(actor).flatMap {
      case Left(error) => IO.pure(Left(error))
      case Right(user) =>
        hiring.applications.find(applicationId).flatMap {
          case None => IO.pure(Left(DomainError.NotFound("application")))
          case Some(application) if application.candidateId == user.id && user.role == UserRole.Candidate => IO.pure(Right(()))
          case Some(_) if user.role == UserRole.Admin && user.adminSingleton => IO.pure(Right(()))
          case Some(application) if user.role == UserRole.Recruiter =>
            hiring.jobs.find(application.jobId).map {
              case Some(job) if job.recruiterId == user.id => Right(())
              case Some(_) => Left(DomainError.Forbidden)
              case None => Left(DomainError.NotFound("job"))
            }
          case Some(_) => IO.pure(Left(DomainError.Forbidden))
        }
    }

  private def preloadApplications(context: Context[RequestContext, Unit], applications: List[Application]): IO[Unit] =
    context.ctx.preloadUsers(applications.map(_.candidateId)) *> context.ctx.preloadJobs(applications.map(_.jobId))

  private def jobConnection(values: List[Job], requested: Int): Connection[Job] = {
    connection(values, requested)(job => CursorCodec.encodeJob(JobCursor(job.createdAt, job.id)))
  }

  private def applicationConnection(values: List[Application], requested: Int): Connection[Application] = {
    connection(values, requested)(application => CursorCodec.encodeApplication(ApplicationCursor(application.createdAt, application.id)))
  }

  private def eventConnection(values: List[ApplicationEvent], requested: Int): Connection[ApplicationEvent] = {
    connection(values, requested)(event => CursorCodec.encodeEvent(ApplicationEventCursor(event.occurredAt, event.id)))
  }

  private def connection[A](values: List[A], requested: Int)(cursor: A => String): Connection[A] = {
    val nodes = values.take(requested)
    Connection(nodes.map(value => Edge(value, cursor(value))), PageInfo(values.size > requested, nodes.lastOption.map(cursor)))
  }

  private def errorConnection[A](error: UseCaseError): Connection[A] =
    Connection(Nil, PageInfo(false, None), List(this.error(error)))

  private def graphQLErrorConnection[A](error: GraphQLError): Connection[A] =
    Connection(Nil, PageInfo(false, None), List(error))

  private def error(error: UseCaseError): GraphQLError =
    error match {
      case AuthenticationError.Unauthorized => GraphQLError("UNAUTHORIZED", "Authentication required")
      case AuthenticationError.SingletonAdminViolation => GraphQLError("FORBIDDEN", "Forbidden")
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
