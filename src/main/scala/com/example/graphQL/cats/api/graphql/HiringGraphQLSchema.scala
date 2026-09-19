package com.example.graphQL.cats.api.graphql

import cats.data.{EitherT, ValidatedNel}
import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.service.{AccountError, ActorContext, AuthenticationError, AvailabilityError, HealthService, ProbeResult, RepositoryError, SearchError, UseCaseError}
import com.example.graphQL.cats.domain.error.{DomainError, DomainValidationError}
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.service.job.{CreateJobInput, UpdateJobInput}
import com.example.graphQL.cats.service.protocol.{AccountProfileInput, AccountUseCases, BootstrapAdminInput, LoginInput, SignUpInput, JobUseCases, SearchUseCases}
import com.example.graphQL.cats.shared.pagination.*
import com.example.graphQL.cats.shared.search.{JobSearchFilter, RankedCandidate, RankedJob}
import io.circe.{Decoder, Json}
import io.circe.derivation.{ConfiguredDecoder, Configuration}
import sangria.execution.{ExceptionHandler, Executor, HandledException, QueryAnalysisError, QueryReducer}
import sangria.execution.deferred.{DeferredResolver, Fetcher, HasId}
import sangria.marshalling.circe.*
import sangria.renderer.SchemaRenderer
import sangria.schema.*
import sangria.schema.Action.deferredAction
import scala.concurrent.ExecutionContext.Implicits.global
import sangria.validation.ValueCoercionViolation

import java.time.Instant
import java.util.UUID
import java.util.Locale
import scala.util.Try

object HiringGraphQLSchema {
  private given Configuration = Configuration.default
  private given Decoder[UserRole] = Decoder.decodeString.emap { raw =>
    UserRole.values.find(_.toString.equalsIgnoreCase(raw)).toRight(s"Unknown user role: $raw")
  }
  private given Decoder[JobId] = Decoder.decodeString.emap(raw =>
    Try(JobId(UUID.fromString(raw))).toEither.left.map(_ => "Invalid JobID"))
  private given Decoder[ApplicationId] = Decoder.decodeString.emap(raw =>
    Try(ApplicationId(UUID.fromString(raw))).toEither.left.map(_ => "Invalid ApplicationID"))
  private given Decoder[Instant] = Decoder.decodeString.emap(raw =>
    Try(Instant.parse(raw)).toEither.left.map(_ => "Invalid Instant"))
  private val MaxQueryDepth = 16
  private val MaxQueryComplexity = 1000d
  private final case class QueryComplexityExceeded(limit: Double)
      extends IllegalArgumentException(s"Query complexity exceeds $limit")
  private val QueryReducers = List(
    QueryReducer.rejectMaxDepth[RequestContext](MaxQueryDepth),
    QueryReducer.rejectComplexQueries[RequestContext](MaxQueryComplexity, (_, _) =>
      QueryComplexityExceeded(MaxQueryComplexity))
  )

  private final case class GraphQLError(code: String, message: String)
  private final case class IdCoercionViolation(typeName: String)
      extends ValueCoercionViolation(s"Invalid $typeName value")
  private final case class InstantCoercionViolation()
      extends ValueCoercionViolation("Invalid Instant value; expected ISO-8601")
  private given HasId[User, UserId] = HasId(_.id)
  private given HasId[Job, JobId] = HasId(_.id)
  private given HasId[EmailVisibility, UserId] = HasId(_.userId)
  private val usersFetcher = Fetcher.caching[RequestContext, User, UserId] { (context, ids) =>
    context.unsafeToFuture(context.users(ids.toList))
  }
  private val jobsFetcher = Fetcher.caching[RequestContext, Job, JobId] { (context, ids) =>
    context.unsafeToFuture(context.jobs(ids.toList))
  }
  private val emailVisibilityFetcher = Fetcher.caching[RequestContext, EmailVisibility, UserId] { (context, ids) =>
    context.unsafeToFuture(context.visibleEmailUsers(ids.toList))
  }
  private final case class PageInfo(hasNextPage: Boolean, endCursor: Option[String])
  private final case class Edge[A](node: A, cursor: String)
  private final case class Connection[A](edges: List[Edge[A]], pageInfo: PageInfo, errors: List[GraphQLError] = Nil)
  private final case class JobPayload(job: Option[Job], errors: List[GraphQLError])
  private final case class ApplicationPayload(application: Option[Application], errors: List[GraphQLError])
  private final case class AccountPayload(user: Option[User], accessToken: Option[String], expiresAt: Option[String], errors: List[GraphQLError])
  private final case class UserPayload(user: Option[User], errors: List[GraphQLError])
  private final case class DeleteAccountPayload(deleted: Boolean, errors: List[GraphQLError])
  private final case class JobFilterGraphQLInput(city: Option[String], skills: Option[List[String]], createdAfter: Option[Instant]) derives ConfiguredDecoder
  private final case class CandidateMatchProfile(skills: Set[String], experienceSummary: Option[String])
  private final case class CandidateMatchCandidate(id: String, name: String, profile: Option[CandidateMatchProfile])
  private enum GraphQLUserProfile {
    case Candidate(value: CandidateProfile)
    case Recruiter(value: RecruiterProfile)
  }
  private final case class RankedJobPayload(job: Job, score: Double, searchMode: SearchMode, model: String, version: Int, searchId: String)
  private final case class RankedCandidatePayload(candidate: CandidateMatchCandidate, score: Double, searchMode: SearchMode, model: String, version: Int, searchId: String)
  private final case class RankedJobResults(results: List[RankedJobPayload], errors: List[GraphQLError])
  private final case class RankedCandidateResults(results: List[RankedCandidatePayload], errors: List[GraphQLError])
  private type GraphQLStep[A] = EitherT[IO, GraphQLError, A]
  private final case class SubmitApplicationGraphQLInput(jobId: JobId) derives ConfiguredDecoder
  private final case class JobGraphQLInput(
      title: String,
      description: String,
      requirements: List[String],
      skills: List[String],
      country: String,
      city: Option[String],
      remote: Boolean
  ) derives ConfiguredDecoder
  private final case class UpdateJobGraphQLInput(id: JobId, patch: JobGraphQLInput) derives ConfiguredDecoder
  private final case class JobActionGraphQLInput(jobId: JobId) derives ConfiguredDecoder
  private final case class ApplicationActionGraphQLInput(applicationId: ApplicationId) derives ConfiguredDecoder
  private final case class RejectApplicationGraphQLInput(applicationId: ApplicationId, feedback: Option[String]) derives ConfiguredDecoder
  private final case class DeclineApplicationGraphQLInput(applicationId: ApplicationId, reason: Option[String]) derives ConfiguredDecoder
  private final case class SignUpGraphQLInput(name: String, role: UserRole, password: String, skills: Option[List[String]], experienceSummary: Option[String], resumeRef: Option[String], organizationName: Option[String], jobTitle: Option[String]) derives ConfiguredDecoder
  private final case class BootstrapAdminGraphQLInput(name: String, password: String) derives ConfiguredDecoder
  private final case class LoginGraphQLInput(name: String, password: String) derives ConfiguredDecoder
  private final case class UpdateProfileGraphQLInput(skills: Option[List[String]], experienceSummary: Option[String], resumeRef: Option[String], organizationName: Option[String], jobTitle: Option[String]) derives ConfiguredDecoder

  private val healthStatus = EnumType("HealthStatus", values = List(EnumValue("UP", value = "UP")))
  private val readinessStatus = EnumType("ReadinessStatus", values = List(
    EnumValue("READY", value = "READY"), EnumValue("NOT_READY", value = "NOT_READY")))
  private val jobStatus = EnumType("JobStatus", values = JobStatus.values.toList.map(status => EnumValue(status.toString, value = status)))
  private val applicationStatus =
    EnumType("ApplicationStatus", values = ApplicationStatus.values.toList.map(status => EnumValue(status.toString, value = status)))
  private val userRole = EnumType("UserRole", values = UserRole.values.toList.map(role => EnumValue(role.toString, value = role)))
  private val userStatus = EnumType("UserStatus", values = AccountStatus.values.toList.map(status => EnumValue(status.toString.toUpperCase(Locale.ROOT), value = status)))
  private val searchMode = EnumType("SearchMode", values = SearchMode.values.toList.map(mode => EnumValue(mode.toString, value = mode)))

  private val instantType = ScalarType[Instant](
    "Instant",
    coerceUserInput = {
      case value: String => Try(Instant.parse(value)).toEither.left.map(_ => InstantCoercionViolation())
      case _ => Left(InstantCoercionViolation())
    },
    coerceOutput = (value, _) => value.toString,
    coerceInput = {
      case sangria.ast.StringValue(value, _, _, _, _) =>
        Try(Instant.parse(value)).toEither.left.map(_ => InstantCoercionViolation())
      case _ => Left(InstantCoercionViolation())
    }
  )

  private def instantField[A](name: String, resolve: A => Instant): Field[RequestContext, A] =
    Field(name, instantType, resolve = context => resolve(context.value))

  private def idScalar[A](name: String, wrap: UUID => A, unwrap: A => UUID): ScalarType[A] =
    ScalarType[A](
      name,
      coerceUserInput = {
        case value: String => Try(UUID.fromString(value)).toEither.left.map(_ => IdCoercionViolation(name)).map(wrap)
        case _ => Left(IdCoercionViolation(name))
      },
      coerceOutput = (value, _) => unwrap(value).toString,
      coerceInput = {
        case sangria.ast.StringValue(value, _, _, _, _) =>
          Try(UUID.fromString(value)).toEither.left.map(_ => IdCoercionViolation(name)).map(wrap)
        case _ => Left(IdCoercionViolation(name))
      }
    )

  private val jobIdType = idScalar("JobID", JobId.apply, _.value)
  private val applicationIdType = idScalar("ApplicationID", ApplicationId.apply, _.value)

  private val idArgument = Argument("id", jobIdType)
  private val jobIdArgument = Argument("jobId", jobIdType)
  private val queryArgument = Argument("query", StringType)
  private val applicationIdArgument = Argument("applicationId", applicationIdType)
  private val firstArgument = Argument("first", IntType)
  private val afterArgument = Argument("after", OptionInputType(StringType))
  private val cityArgument = Argument("city", OptionInputType(StringType))
  private val skillsArgument = Argument("skills", OptionInputType(ListInputType(StringType)))
  private val createdAfterArgument = Argument("createdAfter", OptionInputType(instantType))
  private val jobStatusArgument = Argument("status", OptionInputType(jobStatus))
  private val applicationStatusArgument = Argument("status", OptionInputType(applicationStatus))
  private val userRoleArgument = Argument("role", OptionInputType(userRole))
  private val userStatusArgument = Argument("status", OptionInputType(userStatus))
  private val jobFilterInputType = InputObjectType[JobFilterGraphQLInput]("JobFilter", List(
    InputField("city", OptionInputType(StringType)),
    InputField("skills", OptionInputType(ListInputType(StringType))),
    InputField("createdAfter", OptionInputType(instantType))
  ))
  private val jobFilterArgument = Argument("filter", OptionInputType(jobFilterInputType))
  private val submitApplicationInputType = InputObjectType[SubmitApplicationGraphQLInput]("SubmitApplicationInput", List(
    InputField("jobId", jobIdType)
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
    InputField("id", jobIdType),
    InputField("patch", jobInputType)
  ))
  private val jobActionInputType = InputObjectType[JobActionGraphQLInput]("JobActionInput", List(
    InputField("jobId", jobIdType)
  ))
  private val applicationActionInputType = InputObjectType[ApplicationActionGraphQLInput]("ApplicationActionInput", List(
    InputField("applicationId", applicationIdType)
  ))
  private val rejectApplicationInputType = InputObjectType[RejectApplicationGraphQLInput]("RejectApplicationInput", List(
    InputField("applicationId", applicationIdType),
    InputField("feedback", OptionInputType(StringType))
  ))
  private val declineApplicationInputType = InputObjectType[DeclineApplicationGraphQLInput]("DeclineApplicationInput", List(
    InputField("applicationId", applicationIdType),
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
  private lazy val userProfileType: OutputType[GraphQLUserProfile] =
    UnionType[RequestContext]("UserProfile", List(candidateProfileType, recruiterProfileType))
      .mapValue[GraphQLUserProfile] {
        case GraphQLUserProfile.Candidate(profile) => profile
        case GraphQLUserProfile.Recruiter(profile) => profile
      }
  private lazy val userType: ObjectType[RequestContext, User] = ObjectType("User", fields[RequestContext, User](
    Field("id", IDType, resolve = _.value.id.value.toString),
    Field("email", OptionType(StringType), resolve = context =>
      emailVisibilityFetcher.deferOpt(context.value.id).map(_.flatMap(_ => context.value.email))),
    Field("name", StringType, resolve = _.value.name),
    Field("role", userRole, resolve = _.value.role),
    Field("status", userStatus, resolve = _.value.accountStatus),
    Field("profile", OptionType(userProfileType), resolve = _.value.profile.map {
      case UserProfile.Candidate(profile) => GraphQLUserProfile.Candidate(profile)
      case UserProfile.Recruiter(profile) => GraphQLUserProfile.Recruiter(profile)
    }),
    instantField("createdAt", _.createdAt)))
  private lazy val recruiterProfileType: ObjectType[RequestContext, RecruiterProfile] = ObjectType("RecruiterProfile", fields[RequestContext, RecruiterProfile](
      Field("organizationName", StringType, resolve = _.value.organizationName),
      Field("jobTitle", OptionType(StringType), resolve = _.value.jobTitle)))
  private lazy val jobType: ObjectType[RequestContext, Job] = ObjectType("Job", fields[RequestContext, Job](
    Field("id", jobIdType, resolve = _.value.id),
    Field("title", StringType, resolve = _.value.title),
    Field("description", StringType, resolve = _.value.description),
    Field("requirements", ListType(StringType), resolve = _.value.requirements),
    Field("skills", ListType(StringType), resolve = _.value.skills.toList.sorted),
    Field("location", locationType, resolve = _.value.location),
    Field("status", jobStatus, resolve = _.value.status),
    instantField("createdAt", _.createdAt),
    instantField("updatedAt", _.updatedAt),
    Field("recruiter", OptionType(userType), resolve = context =>
      usersFetcher.deferOpt(context.value.recruiterId))))
  private lazy val applicationType: ObjectType[RequestContext, Application] = ObjectType("Application", fields[RequestContext, Application](
    Field("id", applicationIdType, resolve = _.value.id),
    Field("status", applicationStatus, resolve = _.value.status),
    instantField("createdAt", _.createdAt),
    instantField("updatedAt", _.updatedAt),
    Field("candidate", OptionType(userType), resolve = context =>
      usersFetcher.deferOpt(context.value.candidateId)),
    Field("job", OptionType(jobType), resolve = context =>
      jobsFetcher.deferOpt(context.value.jobId))))
  private val applicationEventType = ObjectType("ApplicationEvent", fields[RequestContext, ApplicationEvent](
    Field("id", IDType, resolve = _.value.id.value.toString),
    Field("previousStatus", OptionType(applicationStatus), resolve = _.value.previousStatus),
    Field("newStatus", applicationStatus, resolve = _.value.newStatus),
    Field("actorId", IDType, resolve = _.value.actorId.value.toString),
    instantField("occurredAt", _.occurredAt),
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
      actor: Option[ActorContext],
      hiring: HiringGraphQLServices,
      ensureHiringReady: IO[ProbeResult],
      contextFactory: RequestContextFactory
  ): IO[Either[Failure, Json]] =
    contextFactory.resource(service.readiness(Some(requestId)), actor, hiring, ensureHiringReady).use { context =>
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
        exceptionHandler = ExceptionHandler {
          case (_, error: QueryAnalysisError) => throw error
          case (_, error: QueryComplexityExceeded) => throw error
          case (_, _) => HandledException("Execution failed")
        },
        queryReducers = QueryReducers,
        deferredResolver = DeferredResolver.fetchers(usersFetcher, jobsFetcher, emailVisibilityFetcher),
        errorsLimit = Some(20)
      ))).map(Right(_)).handleError {
        case _: QueryAnalysisError => Left(Failure.InvalidQuery)
        case _: QueryComplexityExceeded => Left(Failure.InvalidQuery)
        case _ => Left(Failure.Internal)
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

  private def liftUseCase[A](value: IO[Either[UseCaseError, A]]): GraphQLStep[A] =
    EitherT(value.map(_.leftMap(toGraphQLError)))

  private def complete[A](value: GraphQLStep[A], onError: GraphQLError => A): IO[A] =
    value.value.map(_.fold(onError, identity))

  private def authenticatedStep[A](
      context: Context[RequestContext, Unit]
  )(action: (ActorContext, HiringGraphQLServices) => GraphQLStep[A]): GraphQLStep[A] =
    EitherT(authenticated(context).map(_.leftMap(toGraphQLError))).flatMap(action.tupled)

  private def jobs(context: Context[RequestContext, Unit]): IO[Connection[Job]] =
    complete(authenticatedStep(context) { case (actor, hiring) =>
      for {
        (page, requested) <- EitherT(page(context.arg(firstArgument), context.arg(afterArgument), context.ctx.hiring.cursorCodec.decodeJob))
        values            <- liftUseCase(hiring.jobService.searchOpenJobs(actor, JobSearchFilter(
          context.arg(cityArgument), context.arg(skillsArgument).fold(Set.empty[String])(_.toSet), context.arg(createdAfterArgument)), page))
      } yield jobConnection(context, values, requested)
    }, graphQLErrorConnection[Job])

  private def semanticJobSearch(context: Context[RequestContext, Unit]): IO[RankedJobResults] =
    complete(EitherT(authenticatedSearch(context).map(_.leftMap(toGraphQLError))).flatMap { case (actor, _, service) =>
      for {
        size     <- EitherT(pageSize(context.arg(firstArgument)))
        searchId <- EitherT.liftF[IO, GraphQLError, UUID](IO.randomUUID)
        results  <- liftUseCase(service.semanticJobSearch(actor, context.arg(queryArgument), jobFilter(context.arg(jobFilterArgument)), size, searchId))
      } yield rankedJobResults(results)
    }, error => RankedJobResults(Nil, List(error)))

  private def recommendedJobs(context: Context[RequestContext, Unit]): IO[RankedJobResults] =
    complete(EitherT(authenticatedSearch(context).map(_.leftMap(toGraphQLError))).flatMap { case (actor, _, service) =>
      for {
        size     <- EitherT(pageSize(context.arg(firstArgument)))
        searchId <- EitherT.liftF[IO, GraphQLError, UUID](IO.randomUUID)
        results  <- liftUseCase(service.recommendedJobs(actor, size, searchId))
      } yield rankedJobResults(results)
    }, error => RankedJobResults(Nil, List(error)))

  private def candidateMatches(context: Context[RequestContext, Unit]): IO[RankedCandidateResults] =
    complete(EitherT(authenticatedSearch(context).map(_.leftMap(toGraphQLError))).flatMap { case (actor, _, service) =>
      for {
        size     <- EitherT(pageSize(context.arg(firstArgument)))
        searchId <- EitherT.liftF[IO, GraphQLError, UUID](IO.randomUUID)
        results  <- liftUseCase(service.candidateMatches(actor, context.arg(jobIdArgument), size, searchId))
      } yield rankedCandidateResults(results)
    }, error => RankedCandidateResults(Nil, List(error)))

  private def job(context: Context[RequestContext, Unit]): IO[Option[Job]] =
    authenticated(context).flatMap {
      case Left(_) => IO.pure(None)
      case Right((actor, hiring)) =>
        hiring.jobService.viewJob(actor, context.arg(idArgument)).map(_.toOption)
    }

  private def myJobs(context: Context[RequestContext, Unit]): IO[Connection[Job]] =
    complete(authenticatedStep(context) { case (actor, hiring) =>
      for {
        (page, requested) <- EitherT(page(context.arg(firstArgument), context.arg(afterArgument), context.ctx.hiring.cursorCodec.decodeJob))
        values            <- liftUseCase(hiring.jobService.myJobs(actor, page.copy(status = context.arg(jobStatusArgument))))
      } yield jobConnection(context, values, requested)
    }, graphQLErrorConnection[Job])

  private def myApplications(context: Context[RequestContext, Unit]): IO[Connection[Application]] =
    complete(authenticatedStep(context) { case (actor, hiring) =>
      for {
        (page, requested) <- EitherT(applicationPage(context, context.arg(firstArgument), context.arg(afterArgument), context.arg(applicationStatusArgument)))
        values            <- liftUseCase(hiring.applicationService.myApplications(actor, page))
      } yield applicationConnection(context, values, requested)
    }, graphQLErrorConnection[Application])

  private def jobApplications(context: Context[RequestContext, Unit]): IO[Connection[Application]] =
    complete(authenticatedStep(context) { case (actor, hiring) =>
      for {
        (page, requested)  <- EitherT(applicationPage(context, context.arg(firstArgument), context.arg(afterArgument), context.arg(applicationStatusArgument)))
        values             <- liftUseCase(hiring.applicationService.jobApplications(actor, context.arg(jobIdArgument), page))
      } yield applicationConnection(context, values, requested)
    }, graphQLErrorConnection[Application])

  private def applicationHistory(context: Context[RequestContext, Unit]): IO[Connection[ApplicationEvent]] =
    complete(authenticatedStep(context) { case (actor, hiring) =>
      for {
        (page, requested)  <- EitherT(pageEvent(context, context.arg(firstArgument), context.arg(afterArgument)))
        _                  <- liftUseCase(canViewApplication(actor, hiring, context.arg(applicationIdArgument)))
        values             <- EitherT.liftF[IO, GraphQLError, List[ApplicationEvent]](hiring.readModel.applicationHistory(context.arg(applicationIdArgument), page))
      } yield eventConnection(context, values, requested)
    }, graphQLErrorConnection[ApplicationEvent])

  private def submitApplication(context: Context[RequestContext, Unit]): IO[ApplicationPayload] =
    authenticatedPayload(ApplicationPayload(None, _))(context) { case (actor, hiring) =>
      val jobId = context.arg(submitApplicationInputArgument).jobId
      (IO.realTimeInstant, IO.randomUUID, IO.randomUUID).mapN { (now, applicationId, eventId) =>
        hiring.applicationService.submitApplication(actor, jobId, ApplicationId(applicationId), ApplicationEventId(eventId), now)
      }.flatten.map(applicationPayload)
    }

  private def createJob(context: Context[RequestContext, Unit]): IO[JobPayload] =
    authenticatedPayload(JobPayload(None, _))(context) { case (actor, hiring) =>
      jobInput(context.arg(createJobInputArgument), JobStatus.Open).fold(error => IO.pure(jobErrorPayload(error)), input =>
        (IO.realTimeInstant, IO.randomUUID).mapN { (now, jobId) =>
          hiring.jobService.createJob(actor, input, now, JobId(jobId))
        }.flatten.map(jobPayload)
      )
    }

  private def updateJob(context: Context[RequestContext, Unit]): IO[JobPayload] =
    authenticatedPayload(JobPayload(None, _))(context) { case (actor, hiring) =>
      val input = context.arg(updateJobInputArgument)
      updateInput(input.patch).fold(
        error => IO.pure(jobErrorPayload(error)),
        patch => IO.realTimeInstant.flatMap(now => hiring.jobService.updateJob(actor, input.id, patch, now)
            .map(jobPayload))
      )
    }

  private def changeJob(
      context: Context[RequestContext, Unit],
      method: JobUseCases[IO] => (ActorContext, JobId, Instant) => IO[Either[UseCaseError, Job]]
  ): IO[JobPayload] =
    authenticatedPayload(JobPayload(None, _))(context) { case (actor, hiring) =>
      val jobId = context.arg(jobActionInputArgument).jobId
      IO.realTimeInstant.flatMap(now => method(hiring.jobService)(actor, jobId, now).map(jobPayload))
    }

  private def applicationStatusAction(context: Context[RequestContext, Unit], status: ApplicationStatus): IO[ApplicationPayload] = {
    val input = context.arg(applicationActionInputArgument)
    changeApplicationStatus(context, input.applicationId, status, None, None)
  }

  private def changeApplicationStatus(
      context: Context[RequestContext, Unit],
      applicationId: ApplicationId,
      status: ApplicationStatus,
      feedback: Option[String],
      reason: Option[String]
  ): IO[ApplicationPayload] =
    authenticatedPayload(ApplicationPayload(None, _))(context) { case (actor, hiring) =>
      (IO.realTimeInstant, IO.randomUUID).mapN { (now, eventId) =>
        hiring.applicationService.changeStatus(actor, applicationId, status, feedback, reason, ApplicationEventId(eventId), now)
      }.flatten.map(applicationPayload)
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
    context.ctx.hiring.accountService.toRight(UseCaseError.availability(AvailabilityError.ServiceNotReady))

  private def accountMe(context: Context[RequestContext, Unit]): IO[Option[User]] =
    (context.ctx.actor, accountService(context)) match {
      case (Some(actor), Right(service)) => service.me(actor).map(_.toOption)
      case _ => IO.pure(None)
    }

  private def signUp(context: Context[RequestContext, Unit]): IO[AccountPayload] = {
    val input = context.arg(signUpInputArgument)
    (for {
      service <- accountService(context)
      profile <- signUpProfile(input)
    } yield (service, SignUpInput(input.name, input.role, input.password, profile))).fold(
      error => IO.pure(accountErrorPayload(error)),
      { case (service, request) =>
        (IO.realTimeInstant, IO.randomUUID).mapN { (now, id) =>
          service.signUp(request, now, Identifiers.UserId(id))
        }.flatten.map(signUpPayload)
      }
    )
  }

  private def bootstrapAdmin(context: Context[RequestContext, Unit]): IO[AccountPayload] =
    accountService(context).fold(
      error => IO.pure(accountErrorPayload(error)),
      service => {
        val input = context.arg(bootstrapAdminInputArgument)
        (IO.realTimeInstant, IO.randomUUID).mapN { (now, id) =>
          service.bootstrapAdmin(BootstrapAdminInput(input.name, input.password), now, Identifiers.UserId(id))
        }.flatten.map(accountPayload)
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
        updateProfileInput(actor.role, input).fold(
          error => IO.pure(UserPayload(None, List(toGraphQLError(error)))),
          profile => service.updateMyProfile(actor, profile).map(_.fold(error => UserPayload(None, List(toGraphQLError(error))), user => UserPayload(Some(user), Nil)))
        )
      case _ => IO.pure(UserPayload(None, List(GraphQLError("UNAUTHORIZED", "Authentication required"))))
    }

  private def updateProfileInput(role: UserRole, input: UpdateProfileGraphQLInput): Either[UseCaseError, AccountProfileInput] =
    profileFor(role, input.skills, input.experienceSummary, input.resumeRef, input.organizationName, input.jobTitle)
      .map(AccountProfileInput.apply)

  private def deleteMyAccount(context: Context[RequestContext, Unit]): IO[DeleteAccountPayload] =
    (context.ctx.actor, accountService(context)) match {
      case (Some(actor), Right(service)) => IO.realTimeInstant.flatMap(now => service.deleteMyAccount(actor, now)).map(_.fold(error => DeleteAccountPayload(false, List(toGraphQLError(error))), _ => DeleteAccountPayload(true, Nil)))
      case _ => IO.pure(DeleteAccountPayload(false, List(GraphQLError("UNAUTHORIZED", "Authentication required"))))
    }

  private def users(context: Context[RequestContext, Unit]): IO[Connection[User]] =
    (context.ctx.actor, accountService(context)) match {
      case (Some(actor), Right(service)) =>
        val requested = context.arg(firstArgument)
        val status = context.arg(userStatusArgument).getOrElse(AccountStatus.Active)
        val role = context.arg(userRoleArgument)
        userPage(context, requested, context.arg(afterArgument), status, role).flatMap {
          case Left(validationError) => IO.pure(graphQLErrorConnection(validationError))
          case Right((request, pageSize)) =>
            service.listUsers(actor, request).map(_.fold(errorConnection[User], values => userConnection(context, values, pageSize)))
        }
      case _ => IO.pure(errorConnection(UseCaseError.authentication(AuthenticationError.Unauthorized)))
    }

  private def authenticated(context: Context[RequestContext, Unit]): IO[Either[UseCaseError, (ActorContext, HiringGraphQLServices)]] =
    context.ctx.actor match {
      case Some(actor) =>
        val hiring = context.ctx.hiring
        context.ctx.hiringAvailable.map {
          case ProbeResult.Ready => Right((actor, hiring))
          case _ => Left(UseCaseError.availability(AvailabilityError.ServiceNotReady))
        }
      case None => IO.pure(Left(UseCaseError.authentication(AuthenticationError.Unauthorized)))
    }

  private def authenticatedSearch(
      context: Context[RequestContext, Unit]
  ): IO[Either[UseCaseError, (ActorContext, HiringGraphQLServices, SearchUseCases[IO])]] =
    authenticated(context).map(_.flatMap { case (actor, hiring) =>
      hiring.semanticSearchService.map(service => (actor, hiring, service)).toRight(UseCaseError.search(SearchError.VectorSearchUnavailable))
    })

  private def authenticatedPayload[A](
      unauthenticated: List[GraphQLError] => A
  )(context: Context[RequestContext, Unit])(
      action: (ActorContext, HiringGraphQLServices) => IO[A]
  ): IO[A] =
    authenticated(context).flatMap(_.fold(error => IO.pure(unauthenticated(List(toGraphQLError(error)))), action.tupled))

  private def page(
      first: Int,
      after: Option[String],
      decode: String => Either[CursorCodec.CursorError, JobCursor]
  ): IO[Either[GraphQLError, (JobPageRequest, Int)]] =
    cursorPage(first, after, decode)((cursor, size) => JobPageRequest(None, cursor, size))

  private def pageEvent(context: Context[RequestContext, Unit], first: Int, after: Option[String]): IO[Either[GraphQLError, (ApplicationEventPageRequest, Int)]] =
    cursorPage(first, after, context.ctx.hiring.cursorCodec.decodeEvent)((cursor, size) => ApplicationEventPageRequest(cursor, size))

  private def applicationPage(
      context: Context[RequestContext, Unit],
      first: Int,
      after: Option[String],
      status: Option[ApplicationStatus]
  ): IO[Either[GraphQLError, (ApplicationPageRequest, Int)]] =
    cursorPage(first, after, context.ctx.hiring.cursorCodec.decodeApplication)((cursor, size) => ApplicationPageRequest(status, cursor, size))

  private def userPage(
      context: Context[RequestContext, Unit],
      first: Int,
      after: Option[String],
      status: AccountStatus,
      role: Option[UserRole]
  ): IO[Either[GraphQLError, (UserPageRequest, Int)]] =
    cursorPage(first, after, context.ctx.hiring.cursorCodec.decodeUser)((cursor, size) => UserPageRequest(status, role, cursor, size.value))

  private def cursorPage[A, B](
      first: Int,
      after: Option[String],
      decode: String => Either[CursorCodec.CursorError, A]
  )(build: (Option[A], PageSize) => B): IO[Either[GraphQLError, (B, Int)]] =
    pageSize(first).map(_.flatMap { size =>
      after.traverse(decode)
        .leftMap {
          case CursorCodec.CursorError.WrongKind(_) => GraphQLError("WRONG_CURSOR_KIND", "Cursor belongs to a different connection")
          case CursorCodec.CursorError.Malformed(_) => GraphQLError("INVALID_CURSOR", "Invalid cursor")
        }
        .map(cursor => build(cursor, PageSize.next(size)) -> size.value)
    })

  private def pageSize(first: Int): IO[Either[GraphQLError, PageSize]] =
    IO.pure(PageSize.fromInt(first).toEither.leftMap(errors => toGraphQLError(UseCaseError.ValidationFailed(errors))))

  private def jobFilter(value: Option[JobFilterGraphQLInput]): JobSearchFilter =
    value match {
      case None => JobSearchFilter(None, Set.empty, None)
      case Some(filter) => JobSearchFilter(filter.city, filter.skills.fold(Set.empty[String])(_.toSet), filter.createdAfter)
    }

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
    (text("country", input.country), requiredText("city", input.city))
      .mapN(Location(_, _, input.remote))
      .toEither
      .leftMap(UseCaseError.ValidationFailed.apply)

  private def signUpProfile(input: SignUpGraphQLInput): Either[UseCaseError, Option[UserProfile]] =
    input.role match {
      case UserRole.Admin => Right(None)
      case other => profileFor(other, input.skills, input.experienceSummary, input.resumeRef, input.organizationName, input.jobTitle).map(Some(_))
    }

  private def profileFor(
      role: UserRole,
      skills: Option[List[String]],
      experienceSummary: Option[String],
      resumeRef: Option[String],
      organizationName: Option[String],
      jobTitle: Option[String]
  ): Either[UseCaseError, UserProfile] =
    role match {
      case UserRole.Candidate =>
        (requiredValues("skills", skills), optionalText("experienceSummary", experienceSummary), optionalText("resumeRef", resumeRef))
          .mapN(CandidateProfile.apply)
          .map(UserProfile.Candidate.apply)
          .toEither
          .leftMap(UseCaseError.ValidationFailed.apply)
      case UserRole.Recruiter =>
        (requiredText("organizationName", organizationName), optionalText("jobTitle", jobTitle))
          .mapN(RecruiterProfile.apply)
          .map(UserProfile.Recruiter.apply)
          .toEither
          .leftMap(UseCaseError.ValidationFailed.apply)
      case UserRole.Admin =>
        Left(UseCaseError.account(AccountError.ProfileUnsupportedForRole))
    }

  private def text(field: String, value: String): ValidatedNel[DomainValidationError, String] =
    value.trim match {
      case "" => DomainValidationError.BlankField(field).invalidNel
      case trimmed => trimmed.validNel
    }

  private def optionalText(field: String, value: Option[String]): ValidatedNel[DomainValidationError, Option[String]] =
    value.traverse(text(field, _))

  private def requiredText(field: String, value: Option[String]): ValidatedNel[DomainValidationError, String] =
    value.fold(DomainValidationError.BlankField(field).invalidNel[String])(text(field, _))

  private def requiredValues(field: String, values: Option[List[String]]): ValidatedNel[DomainValidationError, Set[String]] =
    values.fold(DomainValidationError.EmptyCollection(field).invalidNel[Set[String]]) { raw =>
      val trimmed = raw.map(_.trim).filter(_.nonEmpty).toSet
      if (trimmed.isEmpty) DomainValidationError.EmptyCollection(field).invalidNel else trimmed.validNel
    }

  private def canViewApplication(
      actor: ActorContext,
      hiring: HiringGraphQLServices,
      applicationId: ApplicationId
  ): IO[Either[UseCaseError, Unit]] =
    hiring.readModel.canViewApplication(actor, applicationId)

  private def jobConnection(context: Context[RequestContext, Unit], values: List[Job], requested: Int): Connection[Job] = {
    connection(values, requested)(job => context.ctx.hiring.cursorCodec.encodeJob(JobCursor(job.createdAt, job.id)))
  }

  private def applicationConnection(context: Context[RequestContext, Unit], values: List[Application], requested: Int): Connection[Application] = {
    connection(values, requested)(application => context.ctx.hiring.cursorCodec.encodeApplication(ApplicationCursor(application.createdAt, application.id)))
  }

  private def eventConnection(context: Context[RequestContext, Unit], values: List[ApplicationEvent], requested: Int): Connection[ApplicationEvent] = {
    connection(values, requested)(event => context.ctx.hiring.cursorCodec.encodeEvent(ApplicationEventCursor(event.occurredAt, event.id)))
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
        value.candidate.candidateProfile.map(profile => CandidateMatchProfile(profile.skills, profile.experienceSummary))
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
    Connection(Nil, PageInfo(false, None), List(toGraphQLError(error)))

  private def graphQLErrorConnection[A](error: GraphQLError): Connection[A] =
    Connection(Nil, PageInfo(false, None), List(error))

  private def jobPayload(result: Either[UseCaseError, Job]): JobPayload =
    result.fold(jobErrorPayload, job => JobPayload(Some(job), Nil))

  private def jobErrorPayload(error: UseCaseError): JobPayload =
    JobPayload(None, List(toGraphQLError(error)))

  private def applicationPayload(result: Either[UseCaseError, Application]): ApplicationPayload =
    result.fold(applicationErrorPayload, application => ApplicationPayload(Some(application), Nil))

  private def applicationErrorPayload(error: UseCaseError): ApplicationPayload =
    ApplicationPayload(None, List(toGraphQLError(error)))

  private def accountPayload(result: Either[UseCaseError, (User, AccountToken)]): AccountPayload =
    result.fold(accountErrorPayload, { case (user, token) => AccountPayload(Some(user), Some(token.value), Some(token.expiresAt.toString), Nil) })

  private def signUpPayload(result: Either[UseCaseError, (User, AccountToken)]): AccountPayload =
    result.fold(signUpErrorPayload, { case (user, token) => AccountPayload(Some(user), Some(token.value), Some(token.expiresAt.toString), Nil) })

  private def accountErrorPayload(error: UseCaseError): AccountPayload =
    AccountPayload(None, None, None, List(toGraphQLError(error)))

  private def signUpErrorPayload(error: UseCaseError): AccountPayload =
    val graphQLError = error match {
      case UseCaseError.Account(AccountError.NameTaken) =>
        GraphQLError("REGISTRATION_FAILED", "Registration failed")
      case other => toGraphQLError(other)
    }
    AccountPayload(None, None, None, List(graphQLError))

  private def userConnection(context: Context[RequestContext, Unit], values: List[User], requested: Int): Connection[User] =
    connection(values, requested)(user => context.ctx.hiring.cursorCodec.encodeUser(UserCursor(user.createdAt, user.id)))

  private def toGraphQLError(error: UseCaseError): GraphQLError =
    error match {
      case UseCaseError.Authentication(AuthenticationError.Unauthorized) => GraphQLError("UNAUTHORIZED", "Authentication required")
      case UseCaseError.Authentication(AuthenticationError.SingletonAdminViolation) => GraphQLError("FORBIDDEN", "Forbidden")
      case UseCaseError.Account(AccountError.BootstrapRequired) => GraphQLError("ADMIN_BOOTSTRAP_REQUIRED", "The first Admin must be bootstrapped")
      case UseCaseError.Account(AccountError.AlreadyBootstrapped) => GraphQLError("ADMIN_ALREADY_BOOTSTRAPPED", "Admin bootstrap is already complete")
      case UseCaseError.Account(AccountError.NameTaken) => GraphQLError("NAME_TAKEN", "Name is already in use")
      case UseCaseError.Account(AccountError.InvalidCredentials) => GraphQLError("INVALID_CREDENTIALS", "Invalid credentials")
      case UseCaseError.Account(AccountError.DeletedAccount) => GraphQLError("UNAUTHORIZED", "Authentication required")
      case UseCaseError.Account(AccountError.ProfileRoleMismatch) => GraphQLError("PROFILE_ROLE_MISMATCH", "Profile does not match the selected role")
      case UseCaseError.Account(AccountError.ProfileUnsupportedForRole) => GraphQLError("PROFILE_UNSUPPORTED_FOR_ROLE", "This role does not support a profile")
      case UseCaseError.Account(AccountError.PasswordPolicyViolation) => GraphQLError("INVALID_PASSWORD", "Password does not meet policy")
      case UseCaseError.Account(AccountError.AccountAlreadyDeleted) => GraphQLError("ACCOUNT_ALREADY_DELETED", "Account is already deleted")
      case UseCaseError.Account(AccountError.AdminSignupForbidden) => GraphQLError("ADMIN_BOOTSTRAP_ONLY", "Admin accounts can only be created through bootstrap")
      case UseCaseError.Availability(AvailabilityError.ServiceNotReady) => GraphQLError("SERVICE_NOT_READY", "Service not ready")
      case UseCaseError.Domain(DomainError.Forbidden) => GraphQLError("FORBIDDEN", "Forbidden")
      case UseCaseError.Domain(DomainError.NotFound(entity)) => GraphQLError("NOT_FOUND", s"$entity not found")
      case UseCaseError.Domain(DomainError.DuplicateApplication) => GraphQLError("DUPLICATE_APPLICATION", "Application already exists")
      case UseCaseError.Domain(DomainError.JobMustBeOpen) => GraphQLError("JOB_MUST_BE_OPEN", "Job must be open")
      case UseCaseError.Domain(DomainError.CandidateRequired) => GraphQLError("CANDIDATE_REQUIRED", "Candidate role required")
      case UseCaseError.Domain(DomainError.RecruiterRequired) => GraphQLError("RECRUITER_REQUIRED", "Recruiter role required")
      case UseCaseError.Domain(DomainError.InvalidJobTransition(_, _)) => GraphQLError("INVALID_JOB_TRANSITION", "Invalid job transition")
      case UseCaseError.Domain(DomainError.InvalidStatusTransition(_, _)) => GraphQLError("INVALID_STATUS_TRANSITION", "Invalid application status transition")
      case UseCaseError.Domain(DomainError.RejectionFeedbackRequired) => GraphQLError("REJECTION_FEEDBACK_REQUIRED", "Rejection feedback is required")
      case UseCaseError.Domain(DomainError.DeclineReasonRequired) => GraphQLError("DECLINE_REASON_REQUIRED", "Decline reason is required")
      case UseCaseError.Repository(RepositoryError.DuplicateApplication) => GraphQLError("DUPLICATE_APPLICATION", "Application already exists")
      case UseCaseError.Repository(RepositoryError.Conflict) => GraphQLError("CONFLICT", "Conflict")
      case UseCaseError.Repository(RepositoryError.Unavailable) => GraphQLError("UNAVAILABLE", "Repository unavailable")
      case UseCaseError.Search(SearchError.MissingEmbedding(entity)) => GraphQLError("MISSING_EMBEDDING", s"$entity embedding is missing")
      case UseCaseError.Search(SearchError.StaleEmbedding(entity)) => GraphQLError("STALE_EMBEDDING", s"$entity embedding is stale")
      case UseCaseError.Search(SearchError.InputTooLarge(field, maximum)) => GraphQLError("INPUT_TOO_LARGE", s"$field must be at most $maximum characters")
      case UseCaseError.Search(SearchError.ProviderUnavailable) => GraphQLError("PROVIDER_UNAVAILABLE", "Embedding provider unavailable")
      case UseCaseError.Search(SearchError.VectorSearchUnavailable) => GraphQLError("VECTOR_SEARCH_UNAVAILABLE", "Vector search unavailable")
      case UseCaseError.ValidationFailed(errors) =>
        val fields = errors.toList.map(validationErrorMessage).mkString(", ")
        GraphQLError("VALIDATION_FAILED", if (fields.isEmpty) "Validation failed" else fields)
    }

  private def validationErrorMessage(error: DomainValidationError): String =
    error match {
      case DomainValidationError.BlankField(field) => s"$field is required"
      case DomainValidationError.EmptyCollection(field) => s"$field must not be empty"
      case DomainValidationError.InvalidNumber(field, minimum, maximum, _) => s"$field must be between $minimum and $maximum"
    }
}
