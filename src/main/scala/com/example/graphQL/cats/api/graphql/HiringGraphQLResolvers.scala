package com.example.graphQL.cats.api.graphql

import cats.data.{EitherT, ValidatedNel}
import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.domain.error.{DomainError, DomainValidationError}
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId}
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.service.job.{CreateJobInput, UpdateJobInput}
import com.example.graphQL.cats.service.protocol.{AccountProfileInput, BootstrapAdminInput, JobUseCases, LoginInput, SignUpInput, SearchUseCases}
import com.example.graphQL.cats.service.{AccountError, ActorContext, AuthenticationError, AvailabilityError, ProbeResult, RepositoryError, SearchError, UseCaseError}
import com.example.graphQL.cats.shared.pagination.*
import com.example.graphQL.cats.shared.search.{JobSearchFilter, RankedCandidate, RankedJob}
import sangria.schema.Context

import java.time.Instant
import java.util.UUID

private[graphql] object HiringGraphQLResolvers {
  def jobs(context: Context[RequestContext, Unit]): IO[Connection[Job]] =
    complete(authenticatedStep(context) { case (actor, hiring) =>
      val cursorCodec = context.ctx.hiring.cursorCodec.jobCursorCodec
      val filter = JobSearchFilter(
        context.arg(cityArgument),
        context.arg(skillsArgument).fold(Set.empty[String])(_.toSet),
        context.arg(createdAfterArgument)
      )
      for {
        (page, requested) <- EitherT(page(context.arg(firstArgument), context.arg(afterArgument), cursorCodec.decode))
        values            <- liftUseCase(hiring.jobService.searchOpenJobs(actor, filter, page))
      } yield jobConnection(values, requested, cursorCodec)
    }, graphQLErrorConnection[Job])

  def semanticJobSearch(context: Context[RequestContext, Unit]): IO[RankedJobResults] =
    complete(EitherT(authenticatedSearch(context).map(_.leftMap(toGraphQLError))).flatMap { case (actor, _, service) =>
      for {
        size     <- EitherT(pageSize(context.arg(firstArgument)))
        searchId <- EitherT.liftF[IO, GraphQLError, UUID](IO.randomUUID)
        results  <- liftUseCase(service.semanticJobSearch(actor, context.arg(queryArgument), jobFilter(context.arg(jobFilterArgument)), size, searchId))
      } yield rankedJobResults(results)
    }, error => RankedJobResults(Nil, List(error)))

  def recommendedJobs(context: Context[RequestContext, Unit]): IO[RankedJobResults] =
    complete(EitherT(authenticatedSearch(context).map(_.leftMap(toGraphQLError))).flatMap { case (actor, _, service) =>
      for {
        size     <- EitherT(pageSize(context.arg(firstArgument)))
        searchId <- EitherT.liftF[IO, GraphQLError, UUID](IO.randomUUID)
        results  <- liftUseCase(service.recommendedJobs(actor, size, searchId))
      } yield rankedJobResults(results)
    }, error => RankedJobResults(Nil, List(error)))

  def candidateMatches(context: Context[RequestContext, Unit]): IO[RankedCandidateResults] =
    complete(EitherT(authenticatedSearch(context).map(_.leftMap(toGraphQLError))).flatMap { case (actor, _, service) =>
      val jobId = context.arg(jobIdArgument)
      for {
        size     <- EitherT(pageSize(context.arg(firstArgument)))
        searchId <- EitherT.liftF[IO, GraphQLError, UUID](IO.randomUUID)
        results  <- liftUseCase(service.candidateMatches(actor, jobId, size, searchId))
      } yield rankedCandidateResults(results)
    }, error => RankedCandidateResults(Nil, List(error)))

  def job(context: Context[RequestContext, Unit]): IO[JobPayload] =
    complete(authenticatedStep(context) { case (actor, hiring) =>
      liftUseCase(hiring.jobService.viewJob(actor, context.arg(idArgument))).map(job => JobPayload(Some(job), Nil))
    }, error => JobPayload(None, List(error)))

  def myJobs(context: Context[RequestContext, Unit]): IO[Connection[Job]] =
    complete(authenticatedStep(context) { case (actor, hiring) =>
      val cursorCodec = context.ctx.hiring.cursorCodec.jobCursorCodec
      for {
        (page, requested) <- EitherT(page(context.arg(firstArgument), context.arg(afterArgument), cursorCodec.decode))
        values            <- liftUseCase(hiring.jobService.myJobs(actor, page.copy(status = context.arg(jobStatusArgument))))
      } yield jobConnection(values, requested, cursorCodec)
    }, graphQLErrorConnection[Job])

  def myApplications(context: Context[RequestContext, Unit]): IO[Connection[Application]] =
    complete(authenticatedStep(context) { case (actor, hiring) =>
      val cursorCodec = context.ctx.hiring.cursorCodec.applicationCursorCodec
      for {
        (page, requested) <- EitherT(applicationPage(context.arg(firstArgument), context.arg(afterArgument), context.arg(applicationStatusArgument), cursorCodec))
        values            <- liftUseCase(hiring.applicationService.myApplications(actor, page))
      } yield applicationConnection(values, requested, cursorCodec)
    }, graphQLErrorConnection[Application])

  def jobApplications(context: Context[RequestContext, Unit]): IO[Connection[Application]] =
    complete(authenticatedStep(context) { case (actor, hiring) =>
      val cursorCodec = context.ctx.hiring.cursorCodec.applicationCursorCodec
      val jobId = context.arg(jobIdArgument)
      for {
        (page, requested)  <- EitherT(applicationPage(context.arg(firstArgument), context.arg(afterArgument), context.arg(applicationStatusArgument), cursorCodec))
        values             <- liftUseCase(hiring.applicationService.jobApplications(actor, jobId, page))
      } yield applicationConnection(values, requested, cursorCodec)
    }, graphQLErrorConnection[Application])

  def applicationHistory(context: Context[RequestContext, Unit]): IO[Connection[ApplicationEvent]] =
    complete(authenticatedStep(context) { case (actor, hiring) =>
      val cursorCodec = context.ctx.hiring.cursorCodec.eventCursorCodec
      val applicationId = context.arg(applicationIdArgument)
      for {
        (page, requested)  <- EitherT(pageEvent(context.arg(firstArgument), context.arg(afterArgument), cursorCodec))
        _                  <- liftUseCase(canViewApplication(actor, hiring, applicationId))
        values             <- EitherT.liftF[IO, GraphQLError, List[ApplicationEvent]](hiring.readModel.applicationHistory(applicationId, page))
      } yield eventConnection(values, requested, cursorCodec)
    }, graphQLErrorConnection[ApplicationEvent])

  def submitApplication(context: Context[RequestContext, Unit]): IO[ApplicationPayload] =
    authenticatedPayload(ApplicationPayload(None, _))(context) { case (actor, hiring) =>
      val jobId = context.arg(submitApplicationInputArgument).jobId
      timestamped { (now, applicationId) =>
        IO.randomUUID.flatMap { eventId =>
          hiring.applicationService.submitApplication(
            actor,
            jobId,
            ApplicationId(applicationId),
            ApplicationEventId(eventId),
            now
          )
        }
      }.map(applicationPayload)
    }

  def createJob(context: Context[RequestContext, Unit]): IO[JobPayload] =
    authenticatedPayload(JobPayload(None, _))(context) { case (actor, hiring) =>
      jobInput(context.arg(createJobInputArgument), JobStatus.Open).fold(error => IO.pure(jobErrorPayload(error)), input =>
        timestamped { (now, jobId) =>
          hiring.jobService.createJob(actor, input, now, JobId(jobId))
        }.map(jobPayload)
      )
    }

  def updateJob(context: Context[RequestContext, Unit]): IO[JobPayload] =
    authenticatedPayload(JobPayload(None, _))(context) { case (actor, hiring) =>
      val input = context.arg(updateJobInputArgument)
      updateInput(input.patch).fold(
        error => IO.pure(jobErrorPayload(error)),
        patch => IO.realTimeInstant.flatMap(now => hiring.jobService.updateJob(actor, input.id, patch, now)
            .map(jobPayload))
      )
    }

  def changeJob(
      context: Context[RequestContext, Unit],
      method: JobUseCases[IO] => (ActorContext, JobId, Instant) => IO[Either[UseCaseError, Job]]
  ): IO[JobPayload] =
    authenticatedPayload(JobPayload(None, _))(context) { case (actor, hiring) =>
      val jobId = context.arg(jobActionInputArgument).jobId
      IO.realTimeInstant.flatMap(now => method(hiring.jobService)(actor, jobId, now).map(jobPayload))
    }

  def applicationStatusAction(context: Context[RequestContext, Unit], status: ApplicationStatus): IO[ApplicationPayload] = {
    val input = context.arg(applicationActionInputArgument)
    changeApplicationStatus(context, input.applicationId, status, None, None)
  }

  def rejectApplication(context: Context[RequestContext, Unit]): IO[ApplicationPayload] = {
    val input = context.arg(rejectApplicationInputArgument)
    changeApplicationStatus(context, input.applicationId, ApplicationStatus.Rejected, input.feedback, None)
  }

  def declineApplication(context: Context[RequestContext, Unit]): IO[ApplicationPayload] = {
    val input = context.arg(declineApplicationInputArgument)
    changeApplicationStatus(context, input.applicationId, ApplicationStatus.Declined, None, input.reason)
  }

  def accountMe(context: Context[RequestContext, Unit]): IO[UserPayload] =
    authenticatedPayload(UserPayload(None, _))(context) { case (actor, hiring) =>
      hiring.accountService.me(actor).map(userPayload)
    }

  def signUp(context: Context[RequestContext, Unit]): IO[AccountPayload] = {
    val input = context.arg(signUpInputArgument)
    signUpProfile(input).fold(
      error => IO.pure(accountErrorPayload(error)),
      profile => timestamped { (now, id) =>
        context.ctx.hiring.accountService.signUp(
          SignUpInput(input.name, input.role, input.password, profile),
          now,
          Identifiers.UserId(id)
        )
      }.map(result => accountPayload(result, signUpErrorPayload))
    )
  }

  def bootstrapAdmin(context: Context[RequestContext, Unit]): IO[AccountPayload] =
    val input = context.arg(bootstrapAdminInputArgument)
    timestamped { (now, id) =>
      context.ctx.hiring.accountService.bootstrapAdmin(
        BootstrapAdminInput(input.name, input.password),
        now,
        Identifiers.UserId(id)
      )
    }.map(result => accountPayload(result))

  def login(context: Context[RequestContext, Unit]): IO[AccountPayload] =
    val input = context.arg(loginInputArgument)
    IO.realTimeInstant.flatMap(now =>
      context.ctx.hiring.accountService.login(LoginInput(input.name, input.password), now)
    ).map(result => accountPayload(result))

  def updateMyProfile(context: Context[RequestContext, Unit]): IO[UserPayload] =
    authenticatedPayload(UserPayload(None, _))(context) { case (actor, hiring) =>
      val input = context.arg(updateProfileInputArgument)
      updateProfileInput(actor.role, input).fold(
        error => IO.pure(UserPayload(None, List(toGraphQLError(error)))),
        profile => hiring.accountService.updateMyProfile(actor, profile).map(userPayload)
      )
    }

  def deleteMyAccount(context: Context[RequestContext, Unit]): IO[DeleteAccountPayload] =
    authenticatedPayload(DeleteAccountPayload(false, _))(context) { case (actor, hiring) =>
      IO.realTimeInstant.flatMap(now => hiring.accountService.deleteMyAccount(actor, now))
        .map(_.fold(error => DeleteAccountPayload(false, List(toGraphQLError(error))), _ => DeleteAccountPayload(true, Nil)))
    }

  def users(context: Context[RequestContext, Unit]): IO[Connection[User]] =
    complete(authenticatedStep(context) { case (actor, hiring) =>
      val cursorCodec = context.ctx.hiring.cursorCodec.userCursorCodec
      val requested = context.arg(firstArgument)
      val status = context.arg(userStatusArgument).getOrElse(AccountStatus.Active)
      val role = context.arg(userRoleArgument)
      EitherT(userPage(requested, context.arg(afterArgument), status, role, cursorCodec)).flatMap {
        case (request, pageSize) =>
          liftUseCase(hiring.accountService.listUsers(actor, request))
            .map(values => userConnection(values, pageSize, cursorCodec))
      }
    }, graphQLErrorConnection[User])

  private def liftUseCase[A](value: IO[Either[UseCaseError, A]]): GraphQLStep[A] =
    EitherT(value.map(_.leftMap(toGraphQLError)))

  private def timestamped[A](f: (Instant, UUID) => IO[A]): IO[A] =
    (IO.realTimeInstant, IO.randomUUID).mapN(f).flatten

  private def complete[A](value: GraphQLStep[A], onError: GraphQLError => A): IO[A] =
    value.value.map(_.fold(onError, identity))

  private def authenticatedStep[A](
      context: Context[RequestContext, Unit]
  )(action: (ActorContext, HiringGraphQLServices) => GraphQLStep[A]): GraphQLStep[A] =
    EitherT(authenticated(context).map(_.leftMap(toGraphQLError))).flatMap(action.tupled)

  private def changeApplicationStatus(
      context: Context[RequestContext, Unit],
      applicationId: ApplicationId,
      status: ApplicationStatus,
      feedback: Option[String],
      reason: Option[String]
  ): IO[ApplicationPayload] =
    authenticatedPayload(ApplicationPayload(None, _))(context) { case (actor, hiring) =>
      timestamped { (now, eventId) =>
        hiring.applicationService.changeStatus(actor, applicationId, status, feedback, reason, ApplicationEventId(eventId), now)
      }.map(applicationPayload)
    }

  private def updateProfileInput(role: UserRole, input: UpdateProfileGraphQLInput): Either[UseCaseError, AccountProfileInput] =
    profileFor(role, input.skills, input.experienceSummary, input.resumeRef, input.organizationName, input.jobTitle)
      .map(AccountProfileInput.apply)

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

  private def pageEvent(
      first: Int,
      after: Option[String],
      cursorCodec: CursorCodec[ApplicationEventCursor]
  ): IO[Either[GraphQLError, (ApplicationEventPageRequest, Int)]] =
    cursorPage(first, after, cursorCodec.decode)((cursor, size) => ApplicationEventPageRequest(cursor, size))

  private def applicationPage(
      first: Int,
      after: Option[String],
      status: Option[ApplicationStatus],
      cursorCodec: CursorCodec[ApplicationCursor]
  ): IO[Either[GraphQLError, (ApplicationPageRequest, Int)]] =
    cursorPage(first, after, cursorCodec.decode)((cursor, size) => ApplicationPageRequest(status, cursor, size))

  private def userPage(
      first: Int,
      after: Option[String],
      status: AccountStatus,
      role: Option[UserRole],
      cursorCodec: CursorCodec[UserCursor]
  ): IO[Either[GraphQLError, (UserPageRequest, Int)]] =
    cursorPage(first, after, cursorCodec.decode)((cursor, size) => UserPageRequest(status, role, cursor, size.value))

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

  private def jobConnection(values: List[Job], requested: Int, cursorCodec: CursorCodec[JobCursor]): Connection[Job] =
    connection(values, requested)(job => cursorCodec.encode(JobCursor(job.createdAt, job.id)))

  private def applicationConnection(values: List[Application], requested: Int, cursorCodec: CursorCodec[ApplicationCursor]): Connection[Application] =
    connection(values, requested)(application => cursorCodec.encode(ApplicationCursor(application.createdAt, application.id)))

  private def eventConnection(values: List[ApplicationEvent], requested: Int, cursorCodec: CursorCodec[ApplicationEventCursor]): Connection[ApplicationEvent] =
    connection(values, requested)(event => cursorCodec.encode(ApplicationEventCursor(event.occurredAt, event.id)))

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

  private def userPayload(result: Either[UseCaseError, User]): UserPayload =
    result.fold(error => UserPayload(None, List(toGraphQLError(error))), user => UserPayload(Some(user), Nil))

  private def accountPayload(
      result: Either[UseCaseError, (User, AccountToken)],
      errorPayload: UseCaseError => AccountPayload = accountErrorPayload
  ): AccountPayload =
    result.fold(errorPayload, { case (user, token) => AccountPayload(Some(user), Some(token.value), Some(token.expiresAt.toString), Nil) })

  private def accountErrorPayload(error: UseCaseError): AccountPayload =
    AccountPayload(None, None, None, List(toGraphQLError(error)))

  private def signUpErrorPayload(error: UseCaseError): AccountPayload =
    val graphQLError = error match {
      case UseCaseError.Account(AccountError.NameTaken) =>
        GraphQLError("REGISTRATION_FAILED", "Registration failed")
      case other => toGraphQLError(other)
    }
    AccountPayload(None, None, None, List(graphQLError))

  private def userConnection(values: List[User], requested: Int, cursorCodec: CursorCodec[UserCursor]): Connection[User] =
    connection(values, requested)(user => cursorCodec.encode(UserCursor(user.createdAt, user.id)))

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
