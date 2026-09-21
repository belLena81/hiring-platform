package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLResolverSupport.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.service.{AccountError, UseCaseError}
import com.example.graphQL.cats.service.protocol.{AccountProfileInput, BootstrapAdminInput, LoginInput, SignUpInput}
import sangria.schema.Context

private[graphql] object HiringGraphQLAccountResolvers {
  def accountMe(context: Context[RequestContext, Unit]): IO[User] =
    authenticatedMutation(context) { case (actor, hiring) => liftUseCase(hiring.accountService.me(actor)) }

  def signUp(context: Context[RequestContext, Unit]): IO[Any] = {
    val input = context.arg(signUpInputArgument)
    publicMutation(context) { hiring =>
      signUpProfile(input).fold(
        error => mutationResult(IO.pure(Left(error): Either[UseCaseError, (User, AccountToken)]))(authSuccess),
        profile => timestamped { (now, id) =>
          hiring.accountService.signUp(
            SignUpInput(input.name, input.role, input.password, profile),
            now,
            Identifiers.UserId(id)
          )
        }.flatMap(result => mutationResult(IO.pure(result))(authSuccess))
      )
    }
  }

  def bootstrapAdmin(context: Context[RequestContext, Unit]): IO[Any] =
    val input = context.arg(bootstrapAdminInputArgument)
    publicMutation(context) { hiring =>
      timestamped { (now, id) =>
        hiring.accountService.bootstrapAdmin(
          BootstrapAdminInput(input.name, input.password),
          now,
          Identifiers.UserId(id)
        )
      }.flatMap(result => mutationResult(IO.pure(result))(authSuccess))
    }

  def login(context: Context[RequestContext, Unit]): IO[Any] =
    val input = context.arg(loginInputArgument)
    publicMutation(context) { hiring =>
      IO.realTimeInstant.flatMap(now => hiring.accountService.login(LoginInput(input.name, input.password), now))
        .flatMap(result => mutationResult(IO.pure(result))(authSuccess))
    }

  def updateMyProfile(context: Context[RequestContext, Unit]): IO[Any] =
    authenticatedMutation(context) { case (actor, hiring) =>
      val input = context.arg(updateProfileInputArgument)
      updateProfileInput(actor.role, input).fold(
        error => mutationResult(IO.pure(Left(error): Either[UseCaseError, User]))(identity),
        profile => IO.realTimeInstant.flatMap(now => hiring.accountService.updateMyProfile(actor, profile, now))
          .flatMap(result => mutationResult(IO.pure(result))(identity))
      )
    }

  def deleteMyAccount(context: Context[RequestContext, Unit]): IO[Any] =
    authenticatedMutation(context) { case (actor, hiring) =>
      IO.realTimeInstant.flatMap(now => hiring.accountService.deleteMyAccount(actor, now))
        .flatMap(result => mutationResult(IO.pure(result.map(_ => DeletionSuccess(true))))(identity))
    }

  def users(context: Context[RequestContext, Unit]): IO[Connection[User]] =
    authenticatedMutation(context) { case (actor, hiring) =>
      given CursorCodec.CursorKey = hiring.cursorKey
      val requested = context.arg(firstArgument)
      val status = context.arg(userStatusArgument).getOrElse(AccountStatus.Active)
      val role = context.arg(userRoleArgument)
      inputResult(userPage(requested, context.arg(afterArgument), status, role, CursorCodec.decode[UserCursor])).flatMap {
        case (request, pageSize) =>
          liftUseCase(hiring.accountService.listUsers(actor, request))
            .map(values => userConnection(values, pageSize))
      }
    }

  private def updateProfileInput(role: UserRole, input: UpdateProfileGraphQLInput): Either[UseCaseError, AccountProfileInput] =
    profileFor(role, input.skills, input.experienceSummary, input.resumeRef, input.organizationName, input.jobTitle)
      .map(AccountProfileInput.apply)

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
        Right(UserProfile.Candidate(CandidateProfile(skills.getOrElse(Nil).toSet, experienceSummary, resumeRef)))
      case UserRole.Recruiter =>
        Right(UserProfile.Recruiter(RecruiterProfile(organizationName.getOrElse(""), jobTitle)))
      case UserRole.Admin =>
        Left(UseCaseError.Account(AccountError.ProfileUnsupportedForRole))
    }

  private def authSuccess(result: (User, AccountToken)): AuthSuccess =
    AuthSuccess(result._1, result._2.value, result._2.expiresAt.toString)

  private def userConnection(values: List[User], requested: Int)(using CursorCodec.CursorKey): Connection[User] =
    connection(values, requested)(user => CursorCodec.encode(UserCursor(user.createdAt, user.id)))
}
