package com.example.graphQL.cats.api.graphql

import cats.data.EitherT
import cats.effect.IO
import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLResolverSupport.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.service.{AccountError, UseCaseError}
import com.example.graphQL.cats.service.protocol.{AccountProfileInput, BootstrapAdminInput, LoginInput, SignUpInput}
import sangria.schema.Context

private[graphql] object HiringGraphQLAccountResolvers {
  def accountMe(context: Context[RequestContext, Unit]): IO[UserPayload] =
    authenticatedPayload(UserPayload(None, _))(context) { case (actor, hiring) =>
      hiring.accountService.me(actor).map(userPayload)
    }

  def signUp(context: Context[RequestContext, Unit]): IO[AccountPayload] = {
    val input = context.arg(signUpInputArgument)
    publicAccountPayload(errors => AccountPayload(None, None, None, errors))(context) { hiring =>
      signUpProfile(input).fold(
        error => IO.pure(accountErrorPayload(error)),
        profile => timestamped { (now, id) =>
          hiring.accountService.signUp(
            SignUpInput(input.name, input.role, input.password, profile),
            now,
            Identifiers.UserId(id)
          )
        }.map(result => accountPayload(result, signUpErrorPayload))
      )
    }
  }

  def bootstrapAdmin(context: Context[RequestContext, Unit]): IO[AccountPayload] =
    val input = context.arg(bootstrapAdminInputArgument)
    publicAccountPayload(errors => AccountPayload(None, None, None, errors))(context) { hiring =>
      timestamped { (now, id) =>
        hiring.accountService.bootstrapAdmin(
          BootstrapAdminInput(input.name, input.password),
          now,
          Identifiers.UserId(id)
        )
      }.map(result => accountPayload(result))
    }

  def login(context: Context[RequestContext, Unit]): IO[AccountPayload] =
    val input = context.arg(loginInputArgument)
    publicAccountPayload(errors => AccountPayload(None, None, None, errors))(context) { hiring =>
      IO.realTimeInstant.flatMap(now => hiring.accountService.login(LoginInput(input.name, input.password), now)).map(result => accountPayload(result))
    }

  def updateMyProfile(context: Context[RequestContext, Unit]): IO[UserPayload] =
    authenticatedPayload(UserPayload(None, _))(context) { case (actor, hiring) =>
      val input = context.arg(updateProfileInputArgument)
      updateProfileInput(actor.role, input).fold(
        error => IO.pure(UserPayload(None, List(toGraphQLError(error)))),
        profile => IO.realTimeInstant.flatMap(now => hiring.accountService.updateMyProfile(actor, profile, now)).map(userPayload)
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
      EitherT.fromEither[IO](userPage(requested, context.arg(afterArgument), status, role, cursorCodec)).flatMap {
        case (request, pageSize) =>
          liftUseCase(hiring.accountService.listUsers(actor, request))
            .map(values => userConnection(values, pageSize, cursorCodec))
      }
    }, graphQLErrorConnection[User])

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
      case UseCaseError.Account(AccountError.NameTaken) => GraphQLError("REGISTRATION_FAILED", "Registration failed")
      case other => toGraphQLError(other)
    }
    AccountPayload(None, None, None, List(graphQLError))

  private def userConnection(values: List[User], requested: Int, cursorCodec: CursorCodec[UserCursor]): Connection[User] =
    connection(values, requested)(user => cursorCodec.encode(UserCursor(user.createdAt, user.id)))
}
