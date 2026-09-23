package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import com.example.graphQL.cats.api.admission.AuthRateLimiter.Operation
import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLResolverSupport.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.service.{AccountError, UseCaseError}
import com.example.graphQL.cats.service.protocol.{
  AccountProfileInput,
  BootstrapAdminInput,
  LoginInput,
  SignUpInput,
  UseCaseIO
}
import sangria.schema.Context
import java.time.Instant

private[graphql] object HiringGraphQLAccountResolvers {
  def accountMe(context: Context[RequestContext, Unit]): IO[User] =
    authenticated(context) { case (actor, hiring) => raiseOnUseCaseError(hiring.accountService.me(actor)) }

  def signUp(context: Context[RequestContext, Unit]): IO[MutationOutcome[AuthSuccess]] =
    rateLimited(context, Operation.SignUp).flatMap { _ =>
      val input = context.arg(signUpInputArgument)
      publicMutation(context) { hiring =>
        signUpProfile(input).fold(
          error => mutationResult(UseCaseIO.left(error)),
          profile =>
            mutationResult(
              hiring.accountService
                .signUp(
                  idempotencyRequest(input.idempotencyKey, input.idempotencyPayload),
                  SignUpInput(input.name, input.role, input.password, profile)
                )
                .map(authSuccess)
            )
        )
      }
    }

  def bootstrapAdmin(context: Context[RequestContext, Unit]): IO[MutationOutcome[AuthSuccess]] =
    rateLimited(context, Operation.BootstrapAdmin).flatMap { _ =>
      val input = context.arg(bootstrapAdminInputArgument)
      publicMutation(context) { hiring =>
        mutationResult(
          hiring.accountService
            .bootstrapAdmin(
              idempotencyRequest(input.idempotencyKey, input.idempotencyPayload),
              BootstrapAdminInput(input.name, input.password)
            )
            .map(authSuccess)
        )
      }
    }

  def login(context: Context[RequestContext, Unit]): IO[MutationOutcome[AuthSuccess]] =
    rateLimited(context, Operation.Login).flatMap { _ =>
      val input = context.arg(loginInputArgument)
      publicMutation(context) { hiring =>
        mutationResult(
          hiring.accountService
            .login(
              idempotencyRequest(input.idempotencyKey, input.idempotencyPayload),
              LoginInput(input.name, input.password)
            )
            .map(authSuccess)
        )
      }
    }

  def updateMyProfile(context: Context[RequestContext, Unit]): IO[MutationOutcome[User]] =
    authenticated(context) { case (actor, hiring) =>
      val input = context.arg(updateProfileInputArgument)
      updateProfileInput(actor.role, input).fold(
        error => mutationResult(UseCaseIO.left(error)),
        profile =>
          mutationResult(
            hiring.accountService.updateMyProfile(
              idempotencyRequest(input.idempotencyKey, input.idempotencyPayload),
              actor,
              profile
            )
          )
      )
    }

  def deleteMyAccount(context: Context[RequestContext, Unit]): IO[MutationOutcome[DeletionSuccess]] =
    authenticated(context) { case (actor, hiring) =>
      val input = context.arg(deleteMyAccountInputArgument)
      mutationResult(
        hiring.accountService
          .deleteMyAccount(idempotencyRequest(input.idempotencyKey, input.idempotencyPayload), actor)
          .semiflatTap(_ => context.ctx.invalidateViewer)
          .map(_ => DeletionSuccess(true))
      )
    }

  def users(context: Context[RequestContext, Unit]): IO[Connection[User]] =
    authenticated(context) { case (actor, hiring) =>
      given CursorCodec.CursorKey = hiring.cursorKey
      val requested = context.arg(firstArgument)
      val status = context.arg(userStatusArgument).getOrElse(AccountStatus.Active)
      val role = context.arg(userRoleArgument)
      IO.realTimeInstant.flatMap { now =>
        inputResult(
          userPage(
            requested,
            context.arg(afterArgument),
            status,
            role,
            cursor => CursorCodec.decode[UserCursor](cursor, now)
          )
        ).flatMap { case (request, pageSize) =>
          raiseOnUseCaseError(hiring.accountService.listUsers(actor, request))
            .map(values => userConnection(values, pageSize, now))
        }
      }
    }

  private def updateProfileInput(
      role: UserRole,
      input: UpdateProfileGraphQLInput
  ): Either[UseCaseError, AccountProfileInput] =
    profileFor(role, input.skills, input.experienceSummary, input.resumeRef, input.organizationName, input.jobTitle)
      .map(AccountProfileInput.apply)

  private def signUpProfile(input: SignUpGraphQLInput): Either[UseCaseError, Option[UserProfile]] =
    input.role match {
      case UserRole.Admin => Right(None)
      case other          =>
        profileFor(
          other,
          input.skills,
          input.experienceSummary,
          input.resumeRef,
          input.organizationName,
          input.jobTitle
        ).map(Some(_))
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

  private def authSuccess(result: (User, AccountToken)): AuthSuccess = result match {
    case (user, token) => AuthSuccess(user, token.value, token.expiresAt)
  }

  private def userConnection(values: List[User], requested: Int, now: Instant)(using
      CursorCodec.CursorKey
  ): Connection[User] =
    connection(values, requested)(user => CursorCodec.encode(UserCursor(user.createdAt, user.id), now))
}
