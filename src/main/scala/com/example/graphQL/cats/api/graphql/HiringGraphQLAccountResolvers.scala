package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import com.example.graphQL.cats.api.admission.AuthRateLimiter.Operation
import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLResolverSupport.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.service.{AccountError, UseCaseError}
import com.example.graphQL.cats.service.protocol.{AccountProfileInput, BootstrapAdminInput, LoginInput, SignUpInput}
import com.example.graphQL.cats.repository.protocol.MutationEntityReference
import io.circe.Json
import sangria.schema.Context

private[graphql] object HiringGraphQLAccountResolvers {
  def accountMe(context: Context[RequestContext, Unit]): IO[User] =
    authenticated(context) { case (actor, hiring) => raiseOnUseCaseError(hiring.accountService.me(actor)) }

  def signUp(context: Context[RequestContext, Unit]): IO[MutationOutcome[AuthSuccess]] =
    rateLimited(context, Operation.SignUp).flatMap { _ =>
      val input = context.arg(signUpInputArgument)
      publicMutation(context) { hiring =>
        signUpProfile(input).fold(
          error => mutationResult(Left(error): Either[UseCaseError, AuthSuccess]),
          profile => executeMutation[(User, AccountToken)](
            hiring,
            "signUp",
            publicActorScope(input.name),
            input.idempotencyKey,
            Json.fromString(input.toString),
            result => MutationEntityReference("user", result._1.id.value.toString),
            reference => replayAuth(hiring, reference)
          ) { _ =>
            timestamped { (now, id) =>
              hiring.accountService.signUp(
                SignUpInput(input.name, input.role, input.password, profile),
                now,
                Identifiers.UserId(id)
              )
            }
          }.map(_.map(authSuccess)).flatMap(mutationResult)
        )
      }
    }

  def bootstrapAdmin(context: Context[RequestContext, Unit]): IO[MutationOutcome[AuthSuccess]] =
    rateLimited(context, Operation.BootstrapAdmin).flatMap { _ =>
      val input = context.arg(bootstrapAdminInputArgument)
      publicMutation(context) { hiring =>
        executeMutation[(User, AccountToken)](
          hiring,
          "bootstrapAdmin",
          publicActorScope(input.name),
          input.idempotencyKey,
          Json.fromString(input.toString),
          result => MutationEntityReference("user", result._1.id.value.toString),
          reference => replayAuth(hiring, reference)
        ) { _ =>
          timestamped { (now, id) =>
            hiring.accountService.bootstrapAdmin(
              BootstrapAdminInput(input.name, input.password),
              now,
              Identifiers.UserId(id)
            )
          }
        }.map(_.map(authSuccess)).flatMap(mutationResult)
      }
    }

  def login(context: Context[RequestContext, Unit]): IO[MutationOutcome[AuthSuccess]] =
    rateLimited(context, Operation.Login).flatMap { _ =>
      val input = context.arg(loginInputArgument)
      publicMutation(context) { hiring =>
        executeMutation[(User, AccountToken)](
          hiring,
          "login",
          publicActorScope(input.name),
          input.idempotencyKey,
          Json.fromString(input.toString),
          result => MutationEntityReference("user", result._1.id.value.toString),
          reference => replayAuth(hiring, reference)
        ) { _ =>
          IO.realTimeInstant.flatMap(now => hiring.accountService.login(LoginInput(input.name, input.password), now))
        }.map(_.map(authSuccess)).flatMap(mutationResult)
      }
    }

  def updateMyProfile(context: Context[RequestContext, Unit]): IO[MutationOutcome[User]] =
    authenticated(context) { case (actor, hiring) =>
      val input = context.arg(updateProfileInputArgument)
      updateProfileInput(actor.role, input).fold(
        error => mutationResult(Left(error): Either[UseCaseError, User]),
        profile => executeMutation[User](
          hiring,
          "updateMyProfile",
          actorScope(actor),
          input.idempotencyKey,
          Json.fromString(input.toString),
          user => MutationEntityReference("user", user.id.value.toString),
          _ => hiring.accountService.me(actor)
        ) { _ =>
          IO.realTimeInstant.flatMap(now => hiring.accountService.updateMyProfile(actor, profile, now))
        }.flatMap(mutationResult)
      )
    }

  def deleteMyAccount(context: Context[RequestContext, Unit]): IO[MutationOutcome[DeletionSuccess]] =
    authenticated(context) { case (actor, hiring) =>
      val input = context.arg(deleteMyAccountInputArgument)
      executeMutation(
        hiring,
        "deleteMyAccount",
        actorScope(actor),
        input.idempotencyKey,
        Json.fromString(input.toString),
        _ => MutationEntityReference("user", actor.userId.value.toString),
        _ => IO.pure(Right(()))
      ) { _ =>
        IO.realTimeInstant.flatMap(now => hiring.accountService.deleteMyAccount(actor, now))
      }.flatTap {
        case Right(_) => context.ctx.invalidateViewer
        case Left(_) => IO.unit
      }
        .map(_.map(_ => DeletionSuccess(true))).flatMap(mutationResult)
    }

  def users(context: Context[RequestContext, Unit]): IO[Connection[User]] =
    authenticated(context) { case (actor, hiring) =>
      given CursorCodec.CursorKey = hiring.cursorKey
      val requested = context.arg(firstArgument)
      val status = context.arg(userStatusArgument).getOrElse(AccountStatus.Active)
      val role = context.arg(userRoleArgument)
      inputResult(userPage(requested, context.arg(afterArgument), status, role, CursorCodec.decode[UserCursor])).flatMap {
        case (request, pageSize) =>
          raiseOnUseCaseError(hiring.accountService.listUsers(actor, request))
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

  private def authSuccess(result: (User, AccountToken)): AuthSuccess = result match {
    case (user, token) => AuthSuccess(user, token.value, token.expiresAt)
  }

  private def replayAuth(
      hiring: HiringGraphQLServices,
      reference: MutationEntityReference
  ): IO[Either[UseCaseError, (User, AccountToken)]] =
    scala.util.Try(Identifiers.UserId(java.util.UUID.fromString(reference.entityId))).toEither.fold(
      _ => IO.pure(Left(UseCaseError.Repository(com.example.graphQL.cats.repository.protocol.RepositoryError.Unavailable))),
      id => IO.realTimeInstant.flatMap(now => hiring.accountService.issueToken(id, now))
    )

  private def userConnection(values: List[User], requested: Int)(using CursorCodec.CursorKey): Connection[User] =
    connection(values, requested)(user => CursorCodec.encode(UserCursor(user.createdAt, user.id)))
}
