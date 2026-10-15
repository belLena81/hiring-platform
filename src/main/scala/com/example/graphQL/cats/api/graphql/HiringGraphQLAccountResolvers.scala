package com.example.graphQL.cats.api.graphql

import cats.data.EitherT
import cats.effect.IO
import com.example.graphQL.cats.api.admission.AuthRateLimiter.Operation
import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLResolverSupport.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.error.DomainValidationError
import com.example.graphQL.cats.service.{AccountError, UseCaseError}
import com.example.graphQL.cats.service.protocol.{AccountProfileInput, LoginInput, SignUpInput}
import sangria.schema.Context

private[graphql] object HiringGraphQLAccountResolvers {
  def accountMe(context: Context[RequestContext, Unit]): HiringGraphQLResult[User] =
    authenticated(context) { case (actor, hiring) => raiseOnUseCaseError(hiring.accountService.me(actor)) }

  def signUp(context: Context[RequestContext, Unit]): HiringGraphQLResult[MutationOutcome[AuthSuccess]] =
    context.ctx.rateLimited(Operation.SignUp).flatMap { _ =>
      val input = context.arg(signUpInputArgument)
      withHiring(context) { hiring =>
        signUpProfile(input).fold(
          error => mutationResult(EitherT.leftT(error), InputFieldPaths.signUp),
          profile =>
            mutationResult(
              hiring.accountService
                .signUp(
                  idempotencyRequest(input.idempotencyKey, input.idempotencyPayload),
                  SignUpInput(input.name, input.role, input.password, profile)
                )
                .map(authSuccess),
              InputFieldPaths.signUp
            )
        )
      }
    }

  def login(context: Context[RequestContext, Unit]): HiringGraphQLResult[MutationOutcome[AuthSuccess]] =
    context.ctx.rateLimited(Operation.Login).flatMap { _ =>
      val input = context.arg(loginInputArgument)
      withHiring(context) { hiring =>
        mutationResult(
          hiring.accountService
            .login(
              idempotencyRequest(input.idempotencyKey, input.idempotencyPayload),
              LoginInput(input.name, input.password)
            )
            .map(authSuccess),
          InputFieldPaths.login
        )
      }
    }

  def updateMyProfile(context: Context[RequestContext, Unit]): HiringGraphQLResult[MutationOutcome[User]] =
    authenticated(context) { case (actor, hiring) =>
      val input = context.arg(updateProfileInputArgument)
      updateProfileInput(actor.role, input).fold(
        error => mutationResult(EitherT.leftT(error), InputFieldPaths.updateMyProfile),
        profile =>
          mutationResult(
            hiring.accountService.updateMyProfile(
              idempotencyRequest(input.idempotencyKey, input.idempotencyPayload),
              actor,
              profile
            ),
            InputFieldPaths.updateMyProfile
          )
      )
    }

  def deleteMyAccount(context: Context[RequestContext, Unit]): HiringGraphQLResult[MutationOutcome[DeletionReceipt]] =
    withHiring(context)(hiring =>
      context.ctx.deletionActor.flatMap { actor =>
        val input = context.arg(deleteMyAccountInputArgument)
        mutationResult(
          hiring.accountService
            .deleteMyAccount(idempotencyRequest(input.idempotencyKey, input.idempotencyPayload), actor)
            .semiflatTap(_ => context.ctx.invalidateViewer)
            .map(receiptId => DeletionReceipt(receiptId, AccountDeletionStatus.Pending))
        )
      }
    )

  def accountDeletionStatus(context: Context[RequestContext, Unit]): HiringGraphQLResult[AccountDeletionStatus] =
    withHiring(context)(hiring =>
      context.ctx.deletionActor.flatMap(actor =>
        raiseOnUseCaseError(hiring.accountService.accountDeletionStatus(actor, context.arg(deletionReceiptIdArgument)))
      )
    )

  def users(context: Context[RequestContext, Unit]): HiringGraphQLResult[Connection[User]] =
    authenticated(context) { case (actor, hiring) =>
      val status = context.arg(userStatusArgument).getOrElse(AccountStatus.Active)
      val role = context.arg(userRoleArgument)
      paged[UserCursor, UserPageRequest, User](hiring, context.arg(firstArgument), context.arg(afterArgument))(
        (cursor, size) => UserPageRequest(status, role, cursor, size)
      )(request => raiseOnUseCaseError(hiring.accountService.listUsers(actor, request)))(user =>
        UserCursor(user.createdAt, user.id)
      )
    }

  private def updateProfileInput(
      role: UserRole,
      input: UpdateProfileGraphQLInput
  ): Either[UseCaseError, AccountProfileInput] =
    role match {
      case UserRole.Admin => Left(UseCaseError.Account(AccountError.ProfileUnsupportedForRole))
      case _              => profileFrom(input.profile).map(AccountProfileInput.apply)
    }

  private def signUpProfile(input: SignUpGraphQLInput): Either[UseCaseError, Option[UserProfile]] =
    input.role match {
      case UserRole.Admin => Right(None)
      case _              =>
        input.profile
          .toRight(profileInvalid(DomainValidationError.BlankField("profile")))
          .flatMap(profileFrom)
          .map(Some(_))
    }

  /** The `@oneOf` rule is checked here too: Sangria enforces it only for a variable typed directly as `ProfileInput`,
    * so the resolver does not rely on query validation alone. Whether the variant matches the account's role is decided
    * by the service.
    */
  private def profileFrom(input: ProfileGraphQLInput): Either[UseCaseError, UserProfile] =
    (input.candidate, input.recruiter) match {
      case (Some(candidate), None) =>
        Right(
          UserProfile.Candidate(
            CandidateProfile(
              candidate.skills.getOrElse(Nil).toSet,
              candidate.experienceSummary,
              candidate.resumeRef,
              candidate.residence.map(residence => CandidateResidence(residence.country, residence.city)),
              candidate.availabilityStatus,
              candidate.recruiterSearchOptIn.getOrElse(false)
            )
          )
        )
      case (None, Some(recruiter)) =>
        Right(UserProfile.Recruiter(RecruiterProfile(recruiter.organizationName, recruiter.jobTitle)))
      case (None, None)       => Left(profileInvalid(DomainValidationError.BlankField("profile")))
      case (Some(_), Some(_)) => Left(profileInvalid(DomainValidationError.TooManyValues("profile", 1, 2)))
    }

  private def profileInvalid(error: DomainValidationError): UseCaseError =
    UseCaseError.ValidationFailed(cats.data.NonEmptyList.one(error))

  private def authSuccess(result: (User, AccountToken)): AuthSuccess = result match {
    case (user, token) => AuthSuccess(user, token.value, token.expiresAt)
  }
}
