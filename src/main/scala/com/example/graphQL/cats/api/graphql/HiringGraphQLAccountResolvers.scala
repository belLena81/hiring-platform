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
    rateLimited(context, Operation.SignUp).flatMap { _ =>
      val input = context.arg(signUpInputArgument)
      withHiring(context) { hiring =>
        signUpProfile(input).fold(
          error => mutationResult(EitherT.leftT(error)),
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

  def login(context: Context[RequestContext, Unit]): HiringGraphQLResult[MutationOutcome[AuthSuccess]] =
    rateLimited(context, Operation.Login).flatMap { _ =>
      val input = context.arg(loginInputArgument)
      withHiring(context) { hiring =>
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

  def updateMyProfile(context: Context[RequestContext, Unit]): HiringGraphQLResult[MutationOutcome[User]] =
    authenticated(context) { case (actor, hiring) =>
      val input = context.arg(updateProfileInputArgument)
      updateProfileInput(actor.role, input).fold(
        error => mutationResult(EitherT.leftT(error)),
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
    profileFor(
      role,
      input.skills,
      input.experienceSummary,
      input.resumeRef,
      input.currentResidenceCountry,
      input.currentResidenceCity,
      input.availabilityStatus,
      input.recruiterSearchOptIn,
      input.organizationName,
      input.jobTitle
    )
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
          input.currentResidenceCountry,
          input.currentResidenceCity,
          input.availabilityStatus,
          input.recruiterSearchOptIn,
          input.organizationName,
          input.jobTitle
        ).map(Some(_))
    }

  private def profileFor(
      role: UserRole,
      skills: Option[List[String]],
      experienceSummary: Option[String],
      resumeRef: Option[String],
      currentResidenceCountry: Option[String],
      currentResidenceCity: Option[String],
      availabilityStatus: Option[CandidateAvailabilityStatus],
      recruiterSearchOptIn: Option[Boolean],
      organizationName: Option[String],
      jobTitle: Option[String]
  ): Either[UseCaseError, UserProfile] =
    role match {
      case UserRole.Candidate =>
        val residence = currentResidenceCountry.map(country => CandidateResidence(country, currentResidenceCity))
        if (currentResidenceCountry.isEmpty && currentResidenceCity.nonEmpty)
          Left(
            UseCaseError.ValidationFailed(
              cats.data.NonEmptyList.one(
                DomainValidationError.BlankField("currentResidence.country")
              )
            )
          )
        else
          Right(
            UserProfile.Candidate(
              CandidateProfile(
                skills.getOrElse(Nil).toSet,
                experienceSummary,
                resumeRef,
                residence,
                availabilityStatus,
                recruiterSearchOptIn.getOrElse(false)
              )
            )
          )
      case UserRole.Recruiter =>
        Right(UserProfile.Recruiter(RecruiterProfile(organizationName.getOrElse(""), jobTitle)))
      case UserRole.Admin =>
        Left(UseCaseError.Account(AccountError.ProfileUnsupportedForRole))
    }

  private def authSuccess(result: (User, AccountToken)): AuthSuccess = result match {
    case (user, token) => AuthSuccess(user, token.value, token.expiresAt)
  }
}
