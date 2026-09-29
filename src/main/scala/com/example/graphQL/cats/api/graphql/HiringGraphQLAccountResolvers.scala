package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import com.example.graphQL.cats.api.admission.AuthRateLimiter.Operation
import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLResolverSupport.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.error.DomainValidationError
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
  def accountMe(context: Context[RequestContext, Unit]): HiringGraphQLResult[User] =
    authenticated(context) { case (actor, hiring) => raiseOnUseCaseError(hiring.accountService.me(actor)) }

  def signUp(context: Context[RequestContext, Unit]): HiringGraphQLResult[MutationOutcome[AuthSuccess]] =
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

  def bootstrapAdmin(context: Context[RequestContext, Unit]): HiringGraphQLResult[MutationOutcome[AuthSuccess]] =
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

  def login(context: Context[RequestContext, Unit]): HiringGraphQLResult[MutationOutcome[AuthSuccess]] =
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

  def updateMyProfile(context: Context[RequestContext, Unit]): HiringGraphQLResult[MutationOutcome[User]] =
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

  def deleteMyAccount(context: Context[RequestContext, Unit]): HiringGraphQLResult[MutationOutcome[DeletionReceipt]] =
    cats.data.EitherT
      .liftF[IO, HiringGraphQLFailure, com.example.graphQL.cats.service.ProbeResult](
        context.ctx.hiringAvailable
      )
      .flatMap {
        case com.example.graphQL.cats.service.ProbeResult.Ready =>
          context.ctx.deletionActor.flatMap { actor =>
            val input = context.arg(deleteMyAccountInputArgument)
            mutationResult(
              context.ctx.hiring.accountService
                .deleteMyAccount(idempotencyRequest(input.idempotencyKey, input.idempotencyPayload), actor)
                .semiflatTap(_ => context.ctx.invalidateViewer)
                .map(receiptId => DeletionReceipt(receiptId, AccountDeletionStatus.Pending))
            )
          }
        case _ =>
          cats.data.EitherT.leftT(
            HiringGraphQLFailure.UseCase(
              UseCaseError.Availability(
                com.example.graphQL.cats.service.AvailabilityError.ServiceNotReady
              )
            )
          )
      }

  def accountDeletionStatus(context: Context[RequestContext, Unit]): HiringGraphQLResult[AccountDeletionStatus] =
    cats.data.EitherT
      .liftF[IO, HiringGraphQLFailure, com.example.graphQL.cats.service.ProbeResult](
        context.ctx.hiringAvailable
      )
      .flatMap {
        case com.example.graphQL.cats.service.ProbeResult.Ready =>
          context.ctx.deletionActor.flatMap { actor =>
            raiseOnUseCaseError(
              context.ctx.hiring.accountService.accountDeletionStatus(actor, context.arg(deletionReceiptIdArgument))
            )
          }
        case _ =>
          cats.data.EitherT.leftT(
            HiringGraphQLFailure.UseCase(
              UseCaseError.Availability(
                com.example.graphQL.cats.service.AvailabilityError.ServiceNotReady
              )
            )
          )
      }

  def users(context: Context[RequestContext, Unit]): HiringGraphQLResult[Connection[User]] =
    authenticated(context) { case (actor, hiring) =>
      given CursorCodec.CursorKey = hiring.cursorKey
      val requested = context.arg(firstArgument)
      val status = context.arg(userStatusArgument).getOrElse(AccountStatus.Active)
      val role = context.arg(userRoleArgument)
      for {
        now <- cats.data.EitherT.liftF[IO, HiringGraphQLFailure, Instant](IO.realTimeInstant)
        (request, pageSize) <- inputResult(
          userPage(
            requested,
            context.arg(afterArgument),
            status,
            role,
            cursor => CursorCodec.decode[UserCursor](cursor, now)
          )
        )
        values <- raiseOnUseCaseError(hiring.accountService.listUsers(actor, request))
      } yield userConnection(values, pageSize, now)
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

  private def userConnection(values: List[User], requested: Int, now: Instant)(using
      CursorCodec.CursorKey
  ): Connection[User] =
    connection(values, requested)(user => CursorCodec.encode(UserCursor(user.createdAt, user.id), now))
}
