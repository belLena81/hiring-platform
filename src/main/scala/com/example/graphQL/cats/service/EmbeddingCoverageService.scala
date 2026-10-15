package com.example.graphQL.cats.service

import cats.data.EitherT
import cats.data.NonEmptyList
import cats.{Applicative, Monad}
import cats.effect.Clock
import com.example.graphQL.cats.domain.error.DomainValidationError
import com.example.graphQL.cats.service.auth.ActorAuthorization
import com.example.graphQL.cats.service.port.{EmbeddingCoverageRepository, UserRepository}
import com.example.graphQL.cats.service.protocol.UseCaseF
import com.example.graphQL.cats.service.search.{
  EmbeddingCoverageLimits,
  EmbeddingCoverageReport,
  EmbeddingCoverageScanRequest
}

import scala.concurrent.duration.*

trait EmbeddingCoverageUseCases[F[_]] {
  def report(actor: ActorContext, expectedModel: Option[String]): UseCaseF[F, EmbeddingCoverageReport]
}

object EmbeddingCoverageUseCases {

  /** Deny by default: an unwired capability answers every caller as unauthorized, never with availability facts. */
  def denyAll[F[_]: Applicative]: EmbeddingCoverageUseCases[F] = new EmbeddingCoverageUseCases[F] {
    override def report(actor: ActorContext, expectedModel: Option[String]): UseCaseF[F, EmbeddingCoverageReport] =
      EitherT.leftT[F, EmbeddingCoverageReport](UseCaseError.Authentication(AuthenticationError.Unauthorized))
  }
}

/** Authorizes the read-only coverage report against the live Admin record before any scan is issued. */
final class EmbeddingCoverageService[F[_]: Monad] private (
    users: UserRepository[F],
    source: Option[EmbeddingCoverageService.Source[F]],
    clock: Clock[F]
) extends EmbeddingCoverageUseCases[F] {
  import EmbeddingCoverageService.*
  private val authorization = ActorAuthorization(users)

  override def report(actor: ActorContext, expectedModel: Option[String]): UseCaseF[F, EmbeddingCoverageReport] =
    for {
      _ <- authorization.requireAdmin(actor)
      model <- EitherT.fromEither[F](validate(expectedModel))
      live <- EitherT.fromEither[F](source.toRight(UseCaseError.Search(SearchError.VectorSearchUnavailable)))
      now <- EitherT.liftF[F, UseCaseError, java.time.Instant](clock.realTimeInstant)
      request = EmbeddingCoverageScanRequest(
        model,
        now,
        now.minusMillis((live.durableRetryCap * StuckAfterRetryCaps).toMillis),
        live.limits
      )
      observation <- UseCaseF.repository(live.repository.observe(request))
    } yield EmbeddingCoverageReport.assemble(observation, model, now)
}

object EmbeddingCoverageService {
  val MaxExpectedModelLength: Int = 128
  val StuckAfterRetryCaps: Int = 3

  private final case class Source[F[_]](
      repository: EmbeddingCoverageRepository[F],
      durableRetryCap: FiniteDuration,
      limits: EmbeddingCoverageLimits
  )

  /** Work waiting longer than three durable retry caps past its availability is reported as stuck. */
  def live[F[_]: Monad](
      users: UserRepository[F],
      repository: EmbeddingCoverageRepository[F],
      durableRetryCap: FiniteDuration,
      limits: EmbeddingCoverageLimits = EmbeddingCoverageLimits.default,
      clock: Clock[F]
  ): EmbeddingCoverageService[F] =
    new EmbeddingCoverageService(users, Some(Source(repository, durableRetryCap, limits)), clock)

  /** Vector search is off: the Admin is still authorized, then the typed unavailable error is returned. */
  def vectorSearchDisabled[F[_]: Monad](users: UserRepository[F])(using clock: Clock[F]): EmbeddingCoverageService[F] =
    new EmbeddingCoverageService(users, None, clock)

  private def validate(expectedModel: Option[String]): Either[UseCaseError, Option[String]] =
    expectedModel.map(_.trim) match {
      case None                         => Right(None)
      case Some(value) if value.isEmpty =>
        Left(UseCaseError.ValidationFailed(NonEmptyList.one(DomainValidationError.BlankField("expectedModel"))))
      case Some(value) if value.length > MaxExpectedModelLength =>
        Left(
          UseCaseError.ValidationFailed(
            NonEmptyList.one(DomainValidationError.TextTooLong("expectedModel", MaxExpectedModelLength, value.length))
          )
        )
      case Some(value) => Right(Some(value))
    }
}
