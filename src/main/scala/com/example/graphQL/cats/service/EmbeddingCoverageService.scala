package com.example.graphQL.cats.service

import cats.data.EitherT
import cats.data.NonEmptyList
import cats.effect.{Clock, IO}
import com.example.graphQL.cats.domain.error.DomainValidationError
import com.example.graphQL.cats.service.auth.ActorAuthorization
import com.example.graphQL.cats.service.port.{EmbeddingCoverageRepository, UserRepository}
import com.example.graphQL.cats.service.protocol.UseCaseIO
import com.example.graphQL.cats.service.search.{
  EmbeddingCoverageLimits,
  EmbeddingCoverageReport,
  EmbeddingCoverageScanRequest
}

import scala.concurrent.duration.*

trait EmbeddingCoverageUseCases {
  def report(actor: ActorContext, expectedModel: Option[String]): UseCaseIO[EmbeddingCoverageReport]
}

object EmbeddingCoverageUseCases {

  /** Deny by default: an unwired capability answers every caller as unauthorized, never with availability facts. */
  val denyAll: EmbeddingCoverageUseCases = new EmbeddingCoverageUseCases {
    override def report(actor: ActorContext, expectedModel: Option[String]): UseCaseIO[EmbeddingCoverageReport] =
      EitherT.leftT(UseCaseError.Authentication(AuthenticationError.Unauthorized))
  }
}

/** Authorizes the read-only coverage report against the live Admin record before any scan is issued. */
final class EmbeddingCoverageService private (
    users: UserRepository,
    source: Option[EmbeddingCoverageService.Source],
    clock: Clock[IO]
) extends EmbeddingCoverageUseCases {
  import EmbeddingCoverageService.*
  private val authorization = ActorAuthorization(users)

  override def report(actor: ActorContext, expectedModel: Option[String]): UseCaseIO[EmbeddingCoverageReport] =
    for {
      _ <- authorization.requireAdmin(actor)
      model <- EitherT.fromEither[IO](validate(expectedModel))
      live <- EitherT.fromEither[IO](source.toRight(UseCaseError.Search(SearchError.VectorSearchUnavailable)))
      now <- EitherT.liftF(clock.realTimeInstant)
      request = EmbeddingCoverageScanRequest(
        model,
        now,
        now.minusMillis((live.durableRetryCap * StuckAfterRetryCaps).toMillis),
        live.limits
      )
      observation <- UseCaseIO.repository(live.repository.observe(request))
    } yield EmbeddingCoverageReport.assemble(observation, model, now)
}

object EmbeddingCoverageService {
  val MaxExpectedModelLength: Int = 128
  val StuckAfterRetryCaps: Int = 3

  private final case class Source(
      repository: EmbeddingCoverageRepository,
      durableRetryCap: FiniteDuration,
      limits: EmbeddingCoverageLimits
  )

  /** Work waiting longer than three durable retry caps past its availability is reported as stuck. */
  def live(
      users: UserRepository,
      repository: EmbeddingCoverageRepository,
      durableRetryCap: FiniteDuration,
      limits: EmbeddingCoverageLimits = EmbeddingCoverageLimits.default,
      clock: Clock[IO] = Clock[IO]
  ): EmbeddingCoverageService =
    new EmbeddingCoverageService(users, Some(Source(repository, durableRetryCap, limits)), clock)

  /** Vector search is off: the Admin is still authorized, then the typed unavailable error is returned. */
  def vectorSearchDisabled(users: UserRepository): EmbeddingCoverageService =
    new EmbeddingCoverageService(users, None, Clock[IO])

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
