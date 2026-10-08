package com.example.graphQL.cats.service

import cats.data.NonEmptyList
import cats.effect.IO
import com.example.graphQL.cats.domain.error.DomainValidationError
import com.example.graphQL.cats.domain.model.{AccountStatus, UserRole}
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
      UseCaseIO.left(UseCaseError.Authentication(AuthenticationError.Unauthorized))
  }
}

/** Authorizes the read-only coverage report against the live Admin record before any scan is issued. */
final class EmbeddingCoverageService private (
    users: UserRepository,
    source: Option[EmbeddingCoverageService.Source],
    clock: IO[java.time.Instant]
) extends EmbeddingCoverageUseCases {
  import EmbeddingCoverageService.*

  override def report(actor: ActorContext, expectedModel: Option[String]): UseCaseIO[EmbeddingCoverageReport] =
    for {
      _ <- UseCaseIO.repository(users.find(actor.userId)).subflatMap {
        case Some(user) if user.role == UserRole.Admin && user.accountStatus == AccountStatus.Active => Right(user)
        case _ => Left(UseCaseError.Authentication(AuthenticationError.Unauthorized))
      }
      model <- UseCaseIO.fromEither(validate(expectedModel))
      live <- UseCaseIO.fromEither(source.toRight(UseCaseError.Search(SearchError.VectorSearchUnavailable)))
      now <- UseCaseIO.liftIO(clock)
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
      clock: IO[java.time.Instant] = IO.realTimeInstant
  ): EmbeddingCoverageService =
    new EmbeddingCoverageService(users, Some(Source(repository, durableRetryCap, limits)), clock)

  /** Vector search is off: the Admin is still authorized, then the typed unavailable error is returned. */
  def vectorSearchDisabled(users: UserRepository): EmbeddingCoverageService =
    new EmbeddingCoverageService(users, None, IO.realTimeInstant)

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
