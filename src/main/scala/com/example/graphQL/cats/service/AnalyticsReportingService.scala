package com.example.graphQL.cats.service

import cats.effect.IO
import com.example.graphQL.cats.domain.model.UserRole
import com.example.graphQL.cats.repository.protocol.{AnalyticsReportRepository, UserRepository}

import java.time.{Duration, Instant}

final case class AnalyticsPeriod(from: Instant, to: Instant)

trait AnalyticsReportingUseCases {
  def report(actor: ActorContext, period: AnalyticsPeriod): IO[Either[UseCaseError, com.example.graphQL.cats.repository.protocol.AnalyticsReportSnapshot]]
}

object AnalyticsReportingUseCases {
  val unavailable: AnalyticsReportingUseCases = new AnalyticsReportingUseCases {
    override def report(actor: ActorContext, period: AnalyticsPeriod): IO[Either[UseCaseError, com.example.graphQL.cats.repository.protocol.AnalyticsReportSnapshot]] =
      IO.pure(Left(UseCaseError.Analytics(AnalyticsError.ReportsUnavailable)))
  }
}

/** Authorizes aggregate analytics at the application boundary, before projection access. */
final class AnalyticsReportingService(
    users: UserRepository,
    reports: AnalyticsReportRepository
) extends AnalyticsReportingUseCases {
  private val maximumPeriodSeconds = 30L * 24L * 60L * 60L

  override def report(actor: ActorContext, period: AnalyticsPeriod): IO[Either[UseCaseError, com.example.graphQL.cats.repository.protocol.AnalyticsReportSnapshot]] =
    validate(period).fold(error => IO.pure(Left(error)), _ =>
      users.find(actor.userId).flatMap {
        case Left(error) => IO.pure(Left(UseCaseError.Repository(error)))
        case Right(Some(user)) if user.role == UserRole.Admin && user.accountStatus == com.example.graphQL.cats.domain.model.AccountStatus.Active =>
          reports.latest.map {
            case Left(error) => Left(UseCaseError.Repository(error))
            case Right(None) => Left(UseCaseError.Analytics(AnalyticsError.ReportsUnavailable))
            case Right(Some(snapshot)) => Right(filter(snapshot, period))
          }
        case _ => IO.pure(Left(UseCaseError.Authentication(AuthenticationError.Unauthorized)))
      }
    )

  private def validate(period: AnalyticsPeriod): Either[UseCaseError, Unit] =
    Either.cond(
      !period.to.isBefore(period.from) && Duration.between(period.from, period.to).compareTo(Duration.ofSeconds(maximumPeriodSeconds)) <= 0,
      (),
      UseCaseError.Analytics(AnalyticsError.InvalidPeriod)
    )

  private def filter(
      snapshot: com.example.graphQL.cats.repository.protocol.AnalyticsReportSnapshot,
      period: AnalyticsPeriod
  ): com.example.graphQL.cats.repository.protocol.AnalyticsReportSnapshot =
    snapshot.copy(
      funnel = snapshot.funnel.filter(row => !row.day.isBefore(period.from) && !row.day.isAfter(period.to)),
      skillPostingActivity = snapshot.skillPostingActivity.filter(row => !row.day.isBefore(period.from) && !row.day.isAfter(period.to))
    )
}
