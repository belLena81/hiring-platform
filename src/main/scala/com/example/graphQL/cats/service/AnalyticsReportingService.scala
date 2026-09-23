package com.example.graphQL.cats.service

import com.example.graphQL.cats.service.protocol.{UseCaseIO, UseCaseIO as UseCase}
import com.example.graphQL.cats.domain.model.UserRole
import com.example.graphQL.cats.repository.protocol.{AnalyticsReportRepository, UserRepository}

import java.time.{Duration, Instant}

final case class AnalyticsPeriod(from: Instant, to: Instant)

trait AnalyticsReportingUseCases {
  def report(
      actor: ActorContext,
      period: AnalyticsPeriod
  ): UseCaseIO[com.example.graphQL.cats.repository.protocol.AnalyticsReportSnapshot]
}

object AnalyticsReportingUseCases {
  val unavailable: AnalyticsReportingUseCases = new AnalyticsReportingUseCases {
    override def report(
        actor: ActorContext,
        period: AnalyticsPeriod
    ): UseCaseIO[com.example.graphQL.cats.repository.protocol.AnalyticsReportSnapshot] =
      UseCase.left(UseCaseError.Analytics(AnalyticsError.ReportsUnavailable))
  }
}

/** Authorizes aggregate analytics at the application boundary, before projection access. */
final class AnalyticsReportingService(
    users: UserRepository,
    reports: AnalyticsReportRepository
) extends AnalyticsReportingUseCases {
  private val maximumPeriodSeconds = 30L * 24L * 60L * 60L

  override def report(
      actor: ActorContext,
      period: AnalyticsPeriod
  ): UseCaseIO[com.example.graphQL.cats.repository.protocol.AnalyticsReportSnapshot] =
    for {
      _ <- UseCase.fromEither(validate(period))
      _ <- UseCase.repository(users.find(actor.userId)).subflatMap {
        case Some(user)
            if user.role == UserRole.Admin && user.accountStatus == com.example.graphQL.cats.domain.model.AccountStatus.Active =>
          Right(user)
        case _ => Left(UseCaseError.Authentication(AuthenticationError.Unauthorized))
      }
      snapshot <- UseCase
        .repository(reports.latest)
        .subflatMap(_.toRight(UseCaseError.Analytics(AnalyticsError.ReportsUnavailable)))
    } yield filter(snapshot, period)

  private def validate(period: AnalyticsPeriod): Either[UseCaseError, Unit] =
    Either.cond(
      !period.to.isBefore(period.from) && Duration
        .between(period.from, period.to)
        .compareTo(Duration.ofSeconds(maximumPeriodSeconds)) <= 0,
      (),
      UseCaseError.Analytics(AnalyticsError.InvalidPeriod)
    )

  private def filter(
      snapshot: com.example.graphQL.cats.repository.protocol.AnalyticsReportSnapshot,
      period: AnalyticsPeriod
  ): com.example.graphQL.cats.repository.protocol.AnalyticsReportSnapshot =
    snapshot.copy(
      funnel = snapshot.funnel.filter(row => !row.day.isBefore(period.from) && !row.day.isAfter(period.to)),
      skillPostingActivity =
        snapshot.skillPostingActivity.filter(row => !row.day.isBefore(period.from) && !row.day.isAfter(period.to))
    )
}
