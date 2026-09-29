package com.example.graphQL.cats.service

import com.example.graphQL.cats.service.protocol.{UseCaseIO, UseCaseIO as UseCase}
import com.example.graphQL.cats.domain.model.UserRole
import com.example.graphQL.cats.service.port.{AnalyticsReportRepository, UserRepository}

import java.time.{Duration, Instant}

final case class AnalyticsPeriod(from: Instant, to: Instant)

final case class AnalyticsReportSnapshot(
    asOf: Instant,
    funnel: List[AnalyticsFunnelDay],
    timeToHire: Option[AnalyticsTimeToHire],
    skillPostingActivity: List[AnalyticsSkillPostingDay]
)

final case class AnalyticsFunnelDay(
    day: Instant,
    created: Long,
    accepted: Long,
    declined: Long,
    interview: Long,
    hired: Long,
    rejected: Long
)
final case class AnalyticsTimeToHire(
    p50Hours: Double,
    p75Hours: Double,
    p90Hours: Double,
    p95Hours: Double,
    eligibleCount: Long,
    excludedCount: Long
)
final case class AnalyticsSkillPostingDay(day: Instant, skill: String, postings: Long)

trait AnalyticsReportingUseCases {
  def report(
      actor: ActorContext,
      period: AnalyticsPeriod
  ): UseCaseIO[AnalyticsReportSnapshot]
}

object AnalyticsReportingUseCases {
  val unavailable: AnalyticsReportingUseCases = new AnalyticsReportingUseCases {
    override def report(
        actor: ActorContext,
        period: AnalyticsPeriod
    ): UseCaseIO[AnalyticsReportSnapshot] =
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
  ): UseCaseIO[AnalyticsReportSnapshot] =
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
      snapshot: AnalyticsReportSnapshot,
      period: AnalyticsPeriod
  ): AnalyticsReportSnapshot =
    snapshot.copy(
      funnel = snapshot.funnel.filter(row => !row.day.isBefore(period.from) && !row.day.isAfter(period.to)),
      skillPostingActivity =
        snapshot.skillPostingActivity.filter(row => !row.day.isBefore(period.from) && !row.day.isAfter(period.to))
    )
}
