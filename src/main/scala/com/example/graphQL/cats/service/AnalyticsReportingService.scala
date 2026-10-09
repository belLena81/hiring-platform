package com.example.graphQL.cats.service

import cats.data.EitherT
import com.example.graphQL.cats.service.protocol.{UseCaseIO, UseCaseIO as UseCase}
import com.example.graphQL.cats.service.auth.ActorAuthorization
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
      EitherT.leftT(UseCaseError.Analytics(AnalyticsError.ReportsUnavailable))
  }
}

/** Authorizes aggregate analytics at the application boundary, before projection access. */
final class AnalyticsReportingService(
    users: UserRepository,
    reports: AnalyticsReportRepository
) extends AnalyticsReportingUseCases {
  private val authorization = ActorAuthorization(users)
  private val maximumPeriodSeconds = 30L * 24L * 60L * 60L

  override def report(
      actor: ActorContext,
      period: AnalyticsPeriod
  ): UseCaseIO[AnalyticsReportSnapshot] =
    for {
      _ <- UseCase.ensure(validPeriod(period), UseCaseError.Analytics(AnalyticsError.InvalidPeriod))
      _ <- authorization.requireAdmin(actor)
      snapshot <- UseCase
        .repository(reports.latest)
        .subflatMap(_.toRight(UseCaseError.Analytics(AnalyticsError.ReportsUnavailable)))
    } yield filter(snapshot, period)

  private def validPeriod(period: AnalyticsPeriod): Boolean =
    !period.to.isBefore(period.from) &&
      Duration.between(period.from, period.to).compareTo(Duration.ofSeconds(maximumPeriodSeconds)) <= 0

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
