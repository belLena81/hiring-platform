package com.example.hiring.analytics.domain

import java.time.Instant

/** Publisher-neutral value model matching the operational analytics report projection. */
final case class AnalyticsFunnelDayOutput(
    day: Instant,
    created: Long,
    accepted: Long,
    declined: Long,
    interview: Long,
    hired: Long,
    rejected: Long
)
final case class AnalyticsTimeToHireOutput(
    p50Hours: Double,
    p75Hours: Double,
    p90Hours: Double,
    p95Hours: Double,
    eligibleCount: Long,
    excludedCount: Long
)

/** The ordered stages of an application: the time between two of them is a hiring stage duration. */
enum AnalyticsHiringStageMetric(val wire: String) {
  case TimeToAccept extends AnalyticsHiringStageMetric("TIME_TO_ACCEPT")
  case TimeToInterview extends AnalyticsHiringStageMetric("TIME_TO_INTERVIEW")
  case TimeToHire extends AnalyticsHiringStageMetric("TIME_TO_HIRE")
  case AcceptedToInterview extends AnalyticsHiringStageMetric("ACCEPTED_TO_INTERVIEW")
  case InterviewToHire extends AnalyticsHiringStageMetric("INTERVIEW_TO_HIRE")
}

object AnalyticsHiringStageMetric {
  def fromWire(value: String): Option[AnalyticsHiringStageMetric] = values.find(_.wire == value)
}

/** A suppressed percentile distribution of one hiring stage duration; `eligibleCount` counts candidate subjects. */
final case class AnalyticsStageDurationOutput(
    metric: AnalyticsHiringStageMetric,
    p50Hours: Double,
    p75Hours: Double,
    p90Hours: Double,
    p95Hours: Double,
    eligibleCount: Long
)
final case class AnalyticsSkillPostingDayOutput(day: Instant, skill: String, postings: Long)
final case class AnalyticsReportOutput(
    asOf: Instant,
    funnel: Vector[AnalyticsFunnelDayOutput],
    timeToHire: Option[AnalyticsTimeToHireOutput],
    stageDurations: Vector[AnalyticsStageDurationOutput],
    skillPostingActivity: Vector[AnalyticsSkillPostingDayOutput]
)
