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
final case class AnalyticsSkillPostingDayOutput(day: Instant, skill: String, postings: Long)
final case class AnalyticsReportOutput(
    asOf: Instant,
    funnel: Vector[AnalyticsFunnelDayOutput],
    timeToHire: Option[AnalyticsTimeToHireOutput],
    skillPostingActivity: Vector[AnalyticsSkillPostingDayOutput]
)
