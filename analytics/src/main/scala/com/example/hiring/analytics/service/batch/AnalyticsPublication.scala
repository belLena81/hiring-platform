package com.example.hiring.analytics.service.batch

import com.example.hiring.analytics.domain.RunId
import java.time.Instant

enum AnalyticsRunOutcome {
  case QualityBlocked, ErasurePending, Published
}

final case class AnalyticsPublication(
    runId: RunId,
    outcome: AnalyticsRunOutcome,
    completedAt: Instant,
    funnelGoldPath: String,
    timeToHireGoldPath: String,
    skillPostingGoldPath: String,
    bronzeRecords: Long,
    validRecords: Long,
    suppressedRecords: Long,
    quarantinedRecords: Long,
    conflictingEventIds: Long
)
