package com.example.graphQL.cats.api.graphql

import com.example.graphQL.cats.api.graphql.HiringGraphQLDsl.describedEnum
import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.AnalyticsReportPayload
import com.example.graphQL.cats.api.graphql.HiringGraphQLTypes.simple
import com.example.graphQL.cats.service.port.{EmbeddingWorkFailure, EmbeddingWorkKind}
import com.example.graphQL.cats.service.search.*
import sangria.schema.*

/** Output types of the analytics report and the embedding-coverage report. */
private[graphql] object HiringGraphQLReportingTypes {
  lazy val analyticsFunnelDayType: ObjectType[RequestContext, com.example.graphQL.cats.service.AnalyticsFunnelDay] =
    ObjectType(
      "AnalyticsFunnelDay",
      fields[RequestContext, com.example.graphQL.cats.service.AnalyticsFunnelDay](
        instantField("day", _.day),
        simple("created", longType)(_.created),
        simple("accepted", longType)(_.accepted),
        simple("declined", longType)(_.declined),
        simple("interview", longType)(_.interview),
        simple("hired", longType)(_.hired),
        simple("rejected", longType)(_.rejected)
      )
    )
  lazy val analyticsTimeToHireType: ObjectType[RequestContext, com.example.graphQL.cats.service.AnalyticsTimeToHire] =
    ObjectType(
      "AnalyticsTimeToHire",
      fields[RequestContext, com.example.graphQL.cats.service.AnalyticsTimeToHire](
        simple("p50Hours", FloatType)(_.p50Hours),
        simple("p75Hours", FloatType)(_.p75Hours),
        simple("p90Hours", FloatType)(_.p90Hours),
        simple("p95Hours", FloatType)(_.p95Hours),
        simple("eligibleCount", longType)(_.eligibleCount),
        simple("excludedCount", longType)(_.excludedCount)
      )
    )
  lazy val analyticsSkillPostingDayType
      : ObjectType[RequestContext, com.example.graphQL.cats.service.AnalyticsSkillPostingDay] =
    ObjectType(
      "AnalyticsSkillPostingDay",
      fields[RequestContext, com.example.graphQL.cats.service.AnalyticsSkillPostingDay](
        instantField("day", _.day),
        simple("skill", StringType)(_.skill),
        simple("postings", longType)(_.postings)
      )
    )
  lazy val analyticsReportType: ObjectType[RequestContext, AnalyticsReportPayload] =
    ObjectType(
      "AnalyticsReport",
      fields[RequestContext, AnalyticsReportPayload](
        instantField("asOf", _.snapshot.asOf),
        simple("funnel", ListType(analyticsFunnelDayType))(_.snapshot.funnel),
        simple("timeToHire", OptionType(analyticsTimeToHireType))(_.snapshot.timeToHire),
        simple("skillPostingActivity", ListType(analyticsSkillPostingDayType))(_.snapshot.skillPostingActivity)
      )
    )

  lazy val embeddingCoverageEntityKindType: EnumType[EmbeddingWorkKind] =
    describedEnum(
      "EmbeddingCoverageEntityKind",
      "The kind of entity that has an embedding.",
      EmbeddingWorkKind.values.toList
    ) {
      case EmbeddingWorkKind.Job              => "A job posting."
      case EmbeddingWorkKind.CandidateProfile => "A candidate profile."
    }
  lazy val embeddingFreshnessType: EnumType[EmbeddingFreshness] =
    describedEnum(
      "EmbeddingFreshness",
      "Whether an entity's embedding matches its current content.",
      EmbeddingFreshness.values.toList
    ) {
      case EmbeddingFreshness.Current        => "The embedding matches the content and the expected model."
      case EmbeddingFreshness.ContentChanged => "The content changed after the embedding was computed."
      case EmbeddingFreshness.ModelMismatch  => "The embedding was computed by a different model."
      case EmbeddingFreshness.NotEmbedded    => "The entity has no embedding."
    }
  lazy val embeddingRepairStateType: EnumType[EmbeddingRepairState] =
    describedEnum(
      "EmbeddingRepairState",
      "Whether a repair is scheduled for an entity.",
      EmbeddingRepairState.values.toList
    ) {
      case EmbeddingRepairState.NoQueuedWork => "No repair is queued."
      case EmbeddingRepairState.Waiting      => "A repair is queued and ready."
      case EmbeddingRepairState.Retrying     => "A repair failed and is waiting to be retried."
      case EmbeddingRepairState.InProgress   => "A worker is repairing the entity."
      case EmbeddingRepairState.LeaseExpired => "A worker claimed the repair and its lease expired."
      case EmbeddingRepairState.Failed       => "The repair failed permanently."
    }
  lazy val embeddingFailureReasonType: EnumType[EmbeddingWorkFailure] =
    describedEnum(
      "EmbeddingFailureReason",
      "Why an embedding repair failed permanently.",
      EmbeddingWorkFailure.values.toList
    ) {
      case EmbeddingWorkFailure.RetryExhausted   => "Every retry failed."
      case EmbeddingWorkFailure.DocumentTooLarge => "The document is too large to embed."
      case EmbeddingWorkFailure.InvalidWorkKey   => "The queued work item was malformed."
      case EmbeddingWorkFailure.InvalidResponse  => "The provider returned an unusable response."
    }
  lazy val embeddingCoverageCheckNameType: EnumType[EmbeddingCoverageCheckName] =
    describedEnum(
      "EmbeddingCoverageCheckName",
      "A health check over the embedding coverage.",
      EmbeddingCoverageCheckName.values.toList
    ) {
      case EmbeddingCoverageCheckName.NoOrphanedGap => "Every entity without a current embedding has queued work."
      case EmbeddingCoverageCheckName.NoStuckWork   => "No queued work is stuck."
    }
  lazy val embeddingCoverageCheckStatusType: EnumType[EmbeddingCoverageCheckStatus] =
    describedEnum(
      "EmbeddingCoverageCheckStatus",
      "The result of a coverage check.",
      EmbeddingCoverageCheckStatus.values.toList
    ) {
      case EmbeddingCoverageCheckStatus.Passed       => "The check passed."
      case EmbeddingCoverageCheckStatus.Failed       => "The check found offending entities."
      case EmbeddingCoverageCheckStatus.Inconclusive => "The scan was truncated, so the check cannot decide."
    }
  lazy val embeddingCoverageKindSummaryType: ObjectType[RequestContext, EmbeddingCoverageKindSummary] = ObjectType(
    "EmbeddingCoverageKindSummary",
    fields[RequestContext, EmbeddingCoverageKindSummary](
      simple("kind", embeddingCoverageEntityKindType)(_.kind),
      simple("searchableCount", longType)(_.searchableCount),
      simple("scannedCount", longType)(_.scannedCount),
      simple("truncated", BooleanType)(_.truncated),
      simple("coverageShare", OptionType(FloatType))(_.coverageShare)
    )
  )
  lazy val embeddingCoverageCellType: ObjectType[RequestContext, EmbeddingCoverageCell] = ObjectType(
    "EmbeddingCoverageCell",
    fields[RequestContext, EmbeddingCoverageCell](
      simple("kind", embeddingCoverageEntityKindType)(_.kind),
      simple("freshness", embeddingFreshnessType)(_.freshness),
      simple("repairState", embeddingRepairStateType)(_.repairState),
      simple("failureReason", OptionType(embeddingFailureReasonType))(_.failure),
      simple("count", longType)(_.count)
    )
  )
  lazy val embeddingObservedModelType: ObjectType[RequestContext, EmbeddingObservedModel] = ObjectType(
    "EmbeddingObservedModel",
    fields[RequestContext, EmbeddingObservedModel](
      simple("kind", embeddingCoverageEntityKindType)(_.kind),
      simple("model", StringType)(_.model),
      simple("count", longType)(_.count)
    )
  )
  lazy val embeddingCoverageLagType: ObjectType[RequestContext, EmbeddingCoverageLag] = ObjectType(
    "EmbeddingCoverageLag",
    fields[RequestContext, EmbeddingCoverageLag](
      simple("p50Seconds", longType)(_.p50Seconds),
      simple("p95Seconds", longType)(_.p95Seconds),
      simple("p99Seconds", longType)(_.p99Seconds),
      simple("sampleCount", longType)(_.sampleCount)
    )
  )
  lazy val embeddingCoverageCheckType: ObjectType[RequestContext, EmbeddingCoverageCheck] = ObjectType(
    "EmbeddingCoverageCheck",
    fields[RequestContext, EmbeddingCoverageCheck](
      simple("check", embeddingCoverageCheckNameType)(_.name),
      simple("status", embeddingCoverageCheckStatusType)(_.status),
      simple("offendingCount", longType)(_.offendingCount)
    )
  )
  lazy val embeddingCoverageReportType: ObjectType[RequestContext, EmbeddingCoverageReport] = ObjectType(
    "EmbeddingCoverageReport",
    fields[RequestContext, EmbeddingCoverageReport](
      instantField("asOf", _.asOf),
      simple("expectedModel", OptionType(StringType))(_.expectedModel),
      simple("kinds", ListType(embeddingCoverageKindSummaryType))(_.kinds),
      simple("cells", ListType(embeddingCoverageCellType))(_.cells),
      simple("observedModels", ListType(embeddingObservedModelType))(_.observedModels),
      simple("queueTruncated", BooleanType)(_.queueTruncated),
      simple("oldestQueuedWorkAgeSeconds", OptionType(longType))(_.oldestQueuedWorkAgeSeconds),
      simple("oldestNotCurrentAgeSeconds", OptionType(longType))(_.oldestNotCurrentAgeSeconds),
      simple("lagEntityKinds", ListType(embeddingCoverageEntityKindType))(_.lagEntityKinds),
      simple("lagSeconds", OptionType(embeddingCoverageLagType))(_.lagSeconds),
      simple("checks", ListType(embeddingCoverageCheckType))(_.checks)
    )
  )
}
