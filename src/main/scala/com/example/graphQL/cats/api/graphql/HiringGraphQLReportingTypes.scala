package com.example.graphQL.cats.api.graphql

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
        simple("created", LongType)(_.created),
        simple("accepted", LongType)(_.accepted),
        simple("declined", LongType)(_.declined),
        simple("interview", LongType)(_.interview),
        simple("hired", LongType)(_.hired),
        simple("rejected", LongType)(_.rejected)
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
        simple("eligibleCount", LongType)(_.eligibleCount),
        simple("excludedCount", LongType)(_.excludedCount)
      )
    )
  lazy val analyticsSkillPostingDayType
      : ObjectType[RequestContext, com.example.graphQL.cats.service.AnalyticsSkillPostingDay] =
    ObjectType(
      "AnalyticsSkillPostingDay",
      fields[RequestContext, com.example.graphQL.cats.service.AnalyticsSkillPostingDay](
        instantField("day", _.day),
        simple("skill", StringType)(_.skill),
        simple("postings", LongType)(_.postings)
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
    enumType("EmbeddingCoverageEntityKind", EmbeddingWorkKind.values.toList)
  lazy val embeddingFreshnessType: EnumType[EmbeddingFreshness] =
    enumType("EmbeddingFreshness", EmbeddingFreshness.values.toList)
  lazy val embeddingRepairStateType: EnumType[EmbeddingRepairState] =
    enumType("EmbeddingRepairState", EmbeddingRepairState.values.toList)
  lazy val embeddingFailureReasonType: EnumType[EmbeddingWorkFailure] =
    enumType("EmbeddingFailureReason", EmbeddingWorkFailure.values.toList)
  lazy val embeddingCoverageCheckNameType: EnumType[EmbeddingCoverageCheckName] =
    enumType("EmbeddingCoverageCheckName", EmbeddingCoverageCheckName.values.toList)
  lazy val embeddingCoverageCheckStatusType: EnumType[EmbeddingCoverageCheckStatus] =
    enumType("EmbeddingCoverageCheckStatus", EmbeddingCoverageCheckStatus.values.toList)
  lazy val embeddingCoverageKindSummaryType: ObjectType[RequestContext, EmbeddingCoverageKindSummary] = ObjectType(
    "EmbeddingCoverageKindSummary",
    fields[RequestContext, EmbeddingCoverageKindSummary](
      simple("kind", embeddingCoverageEntityKindType)(_.kind),
      simple("searchableCount", LongType)(_.searchableCount),
      simple("scannedCount", LongType)(_.scannedCount),
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
      simple("count", LongType)(_.count)
    )
  )
  lazy val embeddingObservedModelType: ObjectType[RequestContext, EmbeddingObservedModel] = ObjectType(
    "EmbeddingObservedModel",
    fields[RequestContext, EmbeddingObservedModel](
      simple("kind", embeddingCoverageEntityKindType)(_.kind),
      simple("model", StringType)(_.model),
      simple("count", LongType)(_.count)
    )
  )
  lazy val embeddingCoverageLagType: ObjectType[RequestContext, EmbeddingCoverageLag] = ObjectType(
    "EmbeddingCoverageLag",
    fields[RequestContext, EmbeddingCoverageLag](
      simple("p50Seconds", LongType)(_.p50Seconds),
      simple("p95Seconds", LongType)(_.p95Seconds),
      simple("p99Seconds", LongType)(_.p99Seconds),
      simple("sampleCount", LongType)(_.sampleCount)
    )
  )
  lazy val embeddingCoverageCheckType: ObjectType[RequestContext, EmbeddingCoverageCheck] = ObjectType(
    "EmbeddingCoverageCheck",
    fields[RequestContext, EmbeddingCoverageCheck](
      simple("check", embeddingCoverageCheckNameType)(_.name),
      simple("status", embeddingCoverageCheckStatusType)(_.status),
      simple("offendingCount", LongType)(_.offendingCount)
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
      simple("oldestQueuedWorkAgeSeconds", OptionType(LongType))(_.oldestQueuedWorkAgeSeconds),
      simple("oldestNotCurrentAgeSeconds", OptionType(LongType))(_.oldestNotCurrentAgeSeconds),
      simple("lagEntityKinds", ListType(embeddingCoverageEntityKindType))(_.lagEntityKinds),
      simple("lagSeconds", OptionType(embeddingCoverageLagType))(_.lagSeconds),
      simple("checks", ListType(embeddingCoverageCheckType))(_.checks)
    )
  )
}
