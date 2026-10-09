package com.example.hiring.analytics.app

import com.example.hiring.analytics.adapter.spark.AnalyticsKeyRetirement
import com.example.hiring.analytics.config.*
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths

/** Typed, validated inputs for the read-only key-retirement diagnostic. */
private[app] object KeyRetirementAuditInputs {
  final case class AuditInputs(
      mongoUri: String,
      mongoDatabase: String,
      sparkMaster: String,
      paths: AnalyticsLakehousePaths,
      operational: AnalyticsOperationalSettings,
      retiringKeyId: String,
      retention: AnalyticsKeyRetirement.RetentionEvidence,
      writers: AnalyticsKeyRetirement.WriterInventory
  )

  def from(settings: AnalyticsKeyRetirementAuditSettings): Either[AnalyticsError, AuditInputs] =
    AppModule.resolveLakehousePaths(settings.lakehouseRoot).map { paths =>
      def writerRecord(value: AnalyticsAuditWriter): AnalyticsKeyRetirement.WriterRecord =
        AnalyticsKeyRetirement.WriterRecord(
          value.identity.fold("")(identity),
          value.disposition.fold(AnalyticsKeyRetirement.WriterDisposition.Unknown) {
            case AnalyticsAuditWriterDisposition.Stopped       => AnalyticsKeyRetirement.WriterDisposition.Stopped
            case AnalyticsAuditWriterDisposition.AccessRevoked => AnalyticsKeyRetirement.WriterDisposition.AccessRevoked
            case AnalyticsAuditWriterDisposition.Active        => AnalyticsKeyRetirement.WriterDisposition.Active
            case AnalyticsAuditWriterDisposition.Unknown       => AnalyticsKeyRetirement.WriterDisposition.Unknown
          },
          value.evidenceReference.fold("")(identity)
        )

      AuditInputs(
        settings.mongoUri,
        settings.mongoDatabase,
        settings.sparkMaster,
        paths,
        settings.operational,
        settings.retiringKeyId,
        AnalyticsKeyRetirement.RetentionEvidence(
          AnalyticsKeyRetirement.KafkaRetentionEvidence(
            settings.kafkaBarrierOffset,
            settings.kafkaEarliestAvailableOffset,
            settings.kafkaEvidenceReference.fold("")(identity)
          ),
          AnalyticsKeyRetirement.RetentionHorizon(
            settings.deltaData.retainedUntil,
            settings.deltaData.evidenceReference.fold("")(identity)
          ),
          AnalyticsKeyRetirement.RetentionHorizon(
            settings.deltaLogs.retainedUntil,
            settings.deltaLogs.evidenceReference.fold("")(identity)
          ),
          AnalyticsKeyRetirement.RetentionHorizon(
            settings.reports.retainedUntil,
            settings.reports.evidenceReference.fold("")(identity)
          )
        ),
        AnalyticsKeyRetirement.WriterInventory(
          settings.writers.observedAt,
          settings.writers.coverageReference.fold("")(identity),
          settings.writers.managed.map(writerRecord),
          settings.writers.unmanaged.map(writerRecord)
        )
      )
    }

}
