package com.example.hiring.analytics.service.batch

/** Policies apply to current rows and their retained files/logs. Control records carry no event payloads or subjects.
  */
enum AnalyticsStoragePrivacy {
  case RawSubjects, PseudonymousSubjects, SuppressedAggregates, SanitizedControl
}

enum AnalyticsStorageRetention {
  case Bronze, Quarantine, Silver, LateFacts, PublishedSnapshot, DeletionMarker
  case RecoveryProgress, PublicationReceipt, ReplayRecovery, Permanent, ProcessLifetime
}

enum AnalyticsStorageKind {
  case Delta, Mongo, SparkCheckpoint, LineageControl, Scratch
}

final case class AnalyticsStorageSurface(
    location: String,
    kind: AnalyticsStorageKind,
    privacy: AnalyticsStoragePrivacy,
    retention: AnalyticsStorageRetention
) {
  def subjectAttributed: Boolean = privacy == AnalyticsStoragePrivacy.RawSubjects ||
    privacy == AnalyticsStoragePrivacy.PseudonymousSubjects
}

/** One inventory drives expiry, erasure, reclamation and retirement. Spark owns its offset/commit retention. */
final class AnalyticsStorageInventory private (val surfaces: Vector[AnalyticsStorageSurface]) {
  val delta: Vector[AnalyticsStorageSurface] = surfaces.filter(_.kind == AnalyticsStorageKind.Delta)
  val subjectDelta: Vector[AnalyticsStorageSurface] = delta.filter(_.subjectAttributed)
  val rawDelta: Vector[AnalyticsStorageSurface] = delta.filter(_.privacy == AnalyticsStoragePrivacy.RawSubjects)
  val mongo: Vector[AnalyticsStorageSurface] = surfaces.filter(_.kind == AnalyticsStorageKind.Mongo)
}

object AnalyticsStorageInventory {
  import AnalyticsStorageKind.*
  import AnalyticsStoragePrivacy.*
  import AnalyticsStorageRetention.*

  def apply(
      paths: AnalyticsLakehousePaths,
      checkpointLocations: Vector[String] = Vector.empty,
      spillDirectories: Vector[String] = Vector.empty
  ): AnalyticsStorageInventory = {
    val delta = Vector(
      AnalyticsStorageSurface(paths.bronze, Delta, RawSubjects, Bronze),
      AnalyticsStorageSurface(paths.quarantine, Delta, RawSubjects, Quarantine),
      AnalyticsStorageSurface(paths.silver, Delta, PseudonymousSubjects, Silver),
      AnalyticsStorageSurface(paths.lateFacts, Delta, PseudonymousSubjects, LateFacts),
      AnalyticsStorageSurface(paths.funnelGold, Delta, SuppressedAggregates, Silver),
      AnalyticsStorageSurface(paths.timeToHireGold, Delta, SuppressedAggregates, Silver),
      AnalyticsStorageSurface(paths.skillsGold, Delta, SuppressedAggregates, Silver),
      AnalyticsStorageSurface(paths.manifests, Delta, SanitizedControl, Permanent),
      AnalyticsStorageSurface(paths.streamingProgress, Delta, SanitizedControl, RecoveryProgress),
      AnalyticsStorageSurface(paths.streamingDecisions, Delta, SanitizedControl, RecoveryProgress),
      AnalyticsStorageSurface(paths.hmacKeyRegistry, Delta, SanitizedControl, Permanent)
    )
    val mongo = Vector(
      AnalyticsStorageSurface("analytics_report_snapshots", Mongo, SuppressedAggregates, PublishedSnapshot),
      AnalyticsStorageSurface("analytics_report_runs", Mongo, SanitizedControl, PublicationReceipt),
      AnalyticsStorageSurface("analytics_report_control", Mongo, SanitizedControl, Permanent),
      AnalyticsStorageSurface("analytics_erasure_requests", Mongo, RawSubjects, DeletionMarker),
      AnalyticsStorageSurface("analytics_erasure_completions", Mongo, RawSubjects, Permanent),
      AnalyticsStorageSurface("analytics_erasure_delta_files", Mongo, RawSubjects, DeletionMarker),
      AnalyticsStorageSurface("analytics_late_fact_replay_requests", Mongo, SanitizedControl, ReplayRecovery),
      AnalyticsStorageSurface("analytics_streaming_activation", Mongo, SanitizedControl, Permanent),
      AnalyticsStorageSurface("analytics_streaming_lakehouses", Mongo, SanitizedControl, Permanent),
      AnalyticsStorageSurface("analytics_lakehouse_mutexes", Mongo, SanitizedControl, Permanent),
      AnalyticsStorageSurface("analytics_hmac_key_retirements", Mongo, SanitizedControl, Permanent),
      AnalyticsStorageSurface("analytics_worker_heartbeats", Mongo, SanitizedControl, ProcessLifetime),
      AnalyticsStorageSurface("event_outbox", Mongo, RawSubjects, Bronze),
      AnalyticsStorageSurface("outbox_subject_fences", Mongo, RawSubjects, Permanent),
      AnalyticsStorageSurface("hiring_migration_ledger", Mongo, SanitizedControl, Permanent)
    )
    val checkpoints = checkpointLocations.map(AnalyticsStorageSurface(_, SparkCheckpoint, SanitizedControl, Permanent))
    val lineage = Vector(AnalyticsStorageSurface(paths.streamingLineage, LineageControl, SanitizedControl, Permanent))
    val scratch = spillDirectories.map(AnalyticsStorageSurface(_, Scratch, RawSubjects, ProcessLifetime)) :+
      AnalyticsStorageSurface(
        s"${paths.root.stripSuffix("/")}/control/purge-rewrite-*",
        Scratch,
        RawSubjects,
        ProcessLifetime
      )
    new AnalyticsStorageInventory(delta ++ mongo ++ lineage ++ checkpoints ++ scratch)
  }
}
