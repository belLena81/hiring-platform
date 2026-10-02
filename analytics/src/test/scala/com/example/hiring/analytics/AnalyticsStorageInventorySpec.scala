package com.example.hiring.analytics

import com.example.hiring.analytics.service.batch.*
import munit.FunSuite

final class AnalyticsStorageInventorySpec extends FunSuite {
  private val paths = AnalyticsLakehousePaths.from("file:/tmp/hiring-inventory").toOption.get

  test("inventory covers facts, retained control datasets, receipts and explicit runtime storage") {
    val inventory = AnalyticsStorageInventory(paths, Vector("file:/tmp/hiring-checkpoint"), Vector("/tmp/hiring-spill"))
    val locations = inventory.surfaces.map(_.location)
    assertEquals(locations.distinct.size, locations.size)
    assertEquals(
      inventory.subjectDelta.map(_.location).toSet,
      Set(paths.bronze, paths.quarantine, paths.silver, paths.lateFacts)
    )
    assert(
      Set(paths.streamingProgress, paths.streamingDecisions, paths.manifests, paths.hmacKeyRegistry)
        .subsetOf(inventory.delta.filter(_.privacy == AnalyticsStoragePrivacy.SanitizedControl).map(_.location).toSet)
    )
    assert(
      Set(
        "analytics_report_runs",
        "analytics_late_fact_replay_requests",
        "analytics_streaming_activation",
        "analytics_streaming_lakehouses"
      )
        .subsetOf(inventory.mongo.map(_.location).toSet)
    )
    val checkpoint = inventory.surfaces.find(_.kind == AnalyticsStorageKind.SparkCheckpoint).get
    assertEquals(checkpoint.retention, AnalyticsStorageRetention.Permanent)
    assertEquals(checkpoint.privacy, AnalyticsStoragePrivacy.SanitizedControl)
    assert(!checkpoint.subjectAttributed)
    val lineage = inventory.surfaces.find(_.kind == AnalyticsStorageKind.LineageControl).get
    assertEquals(lineage.location, paths.streamingLineage)
    assertEquals(lineage.retention, AnalyticsStorageRetention.Permanent)
    assert(
      inventory.surfaces
        .filter(_.kind == AnalyticsStorageKind.Scratch)
        .forall(_.retention == AnalyticsStorageRetention.ProcessLifetime)
    )
  }

  test("permanent continuity and authorization metadata has no subject erasure policy") {
    val permanent = paths.inventory.surfaces.filter(surface =>
      surface.retention == AnalyticsStorageRetention.Permanent &&
        surface.privacy == AnalyticsStoragePrivacy.SanitizedControl
    )
    assert(permanent.nonEmpty)
    assert(permanent.forall(surface => !surface.subjectAttributed))
  }
}
