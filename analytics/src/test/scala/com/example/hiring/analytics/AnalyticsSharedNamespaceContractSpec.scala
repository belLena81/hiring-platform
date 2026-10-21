package com.example.hiring.analytics

import com.example.hiring.analytics.adapter.mongo.{AnalyticsCollections, MongoAnalyticsErasureProgress}
import io.circe.Json
import io.circe.parser.parse

import java.nio.file.{Files, Path}

/** The MongoDB names shared with the application build match the contract fixture asserted by both builds. */
class AnalyticsSharedNamespaceContractSpec extends munit.FunSuite {
  private val contract: Json = {
    val relative = Path.of("test-support", "src", "main", "resources", "mongo-shared-namespace-contract.json")
    val start = Path.of(System.getProperty("user.dir")).toAbsolutePath
    val found = Iterator
      .iterate(Option(start))(_.flatMap(dir => Option(dir.getParent)))
      .flatten
      .map(_.resolve(relative))
      .find(Files.isRegularFile(_))
    parse(Files.readString(found.getOrElse(fail("Mongo shared namespace contract fixture is missing")))).toOption.get
  }

  private def strings(name: String): Map[String, String] =
    contract.hcursor.downField(name).as[Map[String, String]].toOption.get

  test("MSC-07.1 shared collection names match the contract") {
    val actual = Map(
      "analyticsErasureRequests" -> AnalyticsCollections.ErasureRequests,
      "analyticsErasureCompletions" -> AnalyticsCollections.ErasureCompletions,
      "analyticsErasureDeltaFiles" -> AnalyticsCollections.ErasureDeltaFiles,
      "analyticsWorkerHeartbeats" -> AnalyticsCollections.ErasureHeartbeats,
      "analyticsReportSnapshots" -> AnalyticsCollections.ReportSnapshots,
      "analyticsReportControl" -> AnalyticsCollections.ReportControl,
      "analyticsReportRuns" -> AnalyticsCollections.ReportRuns,
      "eventOutbox" -> AnalyticsCollections.EventOutbox,
      "hiringMigrationLedger" -> AnalyticsCollections.HiringMigrationLedger,
      "outboxSubjectFences" -> AnalyticsCollections.OutboxSubjectFences,
      "users" -> AnalyticsCollections.Users,
      "producerRegistrations" -> AnalyticsCollections.ProducerRegistrations
    )
    assertEquals(actual, strings("collections"))
  }

  test("MSC-07.1 shared field names match the contract") {
    val f = AnalyticsCollections.Fields
    val actual = Map(
      "Id" -> f.Id,
      "State" -> f.State,
      "SubjectIds" -> f.SubjectIds,
      "SubjectRefsVersion" -> f.SubjectRefsVersion,
      "LeaseToken" -> f.LeaseToken,
      "LeaseUntil" -> f.LeaseUntil,
      "UpdatedAt" -> f.UpdatedAt,
      "CreatedAt" -> f.CreatedAt,
      "ExpiresAt" -> f.ExpiresAt,
      "ReceiptId" -> f.ReceiptId,
      "AccountStatus" -> f.AccountStatus,
      "RequestedAt" -> f.RequestedAt,
      "FencingVersion" -> f.FencingVersion,
      "Deleted" -> f.Deleted,
      "Topic" -> f.Topic,
      "Generation" -> f.Generation,
      "Revision" -> f.Revision,
      "NextRevision" -> f.NextRevision,
      "LastPublishedRevision" -> f.LastPublishedRevision,
      "LastRunId" -> f.LastRunId,
      "RunId" -> f.RunId,
      "HiddenAt" -> f.HiddenAt,
      "CompletedAt" -> f.CompletedAt,
      "AsOf" -> f.AsOf,
      "SubjectId" -> f.SubjectId,
      "Kind" -> f.Kind,
      "TransactionalId" -> f.TransactionalId,
      "FencedAt" -> f.FencedAt,
      "ProducerRegistry" -> f.ProducerRegistry
    )
    assertEquals(actual, strings("fields"))
  }

  test("MSC-07.1 analytics-owned index names match the contract") {
    val n = AnalyticsCollections.IndexNames
    val known = Set(
      AnalyticsCollections.ReportRuns -> n.ReportRunsRetention,
      AnalyticsCollections.ErasureDeltaFiles -> n.ErasureDeltaFilesRequest
    )
    val owned = contract.hcursor
      .downField("indexes")
      .as[List[Map[String, String]]]
      .toOption
      .get
      .filter(_.get("owner").contains("analytics"))
      .map(i => i("collection") -> i("name"))
    assertEquals(owned.toSet, known)
    assertEquals(owned.size, known.size)
    assertEquals(MongoAnalyticsErasureProgress.DeltaEvidenceRequestIndex, n.ErasureDeltaFilesRequest)
  }

  test("MSC-07.1 analytics-only collection index names keep their persisted values") {
    assertEquals(
      List(
        AnalyticsCollections.IndexNames.LateFactReplayDetailsRetention,
        AnalyticsCollections.IndexNames.LateFactReplayReceiptDependencies
      ),
      List("analytics_late_fact_replay_details_retention_v1", "analytics_late_fact_replay_receipt_dependencies_v1")
    )
  }

  test("MSC-07.1 the ledger migration ids analytics names match the contract") {
    val actual = Map(
      "eventOutboxSubjectReferences" -> AnalyticsCollections.MigrationIds.OutboxSubjectReferences,
      "attributableProducerRegistrations" -> AnalyticsCollections.MigrationIds.AttributableProducerRegistrations
    )
    assertEquals(actual, strings("migrationIds"))
  }

  test("MSC-07.1 producer registration kind and state values match the contract") {
    val r = AnalyticsCollections.ProducerRegistration
    def values(name: String) = contract.hcursor.downField("values").downField(name).as[Map[String, String]].toOption.get
    assertEquals(
      Map("operational" -> r.OperationalKind, "interview" -> r.InterviewKind),
      values("producerRegistrationKinds")
    )
    assertEquals(Map("active" -> r.ActiveState, "fenced" -> r.FencedState), values("producerRegistrationStates"))
  }
}
