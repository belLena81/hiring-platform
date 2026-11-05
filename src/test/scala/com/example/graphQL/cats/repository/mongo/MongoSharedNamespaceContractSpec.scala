package com.example.graphQL.cats.repository.mongo

import io.circe.Json
import io.circe.parser.parse

import java.nio.file.{Files, Path}

/** The MongoDB names shared with the separate analytics build match the contract fixture asserted by both builds. */
class MongoSharedNamespaceContractSpec extends munit.FunSuite {
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
      "analyticsErasureRequests" -> MongoCollections.AnalyticsErasureRequests,
      "analyticsErasureCompletions" -> MongoCollections.AnalyticsErasureCompletions,
      "analyticsErasureDeltaFiles" -> MongoCollections.AnalyticsErasureDeltaFiles,
      "analyticsWorkerHeartbeats" -> MongoCollections.AnalyticsWorkerHeartbeats,
      "analyticsReportSnapshots" -> MongoCollections.AnalyticsReportSnapshots,
      "analyticsReportControl" -> MongoCollections.AnalyticsReportControl,
      "analyticsReportRuns" -> MongoCollections.AnalyticsReportRuns,
      "eventOutbox" -> MongoCollections.EventOutbox,
      "hiringMigrationLedger" -> MongoCollections.HiringMigrationLedger,
      "outboxSubjectFences" -> MongoCollections.OutboxSubjectFences,
      "users" -> MongoCollections.Users,
      "producerRegistrations" -> MongoProducerRegistrations.Collection
    )
    assertEquals(actual, strings("collections"))
  }

  test("MSC-07.1 shared field names match the contract") {
    val actual = Map(
      "Id" -> MongoFields.Id,
      "State" -> MongoFields.State,
      "SubjectIds" -> MongoFields.SubjectIds,
      "SubjectRefsVersion" -> MongoFields.SubjectRefsVersion,
      "LeaseToken" -> MongoFields.LeaseToken,
      "LeaseUntil" -> MongoFields.LeaseUntil,
      "UpdatedAt" -> MongoFields.UpdatedAt,
      "CreatedAt" -> MongoFields.CreatedAt,
      "ExpiresAt" -> MongoFields.ExpiresAt,
      "ReceiptId" -> MongoFields.ReceiptId,
      "AccountStatus" -> MongoFields.AccountStatus,
      "RequestedAt" -> MongoFields.RequestedAt,
      "FencingVersion" -> MongoFields.FencingVersion,
      "Deleted" -> MongoFields.Deleted,
      "Topic" -> MongoFields.Topic,
      "Generation" -> MongoFields.Generation,
      "Revision" -> MongoFields.Revision,
      "NextRevision" -> MongoFields.NextRevision,
      "LastPublishedRevision" -> MongoFields.LastPublishedRevision,
      "LastRunId" -> MongoFields.LastRunId,
      "RunId" -> MongoFields.RunId,
      "HiddenAt" -> MongoFields.HiddenAt,
      "CompletedAt" -> MongoFields.CompletedAt,
      "AsOf" -> MongoFields.AsOf,
      "SubjectId" -> MongoFields.SubjectId,
      "Kind" -> MongoFields.Kind,
      "TransactionalId" -> MongoFields.TransactionalId,
      "GenerationId" -> MongoFields.GenerationId,
      "FencedAt" -> MongoFields.FencedAt,
      "ProducerRegistry" -> MongoFields.ProducerRegistry
    )
    assertEquals(actual, strings("fields"))
  }

  test("MSC-07.1 producer registration kind and state values match the contract") {
    val values = contract.hcursor.downField("values")
    def named(name: String): Map[String, String] =
      values.downField(name).as[Map[String, String]].toOption.get
    assertEquals(
      Map(
        "operational" -> ProducerRegistrationValues.Operational,
        "interview" -> ProducerRegistrationValues.Interview
      ),
      named("producerRegistrationKinds")
    )
    assertEquals(
      Map("active" -> ProducerRegistrationValues.Active, "fenced" -> ProducerRegistrationValues.Fenced),
      named("producerRegistrationStates")
    )
  }

  test("MSC-07.1 shared ledger migration ids match the contract") {
    val actual = Map(
      "eventOutboxSubjectReferences" -> MigrationIds.EventOutboxSubjectReferences.value,
      "attributableProducerRegistrations" -> MigrationIds.AttributableProducerRegistrations.value,
      "producerGenerationRegistrations" -> MigrationIds.ProducerGenerationRegistrations.value
    )
    assertEquals(actual, strings("migrationIds"))
  }

  test("MSC-07.1 main-owned index names on shared collections match the contract") {
    val actual = Set(
      "event_outbox" -> MongoIndexNames.EventOutboxClaim,
      "event_outbox" -> MongoIndexNames.EventOutboxPublishedRetention,
      "event_outbox" -> MongoIndexNames.EventOutboxSubjectIds,
      "outbox_subject_fences" -> MongoIndexNames.OutboxSubjectFenceLease,
      "users" -> MongoIndexNames.UsersEmail,
      "analytics_erasure_requests" -> MongoIndexNames.AnalyticsErasureRequestState,
      "analytics_erasure_requests" -> MongoIndexNames.AnalyticsErasureRequestReceipt,
      "analytics_erasure_requests" -> MongoIndexNames.AnalyticsErasureRequestExpiry,
      "analytics_erasure_completions" -> MongoIndexNames.AnalyticsErasureCompletionReceipt,
      "analytics_worker_heartbeats" -> MongoIndexNames.AnalyticsWorkerHeartbeatExpiry,
      "analytics_report_snapshots" -> MongoIndexNames.AnalyticsReportPublished,
      "analytics_report_snapshots" -> MongoIndexNames.AnalyticsReportExpiry,
      "analytics_report_runs" -> MongoIndexNames.AnalyticsReportRunExpiry,
      "producer_registrations" -> MongoIndexNames.ProducerRegistrationSubjectState,
      "producer_registrations" -> MongoIndexNames.ProducerRegistrationGenerationState,
      "producer_registrations" -> MongoIndexNames.ProducerRegistrationFencedExpiry
    )
    val expected = contract.hcursor
      .downField("indexes")
      .values
      .get
      .map(_.hcursor)
      .filter(_.get[String]("owner").toOption.contains("main"))
      .map(c => c.get[String]("collection").toOption.get -> c.get[String]("name").toOption.get)
      .toSet
    assertEquals(actual, expected)
  }
}
