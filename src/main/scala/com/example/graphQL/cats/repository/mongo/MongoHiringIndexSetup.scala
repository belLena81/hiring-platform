package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import fs2.interop.reactivestreams.*
import com.mongodb.{MongoClientSettings, MongoCommandException}
import com.mongodb.client.model.{Filters, IndexOptions, Indexes}
import mongo4cats.database.MongoDatabase
import mongo4cats.codecs.CodecRegistry
import org.bson.{BsonDocument, Document}
import org.bson.conversions.Bson
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.*

private[mongo] final case class IndexSpec(collection: String, keys: Bson, options: IndexOptions)

/** Ordinary Mongo indexes grouped as declarative collection specifications. */
private[mongo] object MongoHiringIndexSetup {
  import MongoHiringSetup.*

  private val publisherBufferSize = 32
  private val bsonRegistry = MongoClientSettings.getDefaultCodecRegistry

  private val indexSpecs: List[IndexSpec] = List(
    IndexSpec(
      MongoCollections.InterviewSubjectCleanup,
      Indexes.ascending("state", "requestedAt"),
      new IndexOptions().name("interview_subject_cleanup_due")
    ),
    IndexSpec(
      MongoCollections.Users,
      Indexes.ascending(MongoFields.EmailCanonical),
      new IndexOptions().name(UsersEmailIndex).unique(true).sparse(true)
    ),
    IndexSpec(
      MongoCollections.Users,
      Indexes.ascending(MongoFields.NameCanonical),
      new IndexOptions().name(UsersNameIndex).unique(true)
    ),
    IndexSpec(
      MongoCollections.Users,
      Indexes.compoundIndex(
        Indexes.ascending(MongoFields.AccountStatus),
        Indexes.descending(MongoFields.CreatedAt, MongoFields.Id)
      ),
      new IndexOptions().name(UsersStatusCreatedIndex)
    ),
    IndexSpec(
      MongoCollections.Users,
      Indexes.compoundIndex(
        Indexes.ascending(MongoFields.Role, MongoFields.AccountStatus),
        Indexes.descending(MongoFields.CreatedAt, MongoFields.Id)
      ),
      new IndexOptions().name(UsersRoleStatusCreatedIndex)
    ),
    IndexSpec(
      MongoCollections.Users,
      Indexes.ascending(MongoFields.AdminSingletonKey),
      new IndexOptions()
        .name(UsersAdminSingletonIndex)
        .unique(true)
        .partialFilterExpression(Filters.eq(MongoFields.Role, "Admin"))
    ),
    IndexSpec(
      MongoCollections.Users,
      Indexes.ascending(MongoFields.EmbeddingMetaModel, MongoFields.Role),
      new IndexOptions().name(UsersEmbeddingMetaIndex)
    ),
    IndexSpec(
      MongoCollections.Jobs,
      Indexes.compoundIndex(
        Indexes.ascending(MongoFields.RecruiterId, MongoFields.Status),
        Indexes.descending(MongoFields.CreatedAt, MongoFields.Id)
      ),
      new IndexOptions().name(JobsRecruiterStatusCreatedIndex)
    ),
    IndexSpec(
      MongoCollections.Jobs,
      Indexes.compoundIndex(
        Indexes.ascending(MongoFields.RecruiterId),
        Indexes.descending(MongoFields.CreatedAt, MongoFields.Id)
      ),
      new IndexOptions().name(JobsRecruiterCreatedIndex)
    ),
    IndexSpec(
      MongoCollections.Jobs,
      Indexes.descending(MongoFields.CreatedAt, MongoFields.Id),
      new IndexOptions().name(JobsCreatedIndex)
    ),
    IndexSpec(
      MongoCollections.Jobs,
      Indexes.compoundIndex(
        Indexes.ascending(MongoFields.Status),
        Indexes.descending(MongoFields.CreatedAt, MongoFields.Id)
      ),
      new IndexOptions().name(JobsOpenCreatedIndex)
    ),
    IndexSpec(
      MongoCollections.Jobs,
      Indexes.compoundIndex(
        Indexes.ascending(MongoFields.Status, MongoFields.LocationCity),
        Indexes.descending(MongoFields.CreatedAt, MongoFields.Id)
      ),
      new IndexOptions().name(JobsOpenCityCreatedIndex)
    ),
    IndexSpec(
      MongoCollections.Jobs,
      Indexes.geo2dsphere("location.point"),
      new IndexOptions().name(JobsLocationPointIndex)
    ),
    IndexSpec(
      MongoCollections.Jobs,
      Indexes.ascending(
        MongoFields.EmbeddingMetaModel,
        MongoFields.Status,
        MongoFields.LocationCity,
        MongoFields.RecruiterId
      ),
      new IndexOptions().name(JobsEmbeddingMetaIndex)
    ),
    IndexSpec(
      MongoCollections.Applications,
      Indexes.ascending(MongoFields.CandidateId, MongoFields.JobId),
      new IndexOptions().name(ApplicationsCandidateJobIndex).unique(true)
    ),
    IndexSpec(
      MongoCollections.Applications,
      Indexes.compoundIndex(
        Indexes.ascending(MongoFields.CandidateId, MongoFields.Status),
        Indexes.descending(MongoFields.CreatedAt, MongoFields.Id)
      ),
      new IndexOptions().name(ApplicationsCandidateStatusCreatedIndex)
    ),
    IndexSpec(
      MongoCollections.Applications,
      Indexes.compoundIndex(
        Indexes.ascending(MongoFields.CandidateId),
        Indexes.descending(MongoFields.CreatedAt, MongoFields.Id)
      ),
      new IndexOptions().name(ApplicationsCandidateCreatedIndex)
    ),
    IndexSpec(
      MongoCollections.Applications,
      Indexes.compoundIndex(
        Indexes.ascending(MongoFields.JobId, MongoFields.Status),
        Indexes.descending(MongoFields.CreatedAt, MongoFields.Id)
      ),
      new IndexOptions().name(ApplicationsJobStatusCreatedIndex)
    ),
    IndexSpec(
      MongoCollections.Applications,
      Indexes
        .compoundIndex(Indexes.ascending(MongoFields.JobId), Indexes.descending(MongoFields.CreatedAt, MongoFields.Id)),
      new IndexOptions().name(ApplicationsJobCreatedIndex)
    ),
    IndexSpec(
      MongoCollections.ApplicationEvents,
      Indexes.compoundIndex(
        Indexes.ascending(MongoFields.ApplicationId),
        Indexes.descending(MongoFields.OccurredAt, MongoFields.Id)
      ),
      new IndexOptions().name(ApplicationEventsApplicationCreatedIndex)
    ),
    IndexSpec(
      MongoCollections.EmbeddingWork,
      Indexes.ascending(MongoFields.State, MongoFields.AvailableAt, MongoFields.LeaseUntil),
      new IndexOptions().name(EmbeddingWorkAvailableIndex)
    ),
    IndexSpec(
      MongoCollections.EventOutbox,
      Indexes.ascending(
        MongoFields.State,
        MongoFields.AvailableAt,
        MongoFields.LeaseUntil,
        MongoFields.OccurredAt,
        MongoFields.Id
      ),
      new IndexOptions().name(EventOutboxClaimIndex)
    ),
    IndexSpec(
      MongoCollections.EventOutbox,
      Indexes.ascending(MongoFields.RetentionExpiresAt),
      new IndexOptions()
        .name(EventOutboxPublishedRetentionIndex)
        .expireAfter(0L, TimeUnit.SECONDS)
        .partialFilterExpression(Filters.eq(MongoFields.State, "Published"))
    ),
    IndexSpec(
      MongoCollections.EventOutbox,
      Indexes.ascending(MongoFields.SubjectIds),
      new IndexOptions().name(EventOutboxSubjectIdsIndex)
    ),
    IndexSpec(
      MongoCollections.OutboxSubjectFences,
      Indexes.ascending(MongoFields.Deleted, MongoFields.LeaseUntil),
      new IndexOptions().name(OutboxSubjectFenceLeaseIndex)
    ),
    IndexSpec(
      MongoCollections.SearchSessions,
      Indexes.compoundIndex(
        Indexes.ascending(MongoFields.ActorId),
        Indexes.descending(MongoFields.OccurredAt, MongoFields.Id)
      ),
      new IndexOptions().name(SearchSessionsActorIndex)
    ),
    IndexSpec(
      MongoCollections.SearchSessions,
      Indexes.ascending(MongoFields.ExpiresAt),
      new IndexOptions().name(SearchSessionsExpiryIndex).expireAfter(0L, TimeUnit.SECONDS)
    ),
    IndexSpec(
      MongoCollections.SearchSessionWork,
      Indexes.ascending(MongoFields.State, MongoFields.AvailableAt, MongoFields.LeaseUntil, MongoFields.CreatedAt),
      new IndexOptions().name(SearchSessionWorkClaimIndex)
    ),
    IndexSpec(
      MongoCollections.SearchSessionWork,
      Indexes.ascending(MongoFields.RetentionExpiresAt),
      new IndexOptions()
        .name(SearchSessionWorkRetentionIndex)
        .expireAfter(0L, TimeUnit.SECONDS)
        .partialFilterExpression(Filters.eq(MongoFields.State, "Failed"))
    ),
    IndexSpec(
      MongoCollections.ConsumerReceipts,
      Indexes.ascending(MongoFields.ConsumerGroup, MongoFields.EventId),
      new IndexOptions().name(ConsumerReceiptsIdIndex).unique(true)
    ),
    IndexSpec(
      MongoCollections.ConsumerReceipts,
      Indexes.ascending(MongoFields.ExpiresAt),
      new IndexOptions().name(ConsumerReceiptsExpiryIndex).expireAfter(0L, TimeUnit.SECONDS)
    ),
    IndexSpec(
      MongoCollections.MutationReceipts,
      Indexes.ascending(MongoFields.Operation, MongoFields.ActorScope, MongoFields.IdempotencyKey),
      new IndexOptions().name(MutationReceiptsKeyIndex).unique(true)
    ),
    IndexSpec(
      MongoCollections.MutationReceipts,
      Indexes.ascending(MongoFields.ExpiresAt),
      new IndexOptions().name(MutationReceiptsExpiryIndex).expireAfter(0L, TimeUnit.SECONDS)
    ),
    IndexSpec(
      MongoCollections.EventQuarantine,
      Indexes.ascending(MongoFields.Topic, "partition", "offset"),
      new IndexOptions().name(EventQuarantineOffsetIndex).unique(true)
    ),
    IndexSpec(
      MongoCollections.EventQuarantine,
      Indexes.ascending(MongoFields.ExpiresAt),
      new IndexOptions().name(EventQuarantineExpiryIndex).expireAfter(0L, TimeUnit.SECONDS)
    ),
    IndexSpec(
      MongoCollections.AnalyticsErasureRequests,
      Indexes.ascending(MongoFields.State, MongoFields.RequestedAt),
      new IndexOptions().name(AnalyticsErasureRequestStateIndex)
    ),
    IndexSpec(
      MongoCollections.AnalyticsErasureRequests,
      Indexes.ascending(MongoFields.ReceiptId),
      new IndexOptions()
        .name("analytics_erasure_request_receipt_unique")
        .unique(true)
        .partialFilterExpression(Filters.exists(MongoFields.ReceiptId, true))
    ),
    IndexSpec(
      MongoCollections.AnalyticsErasureCompletions,
      Indexes.ascending(MongoFields.ReceiptId),
      new IndexOptions()
        .name("analytics_erasure_completion_receipt_unique")
        .unique(true)
        .partialFilterExpression(Filters.exists(MongoFields.ReceiptId, true))
    ),
    IndexSpec(
      MongoCollections.AnalyticsErasureRequests,
      Indexes.ascending(MongoFields.ExpiresAt),
      new IndexOptions().name("analytics_erasure_request_expiry").expireAfter(0L, TimeUnit.SECONDS)
    ),
    IndexSpec(
      MongoCollections.AnalyticsWorkerHeartbeats,
      Indexes.ascending(MongoFields.LeaseUntil),
      new IndexOptions().name("analytics_worker_heartbeat_expiry").expireAfter(0L, TimeUnit.SECONDS)
    ),
    IndexSpec(
      MongoCollections.AnalyticsReportSnapshots,
      Indexes.compoundIndex(Indexes.ascending(MongoFields.State), Indexes.descending(MongoFields.AsOf)),
      new IndexOptions().name(AnalyticsReportPublishedIndex)
    ),
    IndexSpec(
      MongoCollections.AnalyticsReportSnapshots,
      Indexes.ascending(MongoFields.ExpiresAt),
      new IndexOptions().name(AnalyticsReportExpiryIndex).expireAfter(0L, TimeUnit.SECONDS)
    ),
    IndexSpec(
      MongoCollections.AnalyticsReportRuns,
      Indexes.ascending(MongoFields.ExpiresAt),
      new IndexOptions().name(MongoHiringSetup.AnalyticsReportRunExpiryIndex).expireAfter(0L, TimeUnit.SECONDS)
    ),
    IndexSpec(
      MongoCollections.InterviewWorkflows,
      Indexes.ascending("candidateId", MongoFields.Id),
      new IndexOptions().name(MongoHiringSetup.InterviewWorkflowCandidateIndex)
    ),
    IndexSpec(
      MongoCollections.InterviewWorkflows,
      Indexes.ascending("recruiterId", MongoFields.Id),
      new IndexOptions().name(MongoHiringSetup.InterviewWorkflowRecruiterIndex)
    ),
    IndexSpec(
      MongoCollections.InterviewWorkflowCommands,
      Indexes.ascending("commandState", MongoFields.AvailableAt, MongoFields.Id),
      new IndexOptions().name(MongoHiringSetup.InterviewWorkflowCommandDueIndex)
    ),
    IndexSpec(
      MongoCollections.InterviewWorkflowCommands,
      Indexes.ascending("commandState", "claimUntil", MongoFields.Id),
      new IndexOptions().name(MongoHiringSetup.InterviewWorkflowCommandLeaseIndex)
    ),
    IndexSpec(
      MongoCollections.InterviewWorkflowInbox,
      Indexes.ascending("workflowId", "messageId"),
      new IndexOptions()
        .name(MongoHiringSetup.InterviewWorkflowInboxIdentityIndex)
        .unique(true)
        .partialFilterExpression(Filters.eq("documentType", "inboxReceipt"))
    ),
    IndexSpec(
      MongoCollections.FakeInterviewCalendarReservations,
      Indexes.ascending("participants", "startsAt", "endsAt"),
      new IndexOptions()
        .name(MongoHiringSetup.FakeInterviewCalendarParticipantsIndex)
    ),
    IndexSpec(
      MongoCollections.FakeInterviewCalendarReservations,
      Indexes.ascending("releaseKey"),
      new IndexOptions().name(MongoHiringSetup.FakeInterviewCalendarReleaseIndex).unique(true)
    ),
    IndexSpec(
      MongoCollections.FakeInterviewNotificationReceipts,
      Indexes.ascending("recipientId", "deliveredAt"),
      new IndexOptions().name(MongoHiringSetup.FakeInterviewNotificationRecipientIndex)
    )
  )

  private val interviewRetentionSpecs: List[IndexSpec] = List(
    MongoCollections.InterviewWorkflows,
    MongoCollections.InterviewWorkflowCommands,
    MongoCollections.InterviewWorkflowInbox,
    MongoCollections.FakeInterviewCalendarReservations,
    MongoCollections.FakeInterviewNotificationReceipts
  ).map(name =>
    IndexSpec(
      name,
      Indexes.ascending(MongoFields.RetentionExpiresAt),
      new IndexOptions().name(s"${name}_completed_evidence_expiry").expireAfter(0L, TimeUnit.SECONDS)
    )
  )

  private val interviewSubjectSpecs: List[IndexSpec] = List(
    MongoCollections.InterviewWorkflowCommands,
    MongoCollections.InterviewWorkflowInbox,
    MongoCollections.FakeInterviewNotificationReceipts
  ).map(name => IndexSpec(name, Indexes.ascending("workflowId"), new IndexOptions().name(s"${name}_workflow_identity")))

  def create(database: MongoDatabase[IO]): IO[Unit] =
    (indexSpecs ++ interviewRetentionSpecs ++ interviewSubjectSpecs)
      .groupBy(_.collection)
      .toList
      .traverse { case (collectionName, specs) =>
        listIndexes(database, collectionName).map(indexes => (collectionName, specs, indexes))
      }
      .flatMap { snapshots =>
        snapshots.traverse_ { case (_, specs, existing) =>
          specs.traverse_ { spec =>
            existing
              .find(_.getString("name") == spec.options.getName)
              .traverse_(index =>
                definitionMismatch(spec, index).traverse_(reason => IO.raiseError(indexMismatch(spec, reason)))
              )
          }
        } *> snapshots.traverse_ { case (_, specs, existing) =>
          specs.filterNot(spec => existing.exists(_.getString("name") == spec.options.getName)).traverse_ { spec =>
            database
              .getCollection[Document](spec.collection, CodecRegistry.Default)
              .flatMap(_.createIndex(spec.keys, spec.options))
              .void
          }
        } *> snapshots.traverse_ { case (collectionName, specs, _) =>
          listIndexes(database, collectionName).flatMap { created =>
            specs.traverse_ { spec =>
              created
                .find(_.getString("name") == spec.options.getName)
                .fold(IO.raiseError[Unit](indexMismatch(spec, "missing after setup"))) { index =>
                  definitionMismatch(spec, index).traverse_(reason => IO.raiseError(indexMismatch(spec, reason)))
                }
            }
          }
        }
      }

  private def listIndexes(database: MongoDatabase[IO], collectionName: String): IO[List[Document]] =
    IO.delay(database.underlying.getCollection(collectionName, classOf[Document]).listIndexes())
      .flatMap(_.toStreamBuffered[IO](bufferSize = publisherBufferSize).compile.toList)
      .recoverWith { case error: MongoCommandException if error.getErrorCode == 26 => IO.pure(Nil) }

  private def indexMismatch(spec: IndexSpec, reason: String): IllegalStateException =
    new IllegalStateException(
      s"Mongo index definition mismatch for collection '${spec.collection}' index '${spec.options.getName}': $reason"
    )

  /** Returns only the mismatched definition field, never the potentially sensitive index predicate. */
  private[mongo] def definitionMismatch(spec: IndexSpec, actual: Document): Option[String] = {
    val expectedKeys = asBsonDocument(spec.keys)
    val actualKeys = asBsonDocument(actual.get("key", classOf[Document]))
    val expectedPartial = Option(spec.options.getPartialFilterExpression).map(asBsonDocument)
    val actualPartial = Option(actual.get("partialFilterExpression", classOf[Document])).map(asBsonDocument)
    val expectedExpireAfter = Option(spec.options.getExpireAfter(TimeUnit.SECONDS)).map(_.longValue())
    val actualExpireAfter = Option(actual.get("expireAfterSeconds")).map(_.asInstanceOf[Number].longValue())

    Option
      .when(orderedEntries(expectedKeys) != orderedEntries(actualKeys))("ordered keys")
      .orElse(Option.when(spec.options.isUnique != actual.getBoolean("unique", false))("unique"))
      .orElse(Option.when(spec.options.isSparse != actual.getBoolean("sparse", false))("sparse"))
      .orElse(Option.when(expectedPartial != actualPartial)("partial filter"))
      .orElse(Option.when(expectedExpireAfter != actualExpireAfter)("TTL seconds"))
  }

  private def asBsonDocument(value: Bson): BsonDocument =
    value.toBsonDocument(classOf[Document], bsonRegistry)

  private def orderedEntries(document: BsonDocument): List[(String, org.bson.BsonValue)] =
    document.entrySet().asScala.toList.map(entry => entry.getKey -> entry.getValue)
}
