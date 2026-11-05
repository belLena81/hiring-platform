package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.mongodb.{MongoClientSettings, MongoCommandException}
import com.mongodb.client.model.{Filters, IndexOptions, Indexes}
import mongo4cats.database.MongoDatabase
import org.bson.{BsonDocument, Document}
import org.bson.conversions.Bson
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.*

private[mongo] final case class IndexSpec(collection: String, keys: Bson, options: IndexOptions)

/** Every ordinary Mongo index, declared once. Migration steps that introduce an index reuse these specifications. */
private[mongo] object MongoHiringIndexSetup {
  import MongoIndexNames.*

  private val bsonRegistry = MongoClientSettings.getDefaultCodecRegistry

  private def receiptUniqueIndex(collection: String, name: String): IndexSpec = IndexSpec(
    collection,
    Indexes.ascending(MongoFields.ReceiptId),
    new IndexOptions()
      .name(name)
      .unique(true)
      .partialFilterExpression(Filters.exists(MongoFields.ReceiptId, true))
  )

  /** Introduced by `005_analytics_deletion_receipts`; verified with every other index on each startup. */
  val analyticsErasureReceiptIndexes: List[IndexSpec] = List(
    receiptUniqueIndex(MongoCollections.AnalyticsErasureRequests, AnalyticsErasureRequestReceipt),
    receiptUniqueIndex(MongoCollections.AnalyticsErasureCompletions, AnalyticsErasureCompletionReceipt)
  )

  /** One reservation per workflow and reserve key; introduced by `018_interview_cancellation_reschedule`. */
  val interviewReservationIdentityIndex: IndexSpec = IndexSpec(
    MongoCollections.InterviewCalendarReservations,
    Indexes.ascending(MongoFields.WorkflowId, "reserveKey"),
    new IndexOptions().name(InterviewCalendarReservationIdentity).unique(true)
  )

  /** Due work, including proposal expiry (`availableAt` of an `expireProposal` intent), is claimed through this index.
    */
  val interviewCommandDueIndex: IndexSpec = IndexSpec(
    MongoCollections.InterviewWorkflowCommands,
    Indexes.ascending("commandState", MongoFields.AvailableAt, MongoFields.Id),
    new IndexOptions().name(InterviewWorkflowCommandDue)
  )

  /** Open jobs of one canonical city in page order (list and discovery filters). Replaces `jobs_open_city_created_id`,
    * which keyed the display city; created and the old index dropped by `027_job_canonical_filters`.
    */
  val jobOpenCityIndex: IndexSpec = IndexSpec(
    MongoCollections.Jobs,
    Indexes.compoundIndex(
      Indexes.ascending(MongoFields.Status, MongoFields.LocationCityCanonical),
      Indexes.descending(MongoFields.CreatedAt, MongoFields.Id)
    ),
    new IndexOptions().name(JobsOpenCityCreated)
  )

  /** Embedding-model, status, canonical city and recruiter predicates of the search recheck. Replaces
    * `jobs_embedding_meta_filters`; created and the old index dropped by `027_job_canonical_filters`.
    */
  val jobEmbeddingMetaIndex: IndexSpec = IndexSpec(
    MongoCollections.Jobs,
    Indexes.ascending(
      MongoFields.EmbeddingMetaModel,
      MongoFields.Status,
      MongoFields.LocationCityCanonical,
      MongoFields.RecruiterId
    ),
    new IndexOptions().name(JobsEmbeddingMeta)
  )

  /** Earliest pending event of an aggregate for the outbox claim's per-aggregate order check; introduced by
    * `035_event_outbox_aggregate_order`. Partial on the two non-terminal states, so `Published` and `Failed` rows are
    * not indexed, and ordered `(partitionKey, occurredAt, _id)`, so the earliest event per key is a `DISTINCT_SCAN`.
    */
  val eventOutboxAggregateOrderIndex: IndexSpec = IndexSpec(
    MongoCollections.EventOutbox,
    Indexes.ascending(MongoFields.PartitionKey, MongoFields.OccurredAt, MongoFields.Id),
    new IndexOptions()
      .name(EventOutboxAggregateOrder)
      .partialFilterExpression(Filters.in(MongoFields.State, MongoOutboxClaimBatch.PendingStates.asJava))
  )

  /** Retirement reads the Active generations of one transactional id registered before a cut, in `(registeredAt, _id)`
    * order: equality prefix, range and sort are all served by the key.
    */
  val producerRegistrationGenerationStateIndex: IndexSpec = IndexSpec(
    MongoProducerRegistrations.Collection,
    Indexes.ascending(
      MongoFields.TransactionalId,
      MongoFields.State,
      MongoProducerRegistrations.RegisteredAt,
      MongoFields.Id
    ),
    new IndexOptions().name(ProducerRegistrationGenerationState)
  )

  val jobCanonicalFilterIndexes: List[IndexSpec] = List(jobOpenCityIndex, jobEmbeddingMetaIndex)
  private val indexSpecs: List[IndexSpec] = List(
    IndexSpec(
      MongoCollections.InterviewSubjectCleanup,
      Indexes.ascending(MongoFields.Id, MongoFields.RequestedAt),
      new IndexOptions()
        .name(MongoInterviewCleanupSweepCodec.ActiveIndex)
        .partialFilterExpression(MongoInterviewCleanupSweepCodec.activeFilter)
    ),
    IndexSpec(
      MongoProducerRegistrations.Collection,
      Indexes.ascending("subjectId", MongoFields.Kind, MongoFields.State, MongoFields.Id),
      new IndexOptions().name(ProducerRegistrationSubjectState)
    ),
    producerRegistrationGenerationStateIndex,
    IndexSpec(
      MongoProducerRegistrations.Collection,
      Indexes.ascending(MongoFields.ExpiresAt),
      new IndexOptions().name(ProducerRegistrationFencedExpiry).expireAfter(0L, TimeUnit.SECONDS)
    ),
    IndexSpec(
      MongoCollections.InterviewSubjectCleanup,
      Indexes.ascending(MongoFields.State, MongoFields.RequestedAt),
      new IndexOptions().name(InterviewSubjectCleanupDue)
    ),
    IndexSpec(
      MongoCollections.Users,
      Indexes.ascending(MongoFields.EmailCanonical),
      new IndexOptions().name(UsersEmail).unique(true).sparse(true)
    ),
    IndexSpec(
      MongoCollections.Users,
      Indexes.ascending(MongoFields.NameCanonical),
      new IndexOptions().name(UsersName).unique(true)
    ),
    IndexSpec(
      MongoCollections.Users,
      Indexes.compoundIndex(
        Indexes.ascending(MongoFields.AccountStatus),
        Indexes.descending(MongoFields.CreatedAt, MongoFields.Id)
      ),
      new IndexOptions().name(UsersStatusCreated)
    ),
    IndexSpec(
      MongoCollections.Users,
      Indexes.compoundIndex(
        Indexes.ascending(MongoFields.Role, MongoFields.AccountStatus),
        Indexes.descending(MongoFields.CreatedAt, MongoFields.Id)
      ),
      new IndexOptions().name(UsersRoleStatusCreated)
    ),
    IndexSpec(
      MongoCollections.Users,
      Indexes.ascending(MongoFields.AdminSingletonKey),
      new IndexOptions()
        .name(UsersAdminSingleton)
        .unique(true)
        .partialFilterExpression(Filters.eq(MongoFields.Role, "Admin"))
    ),
    IndexSpec(
      MongoCollections.Users,
      Indexes.ascending(MongoFields.EmbeddingMetaModel, MongoFields.Role),
      new IndexOptions().name(UsersEmbeddingMeta)
    ),
    IndexSpec(
      MongoCollections.Jobs,
      Indexes.compoundIndex(
        Indexes.ascending(MongoFields.RecruiterId, MongoFields.Status),
        Indexes.descending(MongoFields.CreatedAt, MongoFields.Id)
      ),
      new IndexOptions().name(JobsRecruiterStatusCreated)
    ),
    IndexSpec(
      MongoCollections.Jobs,
      Indexes.compoundIndex(
        Indexes.ascending(MongoFields.RecruiterId),
        Indexes.descending(MongoFields.CreatedAt, MongoFields.Id)
      ),
      new IndexOptions().name(JobsRecruiterCreated)
    ),
    IndexSpec(
      MongoCollections.Jobs,
      Indexes.descending(MongoFields.CreatedAt, MongoFields.Id),
      new IndexOptions().name(JobsCreated)
    ),
    IndexSpec(
      MongoCollections.Jobs,
      Indexes.compoundIndex(
        Indexes.ascending(MongoFields.Status),
        Indexes.descending(MongoFields.CreatedAt, MongoFields.Id)
      ),
      new IndexOptions().name(JobsOpenCreated)
    ),
    jobOpenCityIndex,
    IndexSpec(
      MongoCollections.Jobs,
      Indexes.geo2dsphere(MongoFields.LocationPoint),
      new IndexOptions().name(JobsLocationPoint)
    ),
    jobEmbeddingMetaIndex,
    IndexSpec(
      MongoCollections.Applications,
      Indexes.ascending(MongoFields.CandidateId, MongoFields.JobId),
      new IndexOptions().name(ApplicationsCandidateJob).unique(true)
    ),
    IndexSpec(
      MongoCollections.Applications,
      Indexes.compoundIndex(
        Indexes.ascending(MongoFields.CandidateId, MongoFields.Status),
        Indexes.descending(MongoFields.CreatedAt, MongoFields.Id)
      ),
      new IndexOptions().name(ApplicationsCandidateStatusCreated)
    ),
    IndexSpec(
      MongoCollections.Applications,
      Indexes.compoundIndex(
        Indexes.ascending(MongoFields.CandidateId),
        Indexes.descending(MongoFields.CreatedAt, MongoFields.Id)
      ),
      new IndexOptions().name(ApplicationsCandidateCreated)
    ),
    IndexSpec(
      MongoCollections.Applications,
      Indexes.compoundIndex(
        Indexes.ascending(MongoFields.JobId, MongoFields.Status),
        Indexes.descending(MongoFields.CreatedAt, MongoFields.Id)
      ),
      new IndexOptions().name(ApplicationsJobStatusCreated)
    ),
    IndexSpec(
      MongoCollections.Applications,
      Indexes
        .compoundIndex(Indexes.ascending(MongoFields.JobId), Indexes.descending(MongoFields.CreatedAt, MongoFields.Id)),
      new IndexOptions().name(ApplicationsJobCreated)
    ),
    IndexSpec(
      MongoCollections.ApplicationEvents,
      Indexes.compoundIndex(
        Indexes.ascending(MongoFields.ApplicationId),
        Indexes.descending(MongoFields.OccurredAt, MongoFields.Id)
      ),
      new IndexOptions().name(ApplicationEventsApplicationCreated)
    ),
    IndexSpec(
      MongoCollections.EmbeddingWork,
      Indexes.ascending(MongoFields.State, MongoFields.AvailableAt, MongoFields.LeaseUntil),
      new IndexOptions().name(EmbeddingWorkAvailable)
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
      new IndexOptions().name(EventOutboxClaim)
    ),
    eventOutboxAggregateOrderIndex,
    IndexSpec(
      MongoCollections.EventOutbox,
      Indexes.ascending(MongoFields.RetentionExpiresAt),
      new IndexOptions()
        .name(EventOutboxPublishedRetention)
        .expireAfter(0L, TimeUnit.SECONDS)
        .partialFilterExpression(Filters.eq(MongoFields.State, "Published"))
    ),
    IndexSpec(
      MongoCollections.EventOutbox,
      Indexes.ascending(MongoFields.SubjectIds),
      new IndexOptions().name(EventOutboxSubjectIds)
    ),
    IndexSpec(
      MongoCollections.OutboxSubjectFences,
      Indexes.ascending(MongoFields.Deleted, MongoFields.LeaseUntil),
      new IndexOptions().name(OutboxSubjectFenceLease)
    ),
    IndexSpec(
      MongoCollections.SearchSessions,
      Indexes.compoundIndex(
        Indexes.ascending(MongoFields.ActorId),
        Indexes.descending(MongoFields.OccurredAt, MongoFields.Id)
      ),
      new IndexOptions().name(SearchSessionsActor)
    ),
    IndexSpec(
      MongoCollections.SearchSessions,
      Indexes.ascending(MongoFields.ExpiresAt),
      new IndexOptions().name(SearchSessionsExpiry).expireAfter(0L, TimeUnit.SECONDS)
    ),
    IndexSpec(
      MongoCollections.SearchSessionWork,
      Indexes.ascending(MongoFields.State, MongoFields.AvailableAt, MongoFields.LeaseUntil, MongoFields.CreatedAt),
      new IndexOptions().name(SearchSessionWorkClaim)
    ),
    IndexSpec(
      MongoCollections.SearchSessionWork,
      Indexes.ascending(MongoFields.RetentionExpiresAt),
      new IndexOptions()
        .name(SearchSessionWorkRetention)
        .expireAfter(0L, TimeUnit.SECONDS)
        .partialFilterExpression(Filters.eq(MongoFields.State, "Failed"))
    ),
    IndexSpec(
      MongoCollections.ConsumerReceipts,
      Indexes.ascending(MongoFields.ExpiresAt),
      new IndexOptions().name(ConsumerReceiptsExpiry).expireAfter(0L, TimeUnit.SECONDS)
    ),
    IndexSpec(
      MongoCollections.MutationReceipts,
      Indexes.ascending(MongoFields.Operation, MongoFields.ActorScope, MongoFields.IdempotencyKey),
      new IndexOptions().name(MutationReceiptsKey).unique(true)
    ),
    IndexSpec(
      MongoCollections.MutationReceipts,
      Indexes.ascending(MongoFields.ExpiresAt),
      new IndexOptions().name(MutationReceiptsExpiry).expireAfter(0L, TimeUnit.SECONDS)
    ),
    IndexSpec(
      MongoCollections.EventQuarantine,
      Indexes.ascending(MongoFields.Topic, MongoFields.Partition, MongoFields.Offset),
      new IndexOptions().name(EventQuarantineOffset).unique(true)
    ),
    IndexSpec(
      MongoCollections.EventQuarantine,
      Indexes.ascending(MongoFields.ExpiresAt),
      new IndexOptions().name(EventQuarantineExpiry).expireAfter(0L, TimeUnit.SECONDS)
    ),
    IndexSpec(
      MongoCollections.AnalyticsErasureRequests,
      Indexes.ascending(MongoFields.State, MongoFields.RequestedAt),
      new IndexOptions().name(AnalyticsErasureRequestState)
    )
  ) ++ analyticsErasureReceiptIndexes ++ List(
    IndexSpec(
      MongoCollections.AnalyticsErasureRequests,
      Indexes.ascending(MongoFields.ExpiresAt),
      new IndexOptions().name(AnalyticsErasureRequestExpiry).expireAfter(0L, TimeUnit.SECONDS)
    ),
    IndexSpec(
      MongoCollections.AnalyticsWorkerHeartbeats,
      Indexes.ascending(MongoFields.LeaseUntil),
      new IndexOptions().name(AnalyticsWorkerHeartbeatExpiry).expireAfter(0L, TimeUnit.SECONDS)
    ),
    IndexSpec(
      MongoCollections.AnalyticsReportSnapshots,
      Indexes.compoundIndex(Indexes.ascending(MongoFields.State), Indexes.descending(MongoFields.AsOf)),
      new IndexOptions().name(AnalyticsReportPublished)
    ),
    IndexSpec(
      MongoCollections.AnalyticsReportSnapshots,
      Indexes.ascending(MongoFields.ExpiresAt),
      new IndexOptions().name(AnalyticsReportExpiry).expireAfter(0L, TimeUnit.SECONDS)
    ),
    IndexSpec(
      MongoCollections.AnalyticsReportRuns,
      Indexes.ascending(MongoFields.ExpiresAt),
      new IndexOptions().name(AnalyticsReportRunExpiry).expireAfter(0L, TimeUnit.SECONDS)
    ),
    IndexSpec(
      MongoCollections.InterviewWorkflows,
      Indexes.ascending(MongoFields.CandidateId, MongoFields.Id),
      new IndexOptions().name(InterviewWorkflowCandidate)
    ),
    IndexSpec(
      MongoCollections.InterviewWorkflows,
      Indexes.ascending(MongoFields.RecruiterId, MongoFields.Id),
      new IndexOptions().name(InterviewWorkflowRecruiter)
    ),
    // One workflow per application, enforced by the store: an application enters `Interview` once and never leaves it
    // for another scheduling. The partial filter keeps the index to workflow documents.
    // `findByApplication` reads through this index.
    IndexSpec(
      MongoCollections.InterviewWorkflows,
      Indexes.ascending("applicationId"),
      new IndexOptions()
        .name(InterviewWorkflowApplication)
        .unique(true)
        .partialFilterExpression(Filters.eq(MongoFields.DocumentType, "workflow"))
    ),
    interviewCommandDueIndex,
    IndexSpec(
      MongoCollections.InterviewWorkflowCommands,
      Indexes.ascending("commandState", "claimUntil", MongoFields.Id),
      new IndexOptions().name(InterviewWorkflowCommandLease)
    ),
    IndexSpec(
      MongoCollections.InterviewWorkflowInbox,
      Indexes.ascending(MongoFields.WorkflowId, MongoFields.MessageId),
      new IndexOptions()
        .name(InterviewWorkflowInboxIdentity)
        .unique(true)
        .partialFilterExpression(Filters.eq(MongoFields.DocumentType, MongoInterviewFields.InboxReceiptType))
    ),
    IndexSpec(
      MongoCollections.InterviewCalendarReservations,
      Indexes.ascending("participants", "startsAt", "endsAt"),
      new IndexOptions()
        .name(InterviewCalendarParticipants)
    ),
    IndexSpec(
      MongoCollections.InterviewCalendarReservations,
      Indexes.ascending("releaseKey"),
      new IndexOptions().name(InterviewCalendarRelease).unique(true)
    ),
    interviewReservationIdentityIndex,
    IndexSpec(
      MongoCollections.InterviewNotificationReceipts,
      Indexes.ascending("recipientId", "deliveredAt"),
      new IndexOptions().name(InterviewNotificationRecipient)
    )
  )

  private val interviewRetentionSpecs: List[IndexSpec] = List(
    MongoCollections.InterviewWorkflows,
    MongoCollections.InterviewWorkflowCommands,
    MongoCollections.InterviewWorkflowInbox,
    MongoCollections.InterviewCalendarReservations,
    MongoCollections.InterviewNotificationReceipts
  ).map(name =>
    IndexSpec(
      name,
      Indexes.ascending(MongoFields.RetentionExpiresAt),
      new IndexOptions().name(completedEvidenceExpiry(name)).expireAfter(0L, TimeUnit.SECONDS)
    )
  )

  private val interviewSubjectSpecs: List[IndexSpec] = List(
    MongoCollections.InterviewWorkflowCommands,
    MongoCollections.InterviewWorkflowInbox,
    MongoCollections.InterviewNotificationReceipts
  ).map(name =>
    IndexSpec(name, Indexes.ascending(MongoFields.WorkflowId), new IndexOptions().name(workflowIdentity(name)))
  )

  val allSpecs: List[IndexSpec] = indexSpecs ++ interviewRetentionSpecs ++ interviewSubjectSpecs

  def create(database: MongoDatabase[IO]): IO[Unit] = ensure(database, allSpecs)

  /** Verifies existing definitions first, creates only the missing indexes, then re-verifies every definition. */
  def ensure(database: MongoDatabase[IO], specs: List[IndexSpec]): IO[Unit] =
    specs
      .groupBy(_.collection)
      .toList
      .traverse { case (collectionName, collectionSpecs) =>
        listIndexes(database, collectionName).map(indexes => (collectionName, collectionSpecs, indexes))
      }
      .flatMap { snapshots =>
        snapshots.traverse_ { case (_, collectionSpecs, existing) =>
          collectionSpecs.traverse_ { spec =>
            existing.find(_.getString("name") == spec.options.getName).traverse_(verifyDefinition(spec, _))
          }
        } *> snapshots.traverse_ { case (collectionName, collectionSpecs, existing) =>
          collectionSpecs.filterNot(spec => existing.exists(_.getString("name") == spec.options.getName)).traverse_ {
            spec =>
              Mongo4catsCollections
                .documents(database, collectionName)
                .flatMap(_.createIndex(spec.keys, spec.options))
                .void
          }
        } *> snapshots.traverse_ { case (collectionName, collectionSpecs, _) =>
          listIndexes(database, collectionName).flatMap { created =>
            collectionSpecs.traverse_ { spec =>
              created
                .find(_.getString("name") == spec.options.getName)
                .fold(IO.raiseError[Unit](indexMismatch(spec, "missing after setup")))(verifyDefinition(spec, _))
            }
          }
        }
      }

  private def verifyDefinition(spec: IndexSpec, index: Document): IO[Unit] =
    definitionMismatch(spec, index).traverse_(reason => IO.raiseError(indexMismatch(spec, reason)))

  /** A collection that does not exist yet (code 26) simply has no indexes. */
  private def listIndexes(database: MongoDatabase[IO], collectionName: String): IO[List[Document]] =
    Mongo4catsCollections
      .documents(database, collectionName)
      .flatMap(_.listIndexes[Document])
      .map(_.toList)
      .recoverWith { case error: MongoCommandException if error.getErrorCode == 26 => IO.pure(Nil) }

  private def indexMismatch(spec: IndexSpec, reason: String): MigrationError =
    MigrationError.IndexMismatch(spec.collection, spec.options.getName, reason)

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
