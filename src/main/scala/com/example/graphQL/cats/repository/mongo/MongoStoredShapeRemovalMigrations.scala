package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.mongodb.MongoCommandException
import com.mongodb.client.model.{Filters, Indexes, Updates}
import org.bson.Document
import org.bson.conversions.Bson
import scala.jdk.CollectionConverters.*

/** The ledger steps that retire a stored shape: `027_interview_hiring_receipt_removal`,
  * `028_interview_request_receipt_removal`, `029_search_session_work_single_copy`,
  * `030_embedding_work_key_from_identity`, `031_application_event_actor_role` and
  * `032_interview_workflow_initiator_role`. Each one follows the same contract, parameterised by what it removes:
  *
  *   - Expand: the writers already stopped producing the shape (it is derived from another fact, see the owning
  *     capability), so this step only removes what earlier binaries left behind.
  *   - Backfill: a bounded scan in `_id` order removes the stored shape (whole documents of one `documentType`, or the
  *     redundant fields of a work row). Each page is one conditional write guarded by the shape itself, so a replay, a
  *     concurrent start or a writer racing the scan converges. The ledger checkpoint advances after every page; a
  *     restart resumes there. A row that does not look like the shape fails the step closed, naming the collection
  *     only.
  *   - Contract: obsolete indexes are dropped (absent is fine), every remaining row is audited against the successor
  *     validator, and only then is the successor installed. The scan repeats once from the start (without moving the
  *     checkpoint) to repair a row an earlier binary wrote below the checkpoint; if any remain, the step fails.
  *
  * The completed proof is read-only: the successor validator must be installed exactly (anything else fails closed,
  * never overwritten) and no row of the retired shape may exist, otherwise the step reopens. Recovery is forward-only:
  * stop writers running an earlier binary before upgrading (it still writes the retired shape, which the reopened proof
  * repairs on the next start). Application rollback does not restore the removed data.
  */
private[mongo] object MongoStoredShapeRemovalMigrations {
  private val BatchSize = MongoBatchScan.BatchSize

  /** What a step retires and what replaces the validator afterwards. */
  private final case class Removal(
      id: MigrationId,
      collection: String,
      /** Selects every row of the retired shape. */
      selector: Bson,
      /** Whether a selected row is plausibly of that shape (guards against deleting or rewriting something else). */
      recognised: Document => Boolean,
      /** Removes the shape from the selected rows of one page; the write is conditional on the selector. */
      remove: (MongoHiringSetup.SetupCollection, List[String]) => IO[Unit],
      droppedIndexes: List[String],
      successor: Document,
      predecessors: List[MigrationId],
      /** A field removal installs the successor first: the removed fields are required by the earlier validator, so the
        * unset itself would be refused until it is replaced. A document removal installs it last: its retired rows
        * would not satisfy the successor.
        */
      installFirst: Boolean = false,
      /** Later generations of the validator that the completed proof also accepts. */
      laterGenerations: List[Document] = Nil
  )

  private def page(run: MigrationRun, removal: Removal, after: Option[String]): IO[List[Document]] =
    MongoBoundedResults.collect(
      run.database
        .getCollection(removal.collection)
        .find(after.fold(removal.selector)(id => Filters.and(removal.selector, Filters.gt(MongoFields.Id, id))))
        .sort(Indexes.ascending(MongoFields.Id))
        .limit(BatchSize)
        .boundedStream(32),
      BatchSize,
      MigrationError.StepFailed(removal.id, s"setup read exceeded its maximum of $BatchSize elements")
    )

  private def handle(run: MigrationRun, removal: Removal)(rows: List[Document]): IO[Unit] =
    // An empty page has nothing to remove and issues no write.
    if (rows.isEmpty) IO.unit
    else if (rows.forall(removal.recognised))
      removal.remove(run.database.getCollection(removal.collection), rows.map(_.getString(MongoFields.Id)))
    else run.fail(s"found an unrecognised row in ${removal.collection}")

  private def remaining(database: MongoHiringSetup.SetupDatabase, removal: Removal): IO[Boolean] =
    database.getCollection(removal.collection).find(removal.selector).limit(1).first.map(_.nonEmpty)

  private def dropIndex(database: MongoHiringSetup.SetupDatabase, collection: String, index: String): IO[Unit] =
    database
      .runCommand(new Document("dropIndexes", collection).append("index", index))
      .void
      // 26: the collection does not exist yet; 27: the index was never created or is already gone.
      .recoverWith { case error: MongoCommandException if Set(26, 27)(error.getErrorCode) => IO.unit }

  private def audit(run: MigrationRun, removal: Removal): IO[Unit] =
    run.database
      .getCollection(removal.collection)
      .find(new Document("$nor", List(removal.successor).asJava))
      .limit(1)
      .first
      .flatMap(invalid => IO.whenA(invalid.nonEmpty)(run.fail(s"found invalid stored data in ${removal.collection}")))

  private def verifyCompleted(removal: Removal)(database: MongoHiringSetup.SetupDatabase): IO[CompletedProof] =
    MongoHiringValidators
      .collectionOptions(database, List(removal.collection))
      .flatMap { options =>
        val stored = options.get(removal.collection)
        IO.raiseUnless(
          (removal.successor :: removal.laterGenerations)
            .exists(MongoHiringValidators.strictValidatorMatches(stored, _))
        )(
          MigrationError.ValidatorMismatch(removal.collection)
        )
      }
      .productR(remaining(database, removal))
      .map(present => if (present) CompletedProof.Reopen else CompletedProof.Trusted)

  private def laterGenerationInPlace(database: MongoHiringSetup.SetupDatabase, removal: Removal): IO[Boolean] =
    MongoHiringValidators.collectionOptions(database, List(removal.collection)).map { options =>
      removal.laterGenerations.exists(MongoHiringValidators.strictValidatorMatches(options.get(removal.collection), _))
    }

  private def step(removal: Removal): MongoMigrationStep = {
    def scan(run: MigrationRun): IO[Unit] =
      run.scanByStringId(BatchSize, "invalid key")(page(run, removal, _))(handle(run, removal))
    def rescan(run: MigrationRun): IO[Unit] =
      MongoBatchScan.byId[String](BatchSize, None)(page(run, removal, _))(row =>
        Option(row.getString(MongoFields.Id)).fold(run.fail[String]("invalid key"))(IO.pure)
      )(handle(run, removal))
    def cutover(run: MigrationRun): IO[Unit] =
      for {
        _ <- MongoMigrationLedger.requireComplete(run.database, removal.predecessors)
        _ <- IO.whenA(removal.installFirst)(
          audit(run, removal) *> MongoHiringValidators.install(run.database, removal.collection, removal.successor)
        )
        _ <- scan(run)
        _ <- remaining(run.database, removal).flatMap(IO.whenA(_)(rescan(run)))
        _ <- remaining(run.database, removal).flatMap(IO.whenA(_)(run.fail("rows of the retired shape remain")))
        _ <- removal.droppedIndexes.traverse_(dropIndex(run.database, removal.collection, _))
        // A later step's definition already in place is a successor of this one: it is audited by that step, not
        // replaced by this one.
        laterInPlace <- laterGenerationInPlace(run.database, removal)
        _ <- IO.unlessA(removal.installFirst || laterInPlace)(
          audit(run, removal) *> MongoHiringValidators.install(run.database, removal.collection, removal.successor)
        )
        _ <- audit(run, removal)
        proof <- verifyCompleted(removal)(run.database)
        _ <- IO.raiseUnless(proof == CompletedProof.Trusted)(
          MigrationError.StepFailed(run.id, "proof was not established")
        )
      } yield ()
    MongoMigrationStep(removal.id, cutover, verifyCompleted(removal))
  }

  private def deleteWhere(selector: Bson)(
      collection: MongoHiringSetup.SetupCollection,
      ids: List[String]
  ): IO[Unit] =
    collection.deleteMany(Filters.and(Filters.in(MongoFields.Id, ids*), selector)).void

  private def unsetFields(fields: List[String])(
      collection: MongoHiringSetup.SetupCollection,
      ids: List[String]
  ): IO[Unit] =
    collection
      .updateMany(Filters.in(MongoFields.Id, ids*), Updates.combine(fields.map(Updates.unset)*))
      .void

  private def ofType(documentType: String): Bson = Filters.eq(MongoFields.DocumentType, documentType)

  private def anyField(fields: List[String]): Bson = Filters.or(fields.map(Filters.exists(_, true))*)

  /** `027`: the hiring commit is evidenced by its deterministic `application_events` row, so the inbox receipt that
    * duplicated it (ids `<workflowId>:hiring`, subject-attributed by `022`) and its erasure index are removed.
    */
  val hiringReceipts: MongoMigrationStep = step(
    Removal(
      MigrationIds.InterviewHiringReceiptRemoval,
      MongoCollections.InterviewWorkflowInbox,
      ofType(MongoInterviewFields.RetiredHiringReceiptType),
      row => Option(row.getString(MongoFields.Id)).exists(_.endsWith(":hiring")),
      deleteWhere(ofType(MongoInterviewFields.RetiredHiringReceiptType)),
      List(MongoIndexNames.InterviewWorkflowInboxHiringSubjects),
      MongoOperationalValidators.interviewWorkflowInboxValidator,
      List(MigrationIds.InterviewSubjectAttribution, MigrationIds.OperationalCollectionValidators)
    )
  )

  /** `028`: interview request idempotency is a `mutation_receipts` row, so the request receipts stored beside the
    * workflows (ids `request:<actor>:<key>`) and the sparse index that served them are removed.
    */
  val requestReceipts: MongoMigrationStep = step(
    Removal(
      MigrationIds.InterviewRequestReceiptRemoval,
      MongoCollections.InterviewWorkflows,
      ofType(MongoInterviewFields.RetiredRequestReceiptType),
      row => Option(row.getString(MongoFields.Id)).exists(_.startsWith("request:")),
      deleteWhere(ofType(MongoInterviewFields.RetiredRequestReceiptType)),
      List(MongoIndexNames.InterviewWorkflowRequestReceipt),
      MongoOperationalValidators.interviewWorkflowsValidatorThrough028,
      List(MigrationIds.InterviewRequestReceiptIndex, MigrationIds.OperationalCollectionValidators),
      laterGenerations = List(MongoOperationalValidators.interviewWorkflowsValidator)
    )
  )

  /** `029`: the search session work row holds the session only; the stored copy of its `SearchPerformed` event is
    * removed because the event is derived from the session when the work completes.
    */
  val searchSessionWork: MongoMigrationStep = step(
    Removal(
      MigrationIds.SearchSessionWorkSingleCopy,
      MongoCollections.SearchSessionWork,
      Filters.exists(MongoFields.Event, true),
      _ => true,
      unsetFields(List(MongoFields.Event)),
      Nil,
      MongoOperationalValidators.searchSessionWorkValidator,
      List(MigrationIds.OperationalCollectionValidators),
      installFirst = true
    )
  )

  private val EmbeddingWorkKeyFields = List(MongoFields.Kind, MongoFields.WorkEntityId)

  /** `030`: an embedding work row's kind and entity are the two parts of its `_id`; the stored copies are removed. */
  val embeddingWork: MongoMigrationStep = step(
    Removal(
      MigrationIds.EmbeddingWorkKeyFromIdentity,
      MongoCollections.EmbeddingWork,
      anyField(EmbeddingWorkKeyFields),
      _ => true,
      unsetFields(EmbeddingWorkKeyFields),
      Nil,
      MongoOperationalValidators.embeddingWorkValidator,
      List(MigrationIds.OperationalCollectionValidators),
      installFirst = true
    )
  )

  /** `031`: an application event always records the role its actor acted in, so an event without `actorRole` is the
    * retired shape. The role cannot be reconstructed (it may have been an Admin acting on a job, or a user whose role
    * is not recorded elsewhere), so the step never rewrites such a row: it fails closed on the first one, naming the
    * collection only, and installs the successor validator, which requires the field, only when none exists. Pre-MVP
    * the remedy for a failed step is to reset the local database.
    */
  val applicationEventActorRole: MongoMigrationStep = step(
    Removal(
      MigrationIds.ApplicationEventActorRole,
      MongoCollections.ApplicationEvents,
      Filters.exists(MongoFields.ActorRole, false),
      _ => false,
      (_, _) => IO.unit,
      Nil,
      MongoOperationalValidators.applicationEventsValidator,
      List(MigrationIds.OperationalCollectionValidators)
    )
  )

  /** `032`: an interview workflow always records the role of its initiator, which the hiring status change copies into
    * the append-only history as its actor role. A workflow row without `initiatedByRole` is the retired shape; the role
    * cannot be inferred (an Admin can own a job), so the step fails closed on the first one like `031`.
    */
  val interviewWorkflowInitiatorRole: MongoMigrationStep = step(
    Removal(
      MigrationIds.InterviewWorkflowInitiatorRole,
      MongoCollections.InterviewWorkflows,
      Filters.and(ofType(MongoInterviewFields.WorkflowType), Filters.exists("initiatedByRole", false)),
      _ => false,
      (_, _) => IO.unit,
      Nil,
      MongoOperationalValidators.interviewWorkflowsValidator,
      List(MigrationIds.InterviewRequestReceiptRemoval, MigrationIds.OperationalCollectionValidators)
    )
  )
}
