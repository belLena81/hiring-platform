package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.service.resilience.CircuitBreaker

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import mongo4cats.client.{ClientSession, MongoClient}
import mongo4cats.database.MongoDatabase
import mongo4cats.operations.{Accumulator, Aggregate, Sort}
import com.example.graphQL.cats.service.events.{OperationalEventEnvelope, SearchSession}
import com.example.graphQL.cats.service.port.{
  ClaimedOperationalEvent,
  ConsumerReceiptRepository,
  EventQuarantineRecord,
  EventQuarantineRepository,
  MutationWriteContext,
  OperationalEventOutboxRepository,
  RepositoryF,
  SearchRecordingFailureRepository,
  SearchSessionRepository
}
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.Diagnostics
import com.mongodb.MongoCommandException
import scala.concurrent.duration.FiniteDuration
import com.mongodb.client.model.{Sorts, UpdateOptions, Projections}
import org.bson.{BsonArray, BsonDocument, BsonInt32, BsonString, BsonValue, Document}
import org.bson.conversions.Bson
import org.bson.types.Binary
import java.time.Instant
import com.example.graphQL.cats.repository.mongo.MongoDocumentFields.Repository.{instant, string}
import java.util.UUID
import scala.jdk.CollectionConverters.*

final class MongoSearchSessionRepository(database: MongoDatabase[IO], diagnostics: Diagnostics[IO])
    extends SearchSessionRepository[IO]
    with SearchRecordingFailureRepository[IO] {
  private val sessions = Mongo4catsCollections.documents(database, MongoCollections.SearchSessions)
  private val outbox = Mongo4catsCollections.documents(database, MongoCollections.EventOutbox)

  override def find(id: UUID): RepositoryF[IO, Option[SearchSession]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "searchSession.find")(
        RepositoryF
          .lift(
            MongoSessionOperations
              .findOne(sessions, None, MongoFilter.eq(MongoFields.Id, id.toString))
          )
          .subflatMap(document =>
            MongoStoredDocumentDecoding.repository(document.traverse(MongoHiringCodecs.readSearchSession))
          )
      )

  override def record(event: OperationalEventEnvelope): RepositoryF[IO, Boolean] =
    recordInteractionWithSession(event, None)

  override def recordInteraction(
      event: OperationalEventEnvelope,
      context: MutationWriteContext
  ): RepositoryF[IO, Boolean] =
    RepositoryF.lift(MongoMutationWriteContext.session(context)).flatMap(recordInteractionWithSession(event, _))

  private def recordInteractionWithSession(
      event: OperationalEventEnvelope,
      session: Option[ClientSession[IO]]
  ): RepositoryF[IO, Boolean] =
    val insert = MongoF
      .fromEither(MongoHiringCodecs.outboxRecord(event, event.occurredAt).leftMap(_ => RepositoryError.InvalidEvent))
      .flatMap(document => RepositoryF.lift(MongoSessionOperations.insertOne(outbox, session, document).as(true)))
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "searchSession.recordInteraction")(insert)(MongoErrors.duplicateAsConflict)
      .leftFlatMap {
        case RepositoryError.Conflict =>
          MongoRepositorySupport
            .repositoryGuard(diagnostics, "searchSession.recordInteraction.findDuplicate")(
              RepositoryF
                .lift(
                  MongoSessionOperations
                    .findOne(outbox, session, MongoFilter.eq(MongoFields.Id, event.eventId.toString))
                )
                .subflatMap {
                  case Some(existing) if sameEvent(existing, event) => Right(false)
                  case _                                            => Left(RepositoryError.Conflict)
                }
            )
        case error => MongoF.raise(error)
      }

  private def sameEvent(document: Document, event: OperationalEventEnvelope): Boolean =
    MongoHiringCodecs
      .readOperationalEvent(document)
      .toOption
      .exists(existing =>
        existing.eventId == event.eventId &&
          existing.eventType == event.eventType &&
          existing.aggregateType == event.aggregateType &&
          existing.aggregateId == event.aggregateId &&
          existing.actorId == event.actorId &&
          existing.payload == event.payload
      )
}

final class MongoOperationalEventOutboxRepository(
    database: MongoDatabase[IO],
    transactionRunner: MongoTransactionRunner,
    diagnostics: Diagnostics[IO],
    uuidGen: UUIDGen[IO] = UUIDGen[IO],
    maxScanPages: Int = MongoOutboxClaimBatch.MaxScanPages
) extends OperationalEventOutboxRepository[IO] {
  import MongoOperationalEventOutboxRepository.OutboxState
  private val outbox = Mongo4catsCollections.documents(database, MongoCollections.EventOutbox)
  private val subjectFences = Mongo4catsCollections.documents(database, MongoCollections.OutboxSubjectFences)
  private val users = Mongo4catsCollections.documents(database, MongoCollections.Users)

  private val inFlight = MongoFilter.eq(MongoFields.State, OutboxState.InFlight.toString)

  private def updateMany(
      collection: IO[MongoSessionOperations.Documents],
      session: Option[ClientSession[IO]],
      filter: MongoFilter,
      update: MongoUpdate
  ) = MongoSessionOperations.updateMany(collection, session, filter, update)

  /** Age of the oldest due entry, one indexed read bounded by `maxTime`; `None` when nothing is waiting. */
  def oldestDueAge(now: Instant): IO[Option[scala.concurrent.duration.FiniteDuration]] =
    MongoPendingWorkAge.oldest(
      outbox,
      Some(MongoOperationalEventOutboxRepository.OldestDue.filter(now)),
      MongoOperationalEventOutboxRepository.OldestDue.Sort,
      MongoOperationalEventOutboxRepository.OldestDue.Projection,
      MongoFields.OccurredAt,
      now
    )

  override def claim(
      workerId: String,
      transactionalId: String,
      now: Instant,
      leaseDuration: FiniteDuration,
      limit: Int
  ): RepositoryF[IO, List[ClaimedOperationalEvent]] =
    LeaseRequest.whenValid(workerId, leaseDuration)(
      MongoRepositorySupport.repositoryGuard(diagnostics, "outbox.claim")(
        claimPage(workerId, transactionalId, now, leaseDuration, limit)
      )
    )

  private def claimPage(
      workerId: String,
      transactionalId: String,
      now: Instant,
      leaseDuration: FiniteDuration,
      limit: Int
  ): RepositoryF[IO, List[ClaimedOperationalEvent]] = {
    val cursors = Mongo4catsCollections.documents(database, MongoProducerRegistrations.ClaimCursorsCollection)
    def cursorFilter(row: Document, idField: String): Either[RepositoryError, MongoFilter] = for {
      at <- instant(row, "availableAt").map(_.toDate)
      occurred <- instant(row, "occurredAt").map(_.toDate)
      id <- string(row, idField)
    } yield MongoFilter.or(
      MongoFilter.gt(MongoFields.AvailableAt, at),
      MongoFilter.and(MongoFilter.eq(MongoFields.AvailableAt, at), MongoFilter.gt(MongoFields.OccurredAt, occurred)),
      MongoFilter.and(
        MongoFilter.eq(MongoFields.AvailableAt, at),
        MongoFilter.eq(MongoFields.OccurredAt, occurred),
        MongoFilter.gt(MongoFields.Id, id)
      )
    )
    def page(after: Option[MongoFilter]): RepositoryF[IO, List[Document]] = RepositoryF
      .lift(
        outbox.flatMap(
          _.find(after.fold(due(now))(value => MongoFilter.and(due(now), value)).bson)
            .sort(Sorts.ascending(MongoFields.AvailableAt, MongoFields.OccurredAt, MongoFields.Id))
            .projection(
              Projections
                .include(
                  MongoFields.Id,
                  MongoFields.AvailableAt,
                  MongoFields.OccurredAt,
                  MongoFields.SubjectIds,
                  MongoFields.PartitionKey
                )
            )
            .limit(MongoProducerRegistrations.BatchSize)
            .all
        )
      )
      .subflatMap(_.toList.traverse { row =>
        (string(row, MongoFields.Id), instant(row, MongoFields.AvailableAt), instant(row, MongoFields.OccurredAt))
          .mapN((_, _, _) => row)
      })
    def checkpoint(row: Document): RepositoryF[IO, Unit] = RepositoryF
      .lift(
        MongoSessionOperations.updateOne(
          cursors,
          None,
          MongoFilter.eq("_id", workerId),
          MongoUpdate.combine(
            MongoUpdate.set("eventId", row.getString("_id")),
            MongoUpdate.set("availableAt", row.getDate("availableAt")),
            MongoUpdate.set("occurredAt", row.getDate("occurredAt"))
          ),
          new UpdateOptions().upsert(true)
        )
      )
      .void
    // A page can hold only rows that must wait (held subjects, or an earlier event of the same aggregate still
    // retrying). When it is full and the batch is still short, the scan continues with the next page, up to a bound,
    // so other aggregates are not stalled behind it for a poll interval. `position` is the last row scanned and pages
    // the scan; `cursor` is what the worker persists. A row that waits for an earlier event of its aggregate pins the
    // cursor before it (`pinned`): the cursor never passes a blocked row, so it is reached again as soon as the
    // earlier event completes, however much new work keeps arriving behind it.
    def scan(
        rows: List[Document],
        blocked: Set[String],
        values: List[ClaimedOperationalEvent],
        position: Option[Document],
        cursor: Option[Document],
        pinned: Boolean,
        pagesLeft: Int,
        pageFull: Boolean
    ): RepositoryF[IO, List[ClaimedOperationalEvent]] = {
      val need = limit - values.size
      if (need <= 0) cursor.traverse_(checkpoint).as(values)
      else if (rows.isEmpty) {
        val next = if (pageFull && pagesLeft > 0) position.map(row => cursorFilter(row, MongoFields.Id)) else None
        next match {
          case Some(filter) =>
            MongoF
              .fromEither(filter)
              .flatMap(after => page(Some(after)))
              .flatMap(more =>
                pendingBehind(more).flatMap(behind =>
                  scan(
                    more,
                    behind,
                    values,
                    position,
                    cursor,
                    pinned,
                    pagesLeft - 1,
                    more.size >= MongoProducerRegistrations.BatchSize
                  )
                )
              )
          case None => cursor.traverse_(checkpoint).as(values)
        }
      } else {
        val batch = MongoOutboxClaimBatch.select(rows.map(candidate), need, blocked)
        val (examined, rest) = rows.splitAt(batch.examined)
        val (beforeBlocked, _) = examined.span(row => !blocked(row.getString(MongoFields.Id)))
        val nowPinned = pinned || beforeBlocked.size < examined.size
        val nextCursor = if (pinned) cursor else beforeBlocked.lastOption.orElse(cursor)
        claimBatch(workerId, transactionalId, now, leaseDuration, batch.claimable, MongoOutboxClaimBatch.Attempts)
          .flatMap(claimed =>
            scan(
              rest,
              blocked,
              values ++ claimed,
              examined.lastOption.orElse(position),
              nextCursor,
              nowPinned,
              pagesLeft,
              pageFull
            )
          )
      }
    }
    /* Rows of the page that wait for an earlier pending event of their aggregate. This read is only a filter that
     * keeps blocked rows from using the batch budget; the claim transaction re-checks it. */
    def pendingBehind(rows: List[Document]): RepositoryF[IO, Set[String]] =
      earliestPending(None, rows.flatMap(row => Option(row.getString(MongoFields.PartitionKey))).distinct).map(
        earliest =>
          rows.flatMap { row =>
            (Option(row.getString(MongoFields.Id)), Option(row.getString(MongoFields.PartitionKey))).tupled.collect {
              case (id, key) if earliest.get(key).exists(_ != id) => id
            }
          }.toSet
      )
    for {
      current <- RepositoryF.lift(MongoSessionOperations.findOne(cursors, None, MongoFilter.eq("_id", workerId)))
      after <- MongoF.fromEither(current.traverse(cursorFilter(_, "eventId")))
      found <- page(after)
      rows <- if (found.isEmpty && after.nonEmpty) page(None) else MongoF.pure(found)
      behind <- pendingBehind(rows)
      result <- scan(
        rows,
        behind,
        Nil,
        None,
        None,
        false,
        maxScanPages,
        rows.size >= MongoProducerRegistrations.BatchSize
      )
    } yield result
  }

  private def candidate(row: Document): MongoOutboxClaimBatch.Candidate =
    MongoOutboxClaimBatch.Candidate(
      row.getString(MongoFields.Id),
      // An unreadable subject list is not a reason to skip the row: the claim fails it as an invalid event.
      Option(row.get(MongoFields.SubjectIds))
        .collect { case values: java.util.List[?] => values.asScala.toList.collect { case id: String => id } }
        .getOrElse(Nil),
      Option(row.getString(MongoFields.PartitionKey))
    )

  private def due(now: Instant): MongoFilter = MongoOperationalEventOutboxRepository.Claimable.filter(now)

  override def markPublished(
      eventId: UUID,
      leaseToken: String,
      now: Instant,
      retentionExpiresAt: Instant
  ): RepositoryF[IO, Unit] =
    updateClaimed(
      eventId,
      leaseToken,
      MongoUpdate.combine(
        MongoUpdate.set(MongoFields.State, OutboxState.Published.toString),
        MongoUpdate.set(MongoFields.PublishedAt, now.toDate),
        MongoUpdate.set(MongoFields.RetentionExpiresAt, retentionExpiresAt.toDate),
        MongoLeaseQueue.releaseLease,
        MongoUpdate.set(MongoFields.UpdatedAt, now.toDate)
      )
    ).flatMap(_ => releaseSubjectLeases(eventId, leaseToken))

  override def releaseForRetry(
      eventId: UUID,
      leaseToken: String,
      now: Instant,
      availableAt: Instant
  ): RepositoryF[IO, Unit] =
    updateClaimed(
      eventId,
      leaseToken,
      MongoUpdate.combine(
        MongoUpdate.set(MongoFields.State, OutboxState.Retryable.toString),
        MongoUpdate.set(MongoFields.AvailableAt, availableAt.toDate),
        MongoLeaseQueue.releaseLease,
        MongoUpdate.set(MongoFields.UpdatedAt, now.toDate)
      )
    ).flatMap(_ => releaseSubjectLeases(eventId, leaseToken))

  override def markFailed(
      eventId: UUID,
      leaseToken: String,
      now: Instant,
      reason: String
  ): RepositoryF[IO, Unit] =
    updateClaimed(
      eventId,
      leaseToken,
      MongoUpdate.combine(
        MongoUpdate.set(MongoFields.State, OutboxState.Failed.toString),
        MongoUpdate.set(MongoFields.LastError, reason.take(512)),
        MongoLeaseQueue.releaseLease,
        MongoUpdate.set(MongoFields.UpdatedAt, now.toDate)
      )
    ).flatMap(_ => releaseSubjectLeases(eventId, leaseToken))

  override def renewLease(
      eventId: UUID,
      leaseToken: String,
      subjectIds: List[String],
      leaseDuration: FiniteDuration
  ): RepositoryF[IO, Unit] =
    LeaseRequest.whenDurationValid(leaseDuration)(
      MongoRepositorySupport
        .repositoryGuard(diagnostics, "outbox.renewLease")(
          transactionRunner
            .run { session =>
              // Renewal after expiry (on the database clock) is a lost lease, even before another claim takes it over.
              val held = MongoFilter.and(
                MongoFilter.eq(MongoFields.Id, eventId.toString),
                inFlight,
                MongoFilter.eq(MongoFields.LeaseToken, leaseToken),
                MongoLeaseQueue.leaseHeld
              )
              MongoLeaseQueue.renewHeld(outbox, session, held, leaseDuration).flatMap {
                case true =>
                  RepositoryF
                    .lift(
                      MongoSessionOperations.updateManyPipeline(
                        subjectFences,
                        session,
                        MongoFilter.and(
                          MongoFilter.in(MongoFields.Id, subjectIds),
                          MongoFilter.eq(MongoFields.LeaseToken, leaseToken),
                          MongoFilter.eq(MongoFields.LeaseEventId, eventId.toString),
                          MongoFilter.ne(MongoFields.Deleted, true),
                          MongoLeaseQueue.leaseHeld
                        ),
                        MongoLeaseQueue.setStage(MongoLeaseQueue.Standard.extension(leaseDuration))
                      )
                    )
                    .subflatMap(result =>
                      Either.cond(result.getMatchedCount == subjectIds.size.toLong, (), RepositoryError.Conflict)
                    )
                case false => MongoF.raise(RepositoryError.Conflict)
              }
            }
        )
    )

  /** One bounded batch: a single transaction leases the events with one claim token and takes the subject leases of the
    * live ones, so the cost per event falls with the batch size. A lost race on a subject lease (`Conflict`) re-reads
    * the held subjects and tries again with fewer events, `attempts` times, before the page is given up.
    */
  private def claimBatch(
      workerId: String,
      transactionalId: String,
      now: Instant,
      leaseDuration: FiniteDuration,
      candidates: List[MongoOutboxClaimBatch.Candidate],
      attempts: Int
  ): RepositoryF[IO, List[ClaimedOperationalEvent]] =
    if (candidates.isEmpty) MongoF.pure(Nil)
    else
      RepositoryF.lift(uuidGen.randomUUID).flatMap { id =>
        val token = id.toString
        MongoRepositorySupport
          .repositoryGuard(diagnostics, "outbox.claimBatch")(transactionRunner.run { session =>
            claimInSession(session, workerId, transactionalId, now, leaseDuration, candidates, token)
          })
          .leftFlatMap {
            case RepositoryError.Conflict if attempts > 1 =>
              claimBatch(workerId, transactionalId, now, leaseDuration, candidates, attempts - 1)
            case RepositoryError.Conflict => MongoF.pure(Nil)
            case error                    => MongoF.raise(error)
          }
      }

  /** Claims exactly these candidates in one transaction, without the page scan or its blocked-row filter, so the
    * in-transaction aggregate-order check can be exercised on its own.
    */
  private[mongo] def claimCandidates(
      workerId: String,
      transactionalId: String,
      now: Instant,
      leaseDuration: FiniteDuration,
      candidates: List[MongoOutboxClaimBatch.Candidate]
  ): RepositoryF[IO, List[ClaimedOperationalEvent]] =
    claimBatch(workerId, transactionalId, now, leaseDuration, candidates, MongoOutboxClaimBatch.Attempts)

  private def claimInSession(
      session: Option[ClientSession[IO]],
      workerId: String,
      transactionalId: String,
      now: Instant,
      leaseDuration: FiniteDuration,
      candidates: List[MongoOutboxClaimBatch.Candidate],
      token: String
  ): RepositoryF[IO, List[ClaimedOperationalEvent]] =
    for {
      held <- heldSubjects(session, candidates.flatMap(_.subjects).distinct)
      earliest <- earliestPending(session, candidates.flatMap(_.partitionKey).distinct)
      // Per-aggregate order: an event waits while an earlier non-terminal event of its aggregate exists.
      free = candidates.filterNot(c =>
        c.subjects.exists(held) || c.partitionKey.exists(key => earliest.get(key).exists(_ != c.id))
      )
      documents <- leaseEvents(session, workerId, token, now, leaseDuration, free)
      // A row without readable attribution, attempt counter or subject list, or with an event that cannot be
      // interpreted, is ended as Failed (its stored evidence is kept) and never blocks the rest of the batch.
      decoded = documents.map(document => document -> readClaimedRow(document))
      unreadable = decoded.collect { case (document, Left(_)) => document.getString(MongoFields.Id) }
      readable = decoded.collect { case (_, Right(claim)) => claim }
      deleted <- deletedSubjects(session, readable.flatMap(_.subjectIds).distinct)
      (suppressed, live) = readable.partition(_.subjectIds.exists(deleted))
      _ <- failClaimed(session, token, unreadable, "INVALID_EVENT_CONTRACT", now)
      _ <- failClaimed(session, token, suppressed.map(_.event.eventId.toString), "SUBJECT_DELETED", now)
      _ <- leaseSubjects(session, live, transactionalId, token, now, leaseDuration)
    } yield live

  /** Per partition key, the id of the earliest `Retryable` or `InFlight` event by `(occurredAt, _id)`. `Published` and
    * `Failed` events never block; one `$group` row per key bounds the result by the candidate count.
    */
  private def earliestPending(
      session: Option[ClientSession[IO]],
      keys: List[String]
  ): RepositoryF[IO, Map[String, String]] =
    if (keys.isEmpty) MongoF.pure(Map.empty)
    else {
      val pipeline = MongoOutboxClaimBatch.earliestPendingAggregate(keys)
      MongoRepositorySupport.transactionGuard(diagnostics, "outbox.earliestPending", session)(
        RepositoryF.lift(
          outbox
            .flatMap(c =>
              session
                .fold(c.aggregate[Document](pipeline))(active => c.aggregate[Document](active, pipeline))
                .boundedStream(keys.size + 1)
                .compile
                .toList
            )
            .map(_.flatMap(row => (Option(row.getString(MongoFields.Id)), Option(row.getString("first"))).tupled).toMap)
        )
      )
    }

  /** The subjects whose lease is still held by another claim: their events wait for it to be released or to expire. */
  private def heldSubjects(
      session: Option[ClientSession[IO]],
      subjects: List[String]
  ): RepositoryF[IO, Set[String]] =
    if (subjects.isEmpty) MongoF.pure(Set.empty)
    else
      MongoRepositorySupport.transactionGuard(diagnostics, "outbox.heldSubjects", session)(
        RepositoryF
          .lift(
            MongoSessionOperations.findManyById(
              subjectFences,
              session,
              MongoFilter.and(MongoFilter.in(MongoFields.Id, subjects), MongoLeaseQueue.leaseHeld),
              subjects.size,
              Some(Projections.include(MongoFields.Id))
            )
          )
          .map(_.flatMap(row => Option(row.getString(MongoFields.Id))).toSet)
      )

  /** Stamps every due candidate with the batch token in one write and reads back exactly the ones this claim won. */
  private def leaseEvents(
      session: Option[ClientSession[IO]],
      workerId: String,
      token: String,
      now: Instant,
      leaseDuration: FiniteDuration,
      candidates: List[MongoOutboxClaimBatch.Candidate]
  ): RepositoryF[IO, List[Document]] =
    if (candidates.isEmpty) MongoF.pure(Nil)
    else {
      val ids = candidates.map(_.id)
      MongoRepositorySupport.transactionGuard(diagnostics, "outbox.leaseEvents", session)(
        RepositoryF
          .lift(
            MongoSessionOperations.updateManyPipeline(
              outbox,
              session,
              MongoFilter.and(due(now), MongoFilter.in(MongoFields.Id, ids)),
              MongoLeaseQueue.leaseStamp(
                OutboxState.InFlight.toString,
                workerId,
                token,
                leaseDuration,
                now,
                MongoFields.Attempts -> new BsonDocument(
                  "$add",
                  new BsonArray(
                    List[BsonValue](
                      new BsonDocument(
                        "$ifNull",
                        new BsonArray(
                          List[BsonValue](new BsonString("$" + MongoFields.Attempts), new BsonInt32(0)).asJava
                        )
                      ),
                      new BsonInt32(1)
                    ).asJava
                  )
                )
              )
            )
          )
          .flatMap(_ =>
            RepositoryF.lift(
              MongoSessionOperations.findManyById(
                outbox,
                session,
                MongoFilter.and(MongoFilter.in(MongoFields.Id, ids), MongoFilter.eq(MongoFields.LeaseToken, token)),
                ids.size
              )
            )
          )
          .map(MongoOutboxClaimBatch.inOrder(ids, _))
      )
    }

  /** The attributable control evidence every claimed row must carry, even when its event cannot be interpreted. */
  private def checkStored(document: Document): Either[RepositoryError, Unit] =
    (
      requiredSubjectIds(document),
      requiredSubjectRefsVersion(document),
      MongoDocumentFields.requiredInt(document, MongoFields.Attempts, min = 0)
    ).mapN((_, _, _) => ()).leftMap(_ => RepositoryError.InvalidStoredData)

  private def readClaimedRow(document: Document): Either[Unit, ClaimedOperationalEvent] =
    checkStored(document).leftMap(_ => ()).flatMap(_ => readClaimed(document).leftMap(_ => ()))

  private def deletedSubjects(
      session: Option[ClientSession[IO]],
      subjects: List[String]
  ): RepositoryF[IO, Set[String]] =
    if (subjects.isEmpty) MongoF.pure(Set.empty)
    else
      MongoRepositorySupport.transactionGuard(diagnostics, "outbox.subjectIsDeleted", session)(
        RepositoryF
          .lift(
            MongoSessionOperations.findManyById(
              users,
              session,
              MongoFilter
                .and(MongoFilter.in(MongoFields.Id, subjects), MongoFilter.eq(MongoFields.AccountStatus, "Deleted")),
              subjects.size,
              Some(Projections.include(MongoFields.Id))
            )
          )
          .map(_.flatMap(row => Option(row.getString(MongoFields.Id))).toSet)
      )

  /** Ends claimed events that must never be published, under this claim's token. */
  private def failClaimed(
      session: Option[ClientSession[IO]],
      token: String,
      eventIds: List[String],
      reason: String,
      now: Instant
  ): RepositoryF[IO, Unit] =
    if (eventIds.isEmpty) MongoF.pure(())
    else
      MongoRepositorySupport.transactionGuard(diagnostics, "outbox.failClaimed", session)(
        RepositoryF
          .lift(
            updateMany(
              outbox,
              session,
              MongoFilter
                .and(MongoFilter.in(MongoFields.Id, eventIds), inFlight, MongoFilter.eq(MongoFields.LeaseToken, token)),
              MongoUpdate.combine(
                MongoUpdate.set(MongoFields.State, OutboxState.Failed.toString),
                MongoUpdate.set(MongoFields.LastError, reason),
                MongoUpdate.set(MongoFields.UpdatedAt, now.toDate),
                MongoLeaseQueue.releaseLease
              )
            )
          )
          .subflatMap(result =>
            Either.cond(result.getMatchedCount == eventIds.size.toLong, (), RepositoryError.Conflict)
          )
      )

  /** Takes the lease of every subject of the live claims and registers the producer generation, each in one ordered
    * bulk write. A subject lease still held by another claim is a duplicate key: a typed `Conflict` for the retry.
    */
  private[mongo] def leaseSubjects(
      session: Option[ClientSession[IO]],
      live: List[ClaimedOperationalEvent],
      transactionalId: String,
      token: String,
      now: Instant,
      leaseDuration: FiniteDuration
  ): RepositoryF[IO, Unit] =
    if (live.isEmpty) MongoF.pure(())
    else {
      val available = MongoFilter.or(
        MongoFilter.exists(MongoFields.LeaseUntil, false),
        MongoLeaseQueue.Standard.expired
      )
      val grants =
        MongoOutboxClaimBatch.subjectGrants(live.map(claim => claim.event.eventId.toString -> claim.subjectIds))
      def literal(value: String): BsonValue = new BsonDocument("$literal", new BsonString(value))
      val entries = grants.flatMap { case (subject, eventId) =>
        // Deletion is primarily read from `users.accountStatus` inside this transaction; a fence that deletion already
        // marked is refused here too (a duplicate key on the second entry, a typed Conflict), so a lease can never be
        // granted on a deleted subject even if the first read were stale.
        List(
          // 1. A fence that exists and is available (never leased, or expired on the database clock) is leased.
          PipelineUpdate(
            MongoFilter
              .and(MongoFilter.eq(MongoFields.Id, subject), MongoFilter.ne(MongoFields.Deleted, true), available),
            MongoLeaseQueue.setStage(
              MongoFields.LeaseEventId -> literal(eventId),
              MongoFields.LeaseToken -> literal(token),
              MongoLeaseQueue.Standard.extension(leaseDuration)
            )
          ),
          // 2. Matches only a fence this claim just leased (a no-op: every field is kept) or inserts the missing one.
          //    A fence that is held by another claim or deleted matches neither entry: inserting its `_id` is a
          //    duplicate key, the typed Conflict for the retry.
          PipelineUpdate(
            MongoFilter.and(MongoFilter.eq(MongoFields.Id, subject), MongoFilter.eq(MongoFields.LeaseToken, token)),
            MongoLeaseQueue.setStage(
              MongoFields.LeaseEventId -> new BsonDocument(
                "$ifNull",
                new BsonArray(List[BsonValue](new BsonString("$" + MongoFields.LeaseEventId), literal(eventId)).asJava)
              ),
              MongoFields.LeaseToken -> literal(token),
              MongoFields.LeaseUntil -> new BsonDocument(
                "$ifNull",
                new BsonArray(
                  List[BsonValue](
                    new BsonString("$" + MongoFields.LeaseUntil),
                    MongoLeaseQueue.Standard.extension(leaseDuration)._2
                  ).asJava
                )
              )
            ),
            upsert = true
          )
        )
      }
      MongoRepositorySupport
        .transactionGuard(diagnostics, "outbox.acquireSubjectLeases", session)(
          RepositoryF.lift(MongoSessionOperations.pipelineEach(subjectFences, session, entries))
        ) {
          case MongoDuplicateKey(_) => Left(RepositoryError.Conflict)
          case error: MongoCommandException if error.getErrorCode == MongoDuplicateKey.Code =>
            Left(RepositoryError.Conflict)
          case _ => Left(RepositoryError.Unavailable)
        } *>
        MongoProducerRegistrations.registerAll(
          database,
          session,
          grants.map(_._1),
          transactionalId,
          ProducerRegistrationValues.Operational,
          now
        )
    }

  /** Releases the subject leases of one event; the batch token is shared, so the event identity selects its subjects.
    */
  private def releaseSubjectLeases(eventId: UUID, leaseToken: String): RepositoryF[IO, Unit] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "outbox.releaseSubjectLeases")(
        RepositoryF
          .lift(
            updateMany(
              subjectFences,
              None,
              MongoFilter.and(
                MongoFilter.eq(MongoFields.LeaseToken, leaseToken),
                MongoFilter.eq(MongoFields.LeaseEventId, eventId.toString)
              ),
              MongoUpdate.combine(
                MongoUpdate.unset(MongoFields.LeaseEventId),
                MongoUpdate.unset(MongoFields.LeaseToken),
                MongoUpdate.unset(MongoFields.LeaseUntil)
              )
            )
          )
          .subflatMap(result => Either.cond(result.getMatchedCount > 0L, (), RepositoryError.Conflict))
      )

  private def updateClaimed(
      eventId: UUID,
      leaseToken: String,
      update: MongoUpdate
  ): RepositoryF[IO, Unit] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "outbox.updateClaimed")(
        RepositoryF
          .lift(
            MongoSessionOperations
              .updateOne(
                outbox,
                None,
                MongoFilter.and(
                  MongoFilter.eq(MongoFields.Id, eventId.toString),
                  inFlight,
                  MongoFilter.eq(MongoFields.LeaseToken, leaseToken)
                ),
                update
              )
          )
          .subflatMap(MongoRepositorySupport.matchedOne(_))
      )

  private def readClaimed(
      document: Document
  ): Either[NonEmptyList[MongoHiringCodecs.StoredDocumentError], ClaimedOperationalEvent] =
    (
      MongoHiringCodecs.readOperationalEvent(document),
      envelopeBytes(document).toValidatedNel,
      MongoDocumentFields.requiredString(document, MongoFields.PartitionKey).toValidatedNel,
      MongoDocumentFields.requiredString(document, MongoFields.LeaseToken).toValidatedNel,
      MongoDocumentFields.requiredInt(document, MongoFields.Attempts, min = 0).toValidatedNel,
      requiredSubjectIds(document).toValidatedNel,
      requiredSubjectRefsVersion(document).toValidatedNel
    ).mapN((event, bytes, key, token, attempts, subjects, _) =>
      ClaimedOperationalEvent(event, bytes, key, token, attempts, subjects)
    ).toEither
      .flatMap { claim =>
        // `readOperationalEvent` decoded the immutable envelope bytes and matched the queried top-level fields to them.
        val consistent = claim.partitionKey == claim.event.partitionKey &&
          MongoHiringCodecs.outboxSubjectIds(claim.event).contains(claim.subjectIds.sorted)
        Either.cond(consistent, claim, NonEmptyList.one(MongoHiringCodecs.StoredDocumentError.InconsistentDocument))
      }

  private def envelopeBytes(document: Document): Either[MongoHiringCodecs.StoredDocumentError, Array[Byte]] =
    Option(document.get(MongoFields.EnvelopeBytes)) match {
      case Some(value: Array[Byte]) => Right(value)
      case Some(value: Binary)      => Right(value.getData)
      case _ => Left(MongoHiringCodecs.StoredDocumentError.InvalidField(MongoFields.EnvelopeBytes))
    }

  private def requiredSubjectIds(
      document: Document
  ): Either[MongoHiringCodecs.StoredDocumentError, List[String]] =
    MongoDocumentFields.requiredStringList(document, MongoFields.SubjectIds).flatMap { subjects =>
      Either.cond(
        subjects.nonEmpty && subjects.distinct.size == subjects.size &&
          subjects.forall(MongoDocumentFields.parseUuid(MongoFields.SubjectIds, _).isRight),
        subjects,
        MongoHiringCodecs.StoredDocumentError.InvalidField(MongoFields.SubjectIds)
      )
    }

  private def requiredSubjectRefsVersion(
      document: Document
  ): Either[MongoHiringCodecs.StoredDocumentError, Int] =
    MongoDocumentFields
      .requiredInt(document, MongoFields.SubjectRefsVersion)
      .filterOrElse(_ == 1, MongoHiringCodecs.StoredDocumentError.InvalidField(MongoFields.SubjectRefsVersion))
}

object MongoOperationalEventOutboxRepository {

  /** Stored outbox lifecycle values; string forms are the persisted contract and the single source of the names. */
  private[mongo] enum OutboxState {
    case Retryable, InFlight, Published, Failed
  }

  /** The stored states and predicate by which the outbox claim selects work; backlog sampling reuses them. */
  private[mongo] object Claimable {
    val Retryable: String = OutboxState.Retryable.toString
    val InFlight: String = OutboxState.InFlight.toString

    def retryableDue(now: Instant): MongoFilter = MongoLeaseQueue.readyDue(List(Retryable), now)
    val expiredInFlight: MongoFilter = MongoLeaseQueue.leaseExpired(InFlight)

    /** Due `Retryable` entries plus `InFlight` entries whose lease expired on the database clock. */
    def filter(now: Instant): MongoFilter =
      MongoLeaseQueue.claimable(retryableDue(now), MongoFilter.eq(MongoFields.State, InFlight))
  }

  /** Oldest due outbox entry: the claim index (`state`, `availableAt`, ...) yields the first due `Retryable` row
    * without a sort stage. Its age is measured from `occurredAt`, so repeated retries do not hide a stuck event.
    */
  private[mongo] object OldestDue {
    def filter(now: Instant): MongoFilter =
      MongoFilter.and(
        MongoFilter.eq(MongoFields.State, Claimable.Retryable),
        MongoFilter.lte(MongoFields.AvailableAt, now.toDate)
      )
    val Sort: org.bson.conversions.Bson = Sorts.ascending(MongoFields.AvailableAt)
    val Projection: org.bson.conversions.Bson = Projections.include(MongoFields.OccurredAt)
  }

  def transactional(
      database: MongoDatabase[IO],
      client: MongoClient[IO],
      diagnostics: Diagnostics[IO],
      uuidGen: UUIDGen[IO] = UUIDGen[IO],
      breaker: CircuitBreaker[IO] = CircuitBreaker.disabled[IO],
      maxScanPages: Int = MongoOutboxClaimBatch.MaxScanPages
  ): MongoOperationalEventOutboxRepository =
    new MongoOperationalEventOutboxRepository(
      database,
      MongoTransactionRunner.sessions(client, RepositoryError.Conflict, diagnostics = diagnostics, breaker = breaker),
      diagnostics,
      uuidGen,
      maxScanPages
    )
}

final class MongoConsumerReceiptRepository(database: MongoDatabase[IO], diagnostics: Diagnostics[IO])
    extends ConsumerReceiptRepository[IO] {
  private val receipts = Mongo4catsCollections.documents(database, MongoCollections.ConsumerReceipts)

  override def exists(consumerGroup: String, eventId: UUID): RepositoryF[IO, Boolean] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "consumerReceipt.exists")(
        RepositoryF
          .lift(
            MongoSessionOperations
              .findOne(receipts, None, MongoFilter.eq(MongoFields.Id, s"$consumerGroup:${eventId.toString}"))
          )
          .map(_.nonEmpty)
      )

  override def record(
      consumerGroup: String,
      event: OperationalEventEnvelope,
      now: Instant,
      expiresAt: Instant
  ): RepositoryF[IO, Boolean] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "consumerReceipt.record")(
        RepositoryF
          .lift(
            MongoSessionOperations
              .insertOne(
                receipts,
                None,
                new Document(MongoFields.Id, s"$consumerGroup:${event.eventId.toString}")
                  .append(MongoFields.ConsumerGroup, consumerGroup)
                  .append(MongoFields.EventId, event.eventId.toString)
                  .append(MongoFields.AggregateType, event.aggregateType.toString)
                  .append(MongoFields.AggregateId, event.aggregateId)
                  .append(MongoFields.CreatedAt, now.toDate)
                  .append(MongoFields.ExpiresAt, expiresAt.toDate)
              )
          )
          .as(true)
      ) {
        case MongoDuplicateKey(_) => Right(false)
        case _                    => Left(RepositoryError.Unavailable)
      }

}

final class MongoEventQuarantineRepository(database: MongoDatabase[IO], diagnostics: Diagnostics[IO])
    extends EventQuarantineRepository[IO] {
  private val quarantine = Mongo4catsCollections.documents(database, MongoCollections.EventQuarantine)

  override def save(record: EventQuarantineRecord): RepositoryF[IO, Unit] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "eventQuarantine.save")(
        RepositoryF
          .lift(
            MongoSessionOperations
              .insertOne(
                quarantine,
                None,
                new Document()
                  .append(MongoFields.Topic, record.topic)
                  .append(MongoFields.Partition, java.lang.Integer.valueOf(record.partition))
                  .append(MongoFields.Offset, java.lang.Long.valueOf(record.offset))
                  .append(MongoFields.Category, record.category.toString)
                  .append(MongoFields.Reason, record.reason.take(512))
                  .append(MongoFields.RawBytes, record.rawBytes)
                  .append(MongoFields.OccurredAt, record.occurredAt.toDate)
                  .append(MongoFields.ExpiresAt, record.expiresAt.toDate)
              )
          )
          .void
      ) {
        case MongoDuplicateKey(_) => Right(())
        case _                    => Left(RepositoryError.Unavailable)
      }

}

object MongoSearchSessionRepository {
  def apply(database: MongoDatabase[IO], diagnostics: Diagnostics[IO]): MongoSearchSessionRepository =
    new MongoSearchSessionRepository(database, diagnostics)
}

/** The pure part of the batch claim: which events share a transaction and which subject each lease belongs to. */
private[mongo] object MongoOutboxClaimBatch {

  /** A due event as the page scan saw it: its identity and the subjects whose lease its publication needs. */
  final case class Candidate(id: String, subjects: List[String], partitionKey: Option[String] = None)

  /** The earliest pending event per partition key: one `$group` row per key, read through the partial index
    * `event_outbox_aggregate_order` as a `DISTINCT_SCAN` (the sort is the index order, so no `SORT` stage runs).
    */
  def earliestPendingAggregate(keys: List[String]): Aggregate = {
    val pending = MongoFilter.and(
      MongoFilter.in(MongoFields.PartitionKey, keys),
      MongoFilter.in(MongoFields.State, PendingStates)
    )
    Aggregate
      .matchBy(pending.sessionFilter)
      .sort(Sort.asc(MongoFields.PartitionKey).asc(MongoFields.OccurredAt).asc(MongoFields.Id))
      .group("$" + MongoFields.PartitionKey, Accumulator.first("first", "$" + MongoFields.Id))
  }

  /** The non-terminal outbox states: `Retryable` and `InFlight`. */
  val PendingStates: List[String] = List("Retryable", "InFlight")

  /** Pages a single claim may scan when every row of a full page has to wait. */
  val MaxScanPages = 8

  /** Attempts of one page when another claim wins a subject lease between the read and the write. */
  val Attempts = 3

  /** The events of one batch and how many scanned rows it consumed. */
  final case class Selection(claimable: List[Candidate], examined: Int)

  /** Rows in `blocked` (an earlier pending event of their aggregate exists) are examined but never taken. Takes at most
    * `need` events in scan order such that no two share a subject: a subject lease belongs to one event at a time, so a
    * later event of the same subject waits for the next claim. Events without subjects are taken as they are (the claim
    * fails them as invalid). Every row up to the last one taken, or all of them, counts as examined; rows skipped on
    * the way stay `Retryable`.
    */
  def select(rows: List[Candidate], need: Int, blocked: Set[String] = Set.empty): Selection = {
    @annotation.tailrec
    def loop(rest: List[Candidate], taken: List[Candidate], used: Set[String], examined: Int): Selection =
      rest match {
        case _ if taken.size >= need => Selection(taken.reverse, examined)
        case Nil                     => Selection(taken.reverse, examined)
        case row :: tail             =>
          if (blocked(row.id) || row.subjects.exists(used)) loop(tail, taken, used, examined + 1)
          else loop(tail, row :: taken, used ++ row.subjects, examined + 1)
      }
    loop(rows, Nil, Set.empty, 0)
  }

  /** The subject leases to write, `(subject, event)` in ascending subject order so concurrent batches take their locks
    * in the same order and cannot deadlock. The first event that names a subject owns it.
    */
  def subjectGrants(events: List[(String, List[String])]): List[(String, String)] =
    events
      .flatMap { case (eventId, subjects) => subjects.map(_ -> eventId) }
      .groupBy(_._1)
      .view
      .mapValues(_.head._2)
      .toList
      .sortBy(_._1)

  /** The documents in the order of `ids`; anything not among them is dropped. */
  def inOrder(ids: List[String], documents: List[Document]): List[Document] = {
    val position = ids.zipWithIndex.toMap
    documents.filter(document => position.contains(document.getString(MongoFields.Id))).sortBy { document =>
      position(document.getString(MongoFields.Id))
    }
  }
}

/** One bounded read of the oldest waiting row of a durable queue: `limit(1)`, a projection of the timestamp only, and a
  * server-side time limit, so a slow or unavailable store fails the observation instead of the request.
  */
private[mongo] object MongoPendingWorkAge {
  val MaxTime: scala.concurrent.duration.FiniteDuration = scala.concurrent.duration.DurationInt(500).millis

  def oldest(
      collection: IO[MongoSessionOperations.Documents],
      filter: Option[MongoFilter],
      sort: org.bson.conversions.Bson,
      projection: org.bson.conversions.Bson,
      timestampField: String,
      now: Instant
  ): IO[Option[scala.concurrent.duration.FiniteDuration]] =
    collection
      .flatMap(c =>
        filter
          .fold(c.find)(value => c.find(value.bson))
          .sort(sort)
          .projection(projection)
          .limit(1)
          .maxTime(MaxTime)
          .first
      )
      .map(
        _.flatMap(row => Option(row.getDate(timestampField))).map(date =>
          scala.concurrent.duration.FiniteDuration(
            java.time.Duration.between(date.toInstant, now).toMillis.max(0L),
            java.util.concurrent.TimeUnit.MILLISECONDS
          )
        )
      )
}
