package com.example.graphQL.cats.repository.mongo

import cats.effect.{Clock, IO}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField, RepositoryError}
import com.example.graphQL.cats.service.Diagnostics.*
import com.example.graphQL.cats.service.events.{OperationalEventPayload, OperationalEvents}
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.resilience.CircuitBreaker
import com.example.graphQL.cats.shared.StableIds
import com.mongodb.client.model.{Projections, Sorts, UpdateOptions}
import mongo4cats.client.{ClientSession, MongoClient}
import mongo4cats.database.MongoDatabase
import java.time.Instant

/** Closes at most one page of a recruiter's open jobs inside the caller's transaction. The closure time is a value
  * chosen by the deletion, so the `JOB_CLOSED` event identity (job and closure time) is the same on any retry.
  */
private[mongo] final class MongoRecruiterJobClosurePages(
    database: MongoDatabase[IO],
    diagnostics: Diagnostics[IO],
    pageSize: JobClosurePageSize
) {
  private def jobs = Mongo4catsCollections.documents(database, MongoCollections.Jobs)
  private def outbox = Mongo4catsCollections.documents(database, MongoCollections.EventOutbox)
  private def work = Mongo4catsCollections.documents(database, MongoCollections.RecruiterJobClosureWork)

  private def openJobs(recruiter: UserId): MongoFilter = MongoFilter.and(
    MongoFilter.eq(MongoFields.RecruiterId, recruiter.value.toString),
    MongoFilter.eq(MongoFields.Status, JobStatus.Open.toString)
  )

  /** Closes the next page, then reports whether open jobs remain. The read follows the recruiter/status/created index
    * order, so it is a bounded index walk rather than a sort of every open job.
    */
  def closePage(
      recruiter: UserId,
      closedAt: Instant,
      session: Option[ClientSession[IO]]
  ): RepositoryF[IO, MongoRecruiterJobClosurePages.Page] =
    RepositoryF
      .lift(
        jobs.flatMap { collection =>
          val ordered = Sorts.orderBy(Sorts.descending(MongoFields.CreatedAt), Sorts.descending(MongoFields.Id))
          val filter = openJobs(recruiter)
          val limit = pageSize.value
          session.fold(collection.find(filter.bson).sort(ordered).limit(limit).boundedStream(limit).compile.toList)(
            active =>
              collection
                .find(active, filter.sessionFilter)
                .sort(ordered)
                .limit(limit)
                .boundedStream(limit)
                .compile
                .toList
          )
        }
      )
      .subflatMap(documents => MongoStoredDocumentDecoding.values(documents.map(MongoHiringCodecs.readVersionedJob)))
      .flatMap(versioned =>
        close(recruiter, closedAt, session, versioned) *>
          openJobRemains(recruiter, session).map(MongoRecruiterJobClosurePages.Page(versioned.size, _))
      )

  private def openJobRemains(recruiter: UserId, session: Option[ClientSession[IO]]): RepositoryF[IO, Boolean] =
    RepositoryF
      .lift(
        MongoSessionOperations.findOne(jobs, session, openJobs(recruiter), Some(Projections.include(MongoFields.Id)))
      )
      .map(_.nonEmpty)

  private def close(
      recruiter: UserId,
      closedAt: Instant,
      session: Option[ClientSession[IO]],
      versionedJobs: List[Versioned[Job]]
  ): RepositoryF[IO, Unit] =
    if (versionedJobs.isEmpty) RepositoryF.at[IO].pure(())
    else {
      val closedJobs =
        versionedJobs.map(_.value.copy(status = JobStatus.Closed, closedAt = Some(closedAt), updatedAt = closedAt))
      val writes = versionedJobs.zip(closedJobs).traverse_ { case (observed, closed) =>
        for {
          nextVersion <- RepositoryF
            .at[IO]
            .fromEither(Versioned.nextVersion(observed.version).toRight(RepositoryError.Conflict))
          result <- RepositoryF.lift(
            MongoSessionOperations.replaceOne(
              jobs,
              session,
              MongoFilter.and(
                MongoFilter.eq(MongoFields.Id, observed.value.id.value.toString),
                MongoFilter.eq(MongoFields.Version, observed.version),
                MongoFilter.eq(MongoFields.Status, JobStatus.Open.toString)
              ),
              MongoHiringCodecs.job(closed, nextVersion)
            )
          )
          _ <- RepositoryF.at[IO].fromEither(MongoRepositorySupport.matchedOne(result))
        } yield ()
      }
      val events = closedJobs.traverse { closed =>
        OperationalEvents.jobEvent(
          OperationalEventPayload.JobKind.Closed,
          StableIds.uuid(s"job:${closed.id.value}:JOB_CLOSED:${closed.updatedAt.toEpochMilli}"),
          closed,
          recruiter,
          closedAt
        )
      }
      for {
        validEvents <- RepositoryF.at[IO].fromEither(events.leftMap(_ => RepositoryError.InvalidEvent))
        _ <- writes
        _ <- MongoOperationalEventInsertion.insert(outbox, session, validEvents, closedAt, diagnostics)
      } yield ()
    }

  /** Idempotent: a deletion that is replayed or raced by another keeps the first request time. */
  def enqueue(recruiter: UserId, requestedAt: Instant, session: Option[ClientSession[IO]]): RepositoryF[IO, Unit] =
    RepositoryF
      .lift(
        MongoSessionOperations.updateOne(
          work,
          session,
          MongoFilter.eq(MongoFields.Id, recruiter.value.toString),
          MongoUpdate.combine(
            MongoUpdate.setOnInsert(MongoFields.RequestedAt, requestedAt.toDate),
            MongoUpdate.setOnInsert(MongoFields.NotBefore, requestedAt.toDate),
            MongoUpdate.setOnInsert(MongoFields.Attempts, Int.box(0))
          ),
          new UpdateOptions().upsert(true)
        )
      )
      .void
}

private[mongo] object MongoRecruiterJobClosurePages {

  /** Jobs closed by one page and whether open jobs of the recruiter remain after it. */
  final case class Page(closed: Int, remains: Boolean)
}

/** Follow-up closure of the open jobs of deleted recruiters, one bounded transaction per page.
  *
  * The head is the earliest eligible row by `(notBefore, requestedAt, _id)`; it is peeked without a transaction so an
  * idle worker never opens a session. A page that fails (a malformed job, a permanent conflict) rolls back, then its
  * row alone gets `attempts + 1` and a later `notBefore` in a separate small write: it is retried, never dropped, and
  * later rows are served meanwhile.
  */
final class MongoRecruiterJobClosure private[mongo] (
    database: MongoDatabase[IO],
    transactionRunner: MongoTransactionRunner,
    diagnostics: Diagnostics[IO],
    pageSize: JobClosurePageSize,
    retry: JobClosureRetry,
    clock: Clock[IO]
) extends RecruiterJobClosureRepository[IO] {
  private def work = Mongo4catsCollections.documents(database, MongoCollections.RecruiterJobClosureWork)
  private val pages = new MongoRecruiterJobClosurePages(database, diagnostics, pageSize)
  private val order =
    Sorts.orderBy(
      Sorts.ascending(MongoFields.NotBefore),
      Sorts.ascending(MongoFields.RequestedAt),
      Sorts.ascending(MongoFields.Id)
    )

  /** Enqueue always writes `notBefore`, so eligibility is a single range predicate. */
  private def eligible(now: Instant): MongoFilter = MongoFilter.lte(MongoFields.NotBefore, now.toDate)

  /** Age of the oldest pending closure since the deletion requested it. The collection holds one row per deleted
    * recruiter with unfinished closures, so this read is bounded by `limit(1)` and `maxTime`.
    */
  def oldestPendingAge(now: Instant): IO[Option[scala.concurrent.duration.FiniteDuration]] =
    MongoPendingWorkAge.oldest(
      work,
      None,
      Sorts.ascending(MongoFields.RequestedAt),
      Projections.include(MongoFields.RequestedAt),
      MongoFields.RequestedAt,
      now
    )

  override def closeNextPage: RepositoryF[IO, RecruiterJobClosureProgress] =
    MongoRepositorySupport.repositoryGuard(diagnostics, "recruiterJobClosure.closeNextPage")(
      for {
        now <- RepositoryF.lift(clock.realTimeInstant)
        head <- RepositoryF.lift(peek(now))
        progress <- head.fold(RepositoryF.at[IO].pure(RecruiterJobClosureProgress.Idle))(id => closeRow(id, now))
      } yield progress
    )

  private def peek(now: Instant): IO[Option[String]] =
    work
      .flatMap(
        _.find(eligible(now).bson).sort(order).projection(Projections.include(MongoFields.Id)).limit(1).first
      )
      .map(_.flatMap(row => Option(row.getString(MongoFields.Id))))

  private def closeRow(id: String, now: Instant): RepositoryF[IO, RecruiterJobClosureProgress] =
    RepositoryF.fromFEither(
      transactionRunner.run(session => closeFor(id, now, session)).value.attempt.flatMap {
        case Right(Right(progress)) => IO.pure(Right(progress))
        case Right(Left(error))     => rotate(id, now).as(Left(error))
        case Left(error)            => rotate(id, now) *> IO.raiseError(error)
      }
    )

  private def rotate(id: String, now: Instant): IO[Unit] =
    (for {
      row <- MongoSessionOperations.findById(work, None, id)
      attempts = row
        .flatMap(r => Option(r.get(MongoFields.Attempts)))
        .collect { case n: Number => n.intValue() }
        .getOrElse(0)
      next = math.min(attempts, Int.MaxValue - 1) + 1
      _ <- MongoSessionOperations.updateOne(
        work,
        None,
        MongoFilter.eq(MongoFields.Id, id),
        MongoUpdate.combine(
          MongoUpdate.set(MongoFields.Attempts, Int.box(next)),
          MongoUpdate.set(MongoFields.NotBefore, now.plusMillis(retry.delay(next).toMillis).toDate)
        )
      )
      // Attempt count only: a user id never reaches the log.
      _ <- diagnostics.emit(
        LogEvent.RuntimeFailed,
        fields = Map(
          LogField.Worker -> "recruiterJobClosure",
          LogField.SpanName -> "recruiterJobClosure.rotated",
          LogField.Count -> next.toString
        )
      )
    } yield ()).attempt.void

  private def closeFor(
      id: String,
      now: Instant,
      session: Option[ClientSession[IO]]
  ): RepositoryF[IO, RecruiterJobClosureProgress] =
    RepositoryF
      .lift(
        MongoSessionOperations.findOne(
          work,
          session,
          MongoFilter.and(MongoFilter.eq(MongoFields.Id, id), eligible(now))
        )
      )
      .flatMap {
        case None      => RepositoryF.at[IO].pure(RecruiterJobClosureProgress.Idle)
        case Some(row) => closeRecruiter(row, now, session)
      }

  private def closeRecruiter(
      row: org.bson.Document,
      now: Instant,
      session: Option[ClientSession[IO]]
  ): RepositoryF[IO, RecruiterJobClosureProgress] =
    for {
      recruiter <- RepositoryF
        .at[IO]
        .fromEither(
          MongoDocumentFields
            .parseUuid(MongoFields.Id, Option(row.getString(MongoFields.Id)).getOrElse(""))
            .map(UserId.apply)
            .leftMap(_ => RepositoryError.InvalidStoredData)
        )
      requestedAt <- RepositoryF
        .at[IO]
        .fromEither(
          Option(row.getDate(MongoFields.RequestedAt)).map(_.toInstant).toRight(RepositoryError.InvalidStoredData)
        )
      page <- pages.closePage(recruiter, requestedAt, session)
      result <-
        if (page.remains)
          // A successful page clears earlier failures so a stale count never lengthens a later backoff.
          RepositoryF
            .lift(
              MongoSessionOperations.updateOne(
                work,
                session,
                MongoFilter.eq(MongoFields.Id, recruiter.value.toString),
                MongoUpdate.combine(
                  MongoUpdate.set(MongoFields.Attempts, Int.box(0)),
                  MongoUpdate.set(MongoFields.NotBefore, now.toDate)
                )
              )
            )
            .as(RecruiterJobClosureProgress.PageClosed(page.closed))
        else
          RepositoryF
            .lift(
              MongoSessionOperations.deleteOne(work, session, MongoFilter.eq(MongoFields.Id, recruiter.value.toString))
            )
            .as(RecruiterJobClosureProgress.Completed(page.closed))
    } yield result
}

object MongoRecruiterJobClosure {
  def transactional(
      database: MongoDatabase[IO],
      client: MongoClient[IO],
      diagnostics: Diagnostics[IO],
      pageSize: JobClosurePageSize = JobClosurePageSize.Default,
      breaker: CircuitBreaker[IO] = CircuitBreaker.disabled[IO],
      retry: JobClosureRetry = JobClosureRetry.Default
  ): MongoRecruiterJobClosure =
    new MongoRecruiterJobClosure(
      database,
      MongoTransactionRunner.sessions(client, RepositoryError.Conflict, diagnostics = diagnostics, breaker = breaker),
      diagnostics,
      pageSize,
      retry,
      Clock[IO]
    )

  /** Test seam: the same closure over an explicit transaction runner. */
  private[mongo] def withRunner(
      database: MongoDatabase[IO],
      transactionRunner: MongoTransactionRunner,
      diagnostics: Diagnostics[IO],
      pageSize: JobClosurePageSize,
      retry: JobClosureRetry = JobClosureRetry.Default
  ): MongoRecruiterJobClosure =
    new MongoRecruiterJobClosure(database, transactionRunner, diagnostics, pageSize, retry, Clock[IO])
}
