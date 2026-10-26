package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.infrastructure.telemetry.{BacklogSnapshot, BacklogSource}
import com.example.graphQL.cats.service.port.EmbeddingWorkState
import com.mongodb.client.model.{CountOptions, Projections, Sorts}
import java.time.Instant
import java.util.concurrent.TimeUnit
import mongo4cats.database.MongoDatabase
import scala.concurrent.duration.FiniteDuration

/** Read-only backlog sampling of durable queues for metrics.
  *
  * Pending means claimable now, by each queue's own claim predicate: due `Ready`/`Retry` (embedding) or `Retryable`
  * (outbox) work, plus in-progress work whose lease has expired (for example after a worker crash). Work under a live
  * lease is being handled and is not pending. Count and oldest age use the same set.
  *
  * Every read is bounded: the count is capped at [[MaxCounted]] (a value equal to the cap means "at least that many"),
  * each age read is `limit(1)` of one projected timestamp, and every read has a server-side time limit. No vectors,
  * content or payloads are read.
  */
object MongoBacklogSources {
  val MaxCounted: Int = 10000

  /** One bounded oldest-row read over a part of the claimable set. */
  private final case class AgeRead(filter: Instant => MongoFilter, timestampField: String)

  private val ByAvailability = Sorts.ascending(MongoFields.AvailableAt)

  private val EmbeddingReady = List(EmbeddingWorkState.Ready, EmbeddingWorkState.Retry).map(_.toString)
  private val EmbeddingProcessing = EmbeddingWorkState.Processing.toString

  private[mongo] def embeddingClaimable(now: Instant): MongoFilter =
    MongoLeaseQueue.claimable(
      MongoLeaseQueue.readyDue(EmbeddingReady, now),
      MongoFilter.eq(MongoFields.State, EmbeddingProcessing),
      now
    )

  private[mongo] def outboxClaimable(now: Instant): MongoFilter =
    MongoOperationalEventOutboxRepository.Claimable.filter(now)

  /** Embedding age runs from when the oldest claimable row became available; a retry resets `availableAt`. */
  def embeddingWork(database: MongoDatabase[IO], cap: Int = MaxCounted): BacklogSource =
    queue(
      database,
      MongoCollections.EmbeddingWork,
      cap,
      embeddingClaimable,
      List(
        AgeRead(MongoLeaseQueue.readyDue(EmbeddingReady, _), MongoFields.AvailableAt),
        AgeRead(MongoLeaseQueue.leaseExpired(EmbeddingProcessing, _), MongoFields.AvailableAt)
      )
    )

  /** Outbox age runs from when the oldest claimable event occurred, matching the readiness measurement. */
  def operationalEventOutbox(database: MongoDatabase[IO], cap: Int = MaxCounted): BacklogSource =
    queue(
      database,
      MongoCollections.EventOutbox,
      cap,
      outboxClaimable,
      List(
        AgeRead(MongoOperationalEventOutboxRepository.Claimable.retryableDue, MongoFields.OccurredAt),
        AgeRead(MongoOperationalEventOutboxRepository.Claimable.expiredInFlight, MongoFields.OccurredAt)
      )
    )

  private[mongo] def countOptions(cap: Int): CountOptions =
    new CountOptions().limit(cap).maxTime(MongoPendingWorkAge.MaxTime.toMillis, TimeUnit.MILLISECONDS)

  private final class QueueSource(
      database: MongoDatabase[IO],
      collectionName: String,
      cap: Int,
      claimable: Instant => MongoFilter,
      reads: List[AgeRead]
  ) extends BacklogSource {
    private val collection = Mongo4catsCollections.documents(database, collectionName)

    override def sample(now: Instant): IO[BacklogSnapshot] =
      for {
        count <- collection.flatMap(_.count(claimable(now).bson, countOptions(cap)))
        // The oldest claimable row is the oldest of each part's oldest row.
        ages <- reads.traverse(read =>
          MongoPendingWorkAge.oldest(
            collection,
            Some(read.filter(now)),
            ByAvailability,
            Projections.include(read.timestampField),
            read.timestampField,
            now
          )
        )
      } yield BacklogSnapshot(count, ages.flatten.maxOption: Option[FiniteDuration])
  }

  private def queue(
      database: MongoDatabase[IO],
      collectionName: String,
      cap: Int,
      claimable: Instant => MongoFilter,
      reads: List[AgeRead]
  ): BacklogSource = new QueueSource(database, collectionName, cap, claimable, reads)
}
