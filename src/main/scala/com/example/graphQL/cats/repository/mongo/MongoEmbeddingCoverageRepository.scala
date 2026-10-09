package com.example.graphQL.cats.repository.mongo

import cats.Monad
import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.{EmbeddingMeta, SearchableText}
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.port.{
  EmbeddingCoverageRepository,
  EmbeddingWorkFailure,
  EmbeddingWorkKind,
  EmbeddingWorkState,
  RepositoryError,
  RepositoryIO
}
import com.example.graphQL.cats.service.search.*
import com.example.graphQL.cats.shared.crypto.SourceHash
import com.mongodb.{ReadConcern, ReadPreference}
import mongo4cats.database.MongoDatabase
import org.bson.Document

import java.time.Instant
import java.util.Date
import scala.jdk.CollectionConverters.*

/** Read-only coverage scan. Each page is one aggregation under snapshot read concern that reads the entities and their
  * queue rows (`$lookup` by `_id`) at a single point in time, so a worker completing or an enqueue committing between
  * two reads cannot create a false orphaned gap. All reads are primary, projected (no vectors), `_id`-ordered keyset
  * pages bounded by a per-kind cap and `maxTime`. A deployment without snapshot support fails the read, which surfaces
  * as `RepositoryError.Unavailable`.
  *
  * The absence of false `NoOrphanedGap` failures rests on two invariants outside this class; breaking either one
  * reintroduces false failures:
  *   1. Enqueueing repair work is transactional with the entity write that makes the stored embedding stale
  *      (`MongoJobRepository` create/update ~343/390, `MongoUserRepository` insert/profile update ~269/388, enforced by
  *      `MongoEmbeddingWorkRepository.requiresTransaction`), so a snapshot never shows a stale entity without its row.
  *   2. The worker writes the embedding before `complete` deletes the row (`EmbeddingPipeline` `updateEmbedding`
  *      ~247/264, then `work.complete` ~150-151), so a snapshot never shows a deleted row with a still stale embedding.
  */
final class MongoEmbeddingCoverageRepository(database: MongoDatabase[IO], diagnostics: Diagnostics)
    extends EmbeddingCoverageRepository {
  import MongoEmbeddingCoverageRepository.*

  private val primary = database.withReadPreference(ReadPreference.primary())

  private def aggregate(
      collection: String,
      pipeline: List[Document],
      limits: EmbeddingCoverageLimits,
      snapshot: Boolean = false
  ): RepositoryIO[List[Document]] =
    RepositoryIO.lift(
      Mongo4catsCollections
        .documents(primary, collection)
        .map(source => if (snapshot) source.withReadConcern(ReadConcern.SNAPSHOT) else source)
        .flatMap(
          _.aggregate[Document](pipeline).maxTime(limits.maxTime).boundedStream(limits.pageSize + 1).compile.toList
        )
    )

  override def observe(request: EmbeddingCoverageScanRequest): RepositoryIO[EmbeddingCoverageObservation] =
    MongoRepositorySupport.repositoryGuard(diagnostics, "embeddingCoverage.observe") {
      for {
        jobs <- scanKind(JobScan, request, EmbeddingCoverageTally.empty)
        candidates <- scanKind(CandidateScan, request, jobs._1)
        jobQueue <- observeQueue(JobScan, request)
        candidateQueue <- observeQueue(CandidateScan, request)
      } yield EmbeddingCoverageObservation(
        candidates._1,
        List(jobs._2, candidates._2),
        EmbeddingQueueObservation(
          jobQueue.truncated || candidateQueue.truncated,
          jobQueue.stuckCount + candidateQueue.stuckCount,
          (jobQueue.oldestWaitingAvailableAt.toList ++ candidateQueue.oldestWaitingAvailableAt.toList).minOption
        )
      )
    }

  private final case class Page(after: Option[String], scanned: Int, tally: EmbeddingCoverageTally)
  private final case class Scanned(tally: EmbeddingCoverageTally, truncated: Boolean)

  private def scanKind(
      scan: CoverageKind,
      request: EmbeddingCoverageScanRequest,
      initial: EmbeddingCoverageTally
  ): RepositoryIO[(EmbeddingCoverageTally, EmbeddingCoverageKindObservation)] = {
    val limits = request.limits
    def page(state: Page): RepositoryIO[Either[Page, Scanned]] = {
      val remaining = limits.maxEntitiesPerKind - state.scanned
      val limit = math.min(limits.pageSize, remaining + 1)
      aggregate(
        scan.collection,
        entityPagePipeline(scan, state.after, limit),
        limits,
        snapshot = true
      )
        .flatMap(documents =>
          RepositoryIO.fromEither(
            documents.traverse(scan.decode).map(documents.lastOption.map(_.getString(MongoFields.Id)) -> _)
          )
        )
        .map { case (lastId, decoded) =>
          val (within, over) = decoded.flatten.splitAt(remaining)
          val tally = within.foldLeft(state.tally) { case (acc, (_, entity)) =>
            acc.add(entity, request.expectedModel, request.now)
          }
          val advanced = Page(lastId.orElse(state.after), state.scanned + within.size, tally)
          if (over.nonEmpty) Right(Scanned(tally, truncated = true))
          else if (decoded.size < limit) Right(Scanned(tally, truncated = false))
          else Left(advanced)
        }
    }
    for {
      searchable <- countEligible(scan, limits)
      scanned <- Monad[RepositoryIO].tailRecM(Page(None, 0, initial))(page)
    } yield scanned.tally -> EmbeddingCoverageKindObservation(scan.kind, searchable, scanned.truncated)
  }

  private def countEligible(scan: CoverageKind, limits: EmbeddingCoverageLimits): RepositoryIO[Long] =
    aggregate(scan.collection, countPipeline(scan), limits).flatMap(rows =>
      RepositoryIO.fromEither(
        rows.headOption.fold[Either[RepositoryError, Long]](Right(0L))(row =>
          MongoDocumentFields.toRepository(MongoDocumentFields.requiredNumberAsLong(row, "n"))
        )
      )
    )

  private def observeQueue(
      scan: CoverageKind,
      request: EmbeddingCoverageScanRequest
  ): RepositoryIO[EmbeddingQueueObservation] =
    aggregate(MongoCollections.EmbeddingWork, queuePipeline(scan, request), request.limits).flatMap(rows =>
      RepositoryIO.fromEither(rows.headOption match {
        case None      => Right(EmbeddingQueueObservation(truncated = false, 0L, None))
        case Some(row) =>
          MongoDocumentFields.toRepository(for {
            total <- MongoDocumentFields.requiredNumberAsLong(row, "total")
            stuck <- MongoDocumentFields.requiredNumberAsLong(row, "stuck")
            oldest <- MongoDocumentFields.optionalInstant(row, "oldest")
          } yield EmbeddingQueueObservation(total > request.limits.maxEntitiesPerKind.toLong, stuck, oldest))
      })
    )
}

private[mongo] final case class CoverageKind(
    kind: EmbeddingWorkKind,
    collection: String,
    eligible: Document,
    projection: Document,
    decode: Document => Either[RepositoryError, Option[(String, EmbeddingCoverageEntity)]]
) {
  val keyPrefix: String = s"${kind.toString}:"
  val keyUpperBound: String = s"${kind.toString};"
}

private[mongo] object MongoEmbeddingCoverageRepository {
  private val WorkField = "work"

  private def entity(
      kind: EmbeddingWorkKind,
      document: Document,
      base: Document => Either[RepositoryError, Option[(Option[EmbeddingMeta], String, Option[Instant])]]
  ): Either[RepositoryError, Option[(String, EmbeddingCoverageEntity)]] = {
    val source = new Document(document)
    source.remove(WorkField)
    for {
      id <- Either.catchNonFatal(document.getString(MongoFields.Id)).leftMap(_ => RepositoryError.InvalidStoredData)
      work <- queuedWork(document)
      decoded <- base(source)
    } yield decoded.map { case (stored, hash, changedAt) =>
      id -> EmbeddingCoverageEntity(kind, stored, hash, changedAt, work)
    }
  }

  val JobScan: CoverageKind = CoverageKind(
    EmbeddingWorkKind.Job,
    MongoCollections.Jobs,
    new Document(MongoFields.Status, "Open"),
    MongoSearchEligibilityCodecs.projection(MongoSearchEligibilityCodecs.jobFields),
    document =>
      entity(
        EmbeddingWorkKind.Job,
        document,
        source =>
          MongoSearchEligibilityCodecs
            .job(source)
            .map(value =>
              Some((value.metadata, SourceHash.sha256(SearchableText.job(value.job)), Some(value.job.updatedAt)))
            )
      )
  )

  /** Production candidate eligibility; recruiter search opt-in only narrows filtered queries and is not applied. */
  val CandidateScan: CoverageKind = CoverageKind(
    EmbeddingWorkKind.CandidateProfile,
    MongoCollections.Users,
    new Document(MongoFields.Role, "Candidate")
      .append(MongoFields.AccountStatus, "Active")
      .append(MongoFields.Profile, new Document("$type", "object")),
    MongoSearchEligibilityCodecs.projection(MongoSearchEligibilityCodecs.candidateFields),
    document =>
      entity(
        EmbeddingWorkKind.CandidateProfile,
        document,
        source =>
          MongoSearchEligibilityCodecs
            .candidate(source)
            .map(value =>
              value.profile.map(profile => (value.metadata, SourceHash.sha256(SearchableText.candidate(profile)), None))
            )
      )
  )

  /** One keyset page after the given `_id`: entities joined with their queue row inside a single aggregation. */
  def entityPagePipeline(scan: CoverageKind, after: Option[String], limit: Int): List[Document] = {
    val matching = new Document(scan.eligible)
    after.foreach(id => matching.append(MongoFields.Id, new Document("$gt", id)))
    List(
      new Document("$match", matching),
      new Document("$sort", new Document(MongoFields.Id, 1)),
      new Document("$limit", limit),
      new Document("$project", scan.projection),
      new Document(
        "$lookup",
        new Document("from", MongoCollections.EmbeddingWork)
          .append(
            "let",
            new Document("key", new Document("$concat", List(scan.keyPrefix, s"$$${MongoFields.Id}").asJava))
          )
          .append(
            "pipeline",
            List(
              new Document(
                "$match",
                new Document("$expr", new Document("$eq", List(s"$$${MongoFields.Id}", "$$key").asJava))
              ),
              new Document(
                "$project",
                new Document(MongoFields.State, 1)
                  .append(MongoFields.Failure, 1)
                  .append(MongoFields.AvailableAt, 1)
                  .append(MongoFields.LeaseUntil, 1)
              )
            ).asJava
          )
          .append("as", WorkField)
      )
    )
  }

  def countPipeline(scan: CoverageKind): List[Document] =
    List(new Document("$match", scan.eligible), new Document("$count", "n"))

  /** Waiting work excludes failed rows. The `_id` prefix range keeps the read on the primary-key index. */
  def queuePipeline(scan: CoverageKind, request: EmbeddingCoverageScanRequest): List[Document] = {
    val waiting = new Document("$ne", List(s"$$${MongoFields.State}", "Failed").asJava)
    val stuck = new Document(
      "$and",
      List(
        waiting,
        new Document("$lt", List(s"$$${MongoFields.AvailableAt}", Date.from(request.stuckBefore)).asJava)
      ).asJava
    )
    List(
      new Document(
        "$match",
        new Document(MongoFields.Id, new Document("$gte", scan.keyPrefix).append("$lt", scan.keyUpperBound))
      ),
      new Document("$limit", request.limits.maxEntitiesPerKind + 1),
      new Document(
        "$group",
        new Document(MongoFields.Id, null)
          .append("total", new Document("$sum", 1))
          .append("stuck", new Document("$sum", new Document("$cond", List(stuck, 1, 0).asJava)))
          .append(
            "oldest",
            new Document("$min", new Document("$cond", List(waiting, s"$$${MongoFields.AvailableAt}", null).asJava))
          )
      )
    )
  }

  private def queuedWork(document: Document): Either[RepositoryError, Option[EmbeddingQueuedWork]] =
    MongoDocumentFields
      .toRepository(MongoDocumentFields.requiredDocumentVector(document, WorkField))
      .flatMap {
        case Vector()    => Right(None)
        case Vector(row) => queuedRow(row).map(Some(_))
        case _           => Left(RepositoryError.InvalidStoredData)
      }

  private def queuedRow(row: Document): Either[RepositoryError, EmbeddingQueuedWork] =
    MongoDocumentFields
      .toRepository(for {
        state <- MongoDocumentFields.requiredEnum(row, MongoFields.State)(
          MongoDocumentFields.byName(EmbeddingWorkState.values)
        )
        failure <- MongoDocumentFields.optionalEnum(row, MongoFields.Failure)(
          MongoDocumentFields.byName(EmbeddingWorkFailure.values)
        )
        availableAt <- MongoDocumentFields.requiredInstant(row, MongoFields.AvailableAt)
        leaseUntil <- MongoDocumentFields.optionalInstant(row, MongoFields.LeaseUntil)
      } yield (state, failure, availableAt, leaseUntil))
      .filterOrElse(
        { case (state, failure, _, leaseUntil) =>
          (state != EmbeddingWorkState.Failed || failure.nonEmpty) &&
          (state != EmbeddingWorkState.Processing || leaseUntil.nonEmpty)
        },
        RepositoryError.InvalidStoredData
      )
      .map { case (state, failure, availableAt, leaseUntil) =>
        EmbeddingQueuedWork(state, failure, availableAt, leaseUntil)
      }
}
