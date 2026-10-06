package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.data.EitherT
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.events.OperationalEventEnvelope
import com.example.graphQL.cats.domain.pagination.JobPageRequest
import com.example.graphQL.cats.service.search.JobSearchFilter
import com.example.graphQL.cats.service.Diagnostics
import com.mongodb.client.model.Filters
import mongo4cats.client.{ClientSession, MongoClient}
import mongo4cats.database.MongoDatabase

import com.example.graphQL.cats.service.read.*
import org.bson.Document
import java.time.Instant
import scala.util.chaining.*

final class MongoJobRepository(
    database: MongoDatabase[IO],
    transactionRunner: MongoTransactionRunner,
    embeddingWork: MongoEmbeddingWorkEnqueuer,
    diagnostics: Diagnostics
) extends JobRepository
    with MongoConflictWriteMapping
    with MongoOperationalEventInsertion {
  private def collection = Mongo4catsCollections.documents(database, MongoCollections.Jobs)
  private def outbox = Mongo4catsCollections.documents(database, MongoCollections.EventOutbox)

  override def find(id: JobId): RepositoryIO[Option[Job]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "MongoJobRepository.find")(
        RepositoryIO
          .lift(collection.flatMap(_.find(Filters.eq(MongoFields.Id, id.value.toString)).first))
          .subflatMap(document => MongoStoredDocumentDecoding.repository(document.traverse(MongoHiringCodecs.readJob)))
      )(_ => Left(RepositoryError.Unavailable))

  override def findVersioned(id: JobId): RepositoryIO[Option[Versioned[Job]]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "MongoJobRepository.findVersioned")(
        RepositoryIO
          .lift(collection.flatMap(_.find(Filters.eq(MongoFields.Id, id.value.toString)).first))
          .subflatMap(document =>
            MongoStoredDocumentDecoding.repository(document.traverse(MongoHiringCodecs.readVersionedJob))
          )
      )(_ => Left(RepositoryError.Unavailable))

  override def findMany(ids: List[JobId]): RepositoryIO[List[Job]] =
    MongoKeysetPaging.byId(collection, ids.map(_.value.toString))(MongoHiringCodecs.readJob)(diagnostics)

  override def relatedJobs(scope: HiringReadScope, keys: List[JobRelationKey]): RepositoryIO[List[RelatedJob]] = {
    import MongoAuthorizedReadQueries.*
    val requested = keys.distinct
    if (requested.isEmpty) RepositoryIO.fromEither(Right(Nil))
    else {
      val pipeline = List(
        matching(
          Filters.or(
            requested.map(key =>
              Filters.and(
                Filters.eq(MongoFields.Id, key.applicationId.value.toString),
                Filters.eq(MongoFields.JobId, key.jobId.value.toString)
              )
            )*
          )
        )
      ) ++
        actor(scope) ++ applicationAccess(scope) ++ List(
          lookup(MongoCollections.Jobs, MongoFields.JobId, MongoFields.Id, "related"),
          unwind("related")
        )
      documents(database, MongoCollections.Applications, pipeline, requested.size, diagnostics).subflatMap(values =>
        MongoStoredDocumentDecoding.values(
          values.flatMap(document =>
            requested
              .find(key =>
                key.applicationId.value.toString == document.getString(
                  MongoFields.Id
                ) && key.jobId.value.toString == document.getString(MongoFields.JobId)
              )
              .map(key =>
                MongoHiringCodecs.readJob(document.get("related", classOf[Document])).map(job => RelatedJob(key, job))
              )
          )
        )
      )
    }
  }

  override def findOpen(
      filter: JobSearchFilter,
      page: JobPageRequest
  ): RepositoryIO[List[Job]] =
    findMany(baseSearchFilter(filter, page), page)

  override def findAll(page: JobPageRequest): RepositoryIO[List[Job]] =
    findMany(
      baseJobFilter(List(page.status.map(status => MongoFilter.eq(MongoFields.Status, status.toString))), page),
      page
    )

  override def findByRecruiter(
      recruiterId: UserId,
      page: JobPageRequest
  ): RepositoryIO[List[Job]] =
    findMany(
      baseJobFilter(
        List(
          Some(MongoFilter.eq(MongoFields.RecruiterId, recruiterId.value.toString)),
          page.status.map(status => MongoFilter.eq(MongoFields.Status, status.toString))
        ),
        page
      ),
      page
    )

  override def createWithEvents(
      job: Job,
      now: Instant,
      events: List[OperationalEventEnvelope],
      context: MutationWriteContext
  ): RepositoryIO[Unit] =
    MongoMutationWriteContext
      .run(
        context,
        transactionRunner,
        transactionRequired = embeddingWork.requiresTransaction || events.nonEmpty
      )(session => writeJobSession(job, now, events, session))
      .pipe(effect =>
        MongoRepositorySupport
          .repositoryGuard(diagnostics, "MongoJobRepository.createWithEvents")(effect)(mapWrite)
      )

  private def writeJobSession(
      job: Job,
      now: Instant,
      events: List[OperationalEventEnvelope],
      session: Option[ClientSession[IO]]
  ): RepositoryIO[Unit] =
    for {
      result <- RepositoryIO.lift(MongoSessionOperations.insertOne(collection, session, MongoHiringCodecs.job(job)))
      _ <- RepositoryIO.fromEither(MongoRepositorySupport.writeResult(result))
      _ <- embeddingWork.enqueue(session, EmbeddingWorkKey(EmbeddingWorkKind.Job, job.id.value.toString), now)
      _ <- insertOperationalEvents(outbox, session, events, now, diagnostics)
    } yield ()

  override def updateWithEvents(
      expected: Versioned[Job],
      replacement: Job,
      now: Instant,
      events: List[OperationalEventEnvelope],
      context: MutationWriteContext
  ): RepositoryIO[Versioned[Job]] =
    MongoMutationWriteContext
      .run(
        context,
        transactionRunner,
        transactionRequired = embeddingWork.requiresTransaction || events.nonEmpty
      )(session => writeJobUpdateSession(expected, replacement, now, events, session))
      .pipe(effect =>
        MongoRepositorySupport
          .repositoryGuard(diagnostics, "MongoJobRepository.updateWithEvents")(effect)(mapWrite)
      )

  private def writeJobUpdateSession(
      expected: Versioned[Job],
      replacement: Job,
      now: Instant,
      events: List[OperationalEventEnvelope],
      session: Option[ClientSession[IO]]
  ): RepositoryIO[Versioned[Job]] =
    for {
      nextVersion <- RepositoryIO.fromEither(Versioned.nextVersion(expected.version).toRight(RepositoryError.Conflict))
      result <- RepositoryIO.lift(
        MongoSessionOperations.replaceOne(
          collection,
          session,
          MongoFilter.and(
            MongoFilter.eq(MongoFields.Id, expected.value.id.value.toString),
            MongoFilter.eq(MongoFields.Version, expected.version)
          ),
          MongoHiringCodecs.job(replacement, nextVersion)
        )
      )
      _ <- RepositoryIO.fromEither(result match {
        case Some(value) if value.getMatchedCount == 1L => Right(())
        case Some(_)                                    => Left(RepositoryError.Conflict)
        case None                                       => Left(RepositoryError.MissingWriteResult)
      })
      _ <- embeddingWork.enqueue(session, EmbeddingWorkKey(EmbeddingWorkKind.Job, replacement.id.value.toString), now)
      _ <- insertOperationalEvents(outbox, session, events, now, diagnostics)
    } yield Versioned(replacement, nextVersion)

  override def updateEmbedding(id: JobId, embedding: EntityEmbedding): RepositoryIO[Unit] =
    findVersioned(id).flatMap {
      case Some(job) => updateEmbedding(job, embedding)
      case None      => EitherT.leftT[IO, Unit](RepositoryError.Conflict)
    }

  override def updateEmbedding(
      observed: Versioned[Job],
      embedding: EntityEmbedding
  ): RepositoryIO[Unit] = {
    val encoded = MongoHiringCodecs.embeddingDocument(embedding)
    RepositoryIO
      .lift(
        MongoSessionOperations
          .updateOne(
            collection,
            None,
            MongoFilter.and(
              MongoFilter.eq(MongoFields.Id, observed.value.id.value.toString),
              MongoFilter.eq(MongoFields.Version, observed.version),
              MongoFilter.lt(MongoFields.Version, Long.MaxValue)
            ),
            MongoUpdate.combine(
              MongoUpdate.set(MongoFields.Embedding, encoded.get(MongoFields.Embedding)),
              MongoUpdate.set(MongoFields.EmbeddingMeta, encoded.get(MongoFields.EmbeddingMeta)),
              MongoUpdate.inc(MongoFields.Version, 1L)
            )
          )
      )
      .subflatMap {
        case Some(result) if result.getMatchedCount == 1L => Right(())
        case Some(_)                                      => Left(RepositoryError.Conflict)
        case None                                         => Left(RepositoryError.MissingWriteResult)
      }
      .pipe(effect =>
        MongoRepositorySupport
          .repositoryGuard(diagnostics, "MongoJobRepository.updateEmbedding")(effect)(mapWrite)
      )
  }

  private def findMany(filter: MongoFilter, page: JobPageRequest): RepositoryIO[List[Job]] =
    MongoKeysetPaging.page(collection, filter, MongoFields.CreatedAt, page.pageSize)(MongoHiringCodecs.readJob)(
      diagnostics
    )

  private def baseSearchFilter(filter: JobSearchFilter, page: JobPageRequest): MongoFilter =
    baseJobFilter(
      List(
        Some(MongoFilter.eq(MongoFields.Status, JobStatus.Open.toString)),
        filter.city.map(city => MongoFilter.eq(s"${MongoFields.Location}.${MongoFields.City}", city)),
        Option.when(filter.skills.nonEmpty)(MongoFilter.all(MongoFields.Skills, filter.skills.toList.sorted)),
        filter.createdAfter.map(createdAfter =>
          MongoFilter.gte(MongoFields.CreatedAt, java.util.Date.from(createdAfter))
        )
      ),
      page
    )

  private def baseJobFilter(filters: List[Option[MongoFilter]], page: JobPageRequest): MongoFilter = {
    val cursorFilter =
      page.cursor.map(cursor =>
        MongoFilter.beforeCursor(MongoFields.CreatedAt, cursor.createdAt, cursor.id.value.toString)
      )
    val activeFilters = (filters :+ cursorFilter).flatten
    if (activeFilters.isEmpty) MongoFilter.and() else MongoFilter.and(activeFilters*)
  }
}

object MongoJobRepository {
  def transactional(
      database: MongoDatabase[IO],
      client: MongoClient[IO],
      embeddingWork: MongoEmbeddingWorkEnqueuer,
      diagnostics: Diagnostics
  ): MongoJobRepository =
    new MongoJobRepository(
      database,
      MongoTransactionRunner.sessions(client, RepositoryError.Conflict, diagnostics = diagnostics),
      embeddingWork,
      diagnostics
    )
}
