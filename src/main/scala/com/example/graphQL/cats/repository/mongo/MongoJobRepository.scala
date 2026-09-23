package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.repository.protocol.*
import com.example.graphQL.cats.shared.events.OperationalEventEnvelope
import com.example.graphQL.cats.shared.pagination.JobPageRequest
import com.example.graphQL.cats.shared.search.JobSearchFilter
import com.mongodb.client.model.{Filters, Updates}
import com.mongodb.reactivestreams.client.{ClientSession, MongoClient, MongoDatabase}
import org.bson.Document
import org.bson.conversions.Bson

import java.time.Instant

final class MongoJobRepository(
    database: MongoDatabase,
    transactionRunner: MongoTransactionRunner = MongoTransactionRunner.noTransaction,
    embeddingWork: Option[MongoEmbeddingWorkRepository] = None
) extends JobRepository
    with MongoConflictWriteMapping
    with MongoOperationalEventInsertion {
  private val collection = database.getCollection("jobs")
  private val outbox = database.getCollection("event_outbox")

  override def find(id: JobId): IO[Either[RepositoryError, Option[Job]]] =
    PublisherBridge
      .first(collection.find(Filters.eq("_id", id.value.toString)))
      .map(document => MongoStoredDocumentDecoding.repository(document.traverse(MongoHiringCodecs.readJob)))
      .handleError(_ => Left(RepositoryError.Unavailable))

  override def findVersioned(id: JobId): IO[Either[RepositoryError, Option[Versioned[Job]]]] =
    PublisherBridge
      .first(collection.find(Filters.eq("_id", id.value.toString)))
      .map(document => MongoStoredDocumentDecoding.repository(document.traverse(MongoHiringCodecs.readVersionedJob)))
      .handleError(_ => Left(RepositoryError.Unavailable))

  override def findMany(ids: List[JobId]): IO[Either[RepositoryError, List[Job]]] =
    MongoKeysetPaging.byId(collection, ids.map(_.value.toString))(MongoHiringCodecs.readJob)

  override def findOpen(
      filter: JobSearchFilter,
      page: JobPageRequest
  ): IO[Either[RepositoryError, List[Job]]] =
    findMany(baseSearchFilter(filter, page), page)

  override def findAll(page: JobPageRequest): IO[Either[RepositoryError, List[Job]]] =
    findMany(baseJobFilter(List(page.status.map(status => Filters.eq("status", status.toString))), page), page)

  override def findByRecruiter(
      recruiterId: UserId,
      page: JobPageRequest
  ): IO[Either[RepositoryError, List[Job]]] =
    findMany(
      baseJobFilter(
        List(
          Some(Filters.eq("recruiterId", recruiterId.value.toString)),
          page.status.map(status => Filters.eq("status", status.toString))
        ),
        page
      ),
      page
    )

  override def create(job: Job, now: Instant): IO[Either[RepositoryError, Unit]] =
    if (embeddingWork.nonEmpty)
      transactionRunner.run(session => writeJobSession(job, now, Nil, session)).handleError(mapWrite)
    else writeJobSession(job, now, Nil, None).handleError(mapWrite)

  override def createWithEvents(
      job: Job,
      now: Instant,
      events: List[OperationalEventEnvelope],
      context: MutationWriteContext
  ): IO[Either[RepositoryError, Unit]] =
    MongoMutationWriteContext
      .run(
        context,
        transactionRunner,
        transactionRequired = embeddingWork.nonEmpty || events.nonEmpty
      )(session => writeJobSession(job, now, events, session))
      .handleError(mapWrite)

  private def writeJobSession(
      job: Job,
      now: Instant,
      events: List[OperationalEventEnvelope],
      session: Option[ClientSession]
  ): IO[Either[RepositoryError, Unit]] =
    insertOne(session, MongoHiringCodecs.job(job)).flatMap {
      case Some(_) =>
        embeddingWork
          .fold(IO.pure(Right(()): Either[RepositoryError, Unit]))(
            _.enqueue(session, EmbeddingWorkKey(EmbeddingWorkKind.Job, job.id.value.toString), now)
          )
          .flatMap {
            case Right(())   => insertOperationalEvents(outbox, session, events, now)
            case Left(error) => IO.pure(Left(error))
          }
      case None => IO.pure(Left(RepositoryError.Unavailable))
    }

  override def update(
      expected: Versioned[Job],
      replacement: Job,
      now: Instant
  ): IO[Either[RepositoryError, Versioned[Job]]] =
    if (embeddingWork.nonEmpty) {
      transactionRunner
        .run(session => writeJobUpdateSession(expected, replacement, now, Nil, session))
        .handleError(mapWrite)
    } else writeJobUpdateSession(expected, replacement, now, Nil, None).handleError(mapWrite)

  override def updateWithEvents(
      expected: Versioned[Job],
      replacement: Job,
      now: Instant,
      events: List[OperationalEventEnvelope],
      context: MutationWriteContext
  ): IO[Either[RepositoryError, Versioned[Job]]] =
    MongoMutationWriteContext
      .run(
        context,
        transactionRunner,
        transactionRequired = embeddingWork.nonEmpty || events.nonEmpty
      )(session => writeJobUpdateSession(expected, replacement, now, events, session))
      .handleError(mapWrite)

  private def writeJobUpdateSession(
      expected: Versioned[Job],
      replacement: Job,
      now: Instant,
      events: List[OperationalEventEnvelope],
      session: Option[ClientSession]
  ): IO[Either[RepositoryError, Versioned[Job]]] =
    Versioned.nextVersion(expected.version) match {
      case None              => IO.pure(Left(RepositoryError.Conflict))
      case Some(nextVersion) =>
        replaceOne(
          session,
          MongoObservedStateFilters.jobReplacement(expected),
          MongoHiringCodecs.job(replacement, nextVersion)
        ).flatMap {
          case Some(result) if result.getMatchedCount == 1L =>
            embeddingWork
              .fold(IO.pure(Right(()): Either[RepositoryError, Unit]))(
                _.enqueue(session, EmbeddingWorkKey(EmbeddingWorkKind.Job, replacement.id.value.toString), now)
              )
              .flatMap {
                case Right(()) =>
                  insertOperationalEvents(outbox, session, events, now).map(_.as(Versioned(replacement, nextVersion)))
                case Left(error) => IO.pure(Left(error))
              }
          case Some(_) => IO.pure(Left(RepositoryError.Conflict))
          case None    => IO.pure(Left(RepositoryError.Unavailable))
        }
    }

  override def updateEmbedding(id: JobId, embedding: EntityEmbedding): IO[Either[RepositoryError, Unit]] =
    findVersioned(id).flatMap {
      case Right(Some(job)) => updateEmbedding(job, embedding)
      case Right(None)      => IO.pure(Left(RepositoryError.Conflict))
      case Left(error)      => IO.pure(Left(error))
    }

  override def updateEmbedding(
      observed: Versioned[Job],
      embedding: EntityEmbedding
  ): IO[Either[RepositoryError, Unit]] = {
    val encoded = MongoHiringCodecs.embeddingDocument(embedding)
    PublisherBridge
      .first(
        collection.updateOne(
          MongoObservedStateFilters.jobEmbedding(observed),
          Updates.combine(
            Updates.set("embedding", encoded.get("embedding")),
            Updates.set("embeddingMeta", encoded.get("embeddingMeta")),
            Updates.inc("version", 1L)
          )
        )
      )
      .map {
        case Some(result) if result.getMatchedCount == 1L => Right(())
        case Some(_)                                      => Left(RepositoryError.Conflict)
        case None                                         => Left(RepositoryError.Unavailable)
      }
      .handleError(mapWrite)
  }

  private def findMany(filter: Bson, page: JobPageRequest): IO[Either[RepositoryError, List[Job]]] =
    MongoKeysetPaging.page(collection, filter, "createdAt", page.pageSize)(MongoHiringCodecs.readJob)

  private def baseSearchFilter(filter: JobSearchFilter, page: JobPageRequest): Bson =
    baseJobFilter(
      List(
        Some(Filters.eq("status", JobStatus.Open.toString)),
        filter.city.map(city => Filters.eq("location.city", city)),
        Option.when(filter.skills.nonEmpty)(Filters.all("skills", filter.skills.toList.sorted*)),
        filter.createdAfter.map(createdAfter => Filters.gte("createdAt", java.util.Date.from(createdAfter)))
      ),
      page
    )

  private def baseJobFilter(filters: List[Option[Bson]], page: JobPageRequest): Bson = {
    val cursorFilter =
      page.cursor.map(cursor => MongoKeysetPaging.beforeCursor("createdAt", cursor.createdAt, cursor.id.value.toString))
    val activeFilters = (filters :+ cursorFilter).flatten
    if (activeFilters.isEmpty) new Document() else Filters.and(activeFilters*)
  }

  private def insertOne(session: Option[ClientSession], document: Document) =
    session.fold(PublisherBridge.first(collection.insertOne(document)))(active =>
      PublisherBridge.first(collection.insertOne(active, document))
    )

  private def replaceOne(session: Option[ClientSession], filter: Bson, document: Document) =
    session.fold(PublisherBridge.first(collection.replaceOne(filter, document)))(active =>
      PublisherBridge.first(collection.replaceOne(active, filter, document))
    )
}

object MongoJobRepository {
  def standalone(database: MongoDatabase): MongoJobRepository =
    new MongoJobRepository(database)

  def transactional(
      database: MongoDatabase,
      client: MongoClient,
      embeddingWork: Option[MongoEmbeddingWorkRepository] = None
  ): MongoJobRepository =
    new MongoJobRepository(database, MongoTransactionRunner.sessions(client, RepositoryError.Conflict), embeddingWork)
}
