package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.data.EitherT
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.events.OperationalEventEnvelope
import com.example.graphQL.cats.domain.pagination.*
import com.example.graphQL.cats.service.Diagnostics
import com.mongodb.{MongoCommandException, MongoWriteException}
import com.mongodb.client.model.Filters
import mongo4cats.client.{ClientSession, MongoClient}
import mongo4cats.database.MongoDatabase

import java.util.Date

final class MongoApplicationRepository private (
    database: MongoDatabase[IO],
    transactionRunner: MongoTransactionRunner,
    diagnostics: Diagnostics
) extends ApplicationRepository
    with MongoApplicationEventInsertion
    with MongoOperationalEventInsertion {
  private def collection = Mongo4catsCollections.documents(database, MongoCollections.Applications)
  private def events = Mongo4catsCollections.documents(database, MongoCollections.ApplicationEvents)
  private def jobs = Mongo4catsCollections.documents(database, MongoCollections.Jobs)
  private def outbox = Mongo4catsCollections.documents(database, MongoCollections.EventOutbox)

  override def find(id: ApplicationId): RepositoryIO[Option[Application]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "applications.find") {
        collection
          .flatMap(_.find(Filters.eq(MongoFields.Id, id.value.toString)).first)
          .map(document => MongoStoredDocumentDecoding.repository(document.traverse(MongoHiringCodecs.readApplication)))
      }(_ => Left(RepositoryError.Unavailable))

  override def findByCandidate(
      candidateId: UserId,
      page: ApplicationPageRequest
  ): RepositoryIO[List[Application]] =
    findMany(baseFilter(MongoFields.CandidateId, candidateId.value.toString, page), page)

  override def findByJob(jobId: JobId, page: ApplicationPageRequest): RepositoryIO[List[Application]] =
    findMany(baseFilter(MongoFields.JobId, jobId.value.toString, page), page)

  override def history(
      applicationId: ApplicationId,
      page: ApplicationEventPageRequest
  ): RepositoryIO[List[ApplicationEvent]] =
    MongoKeysetPaging.page(events, eventFilter(applicationId, page), MongoFields.OccurredAt, page.pageSize)(
      MongoHiringCodecs.readEvent
    )(diagnostics)

  override def createForOpenJob(
      observedJob: Versioned[Job],
      application: Application,
      initialEvent: ApplicationEvent
  ): RepositoryIO[Unit] =
    if (!isConsistentSubmit(observedJob, application, initialEvent))
      EitherT.leftT[IO, Unit](RepositoryError.Conflict)
    else RepositoryIO.fromIOEither(submitWithRetry(observedJob, application, initialEvent, remainingRetries = 2))

  def createForOpenJobWithEvents(
      observedJob: Versioned[Job],
      application: Application,
      initialEvent: ApplicationEvent,
      operationalEvents: List[OperationalEventEnvelope]
  ): RepositoryIO[Unit] =
    if (!isConsistentSubmit(observedJob, application, initialEvent))
      EitherT.leftT[IO, Unit](RepositoryError.Conflict)
    else
      RepositoryIO.fromIOEither(
        submitWithRetry(observedJob, application, initialEvent, operationalEvents, remainingRetries = 2)
      )

  override def createForOpenJobWithEvents(
      observedJob: Versioned[Job],
      application: Application,
      initialEvent: ApplicationEvent,
      operationalEvents: List[OperationalEventEnvelope],
      context: MutationWriteContext
  ): RepositoryIO[Unit] =
    if (!isConsistentSubmit(observedJob, application, initialEvent))
      EitherT.leftT[IO, Unit](RepositoryError.Conflict)
    else
      MongoRepositorySupport
        .repositoryGuard(diagnostics, "applications.createWithEvents") {
          MongoMutationWriteContext
            .session(context)
            .flatMap(session =>
              submitOnceWithSession(observedJob, application, initialEvent, operationalEvents, session)
            )
        }(mapWrite)

  override def updateStatus(application: Application, event: ApplicationEvent): RepositoryIO[Unit] =
    updateStatusWithEvents(application, event, Nil)

  def updateStatusWithEvents(
      application: Application,
      event: ApplicationEvent,
      operationalEvents: List[OperationalEventEnvelope]
  ): RepositoryIO[Unit] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "applications.updateStatus") {
        transactionRunner.run(session => updateStatusWithSession(application, event, operationalEvents, session))
      }(mapWrite)

  override def updateStatusWithEvents(
      application: Application,
      event: ApplicationEvent,
      operationalEvents: List[OperationalEventEnvelope],
      context: MutationWriteContext
  ): RepositoryIO[Unit] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "applications.updateStatusWithContext") {
        MongoMutationWriteContext
          .session(context)
          .flatMap(session => updateStatusWithSession(application, event, operationalEvents, session))
      }(mapWrite)

  private def updateStatusWithSession(
      application: Application,
      event: ApplicationEvent,
      operationalEvents: List[OperationalEventEnvelope],
      session: Option[ClientSession[IO]]
  ): IO[Either[RepositoryError, Unit]] =
    val statusGuard = event.previousStatus
      .map(status => MongoFilter.eq(MongoFields.Status, status.toString))
      .getOrElse(MongoFilter.exists(MongoFields.Status))
    val filter = MongoFilter.and(MongoFilter.eq(MongoFields.Id, application.id.value.toString), statusGuard)
    val update =
      MongoSessionOperations.replaceOne(collection, session, filter, MongoHiringCodecs.application(application))
    update.flatMap {
      case Some(result) if result.getMatchedCount == 1L =>
        MongoRepositorySupport
          .repositoryGuard(diagnostics, "applications.insertStatusEvent") {
            insertApplicationEvent(events, session, event).map {
              case Some(_) => Right(())
              case None    => Left(RepositoryError.MissingWriteResult)
            }
          }(error => mapDuplicateAs(RepositoryError.Conflict)(error))
          .value
          .flatMap {
            case Right(()) =>
              insertOperationalEvents(outbox, session, operationalEvents, application.updatedAt, diagnostics)
            case Left(error) => IO.pure(Left(error))
          }
      case Some(_) => IO.pure(Left(RepositoryError.Conflict))
      case None    => IO.pure(Left(RepositoryError.MissingWriteResult))
    }

  private def findMany(
      filter: MongoFilter,
      page: ApplicationPageRequest
  ): RepositoryIO[List[Application]] =
    MongoKeysetPaging.page(collection, filter, MongoFields.CreatedAt, page.pageSize)(MongoHiringCodecs.readApplication)(
      diagnostics
    )

  private def baseFilter(field: String, id: String, page: ApplicationPageRequest): MongoFilter = {
    val statusFilter = page.status.map(status => MongoFilter.eq(MongoFields.Status, status.toString))
    val cursorFilter =
      page.cursor.map(cursor =>
        MongoFilter.beforeCursor(MongoFields.CreatedAt, cursor.createdAt, cursor.id.value.toString)
      )
    MongoFilter.and((List(Some(MongoFilter.eq(field, id)), statusFilter, cursorFilter).flatten)*)
  }

  private def eventFilter(applicationId: ApplicationId, page: ApplicationEventPageRequest): MongoFilter = {
    val cursorFilter = page.cursor.map(cursor =>
      MongoFilter.beforeCursor(MongoFields.OccurredAt, cursor.occurredAt, cursor.id.value.toString)
    )
    MongoFilter.and(
      (List(Some(MongoFilter.eq(MongoFields.ApplicationId, applicationId.value.toString)), cursorFilter).flatten)*
    )
  }

  private def submitWithRetry(
      observedJob: Versioned[Job],
      application: Application,
      initialEvent: ApplicationEvent,
      remainingRetries: Int
  ): IO[Either[RepositoryError, Unit]] =
    submitWithRetry(observedJob, application, initialEvent, Nil, remainingRetries)

  private def submitWithRetry(
      observedJob: Versioned[Job],
      application: Application,
      initialEvent: ApplicationEvent,
      operationalEvents: List[OperationalEventEnvelope],
      remainingRetries: Int
  ): IO[Either[RepositoryError, Unit]] =
    submitOnce(observedJob, application, initialEvent, operationalEvents).flatMap {
      case Left(RepositoryError.Conflict) if remainingRetries > 0 =>
        currentOpenJob(observedJob.value.id).flatMap {
          case Right(Some(current)) =>
            submitWithRetry(current, application, initialEvent, operationalEvents, remainingRetries - 1)
          case Right(None) => IO.pure(Left(RepositoryError.Conflict))
          case Left(error) => IO.pure(Left(error))
        }
      case result => IO.pure(result)
    }

  private def isConsistentSubmit(
      observedJob: Versioned[Job],
      application: Application,
      initialEvent: ApplicationEvent
  ): Boolean =
    observedJob.value.id == application.jobId &&
      initialEvent.applicationId == application.id &&
      initialEvent.previousStatus.isEmpty &&
      initialEvent.newStatus == ApplicationStatus.Created &&
      initialEvent.actorId == application.candidateId

  private def submitOnce(
      observedJob: Versioned[Job],
      application: Application,
      initialEvent: ApplicationEvent,
      operationalEvents: List[OperationalEventEnvelope]
  ): IO[Either[RepositoryError, Unit]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "applications.submit") {
        transactionRunner
          .run(session => submitOnceWithSession(observedJob, application, initialEvent, operationalEvents, session))
      }(mapWrite)
      .value

  private def submitOnceWithSession(
      observedJob: Versioned[Job],
      application: Application,
      initialEvent: ApplicationEvent,
      operationalEvents: List[OperationalEventEnvelope],
      session: Option[ClientSession[IO]]
  ): IO[Either[RepositoryError, Unit]] =
    val guardFilter = MongoFilter.and(
      MongoFilter.eq(MongoFields.Id, observedJob.value.id.value.toString),
      MongoFilter.eq(MongoFields.Version, observedJob.version),
      MongoFilter.lt(MongoFields.Version, Long.MaxValue),
      MongoFilter.eq(MongoFields.Status, JobStatus.Open.toString)
    )
    val update = MongoUpdate.combine(
      MongoUpdate.set(MongoFields.UpdatedAt, Date.from(application.createdAt)),
      MongoUpdate.inc(MongoFields.Version, 1L)
    )
    val guard = MongoSessionOperations.updateOne(jobs, session, guardFilter, update)
    guard.flatMap {
      case Some(result) if result.getMatchedCount == 1L =>
        insertApplicationAndEvent(session, application, initialEvent, operationalEvents)
      case Some(_) => IO.pure(Left(RepositoryError.Conflict))
      case None    => IO.pure(Left(RepositoryError.MissingWriteResult))
    }

  private def insertApplicationAndEvent(
      session: Option[ClientSession[IO]],
      application: Application,
      initialEvent: ApplicationEvent,
      operationalEvents: List[OperationalEventEnvelope]
  ): IO[Either[RepositoryError, Unit]] = {
    val insertApplication =
      MongoSessionOperations.insertOne(collection, session, MongoHiringCodecs.application(application))
    val insertEvent = insertApplicationEvent(events, session, initialEvent)
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "applications.insert") {
        insertApplication.map {
          case Some(_) => Right(())
          case None    => Left(RepositoryError.MissingWriteResult)
        }
      }(error => mapDuplicateAs(RepositoryError.DuplicateApplication)(error))
      .value
      .flatMap {
        case Left(error) => IO.pure(Left(error))
        case Right(())   =>
          MongoRepositorySupport
            .repositoryGuard(diagnostics, "applications.insertInitialEvent") {
              insertEvent.map {
                case Some(_) => Right(())
                case None    => Left(RepositoryError.MissingWriteResult)
              }
            }(error => mapDuplicateAs(RepositoryError.Conflict)(error))
            .value
            .flatMap {
              case Left(error) => IO.pure(Left(error))
              case Right(())   =>
                insertOperationalEvents(outbox, session, operationalEvents, application.createdAt, diagnostics)
            }
      }
  }

  private def currentOpenJob(id: JobId): IO[Either[RepositoryError, Option[Versioned[Job]]]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "applications.findOpenJob") {
        jobs
          .flatMap(_.find(Filters.eq(MongoFields.Id, id.value.toString)).first)
          .map(document =>
            MongoStoredDocumentDecoding
              .repository(document.traverse(MongoHiringCodecs.readVersionedJob))
              .map(_.filter(_.value.status == JobStatus.Open))
          )
      }(_ => Left(RepositoryError.Unavailable))
      .value

  private def mapWrite(error: Throwable): Either[RepositoryError, Unit] =
    error match {
      case write: MongoWriteException if write.getError.getCode == 11000 => Left(RepositoryError.DuplicateApplication)
      case command: MongoCommandException if MongoTransactionRunner.isWriteConflict(command) =>
        Left(RepositoryError.Conflict)
      case _ => Left(RepositoryError.Unavailable)
    }

  private def mapDuplicateAs(error: RepositoryError)(throwable: Throwable): Either[RepositoryError, Unit] =
    throwable match {
      case write: MongoWriteException if write.getError.getCode == 11000                     => Left(error)
      case command: MongoCommandException if MongoTransactionRunner.isWriteConflict(command) =>
        Left(RepositoryError.Conflict)
      case _ => Left(RepositoryError.Unavailable)
    }
}

object MongoApplicationRepository {
  def transactional(
      database: MongoDatabase[IO],
      client: MongoClient[IO],
      diagnostics: Diagnostics = Diagnostics.noop
  ): MongoApplicationRepository =
    new MongoApplicationRepository(
      database,
      MongoTransactionRunner.sessions(client, RepositoryError.DuplicateApplication, diagnostics = diagnostics),
      diagnostics
    )
}
