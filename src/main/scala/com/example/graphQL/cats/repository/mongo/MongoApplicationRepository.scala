package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.repository.protocol.*
import com.example.graphQL.cats.shared.events.OperationalEventEnvelope
import com.example.graphQL.cats.shared.pagination.*
import com.mongodb.{MongoCommandException, MongoWriteException}
import com.mongodb.client.model.{Filters, Updates}
import com.mongodb.reactivestreams.client.{ClientSession, MongoClient, MongoDatabase}
import org.bson.conversions.Bson

import java.util.Date

final class MongoApplicationRepository private (
    database: MongoDatabase,
    transactionRunner: MongoTransactionRunner
) extends ApplicationRepository
    with MongoApplicationEventInsertion
    with MongoOperationalEventInsertion {
  private val collection = database.getCollection("applications")
  private val events = database.getCollection("application_events")
  private val jobs = database.getCollection("jobs")
  private val outbox = database.getCollection("event_outbox")

  override def find(id: ApplicationId): IO[Either[RepositoryError, Option[Application]]] =
    PublisherBridge
      .first(collection.find(Filters.eq("_id", id.value.toString)))
      .map(document => MongoStoredDocumentDecoding.repository(document.traverse(MongoHiringCodecs.readApplication)))
      .handleError(_ => Left(RepositoryError.Unavailable))

  override def findByCandidate(
      candidateId: UserId,
      page: ApplicationPageRequest
  ): IO[Either[RepositoryError, List[Application]]] =
    findMany(baseFilter("candidateId", candidateId.value.toString, page), page)

  override def findByJob(jobId: JobId, page: ApplicationPageRequest): IO[Either[RepositoryError, List[Application]]] =
    findMany(baseFilter("jobId", jobId.value.toString, page), page)

  override def history(
      applicationId: ApplicationId,
      page: ApplicationEventPageRequest
  ): IO[Either[RepositoryError, List[ApplicationEvent]]] =
    MongoKeysetPaging.page(events, eventFilter(applicationId, page), "occurredAt", page.pageSize)(
      MongoHiringCodecs.readEvent
    )

  override def createForOpenJob(
      observedJob: Job,
      application: Application,
      initialEvent: ApplicationEvent
  ): IO[Either[RepositoryError, Unit]] =
    if (!isConsistentSubmit(observedJob, application, initialEvent)) IO.pure(Left(RepositoryError.Conflict))
    else submitWithRetry(observedJob, application, initialEvent, remainingRetries = 2)

  def createForOpenJobWithEvents(
      observedJob: Job,
      application: Application,
      initialEvent: ApplicationEvent,
      operationalEvents: List[OperationalEventEnvelope]
  ): IO[Either[RepositoryError, Unit]] =
    if (!isConsistentSubmit(observedJob, application, initialEvent)) IO.pure(Left(RepositoryError.Conflict))
    else submitWithRetry(observedJob, application, initialEvent, operationalEvents, remainingRetries = 2)

  override def createForOpenJobWithEvents(
      observedJob: Job,
      application: Application,
      initialEvent: ApplicationEvent,
      operationalEvents: List[OperationalEventEnvelope],
      context: MutationWriteContext
  ): IO[Either[RepositoryError, Unit]] =
    if (!isConsistentSubmit(observedJob, application, initialEvent)) IO.pure(Left(RepositoryError.Conflict))
    else
      submitOnceWithSession(
        observedJob,
        application,
        initialEvent,
        operationalEvents,
        MongoMutationWriteContext.session(context)
      ).handleError(mapWrite)

  override def updateStatus(application: Application, event: ApplicationEvent): IO[Either[RepositoryError, Unit]] =
    updateStatusWithEvents(application, event, Nil)

  def updateStatusWithEvents(
      application: Application,
      event: ApplicationEvent,
      operationalEvents: List[OperationalEventEnvelope]
  ): IO[Either[RepositoryError, Unit]] =
    transactionRunner
      .run(session => updateStatusWithSession(application, event, operationalEvents, session))
      .handleError(mapWrite)

  override def updateStatusWithEvents(
      application: Application,
      event: ApplicationEvent,
      operationalEvents: List[OperationalEventEnvelope],
      context: MutationWriteContext
  ): IO[Either[RepositoryError, Unit]] =
    updateStatusWithSession(application, event, operationalEvents, MongoMutationWriteContext.session(context))
      .handleError(mapWrite)

  private def updateStatusWithSession(
      application: Application,
      event: ApplicationEvent,
      operationalEvents: List[OperationalEventEnvelope],
      session: Option[ClientSession]
  ): IO[Either[RepositoryError, Unit]] =
    val statusGuard =
      event.previousStatus.map(status => Filters.eq("status", status.toString)).getOrElse(Filters.exists("status"))
    val filter = Filters.and(Filters.eq("_id", application.id.value.toString), statusGuard)
    val update = session.fold(
      PublisherBridge.first(collection.replaceOne(filter, MongoHiringCodecs.application(application)))
    ) { active =>
      PublisherBridge.first(collection.replaceOne(active, filter, MongoHiringCodecs.application(application)))
    }
    update.flatMap {
      case Some(result) if result.getMatchedCount == 1L =>
        insertApplicationEvent(events, session, event).attempt.flatMap {
          case Right(_)    => insertOperationalEvents(outbox, session, operationalEvents, application.updatedAt)
          case Left(error) => IO.pure(mapDuplicateAs(RepositoryError.Conflict)(error))
        }
      case Some(_) => IO.pure(Left(RepositoryError.Conflict))
      case None    => IO.pure(Left(RepositoryError.Unavailable))
    }

  private def findMany(filter: Bson, page: ApplicationPageRequest): IO[Either[RepositoryError, List[Application]]] =
    MongoKeysetPaging.page(collection, filter, "createdAt", page.pageSize)(MongoHiringCodecs.readApplication)

  private def baseFilter(field: String, id: String, page: ApplicationPageRequest): Bson = {
    val statusFilter = page.status.map(status => Filters.eq("status", status.toString))
    val cursorFilter =
      page.cursor.map(cursor => MongoKeysetPaging.beforeCursor("createdAt", cursor.createdAt, cursor.id.value.toString))
    MongoKeysetPaging.filter(List(Some(Filters.eq(field, id)), statusFilter, cursorFilter))
  }

  private def eventFilter(applicationId: ApplicationId, page: ApplicationEventPageRequest): Bson = {
    val cursorFilter = page.cursor.map(cursor =>
      MongoKeysetPaging.beforeCursor("occurredAt", cursor.occurredAt, cursor.id.value.toString)
    )
    MongoKeysetPaging.filter(List(Some(Filters.eq("applicationId", applicationId.value.toString)), cursorFilter))
  }

  private def submitWithRetry(
      observedJob: Job,
      application: Application,
      initialEvent: ApplicationEvent,
      remainingRetries: Int
  ): IO[Either[RepositoryError, Unit]] =
    submitWithRetry(observedJob, application, initialEvent, Nil, remainingRetries)

  private def submitWithRetry(
      observedJob: Job,
      application: Application,
      initialEvent: ApplicationEvent,
      operationalEvents: List[OperationalEventEnvelope],
      remainingRetries: Int
  ): IO[Either[RepositoryError, Unit]] =
    submitOnce(observedJob, application, initialEvent, operationalEvents).flatMap {
      case Left(RepositoryError.Conflict) if remainingRetries > 0 =>
        currentOpenJob(observedJob.id).flatMap {
          case Right(Some(current)) =>
            submitWithRetry(current, application, initialEvent, operationalEvents, remainingRetries - 1)
          case Right(None) => IO.pure(Left(RepositoryError.Conflict))
          case Left(error) => IO.pure(Left(error))
        }
      case result => IO.pure(result)
    }

  private def isConsistentSubmit(observedJob: Job, application: Application, initialEvent: ApplicationEvent): Boolean =
    observedJob.id == application.jobId &&
      initialEvent.applicationId == application.id &&
      initialEvent.previousStatus.isEmpty &&
      initialEvent.newStatus == ApplicationStatus.Created &&
      initialEvent.actorId == application.candidateId

  private def submitOnce(
      observedJob: Job,
      application: Application,
      initialEvent: ApplicationEvent,
      operationalEvents: List[OperationalEventEnvelope]
  ): IO[Either[RepositoryError, Unit]] =
    transactionRunner
      .run(session => submitOnceWithSession(observedJob, application, initialEvent, operationalEvents, session))
      .handleError(mapWrite)

  private def submitOnceWithSession(
      observedJob: Job,
      application: Application,
      initialEvent: ApplicationEvent,
      operationalEvents: List[OperationalEventEnvelope],
      session: Option[ClientSession]
  ): IO[Either[RepositoryError, Unit]] =
    val guardFilter = Filters.and(
      Filters.eq("_id", observedJob.id.value.toString),
      Filters.eq("status", JobStatus.Open.toString)
    )
    val guard = session.fold(
      PublisherBridge.first(jobs.updateOne(guardFilter, Updates.set("updatedAt", Date.from(application.createdAt))))
    ) { active =>
      PublisherBridge.first(
        jobs.updateOne(active, guardFilter, Updates.set("updatedAt", Date.from(application.createdAt)))
      )
    }
    guard.flatMap {
      case Some(result) if result.getMatchedCount == 1L =>
        insertApplicationAndEvent(session, application, initialEvent, operationalEvents)
      case Some(_) => IO.pure(Left(RepositoryError.Conflict))
      case None    => IO.pure(Left(RepositoryError.Unavailable))
    }

  private def insertApplicationAndEvent(
      session: Option[ClientSession],
      application: Application,
      initialEvent: ApplicationEvent,
      operationalEvents: List[OperationalEventEnvelope]
  ): IO[Either[RepositoryError, Unit]] = {
    val insertApplication = session.fold(
      PublisherBridge.first(collection.insertOne(MongoHiringCodecs.application(application)))
    ) { active =>
      PublisherBridge.first(collection.insertOne(active, MongoHiringCodecs.application(application)))
    }
    val insertEvent = insertApplicationEvent(events, session, initialEvent)
    insertApplication.attempt.flatMap {
      case Left(error) => IO.pure(mapDuplicateAs(RepositoryError.DuplicateApplication)(error))
      case Right(_)    =>
        insertEvent.attempt.flatMap {
          case Right(_)    => insertOperationalEvents(outbox, session, operationalEvents, application.createdAt)
          case Left(error) => IO.pure(mapDuplicateAs(RepositoryError.Conflict)(error))
        }
    }
  }

  private def currentOpenJob(id: JobId): IO[Either[RepositoryError, Option[Job]]] =
    PublisherBridge
      .first(jobs.find(Filters.eq("_id", id.value.toString)))
      .map(document =>
        MongoStoredDocumentDecoding
          .repository(document.traverse(MongoHiringCodecs.readJob))
          .map(_.filter(_.status == JobStatus.Open))
      )
      .handleError(_ => Left(RepositoryError.Unavailable))

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
  def standalone(database: MongoDatabase): MongoApplicationRepository =
    new MongoApplicationRepository(database, MongoTransactionRunner.noTransaction)

  def transactional(database: MongoDatabase, client: MongoClient): MongoApplicationRepository =
    new MongoApplicationRepository(
      database,
      MongoTransactionRunner.sessions(client, RepositoryError.DuplicateApplication)
    )
}
