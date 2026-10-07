package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.data.EitherT
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId}
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
import org.bson.Document
import com.example.graphQL.cats.service.read.HiringReadScope

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
        RepositoryIO
          .lift(collection.flatMap(_.find(Filters.eq(MongoFields.Id, id.value.toString)).first))
          .subflatMap(document =>
            MongoStoredDocumentDecoding.repository(document.traverse(MongoHiringCodecs.readApplication))
          )
      }(_ => Left(RepositoryError.Unavailable))

  override def findByCandidate(
      scope: HiringReadScope,
      page: ApplicationPageRequest
  ): RepositoryIO[List[Application]] = {
    import MongoAuthorizedReadQueries.*
    if (scope.role != UserRole.Candidate) RepositoryIO.fromEither(Right(Nil))
    else {
      val pipeline = List(matching(baseFilter(MongoFields.CandidateId, scope.userId.value.toString, page).bson)) ++
        actor(scope) ++ List(new Document("$sort", new Document(MongoFields.CreatedAt, -1).append(MongoFields.Id, -1)))
      documents(database, MongoCollections.Applications, pipeline, page.pageSize.value, diagnostics)
        .subflatMap(values => MongoStoredDocumentDecoding.values(values.map(MongoHiringCodecs.readApplication)))
    }
  }

  override def findByJob(
      scope: HiringReadScope,
      jobId: JobId,
      page: ApplicationPageRequest
  ): RepositoryIO[List[Application]] = {
    import MongoAuthorizedReadQueries.*
    val inner = List(
      matching(baseFilter(MongoFields.JobId, jobId.value.toString, page).bson),
      new Document("$sort", new Document(MongoFields.CreatedAt, -1).append(MongoFields.Id, -1)),
      new Document("$limit", page.pageSize.value)
    )
    val pipeline = List(matching(Filters.and(Filters.eq(MongoFields.Id, jobId.value.toString), managedJob(scope)))) ++
      actor(scope) ++ List(
        lookup(MongoCollections.Applications, MongoFields.Id, MongoFields.JobId, "selected", inner),
        unwind("selected"),
        replace("selected")
      )
    documents(database, MongoCollections.Jobs, pipeline, page.pageSize.value, diagnostics)
      .subflatMap(values => MongoStoredDocumentDecoding.values(values.map(MongoHiringCodecs.readApplication)))
  }

  override def history(
      scope: HiringReadScope,
      applicationId: ApplicationId,
      page: ApplicationEventPageRequest
  ): RepositoryIO[List[ApplicationEvent]] = {
    import MongoAuthorizedReadQueries.*
    val inner = List(
      matching(eventFilter(applicationId, page).bson),
      new Document("$sort", new Document(MongoFields.OccurredAt, -1).append(MongoFields.Id, -1)),
      new Document("$limit", page.pageSize.value)
    )
    val pipeline = List(matching(Filters.eq(MongoFields.Id, applicationId.value.toString))) ++ actor(scope) ++
      applicationAccess(scope) ++ List(
        lookup(MongoCollections.ApplicationEvents, MongoFields.Id, MongoFields.ApplicationId, "selected", inner),
        unwind("selected"),
        replace("selected")
      )
    documents(database, MongoCollections.Applications, pipeline, page.pageSize.value, diagnostics)
      .subflatMap(values => MongoStoredDocumentDecoding.values(values.map(MongoHiringCodecs.readEvent)))
  }

  override def createForOpenJob(
      observedJob: JobSubmissionSnapshot,
      application: Application,
      initialEvent: ApplicationEvent
  ): RepositoryIO[Unit] =
    if (!isConsistentSubmit(observedJob, application, initialEvent))
      EitherT.leftT[IO, Unit](RepositoryError.Conflict)
    else submitWithRetry(observedJob, application, initialEvent, remainingRetries = 2)

  def createForOpenJobWithEvents(
      observedJob: JobSubmissionSnapshot,
      application: Application,
      initialEvent: ApplicationEvent,
      operationalEvents: List[OperationalEventEnvelope]
  ): RepositoryIO[Unit] =
    if (!isConsistentSubmit(observedJob, application, initialEvent))
      EitherT.leftT[IO, Unit](RepositoryError.Conflict)
    else
      submitWithRetry(observedJob, application, initialEvent, operationalEvents, remainingRetries = 2)

  override def createForOpenJobWithEvents(
      observedJob: JobSubmissionSnapshot,
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
          RepositoryIO
            .lift(MongoMutationWriteContext.session(context))
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
        RepositoryIO
          .lift(MongoMutationWriteContext.session(context))
          .flatMap(session => updateStatusWithSession(application, event, operationalEvents, session))
      }(mapWrite)

  private def updateStatusWithSession(
      application: Application,
      event: ApplicationEvent,
      operationalEvents: List[OperationalEventEnvelope],
      session: Option[ClientSession[IO]]
  ): RepositoryIO[Unit] =
    val statusGuard = event.previousStatus
      .map(status => MongoFilter.eq(MongoFields.Status, status.toString))
      .getOrElse(MongoFilter.exists(MongoFields.Status))
    val filter = MongoFilter.and(MongoFilter.eq(MongoFields.Id, application.id.value.toString), statusGuard)
    for {
      result <- RepositoryIO.lift(
        MongoSessionOperations.replaceOne(collection, session, filter, MongoHiringCodecs.application(application))
      )
      _ <- RepositoryIO.fromEither(result match {
        case Some(value) if value.getMatchedCount == 1L => Right(())
        case Some(_)                                    => Left(RepositoryError.Conflict)
        case None                                       => Left(RepositoryError.MissingWriteResult)
      })
      _ <- MongoRepositorySupport.repositoryGuard(diagnostics, "applications.insertStatusEvent") {
        RepositoryIO
          .lift(insertApplicationEvent(events, session, event))
          .subflatMap(MongoRepositorySupport.writeResult(_).void)
      }(error => mapDuplicateAs(RepositoryError.Conflict)(error))
      _ <- insertOperationalEvents(outbox, session, operationalEvents, application.updatedAt, diagnostics)
    } yield ()

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
      observedJob: JobSubmissionSnapshot,
      application: Application,
      initialEvent: ApplicationEvent,
      remainingRetries: Int
  ): RepositoryIO[Unit] =
    submitWithRetry(observedJob, application, initialEvent, Nil, remainingRetries)

  private def submitWithRetry(
      observedJob: JobSubmissionSnapshot,
      application: Application,
      initialEvent: ApplicationEvent,
      operationalEvents: List[OperationalEventEnvelope],
      remainingRetries: Int
  ): RepositoryIO[Unit] =
    submitOnce(observedJob, application, initialEvent, operationalEvents).leftFlatMap {
      case RepositoryError.Conflict if remainingRetries > 0 =>
        currentOpenJob(observedJob.id).flatMap {
          case Some(current) =>
            submitWithRetry(current, application, initialEvent, operationalEvents, remainingRetries - 1)
          case None => RepositoryIO.fromEither(Left(RepositoryError.Conflict))
        }
      case error => RepositoryIO.fromEither(Left(error))
    }

  private def isConsistentSubmit(
      observedJob: JobSubmissionSnapshot,
      application: Application,
      initialEvent: ApplicationEvent
  ): Boolean =
    observedJob.id == application.jobId &&
      initialEvent.applicationId == application.id &&
      initialEvent.previousStatus.isEmpty &&
      initialEvent.newStatus == ApplicationStatus.Created &&
      initialEvent.actorId == application.candidateId

  private def submitOnce(
      observedJob: JobSubmissionSnapshot,
      application: Application,
      initialEvent: ApplicationEvent,
      operationalEvents: List[OperationalEventEnvelope]
  ): RepositoryIO[Unit] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "applications.submit") {
        transactionRunner
          .run(session => submitOnceWithSession(observedJob, application, initialEvent, operationalEvents, session))
      }(mapWrite)

  private def submitOnceWithSession(
      observedJob: JobSubmissionSnapshot,
      application: Application,
      initialEvent: ApplicationEvent,
      operationalEvents: List[OperationalEventEnvelope],
      session: Option[ClientSession[IO]]
  ): RepositoryIO[Unit] =
    val guardFilter = MongoFilter.and(
      MongoFilter.eq(MongoFields.Id, observedJob.id.value.toString),
      MongoFilter.eq(MongoFields.Version, observedJob.revision),
      MongoFilter.lt(MongoFields.Version, Long.MaxValue),
      MongoFilter.eq(MongoFields.Status, JobStatus.Open.toString)
    )
    val update = MongoUpdate.combine(
      MongoUpdate.set(MongoFields.UpdatedAt, Date.from(application.createdAt)),
      MongoUpdate.inc(MongoFields.Version, 1L)
    )
    val guard = MongoSessionOperations.updateOne(jobs, session, guardFilter, update)
    RepositoryIO.lift(guard).flatMap {
      case Some(result) if result.getMatchedCount == 1L =>
        insertApplicationAndEvent(session, application, initialEvent, operationalEvents)
      case Some(_) => RepositoryIO.fromEither(Left(RepositoryError.Conflict))
      case None    => RepositoryIO.fromEither(Left(RepositoryError.MissingWriteResult))
    }

  private def insertApplicationAndEvent(
      session: Option[ClientSession[IO]],
      application: Application,
      initialEvent: ApplicationEvent,
      operationalEvents: List[OperationalEventEnvelope]
  ): RepositoryIO[Unit] =
    for {
      _ <- MongoRepositorySupport.repositoryGuard(diagnostics, "applications.insert") {
        RepositoryIO
          .lift(MongoSessionOperations.insertOne(collection, session, MongoHiringCodecs.application(application)))
          .subflatMap(MongoRepositorySupport.writeResult(_).void)
      }(error => mapDuplicateAs(RepositoryError.DuplicateApplication)(error))
      _ <- MongoRepositorySupport.repositoryGuard(diagnostics, "applications.insertInitialEvent") {
        RepositoryIO
          .lift(insertApplicationEvent(events, session, initialEvent))
          .subflatMap(MongoRepositorySupport.writeResult(_).void)
      }(error => mapDuplicateAs(RepositoryError.Conflict)(error))
      _ <- insertOperationalEvents(outbox, session, operationalEvents, application.createdAt, diagnostics)
    } yield ()

  private def currentOpenJob(id: JobId): RepositoryIO[Option[JobSubmissionSnapshot]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "applications.findOpenJob") {
        RepositoryIO
          .lift(
            jobs.flatMap(
              _.find(Filters.eq(MongoFields.Id, id.value.toString))
                .projection(MongoJobSubmissionSnapshotCodec.projection)
                .first
            )
          )
          .subflatMap(document =>
            MongoStoredDocumentDecoding
              .repository(document.traverse(MongoJobSubmissionSnapshotCodec.read))
              .map(_.filter(_.status == JobStatus.Open))
          )
      }(_ => Left(RepositoryError.Unavailable))

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
      diagnostics: Diagnostics
  ): MongoApplicationRepository =
    new MongoApplicationRepository(
      database,
      MongoTransactionRunner.sessions(client, RepositoryError.DuplicateApplication, diagnostics = diagnostics),
      diagnostics
    )
}
