package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.service.resilience.CircuitBreaker

import cats.effect.IO
import cats.data.EitherT
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId}
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.events.OperationalEventEnvelope
import com.example.graphQL.cats.domain.pagination.*
import com.example.graphQL.cats.service.Diagnostics
import com.mongodb.client.model.Filters
import mongo4cats.client.{ClientSession, MongoClient}
import mongo4cats.database.MongoDatabase

import org.bson.Document
import com.example.graphQL.cats.service.read.HiringReadScope

final class MongoApplicationRepository private (
    database: MongoDatabase[IO],
    transactionRunner: MongoTransactionRunner,
    diagnostics: Diagnostics[IO]
) extends ApplicationRepository[IO] {
  private def collection = Mongo4catsCollections.documents(database, MongoCollections.Applications)
  private def events = Mongo4catsCollections.documents(database, MongoCollections.ApplicationEvents)
  private def jobs = Mongo4catsCollections.documents(database, MongoCollections.Jobs)
  private def outbox = Mongo4catsCollections.documents(database, MongoCollections.EventOutbox)

  override def find(id: ApplicationId): RepositoryF[IO, Option[Application]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "applications.find") {
        RepositoryF
          .lift(MongoSessionOperations.findById(collection, None, id.value.toString))
          .subflatMap(document =>
            MongoStoredDocumentDecoding.repository(document.traverse(MongoHiringCodecs.readApplication))
          )
      }

  override def findByCandidate(
      scope: HiringReadScope,
      page: ApplicationPageRequest
  ): RepositoryF[IO, List[Application]] = {
    import MongoAuthorizedReadQueries.*
    if (scope.role != UserRole.Candidate) RepositoryF.at[IO].pure(Nil)
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
  ): RepositoryF[IO, List[Application]] = {
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
  ): RepositoryF[IO, List[ApplicationEvent]] = {
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

  override def createForOpenJobWithEvents(
      observedJob: JobSubmissionSnapshot,
      application: Application,
      initialEvent: ApplicationEvent,
      operationalEvents: List[OperationalEventEnvelope],
      context: MutationWriteContext
  ): RepositoryF[IO, Unit] =
    if (!isConsistentSubmit(observedJob, application, initialEvent))
      EitherT.leftT[IO, Unit](RepositoryError.Conflict)
    else
      MongoRepositorySupport
        .repositoryGuard(diagnostics, "applications.createWithEvents") {
          MongoMutationWriteContext.run(context, transactionRunner, transactionRequired = true)(session =>
            submitOnceWithSession(observedJob, application, initialEvent, operationalEvents, session)
          )
        }(MongoErrors.duplicateAs(RepositoryError.DuplicateApplication))

  override def updateStatusWithEvents(
      application: Application,
      event: ApplicationEvent,
      operationalEvents: List[OperationalEventEnvelope],
      context: MutationWriteContext
  ): RepositoryF[IO, Unit] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "applications.updateStatusWithContext") {
        MongoMutationWriteContext.run(context, transactionRunner, transactionRequired = true)(session =>
          updateStatusWithSession(application, event, operationalEvents, session)
        )
      }(MongoErrors.duplicateAs(RepositoryError.DuplicateApplication))

  private def updateStatusWithSession(
      application: Application,
      event: ApplicationEvent,
      operationalEvents: List[OperationalEventEnvelope],
      session: Option[ClientSession[IO]]
  ): RepositoryF[IO, Unit] =
    val statusGuard = event.previousStatus
      .map(status => MongoFilter.eq(MongoFields.Status, status.toString))
      .getOrElse(MongoFilter.exists(MongoFields.Status))
    val filter = MongoFilter.and(MongoFilter.eq(MongoFields.Id, application.id.value.toString), statusGuard)
    for {
      result <- RepositoryF.lift(
        MongoSessionOperations.replaceOne(collection, session, filter, MongoHiringCodecs.application(application))
      )
      _ <- RepositoryF.at[IO].fromEither(MongoRepositorySupport.matchedOne(result))
      _ <- MongoRepositorySupport.repositoryGuard(diagnostics, "applications.insertStatusEvent") {
        RepositoryF.lift(MongoApplicationEventInsertion.insert(events, session, event)).void
      }(MongoErrors.duplicateAs(RepositoryError.Conflict))
      _ <- MongoOperationalEventInsertion.insert(outbox, session, operationalEvents, application.updatedAt, diagnostics)
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

  private def submitOnceWithSession(
      observedJob: JobSubmissionSnapshot,
      application: Application,
      initialEvent: ApplicationEvent,
      operationalEvents: List[OperationalEventEnvelope],
      session: Option[ClientSession[IO]]
  ): RepositoryF[IO, Unit] =
    val guardFilter = MongoFilter.and(
      MongoFilter.eq(MongoFields.Id, observedJob.id.value.toString),
      MongoFilter.eq(MongoFields.Version, observedJob.revision),
      MongoFilter.lt(MongoFields.Version, Long.MaxValue),
      MongoFilter.eq(MongoFields.Status, JobStatus.Open.toString)
    )
    val update = MongoUpdate.combine(
      MongoUpdate.set(MongoFields.UpdatedAt, application.createdAt.toDate),
      MongoUpdate.inc(MongoFields.Version, 1L)
    )
    val guard = MongoSessionOperations.updateOne(jobs, session, guardFilter, update)
    RepositoryF.lift(guard).flatMap { result =>
      RepositoryF.at[IO].fromEither(MongoRepositorySupport.matchedOne(result)) *>
        insertApplicationAndEvent(session, application, initialEvent, operationalEvents)
    }

  private def insertApplicationAndEvent(
      session: Option[ClientSession[IO]],
      application: Application,
      initialEvent: ApplicationEvent,
      operationalEvents: List[OperationalEventEnvelope]
  ): RepositoryF[IO, Unit] =
    for {
      _ <- MongoRepositorySupport.repositoryGuard(diagnostics, "applications.insert") {
        RepositoryF
          .lift(MongoSessionOperations.insertOne(collection, session, MongoHiringCodecs.application(application)))
          .void
      }(MongoErrors.duplicateAs(RepositoryError.DuplicateApplication))
      _ <- MongoRepositorySupport.repositoryGuard(diagnostics, "applications.insertInitialEvent") {
        RepositoryF.lift(MongoApplicationEventInsertion.insert(events, session, initialEvent)).void
      }(MongoErrors.duplicateAs(RepositoryError.Conflict))
      _ <- MongoOperationalEventInsertion.insert(outbox, session, operationalEvents, application.createdAt, diagnostics)
    } yield ()

}

object MongoApplicationRepository {
  def transactional(
      database: MongoDatabase[IO],
      client: MongoClient[IO],
      diagnostics: Diagnostics[IO],
      breaker: CircuitBreaker[IO] = CircuitBreaker.disabled[IO]
  ): MongoApplicationRepository =
    new MongoApplicationRepository(
      database,
      MongoTransactionRunner
        .sessions(client, RepositoryError.DuplicateApplication, diagnostics = diagnostics, breaker = breaker),
      diagnostics
    )
}
