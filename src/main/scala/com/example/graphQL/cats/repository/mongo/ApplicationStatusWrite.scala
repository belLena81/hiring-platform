package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.{Application, ApplicationEvent, ApplicationStatus}
import com.example.graphQL.cats.service.events.OperationalEventEnvelope
import com.example.graphQL.cats.service.port.RepositoryF
import com.example.graphQL.cats.service.{Diagnostics, RepositoryError}
import mongo4cats.client.ClientSession
import mongo4cats.database.MongoDatabase

/** The status write: the one session-scoped sequence that moves an application to its next status, used by the API
  * status change, the interview hiring commit and the interview cancellation.
  *
  *   1. `$set status, updatedAt` on the application, guarded by the previous status and by the stored owner facts
  *      (`jobId`, `candidateId`), so nothing else in the document is rewritten and a stale or forged write matches
  *      nothing and is a `Conflict`;
  *   2. the append-only `application_events` row;
  *   3. the outbox rows.
  *
  * The caller supplies the transaction session; all three steps commit or abort together, so `applications.status`
  * equals the `newStatus` of the latest event of that application.
  */
private[mongo] final class ApplicationStatusWrite(database: MongoDatabase[IO], diagnostics: Diagnostics[IO]) {
  private def applications = Mongo4catsCollections.documents(database, MongoCollections.Applications)
  private def events = Mongo4catsCollections.documents(database, MongoCollections.ApplicationEvents)
  private def outbox = Mongo4catsCollections.documents(database, MongoCollections.EventOutbox)

  def apply(
      transaction: Option[ClientSession[IO]],
      updated: Application,
      event: ApplicationEvent,
      operationalEvents: List[OperationalEventEnvelope]
  ): RepositoryF[IO, Unit] =
    // The three steps must commit together: without a transaction nothing is written.
    transaction.fold[RepositoryF[IO, Unit]](MongoF.raise(RepositoryError.Unavailable))(active =>
      write(Some(active), updated, event, operationalEvents)
    )

  private def write(
      session: Option[ClientSession[IO]],
      updated: Application,
      event: ApplicationEvent,
      operationalEvents: List[OperationalEventEnvelope]
  ): RepositoryF[IO, Unit] =
    for {
      previous <- MongoF.fromEither(
        Either
          .cond(event.applicationId == updated.id && event.newStatus == updated.status, (), RepositoryError.Conflict)
          .flatMap(_ => event.previousStatus.toRight(RepositoryError.Conflict))
      )
      result <- RepositoryF.lift(
        MongoSessionOperations.updateOne(applications, session, guard(updated, previous), set(updated))
      )
      _ <- MongoF.fromEither(MongoRepositorySupport.matchedOne(result))
      _ <- MongoRepositorySupport.repositoryGuard(diagnostics, "applications.insertStatusEvent") {
        RepositoryF.lift(MongoApplicationEventInsertion.insert(events, session, event)).void
      }(MongoErrors.duplicateAs(RepositoryError.Conflict))
      _ <- MongoOperationalEventInsertion.insert(outbox, session, operationalEvents, updated.updatedAt, diagnostics)
    } yield ()

  private def guard(updated: Application, previous: ApplicationStatus) =
    MongoFilter.and(
      MongoFilter.eq(MongoFields.Id, updated.id.value.toString),
      MongoFilter.eq(MongoFields.JobId, updated.jobId.value.toString),
      MongoFilter.eq(MongoFields.CandidateId, updated.candidateId.value.toString),
      MongoFilter.eq(MongoFields.Status, previous.toString)
    )

  private def set(updated: Application) =
    MongoUpdate.combine(
      MongoUpdate.set(MongoFields.Status, updated.status.toString),
      MongoUpdate.set(MongoFields.UpdatedAt, updated.updatedAt.toDate)
    )
}
