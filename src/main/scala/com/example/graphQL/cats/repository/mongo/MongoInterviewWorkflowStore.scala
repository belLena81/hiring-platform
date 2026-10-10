package com.example.graphQL.cats.repository.mongo

import cats.effect.std.UUIDGen
import cats.effect.{Clock, IO}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, UserId}
import com.example.graphQL.cats.domain.model.{Application, ApplicationStatus}
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.RepositoryError
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.result.UpdateResult
import mongo4cats.client.ClientSession
import mongo4cats.database.MongoDatabase
import org.bson.Document

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

extension [A](found: RepositoryIO[Option[A]])
  /** An absent document is a lost race with another writer. */
  private[mongo] def orConflict: RepositoryIO[A] = found.subflatMap(_.toRight(RepositoryError.Conflict))

/** Collections and write fences shared by the interview workflow components. */
final class MongoInterviewWorkflowStore(
    val database: MongoDatabase[IO],
    val transactionRunner: MongoTransactionRunner,
    val diagnostics: Diagnostics,
    val completedEvidenceRetention: FiniteDuration,
    val clock: Clock[IO],
    val uuids: UUIDGen[IO]
) {
  import MongoInterviewFields.*
  import MongoInterviewWorkflowCodec.*

  private def collection(name: String) = Mongo4catsCollections.documents(database, name)

  val workflows = collection(MongoCollections.InterviewWorkflows)
  val commands = collection(MongoCollections.InterviewWorkflowCommands)
  val inbox = collection(MongoCollections.InterviewWorkflowInbox)
  val reservations = collection(MongoCollections.InterviewCalendarReservations)
  val participantLocks = collection(MongoCollections.InterviewCalendarParticipantLocks)
  val notificationReceipts = collection(MongoCollections.InterviewNotificationReceipts)
  val applications = collection(MongoCollections.Applications)
  val applicationEvents = collection(MongoCollections.ApplicationEvents)
  val jobs = collection(MongoCollections.Jobs)
  val users = collection(MongoCollections.Users)
  val outbox = collection(MongoCollections.EventOutbox)
  val subjectFences = collection(MongoCollections.OutboxSubjectFences)

  private[mongo] def guardWrite(
      collection: IO[MongoSessionOperations.Documents],
      session: Option[ClientSession[IO]],
      filter: MongoFilter,
      update: MongoUpdate
  ): RepositoryIO[Unit] =
    RepositoryIO
      .lift(MongoSessionOperations.updateOne(collection, session, filter, update))
      .subflatMap(MongoRepositorySupport.matchedOne(_))

  private[mongo] def participantsOf(workflow: InterviewWorkflow): List[UserId] =
    List(workflow.candidateId, workflow.recruiterId, workflow.initiatedBy)

  /** Serializes against account deletion and publisher registration for every person the workflow involves. */
  private[mongo] def fenceParticipants(
      session: Option[ClientSession[IO]],
      workflow: InterviewWorkflow,
      extra: List[UserId] = Nil
  ): RepositoryIO[Unit] = fenceSubjects(session, participantsOf(workflow) ++ extra)

  private[mongo] def fenceSubjects(session: Option[ClientSession[IO]], subjects: List[UserId]): RepositoryIO[Unit] =
    subjects.distinct
      .sortBy(_.value.toString)
      .traverse_(id =>
        guardWrite(
          users,
          session,
          MongoFilter.and(
            MongoFilter.eq(MongoFields.Id, id.value.toString),
            MongoFilter.eq(MongoFields.AccountStatus, "Active")
          ),
          MongoUpdate.inc(MongoFields.Version, 1L)
        ) *>
          RepositoryIO
            .lift(
              MongoSessionOperations.updateOne(
                subjectFences,
                session,
                MongoFilter.eq(MongoFields.Id, id.value.toString),
                MongoUpdate.inc(MongoFields.FencingVersion, 1L),
                new UpdateOptions().upsert(true)
              )
            )
            .void
      )

  private[mongo] def fenceExecution(
      session: Option[ClientSession[IO]],
      workflowId: InterviewWorkflowId,
      execution: Option[ClaimedInterviewWorkflowCommand]
  ): RepositoryIO[Unit] = execution match {
    case None                                                 => ().pure[RepositoryIO]
    case Some(claim) if claim.record.workflowId != workflowId => RepositoryIO.raise(RepositoryError.Conflict)
    case Some(claim)                                          =>
      RepositoryIO
        .lift(clock.realTimeInstant)
        .flatMap(now =>
          guardWrite(
            commands,
            session,
            MongoFilter.and(
              MongoFilter.eq(MongoFields.Id, commandId(workflowId, claim.record.stepId)),
              MongoFilter.eq(CommandStateField, InterviewWorkflowCommandState.Executing.toString),
              MongoFilter.eq(Lease.owner, claim.owner),
              MongoFilter.eq(Lease.token, claim.fencingToken.toString),
              MongoFilter.gt(Lease.until, now.toDate)
            ),
            MongoUpdate.inc("effectFence", 1L)
          )
        )
  }

  /** Moves the workflow from `expectedRevision` to `next`; extra filters and updates carry the caller's own guards. */
  private[mongo] def reviseWorkflow(
      session: Option[ClientSession[IO]],
      expectedRevision: Long,
      next: InterviewWorkflow,
      extraFilters: List[MongoFilter] = Nil,
      extraUpdates: List[MongoUpdate] = Nil
  ): RepositoryIO[UpdateResult] =
    RepositoryIO.lift(
      MongoSessionOperations.updateOne(
        workflows,
        session,
        MongoFilter.and(
          (MongoFilter.eq(MongoFields.Id, next.id.value.toString) ::
            MongoFilter.eq(DocumentTypeField, "workflow") ::
            MongoFilter.eq(RevisionField, expectedRevision) :: extraFilters)*
        ),
        MongoUpdate.combine(
          (MongoUpdate.set(RevisionField, next.revision) :: MongoUpdate.set("phase", next.phase.toString) ::
            extraUpdates)*
        )
      )
    )

  /** Records one row per emitted command; `shape` gives each its state, availability and occurrence time. */
  private[mongo] def insertCommands[C <: InterviewCommand](
      session: Option[ClientSession[IO]],
      workflowId: InterviewWorkflowId,
      revision: Long,
      emitted: List[C]
  )(shape: C => (InterviewWorkflowCommandState, Instant, Instant)): RepositoryIO[Unit] =
    emitted.zipWithIndex.traverse_ { case (command, ordinal) =>
      val (state, availableAt, occurredAt) = shape(command)
      insert(
        session,
        commands,
        commandDocument(
          InterviewWorkflowCommandRecord(
            workflowId,
            stepId(workflowId, revision, ordinal),
            revision,
            command,
            state,
            0,
            availableAt,
            occurredAt
          )
        )
      )
    }

  /** Moves the workflow's provider fence so concurrent provider effects and phase changes serialize. */
  private[mongo] def bumpProviderFence(
      session: Option[ClientSession[IO]],
      workflowId: InterviewWorkflowId,
      phase: InterviewWorkflowPhase
  ): RepositoryIO[Unit] =
    guardWrite(
      workflows,
      session,
      MongoFilter.and(
        MongoFilter.eq(MongoFields.Id, workflowId.value.toString),
        MongoFilter.eq("phase", phase.toString)
      ),
      MongoUpdate.inc("providerFence", 1L)
    )

  /** Bumps the owning recruiter's job revision, which fails when the job is not theirs. */
  private[mongo] def touchJobOwner(
      session: Option[ClientSession[IO]],
      jobId: String,
      recruiterId: UserId
  ): RepositoryIO[Unit] =
    guardWrite(
      jobs,
      session,
      MongoFilter.and(
        MongoFilter.eq(MongoFields.Id, jobId),
        MongoFilter.eq(MongoFields.RecruiterId, recruiterId.value.toString)
      ),
      MongoUpdate.inc(MongoFields.Version, 1L)
    )

  private[mongo] def writeApplicationStatus(
      session: Option[ClientSession[IO]],
      applicationId: ApplicationId,
      previous: ApplicationStatus,
      updated: Application
  ): RepositoryIO[Unit] =
    guardWrite(
      applications,
      session,
      MongoFilter.and(
        MongoFilter.eq(MongoFields.Id, applicationId.value.toString),
        MongoFilter.eq(MongoFields.Status, previous.toString)
      ),
      MongoUpdate.combine(
        MongoUpdate.set(MongoFields.Status, updated.status.toString),
        MongoUpdate.set(MongoFields.UpdatedAt, updated.updatedAt.toDate)
      )
    )

  private[mongo] def readApplication(document: Document): RepositoryIO[Application] =
    RepositoryIO.fromEither(MongoStoredDocumentDecoding.repository(MongoHiringCodecs.readApplication(document)))

  private def retention(
      session: Option[ClientSession[IO]],
      workflow: InterviewWorkflow,
      update: MongoUpdate
  ): RepositoryIO[Unit] =
    MongoInterviewFields
      .retentionTargets(workflow.id)
      .traverse_((name, related) =>
        RepositoryIO.lift(MongoSessionOperations.updateMany(collection(name), session, related, update)).void
      )

  private[mongo] def releaseRetention(
      session: Option[ClientSession[IO]],
      workflow: InterviewWorkflow
  ): RepositoryIO[Unit] =
    retention(session, workflow, MongoUpdate.unset(MongoFields.RetentionExpiresAt))

  private[mongo] def retainCompleted(
      session: Option[ClientSession[IO]],
      workflow: InterviewWorkflow,
      now: Instant
  ): RepositoryIO[Unit] =
    // One expiry for all evidence: it outlives the interview, so a far-future interview stays cancellable.
    retention(
      session,
      workflow,
      MongoUpdate.set(
        MongoFields.RetentionExpiresAt,
        (if (workflow.interval.endsAt.isAfter(now)) workflow.interval.endsAt else now)
          .plusMillis(completedEvidenceRetention.toMillis)
          .toDate
      )
    )

  private[mongo] def mapProvider[A](result: RepositoryIO[A]): InterviewProviderIO[A] =
    result.leftMap {
      case RepositoryError.Conflict | RepositoryError.DuplicateApplication => InterviewProviderError.Conflict
      case _                                                               => InterviewProviderError.Unavailable
    }

  private[mongo] def insert(
      session: Option[ClientSession[IO]],
      collection: IO[MongoSessionOperations.Documents],
      document: Document
  ): RepositoryIO[Unit] =
    RepositoryIO.lift(MongoSessionOperations.insertOne(collection, session, document)).void

  /** The workflow with this identity that also satisfies `visibleTo`, the acting role's ownership predicate. */
  private[mongo] def loadWorkflow(
      id: InterviewWorkflowId,
      session: Option[ClientSession[IO]],
      visibleTo: List[MongoFilter] = Nil
  ): RepositoryIO[Option[InterviewWorkflow]] =
    RepositoryIO
      .lift(
        MongoSessionOperations.findOne(
          workflows,
          session,
          MongoFilter.and(
            (MongoFilter.eq(MongoFields.Id, id.value.toString) :: MongoFilter.eq(DocumentTypeField, "workflow") ::
              visibleTo)*
          )
        )
      )
      .flatMap(_.traverse(document => RepositoryIO.fromEither(decodeWorkflow(document))))
}
