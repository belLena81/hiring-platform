package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.repository.mongo.MongoF.asRepository
import com.example.graphQL.cats.service.resilience.CircuitBreaker

import cats.data.EitherT
import cats.effect.{Clock, IO}
import cats.effect.std.UUIDGen
import mongo4cats.client.MongoClient
import mongo4cats.database.MongoDatabase
import scala.concurrent.duration.*
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, UserId}
import com.example.graphQL.cats.domain.model.{ApplicationEvent, ApplicationStatus, UserRole}
import com.example.graphQL.cats.domain.policy.ApplicationLifecycle
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.repository.mongo.MongoDocumentFields.Repository.*
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.events.OperationalEvents
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.RepositoryError

import java.time.Instant
import java.util.UUID
import scala.jdk.CollectionConverters.*

/** Entry point of the interview workflow: creation, hiring commit, repair and the generic advance path. */
final class MongoInterviewWorkflowRepository(
    store: MongoInterviewWorkflowStore,
    lifecycle: MongoInterviewLifecycleStore,
    commandQueue: MongoInterviewCommandQueue,
    calendar: MongoInterviewCalendarLedger,
    notifications: MongoInterviewNotificationLedger
) extends InterviewWorkflowRepository[IO],
      InterviewCalendarLedger[IO],
      InterviewNotificationReceiptLedger[IO] {
  import MongoInterviewFields.*
  import MongoInterviewWorkflowCodec.*
  import store.*

  export commandQueue.{
    attemptCount,
    authorizePublication,
    claimDueCommands,
    claimExecution,
    deferExpiry,
    findCommand,
    findNotificationRepairs,
    markPublished,
    quarantine,
    recordResult,
    renewPublication,
    repairNotifications,
    requireRepair,
    retry,
    settleNotificationResult
  }
  export lifecycle.{applyLifecycle, approveRescheduledInterval, hasRescheduleApproval}
  export calendar.{cancel, release, reserveIfAvailable}
  export calendar.find
  export notifications.{deliverOnce, find}

  /** The hiring commit is its status write: the deterministic `application_events` row exists exactly when it
    * committed.
    */
  override def isHiringCommitted(workflowId: InterviewWorkflowId): RepositoryF[IO, Boolean] =
    store.isHiringCommitted(None, workflowId)

  override def commitHiring(
      workflow: InterviewWorkflow,
      now: Instant,
      execution: Option[ClaimedInterviewWorkflowCommand] = None
  ): RepositoryF[IO, Unit] =
    MongoRepositorySupport.repositoryGuard(diagnostics, "interviewWorkflow.commitHiring") {
      transactionRunner.run { session =>
        for {
          _ <- fenceExecution(session, workflow.id, execution)
          committed <- store.isHiringCommitted(session, workflow.id)
          _ <-
            if (committed) ().asRepository
            else
              for {
                committedAt <- RepositoryF.lift(clock.realTimeInstant)
                _ <- MongoF.fromEither(
                  Either.cond(
                    now.isBefore(workflow.preCommitDeadline) && committedAt.isBefore(workflow.preCommitDeadline),
                    (),
                    RepositoryError.Conflict
                  )
                )
                current <- loadWorkflow(workflow.id, session)
                _ <- MongoF.fromEither(
                  Either.cond(
                    current.contains(workflow) && workflow.phase == InterviewWorkflowPhase.StatusCommitPending,
                    (),
                    RepositoryError.Conflict
                  )
                )
                reserved <- RepositoryF.lift(
                  MongoSessionOperations.findOne(
                    reservations,
                    session,
                    MongoFilter.and(
                      reservationIdentity(workflow.id, InterviewWorkflow.reservationKey(workflow.id, 0)),
                      MongoFilter.exists(ReleasedAtField, false)
                    )
                  )
                )
                _ <- MongoF.fromEither(Either.cond(reserved.nonEmpty, (), RepositoryError.Conflict))
                application <- RepositoryF
                  .lift(
                    MongoSessionOperations.findOne(
                      applications,
                      session,
                      MongoFilter.and(
                        MongoFilter.eq(MongoFields.Id, workflow.applicationId.value.toString),
                        MongoFilter.eq(MongoFields.CandidateId, workflow.candidateId.value.toString),
                        MongoFilter.eq(MongoFields.Status, ApplicationStatus.Accepted.toString)
                      )
                    )
                  )
                  .orConflict
                decoded <- readApplication(application)
                transition <- MongoF.fromEither(
                  ApplicationLifecycle
                    .changeStatus(
                      ApplicationStatus.Interview,
                      workflow.initiatedBy,
                      workflow.initiatedByRole,
                      now,
                      None,
                      None
                    )
                    .run(decoded)
                    .leftMap(_ => RepositoryError.Conflict)
                )
                (updated, change) = transition
                _ <- touchJobOwner(session, updated.jobId.value.toString, workflow.recruiterId)
                _ <- fenceParticipants(session, workflow)
                eventId = hiringStatusEventId(workflow.id)
                statusEvent = ApplicationEvent(
                  ApplicationEventId(eventId),
                  updated.id,
                  Some(change.previousStatus),
                  change.newStatus,
                  change.actorId,
                  change.occurredAt,
                  change.feedback,
                  change.reason,
                  change.actorRole
                )
                _ <- statusWrite(
                  session,
                  updated,
                  statusEvent,
                  List(OperationalEvents.statusChanged(eventId, updated, statusEvent))
                )
                next <- MongoF.fromEither(
                  InterviewWorkflow
                    .decide(workflow, workflow.revision, InterviewWorkflowEvent.StatusCommitted)
                    .leftMap(_ => RepositoryError.Conflict)
                )
                _ <- reviseWorkflow(session, workflow.revision, next.workflow).subflatMap(
                  MongoRepositorySupport.matchedOne(_)
                )
                _ <- insertCommands(session, workflow.id, next.workflow.revision, next.commands)(_ =>
                  (InterviewWorkflowCommandState.Pending, now, now)
                )
              } yield ()
        } yield ()
      }
    }

  override def repair(
      workflow: InterviewWorkflow,
      expectedRevision: Long,
      requestKey: UUID,
      now: Instant,
      actorId: UserId
  ): RepositoryF[IO, InterviewWorkflow] =
    RepositoryF
      .lift(MongoSessionOperations.findById(inbox, None, s"${workflow.id.value}:repair:$requestKey"))
      .flatMap {
        case Some(receipt) =>
          // A repeated request returns the workflow as it is now, not the failed state the caller still holds.
          MongoF.fromEither(
            int64(receipt, RevisionField)
              .flatMap(revision => Either.cond(revision == expectedRevision + 1L, (), RepositoryError.Conflict))
          ) *> loadWorkflow(workflow.id, None).orConflict
        case None if workflow.repairOrigin.nonEmpty =>
          // A cancel or reschedule step exhausted its retries: resume that step by lookup, keeping the audit trail.
          lifecycle
            .applyLifecycle(
              workflow.id,
              expectedRevision,
              InterviewLifecycleEvent.Repair,
              InterviewLifecycleOrigin.Internal(InterviewAdvanceCause.AdminRepair(requestKey, actorId)),
              now,
              None
            )
            .flatMap {
              case InterviewLifecycleOutcome.Applied(next)       => next.asRepository
              case InterviewLifecycleOutcome.Duplicate(existing) => existing.asRepository
              case _                                             => MongoF.raise(RepositoryError.Conflict)
            }
        case None =>
          isHiringCommitted(workflow.id).flatMap { committed =>
            MongoF
              .fromEither(
                InterviewWorkflowPolicy
                  .repair(workflow, expectedRevision, committed)
                  .leftMap(_ => RepositoryError.Conflict)
              )
              .flatMap { case InterviewWorkflowDecision(next, emitted) =>
                advance(next, expectedRevision, InterviewAdvanceCause.AdminRepair(requestKey, actorId), emitted, now)
                  .flatMap {
                    case InterviewWorkflowAdvanceResult.Applied             => next.asRepository
                    case InterviewWorkflowAdvanceResult.Duplicate(existing) => existing.asRepository
                    case InterviewWorkflowAdvanceResult.StaleRevision       =>
                      MongoF.raise(RepositoryError.Conflict)
                  }
              }
          }
      }

  override def create(
      workflow: InterviewWorkflow,
      initialCommand: InterviewWorkflowCommand,
      createdAt: Instant,
      context: MutationWriteContext
  ): RepositoryF[IO, Unit] =
    if (
      workflow.revision != 0L || workflow.phase != InterviewWorkflowPhase.ReservationPending ||
      initialCommand != InterviewWorkflow.initialCommand(workflow)
    )
      MongoF.raise(RepositoryError.InvalidEvent)
    else {
      val command = InterviewWorkflowCommandRecord(
        workflow.id,
        stepId(workflow.id, workflow.revision, 0),
        workflow.revision,
        initialCommand,
        InterviewWorkflowCommandState.Pending,
        publicationAttempts = 0,
        availableAt = createdAt,
        occurredAt = createdAt
      )
      MongoRepositorySupport.repositoryGuard(diagnostics, "interviewWorkflow.create")(
        MongoMutationWriteContext.run(context, transactionRunner, transactionRequired = true) { session =>
          for {
            _ <- fenceParticipants(session, workflow)
            application <- RepositoryF
              .lift(
                MongoSessionOperations.findOne(
                  applications,
                  session,
                  MongoFilter.and(
                    MongoFilter.eq(MongoFields.Id, workflow.applicationId.value.toString),
                    MongoFilter.eq(MongoFields.CandidateId, workflow.candidateId.value.toString),
                    MongoFilter.eq(MongoFields.Status, ApplicationStatus.Accepted.toString)
                  )
                )
              )
              .orConflict
            jobId <- MongoF.fromEither(string(application, MongoFields.JobId))
            transactionNow <- RepositoryF.lift(clock.realTimeInstant)
            observedUpdatedAt <- MongoF.fromEither(instant(application, MongoFields.UpdatedAt))
            acceptanceUpdatedAt =
              if (transactionNow.toEpochMilli > observedUpdatedAt.toEpochMilli) transactionNow
              else observedUpdatedAt.plusMillis(1L)
            _ <- guardWrite(
              applications,
              session,
              MongoFilter.and(
                MongoFilter.eq(MongoFields.Id, workflow.applicationId.value.toString),
                MongoFilter.eq(MongoFields.Status, ApplicationStatus.Accepted.toString)
              ),
              MongoUpdate.set(MongoFields.UpdatedAt, acceptanceUpdatedAt.toDate)
            )
            _ <- touchJobOwner(session, jobId, workflow.recruiterId)
            _ <- insert(session, workflows, workflowDocument(workflow))
            _ <- insert(session, commands, commandDocument(command))
          } yield ()
        }
      )(MongoErrors.duplicateAsConflict)
    }

  override def findForActor(
      workflowId: InterviewWorkflowId,
      access: InterviewWorkflowAccess
  ): RepositoryF[IO, Option[InterviewWorkflow]] =
    access.role match {
      case UserRole.Admin => none[InterviewWorkflow].asRepository
      case role           =>
        val ownerField = if (role == UserRole.Candidate) "candidateId" else "recruiterId"
        MongoRepositorySupport.repositoryGuard(diagnostics, "interviewWorkflow.findForActor")(
          loadWorkflow(workflowId, None, List(MongoFilter.eq(ownerField, access.actorId.value.toString)))
        )
    }

  override def findForAdmin(workflowId: InterviewWorkflowId): RepositoryF[IO, Option[InterviewWorkflow]] =
    MongoRepositorySupport.repositoryGuard(diagnostics, "interviewWorkflow.findForAdmin")(
      loadWorkflow(workflowId, None)
    )

  override def findByApplication(
      applicationId: ApplicationId,
      access: InterviewWorkflowAccess
  ): RepositoryF[IO, Option[InterviewWorkflow]] = {
    val participant = access.role match {
      case UserRole.Admin     => Nil
      case UserRole.Candidate => List(MongoFilter.eq("candidateId", access.actorId.value.toString))
      case UserRole.Recruiter => List(MongoFilter.eq("recruiterId", access.actorId.value.toString))
    }
    MongoRepositorySupport.repositoryGuard(diagnostics, "interviewWorkflow.findByApplication")(
      store.loadWorkflowByApplication(applicationId, participant)
    )
  }

  override def advance(
      workflow: InterviewWorkflow,
      expectedRevision: Long,
      cause: InterviewAdvanceCause,
      emittedCommands: List[InterviewWorkflowCommand],
      occurredAt: Instant,
      availableAt: Option[Instant] = None
  ): RepositoryF[IO, InterviewWorkflowAdvanceResult] = {
    val inboxMessageId = cause.receiptIdentity
    if (
      inboxMessageId.isEmpty || inboxMessageId.length > 256 || expectedRevision == Long.MaxValue ||
      workflow.revision != expectedRevision + 1L
    )
      MongoF.raise(RepositoryError.InvalidEvent)
    else {
      val inboxId = s"${workflow.id.value}:$inboxMessageId"
      MongoRepositorySupport.repositoryGuard(diagnostics, "interviewWorkflow.advance") {
        transactionRunner.run { session =>
          for {
            _ <- fenceParticipants(session, workflow)
            duplicate <- RepositoryF.lift(MongoSessionOperations.findById(inbox, session, inboxId))
            result <- duplicate match {
              case Some(_) =>
                loadWorkflow(workflow.id, session).map(
                  _.fold[InterviewWorkflowAdvanceResult](InterviewWorkflowAdvanceResult.StaleRevision)(
                    InterviewWorkflowAdvanceResult.Duplicate.apply
                  )
                )
              case None =>
                reviseWorkflow(
                  session,
                  expectedRevision,
                  workflow,
                  extraUpdates = List(
                    MongoUpdate.set("notified", workflow.notified.toList.map(_.toString).sorted.asJava),
                    if (cause.resetsAttempts) MongoUpdate.set("attemptEpochRevision", workflow.revision)
                    else MongoUpdate.inc("attemptEpochRevision", 0L)
                  )
                ).flatMap {
                  case update if update.getMatchedCount == 1L =>
                    for {
                      _ <- insert(
                        session,
                        inbox,
                        cause.auditActor.fold(
                          inboxDocument(inboxId, workflow.id, inboxMessageId, workflow.revision, occurredAt)
                        )(actor =>
                          inboxDocument(inboxId, workflow.id, inboxMessageId, workflow.revision, occurredAt)
                            .append("auditActorId", actor.value.toString)
                        )
                      )
                      _ <-
                        if (cause.resetsAttempts)
                          RepositoryF
                            .lift(
                              MongoSessionOperations.updateMany(
                                commands,
                                session,
                                MongoFilter.and(
                                  MongoFilter.eq(WorkflowIdField, workflow.id.value.toString),
                                  MongoFilter.lte(RevisionField, expectedRevision)
                                ),
                                MongoUpdate.combine(
                                  MongoUpdate.set(CommandStateField, InterviewWorkflowCommandState.Superseded.toString),
                                  Lease.release
                                )
                              )
                            )
                            .void
                        else ().asRepository
                      _ <- insertCommands(session, workflow.id, workflow.revision, emittedCommands)(command =>
                        (
                          command match {
                            case InterviewWorkflowCommand.RequireRepair(_) =>
                              InterviewWorkflowCommandState.RepairRequired
                            case _ => InterviewWorkflowCommandState.Pending
                          },
                          availableAt.getOrElse(occurredAt),
                          occurredAt
                        )
                      )
                      _ <-
                        if (workflow.phase == InterviewWorkflowPhase.Completed)
                          retainCompleted(session, workflow, occurredAt)
                        else ().asRepository
                    } yield InterviewWorkflowAdvanceResult.Applied
                  case _ => InterviewWorkflowAdvanceResult.StaleRevision.asRepository
                }
            }
          } yield result
        }
      }
    }
  }
}

object MongoInterviewWorkflowRepository {
  def apply(
      database: MongoDatabase[IO],
      transactionRunner: MongoTransactionRunner,
      diagnostics: Diagnostics[IO],
      completedEvidenceRetention: FiniteDuration = 8.days,
      clock: Clock[IO] = Clock[IO],
      uuids: UUIDGen[IO] = UUIDGen[IO]
  ): MongoInterviewWorkflowRepository = {
    val store =
      new MongoInterviewWorkflowStore(
        database,
        transactionRunner,
        diagnostics,
        completedEvidenceRetention,
        clock,
        uuids
      )
    val lifecycle = new MongoInterviewLifecycleStore(store)
    new MongoInterviewWorkflowRepository(
      store,
      lifecycle,
      new MongoInterviewCommandQueue(store, lifecycle),
      new MongoInterviewCalendarLedger(store),
      new MongoInterviewNotificationLedger(store)
    )
  }

  def live(
      database: MongoDatabase[IO],
      client: MongoClient[IO],
      diagnostics: Diagnostics[IO],
      completedEvidenceRetention: FiniteDuration = 8.days,
      breaker: CircuitBreaker[IO] = CircuitBreaker.disabled[IO]
  ): MongoInterviewWorkflowRepository =
    apply(
      database,
      MongoTransactionRunner
        .sessions(
          client,
          RepositoryError.Conflict,
          diagnostics = diagnostics,
          transientExhaustionError = RepositoryError.Unavailable,
          breaker = breaker
        ),
      diagnostics,
      completedEvidenceRetention
    )
}
