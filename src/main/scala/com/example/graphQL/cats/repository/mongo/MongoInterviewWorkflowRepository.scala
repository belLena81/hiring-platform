package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.service.resilience.CircuitBreaker

import com.example.graphQL.cats.shared.StableIds
import cats.data.EitherT
import cats.effect.{Clock, IO}
import cats.effect.std.UUIDGen
import mongo4cats.client.MongoClient
import mongo4cats.database.MongoDatabase
import scala.concurrent.duration.*
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, UserId}
import com.example.graphQL.cats.domain.model.{ApplicationEvent, ApplicationStatus, UserRole}
import com.example.graphQL.cats.domain.policy.ApplicationLifecycle
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.repository.mongo.MongoDocumentFields.Repository.*
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.events.OperationalEvents
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.RepositoryError
import mongo4cats.client.ClientSession
import org.bson.Document

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

  override def findRequest(
      actorId: UserId,
      requestKey: UUID,
      fingerprint: MutationReceiptFingerprint
  ): RepositoryF[IO, Option[InterviewWorkflow]] =
    requestReceipt(requestReceiptId(actorId, requestKey), None).flatMap {
      case None => none[InterviewWorkflow].pure[[A] =>> RepositoryF[IO, A]]
      case Some((id, existing)) if existing == fingerprint.value => loadWorkflow(id, None)
      case Some(_)                                               => RepositoryF.at[IO].raise(RepositoryError.Conflict)
    }

  override def hasHiringReceipt(workflowId: InterviewWorkflowId): RepositoryF[IO, Boolean] =
    RepositoryF
      .lift(MongoSessionOperations.findById(inbox, None, s"${workflowId.value}:hiring"))
      .map(_.nonEmpty)

  override def commitHiring(
      workflow: InterviewWorkflow,
      now: Instant,
      execution: Option[ClaimedInterviewWorkflowCommand] = None
  ): RepositoryF[IO, Unit] =
    MongoRepositorySupport.repositoryGuard(diagnostics, "interviewWorkflow.commitHiring") {
      transactionRunner.run { session =>
        val receiptId = s"${workflow.id.value}:hiring"
        for {
          _ <- fenceExecution(session, workflow.id, execution)
          existing <- RepositoryF.lift(MongoSessionOperations.findById(inbox, session, receiptId))
          _ <-
            if (existing.nonEmpty) ().pure[[A] =>> RepositoryF[IO, A]]
            else
              for {
                committedAt <- RepositoryF.lift(clock.realTimeInstant)
                _ <- RepositoryF
                  .at[IO]
                  .fromEither(
                    Either.cond(
                      now.isBefore(workflow.preCommitDeadline) && committedAt.isBefore(workflow.preCommitDeadline),
                      (),
                      RepositoryError.Conflict
                    )
                  )
                current <- loadWorkflow(workflow.id, session)
                _ <- RepositoryF
                  .at[IO]
                  .fromEither(
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
                _ <- RepositoryF.at[IO].fromEither(Either.cond(reserved.nonEmpty, (), RepositoryError.Conflict))
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
                transition <- RepositoryF
                  .at[IO]
                  .fromEither(
                    ApplicationLifecycle
                      .changeStatus(ApplicationStatus.Interview, workflow.initiatedBy, now, None, None)
                      .run(decoded)
                      .leftMap(_ => RepositoryError.Conflict)
                  )
                (updated, change) = transition
                _ <- touchJobOwner(session, updated.jobId.value.toString, workflow.recruiterId)
                _ <- participantsOf(workflow).distinct.traverse_(id =>
                  guardWrite(
                    users,
                    session,
                    MongoFilter.and(
                      MongoFilter.eq(MongoFields.Id, id.value.toString),
                      MongoFilter.eq(MongoFields.AccountStatus, "Active")
                    ),
                    MongoUpdate.inc(MongoFields.Version, 1L)
                  )
                )
                _ <- writeApplicationStatus(session, workflow.applicationId, change.previousStatus, updated)
                eventId = StableIds.uuid(s"${workflow.id.value}:status")
                statusEvent = ApplicationEvent(
                  ApplicationEventId(eventId),
                  updated.id,
                  Some(change.previousStatus),
                  change.newStatus,
                  change.actorId,
                  change.occurredAt,
                  change.feedback,
                  change.reason
                )
                _ <- insert(session, applicationEvents, MongoHiringCodecs.event(statusEvent))
                _ <- MongoOperationalEventInsertion.insert(
                  outbox,
                  session,
                  List(OperationalEvents.statusChanged(eventId, updated, statusEvent)),
                  now,
                  diagnostics
                )
                _ <- insert(
                  session,
                  inbox,
                  new Document(MongoFields.Id, receiptId)
                    .append(WorkflowIdField, workflow.id.value.toString)
                    .append("candidateId", workflow.candidateId.value.toString)
                    .append("recruiterId", workflow.recruiterId.value.toString)
                    .append(MongoFields.OccurredAt, now.toDate)
                )
                next <- RepositoryF
                  .at[IO]
                  .fromEither(
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
          RepositoryF
            .at[IO]
            .fromEither(
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
              case InterviewLifecycleOutcome.Applied(next)       => next.pure[[A] =>> RepositoryF[IO, A]]
              case InterviewLifecycleOutcome.Duplicate(existing) => existing.pure[[A] =>> RepositoryF[IO, A]]
              case _                                             => RepositoryF.at[IO].raise(RepositoryError.Conflict)
            }
        case None =>
          hasHiringReceipt(workflow.id).flatMap { committed =>
            RepositoryF
              .at[IO]
              .fromEither(
                InterviewWorkflowPolicy
                  .repair(workflow, expectedRevision, committed)
                  .leftMap(_ => RepositoryError.Conflict)
              )
              .flatMap { case InterviewWorkflowDecision(next, emitted) =>
                advance(next, expectedRevision, InterviewAdvanceCause.AdminRepair(requestKey, actorId), emitted, now)
                  .flatMap {
                    case InterviewWorkflowAdvanceResult.Applied             => next.pure[[A] =>> RepositoryF[IO, A]]
                    case InterviewWorkflowAdvanceResult.Duplicate(existing) => existing.pure[[A] =>> RepositoryF[IO, A]]
                    case InterviewWorkflowAdvanceResult.StaleRevision       =>
                      RepositoryF.at[IO].raise(RepositoryError.Conflict)
                  }
              }
          }
      }

  override def create(
      workflow: InterviewWorkflow,
      initialCommand: InterviewWorkflowCommand,
      requestKey: UUID,
      fingerprint: MutationReceiptFingerprint,
      createdAt: Instant
  ): RepositoryF[IO, InterviewWorkflowAdvanceResult] =
    findRequest(workflow.initiatedBy, requestKey, fingerprint).flatMap {
      case Some(existing) => InterviewWorkflowAdvanceResult.Duplicate(existing).pure[[A] =>> RepositoryF[IO, A]]
      case None           =>
        if (
          workflow.revision != 0L || workflow.phase != InterviewWorkflowPhase.ReservationPending ||
          initialCommand != InterviewWorkflow.initialCommand(workflow)
        )
          RepositoryF.at[IO].raise(RepositoryError.InvalidEvent)
        else createValid(workflow, initialCommand, requestKey, fingerprint, createdAt)
    }

  private def createValid(
      workflow: InterviewWorkflow,
      initialCommand: InterviewWorkflowCommand,
      requestKey: UUID,
      fingerprint: MutationReceiptFingerprint,
      createdAt: Instant
  ): RepositoryF[IO, InterviewWorkflowAdvanceResult] = {
    val receiptId = requestReceiptId(workflow.initiatedBy, requestKey)
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
    def duplicateOf(
        receipt: Option[(InterviewWorkflowId, String)],
        session: Option[ClientSession[IO]]
    ): Option[RepositoryF[IO, InterviewWorkflowAdvanceResult]] =
      receipt.map { case (existingWorkflowId, existingFingerprint) =>
        if (existingFingerprint != fingerprint.value) RepositoryF.at[IO].raise(RepositoryError.Conflict)
        else
          loadWorkflow(existingWorkflowId, session)
            .subflatMap(
              _.map(InterviewWorkflowAdvanceResult.Duplicate.apply).toRight(RepositoryError.InvalidStoredData)
            )
      }
    val write = transactionRunner.run { session =>
      for {
        _ <- fenceParticipants(session, workflow)
        receipt <- requestReceipt(receiptId, session)
        result <- duplicateOf(receipt, session).getOrElse(
          for {
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
            jobId <- RepositoryF.at[IO].fromEither(string(application, MongoFields.JobId))
            transactionNow <- RepositoryF.lift(clock.realTimeInstant)
            observedUpdatedAt <- RepositoryF.at[IO].fromEither(instant(application, MongoFields.UpdatedAt))
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
            _ <- insert(session, workflows, requestReceiptDocument(receiptId, workflow.id, fingerprint, createdAt))
            _ <- insert(session, commands, commandDocument(command))
          } yield InterviewWorkflowAdvanceResult.Applied
        )
      } yield result
    }
    // A concurrent identical request commits its receipt first; its stored result is this request's result.
    val resolved = write.recoverWith { case RepositoryError.Conflict =>
      requestReceipt(receiptId, None).flatMap(
        duplicateOf(_, None).getOrElse(RepositoryF.at[IO].raise(RepositoryError.Conflict))
      )
    }
    MongoRepositorySupport.repositoryGuard(diagnostics, "interviewWorkflow.create")(resolved)
  }

  override def findForActor(
      workflowId: InterviewWorkflowId,
      access: InterviewWorkflowAccess
  ): RepositoryF[IO, Option[InterviewWorkflow]] =
    access.role match {
      case UserRole.Admin => none[InterviewWorkflow].pure[[A] =>> RepositoryF[IO, A]]
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
      RepositoryF.at[IO].raise(RepositoryError.InvalidEvent)
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
                        inboxDocument(inboxId, workflow.id, inboxMessageId, workflow.revision, occurredAt)
                          .append("auditActorId", cause.auditActor.map(_.value.toString).orNull)
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
                        else ().pure[[A] =>> RepositoryF[IO, A]]
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
                        else ().pure[[A] =>> RepositoryF[IO, A]]
                    } yield InterviewWorkflowAdvanceResult.Applied
                  case _ => InterviewWorkflowAdvanceResult.StaleRevision.pure[[A] =>> RepositoryF[IO, A]]
                }
            }
          } yield result
        }
      }
    }
  }

  private def requestReceipt(
      receiptId: String,
      session: Option[ClientSession[IO]]
  ): RepositoryF[IO, Option[(InterviewWorkflowId, String)]] =
    RepositoryF
      .lift(MongoSessionOperations.findById(workflows, session, receiptId))
      .flatMap(_.traverse(document => RepositoryF.at[IO].fromEither(decodeReceipt(document))))
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
