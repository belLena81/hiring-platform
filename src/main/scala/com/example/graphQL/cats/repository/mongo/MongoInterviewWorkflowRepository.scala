package com.example.graphQL.cats.repository.mongo

import cats.data.EitherT
import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.model.{ApplicationStatus, UserRole, ApplicationEvent}
import com.example.graphQL.cats.domain.model.Identifiers.ApplicationEventId
import com.example.graphQL.cats.domain.policy.ApplicationLifecycle
import com.example.graphQL.cats.service.events.OperationalEvents
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.application.{
  InterviewExecutionPolicy,
  InterviewExecutionAdmission,
  InterviewExecutionLease,
  InterviewExecutionLeaseDisposition,
  InterviewExecutionBudgetDisposition
}
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.port.*
import com.mongodb.client.model.{FindOneAndUpdateOptions, ReturnDocument, Sorts, UpdateOptions}
import mongo4cats.client.{ClientSession, MongoClient}
import mongo4cats.database.MongoDatabase
import org.bson.Document

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.{Date, UUID}
import scala.jdk.CollectionConverters.*
import scala.concurrent.duration.*

/** Mongo persistence for the interview workflow and its durable ledger-backed providers.
  *
  * Collection/index creation is intentionally owned by hiring setup and migrations. Before wiring this adapter, setup
  * must create the collections named below and install the migration/index catalog described by the workflow
  * specification. Mongo's intrinsic `_id` uniqueness protects request receipts, inbox messages, commands, calendar
  * reservations, participant locks, and notification receipts.
  */
final class MongoInterviewWorkflowRepository(
    database: MongoDatabase[IO],
    transactionRunner: MongoTransactionRunner,
    diagnostics: Diagnostics,
    completedEvidenceRetention: FiniteDuration = 8.days
) extends InterviewWorkflowRepository
    with InterviewCalendarLedger
    with InterviewNotificationReceiptLedger
    with MongoOperationalEventInsertion {
  private val workflows = Mongo4catsCollections.documents(database, MongoCollections.InterviewWorkflows)
  private val commands = Mongo4catsCollections.documents(database, MongoCollections.InterviewWorkflowCommands)
  private val inbox = Mongo4catsCollections.documents(database, MongoCollections.InterviewWorkflowInbox)
  private val reservations =
    Mongo4catsCollections.documents(database, MongoCollections.InterviewCalendarReservations)
  private val participantLocks =
    Mongo4catsCollections.documents(database, MongoCollections.InterviewCalendarParticipantLocks)
  private val notificationReceipts =
    Mongo4catsCollections.documents(database, MongoCollections.InterviewNotificationReceipts)

  private val WorkflowIdField = "workflowId"
  private val RevisionField = "revision"
  private val DocumentTypeField = "documentType"
  private val RequestFingerprintField = "requestFingerprint"
  private val RequestWorkflowField = "requestWorkflowId"
  private val MessageIdField = "messageId"
  private val StepIdField = "stepId"
  private val CommandField = "command"
  private val CommandStateField = "commandState"
  private val OwnerField = "claimOwner"
  private val FencingTokenField = "claimToken"
  private val LeaseUntilField = "claimUntil"
  private val FailureCodeField = "failureCode"
  private val ReleasedAtField = "releasedAt"
  private val StartField = "startsAt"
  private val EndField = "endsAt"
  private val IdempotencyKeyField = "idempotencyKey"

  override def claimExecution(
      command: InterviewWorkflowCommandRecord,
      workerId: String,
      now: Instant,
      leaseUntil: Instant,
      maxAttempts: Int
  ): RepositoryIO[InterviewExecutionClaimOutcome] =
    if (workerId.isEmpty || workerId.length > 128 || !leaseUntil.isAfter(now) || maxAttempts <= 0)
      RepositoryIO.fromEither(Left(RepositoryError.InvalidStoredData))
    else
      RepositoryIO.lift(IO.randomUUID).flatMap { token =>
        transactionRunner.run { session =>
          loadWorkflow(command.workflowId, session).flatMap {
            case None           => RepositoryIO.fromEither(Right(InterviewExecutionClaimOutcome.AlreadyHandled))
            case Some(workflow) =>
              for {
                _ <- fenceSubjects(session, List(workflow.candidateId, workflow.recruiterId, workflow.initiatedBy))
                current <- RepositoryIO.lift(
                  MongoSessionOperations.findOne(
                    commands,
                    session,
                    MongoFilter.eq(MongoFields.Id, commandId(command.workflowId, command.stepId))
                  )
                )
                outcome <- current match {
                  case None           => RepositoryIO.fromEither(Right(InterviewExecutionClaimOutcome.AlreadyHandled))
                  case Some(document) =>
                    RepositoryIO.fromEither(decodeCommandRecord(document)).flatMap { stored =>
                      InterviewExecutionPolicy.admission(workflow, stored) match {
                        case InterviewExecutionAdmission.AlreadyHandled =>
                          RepositoryIO.fromEither(Right(InterviewExecutionClaimOutcome.AlreadyHandled))
                        case InterviewExecutionAdmission.InspectLease => {
                          val identity = for {
                            owner <- string(document, OwnerField)
                            token <- string(document, FencingTokenField).flatMap(uuid)
                            until <- instant(document, LeaseUntilField)
                          } yield InterviewExecutionLease(owner, token, until)
                          RepositoryIO.fromEither(identity).flatMap { lease =>
                            InterviewExecutionPolicy.lease(lease, now) match {
                              case InterviewExecutionLeaseDisposition.Busy =>
                                RepositoryIO.fromEither(Right(InterviewExecutionClaimOutcome.Busy))
                              case InterviewExecutionLeaseDisposition.Reconcile =>
                                guardWrite(
                                  commands,
                                  session,
                                  MongoFilter.and(
                                    MongoFilter.eq(MongoFields.Id, commandId(stored.workflowId, stored.stepId)),
                                    MongoFilter.eq(CommandStateField, InterviewWorkflowCommandState.Executing.toString),
                                    MongoFilter.eq(OwnerField, lease.owner),
                                    MongoFilter.eq(FencingTokenField, lease.token.toString),
                                    MongoFilter.lte(LeaseUntilField, Date.from(now)),
                                    MongoFilter.exists("result", false)
                                  ),
                                  MongoUpdate.combine(
                                    MongoUpdate.set("result", InterviewCommandResult.OutcomeUnknown.toString),
                                    MongoUpdate
                                      .set(CommandStateField, InterviewWorkflowCommandState.ResultPending.toString),
                                    availableFrom(stored.command, now),
                                    MongoUpdate.unset(OwnerField),
                                    MongoUpdate.unset(FencingTokenField),
                                    MongoUpdate.unset(LeaseUntilField)
                                  )
                                )
                                  .as(InterviewExecutionClaimOutcome.ReconciliationQueued)
                            }
                          }
                        }
                        case InterviewExecutionAdmission.CheckBudget => {
                          executionAttemptCount(stored.workflowId, stored.command, session).flatMap { consumed =>
                            InterviewExecutionPolicy.budget(stored, consumed, maxAttempts) match {
                              case InterviewExecutionBudgetDisposition.RequireRepair =>
                                applyRepair(
                                  session,
                                  workflow,
                                  stored.command,
                                  InterviewAdvanceCause.ExecutionExhausted(stored.stepId),
                                  "execution_exhausted",
                                  now
                                ).void *>
                                  guardWrite(
                                    commands,
                                    session,
                                    MongoFilter.and(
                                      MongoFilter.eq(MongoFields.Id, commandId(stored.workflowId, stored.stepId)),
                                      MongoFilter.eq(CommandStateField, stored.state.toString)
                                    ),
                                    MongoUpdate.combine(
                                      MongoUpdate
                                        .set(CommandStateField, InterviewWorkflowCommandState.RepairRequired.toString),
                                      MongoUpdate.set(FailureCodeField, "execution_exhausted"),
                                      MongoUpdate.unset(OwnerField),
                                      MongoUpdate.unset(FencingTokenField),
                                      MongoUpdate.unset(LeaseUntilField)
                                    )
                                  )
                                    .as(InterviewExecutionClaimOutcome.RepairRequired)
                              case InterviewExecutionBudgetDisposition.Acquire(additional) =>
                                guardWrite(
                                  commands,
                                  session,
                                  MongoFilter.and(
                                    MongoFilter.eq(MongoFields.Id, commandId(stored.workflowId, stored.stepId)),
                                    MongoFilter.eq(RevisionField, stored.revision),
                                    MongoFilter.eq(CommandStateField, stored.state.toString)
                                  ),
                                  MongoUpdate.combine(
                                    MongoUpdate
                                      .set(CommandStateField, InterviewWorkflowCommandState.Executing.toString),
                                    MongoUpdate.set(OwnerField, workerId),
                                    MongoUpdate.set(FencingTokenField, token.toString),
                                    MongoUpdate.set(LeaseUntilField, Date.from(leaseUntil)),
                                    MongoUpdate.inc("executionAttempts", additional)
                                  )
                                )
                                  .as(
                                    InterviewExecutionClaimOutcome.Acquired(
                                      ClaimedInterviewWorkflowCommand(
                                        stored.copy(executionAttempts = stored.executionAttempts + additional),
                                        workerId,
                                        token,
                                        leaseUntil
                                      )
                                    )
                                  )
                            }
                          }
                        }
                      }
                    }
                }
              } yield outcome
          }
        }
      }

  override def authorizePublication(
      claim: ClaimedInterviewWorkflowCommand,
      generation: InterviewPublisherGeneration,
      now: Instant
  ): RepositoryIO[Boolean] = transactionRunner.run { session =>
    for {
      workflow <- loadWorkflow(claim.record.workflowId, session).subflatMap(_.toRight(RepositoryError.Conflict))
      _ <- fenceSubjects(session, List(workflow.candidateId, workflow.recruiterId, workflow.initiatedBy))
      disposition <- publicationDisposition(session, workflow, claim.record, Int.MaxValue)
      valid =
        disposition == InterviewPublicationDisposition.Send && claim.record.state != InterviewWorkflowCommandState.Superseded
      _ <- guardWrite(
        commands,
        session,
        if (valid)
          MongoFilter.and(
            claimFilter(claim, now),
            MongoFilter.eq(MongoFields.Attempts, Int.box(claim.record.publicationAttempts))
          )
        else claimFilter(claim, now),
        if (valid)
          MongoUpdate.combine(
            MongoUpdate.set("publicationCheckedAt", Date.from(now)),
            MongoUpdate.inc(MongoFields.Attempts, java.lang.Integer.valueOf(1))
          )
        else terminalCommand(InterviewWorkflowCommandState.Superseded, "publication_obsolete", now)
      )
      _ <-
        if (valid)
          List(workflow.candidateId, workflow.recruiterId, workflow.initiatedBy).distinct.traverse_(subject =>
            MongoProducerRegistrations
              .register(database, session, subject.value.toString, generation.transactionalId, "Interview", now)
          )
        else RepositoryIO.fromEither(Right(()))
    } yield valid
  }

  private def publicationDisposition(
      session: Option[ClientSession[IO]],
      workflow: InterviewWorkflow,
      record: InterviewWorkflowCommandRecord,
      maximumAttempts: Int
  ): RepositoryIO[InterviewPublicationDisposition] = {
    def decide(applied: Boolean): InterviewPublicationDisposition =
      InterviewWorkflowPolicy.publicationDisposition(
        workflow,
        record.revision,
        record.command,
        record.result.nonEmpty,
        applied,
        record.publicationAttempts,
        maximumAttempts
      )
    val preliminary = decide(false)
    if (record.result.isEmpty || preliminary == InterviewPublicationDisposition.Supersede)
      RepositoryIO.fromEither(Right(preliminary))
    else {
      val commandId = UUID.nameUUIDFromBytes(record.stepId.getBytes(java.nio.charset.StandardCharsets.UTF_8))
      val resultId = UUID.nameUUIDFromBytes(s"$commandId:result".getBytes(java.nio.charset.StandardCharsets.UTF_8))
      RepositoryIO
        .lift(
          MongoSessionOperations
            .findOne(inbox, session, MongoFilter.eq(MongoFields.Id, s"${workflow.id.value}:$resultId"))
        )
        .map(receipt => decide(receipt.nonEmpty))
    }
  }

  override def attemptCount(workflowId: InterviewWorkflowId, command: InterviewCommand): RepositoryIO[Long] =
    executionAttemptCount(workflowId, command, None)

  /** The stored kinds and extra identity that make up one retry budget. `None` means the command is never retried. */
  private def attemptScope(command: InterviewCommand): Option[(List[String], List[MongoFilter])] = {
    import MongoInterviewWorkflowCommandCodec.LifecycleKinds as K
    def keyed(value: String) = List(MongoFilter.eq(s"$CommandField.$IdempotencyKeyField", value))
    command match {
      case InterviewWorkflowCommand.ReserveCalendarSlot(_) | InterviewWorkflowCommand.LookupCalendarReservation(_) =>
        Some((List("reserveCalendar", "lookupCalendar"), Nil))
      case InterviewWorkflowCommand.CommitAcceptedToInterview(_) |
          InterviewWorkflowCommand.LookupStatusCommitReceipt(_) =>
        Some((List("commitInterview", "lookupStatusCommit"), Nil))
      case InterviewWorkflowCommand.ReleaseCalendarSlot(_) => Some((List("releaseCalendar"), Nil))
      case InterviewWorkflowCommand.Notify(_, key)         => Some((List("notify", "lookupNotification"), keyed(key)))
      case InterviewWorkflowCommand.LookupNotificationReceipt(_, key) =>
        Some((List("notify", "lookupNotification"), keyed(key)))
      case InterviewWorkflowCommand.RequireRepair(_) | InterviewLifecycleCommand.RequireRepair(_) =>
        Some((List("requireRepair"), Nil))
      case InterviewLifecycleCommand.CancelCalendarSlot(key) =>
        Some((List(K.CancelCalendar, K.LookupCancellation), keyed(key)))
      case InterviewLifecycleCommand.LookupCalendarCancellation(key) =>
        Some((List(K.CancelCalendar, K.LookupCancellation), keyed(key)))
      case InterviewLifecycleCommand.HoldReplacementSlot(key, _) =>
        Some((List(K.HoldReplacement, K.LookupReplacementHold), keyed(key)))
      case InterviewLifecycleCommand.LookupReplacementHold(key) =>
        Some((List(K.HoldReplacement, K.LookupReplacementHold), keyed(key)))
      case InterviewLifecycleCommand.CommitRescheduledInterval(_, generation) =>
        Some((List(K.CommitReschedule, K.LookupRescheduleCommit), generationScope(generation)))
      case InterviewLifecycleCommand.LookupRescheduleCommitReceipt(_, generation) =>
        Some((List(K.CommitReschedule, K.LookupRescheduleCommit), generationScope(generation)))
      case InterviewLifecycleCommand.Notify(_, _, key) =>
        Some((List(K.NotifyKind, K.LookupNotificationKind), keyed(key)))
      case InterviewLifecycleCommand.LookupNotificationReceipt(_, _, key) =>
        Some((List(K.NotifyKind, K.LookupNotificationKind), keyed(key)))
      // Executed once per proposal and never retried, so it has no budget to exhaust.
      case InterviewLifecycleCommand.ExpireProposal(_) => None
    }
  }

  private def generationScope(generation: Int) = List(MongoFilter.eq(s"$CommandField.generation", Int.box(generation)))

  private def executionAttemptCount(
      workflowId: InterviewWorkflowId,
      command: InterviewCommand,
      session: Option[ClientSession[IO]]
  ): RepositoryIO[Long] =
    attemptScope(command).fold(RepositoryIO.fromEither[Long](Right(0L))) { case (kinds, scope) =>
      RepositoryIO
        .lift(
          MongoSessionOperations.findOne(workflows, session, MongoFilter.eq(MongoFields.Id, workflowId.value.toString))
        )
        .subflatMap(_.toRight(RepositoryError.Conflict))
        .flatMap { stored =>
          val observedEpoch = Option(stored.get("attemptEpochRevision")) match {
            case None                                                   => Right(0L)
            case Some(number: java.lang.Long) if number.longValue >= 0L => Right(number.longValue)
            case _                                                      => Left(RepositoryError.InvalidStoredData)
          }
          RepositoryIO.fromEither(observedEpoch).flatMap { epoch =>
            val filters = List(
              MongoFilter.eq(WorkflowIdField, workflowId.value.toString),
              MongoFilter.gte(RevisionField, epoch),
              MongoFilter.in(s"$CommandField.kind", kinds)
            ) ++ scope
            val filter = MongoFilter.and(filters*)
            RepositoryIO.fromIOEither(commands.flatMap { collection =>
              val query =
                session.fold(collection.find(filter.bson))(active => collection.find(active, filter.sessionFilter))
              query.boundedStream(32).compile.fold[Either[RepositoryError, Long]](Right(0L)) { (sum, row) =>
                for {
                  total <- sum; attempts <- integer(row, "executionAttempts");
                  _ <- Either.cond(attempts >= 0, (), RepositoryError.InvalidStoredData)
                } yield total + attempts.toLong
              }
            })
          }
        }
    }

  override def quarantine(identity: String, now: Instant): RepositoryIO[Unit] =
    if (identity.isEmpty || identity.length > 256) RepositoryIO.fromEither(Left(RepositoryError.InvalidStoredData))
    else
      RepositoryIO
        .lift(
          MongoSessionOperations.updateOne(
            inbox,
            None,
            MongoFilter.eq(MongoFields.Id, s"quarantine:$identity"),
            MongoUpdate.combine(
              MongoUpdate.setOnInsert(DocumentTypeField, "quarantine"),
              MongoUpdate.setOnInsert("failureCode", "invalid_message"),
              MongoUpdate.setOnInsert(MongoFields.OccurredAt, Date.from(now)),
              MongoUpdate.setOnInsert(
                MongoFields.RetentionExpiresAt,
                Date.from(now.plusMillis(completedEvidenceRetention.toMillis))
              )
            ),
            new UpdateOptions().upsert(true)
          )
        )
        .subflatMap {
          case Some(result) if result.wasAcknowledged() => Right(())
          case _                                        => Left(RepositoryError.MissingWriteResult)
        }

  override def recordResult(
      claim: ClaimedInterviewWorkflowCommand,
      result: InterviewCommandResult,
      now: Instant
  ): RepositoryIO[Unit] =
    transactionRunner.run { session =>
      val command = claim.record
      loadWorkflow(command.workflowId, session).flatMap {
        case None           => RepositoryIO.fromEither(Right(()))
        case Some(workflow) =>
          fenceSubjects(session, List(workflow.candidateId, workflow.recruiterId, workflow.initiatedBy)) *>
            guardWrite(
              commands,
              session,
              MongoFilter.and(
                MongoFilter.eq(MongoFields.Id, commandId(command.workflowId, command.stepId)),
                MongoFilter.eq(RevisionField, command.revision),
                MongoFilter.eq(OwnerField, claim.owner),
                MongoFilter.eq(FencingTokenField, claim.fencingToken.toString),
                MongoFilter.eq(CommandStateField, InterviewWorkflowCommandState.Executing.toString),
                MongoFilter.gt(LeaseUntilField, Date.from(now)),
                MongoFilter.exists("result", false)
              ),
              MongoUpdate.combine(
                MongoUpdate.set("result", result.toString),
                MongoUpdate.set(CommandStateField, InterviewWorkflowCommandState.ResultPending.toString),
                availableFrom(command.command, now)
              )
            )
      }
    }

  override def findRequest(
      actorId: UserId,
      requestKey: UUID,
      fingerprint: MutationReceiptFingerprint
  ): RepositoryIO[Option[InterviewWorkflow]] =
    requestReceipt(requestReceiptId(actorId, requestKey)).flatMap {
      case None                                                  => RepositoryIO.fromEither(Right(None))
      case Some((id, existing)) if existing == fingerprint.value => loadWorkflow(id, None)
      case Some(_) => RepositoryIO.fromEither(Left(RepositoryError.Conflict))
    }

  override def findCommand(
      workflowId: InterviewWorkflowId,
      stepId: String
  ): RepositoryIO[Option[InterviewWorkflowCommandRecord]] =
    RepositoryIO
      .lift(
        MongoSessionOperations.findOne(commands, None, MongoFilter.eq(MongoFields.Id, commandId(workflowId, stepId)))
      )
      .flatMap(_.traverse(document => RepositoryIO.fromEither(decodeCommandRecord(document))))

  private def swapReceiptId(workflowId: InterviewWorkflowId, generation: Int): String =
    s"${workflowId.value}:swap:g$generation"

  override def hasRescheduleApproval(workflowId: InterviewWorkflowId, generation: Int): RepositoryIO[Boolean] =
    RepositoryIO
      .lift(
        MongoSessionOperations
          .findOne(inbox, None, MongoFilter.eq(MongoFields.Id, swapReceiptId(workflowId, generation)))
      )
      .map(_.nonEmpty)

  override def approveRescheduledInterval(
      workflow: InterviewWorkflow,
      now: Instant,
      execution: ClaimedInterviewWorkflowCommand
  ): RepositoryIO[Unit] =
    MongoRepositorySupport.repositoryGuard(diagnostics, "interviewWorkflow.approveRescheduledInterval") {
      transactionRunner.run { session =>
        val receiptId = swapReceiptId(workflow.id, workflow.replacementGeneration)
        def held(generation: Int): RepositoryIO[InterviewCalendarReservation] =
          RepositoryIO
            .lift(
              MongoSessionOperations.findOne(
                reservations,
                session,
                MongoFilter.and(
                  reservationIdentity(workflow.id, InterviewWorkflow.reservationKey(workflow.id, generation)),
                  MongoFilter.exists(ReleasedAtField, false)
                )
              )
            )
            .subflatMap(_.toRight(RepositoryError.Conflict))
            .flatMap(document => RepositoryIO.fromEither(decodeReservation(document)))
        for {
          _ <- fenceExecution(session, workflow.id, Some(execution))
          existing <- RepositoryIO.lift(
            MongoSessionOperations.findOne(inbox, session, MongoFilter.eq(MongoFields.Id, receiptId))
          )
          _ <-
            if (existing.nonEmpty) RepositoryIO.fromEither(Right(()))
            else
              for {
                current <- loadWorkflow(workflow.id, session).subflatMap(_.toRight(RepositoryError.Conflict))
                pending <- RepositoryIO.fromEither(workflow.pendingInterval.toRight(RepositoryError.Conflict))
                _ <- RepositoryIO.fromEither(
                  Either.cond(
                    current == workflow && workflow.phase == InterviewWorkflowPhase.RescheduleSwapPending &&
                      now.isBefore(InterviewCalendarFence.swapDeadline(workflow)),
                    (),
                    RepositoryError.Conflict
                  )
                )
                _ <- fenceSubjects(session, List(workflow.candidateId, workflow.recruiterId, workflow.initiatedBy))
                _ <- RepositoryIO
                  .lift(
                    MongoSessionOperations.findOne(
                      Mongo4catsCollections.documents(database, MongoCollections.Applications),
                      session,
                      MongoFilter.and(
                        MongoFilter.eq(MongoFields.Id, workflow.applicationId.value.toString),
                        MongoFilter.eq(MongoFields.Status, ApplicationStatus.Interview.toString)
                      )
                    )
                  )
                  .subflatMap(_.toRight(RepositoryError.Conflict))
                _ <- held(workflow.generation)
                replacement <- held(workflow.replacementGeneration)
                _ <- RepositoryIO.fromEither(Either.cond(replacement.interval == pending, (), RepositoryError.Conflict))
                _ <- insert(
                  session,
                  inbox,
                  inboxDocument(
                    receiptId,
                    workflow.id,
                    s"swap:g${workflow.replacementGeneration}",
                    workflow.revision,
                    now
                  )
                )
              } yield ()
        } yield ()
      }
    }(_ => Left(RepositoryError.Unavailable))

  override def settleNotificationResult(
      record: InterviewWorkflowCommandRecord,
      settlement: InterviewNotificationSettlement,
      now: Instant
  ): RepositoryIO[Boolean] =
    if (!InterviewCommands.isInformational(record.command)) RepositoryIO.fromEither(Left(RepositoryError.InvalidEvent))
    else
      MongoRepositorySupport.repositoryGuard(diagnostics, "interviewWorkflow.settleNotificationResult") {
        val target = MongoFilter.and(
          MongoFilter.eq(MongoFields.Id, commandId(record.workflowId, record.stepId)),
          MongoFilter.eq(RevisionField, record.revision)
        )
        val live = List(
          InterviewWorkflowCommandState.Pending,
          InterviewWorkflowCommandState.Claimed,
          InterviewWorkflowCommandState.Published,
          InterviewWorkflowCommandState.Executing,
          InterviewWorkflowCommandState.ResultPending,
          InterviewWorkflowCommandState.ResultPublished
        )
        val failedResult = settlement match {
          // A retry answers a recorded, unsuccessful delivery result.
          case InterviewNotificationSettlement.Retry(_) =>
            MongoFilter.and(
              target,
              MongoFilter.in(
                CommandStateField,
                List(
                  InterviewWorkflowCommandState.Claimed,
                  InterviewWorkflowCommandState.ResultPending,
                  InterviewWorkflowCommandState.ResultPublished
                ).map(_.toString)
              ),
              MongoFilter.exists("result"),
              MongoFilter.ne("result", InterviewCommandResult.Succeeded.toString)
            )
          // Exhaustion also ends a command whose message can no longer be delivered in time.
          case InterviewNotificationSettlement.Exhausted(_) =>
            MongoFilter.and(target, MongoFilter.in(CommandStateField, live.map(_.toString)))
        }
        val update = settlement match {
          case InterviewNotificationSettlement.Retry(availableAt) =>
            MongoUpdate.combine(
              MongoUpdate.set(CommandStateField, InterviewWorkflowCommandState.Pending.toString),
              MongoUpdate.set(MongoFields.AvailableAt, Date.from(availableAt)),
              MongoUpdate.set(FailureCodeField, "notification_retry"),
              MongoUpdate.inc("executionAttempts", Int.box(1)),
              // Publication attempts bound one delivery cycle of the broker; every logical retry starts a fresh one.
              MongoUpdate.set(MongoFields.Attempts, Int.box(0)),
              MongoUpdate.unset("result"),
              MongoUpdate.unset(OwnerField),
              MongoUpdate.unset(FencingTokenField),
              MongoUpdate.unset(LeaseUntilField)
            )
          case InterviewNotificationSettlement.Exhausted(code) =>
            terminalCommand(InterviewWorkflowCommandState.RepairRequired, code, now)
        }
        val failureCodeValid = settlement match {
          case InterviewNotificationSettlement.Exhausted(code) => safeFailureCode(code)
          case _                                               => true
        }
        if (!failureCodeValid) RepositoryIO.fromEither[Boolean](Left(RepositoryError.InvalidStoredData))
        else
          RepositoryIO
            .lift(MongoSessionOperations.updateOne(commands, None, failedResult, update))
            .subflatMap {
              case Some(result) => Right(result.getMatchedCount == 1L)
              case None         => Left(RepositoryError.MissingWriteResult)
            }
      }(_ => Left(RepositoryError.Unavailable))

  override def deferExpiry(record: InterviewWorkflowCommandRecord, availableAt: Instant): RepositoryIO[Boolean] =
    record.command match {
      case InterviewLifecycleCommand.ExpireProposal(due) if !availableAt.isBefore(due) =>
        MongoRepositorySupport.repositoryGuard(diagnostics, "interviewWorkflow.deferExpiry") {
          RepositoryIO
            .lift(
              MongoSessionOperations.updateOne(
                commands,
                None,
                MongoFilter.and(
                  MongoFilter.eq(MongoFields.Id, commandId(record.workflowId, record.stepId)),
                  MongoFilter.in(
                    CommandStateField,
                    List(
                      InterviewWorkflowCommandState.Pending,
                      InterviewWorkflowCommandState.Claimed,
                      InterviewWorkflowCommandState.Published
                    ).map(_.toString)
                  )
                ),
                MongoUpdate.combine(
                  MongoUpdate.set(CommandStateField, InterviewWorkflowCommandState.Pending.toString),
                  availableFrom(record.command, availableAt),
                  MongoUpdate.unset(OwnerField),
                  MongoUpdate.unset(FencingTokenField),
                  MongoUpdate.unset(LeaseUntilField)
                )
              )
            )
            .subflatMap {
              case Some(result) => Right(result.getMatchedCount == 1L)
              case None         => Left(RepositoryError.MissingWriteResult)
            }
        }(_ => Left(RepositoryError.Unavailable))
      case _ => RepositoryIO.fromEither(Left(RepositoryError.InvalidEvent))
    }

  private val NotificationRepairLimit = 32

  private def notificationRepairFilter(workflowId: InterviewWorkflowId): MongoFilter =
    MongoFilter.and(
      MongoFilter.eq(WorkflowIdField, workflowId.value.toString),
      MongoFilter.eq(CommandStateField, InterviewWorkflowCommandState.RepairRequired.toString),
      MongoFilter.eq(s"$CommandField.kind", MongoInterviewWorkflowCommandCodec.LifecycleKinds.NotifyKind),
      MongoFilter.in(
        s"$CommandField.notificationKind",
        InterviewNotificationKind.values.toList.filterNot(InterviewCommands.isRoundKind).map(_.toString)
      )
    )

  override def findNotificationRepairs(
      workflowId: InterviewWorkflowId
  ): RepositoryIO[List[InterviewWorkflowCommandRecord]] =
    MongoRepositorySupport.repositoryGuard(diagnostics, "interviewWorkflow.findNotificationRepairs") {
      RepositoryIO
        .lift(
          MongoSessionOperations
            .findManyById(commands, None, notificationRepairFilter(workflowId), NotificationRepairLimit)
        )
        .flatMap(_.traverse(document => RepositoryIO.fromEither(decodeCommandRecord(document))))
    }(_ => Left(RepositoryError.Unavailable))

  override def repairNotifications(
      workflowId: InterviewWorkflowId,
      requestKey: UUID,
      now: Instant,
      actorId: UserId
  ): RepositoryIO[Int] =
    MongoRepositorySupport.repositoryGuard(diagnostics, "interviewWorkflow.repairNotifications") {
      transactionRunner.run { session =>
        val receiptId = s"${workflowId.value}:repair-notifications:$requestKey"
        RepositoryIO
          .lift(MongoSessionOperations.findOne(inbox, session, MongoFilter.eq(MongoFields.Id, receiptId)))
          .flatMap {
            case Some(receipt) => RepositoryIO.fromEither(integer(receipt, "repaired"))
            case None          =>
              for {
                workflow <- loadWorkflow(workflowId, session).subflatMap(_.toRight(RepositoryError.Conflict))
                _ <- fenceSubjects(session, List(workflow.candidateId, workflow.recruiterId, workflow.initiatedBy))
                rows <- RepositoryIO.lift(
                  MongoSessionOperations
                    .findManyById(commands, session, notificationRepairFilter(workflowId), NotificationRepairLimit)
                )
                _ <- rows.traverse_(row =>
                  guardWrite(
                    commands,
                    session,
                    MongoFilter.and(
                      MongoFilter.eq(MongoFields.Id, row.getString(MongoFields.Id)),
                      MongoFilter.eq(CommandStateField, InterviewWorkflowCommandState.RepairRequired.toString)
                    ),
                    MongoUpdate.combine(
                      MongoUpdate.set(CommandStateField, InterviewWorkflowCommandState.Pending.toString),
                      MongoUpdate.set(MongoFields.AvailableAt, Date.from(now)),
                      MongoUpdate.set("executionAttempts", Int.box(0)),
                      MongoUpdate.set(MongoFields.Attempts, Int.box(0)),
                      MongoUpdate.unset(FailureCodeField),
                      MongoUpdate.unset("finishedAt")
                    )
                  )
                )
                _ <- insert(
                  session,
                  inbox,
                  inboxDocument(receiptId, workflowId, s"repair-notifications:$requestKey", workflow.revision, now)
                    .append("repaired", Int.box(rows.size))
                    .append("auditActorId", actorId.value.toString)
                )
              } yield rows.size
          }
      }
    }(_ => Left(RepositoryError.Unavailable))

  override def hasHiringReceipt(workflowId: InterviewWorkflowId): RepositoryIO[Boolean] =
    RepositoryIO
      .lift(MongoSessionOperations.findOne(inbox, None, MongoFilter.eq(MongoFields.Id, s"${workflowId.value}:hiring")))
      .map(_.nonEmpty)

  override def commitHiring(
      workflow: InterviewWorkflow,
      now: Instant,
      execution: Option[ClaimedInterviewWorkflowCommand] = None
  ): RepositoryIO[Unit] =
    MongoRepositorySupport.repositoryGuard(diagnostics, "interviewWorkflow.commitHiring") {
      transactionRunner.run { session =>
        val receiptId = s"${workflow.id.value}:hiring"
        for {
          _ <- fenceExecution(session, workflow.id, execution)
          existing <- RepositoryIO.lift(
            MongoSessionOperations.findOne(inbox, session, MongoFilter.eq(MongoFields.Id, receiptId))
          )
          _ <-
            if (existing.nonEmpty) RepositoryIO.fromEither(Right(()))
            else {
              val applications = Mongo4catsCollections.documents(database, MongoCollections.Applications)
              val jobs = Mongo4catsCollections.documents(database, MongoCollections.Jobs)
              val users = Mongo4catsCollections.documents(database, MongoCollections.Users)
              for {
                committedAt <- RepositoryIO.lift(IO.realTimeInstant)
                _ <- RepositoryIO.fromEither(
                  Either.cond(
                    now.isBefore(workflow.preCommitDeadline) && committedAt.isBefore(workflow.preCommitDeadline),
                    (),
                    RepositoryError.Conflict
                  )
                )
                current <- loadWorkflow(workflow.id, session)
                _ <- RepositoryIO.fromEither(
                  Either.cond(
                    current.contains(workflow) && workflow.phase == InterviewWorkflowPhase.StatusCommitPending,
                    (),
                    RepositoryError.Conflict
                  )
                )
                reserved <- RepositoryIO.lift(
                  MongoSessionOperations.findOne(
                    reservations,
                    session,
                    MongoFilter.and(
                      reservationIdentity(workflow.id, InterviewWorkflow.reservationKey(workflow.id, 0)),
                      MongoFilter.exists(ReleasedAtField, false)
                    )
                  )
                )
                _ <- RepositoryIO.fromEither(Either.cond(reserved.nonEmpty, (), RepositoryError.Conflict))
                application <- RepositoryIO
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
                  .subflatMap(_.toRight(RepositoryError.Conflict))
                decoded <- RepositoryIO.fromEither(
                  MongoHiringCodecs
                    .readApplication(application)
                    .toEither
                    .leftMap(_ => RepositoryError.InvalidStoredData)
                )
                lifecycle <- RepositoryIO.fromEither(
                  ApplicationLifecycle
                    .changeStatus(ApplicationStatus.Interview, workflow.initiatedBy, now, None, None)
                    .run(decoded)
                    .leftMap(_ => RepositoryError.Conflict)
                )
                (updated, change) = lifecycle
                _ <- guardWrite(
                  jobs,
                  session,
                  MongoFilter.and(
                    MongoFilter.eq(MongoFields.Id, updated.jobId.value.toString),
                    MongoFilter.eq(MongoFields.RecruiterId, workflow.recruiterId.value.toString)
                  ),
                  MongoUpdate.inc(MongoFields.Version, 1L)
                )
                _ <- List(workflow.candidateId, workflow.recruiterId, workflow.initiatedBy).distinct.traverse_(id =>
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
                _ <- guardWrite(
                  applications,
                  session,
                  MongoFilter.and(
                    MongoFilter.eq(MongoFields.Id, workflow.applicationId.value.toString),
                    MongoFilter.eq(MongoFields.Status, change.previousStatus.toString)
                  ),
                  MongoUpdate.combine(
                    MongoUpdate.set(MongoFields.Status, updated.status.toString),
                    MongoUpdate.set(MongoFields.UpdatedAt, Date.from(updated.updatedAt))
                  )
                )
                eventId = UUID.nameUUIDFromBytes(
                  s"${workflow.id.value}:status".getBytes(java.nio.charset.StandardCharsets.UTF_8)
                )
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
                _ <- insert(
                  session,
                  Mongo4catsCollections.documents(database, MongoCollections.ApplicationEvents),
                  MongoHiringCodecs.event(statusEvent)
                )
                _ <- insertOperationalEvents(
                  Mongo4catsCollections.documents(database, MongoCollections.EventOutbox),
                  session,
                  List(
                    OperationalEvents.statusChanged(eventId, updated, statusEvent)
                  ),
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
                    .append(MongoFields.OccurredAt, Date.from(now))
                )
                next <- RepositoryIO.fromEither(
                  InterviewWorkflow
                    .decide(workflow, workflow.revision, InterviewWorkflowEvent.StatusCommitted)
                    .leftMap(_ => RepositoryError.Conflict)
                )
                _ <- guardWrite(
                  workflows,
                  session,
                  MongoFilter.and(
                    MongoFilter.eq(MongoFields.Id, workflow.id.value.toString),
                    MongoFilter.eq(RevisionField, workflow.revision)
                  ),
                  MongoUpdate.combine(
                    MongoUpdate.set(RevisionField, next.workflow.revision),
                    MongoUpdate.set("phase", next.workflow.phase.toString)
                  )
                )
                _ <- next.commands.zipWithIndex.traverse_ { case (command, ordinal) =>
                  insert(
                    session,
                    commands,
                    commandDocument(
                      InterviewWorkflowCommandRecord(
                        workflow.id,
                        stepId(workflow.id, next.workflow.revision, ordinal),
                        next.workflow.revision,
                        command,
                        InterviewWorkflowCommandState.Pending,
                        0,
                        now,
                        now
                      )
                    )
                  )
                }
              } yield ()
            }
        } yield ()
      }
    }(_ => Left(RepositoryError.Unavailable))

  private def guardWrite(
      collection: IO[MongoSessionOperations.Documents],
      session: Option[ClientSession[IO]],
      filter: MongoFilter,
      update: MongoUpdate
  ): RepositoryIO[Unit] =
    RepositoryIO.lift(MongoSessionOperations.updateOne(collection, session, filter, update)).subflatMap {
      case Some(result) if result.getMatchedCount == 1L => Right(())
      case Some(_)                                      => Left(RepositoryError.Conflict)
      case None                                         => Left(RepositoryError.MissingWriteResult)
    }

  override def repair(
      workflow: InterviewWorkflow,
      expectedRevision: Long,
      requestKey: UUID,
      now: Instant,
      actorId: UserId
  ): RepositoryIO[InterviewWorkflow] =
    RepositoryIO
      .lift(
        MongoSessionOperations
          .findOne(inbox, None, MongoFilter.eq(MongoFields.Id, s"${workflow.id.value}:repair:$requestKey"))
      )
      .flatMap {
        case Some(receipt) =>
          // A repeated request returns the workflow as it is now, not the failed state the caller still holds.
          RepositoryIO
            .fromEither(
              long(receipt, RevisionField)
                .flatMap(revision => Either.cond(revision == expectedRevision + 1L, (), RepositoryError.Conflict))
            ) *> loadWorkflow(workflow.id, None).subflatMap(_.toRight(RepositoryError.Conflict))
        case None if workflow.repairOrigin.nonEmpty =>
          // A cancel or reschedule step exhausted its retries: resume that step by lookup, keeping the audit trail.
          applyLifecycle(
            workflow.id,
            expectedRevision,
            InterviewLifecycleEvent.Repair,
            InterviewLifecycleOrigin.Internal(InterviewAdvanceCause.AdminRepair(requestKey, actorId)),
            now,
            None
          ).flatMap {
            case InterviewLifecycleOutcome.Applied(next)       => RepositoryIO.fromEither(Right(next))
            case InterviewLifecycleOutcome.Duplicate(existing) => RepositoryIO.fromEither(Right(existing))
            case _ => RepositoryIO.fromEither(Left(RepositoryError.Conflict))
          }
        case None => {
          hasHiringReceipt(workflow.id).flatMap { committed =>
            RepositoryIO
              .fromEither(
                InterviewWorkflowPolicy
                  .repair(workflow, expectedRevision, committed)
                  .leftMap(_ => RepositoryError.Conflict)
              )
              .flatMap { case InterviewWorkflowDecision(next, emitted) =>
                advance(next, expectedRevision, InterviewAdvanceCause.AdminRepair(requestKey, actorId), emitted, now)
                  .flatMap {
                    case InterviewWorkflowAdvanceResult.Applied             => RepositoryIO.fromEither(Right(next))
                    case InterviewWorkflowAdvanceResult.Duplicate(existing) => RepositoryIO.fromEither(Right(existing))
                    case InterviewWorkflowAdvanceResult.StaleRevision       =>
                      RepositoryIO.fromEither(Left(RepositoryError.Conflict))
                  }
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
  ): RepositoryIO[InterviewWorkflowAdvanceResult] =
    findRequest(workflow.initiatedBy, requestKey, fingerprint).flatMap {
      case Some(existing) => RepositoryIO.fromEither(Right(InterviewWorkflowAdvanceResult.Duplicate(existing)))
      case None           => {
        if (
          workflow.revision != 0L || workflow.phase != InterviewWorkflowPhase.ReservationPending ||
          initialCommand != InterviewWorkflow.initialCommand(workflow)
        )
          RepositoryIO.fromEither(Left(RepositoryError.InvalidStoredData))
        else {
          createValid(workflow, initialCommand, requestKey, fingerprint, createdAt)
        }
      }
    }

  private def createValid(
      workflow: InterviewWorkflow,
      initialCommand: InterviewWorkflowCommand,
      requestKey: UUID,
      fingerprint: MutationReceiptFingerprint,
      createdAt: Instant
  ): RepositoryIO[InterviewWorkflowAdvanceResult] = {
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
    val write = transactionRunner.run { session =>
      for {
        _ <- fenceSubjects(session, List(workflow.candidateId, workflow.recruiterId, workflow.initiatedBy))
        receipt <- RepositoryIO
          .lift(
            MongoSessionOperations.findOne(
              workflows,
              session,
              MongoFilter.eq(MongoFields.Id, receiptId)
            )
          )
          .flatMap {
            case Some(document) =>
              RepositoryIO.fromEither(decodeReceipt(document).leftMap(_ => RepositoryError.InvalidStoredData))
            case None => RepositoryIO.fromEither(Right(None))
          }
        result <- receipt match {
          case Some((existingWorkflowId, existingFingerprint)) =>
            if (existingFingerprint != fingerprint.value) RepositoryIO.fromEither(Left(RepositoryError.Conflict))
            else
              loadWorkflow(existingWorkflowId, session).subflatMap(
                _.map(InterviewWorkflowAdvanceResult.Duplicate.apply)
                  .toRight(RepositoryError.InvalidStoredData)
              )
          case None =>
            for {
              application <- RepositoryIO
                .lift(
                  MongoSessionOperations.findOne(
                    Mongo4catsCollections.documents(database, MongoCollections.Applications),
                    session,
                    MongoFilter.and(
                      MongoFilter.eq(MongoFields.Id, workflow.applicationId.value.toString),
                      MongoFilter.eq(MongoFields.CandidateId, workflow.candidateId.value.toString),
                      MongoFilter.eq(MongoFields.Status, ApplicationStatus.Accepted.toString)
                    )
                  )
                )
                .subflatMap(_.toRight(RepositoryError.Conflict))
              jobId <- RepositoryIO.fromEither(string(application, MongoFields.JobId))
              transactionNow <- RepositoryIO.lift(IO.realTimeInstant)
              observedUpdatedAt <- RepositoryIO.fromEither(instant(application, MongoFields.UpdatedAt))
              acceptanceUpdatedAt =
                if (transactionNow.toEpochMilli > observedUpdatedAt.toEpochMilli) transactionNow
                else observedUpdatedAt.plusMillis(1L)
              _ <- guardWrite(
                Mongo4catsCollections.documents(database, MongoCollections.Applications),
                session,
                MongoFilter.and(
                  MongoFilter.eq(MongoFields.Id, workflow.applicationId.value.toString),
                  MongoFilter.eq(MongoFields.Status, ApplicationStatus.Accepted.toString)
                ),
                MongoUpdate.set(MongoFields.UpdatedAt, Date.from(acceptanceUpdatedAt))
              )
              _ <- guardWrite(
                Mongo4catsCollections.documents(database, MongoCollections.Jobs),
                session,
                MongoFilter.and(
                  MongoFilter.eq(MongoFields.Id, jobId),
                  MongoFilter.eq(MongoFields.RecruiterId, workflow.recruiterId.value.toString)
                ),
                MongoUpdate.inc(MongoFields.Version, 1L)
              )
              _ <- insert(session, workflows, workflowDocument(workflow))
              _ <- insert(session, workflows, requestReceiptDocument(receiptId, workflow.id, fingerprint, createdAt))
              _ <- insert(session, commands, commandDocument(command))
            } yield InterviewWorkflowAdvanceResult.Applied
        }
      } yield result
    }
    val resolved = RepositoryIO.fromIOEither(write.value.flatMap {
      case Left(RepositoryError.Conflict) =>
        requestReceipt(receiptId).value.flatMap {
          case Right(Some((existingId, existingFingerprint))) if existingFingerprint == fingerprint.value =>
            loadWorkflow(existingId, None).value.map(
              _.flatMap(
                _.map(InterviewWorkflowAdvanceResult.Duplicate.apply)
                  .toRight(RepositoryError.InvalidStoredData)
              )
            )
          case Right(Some(_)) => IO.pure(Left(RepositoryError.Conflict))
          case Right(None)    => IO.pure(Left(RepositoryError.Conflict))
          case Left(error)    => IO.pure(Left(error))
        }
      case other => IO.pure(other)
    })
    MongoRepositorySupport.repositoryGuard(diagnostics, "interviewWorkflow.create")(resolved)(_ =>
      Left(RepositoryError.Unavailable)
    )
  }

  override def findForActor(
      workflowId: InterviewWorkflowId,
      access: InterviewWorkflowAccess
  ): RepositoryIO[Option[InterviewWorkflow]] = {
    val ownerField = access.role match {
      case UserRole.Candidate => Some("candidateId")
      case UserRole.Recruiter => Some("recruiterId")
      case UserRole.Admin     => None
    }
    ownerField match {
      case None        => RepositoryIO.fromEither(Right(None))
      case Some(field) =>
        MongoRepositorySupport.repositoryGuard(diagnostics, "interviewWorkflow.findForActor") {
          RepositoryIO
            .lift(
              MongoSessionOperations
                .findOne(
                  workflows,
                  None,
                  MongoFilter.and(
                    MongoFilter.eq(MongoFields.Id, workflowId.value.toString),
                    MongoFilter.eq(DocumentTypeField, "workflow"),
                    MongoFilter.eq(field, access.actorId.value.toString)
                  )
                )
            )
            .flatMap(_.traverse(document => RepositoryIO.fromEither(decodeWorkflow(document))))
        }(_ => Left(RepositoryError.Unavailable))
    }
  }

  override def findForAdmin(workflowId: InterviewWorkflowId): RepositoryIO[Option[InterviewWorkflow]] =
    MongoRepositorySupport.repositoryGuard(diagnostics, "interviewWorkflow.findForAdmin") {
      RepositoryIO
        .lift(
          MongoSessionOperations
            .findOne(
              workflows,
              None,
              MongoFilter.and(
                MongoFilter.eq(MongoFields.Id, workflowId.value.toString),
                MongoFilter.eq(DocumentTypeField, "workflow")
              )
            )
        )
        .flatMap(_.traverse(document => RepositoryIO.fromEither(decodeWorkflow(document))))
    }(_ => Left(RepositoryError.Unavailable))

  override def advance(
      workflow: InterviewWorkflow,
      expectedRevision: Long,
      cause: InterviewAdvanceCause,
      emittedCommands: List[InterviewWorkflowCommand],
      occurredAt: Instant,
      availableAt: Option[Instant] = None
  ): RepositoryIO[InterviewWorkflowAdvanceResult] = {
    val inboxMessageId = cause.receiptIdentity
    if (
      inboxMessageId.isEmpty || inboxMessageId.length > 256 || expectedRevision == Long.MaxValue ||
      workflow.revision != expectedRevision + 1L
    )
      RepositoryIO.fromEither(Left(RepositoryError.InvalidStoredData))
    else {
      val inboxId = s"${workflow.id.value}:$inboxMessageId"
      val newCommands = emittedCommands.zipWithIndex.map { case (command, index) =>
        InterviewWorkflowCommandRecord(
          workflow.id,
          stepId(workflow.id, workflow.revision, index),
          workflow.revision,
          command,
          command match {
            case InterviewWorkflowCommand.RequireRepair(_) => InterviewWorkflowCommandState.RepairRequired
            case _                                         => InterviewWorkflowCommandState.Pending
          },
          publicationAttempts = 0,
          availableAt = availableAt.getOrElse(occurredAt),
          occurredAt = occurredAt
        )
      }
      MongoRepositorySupport.repositoryGuard(diagnostics, "interviewWorkflow.advance") {
        transactionRunner.run { session =>
          for {
            _ <- fenceSubjects(session, List(workflow.candidateId, workflow.recruiterId, workflow.initiatedBy))
            duplicate <- RepositoryIO.lift(
              MongoSessionOperations.findOne(inbox, session, MongoFilter.eq(MongoFields.Id, inboxId))
            )
            result <- duplicate match {
              case Some(_) =>
                loadWorkflow(workflow.id, session).map(
                  _.fold[InterviewWorkflowAdvanceResult](
                    InterviewWorkflowAdvanceResult.StaleRevision
                  )(InterviewWorkflowAdvanceResult.Duplicate.apply)
                )
              case None =>
                RepositoryIO
                  .lift(
                    MongoSessionOperations
                      .updateOne(
                        workflows,
                        session,
                        MongoFilter.and(
                          MongoFilter.eq(MongoFields.Id, workflow.id.value.toString),
                          MongoFilter.eq(DocumentTypeField, "workflow"),
                          MongoFilter.eq(RevisionField, java.lang.Long.valueOf(expectedRevision))
                        ),
                        MongoUpdate.combine(
                          MongoUpdate.set(RevisionField, java.lang.Long.valueOf(workflow.revision)),
                          MongoUpdate.set("phase", workflow.phase.toString),
                          MongoUpdate.set("notified", workflow.notified.toList.map(_.toString).sorted.asJava),
                          if (cause.resetsAttempts)
                            MongoUpdate.set("attemptEpochRevision", workflow.revision)
                          else MongoUpdate.inc("attemptEpochRevision", 0L)
                        )
                      )
                  )
                  .flatMap {
                    case Some(update) if update.getMatchedCount == 1L =>
                      for {
                        _ <- insert(
                          session,
                          inbox,
                          inboxDocument(inboxId, workflow.id, inboxMessageId, workflow.revision, occurredAt)
                            .append("auditActorId", cause.auditActor.map(_.value.toString).orNull)
                        )
                        _ <-
                          if (cause.resetsAttempts)
                            RepositoryIO
                              .lift(
                                MongoSessionOperations.updateMany(
                                  commands,
                                  session,
                                  MongoFilter.and(
                                    MongoFilter.eq(WorkflowIdField, workflow.id.value.toString),
                                    MongoFilter.lte(RevisionField, expectedRevision)
                                  ),
                                  MongoUpdate.combine(
                                    MongoUpdate
                                      .set(CommandStateField, InterviewWorkflowCommandState.Superseded.toString),
                                    MongoUpdate.unset(OwnerField),
                                    MongoUpdate.unset(FencingTokenField),
                                    MongoUpdate.unset(LeaseUntilField)
                                  )
                                )
                              )
                              .void
                          else RepositoryIO.fromEither(Right(()))
                        _ <- newCommands.traverse_(record => insert(session, commands, commandDocument(record)))
                        _ <-
                          if (workflow.phase == InterviewWorkflowPhase.Completed)
                            retainCompleted(session, workflow, occurredAt)
                          else RepositoryIO.fromEither(Right(()))
                      } yield InterviewWorkflowAdvanceResult.Applied
                    case Some(_) => RepositoryIO.fromEither(Right(InterviewWorkflowAdvanceResult.StaleRevision))
                    case None    => RepositoryIO.fromEither(Left(RepositoryError.MissingWriteResult))
                  }
            }
          } yield result
        }
      }(_ => Left(RepositoryError.Unavailable))
    }
  }

  override def applyLifecycle(
      workflowId: InterviewWorkflowId,
      expectedRevision: Long,
      event: InterviewLifecycleEvent,
      origin: InterviewLifecycleOrigin,
      now: Instant,
      availableAt: Option[Instant]
  ): RepositoryIO[InterviewLifecycleOutcome] =
    admitted(event, origin) match {
      case Left(error) => RepositoryIO.fromEither(Left(error))
      case Right(_)    => applyAdmittedLifecycle(workflowId, expectedRevision, event, origin, now, availableAt)
    }

  /** A person may only drive the seven user events their role allows; the system may only drive the others. */
  private def admitted(
      event: InterviewLifecycleEvent,
      origin: InterviewLifecycleOrigin
  ): Either[RepositoryError, Unit] =
    origin match {
      case InterviewLifecycleOrigin.Actor(access, _, _) =>
        Either.cond(InterviewActorPolicy.permits(access.role, access.actorId, event), (), RepositoryError.InvalidEvent)
      case InterviewLifecycleOrigin.Internal(_) =>
        Either.cond(!InterviewActorPolicy.isUserEvent(event), (), RepositoryError.InvalidEvent)
    }

  private def applyAdmittedLifecycle(
      workflowId: InterviewWorkflowId,
      expectedRevision: Long,
      event: InterviewLifecycleEvent,
      origin: InterviewLifecycleOrigin,
      now: Instant,
      availableAt: Option[Instant]
  ): RepositoryIO[InterviewLifecycleOutcome] =
    MongoRepositorySupport.repositoryGuard(diagnostics, "interviewWorkflow.applyLifecycle") {
      val write = transactionRunner.run { session =>
        lifecycleReplay(session, workflowId, origin).flatMap {
          case Some(replayed) => RepositoryIO.fromEither(Right(replayed))
          case None           =>
            loadVisibleWorkflow(session, workflowId, origin).flatMap {
              case None          => RepositoryIO.fromEither(Right(InterviewLifecycleOutcome.NotVisible))
              case Some(current) =>
                InterviewLifecyclePolicy.decide(current, expectedRevision, event) match {
                  case Left(error)     => RepositoryIO.fromEither(Right(InterviewLifecycleOutcome.Rejected(error)))
                  case Right(decision) => commitLifecycle(session, current, decision, event, origin, now, availableAt)
                }
            }
        }
      }
      // A concurrent identical request commits its receipt first; its stored result is this request's result.
      RepositoryIO.fromIOEither(write.value.flatMap {
        case Left(RepositoryError.Conflict) =>
          lifecycleReplay(None, workflowId, origin).value.map {
            case Right(Some(replayed)) => Right(replayed)
            case Right(None)           => Left(RepositoryError.Conflict)
            case Left(error)           => Left(error)
          }
        case other => IO.pure(other)
      })
    }(_ => Left(RepositoryError.Unavailable))

  /** A repeated request (same actor and key) or cause returns the stored workflow; a reused key with another operation,
    * input or workflow is a typed conflict.
    */
  private def lifecycleReplay(
      session: Option[ClientSession[IO]],
      workflowId: InterviewWorkflowId,
      origin: InterviewLifecycleOrigin
  ): RepositoryIO[Option[InterviewLifecycleOutcome]] = {
    def stored(id: InterviewWorkflowId): RepositoryIO[Option[InterviewLifecycleOutcome]] =
      loadWorkflow(id, session)
        .subflatMap(_.toRight(RepositoryError.InvalidStoredData))
        .map(existing => Some(InterviewLifecycleOutcome.Duplicate(existing)))
    origin match {
      case InterviewLifecycleOrigin.Actor(access, requestKey, fingerprint) =>
        RepositoryIO
          .lift(
            MongoSessionOperations
              .findOne(workflows, session, MongoFilter.eq(MongoFields.Id, requestReceiptId(access.actorId, requestKey)))
          )
          .flatMap {
            case None           => RepositoryIO.fromEither(Right(None))
            case Some(document) =>
              RepositoryIO.fromEither(decodeReceipt(document)).flatMap {
                case Some((id, existing)) if id == workflowId && existing == fingerprint.value => stored(id)
                case _ => RepositoryIO.fromEither(Left(RepositoryError.Conflict))
              }
          }
      case InterviewLifecycleOrigin.Internal(cause) =>
        RepositoryIO
          .lift(
            MongoSessionOperations
              .findOne(inbox, session, MongoFilter.eq(MongoFields.Id, s"${workflowId.value}:${cause.receiptIdentity}"))
          )
          .flatMap(found => if (found.isEmpty) RepositoryIO.fromEither(Right(None)) else stored(workflowId))
    }
  }

  /** The participant predicate of the acting role; Admin has no ownership predicate and internal work has none. */
  private def participantPredicate(origin: InterviewLifecycleOrigin): List[MongoFilter] = origin match {
    case InterviewLifecycleOrigin.Actor(access, _, _) =>
      access.role match {
        case UserRole.Candidate => List(MongoFilter.eq("candidateId", access.actorId.value.toString))
        case UserRole.Recruiter => List(MongoFilter.eq("recruiterId", access.actorId.value.toString))
        case UserRole.Admin     => Nil
      }
    case InterviewLifecycleOrigin.Internal(_) => Nil
  }

  private def loadVisibleWorkflow(
      session: Option[ClientSession[IO]],
      workflowId: InterviewWorkflowId,
      origin: InterviewLifecycleOrigin
  ): RepositoryIO[Option[InterviewWorkflow]] =
    RepositoryIO
      .lift(
        MongoSessionOperations.findOne(
          workflows,
          session,
          MongoFilter.and(
            (MongoFilter.eq(MongoFields.Id, workflowId.value.toString) ::
              MongoFilter.eq(DocumentTypeField, "workflow") :: participantPredicate(origin))*
          )
        )
      )
      .flatMap(_.traverse(document => RepositoryIO.fromEither(decodeWorkflow(document))))

  /** What a cancellation changes besides the workflow: the rejected application and its append-only history. */
  private final case class CancellationWrite(
      application: com.example.graphQL.cats.domain.model.Application,
      previousStatus: ApplicationStatus,
      history: ApplicationEvent
  )

  private def cancellationWrite(
      session: Option[ClientSession[IO]],
      current: InterviewWorkflow,
      initiator: InterviewCancellationInitiator,
      origin: InterviewLifecycleOrigin,
      at: Instant
  ): RepositoryIO[Either[InterviewLifecycleOutcome, CancellationWrite]] = {
    val authorized = origin match {
      case InterviewLifecycleOrigin.Actor(access, _, _) =>
        Right(access).filterOrElse(
          value =>
            (initiator, value.role) match {
              case (InterviewCancellationInitiator.Candidate, UserRole.Candidate) => true
              case (InterviewCancellationInitiator.Recruiter, UserRole.Recruiter) => true
              case (InterviewCancellationInitiator.Admin, UserRole.Admin)         => true
              case _                                                              => false
            },
          RepositoryError.InvalidEvent
        )
      case InterviewLifecycleOrigin.Internal(_) => Left(RepositoryError.InvalidEvent)
    }
    for {
      access <- RepositoryIO.fromEither(authorized)
      document <- RepositoryIO
        .lift(
          MongoSessionOperations.findOne(
            Mongo4catsCollections.documents(database, MongoCollections.Applications),
            session,
            MongoFilter.and(
              MongoFilter.eq(MongoFields.Id, current.applicationId.value.toString),
              MongoFilter.eq(MongoFields.CandidateId, current.candidateId.value.toString)
            )
          )
        )
        .subflatMap(_.toRight(RepositoryError.Conflict))
      application <- RepositoryIO.fromEither(
        MongoHiringCodecs.readApplication(document).toEither.leftMap(_ => RepositoryError.InvalidStoredData)
      )
      eventId = ApplicationEventId(
        UUID.nameUUIDFromBytes(s"${current.id.value}:cancel:status".getBytes(java.nio.charset.StandardCharsets.UTF_8))
      )
      outcome <- ApplicationLifecycle
        .changeStatus(
          ApplicationStatus.Rejected,
          access.actorId,
          at,
          Some(InterviewCancellationFeedback.text(initiator)),
          None
        )
        .run(application) match {
        case Left(_) =>
          RepositoryIO.fromEither(Right(Left(InterviewLifecycleOutcome.ApplicationNotInterview(application.status))))
        case Right((updated, change)) =>
          RepositoryIO
            .fromEither(
              ApplicationEvent
                .validate(
                  eventId,
                  updated.id,
                  Some(change.previousStatus),
                  change.newStatus,
                  change.actorId,
                  change.occurredAt,
                  change.feedback,
                  change.reason
                )
                .toEither
                .leftMap(_ => RepositoryError.InvalidEvent)
            )
            .map(history => Right(CancellationWrite(updated, change.previousStatus, history)))
      }
    } yield outcome
  }

  /** A reschedule request, proposal or acceptance only makes sense while the application is still `Interview`. */
  private def requireInterviewApplication(
      session: Option[ClientSession[IO]],
      current: InterviewWorkflow
  ): RepositoryIO[Either[InterviewLifecycleOutcome, Option[CancellationWrite]]] =
    RepositoryIO
      .lift(
        MongoSessionOperations.findOne(
          Mongo4catsCollections.documents(database, MongoCollections.Applications),
          session,
          MongoFilter.eq(MongoFields.Id, current.applicationId.value.toString)
        )
      )
      .subflatMap(_.toRight(RepositoryError.Conflict))
      .flatMap(document =>
        RepositoryIO.fromEither(
          MongoHiringCodecs.readApplication(document).toEither.leftMap(_ => RepositoryError.InvalidStoredData)
        )
      )
      .map(application =>
        Either.cond(
          application.status == ApplicationStatus.Interview,
          Option.empty[CancellationWrite],
          InterviewLifecycleOutcome.ApplicationNotInterview(application.status)
        )
      )

  private def commitLifecycle(
      session: Option[ClientSession[IO]],
      current: InterviewWorkflow,
      decision: InterviewLifecycleDecision,
      event: InterviewLifecycleEvent,
      origin: InterviewLifecycleOrigin,
      now: Instant,
      availableAt: Option[Instant]
  ): RepositoryIO[InterviewLifecycleOutcome] = {
    val next = decision.workflow
    val cancellation: RepositoryIO[Either[InterviewLifecycleOutcome, Option[CancellationWrite]]] = event match {
      case InterviewLifecycleEvent.Cancel(initiator, at) =>
        cancellationWrite(session, current, initiator, origin, at).map(_.map(Some(_)))
      case InterviewLifecycleEvent.Propose(_, _, _, _, _) | InterviewLifecycleEvent.AcceptProposal(_) |
          InterviewLifecycleEvent.RequestReschedule(_) =>
        requireInterviewApplication(session, current)
      case _ => RepositoryIO.fromEither(Right(Right(None)))
    }
    // Only Admin repair resets the retry budget, and Admin repair is the only event allowed to.
    val resetsAttempts = origin match {
      case InterviewLifecycleOrigin.Internal(cause) => cause.resetsAttempts
      case _                                        => false
    }
    cancellation.flatMap {
      case Left(outcome)                                 => RepositoryIO.fromEither(Right(outcome))
      case Right(_) if next.revision == current.revision =>
        // A no-op (the request flag is already set) writes nothing: no receipt and no retention refresh. Writing a
        // receipt per fresh key would let one actor grow storage and trigger updates without bound; repeating the
        // request is idempotent without one.
        RepositoryIO.fromEither(Right(InterviewLifecycleOutcome.Duplicate(current)))
      case Right(write) =>
        val actor = origin match {
          case InterviewLifecycleOrigin.Actor(access, _, _) => List(access.actorId)
          case InterviewLifecycleOrigin.Internal(_)         => Nil
        }
        for {
          _ <- RepositoryIO.fromEither(
            Either.cond((event == InterviewLifecycleEvent.Repair) == resetsAttempts, (), RepositoryError.InvalidEvent)
          )
          _ <- fenceSubjects(session, List(current.candidateId, current.recruiterId, current.initiatedBy) ++ actor)
          _ <- guardWrite(
            workflows,
            session,
            MongoFilter.and(
              (MongoFilter.eq(MongoFields.Id, current.id.value.toString) ::
                MongoFilter.eq(DocumentTypeField, "workflow") ::
                MongoFilter.eq(RevisionField, current.revision) :: participantPredicate(origin))*
            ),
            MongoUpdate.combine(
              (List(
                MongoUpdate.set(RevisionField, next.revision),
                MongoUpdate.set("phase", next.phase.toString),
                MongoUpdate.set("notified", next.notified.toList.map(_.toString).sorted.asJava),
                MongoInterviewWorkflowLifecycleCodec.stateUpdate(next)
              ) ++ (if (resetsAttempts) List(MongoUpdate.set("attemptEpochRevision", next.revision)) else Nil))*
            )
          )
          _ <- write.traverse_(applyCancellation(session, current, origin, _))
          _ <- recordLifecycleReceipt(session, current, next.revision, origin, now)
          _ <- if (resetsAttempts) supersedeGatingCommands(session, current) else RepositoryIO.fromEither(Right(()))
          _ <- decision.commands.zipWithIndex.traverse_ { case (command, ordinal) =>
            val dueAt = command match {
              case InterviewLifecycleCommand.ExpireProposal(at) => Some(at)
              case _                                            => None
            }
            insert(
              session,
              commands,
              commandDocument(
                InterviewWorkflowCommandRecord(
                  current.id,
                  stepId(current.id, next.revision, ordinal),
                  next.revision,
                  command,
                  command match {
                    case InterviewLifecycleCommand.RequireRepair(_) => InterviewWorkflowCommandState.RepairRequired
                    case _                                          => InterviewWorkflowCommandState.Pending
                  },
                  publicationAttempts = 0,
                  availableAt = dueAt.orElse(availableAt).getOrElse(now),
                  // Expiry is due work: its message is as old as the moment it falls due, not as the proposal.
                  occurredAt = dueAt.getOrElse(now)
                )
              )
            )
          }
          _ <-
            // Idempotent: every transition that ends in a settled phase refreshes the evidence it just wrote.
            if (Set(InterviewWorkflowPhase.Completed, InterviewWorkflowPhase.Cancelled)(next.phase))
              retainCompleted(session, next, now)
            // An open proposal, cancellation or reschedule is live work: its evidence must not expire.
            else if (current.phase == InterviewWorkflowPhase.Completed) releaseRetention(session, next)
            else RepositoryIO.fromEither(Right(()))
        } yield InterviewLifecycleOutcome.Applied(next)
    }
  }

  /** Admin repair supersedes every earlier intent that gated the workflow. Informational notifications are independent
    * of the workflow phase: they stay deliverable in every phase, so repair neither supersedes nor resends them.
    */
  private def supersedeGatingCommands(
      session: Option[ClientSession[IO]],
      current: InterviewWorkflow
  ): RepositoryIO[Unit] =
    RepositoryIO
      .lift(
        MongoSessionOperations.updateMany(
          commands,
          session,
          MongoFilter.and(
            MongoFilter.eq(WorkflowIdField, current.id.value.toString),
            MongoFilter.lte(RevisionField, current.revision),
            MongoFilter.or(
              MongoFilter.ne(s"$CommandField.kind", MongoInterviewWorkflowCommandCodec.LifecycleKinds.NotifyKind),
              MongoFilter.in(
                s"$CommandField.notificationKind",
                List(InterviewNotificationKind.Cancelled.toString, InterviewNotificationKind.Rescheduled.toString)
              )
            )
          ),
          MongoUpdate.combine(
            MongoUpdate.set(CommandStateField, InterviewWorkflowCommandState.Superseded.toString),
            MongoUpdate.unset(OwnerField),
            MongoUpdate.unset(FencingTokenField),
            MongoUpdate.unset(LeaseUntilField)
          )
        )
      )
      .void

  private def recordLifecycleReceipt(
      session: Option[ClientSession[IO]],
      current: InterviewWorkflow,
      revision: Long,
      origin: InterviewLifecycleOrigin,
      now: Instant
  ): RepositoryIO[Unit] = origin match {
    case InterviewLifecycleOrigin.Actor(access, requestKey, fingerprint) =>
      insert(
        session,
        workflows,
        requestReceiptDocument(requestReceiptId(access.actorId, requestKey), current.id, fingerprint, now)
      )
    case InterviewLifecycleOrigin.Internal(cause) =>
      insert(
        session,
        inbox,
        inboxDocument(s"${current.id.value}:${cause.receiptIdentity}", current.id, cause.receiptIdentity, revision, now)
          .append("auditActorId", cause.auditActor.map(_.value.toString).orNull)
      )
  }

  private def applyCancellation(
      session: Option[ClientSession[IO]],
      current: InterviewWorkflow,
      origin: InterviewLifecycleOrigin,
      write: CancellationWrite
  ): RepositoryIO[Unit] = {
    val applications = Mongo4catsCollections.documents(database, MongoCollections.Applications)
    val ownerGuard = origin match {
      case InterviewLifecycleOrigin.Actor(access, _, _) if access.role == UserRole.Recruiter =>
        guardWrite(
          Mongo4catsCollections.documents(database, MongoCollections.Jobs),
          session,
          MongoFilter.and(
            MongoFilter.eq(MongoFields.Id, write.application.jobId.value.toString),
            MongoFilter.eq(MongoFields.RecruiterId, access.actorId.value.toString)
          ),
          MongoUpdate.inc(MongoFields.Version, 1L)
        )
      case _ => RepositoryIO.fromEither(Right(()))
    }
    ownerGuard *>
      guardWrite(
        applications,
        session,
        MongoFilter.and(
          MongoFilter.eq(MongoFields.Id, current.applicationId.value.toString),
          MongoFilter.eq(MongoFields.Status, write.previousStatus.toString)
        ),
        MongoUpdate.combine(
          MongoUpdate.set(MongoFields.Status, write.application.status.toString),
          MongoUpdate.set(MongoFields.UpdatedAt, Date.from(write.application.updatedAt))
        )
      ) *>
      insert(
        session,
        Mongo4catsCollections.documents(database, MongoCollections.ApplicationEvents),
        MongoHiringCodecs.event(write.history)
      ) *>
      insertOperationalEvents(
        Mongo4catsCollections.documents(database, MongoCollections.EventOutbox),
        session,
        List(OperationalEvents.statusChanged(write.history.id.value, write.application, write.history)),
        write.application.updatedAt,
        diagnostics
      )
  }

  private def releaseRetention(session: Option[ClientSession[IO]], workflow: InterviewWorkflow): RepositoryIO[Unit] =
    retentionTargets(workflow.id).traverse_((collection, related) =>
      RepositoryIO
        .lift(
          MongoSessionOperations
            .updateMany(collection, session, related, MongoUpdate.unset(MongoFields.RetentionExpiresAt))
        )
        .void
    )

  private def retentionTargets(id: InterviewWorkflowId): List[(IO[MongoSessionOperations.Documents], MongoFilter)] =
    MongoInterviewWorkflowRepository
      .retentionTargets(id)
      .map((name, filter) => Mongo4catsCollections.documents(database, name) -> filter)

  override def claimDueCommands(
      workerId: String,
      now: Instant,
      leaseUntil: Instant,
      limit: Int
  ): RepositoryIO[List[ClaimedInterviewWorkflowCommand]] =
    if (workerId.isEmpty || workerId.length > 128 || !leaseUntil.isAfter(now) || limit < 0 || limit > 64)
      RepositoryIO.fromEither(Left(RepositoryError.InvalidStoredData))
    else if (limit == 0) RepositoryIO.fromEither(Right(Nil))
    else
      claimOne(workerId, now, leaseUntil).flatMap {
        case None        => RepositoryIO.fromEither(Right(Nil))
        case Some(claim) => claimDueCommands(workerId, now, leaseUntil, limit - 1).map(claim :: _)
      }

  private def claimOne(
      workerId: String,
      now: Instant,
      leaseUntil: Instant
  ): RepositoryIO[Option[ClaimedInterviewWorkflowCommand]] =
    RepositoryIO.lift(IO.randomUUID).flatMap { token =>
      val available = MongoFilter.and(
        MongoFilter.in(
          CommandStateField,
          List(InterviewWorkflowCommandState.Pending.toString, InterviewWorkflowCommandState.ResultPending.toString)
        ),
        MongoFilter.lte(MongoFields.AvailableAt, Date.from(now))
      )
      val expired = MongoFilter.and(
        MongoFilter.eq(CommandStateField, InterviewWorkflowCommandState.Claimed.toString),
        MongoFilter.lte(LeaseUntilField, Date.from(now))
      )
      val update = MongoUpdate.combine(
        MongoUpdate.set(CommandStateField, InterviewWorkflowCommandState.Claimed.toString),
        MongoUpdate.set(OwnerField, workerId),
        MongoUpdate.set(FencingTokenField, token.toString),
        MongoUpdate.set(LeaseUntilField, Date.from(leaseUntil))
      )
      val options = new FindOneAndUpdateOptions()
        .returnDocument(ReturnDocument.AFTER)
        .sort(Sorts.ascending(MongoFields.AvailableAt, MongoFields.Id))
      MongoRepositorySupport.repositoryGuard(diagnostics, "interviewWorkflow.claim") {
        RepositoryIO
          .lift(
            commands.flatMap(_.findOneAndUpdate(MongoFilter.or(available, expired).bson, update.bson, options))
          )
          .flatMap(_.traverse(document => RepositoryIO.fromEither(readClaim(document))))
      }(_ => Left(RepositoryError.Unavailable))
    }

  override def renewPublication(
      claim: ClaimedInterviewWorkflowCommand,
      now: Instant,
      leaseUntil: Instant
  ): RepositoryIO[Boolean] =
    if (!leaseUntil.isAfter(now)) RepositoryIO.fromEither(Left(RepositoryError.InvalidStoredData))
    else
      MongoRepositorySupport.repositoryGuard(diagnostics, "interviewWorkflow.renewPublication") {
        RepositoryIO
          .lift(
            MongoSessionOperations.updateOne(
              commands,
              None,
              claimFilter(claim, now),
              MongoUpdate.set(LeaseUntilField, Date.from(leaseUntil))
            )
          )
          .subflatMap {
            case Some(result) => Right(result.getMatchedCount == 1L)
            case None         => Left(RepositoryError.MissingWriteResult)
          }
      }(_ => Left(RepositoryError.Unavailable))

  override def markPublished(claim: ClaimedInterviewWorkflowCommand, publishedAt: Instant): RepositoryIO[Unit] =
    transitionClaim(
      claim,
      MongoUpdate.combine(
        MongoUpdate.set(
          CommandStateField,
          (if (claim.record.result.nonEmpty) InterviewWorkflowCommandState.ResultPublished
           else InterviewWorkflowCommandState.Published).toString
        ),
        MongoUpdate.set("publishedAt", Date.from(publishedAt)),
        MongoUpdate.unset(OwnerField),
        MongoUpdate.unset(FencingTokenField),
        MongoUpdate.unset(LeaseUntilField)
      ),
      "interviewWorkflow.markPublished",
      publishedAt
    )

  override def retry(
      claim: ClaimedInterviewWorkflowCommand,
      now: Instant,
      availableAt: Instant,
      failureCode: String
  ): RepositoryIO[Unit] =
    if (!safeFailureCode(failureCode)) RepositoryIO.fromEither(Left(RepositoryError.InvalidStoredData))
    else
      transitionClaim(
        claim,
        MongoUpdate.combine(
          MongoUpdate.set(CommandStateField, InterviewWorkflowCommandState.Pending.toString),
          availableFrom(claim.record.command, availableAt),
          MongoUpdate.set(FailureCodeField, failureCode),
          MongoUpdate.unset(OwnerField),
          MongoUpdate.unset(FencingTokenField),
          MongoUpdate.unset(LeaseUntilField)
        ),
        "interviewWorkflow.retry",
        now
      )

  override def requireRepair(
      claim: ClaimedInterviewWorkflowCommand,
      now: Instant,
      failureCode: String
  ): RepositoryIO[InterviewPublicationResolution] =
    if (!safeFailureCode(failureCode)) RepositoryIO.fromEither(Left(RepositoryError.InvalidStoredData))
    else
      transactionRunner.run { session =>
        for {
          workflow <- loadWorkflow(claim.record.workflowId, session).subflatMap(_.toRight(RepositoryError.Conflict))
          _ <- fenceSubjects(session, List(workflow.candidateId, workflow.recruiterId, workflow.initiatedBy))
          disposition <- publicationDisposition(session, workflow, claim.record, 0)
          resolution <- disposition match {
            case InterviewPublicationDisposition.Supersede =>
              guardWrite(
                commands,
                session,
                claimFilter(claim, now),
                terminalCommand(InterviewWorkflowCommandState.Superseded, failureCode, now)
              )
                .as(InterviewPublicationResolution.Superseded)
            case _ =>
              applyRepair(
                session,
                workflow,
                claim.record.command,
                InterviewAdvanceCause.PublicationExhausted(claim.record.stepId),
                failureCode,
                now
              ).flatMap { resolvedWorkflow =>
                // An expiry whose give-up expired the proposal needs no repair: its row is superseded.
                val rowState =
                  if (resolvedWorkflow) InterviewWorkflowCommandState.Superseded
                  else InterviewWorkflowCommandState.RepairRequired
                guardWrite(commands, session, claimFilter(claim, now), terminalCommand(rowState, failureCode, now))
                  .as(InterviewPublicationResolution.Repaired)
              }
          }
        } yield resolution
      }

  /** The row becomes available at `at`. An expiry intent states its own due time, which the stored shape requires to
    * equal the row's availability, so both move together; it is only ever moved later than the proposal's expiry.
    */
  private def availableFrom(command: InterviewCommand, at: Instant): MongoUpdate = command match {
    case InterviewLifecycleCommand.ExpireProposal(_) =>
      MongoUpdate.combine(
        MongoUpdate.set(MongoFields.AvailableAt, Date.from(at)),
        MongoUpdate.set(s"$CommandField.${MongoFields.AvailableAt}", Date.from(at))
      )
    case _ => MongoUpdate.set(MongoFields.AvailableAt, Date.from(at))
  }

  private def terminalCommand(state: InterviewWorkflowCommandState, reason: String, now: Instant): MongoUpdate =
    MongoUpdate.combine(
      MongoUpdate.set(CommandStateField, state.toString),
      MongoUpdate.set(FailureCodeField, reason),
      MongoUpdate.set("finishedAt", Date.from(now)),
      MongoUpdate.unset(OwnerField),
      MongoUpdate.unset(FencingTokenField),
      MongoUpdate.unset(LeaseUntilField)
    )

  /** Exhausted retries: a scheduling command moves the scheduling workflow to `RepairRequired`, a cancel or reschedule
    * command moves it through the lifecycle policy (recording the origin phase), and an informational notification
    * never changes the workflow: only its own command becomes `RepairRequired`. The result is true only when an
    * expiry's give-up resolved the workflow by expiring its proposal.
    */
  private def applyRepair(
      session: Option[ClientSession[IO]],
      workflow: InterviewWorkflow,
      command: InterviewCommand,
      cause: InterviewAdvanceCause,
      reason: String,
      now: Instant
  ): RepositoryIO[Boolean] = command match {
    case _ if InterviewCommands.isInformational(command) => RepositoryIO.fromEither(Right(false))
    case lifecycle: InterviewLifecycleCommand            =>
      // Expiry has no external effect: giving up on it expires the proposal (and, if that is no longer possible, only
      // marks the command), so a proposal is never left open by a failed expiry and the claimer is never starved.
      val event = InterviewCommands.giveUpEvent(lifecycle, reason, now)
      InterviewLifecyclePolicy.decide(workflow, workflow.revision, event) match {
        case Left(_) if InterviewCommands.isExpiry(lifecycle) => RepositoryIO.fromEither(Right(false))
        case Left(_)                                          => RepositoryIO.fromEither(Left(RepositoryError.Conflict))
        case Right(decision)                                  =>
          commitLifecycle(session, workflow, decision, event, InterviewLifecycleOrigin.Internal(cause), now, None)
            .as(InterviewCommands.isExpiry(lifecycle))
      }
    case _: InterviewWorkflowCommand => applyScheduleRepair(session, workflow, cause, reason, now).as(false)
  }

  private def applyScheduleRepair(
      session: Option[ClientSession[IO]],
      workflow: InterviewWorkflow,
      cause: InterviewAdvanceCause,
      reason: String,
      now: Instant
  ): RepositoryIO[Unit] =
    RepositoryIO
      .fromEither(
        InterviewWorkflow
          .decide(workflow, workflow.revision, InterviewWorkflowEvent.RetryExhausted(reason))
          .leftMap(_ => RepositoryError.Conflict)
      )
      .flatMap { case InterviewWorkflowDecision(next, emitted) =>
        guardWrite(
          workflows,
          session,
          MongoFilter.and(
            MongoFilter.eq(MongoFields.Id, workflow.id.value.toString),
            MongoFilter.eq(RevisionField, workflow.revision)
          ),
          MongoUpdate
            .combine(MongoUpdate.set("phase", next.phase.toString), MongoUpdate.set(RevisionField, next.revision))
        ) *>
          insert(
            session,
            inbox,
            inboxDocument(
              s"${workflow.id.value}:${cause.receiptIdentity}",
              workflow.id,
              cause.receiptIdentity,
              next.revision,
              now
            )
          ) *>
          emitted.zipWithIndex.traverse_ { case (command, ordinal) =>
            insert(
              session,
              commands,
              commandDocument(
                InterviewWorkflowCommandRecord(
                  workflow.id,
                  stepId(workflow.id, next.revision, ordinal),
                  next.revision,
                  command,
                  InterviewWorkflowCommandState.RepairRequired,
                  0,
                  now,
                  now
                )
              )
            )
          }
      }

  private def transitionClaim(
      claim: ClaimedInterviewWorkflowCommand,
      update: MongoUpdate,
      operation: String,
      now: Instant
  ): RepositoryIO[Unit] =
    MongoRepositorySupport.repositoryGuard(diagnostics, operation) {
      RepositoryIO
        .lift(
          MongoSessionOperations
            .updateOne(commands, None, claimFilter(claim, now), update)
        )
        .flatMap {
          case Some(result) if result.getMatchedCount == 1L => RepositoryIO.fromEither(Right(()))
          case Some(_)                                      => RepositoryIO.fromEither(Left(RepositoryError.Conflict))
          case None => RepositoryIO.fromEither(Left(RepositoryError.MissingWriteResult))
        }
    }(_ => Left(RepositoryError.Unavailable))

  private def claimFilter(claim: ClaimedInterviewWorkflowCommand, now: Instant): MongoFilter =
    MongoFilter.and(
      MongoFilter.eq(MongoFields.Id, commandId(claim.record.workflowId, claim.record.stepId)),
      MongoFilter.eq(CommandStateField, InterviewWorkflowCommandState.Claimed.toString),
      MongoFilter.eq(OwnerField, claim.owner),
      MongoFilter.eq(FencingTokenField, claim.fencingToken.toString),
      MongoFilter.eq(RevisionField, java.lang.Long.valueOf(claim.record.revision)),
      MongoFilter.gt(LeaseUntilField, Date.from(now))
    )

  override def reserveIfAvailable(
      reservation: InterviewCalendarReservation,
      execution: Option[ClaimedInterviewWorkflowCommand] = None
  ): InterviewProviderIO[InterviewCalendarReservation] = {
    val participants = List(reservation.candidateId, reservation.recruiterId).distinct.sortBy(_.value.toString)
    val generation = InterviewCalendarKeys.reservationGeneration(reservation.workflowId, reservation.idempotencyKey)
    val write = RepositoryIO.fromEither(generation.toRight(RepositoryError.Conflict)).flatMap { generation =>
      transactionRunner.run { session =>
        for {
          _ <- fenceSubjects(session, participants)
          _ <- fenceExecution(session, reservation.workflowId, execution)
          workflow <- loadWorkflow(reservation.workflowId, session).subflatMap(_.toRight(RepositoryError.Conflict))
          actualNow <- RepositoryIO.lift(IO.realTimeInstant)
          // Only the original scheduling hold is bounded by the pre-commit window; a replacement hold follows consent.
          _ <- RepositoryIO.fromEither(
            Either.cond(generation > 0 || actualNow.isBefore(workflow.preCommitDeadline), (), RepositoryError.Conflict)
          )
          _ <- RepositoryIO.fromEither(
            Either.cond(
              workflow.candidateId == reservation.candidateId &&
                workflow.recruiterId == reservation.recruiterId &&
                InterviewCalendarFence.holdableGeneration(workflow).contains(generation) &&
                InterviewCalendarFence.holdInterval(workflow, generation).contains(reservation.interval),
              (),
              RepositoryError.Conflict
            )
          )
          _ <- guardWrite(
            workflows,
            session,
            MongoFilter.and(
              MongoFilter.eq(MongoFields.Id, workflow.id.value.toString),
              MongoFilter.eq("phase", workflow.phase.toString)
            ),
            MongoUpdate.inc("providerFence", 1L)
          )
          _ <- participants.traverse_(id => touchParticipantLock(session, id))
          existing <- RepositoryIO.lift(
            MongoSessionOperations.findOne(
              reservations,
              session,
              reservationIdentity(reservation.workflowId, reservation.idempotencyKey)
            )
          )
          saved <- existing match {
            case Some(document) =>
              RepositoryIO.fromEither(decodeReservation(document).flatMap { value =>
                Either.cond(
                  sameReservation(value, reservation) && value.releasedAt.isEmpty,
                  value,
                  RepositoryError.Conflict
                )
              })
            case None =>
              // Other workflows' holds conflict; a workflow's own holds (old and replacement) never do.
              val overlap = MongoFilter.and(
                MongoFilter.in("participants", participants.map(_.value.toString)),
                MongoFilter.ne(WorkflowIdField, reservation.workflowId.value.toString),
                MongoFilter.lt(StartField, Date.from(reservation.interval.endsAt)),
                MongoFilter.gt(EndField, Date.from(reservation.interval.startsAt)),
                MongoFilter.exists(ReleasedAtField, false)
              )
              RepositoryIO.lift(MongoSessionOperations.findOne(reservations, session, overlap)).flatMap {
                case Some(_) => RepositoryIO.fromEither(Left(RepositoryError.Conflict))
                case None    =>
                  // Stored with millisecond precision, so the first answer equals every replayed answer.
                  val stored = reservation.copy(reservedAt = reservation.reservedAt.truncatedTo(ChronoUnit.MILLIS))
                  insert(session, reservations, reservationDocument(stored, generation)).as(stored)
              }
          }
        } yield saved
      }
    }
    mapProvider(write)
  }

  override def find(
      workflowId: InterviewWorkflowId,
      idempotencyKey: String
  ): InterviewProviderIO[Option[InterviewCalendarReservation]] =
    InterviewCalendarKeys.reservationKeyFor(workflowId, idempotencyKey) match {
      case None      => EitherT.rightT[IO, InterviewProviderError](Option.empty[InterviewCalendarReservation])
      case Some(key) =>
        MongoRepositorySupport
          .repositoryGuard(diagnostics, "interviewCalendar.find") {
            RepositoryIO
              .lift(
                MongoSessionOperations.findOne(reservations, None, reservationIdentity(workflowId, key))
              )
              .flatMap(_.traverse(document => RepositoryIO.fromEither(decodeReservation(document))))
          }(_ => Left(RepositoryError.Unavailable))
          .leftMap(_ => InterviewProviderError.Unavailable)
    }

  override def release(
      idempotencyKey: String,
      at: Instant,
      execution: Option[ClaimedInterviewWorkflowCommand] = None
  ): InterviewProviderIO[Unit] = {
    val result = MongoRepositorySupport.repositoryGuard(diagnostics, "interviewCalendar.release") {
      transactionRunner.run { session =>
        RepositoryIO
          .lift(MongoSessionOperations.findOne(reservations, session, MongoFilter.eq("releaseKey", idempotencyKey)))
          .flatMap {
            case None                                                    => RepositoryIO.fromEither(Right(()))
            case Some(document) if document.containsKey(ReleasedAtField) => RepositoryIO.fromEither(Right(()))
            case Some(document)                                          =>
              for {
                reservation <- RepositoryIO.fromEither(decodeReservation(document))
                _ <- fenceExecution(session, reservation.workflowId, execution)
                receipt <- RepositoryIO.lift(
                  MongoSessionOperations
                    .findOne(inbox, session, MongoFilter.eq(MongoFields.Id, s"${reservation.workflowId.value}:hiring"))
                )
                _ <- RepositoryIO.fromEither(Either.cond(receipt.isEmpty, (), RepositoryError.Conflict))
                _ <- guardWrite(
                  workflows,
                  session,
                  MongoFilter.and(
                    MongoFilter.eq(MongoFields.Id, reservation.workflowId.value.toString),
                    MongoFilter.eq("phase", InterviewWorkflowPhase.CompensationPending.toString)
                  ),
                  MongoUpdate.inc("providerFence", 1L)
                )
                _ <- markReleased(session, reservation, at)
              } yield ()
          }
      }
    }(_ => Left(RepositoryError.Unavailable))
    result.leftMap(_ => InterviewProviderError.Unavailable)
  }

  override def cancel(
      workflowId: InterviewWorkflowId,
      idempotencyKey: String,
      at: Instant,
      execution: Option[ClaimedInterviewWorkflowCommand] = None
  ): InterviewProviderIO[InterviewCalendarCancellation] =
    InterviewCalendarKeys.cancellationGeneration(workflowId, idempotencyKey) match {
      case None =>
        EitherT.rightT[IO, InterviewProviderError](InterviewCalendarCancellation.UnknownReservation)
      case Some(generation) =>
        val released = at.truncatedTo(ChronoUnit.MILLIS)
        val reserveKey = InterviewWorkflow.reservationKey(workflowId, generation)
        val result = MongoRepositorySupport.repositoryGuard(diagnostics, "interviewCalendar.cancel") {
          transactionRunner.run { session =>
            RepositoryIO
              .lift(MongoSessionOperations.findOne(reservations, session, reservationIdentity(workflowId, reserveKey)))
              .flatMap {
                case None           => RepositoryIO.fromEither(Right(InterviewCalendarCancellation.UnknownReservation))
                case Some(document) =>
                  RepositoryIO.fromEither(decodeReservation(document)).flatMap { reservation =>
                    reservation.releasedAt match {
                      case Some(released) =>
                        RepositoryIO.fromEither(Right(InterviewCalendarCancellation.AlreadyCancelled(released)))
                      case None =>
                        for {
                          _ <- fenceExecution(session, workflowId, execution)
                          workflow <- loadWorkflow(workflowId, session).subflatMap(_.toRight(RepositoryError.Conflict))
                          // The provider only cancels what the workflow currently requires cancelled.
                          _ <- RepositoryIO.fromEither(
                            Either.cond(
                              InterviewCalendarFence.cancellableGeneration(workflow).contains(generation),
                              (),
                              RepositoryError.Conflict
                            )
                          )
                          _ <- guardWrite(
                            workflows,
                            session,
                            MongoFilter.and(
                              MongoFilter.eq(MongoFields.Id, workflow.id.value.toString),
                              MongoFilter.eq("phase", workflow.phase.toString)
                            ),
                            MongoUpdate.inc("providerFence", 1L)
                          )
                          _ <- markReleased(session, reservation, released)
                        } yield InterviewCalendarCancellation.Cancelled(released)
                    }
                  }
              }
          }
        }(_ => Left(RepositoryError.Unavailable))
        mapProvider(result)
    }

  /** The single place a reservation becomes cancelled; the update is guarded so only a held reservation changes. */
  private def markReleased(
      session: Option[ClientSession[IO]],
      reservation: InterviewCalendarReservation,
      at: Instant
  ): RepositoryIO[Unit] =
    fenceSubjects(session, List(reservation.candidateId, reservation.recruiterId)) *>
      List(reservation.candidateId, reservation.recruiterId).distinct.traverse_(touchParticipantLock(session, _)) *>
      guardWrite(
        reservations,
        session,
        MongoFilter.and(
          reservationIdentity(reservation.workflowId, reservation.idempotencyKey),
          MongoFilter.exists(ReleasedAtField, false)
        ),
        MongoUpdate.set(ReleasedAtField, Date.from(at))
      )

  /** A reservation is addressed by workflow and reserve key, never by workflow alone. */
  private def reservationIdentity(workflowId: InterviewWorkflowId, reserveKey: String): MongoFilter =
    MongoFilter.and(
      MongoFilter.eq(WorkflowIdField, workflowId.value.toString),
      MongoFilter.eq("reserveKey", reserveKey)
    )

  private def touchParticipantLock(session: Option[ClientSession[IO]], participant: UserId): RepositoryIO[Unit] = {
    val update = MongoUpdate.inc("fence", java.lang.Long.valueOf(1L))
    RepositoryIO
      .lift(
        MongoSessionOperations
          .updateOne(
            participantLocks,
            session,
            MongoFilter.eq(MongoFields.Id, participant.value.toString),
            update,
            new UpdateOptions().upsert(true)
          )
      )
      .flatMap {
        case Some(result) if result.wasAcknowledged() => RepositoryIO.fromEither(Right(()))
        case Some(_)                                  => RepositoryIO.fromEither(Left(RepositoryError.Conflict))
        case None => RepositoryIO.fromEither(Left(RepositoryError.MissingWriteResult))
      }
  }

  private def fenceSubjects(session: Option[ClientSession[IO]], subjects: List[UserId]): RepositoryIO[Unit] =
    subjects.distinct
      .sortBy(_.value.toString)
      .traverse_(id =>
        guardWrite(
          Mongo4catsCollections.documents(database, MongoCollections.Users),
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
                Mongo4catsCollections.documents(database, MongoCollections.OutboxSubjectFences),
                session,
                MongoFilter.eq(MongoFields.Id, id.value.toString),
                MongoUpdate.inc(MongoFields.FencingVersion, 1L),
                new UpdateOptions().upsert(true)
              )
            )
            .subflatMap {
              case Some(result) if result.wasAcknowledged() => Right(())
              case _                                        => Left(RepositoryError.MissingWriteResult)
            }
      )

  private def fenceExecution(
      session: Option[ClientSession[IO]],
      workflowId: InterviewWorkflowId,
      execution: Option[ClaimedInterviewWorkflowCommand]
  ): RepositoryIO[Unit] = execution match {
    case None                                                 => RepositoryIO.fromEither(Right(()))
    case Some(claim) if claim.record.workflowId != workflowId => RepositoryIO.fromEither(Left(RepositoryError.Conflict))
    case Some(claim)                                          =>
      RepositoryIO
        .lift(IO.realTimeInstant)
        .flatMap(now =>
          guardWrite(
            commands,
            session,
            MongoFilter.and(
              MongoFilter.eq(MongoFields.Id, commandId(workflowId, claim.record.stepId)),
              MongoFilter.eq(CommandStateField, InterviewWorkflowCommandState.Executing.toString),
              MongoFilter.eq(OwnerField, claim.owner),
              MongoFilter.eq(FencingTokenField, claim.fencingToken.toString),
              MongoFilter.gt(LeaseUntilField, Date.from(now))
            ),
            MongoUpdate.inc("effectFence", 1L)
          )
        )
  }

  private def retainCompleted(
      session: Option[ClientSession[IO]],
      workflow: InterviewWorkflow,
      now: Instant
  ): RepositoryIO[Unit] = {
    // One expiry for all evidence: it outlives the interview, so a far-future interview stays cancellable.
    val expires = (if (workflow.interval.endsAt.isAfter(now)) workflow.interval.endsAt else now)
      .plusMillis(completedEvidenceRetention.toMillis)
    retentionTargets(workflow.id).traverse_((collection, related) =>
      RepositoryIO
        .lift(
          MongoSessionOperations.updateMany(
            collection,
            session,
            related,
            MongoUpdate.set(MongoFields.RetentionExpiresAt, Date.from(expires))
          )
        )
        .void
    )
  }

  override def deliverOnce(
      receipt: InterviewNotificationReceipt,
      execution: Option[ClaimedInterviewWorkflowCommand] = None
  ): InterviewProviderIO[InterviewNotificationReceipt] = {
    val result = MongoRepositorySupport.repositoryGuard(diagnostics, "interviewNotification.deliverOnce") {
      transactionRunner.run { session =>
        fenceExecution(session, receipt.workflowId, execution) *>
          loadWorkflow(receipt.workflowId, session)
            .subflatMap(_.toRight(RepositoryError.Conflict))
            .flatMap(workflow =>
              for {
                _ <- fenceSubjects(session, List(workflow.candidateId, workflow.recruiterId, workflow.initiatedBy))
                _ <- RepositoryIO.fromEither(
                  Either.cond(
                    InterviewNotificationFence.permits(receipt.kind, workflow.phase),
                    (),
                    RepositoryError.Conflict
                  )
                )
                _ <- guardWrite(
                  workflows,
                  session,
                  MongoFilter.and(
                    MongoFilter.eq(MongoFields.Id, workflow.id.value.toString),
                    MongoFilter.eq("phase", workflow.phase.toString)
                  ),
                  MongoUpdate.inc("providerFence", 1L)
                )
                receiptExists <- RepositoryIO.lift(
                  MongoSessionOperations
                    .findOne(inbox, session, MongoFilter.eq(MongoFields.Id, s"${workflow.id.value}:hiring"))
                )
                // Only the scheduling notification requires the hiring commit; later kinds follow other durable events.
                _ <- RepositoryIO.fromEither(
                  Either.cond(
                    receipt.kind != InterviewNotificationKind.Scheduled || receiptExists.nonEmpty,
                    (),
                    RepositoryError.Conflict
                  )
                )
              } yield ()
            ) *>
          RepositoryIO
            .lift(
              MongoSessionOperations.findOne(
                notificationReceipts,
                session,
                MongoFilter.eq(MongoFields.Id, receipt.idempotencyKey)
              )
            )
            .flatMap {
              case Some(document) =>
                RepositoryIO.fromEither(decodeNotification(document).flatMap { existing =>
                  Either.cond(sameNotification(existing, receipt), existing, RepositoryError.Conflict)
                })
              case None =>
                RepositoryIO
                  .lift(MongoSessionOperations.insertOne(notificationReceipts, session, notificationDocument(receipt)))
                  .flatMap {
                    case Some(inserted) if inserted.wasAcknowledged() => RepositoryIO.fromEither(Right(receipt))
                    case Some(_) => RepositoryIO.fromEither(Left(RepositoryError.Conflict))
                    case None    => RepositoryIO.fromEither(Left(RepositoryError.MissingWriteResult))
                  }
            }
      }
    }(_ => Left(RepositoryError.Unavailable))
    mapProvider(result)
  }

  override def find(idempotencyKey: String): InterviewProviderIO[Option[InterviewNotificationReceipt]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "interviewNotification.find") {
        RepositoryIO
          .lift(
            MongoSessionOperations
              .findOne(notificationReceipts, None, MongoFilter.eq(MongoFields.Id, idempotencyKey))
          )
          .flatMap(_.traverse(document => RepositoryIO.fromEither(decodeNotification(document))))
      }(_ => Left(RepositoryError.Unavailable))
      .leftMap(_ => InterviewProviderError.Unavailable)

  private def mapProvider[A](result: RepositoryIO[A]): InterviewProviderIO[A] =
    result.leftMap {
      case RepositoryError.Conflict | RepositoryError.DuplicateApplication => InterviewProviderError.Conflict
      case _                                                               => InterviewProviderError.Unavailable
    }

  private def insert(
      session: Option[ClientSession[IO]],
      collection: IO[MongoSessionOperations.Documents],
      document: Document
  ): RepositoryIO[Unit] =
    RepositoryIO.lift(MongoSessionOperations.insertOne(collection, session, document)).flatMap {
      case Some(result) if result.wasAcknowledged() => RepositoryIO.fromEither(Right(()))
      case Some(_)                                  => RepositoryIO.fromEither(Left(RepositoryError.Conflict))
      case None                                     => RepositoryIO.fromEither(Left(RepositoryError.MissingWriteResult))
    }

  private def loadWorkflow(
      id: InterviewWorkflowId,
      session: Option[ClientSession[IO]]
  ): RepositoryIO[Option[InterviewWorkflow]] =
    RepositoryIO
      .lift(
        MongoSessionOperations
          .findOne(
            workflows,
            session,
            MongoFilter.and(
              MongoFilter.eq(MongoFields.Id, id.value.toString),
              MongoFilter.eq(DocumentTypeField, "workflow")
            )
          )
      )
      .flatMap(_.traverse(document => RepositoryIO.fromEither(decodeWorkflow(document))))

  private def workflowDocument(value: InterviewWorkflow): Document = {
    val base = new Document(MongoFields.Id, value.id.value.toString)
      .append(DocumentTypeField, "workflow")
      .append("applicationId", value.applicationId.value.toString)
      .append("initiatedBy", value.initiatedBy.value.toString)
      .append("candidateId", value.candidateId.value.toString)
      .append("recruiterId", value.recruiterId.value.toString)
      .append(
        MongoFields.SubjectIds,
        List(value.candidateId.value.toString, value.recruiterId.value.toString).distinct.asJava
      )
      .append(StartField, Date.from(value.interval.startsAt))
      .append(EndField, Date.from(value.interval.endsAt))
      .append("preCommitDeadline", Date.from(value.preCommitDeadline))
      .append(IdempotencyKeyField, value.idempotencyKey.toString)
      .append(RevisionField, value.revision)
      .append("phase", value.phase.toString)
      .append("notified", value.notified.toList.map(_.toString).sorted.asJava)
    MongoInterviewWorkflowLifecycleCodec.insertFields(value).foldLeft(base) { case (document, (field, fieldValue)) =>
      document.append(field, fieldValue)
    }
  }

  private def requestReceiptDocument(
      receiptId: String,
      workflowId: InterviewWorkflowId,
      fingerprint: MutationReceiptFingerprint,
      createdAt: Instant
  ): Document =
    new Document(MongoFields.Id, receiptId)
      .append(DocumentTypeField, "requestReceipt")
      .append(RequestWorkflowField, workflowId.value.toString)
      .append(RequestFingerprintField, fingerprint.value)
      .append(MongoFields.CreatedAt, Date.from(createdAt))

  private def requestReceiptId(actorId: UserId, requestKey: UUID): String =
    s"request:${actorId.value}:$requestKey"

  private def decodeReceipt(document: Document): Either[RepositoryError, Option[(InterviewWorkflowId, String)]] =
    for {
      workflow <- string(document, RequestWorkflowField).flatMap(uuid).map(InterviewWorkflowId.apply)
      fingerprint <- string(document, RequestFingerprintField)
    } yield Some(workflow -> fingerprint)

  private def requestReceipt(
      receiptId: String
  ): RepositoryIO[Option[(InterviewWorkflowId, String)]] =
    RepositoryIO
      .lift(MongoSessionOperations.findOne(workflows, None, MongoFilter.eq(MongoFields.Id, receiptId)))
      .flatMap {
        case None           => RepositoryIO.fromEither(Right(None))
        case Some(document) =>
          RepositoryIO.fromEither(decodeReceipt(document))
      }

  private def inboxDocument(
      id: String,
      workflowId: InterviewWorkflowId,
      messageId: String,
      revision: Long,
      at: Instant
  ): Document =
    new Document(MongoFields.Id, id)
      .append(DocumentTypeField, "inboxReceipt")
      .append(WorkflowIdField, workflowId.value.toString)
      .append(MessageIdField, messageId)
      .append(RevisionField, revision)
      .append(MongoFields.OccurredAt, Date.from(at))

  private def commandDocument(record: InterviewWorkflowCommandRecord): Document =
    commandEnvelope(
      record.workflowId,
      record.stepId,
      record.revision,
      record.command match {
        case scheduling: InterviewWorkflowCommand => encodeCommand(scheduling)
        case lifecycle: InterviewLifecycleCommand => MongoInterviewWorkflowCommandCodec.encodeLifecycle(lifecycle)
      },
      record.state,
      record.publicationAttempts,
      record.executionAttempts,
      record.availableAt,
      record.occurredAt
    )

  private def commandEnvelope(
      workflowId: InterviewWorkflowId,
      stepId: String,
      revision: Long,
      command: Document,
      state: InterviewWorkflowCommandState,
      publicationAttempts: Int,
      executionAttempts: Int,
      availableAt: Instant,
      occurredAt: Instant
  ): Document =
    new Document(MongoFields.Id, commandId(workflowId, stepId))
      .append(DocumentTypeField, "command")
      .append(WorkflowIdField, workflowId.value.toString)
      .append(StepIdField, stepId)
      .append(RevisionField, revision)
      .append(CommandField, command)
      .append(CommandStateField, state.toString)
      .append(MongoFields.Attempts, publicationAttempts)
      .append("executionAttempts", executionAttempts)
      .append(MongoFields.AvailableAt, Date.from(availableAt))
      .append(MongoFields.OccurredAt, Date.from(occurredAt))

  private def commandId(workflowId: InterviewWorkflowId, stepId: String): String =
    s"${workflowId.value}:$stepId"

  private def stepId(workflowId: InterviewWorkflowId, revision: Long, ordinal: Int): String =
    s"${workflowId.value}:$revision:$ordinal"

  private def encodeCommand(command: InterviewWorkflowCommand): Document = command match {
    case InterviewWorkflowCommand.ReserveCalendarSlot(key) =>
      new Document("kind", "reserveCalendar").append(IdempotencyKeyField, key)
    case InterviewWorkflowCommand.LookupCalendarReservation(id) =>
      new Document("kind", "lookupCalendar").append(WorkflowIdField, id.value.toString)
    case InterviewWorkflowCommand.CommitAcceptedToInterview(status) =>
      new Document("kind", "commitInterview").append("expectedStatus", status.toString)
    case InterviewWorkflowCommand.LookupStatusCommitReceipt(id) =>
      new Document("kind", "lookupStatusCommit").append(WorkflowIdField, id.value.toString)
    case InterviewWorkflowCommand.ReleaseCalendarSlot(key) =>
      new Document("kind", "releaseCalendar").append(IdempotencyKeyField, key)
    case InterviewWorkflowCommand.Notify(participant, key) =>
      new Document("kind", "notify").append("participant", participant.toString).append(IdempotencyKeyField, key)
    case InterviewWorkflowCommand.LookupNotificationReceipt(participant, key) =>
      new Document("kind", "lookupNotification")
        .append("participant", participant.toString)
        .append(IdempotencyKeyField, key)
    case InterviewWorkflowCommand.RequireRepair(reason) =>
      new Document("kind", "requireRepair").append("reason", reason)
  }

  private def readClaim(document: Document): Either[RepositoryError, ClaimedInterviewWorkflowCommand] =
    for {
      record <- decodeCommandRecord(document)
      owner <- string(document, OwnerField)
      token <- string(document, FencingTokenField).flatMap(uuid)
      lease <- instant(document, LeaseUntilField)
    } yield ClaimedInterviewWorkflowCommand(record, owner, token, lease)

  private def decodeCommandRecord(document: Document): Either[RepositoryError, InterviewWorkflowCommandRecord] =
    MongoInterviewWorkflowCommandCodec.decode(document)

  /** The original hold keeps the workflow-id identity; replacement holds are identified by their own reserve key. */
  private def reservationDocument(value: InterviewCalendarReservation, generation: Int): Document =
    new Document(
      MongoFields.Id,
      if (generation == 0) value.workflowId.value.toString else value.idempotencyKey
    )
      .append(WorkflowIdField, value.workflowId.value.toString)
      .append("reserveKey", value.idempotencyKey)
      .append(
        "releaseKey",
        if (generation == 0) s"${value.workflowId.value}:release" else s"${value.workflowId.value}:release:g$generation"
      )
      .append("candidateId", value.candidateId.value.toString)
      .append("recruiterId", value.recruiterId.value.toString)
      .append("participants", List(value.candidateId.value.toString, value.recruiterId.value.toString).distinct.asJava)
      .append(StartField, Date.from(value.interval.startsAt))
      .append(EndField, Date.from(value.interval.endsAt))
      .append("reservedAt", Date.from(value.reservedAt))

  private def decodeReservation(document: Document): Either[RepositoryError, InterviewCalendarReservation] =
    for {
      workflowId <- string(document, WorkflowIdField).flatMap(uuid).map(InterviewWorkflowId.apply)
      reserveKey <- string(document, "reserveKey")
      candidate <- string(document, "candidateId").flatMap(uuid).map(UserId.apply)
      recruiter <- string(document, "recruiterId").flatMap(uuid).map(UserId.apply)
      starts <- instant(document, StartField)
      ends <- instant(document, EndField)
      reserved <- instant(document, "reservedAt")
      released <- optionalInstant(document, ReleasedAtField)
    } yield InterviewCalendarReservation(
      workflowId,
      reserveKey,
      candidate,
      recruiter,
      InterviewInterval(starts, ends),
      reserved,
      released
    )

  private def sameReservation(left: InterviewCalendarReservation, right: InterviewCalendarReservation): Boolean =
    left.workflowId == right.workflowId && left.idempotencyKey == right.idempotencyKey &&
      left.candidateId == right.candidateId && left.recruiterId == right.recruiterId && left.interval == right.interval

  private def notificationDocument(value: InterviewNotificationReceipt): Document =
    new Document(MongoFields.Id, value.idempotencyKey)
      .append(WorkflowIdField, value.workflowId.value.toString)
      .append("recipientId", value.recipientId.value.toString)
      .append(MongoFields.SubjectIds, List(value.recipientId.value.toString).asJava)
      .append("participant", value.participant.toString)
      .append("kind", value.kind.toString)
      .append("deliveredAt", Date.from(value.deliveredAt))

  private def decodeNotification(document: Document): Either[RepositoryError, InterviewNotificationReceipt] =
    for {
      workflowId <- string(document, WorkflowIdField).flatMap(uuid).map(InterviewWorkflowId.apply)
      recipient <- string(document, "recipientId").flatMap(uuid).map(UserId.apply)
      participantName <- string(document, "participant")
      participant <- InterviewParticipant.values
        .find(_.toString == participantName)
        .toRight(RepositoryError.InvalidStoredData)
      kind <- Option(document.get("kind")) match {
        // Receipts written before message kinds existed are scheduling notifications.
        case None               => Right(InterviewNotificationKind.Scheduled)
        case Some(name: String) =>
          InterviewNotificationKind.values.find(_.toString == name).toRight(RepositoryError.InvalidStoredData)
        case _ => Left(RepositoryError.InvalidStoredData)
      }
      key <- string(document, MongoFields.Id)
      delivered <- instant(document, "deliveredAt")
    } yield InterviewNotificationReceipt(workflowId, recipient, participant, kind, key, delivered)

  private def sameNotification(left: InterviewNotificationReceipt, right: InterviewNotificationReceipt): Boolean =
    left.workflowId == right.workflowId && left.recipientId == right.recipientId &&
      left.participant == right.participant && left.kind == right.kind && left.idempotencyKey == right.idempotencyKey

  private def decodeWorkflow(document: Document): Either[RepositoryError, InterviewWorkflow] =
    for {
      id <- string(document, MongoFields.Id).flatMap(uuid).map(InterviewWorkflowId.apply)
      initiator <- string(document, "initiatedBy").flatMap(uuid).map(UserId.apply)
      application <- string(document, "applicationId")
        .flatMap(uuid)
        .map(com.example.graphQL.cats.domain.model.Identifiers.ApplicationId.apply)
      candidate <- string(document, "candidateId").flatMap(uuid).map(UserId.apply)
      recruiter <- string(document, "recruiterId").flatMap(uuid).map(UserId.apply)
      starts <- instant(document, StartField)
      ends <- instant(document, EndField)
      _ <- Either.cond(ends.isAfter(starts), (), RepositoryError.InvalidStoredData)
      deadline <- instant(document, "preCommitDeadline")
      requestKey <- string(document, IdempotencyKeyField).flatMap(uuid)
      revision <- long(document, RevisionField)
      phaseName <- string(document, "phase")
      phase <- InterviewWorkflowPhase.values.find(_.toString == phaseName).toRight(RepositoryError.InvalidStoredData)
      notified <- stringList(document, "notified").flatMap(names =>
        names
          .traverse(name =>
            InterviewParticipant.values.find(_.toString == name).toRight(RepositoryError.InvalidStoredData)
          )
          .map(_.toSet)
      )
      lifecycle <- MongoInterviewWorkflowLifecycleCodec.decode(document, phase)
      _ <- Either.cond(
        revision >= 0L && (!Set(InterviewWorkflowPhase.Completed, InterviewWorkflowPhase.Cancelled)(phase) ||
          notified == InterviewParticipant.values.toSet),
        (),
        RepositoryError.InvalidStoredData
      )
    } yield InterviewWorkflow(
      id,
      application,
      candidate,
      recruiter,
      InterviewInterval(starts, ends),
      deadline,
      requestKey,
      revision,
      phase,
      notified,
      initiator,
      lifecycle.generation,
      lifecycle.pendingInterval,
      lifecycle.cancelledAt,
      lifecycle.proposal,
      lifecycle.rescheduleRequestedAt,
      lifecycle.repairOrigin,
      lifecycle.skippedGenerations
    )

  private def optionalInstant(document: Document, field: String): Either[RepositoryError, Option[Instant]] =
    Option(document.get(field)) match {
      case None              => Right(None)
      case Some(value: Date) => Right(Some(value.toInstant))
      case _                 => Left(RepositoryError.InvalidStoredData)
    }

  private def instant(document: Document, field: String): Either[RepositoryError, Instant] =
    Option(document.get(field))
      .collect { case value: Date => value.toInstant }
      .toRight(RepositoryError.InvalidStoredData)

  private def string(document: Document, field: String): Either[RepositoryError, String] =
    Option(document.get(field)).collect { case value: String => value }.toRight(RepositoryError.InvalidStoredData)

  private def stringList(document: Document, field: String): Either[RepositoryError, List[String]] =
    Option(document.get(field))
      .collect { case values: java.util.List[?] => values.asScala.toList }
      .toRight(RepositoryError.InvalidStoredData)
      .flatMap(_.traverse {
        case value: String => Right(value)
        case _             => Left(RepositoryError.InvalidStoredData)
      })

  private def long(document: Document, field: String): Either[RepositoryError, Long] =
    Option(document.get(field))
      .collect { case value: java.lang.Number => value.longValue }
      .toRight(RepositoryError.InvalidStoredData)

  private def integer(document: Document, field: String): Either[RepositoryError, Int] =
    Option(document.get(field))
      .collect { case value: java.lang.Integer => value.intValue }
      .toRight(RepositoryError.InvalidStoredData)

  private def uuid(value: String): Either[RepositoryError, UUID] =
    Either.catchNonFatal(UUID.fromString(value)).leftMap(_ => RepositoryError.InvalidStoredData)

  private def safeFailureCode(value: String): Boolean =
    value.nonEmpty && value.length <= 128 && value.forall(char => char.isLetterOrDigit || char == '_' || char == '-')
}

object MongoInterviewWorkflowRepository {

  /** Every collection that keeps evidence of a workflow, with an index-served filter for it: the workflow and its
    * request receipts share a collection (`_id` and the indexed `requestWorkflowId`), the rest carry an indexed
    * `workflowId`. No branch is unindexed, so retention never scans a collection.
    */
  private[mongo] def retentionTargets(id: InterviewWorkflowId): List[(String, MongoFilter)] = {
    val identity = id.value.toString
    val ownRecords =
      MongoFilter.or(MongoFilter.eq(MongoFields.Id, identity), MongoFilter.eq("requestWorkflowId", identity))
    val evidence = MongoFilter.eq("workflowId", identity)
    List(
      MongoCollections.InterviewWorkflows -> ownRecords,
      MongoCollections.InterviewWorkflowCommands -> evidence,
      MongoCollections.InterviewWorkflowInbox -> evidence,
      MongoCollections.InterviewNotificationReceipts -> evidence,
      MongoCollections.InterviewCalendarReservations -> evidence
    )
  }

  def live(
      database: MongoDatabase[IO],
      client: MongoClient[IO],
      diagnostics: Diagnostics,
      completedEvidenceRetention: FiniteDuration = 8.days
  ): MongoInterviewWorkflowRepository =
    new MongoInterviewWorkflowRepository(
      database,
      MongoTransactionRunner
        .sessions(
          client,
          com.example.graphQL.cats.service.RepositoryError.Conflict,
          diagnostics = diagnostics,
          transientExhaustionError = RepositoryError.Unavailable
        ),
      diagnostics,
      completedEvidenceRetention
    )
}
