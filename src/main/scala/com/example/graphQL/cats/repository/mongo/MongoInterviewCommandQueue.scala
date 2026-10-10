package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.shared.StableIds
import cats.data.EitherT
import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.repository.mongo.MongoDocumentFields.Repository.*
import com.example.graphQL.cats.service.application.*
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.RepositoryError
import com.mongodb.client.model.{Sorts, UpdateOptions}
import mongo4cats.client.ClientSession
import org.bson.Document

import java.time.Instant
import java.util.{Date, UUID}

private enum RepairOutcome {
  case ProposalExpired
  case CommandOnly
}

/** Durable command queue of the interview workflow: claims, execution budgets, publication and settlement. */
final class MongoInterviewCommandQueue(
    store: MongoInterviewWorkflowStore,
    lifecycleStore: MongoInterviewLifecycleStore
) {
  import MongoInterviewFields.*
  import MongoInterviewWorkflowCodec.*
  import store.*

  def claimExecution(
      command: InterviewWorkflowCommandRecord,
      workerId: String,
      now: Instant,
      leaseUntil: Instant,
      maxAttempts: Int
  ): RepositoryIO[InterviewExecutionClaimOutcome] =
    if (workerId.isEmpty || workerId.length > 128 || !leaseUntil.isAfter(now) || maxAttempts <= 0)
      RepositoryIO.raise(RepositoryError.InvalidEvent)
    else
      RepositoryIO.lift(uuids.randomUUID).flatMap { token =>
        transactionRunner.run { session =>
          loadWorkflow(command.workflowId, session).flatMap {
            case None =>
              (InterviewExecutionClaimOutcome.AlreadyHandled: InterviewExecutionClaimOutcome).pure[RepositoryIO]
            case Some(workflow) =>
              for {
                _ <- fenceParticipants(session, workflow)
                current <- RepositoryIO.lift(
                  MongoSessionOperations.findById(commands, session, commandId(command.workflowId, command.stepId))
                )
                outcome <- current match {
                  case None =>
                    (InterviewExecutionClaimOutcome.AlreadyHandled: InterviewExecutionClaimOutcome).pure[RepositoryIO]
                  case Some(document) =>
                    RepositoryIO.fromEither(MongoInterviewWorkflowCommandCodec.decode(document)).flatMap { stored =>
                      InterviewExecutionPolicy.admission(workflow, stored) match {
                        case InterviewExecutionAdmission.AlreadyHandled =>
                          (InterviewExecutionClaimOutcome.AlreadyHandled: InterviewExecutionClaimOutcome)
                            .pure[RepositoryIO]
                        case InterviewExecutionAdmission.InspectLease => {
                          RepositoryIO.fromEither(readLease(document)).flatMap { lease =>
                            InterviewExecutionPolicy.lease(lease, now) match {
                              case InterviewExecutionLeaseDisposition.Busy =>
                                RepositoryIO.pure(InterviewExecutionClaimOutcome.Busy)
                              case InterviewExecutionLeaseDisposition.Reconcile =>
                                guardWrite(
                                  commands,
                                  session,
                                  MongoFilter.and(
                                    MongoFilter.eq(MongoFields.Id, commandId(stored.workflowId, stored.stepId)),
                                    MongoFilter.eq(CommandStateField, InterviewWorkflowCommandState.Executing.toString),
                                    MongoFilter.eq(Lease.owner, lease.owner),
                                    MongoFilter.eq(Lease.token, lease.token.toString),
                                    MongoFilter.lte(Lease.until, now.toDate),
                                    MongoFilter.exists("result", false)
                                  ),
                                  MongoUpdate.combine(
                                    MongoUpdate.set("result", InterviewResult.OutcomeUnknown.toString),
                                    MongoUpdate
                                      .set(CommandStateField, InterviewWorkflowCommandState.ResultPending.toString),
                                    availableFrom(stored.command, now),
                                    Lease.release
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
                                  InterviewFailureCode.ExecutionExhausted,
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
                                      MongoUpdate.set(FailureCodeField, InterviewFailureCode.ExecutionExhausted.wire),
                                      Lease.release
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
                                    MongoUpdate.set(Lease.owner, workerId),
                                    MongoUpdate.set(Lease.token, token.toString),
                                    MongoUpdate.set(Lease.until, leaseUntil.toDate),
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

  def authorizePublication(
      claim: ClaimedInterviewWorkflowCommand,
      generation: InterviewPublisherGeneration,
      now: Instant
  ): RepositoryIO[Boolean] = transactionRunner.run { session =>
    for {
      workflow <- loadWorkflow(claim.record.workflowId, session).orConflict
      _ <- fenceParticipants(session, workflow)
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
            MongoUpdate.set("publicationCheckedAt", now.toDate),
            MongoUpdate.inc(MongoFields.Attempts, java.lang.Integer.valueOf(1))
          )
        else terminalCommand(InterviewWorkflowCommandState.Superseded, InterviewFailureCode.PublicationObsolete, now)
      )
      _ <-
        if (valid)
          participantsOf(workflow).distinct.traverse_(subject =>
            MongoProducerRegistrations
              .register(database, session, subject.value.toString, generation.transactionalId, "Interview", now)
          )
        else ().pure[RepositoryIO]
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
      RepositoryIO.pure(preliminary)
    else {
      val commandId = StableIds.uuid(record.stepId)
      val resultId = StableIds.uuid(s"$commandId:result")
      RepositoryIO
        .lift(
          MongoSessionOperations.findById(inbox, session, s"${workflow.id.value}:$resultId")
        )
        .map(receipt => decide(receipt.nonEmpty))
    }
  }

  def attemptCount(workflowId: InterviewWorkflowId, command: InterviewCommand): RepositoryIO[Long] =
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
          MongoSessionOperations.findById(workflows, session, workflowId.value.toString)
        )
        .orConflict
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
                  total <- sum; attempts <- int32(row, "executionAttempts");
                  _ <- Either.cond(attempts >= 0, (), RepositoryError.InvalidStoredData)
                } yield total + attempts.toLong
              }
            })
          }
        }
    }

  def quarantine(identity: String, now: Instant): RepositoryIO[Unit] =
    if (identity.isEmpty || identity.length > 256) RepositoryIO.raise(RepositoryError.InvalidEvent)
    else
      RepositoryIO
        .lift(
          MongoSessionOperations.updateOne(
            inbox,
            None,
            MongoFilter.eq(MongoFields.Id, s"quarantine:$identity"),
            MongoUpdate.combine(
              MongoUpdate.setOnInsert(DocumentTypeField, "quarantine"),
              MongoUpdate.setOnInsert("failureCode", InterviewFailureCode.InvalidMessage.wire),
              MongoUpdate.setOnInsert(MongoFields.OccurredAt, now.toDate),
              MongoUpdate.setOnInsert(
                MongoFields.RetentionExpiresAt,
                Date.from(now.plusMillis(completedEvidenceRetention.toMillis))
              )
            ),
            new UpdateOptions().upsert(true)
          )
        )
        .void

  def recordResult(
      claim: ClaimedInterviewWorkflowCommand,
      result: InterviewResult,
      now: Instant
  ): RepositoryIO[Unit] =
    transactionRunner.run { session =>
      val command = claim.record
      loadWorkflow(command.workflowId, session).flatMap {
        case None           => ().pure[RepositoryIO]
        case Some(workflow) =>
          fenceParticipants(session, workflow) *>
            guardWrite(
              commands,
              session,
              MongoFilter.and(
                MongoFilter.eq(MongoFields.Id, commandId(command.workflowId, command.stepId)),
                MongoFilter.eq(RevisionField, command.revision),
                MongoFilter.eq(Lease.owner, claim.owner),
                MongoFilter.eq(Lease.token, claim.fencingToken.toString),
                MongoFilter.eq(CommandStateField, InterviewWorkflowCommandState.Executing.toString),
                MongoFilter.gt(Lease.until, now.toDate),
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

  def findCommand(
      workflowId: InterviewWorkflowId,
      stepId: String
  ): RepositoryIO[Option[InterviewWorkflowCommandRecord]] =
    RepositoryIO
      .lift(
        MongoSessionOperations.findById(commands, None, commandId(workflowId, stepId))
      )
      .flatMap(_.traverse(document => RepositoryIO.fromEither(MongoInterviewWorkflowCommandCodec.decode(document))))

  def settleNotificationResult(
      record: InterviewWorkflowCommandRecord,
      settlement: InterviewNotificationSettlement,
      now: Instant
  ): RepositoryIO[Boolean] =
    if (!InterviewCommands.isInformational(record.command)) RepositoryIO.raise(RepositoryError.InvalidEvent)
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
              MongoFilter.ne("result", InterviewResult.Succeeded.toString)
            )
          // Exhaustion also ends a command whose message can no longer be delivered in time.
          case InterviewNotificationSettlement.Exhausted(_) =>
            MongoFilter.and(target, MongoFilter.in(CommandStateField, live.map(_.toString)))
        }
        val update = settlement match {
          case InterviewNotificationSettlement.Retry(availableAt) =>
            MongoUpdate.combine(
              MongoUpdate.set(CommandStateField, InterviewWorkflowCommandState.Pending.toString),
              MongoUpdate.set(MongoFields.AvailableAt, availableAt.toDate),
              MongoUpdate.set(FailureCodeField, InterviewFailureCode.NotificationRetry.wire),
              MongoUpdate.inc("executionAttempts", Int.box(1)),
              // Publication attempts bound one delivery cycle of the broker; every logical retry starts a fresh one.
              MongoUpdate.set(MongoFields.Attempts, Int.box(0)),
              MongoUpdate.unset("result"),
              Lease.release
            )
          case InterviewNotificationSettlement.Exhausted(code) =>
            terminalCommand(InterviewWorkflowCommandState.RepairRequired, code, now)
        }
        RepositoryIO
          .lift(MongoSessionOperations.updateOne(commands, None, failedResult, update))
          .map(_.getMatchedCount == 1L)
      }

  def deferExpiry(record: InterviewWorkflowCommandRecord, availableAt: Instant): RepositoryIO[Boolean] =
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
                  Lease.release
                )
              )
            )
            .map(_.getMatchedCount == 1L)
        }
      case _ => RepositoryIO.raise(RepositoryError.InvalidEvent)
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

  def findNotificationRepairs(
      workflowId: InterviewWorkflowId
  ): RepositoryIO[List[InterviewWorkflowCommandRecord]] =
    MongoRepositorySupport.repositoryGuard(diagnostics, "interviewWorkflow.findNotificationRepairs") {
      RepositoryIO
        .lift(
          MongoSessionOperations
            .findManyById(commands, None, notificationRepairFilter(workflowId), NotificationRepairLimit)
        )
        .flatMap(_.traverse(document => RepositoryIO.fromEither(MongoInterviewWorkflowCommandCodec.decode(document))))
    }

  def repairNotifications(
      workflowId: InterviewWorkflowId,
      requestKey: UUID,
      now: Instant,
      actorId: UserId
  ): RepositoryIO[Int] =
    MongoRepositorySupport.repositoryGuard(diagnostics, "interviewWorkflow.repairNotifications") {
      transactionRunner.run { session =>
        val receiptId = s"${workflowId.value}:repair-notifications:$requestKey"
        RepositoryIO
          .lift(MongoSessionOperations.findById(inbox, session, receiptId))
          .flatMap {
            case Some(receipt) => RepositoryIO.fromEither(int32(receipt, "repaired"))
            case None          =>
              for {
                workflow <- loadWorkflow(workflowId, session).orConflict
                _ <- fenceParticipants(session, workflow)
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
                      MongoUpdate.set(MongoFields.AvailableAt, now.toDate),
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
    }

  def claimDueCommands(
      workerId: String,
      now: Instant,
      leaseUntil: Instant,
      limit: Int
  ): RepositoryIO[List[ClaimedInterviewWorkflowCommand]] =
    if (workerId.isEmpty || workerId.length > 128 || !leaseUntil.isAfter(now) || limit < 0 || limit > 64)
      RepositoryIO.raise(RepositoryError.InvalidEvent)
    else if (limit == 0) List.empty[ClaimedInterviewWorkflowCommand].pure[RepositoryIO]
    else
      claimOne(workerId, now, leaseUntil).flatMap {
        case None        => List.empty[ClaimedInterviewWorkflowCommand].pure[RepositoryIO]
        case Some(claim) => claimDueCommands(workerId, now, leaseUntil, limit - 1).map(claim :: _)
      }

  private def claimOne(
      workerId: String,
      now: Instant,
      leaseUntil: Instant
  ): RepositoryIO[Option[ClaimedInterviewWorkflowCommand]] =
    RepositoryIO.lift(uuids.randomUUID).flatMap { token =>
      val available = MongoFilter.and(
        MongoFilter.in(
          CommandStateField,
          List(InterviewWorkflowCommandState.Pending.toString, InterviewWorkflowCommandState.ResultPending.toString)
        ),
        MongoFilter.lte(MongoFields.AvailableAt, now.toDate)
      )
      val expired = MongoFilter.and(
        MongoFilter.eq(CommandStateField, InterviewWorkflowCommandState.Claimed.toString),
        Lease.expired(now)
      )
      val update = MongoUpdate.combine(
        MongoUpdate.set(CommandStateField, InterviewWorkflowCommandState.Claimed.toString),
        Lease.stamp(workerId, token.toString, leaseUntil)
      )
      MongoRepositorySupport.repositoryGuard(diagnostics, "interviewWorkflow.claim") {
        MongoLeaseQueue.claimNext(
          commands,
          None,
          MongoFilter.or(available, expired),
          update,
          Sorts.ascending(MongoFields.AvailableAt, MongoFields.Id)
        )(document => RepositoryIO.fromEither(readClaim(document)))
      }
    }

  def renewPublication(
      claim: ClaimedInterviewWorkflowCommand,
      now: Instant,
      leaseUntil: Instant
  ): RepositoryIO[Boolean] =
    if (!leaseUntil.isAfter(now)) RepositoryIO.raise(RepositoryError.InvalidEvent)
    else
      MongoRepositorySupport.repositoryGuard(diagnostics, "interviewWorkflow.renewPublication") {
        MongoLeaseQueue.renewHeld(commands, None, claimFilter(claim, now), leaseUntil, Lease.until)
      }

  def markPublished(claim: ClaimedInterviewWorkflowCommand, publishedAt: Instant): RepositoryIO[Unit] =
    transitionClaim(
      claim,
      MongoUpdate.combine(
        MongoUpdate.set(
          CommandStateField,
          (if (claim.record.result.nonEmpty) InterviewWorkflowCommandState.ResultPublished
           else InterviewWorkflowCommandState.Published).toString
        ),
        MongoUpdate.set("publishedAt", publishedAt.toDate),
        Lease.release
      ),
      "interviewWorkflow.markPublished",
      publishedAt
    )

  def retry(
      claim: ClaimedInterviewWorkflowCommand,
      now: Instant,
      availableAt: Instant,
      failureCode: InterviewFailureCode
  ): RepositoryIO[Unit] =
    transitionClaim(
      claim,
      MongoUpdate.combine(
        MongoUpdate.set(CommandStateField, InterviewWorkflowCommandState.Pending.toString),
        availableFrom(claim.record.command, availableAt),
        MongoUpdate.set(FailureCodeField, failureCode.wire),
        Lease.release
      ),
      "interviewWorkflow.retry",
      now
    )

  def requireRepair(
      claim: ClaimedInterviewWorkflowCommand,
      now: Instant,
      failureCode: InterviewFailureCode
  ): RepositoryIO[InterviewPublicationResolution] =
    transactionRunner.run { session =>
      for {
        workflow <- loadWorkflow(claim.record.workflowId, session).orConflict
        _ <- fenceParticipants(session, workflow)
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
              val rowState = resolvedWorkflow match {
                case RepairOutcome.ProposalExpired => InterviewWorkflowCommandState.Superseded
                case RepairOutcome.CommandOnly     => InterviewWorkflowCommandState.RepairRequired
              }
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
        MongoUpdate.set(MongoFields.AvailableAt, at.toDate),
        MongoUpdate.set(s"$CommandField.${MongoFields.AvailableAt}", at.toDate)
      )
    case _ => MongoUpdate.set(MongoFields.AvailableAt, at.toDate)
  }

  private def terminalCommand(
      state: InterviewWorkflowCommandState,
      reason: InterviewFailureCode,
      now: Instant
  ): MongoUpdate =
    MongoUpdate.combine(
      MongoUpdate.set(CommandStateField, state.toString),
      MongoUpdate.set(FailureCodeField, reason.wire),
      MongoUpdate.set("finishedAt", now.toDate),
      Lease.release
    )

  /** Exhausted retries: a scheduling command moves the scheduling workflow to `RepairRequired`, a cancel or reschedule
    * command moves it through the lifecycle policy (recording the origin phase), and an informational notification
    * never changes the workflow: only its own command becomes `RepairRequired`. The outcome says whether an expiry's
    * give-up resolved the workflow by expiring its proposal.
    */
  private def applyRepair(
      session: Option[ClientSession[IO]],
      workflow: InterviewWorkflow,
      command: InterviewCommand,
      cause: InterviewAdvanceCause,
      reason: InterviewFailureCode,
      now: Instant
  ): RepositoryIO[RepairOutcome] = command match {
    case _ if InterviewCommands.isInformational(command) => RepairOutcome.CommandOnly.pure[RepositoryIO]
    case lifecycle: InterviewLifecycleCommand            =>
      // Expiry has no external effect: giving up on it expires the proposal (and, if that is no longer possible, only
      // marks the command), so a proposal is never left open by a failed expiry and the claimer is never starved.
      val event = InterviewCommands.giveUpEvent(lifecycle, reason, now)
      InterviewLifecyclePolicy.decide(workflow, workflow.revision, event) match {
        case Left(_) if InterviewCommands.isExpiry(lifecycle) => RepairOutcome.CommandOnly.pure[RepositoryIO]
        case Left(_)                                          => RepositoryIO.raise(RepositoryError.Conflict)
        case Right(decision)                                  =>
          lifecycleStore
            .commitLifecycle(session, workflow, decision, event, InterviewLifecycleOrigin.Internal(cause), now, None)
            .as(if (InterviewCommands.isExpiry(lifecycle)) RepairOutcome.ProposalExpired else RepairOutcome.CommandOnly)
      }
    case _: InterviewWorkflowCommand =>
      applyScheduleRepair(session, workflow, cause, reason, now).as(RepairOutcome.CommandOnly)
  }

  private def applyScheduleRepair(
      session: Option[ClientSession[IO]],
      workflow: InterviewWorkflow,
      cause: InterviewAdvanceCause,
      reason: InterviewFailureCode,
      now: Instant
  ): RepositoryIO[Unit] =
    RepositoryIO
      .fromEither(
        InterviewWorkflow
          .decide(workflow, workflow.revision, InterviewWorkflowEvent.RetryExhausted(reason))
          .leftMap(_ => RepositoryError.Conflict)
      )
      .flatMap { case InterviewWorkflowDecision(next, emitted) =>
        reviseWorkflow(session, workflow.revision, next).subflatMap(MongoRepositorySupport.matchedOne(_)) *>
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
          insertCommands(session, workflow.id, next.revision, emitted)(_ =>
            (InterviewWorkflowCommandState.RepairRequired, now, now)
          )
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
        .subflatMap(MongoRepositorySupport.matchedOne(_))
    }

  private def claimFilter(claim: ClaimedInterviewWorkflowCommand, now: Instant): MongoFilter =
    MongoFilter.and(
      MongoFilter.eq(MongoFields.Id, commandId(claim.record.workflowId, claim.record.stepId)),
      MongoFilter.eq(CommandStateField, InterviewWorkflowCommandState.Claimed.toString),
      MongoFilter.eq(Lease.owner, claim.owner),
      MongoFilter.eq(Lease.token, claim.fencingToken.toString),
      MongoFilter.eq(RevisionField, java.lang.Long.valueOf(claim.record.revision)),
      Lease.held(now)
    )

  private def readLease(document: Document): Either[RepositoryError, InterviewExecutionLease] =
    for {
      owner <- string(document, Lease.owner)
      token <- uuid(document, Lease.token)
      until <- instant(document, Lease.until)
    } yield InterviewExecutionLease(owner, token, until)

  private def readClaim(document: Document): Either[RepositoryError, ClaimedInterviewWorkflowCommand] =
    for {
      record <- MongoInterviewWorkflowCommandCodec.decode(document)
      lease <- readLease(document)
    } yield ClaimedInterviewWorkflowCommand(record, lease.owner, lease.token, lease.until)
}
