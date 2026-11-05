package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.repository.mongo.MongoF.asRepository
import com.example.graphQL.cats.service.TransactionOperation
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
import org.bson.conversions.Bson
import org.bson.{BsonArray, BsonDocument, BsonInt32, BsonString, BsonValue, Document}

import java.time.Instant
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.*
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
      leaseDuration: FiniteDuration,
      maxAttempts: Int
  ): RepositoryF[IO, InterviewExecutionClaimOutcome] =
    if (maxAttempts <= 0) MongoF.raise(RepositoryError.InvalidEvent)
    else
      LeaseRequest.whenValid(workerId, leaseDuration)(RepositoryF.lift(uuids.randomUUID)).flatMap { token =>
        transactionRunner.run(TransactionOperation.InterviewWorkflowClaimExecution) { session =>
          loadWorkflow(command.workflowId, session).flatMap {
            case None =>
              (InterviewExecutionClaimOutcome.AlreadyHandled: InterviewExecutionClaimOutcome).asRepository
            case Some(workflow) =>
              for {
                _ <- fenceParticipants(session, workflow)
                current <- RepositoryF.lift(
                  MongoSessionOperations.findById(commands, session, commandId(command.workflowId, command.stepId))
                )
                outcome <- current match {
                  case None =>
                    (InterviewExecutionClaimOutcome.AlreadyHandled: InterviewExecutionClaimOutcome).asRepository
                  case Some(document) =>
                    MongoF.fromEither(MongoInterviewWorkflowCommandCodec.decode(document)).flatMap { stored =>
                      InterviewExecutionPolicy.admission(workflow, stored) match {
                        case InterviewExecutionAdmission.AlreadyHandled =>
                          (InterviewExecutionClaimOutcome.AlreadyHandled: InterviewExecutionClaimOutcome).asRepository
                        case InterviewExecutionAdmission.InspectLease => {
                          MongoF.fromEither(readLease(document)).flatMap { lease =>
                            InterviewExecutionPolicy.lease(lease, now) match {
                              case InterviewExecutionLeaseDisposition.Busy =>
                                MongoF.pure(InterviewExecutionClaimOutcome.Busy)
                              case InterviewExecutionLeaseDisposition.Reconcile =>
                                // The caller's clock only suggests the lease expired; the database clock decides. A
                                // lease still live there (a skewed caller) matches nothing and stays `Busy`.
                                RepositoryF
                                  .lift(
                                    MongoSessionOperations.updateOne(
                                      commands,
                                      session,
                                      MongoFilter.and(
                                        MongoFilter.eq(MongoFields.Id, commandId(stored.workflowId, stored.stepId)),
                                        MongoFilter
                                          .eq(CommandStateField, InterviewWorkflowCommandState.Executing.toString),
                                        MongoFilter.eq(Lease.owner, lease.owner),
                                        MongoFilter.eq(Lease.token, lease.token.toString),
                                        Lease.expired,
                                        MongoFilter.exists("result", false)
                                      ),
                                      MongoUpdate.combine(
                                        MongoUpdate.set("result", InterviewResult.OutcomeUnknown.toString),
                                        MongoUpdate
                                          .set(
                                            CommandStateField,
                                            InterviewWorkflowCommandState.ResultPending.toString
                                          ),
                                        availableFrom(stored.command, now),
                                        Lease.release
                                      )
                                    )
                                  )
                                  .map(result =>
                                    if (result.getMatchedCount == 1L)
                                      InterviewExecutionClaimOutcome.ReconciliationQueued
                                    else InterviewExecutionClaimOutcome.Busy
                                  )
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
                                        .set(
                                          CommandStateField,
                                          InterviewWorkflowCommandState.RepairRequired.toString
                                        ),
                                      MongoUpdate.set(FailureCodeField, InterviewFailureCode.ExecutionExhausted.wire),
                                      Lease.release
                                    )
                                  )
                                    .as(InterviewExecutionClaimOutcome.RepairRequired)
                              case InterviewExecutionBudgetDisposition.Acquire(additional) =>
                                guardPipelineWrite(
                                  session,
                                  MongoFilter.and(
                                    MongoFilter.eq(MongoFields.Id, commandId(stored.workflowId, stored.stepId)),
                                    MongoFilter.eq(RevisionField, stored.revision),
                                    MongoFilter.eq(CommandStateField, stored.state.toString)
                                  ),
                                  MongoLeaseQueue.setStage(
                                    (List[(String, BsonValue)](
                                      CommandStateField -> new BsonDocument(
                                        "$literal",
                                        new BsonString(InterviewWorkflowCommandState.Executing.toString)
                                      ),
                                      "executionAttempts" -> new BsonDocument(
                                        "$add",
                                        new BsonArray(
                                          List[BsonValue](
                                            new BsonString("$executionAttempts"),
                                            new BsonInt32(additional)
                                          ).asJava
                                        )
                                      )
                                    ) ++ Lease.stamp(workerId, token.toString, leaseDuration))*
                                  )
                                )
                                  .as(
                                    InterviewExecutionClaimOutcome.Acquired(
                                      ClaimedInterviewWorkflowCommand(
                                        stored.copy(executionAttempts = stored.executionAttempts + additional),
                                        workerId,
                                        token
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
  ): RepositoryF[IO, Boolean] = transactionRunner.run(TransactionOperation.InterviewWorkflowAuthorizePublication) {
    session =>
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
              claimFilter(claim),
              MongoFilter.eq(MongoFields.Attempts, Int.box(claim.record.publicationAttempts))
            )
          else claimFilter(claim),
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
                .register(
                  database,
                  session,
                  subject.value.toString,
                  generation.producerGeneration,
                  ProducerRegistrationValues.Interview
                )
            )
          else ().asRepository
      } yield valid
  }

  private def publicationDisposition(
      session: Option[ClientSession[IO]],
      workflow: InterviewWorkflow,
      record: InterviewWorkflowCommandRecord,
      maximumAttempts: Int
  ): RepositoryF[IO, InterviewPublicationDisposition] = {
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
      MongoF.pure(preliminary)
    else {
      val commandId = StableIds.uuid(record.stepId)
      val resultId = StableIds.uuid(s"$commandId:result")
      RepositoryF
        .lift(
          MongoSessionOperations.findById(inbox, session, s"${workflow.id.value}:$resultId")
        )
        .map(receipt => decide(receipt.nonEmpty))
    }
  }

  def attemptCount(workflowId: InterviewWorkflowId, command: InterviewCommand): RepositoryF[IO, Long] =
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
  ): RepositoryF[IO, Long] =
    attemptScope(command).fold(MongoF.fromEither[Long](Right(0L))) { case (kinds, scope) =>
      RepositoryF
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
          MongoF.fromEither(observedEpoch).flatMap { epoch =>
            val filters = List(
              MongoFilter.eq(WorkflowIdField, workflowId.value.toString),
              MongoFilter.gte(RevisionField, epoch),
              MongoFilter.in(s"$CommandField.kind", kinds)
            ) ++ scope
            val filter = MongoFilter.and(filters*)
            RepositoryF.fromFEither(commands.flatMap { collection =>
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

  def quarantine(identity: String, now: Instant): RepositoryF[IO, Unit] =
    if (identity.isEmpty || identity.length > 256) MongoF.raise(RepositoryError.InvalidEvent)
    else
      RepositoryF
        .lift(
          MongoSessionOperations.updateOne(
            inbox,
            None,
            MongoFilter.eq(MongoFields.Id, s"quarantine:$identity"),
            MongoUpdate.combine(
              MongoUpdate.setOnInsert(DocumentTypeField, QuarantineType),
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
  ): RepositoryF[IO, Unit] =
    transactionRunner.run(TransactionOperation.InterviewWorkflowRecordResult) { session =>
      val command = claim.record
      loadWorkflow(command.workflowId, session).flatMap {
        case None           => ().asRepository
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
                Lease.held,
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
  ): RepositoryF[IO, Option[InterviewWorkflowCommandRecord]] =
    RepositoryF
      .lift(
        MongoSessionOperations.findById(commands, None, commandId(workflowId, stepId))
      )
      .flatMap(
        _.traverse(document => MongoF.fromEither(MongoInterviewWorkflowCommandCodec.decode(document)))
      )

  def settleNotificationResult(
      record: InterviewWorkflowCommandRecord,
      settlement: InterviewNotificationSettlement,
      now: Instant
  ): RepositoryF[IO, Boolean] =
    if (!InterviewCommands.isInformational(record.command)) MongoF.raise(RepositoryError.InvalidEvent)
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
        RepositoryF
          .lift(MongoSessionOperations.updateOne(commands, None, failedResult, update))
          .map(_.getMatchedCount == 1L)
      }

  def deferExpiry(record: InterviewWorkflowCommandRecord, availableAt: Instant): RepositoryF[IO, Boolean] =
    record.command match {
      case InterviewLifecycleCommand.ExpireProposal(due) if !availableAt.isBefore(due) =>
        MongoRepositorySupport.repositoryGuard(diagnostics, "interviewWorkflow.deferExpiry") {
          RepositoryF
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
      case _ => MongoF.raise(RepositoryError.InvalidEvent)
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
  ): RepositoryF[IO, List[InterviewWorkflowCommandRecord]] =
    MongoRepositorySupport.repositoryGuard(diagnostics, "interviewWorkflow.findNotificationRepairs") {
      RepositoryF
        .lift(
          MongoSessionOperations
            .findManyById(commands, None, notificationRepairFilter(workflowId), NotificationRepairLimit)
        )
        .flatMap(
          _.traverse(document => MongoF.fromEither(MongoInterviewWorkflowCommandCodec.decode(document)))
        )
    }

  def repairNotifications(
      workflowId: InterviewWorkflowId,
      requestKey: UUID,
      now: Instant,
      actorId: UserId
  ): RepositoryF[IO, Int] =
    MongoRepositorySupport.repositoryGuard(diagnostics, "interviewWorkflow.repairNotifications") {
      transactionRunner.run(TransactionOperation.InterviewWorkflowRepairNotifications) { session =>
        val receiptId = s"${workflowId.value}:repair-notifications:$requestKey"
        RepositoryF
          .lift(MongoSessionOperations.findById(inbox, session, receiptId))
          .flatMap {
            case Some(receipt) => MongoF.fromEither(int32(receipt, "repaired"))
            case None          =>
              for {
                workflow <- loadWorkflow(workflowId, session).orConflict
                _ <- fenceParticipants(session, workflow)
                rows <- RepositoryF.lift(
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
      leaseDuration: FiniteDuration,
      limit: Int
  ): RepositoryF[IO, List[ClaimedInterviewWorkflowCommand]] =
    if (limit < 0 || limit > 64) MongoF.raise(RepositoryError.InvalidEvent)
    else
      LeaseRequest.whenValid(workerId, leaseDuration) {
        if (limit == 0) List.empty[ClaimedInterviewWorkflowCommand].asRepository
        else
          claimOne(workerId, now, leaseDuration).flatMap {
            case None        => List.empty[ClaimedInterviewWorkflowCommand].asRepository
            case Some(claim) => claimDueCommands(workerId, now, leaseDuration, limit - 1).map(claim :: _)
          }
      }

  private def claimOne(
      workerId: String,
      now: Instant,
      leaseDuration: FiniteDuration
  ): RepositoryF[IO, Option[ClaimedInterviewWorkflowCommand]] =
    RepositoryF.lift(uuids.randomUUID).flatMap { token =>
      val update = MongoLeaseQueue.setStage(
        (List[(String, BsonValue)](
          CommandStateField -> new BsonDocument(
            "$literal",
            new BsonString(InterviewWorkflowCommandState.Claimed.toString)
          )
        ) ++ Lease.stamp(workerId, token.toString, leaseDuration))*
      )
      MongoRepositorySupport.repositoryGuard(diagnostics, "interviewWorkflow.claim") {
        MongoLeaseQueue.claimNext(
          commands,
          None,
          MongoInterviewCommandQueue.claimable(now),
          update,
          Sorts.ascending(MongoFields.AvailableAt, MongoFields.Id)
        )(document => MongoF.fromEither(readClaim(document)))
      }
    }

  def renewPublication(
      claim: ClaimedInterviewWorkflowCommand,
      leaseDuration: FiniteDuration
  ): RepositoryF[IO, Boolean] =
    LeaseRequest.whenDurationValid(leaseDuration) {
      MongoRepositorySupport.repositoryGuard(diagnostics, "interviewWorkflow.renewPublication") {
        MongoLeaseQueue.renewHeld(commands, None, claimFilter(claim), leaseDuration, Lease)
      }
    }

  def markPublished(claim: ClaimedInterviewWorkflowCommand, publishedAt: Instant): RepositoryF[IO, Unit] =
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
      "interviewWorkflow.markPublished"
    )

  def retry(
      claim: ClaimedInterviewWorkflowCommand,
      availableAt: Instant,
      failureCode: InterviewFailureCode
  ): RepositoryF[IO, Unit] =
    transitionClaim(
      claim,
      MongoUpdate.combine(
        MongoUpdate.set(CommandStateField, InterviewWorkflowCommandState.Pending.toString),
        availableFrom(claim.record.command, availableAt),
        MongoUpdate.set(FailureCodeField, failureCode.wire),
        Lease.release
      ),
      "interviewWorkflow.retry"
    )

  def requireRepair(
      claim: ClaimedInterviewWorkflowCommand,
      now: Instant,
      failureCode: InterviewFailureCode
  ): RepositoryF[IO, InterviewPublicationResolution] =
    transactionRunner.run(TransactionOperation.InterviewWorkflowRequireRepair) { session =>
      for {
        workflow <- loadWorkflow(claim.record.workflowId, session).orConflict
        _ <- fenceParticipants(session, workflow)
        disposition <- publicationDisposition(session, workflow, claim.record, 0)
        resolution <- disposition match {
          case InterviewPublicationDisposition.Supersede =>
            guardWrite(
              commands,
              session,
              claimFilter(claim),
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
              guardWrite(commands, session, claimFilter(claim), terminalCommand(rowState, failureCode, now))
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
  ): RepositoryF[IO, RepairOutcome] = command match {
    case _ if InterviewCommands.isInformational(command) => RepairOutcome.CommandOnly.asRepository
    case lifecycle: InterviewLifecycleCommand            =>
      // Expiry has no external effect: giving up on it expires the proposal (and, if that is no longer possible, only
      // marks the command), so a proposal is never left open by a failed expiry and the claimer is never starved.
      val event = InterviewCommands.giveUpEvent(lifecycle, reason, now)
      InterviewLifecyclePolicy.decide(workflow, workflow.revision, event) match {
        case Left(_) if InterviewCommands.isExpiry(lifecycle) =>
          RepairOutcome.CommandOnly.asRepository
        case Left(_)         => MongoF.raise(RepositoryError.Conflict)
        case Right(decision) =>
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
  ): RepositoryF[IO, Unit] =
    MongoF
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
      operation: String
  ): RepositoryF[IO, Unit] =
    MongoRepositorySupport.repositoryGuard(diagnostics, operation) {
      RepositoryF
        .lift(
          MongoSessionOperations
            .updateOne(commands, None, claimFilter(claim), update)
        )
        .subflatMap(MongoRepositorySupport.matchedOne(_))
    }

  /** A guarded single-document pipeline update (so `$$NOW` is usable): exactly one match, else `Conflict`. */
  private def guardPipelineWrite(
      session: Option[ClientSession[IO]],
      filter: MongoFilter,
      pipeline: Seq[Bson]
  ): RepositoryF[IO, Unit] =
    RepositoryF
      .lift(MongoSessionOperations.updateOnePipeline(commands, session, filter, pipeline, new UpdateOptions))
      .subflatMap(result => Either.cond(result.getMatchedCount == 1L, (), RepositoryError.Conflict))

  private def claimFilter(claim: ClaimedInterviewWorkflowCommand): MongoFilter =
    MongoFilter.and(
      MongoFilter.eq(MongoFields.Id, commandId(claim.record.workflowId, claim.record.stepId)),
      MongoFilter.eq(CommandStateField, InterviewWorkflowCommandState.Claimed.toString),
      MongoFilter.eq(Lease.owner, claim.owner),
      MongoFilter.eq(Lease.token, claim.fencingToken.toString),
      MongoFilter.eq(RevisionField, java.lang.Long.valueOf(claim.record.revision)),
      Lease.held
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
    } yield ClaimedInterviewWorkflowCommand(record, lease.owner, lease.token)
}

private[mongo] object MongoInterviewCommandQueue {

  /** Due `Pending`/`ResultPending` commands (a caller scheduling value) plus `Claimed` commands whose lease expired on
    * the database clock.
    */
  def claimable(now: Instant): MongoFilter = {
    import MongoInterviewFields.*
    MongoFilter.or(
      MongoFilter.and(
        MongoFilter.in(
          CommandStateField,
          List(InterviewWorkflowCommandState.Pending.toString, InterviewWorkflowCommandState.ResultPending.toString)
        ),
        MongoFilter.lte(MongoFields.AvailableAt, now.toDate)
      ),
      MongoFilter.and(
        MongoFilter.eq(CommandStateField, InterviewWorkflowCommandState.Claimed.toString),
        Lease.expired
      )
    )
  }
}
