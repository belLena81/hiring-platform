package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.shared.StableIds
import cats.data.EitherT
import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.ApplicationEventId
import com.example.graphQL.cats.domain.model.{ApplicationEvent, ApplicationStatus, UserRole}
import com.example.graphQL.cats.domain.policy.ApplicationLifecycle
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.events.OperationalEvents
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.RepositoryError
import mongo4cats.client.ClientSession
import org.bson.Document

import java.time.Instant
import scala.jdk.CollectionConverters.*

/** Cancellation, rescheduling and proposal transitions: the one write path for lifecycle events and their evidence. */
final class MongoInterviewLifecycleStore(store: MongoInterviewWorkflowStore) {
  import MongoInterviewFields.*
  import MongoInterviewWorkflowCodec.*
  import store.*

  private def swapReceiptId(workflowId: InterviewWorkflowId, generation: Int): String =
    s"${workflowId.value}:swap:g$generation"

  def hasRescheduleApproval(workflowId: InterviewWorkflowId, generation: Int): RepositoryF[IO, Boolean] =
    RepositoryF
      .lift(
        MongoSessionOperations.findById(inbox, None, swapReceiptId(workflowId, generation))
      )
      .map(_.nonEmpty)

  def approveRescheduledInterval(
      workflow: InterviewWorkflow,
      now: Instant,
      execution: ClaimedInterviewWorkflowCommand
  ): RepositoryF[IO, Unit] =
    MongoRepositorySupport.repositoryGuard(diagnostics, "interviewWorkflow.approveRescheduledInterval") {
      transactionRunner.run { session =>
        val receiptId = swapReceiptId(workflow.id, workflow.replacementGeneration)
        def held(generation: Int): RepositoryF[IO, InterviewCalendarReservation] =
          RepositoryF
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
            .orConflict
            .flatMap(document => RepositoryF.at[IO].fromEither(decodeReservation(document)))
        for {
          _ <- fenceExecution(session, workflow.id, Some(execution))
          existing <- RepositoryF.lift(
            MongoSessionOperations.findById(inbox, session, receiptId)
          )
          _ <-
            if (existing.nonEmpty) ().pure[[A] =>> RepositoryF[IO, A]]
            else
              for {
                current <- loadWorkflow(workflow.id, session).orConflict
                pending <- RepositoryF.at[IO].fromEither(workflow.pendingInterval.toRight(RepositoryError.Conflict))
                _ <- RepositoryF
                  .at[IO]
                  .fromEither(
                    Either.cond(
                      current == workflow && workflow.phase == InterviewWorkflowPhase.RescheduleSwapPending &&
                        now.isBefore(InterviewCalendarFence.swapDeadline(workflow)),
                      (),
                      RepositoryError.Conflict
                    )
                  )
                _ <- fenceParticipants(session, workflow)
                _ <- RepositoryF
                  .lift(
                    MongoSessionOperations.findOne(
                      applications,
                      session,
                      MongoFilter.and(
                        MongoFilter.eq(MongoFields.Id, workflow.applicationId.value.toString),
                        MongoFilter.eq(MongoFields.Status, ApplicationStatus.Interview.toString)
                      )
                    )
                  )
                  .orConflict
                _ <- held(workflow.generation)
                replacement <- held(workflow.replacementGeneration)
                _ <- RepositoryF
                  .at[IO]
                  .fromEither(Either.cond(replacement.interval == pending, (), RepositoryError.Conflict))
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
    }

  def applyLifecycle(
      workflowId: InterviewWorkflowId,
      expectedRevision: Long,
      event: InterviewLifecycleEvent,
      origin: InterviewLifecycleOrigin,
      now: Instant,
      availableAt: Option[Instant]
  ): RepositoryF[IO, InterviewLifecycleOutcome] =
    if (InterviewActorPolicy.admits(origin.actor, event))
      applyAdmittedLifecycle(workflowId, expectedRevision, event, origin, now, availableAt)
    else RepositoryF.at[IO].raise(RepositoryError.InvalidEvent)

  private def applyAdmittedLifecycle(
      workflowId: InterviewWorkflowId,
      expectedRevision: Long,
      event: InterviewLifecycleEvent,
      origin: InterviewLifecycleOrigin,
      now: Instant,
      availableAt: Option[Instant]
  ): RepositoryF[IO, InterviewLifecycleOutcome] =
    MongoRepositorySupport.repositoryGuard(diagnostics, "interviewWorkflow.applyLifecycle") {
      transactionRunner
        .run { session =>
          lifecycleReplay(session, workflowId, origin).flatMap {
            case Some(replayed) => replayed.pure[[A] =>> RepositoryF[IO, A]]
            case None           =>
              loadWorkflow(workflowId, session, participantPredicate(origin)).flatMap {
                case None          => InterviewLifecycleOutcome.NotVisible.pure[[A] =>> RepositoryF[IO, A]]
                case Some(current) =>
                  InterviewLifecyclePolicy.decide(current, expectedRevision, event) match {
                    case Left(error)     => InterviewLifecycleOutcome.Rejected(error).pure[[A] =>> RepositoryF[IO, A]]
                    case Right(decision) => commitLifecycle(session, current, decision, event, origin, now, availableAt)
                  }
              }
          }
        }
        // A concurrent identical request commits its receipt first; its stored result is this request's result.
        .recoverWith { case RepositoryError.Conflict =>
          lifecycleReplay(None, workflowId, origin).subflatMap(_.toRight(RepositoryError.Conflict))
        }
    }

  /** A repeated request (same actor and key) or cause returns the stored workflow; a reused key with another operation,
    * input or workflow is a typed conflict.
    */
  private def lifecycleReplay(
      session: Option[ClientSession[IO]],
      workflowId: InterviewWorkflowId,
      origin: InterviewLifecycleOrigin
  ): RepositoryF[IO, Option[InterviewLifecycleOutcome]] = {
    def stored(id: InterviewWorkflowId): RepositoryF[IO, Option[InterviewLifecycleOutcome]] =
      loadWorkflow(id, session)
        .subflatMap(_.toRight(RepositoryError.InvalidStoredData))
        .map(existing => Some(InterviewLifecycleOutcome.Duplicate(existing)))
    origin match {
      case InterviewLifecycleOrigin.Actor(access, requestKey, fingerprint) =>
        RepositoryF
          .lift(MongoSessionOperations.findById(workflows, session, requestReceiptId(access.actorId, requestKey)))
          .flatMap {
            case None           => none[InterviewLifecycleOutcome].pure[[A] =>> RepositoryF[IO, A]]
            case Some(document) =>
              RepositoryF.at[IO].fromEither(decodeReceipt(document)).flatMap {
                case (id, existing) if id == workflowId && existing == fingerprint.value => stored(id)
                case _ => RepositoryF.at[IO].raise(RepositoryError.Conflict)
              }
          }
      case InterviewLifecycleOrigin.Internal(cause) =>
        RepositoryF
          .lift(MongoSessionOperations.findById(inbox, session, s"${workflowId.value}:${cause.receiptIdentity}"))
          .flatMap(found =>
            if (found.isEmpty) none[InterviewLifecycleOutcome].pure[[A] =>> RepositoryF[IO, A]] else stored(workflowId)
          )
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
  ): RepositoryF[IO, Either[InterviewLifecycleOutcome, CancellationWrite]] = {
    val authorized = origin match {
      case InterviewLifecycleOrigin.Actor(access, _, _) =>
        Right(access).filterOrElse(
          value => InterviewCancellationInitiator.of(value.role) == initiator,
          RepositoryError.InvalidEvent
        )
      case InterviewLifecycleOrigin.Internal(_) => Left(RepositoryError.InvalidEvent)
    }
    for {
      access <- RepositoryF.at[IO].fromEither(authorized)
      document <- RepositoryF
        .lift(
          MongoSessionOperations.findOne(
            applications,
            session,
            MongoFilter.and(
              MongoFilter.eq(MongoFields.Id, current.applicationId.value.toString),
              MongoFilter.eq(MongoFields.CandidateId, current.candidateId.value.toString)
            )
          )
        )
        .orConflict
      application <- readApplication(document)
      eventId = ApplicationEventId(
        StableIds.uuid(s"${current.id.value}:cancel:status")
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
          Left[InterviewLifecycleOutcome, CancellationWrite](
            InterviewLifecycleOutcome.ApplicationNotInterview(application.status)
          )
            .pure[[A] =>> RepositoryF[IO, A]]
        case Right((updated, change)) =>
          RepositoryF
            .at[IO]
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
  ): RepositoryF[IO, Either[InterviewLifecycleOutcome, Option[CancellationWrite]]] =
    RepositoryF
      .lift(
        MongoSessionOperations.findOne(
          applications,
          session,
          MongoFilter.eq(MongoFields.Id, current.applicationId.value.toString)
        )
      )
      .orConflict
      .flatMap(readApplication)
      .map(application =>
        Either.cond(
          application.status == ApplicationStatus.Interview,
          Option.empty[CancellationWrite],
          InterviewLifecycleOutcome.ApplicationNotInterview(application.status)
        )
      )

  private[mongo] def commitLifecycle(
      session: Option[ClientSession[IO]],
      current: InterviewWorkflow,
      decision: InterviewLifecycleDecision,
      event: InterviewLifecycleEvent,
      origin: InterviewLifecycleOrigin,
      now: Instant,
      availableAt: Option[Instant]
  ): RepositoryF[IO, InterviewLifecycleOutcome] = {
    val next = decision.workflow
    val cancellation: RepositoryF[IO, Either[InterviewLifecycleOutcome, Option[CancellationWrite]]] = event match {
      case InterviewLifecycleEvent.Cancel(initiator, at) =>
        cancellationWrite(session, current, initiator, origin, at).map(_.map(Some(_)))
      case InterviewLifecycleEvent.Propose(_, _, _, _, _) | InterviewLifecycleEvent.AcceptProposal(_) |
          InterviewLifecycleEvent.RequestReschedule(_) =>
        requireInterviewApplication(session, current)
      case _ => Right[InterviewLifecycleOutcome, Option[CancellationWrite]](None).pure[[A] =>> RepositoryF[IO, A]]
    }
    // Only Admin repair resets the retry budget, and Admin repair is the only event allowed to.
    val resetsAttempts = origin match {
      case InterviewLifecycleOrigin.Internal(cause) => cause.resetsAttempts
      case _                                        => false
    }
    cancellation.flatMap {
      case Left(outcome)                                 => outcome.pure[[A] =>> RepositoryF[IO, A]]
      case Right(_) if next.revision == current.revision =>
        // A no-op (the request flag is already set) writes nothing: no receipt and no retention refresh. Writing a
        // receipt per fresh key would let one actor grow storage and trigger updates without bound; repeating the
        // request is idempotent without one.
        (InterviewLifecycleOutcome.Duplicate(current): InterviewLifecycleOutcome).pure[[A] =>> RepositoryF[IO, A]]
      case Right(write) =>
        val actor = origin match {
          case InterviewLifecycleOrigin.Actor(access, _, _) => List(access.actorId)
          case InterviewLifecycleOrigin.Internal(_)         => Nil
        }
        for {
          _ <- RepositoryF
            .at[IO]
            .fromEither(
              Either.cond((event == InterviewLifecycleEvent.Repair) == resetsAttempts, (), RepositoryError.InvalidEvent)
            )
          _ <- fenceParticipants(session, current, actor)
          _ <- reviseWorkflow(
            session,
            current.revision,
            next,
            participantPredicate(origin),
            List(
              MongoUpdate.set("notified", next.notified.toList.map(_.toString).sorted.asJava),
              MongoInterviewWorkflowLifecycleCodec.stateUpdate(next)
            ) ++ Option.when(resetsAttempts)(MongoUpdate.set("attemptEpochRevision", next.revision))
          ).subflatMap(MongoRepositorySupport.matchedOne(_))
          _ <- write.traverse_(applyCancellation(session, current, origin, _))
          _ <- recordLifecycleReceipt(session, current, next.revision, origin, now)
          _ <- if (resetsAttempts) supersedeGatingCommands(session, current) else ().pure[[A] =>> RepositoryF[IO, A]]
          _ <- insertCommands(session, current.id, next.revision, decision.commands) { command =>
            val dueAt = command match {
              case InterviewLifecycleCommand.ExpireProposal(at) => Some(at)
              case _                                            => None
            }
            (
              command match {
                case InterviewLifecycleCommand.RequireRepair(_) => InterviewWorkflowCommandState.RepairRequired
                case _                                          => InterviewWorkflowCommandState.Pending
              },
              dueAt.orElse(availableAt).getOrElse(now),
              // Expiry is due work: its message is as old as the moment it falls due, not as the proposal.
              dueAt.getOrElse(now)
            )
          }
          _ <-
            // Idempotent: every transition that ends in a settled phase refreshes the evidence it just wrote.
            if (Set(InterviewWorkflowPhase.Completed, InterviewWorkflowPhase.Cancelled)(next.phase))
              retainCompleted(session, next, now)
            // An open proposal, cancellation or reschedule is live work: its evidence must not expire.
            else if (current.phase == InterviewWorkflowPhase.Completed) releaseRetention(session, next)
            else ().pure[[A] =>> RepositoryF[IO, A]]
        } yield InterviewLifecycleOutcome.Applied(next)
    }
  }

  /** Admin repair supersedes every earlier intent that gated the workflow. Informational notifications are independent
    * of the workflow phase: they stay deliverable in every phase, so repair neither supersedes nor resends them.
    */
  private def supersedeGatingCommands(
      session: Option[ClientSession[IO]],
      current: InterviewWorkflow
  ): RepositoryF[IO, Unit] =
    RepositoryF
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
            Lease.release
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
  ): RepositoryF[IO, Unit] = origin match {
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
  ): RepositoryF[IO, Unit] = {
    val ownerGuard = origin match {
      case InterviewLifecycleOrigin.Actor(access, _, _) if access.role == UserRole.Recruiter =>
        touchJobOwner(session, write.application.jobId.value.toString, access.actorId)
      case _ => ().pure[[A] =>> RepositoryF[IO, A]]
    }
    ownerGuard *>
      writeApplicationStatus(session, current.applicationId, write.previousStatus, write.application) *>
      insert(
        session,
        applicationEvents,
        MongoHiringCodecs.event(write.history)
      ) *>
      MongoOperationalEventInsertion.insert(
        outbox,
        session,
        List(OperationalEvents.statusChanged(write.history.id.value, write.application, write.history)),
        write.application.updatedAt,
        diagnostics
      )
  }
}
