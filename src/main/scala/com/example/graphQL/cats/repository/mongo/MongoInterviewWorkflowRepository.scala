package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.model.{ApplicationStatus, UserRole, ApplicationEvent}
import com.example.graphQL.cats.domain.model.Identifiers.ApplicationEventId
import com.example.graphQL.cats.service.events.OperationalEvents
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.port.*
import com.mongodb.client.model.{FindOneAndUpdateOptions, ReturnDocument, Sorts, UpdateOptions}
import mongo4cats.client.{ClientSession, MongoClient}
import mongo4cats.database.MongoDatabase
import org.bson.Document

import java.time.Instant
import java.util.{Date, UUID}
import scala.jdk.CollectionConverters.*
import scala.concurrent.duration.*

/** Mongo persistence for the interview workflow and its local fake providers.
  *
  * Collection/index creation is intentionally owned by hiring setup and migrations. Before wiring this adapter, setup
  * must create the collections named below and install the migration/index catalog described by the workflow
  * specification. Mongo's intrinsic `_id` uniqueness protects request receipts, inbox messages, commands, fake calendar
  * reservations, participant locks, and notification receipts.
  */
final class MongoInterviewWorkflowRepository(
    database: MongoDatabase[IO],
    transactionRunner: MongoTransactionRunner,
    diagnostics: Diagnostics,
    completedEvidenceRetention: FiniteDuration = 8.days
) extends InterviewWorkflowRepository
    with FakeInterviewCalendarStore
    with FakeInterviewNotificationStore
    with MongoOperationalEventInsertion {
  private val workflows = Mongo4catsCollections.documents(database, MongoCollections.InterviewWorkflows)
  private val commands = Mongo4catsCollections.documents(database, MongoCollections.InterviewWorkflowCommands)
  private val inbox = Mongo4catsCollections.documents(database, MongoCollections.InterviewWorkflowInbox)
  private val reservations =
    Mongo4catsCollections.documents(database, MongoCollections.FakeInterviewCalendarReservations)
  private val participantLocks =
    Mongo4catsCollections.documents(database, MongoCollections.FakeInterviewCalendarParticipantLocks)
  private val notificationReceipts =
    Mongo4catsCollections.documents(database, MongoCollections.FakeInterviewNotificationReceipts)

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
      leaseUntil: Instant
  ): RepositoryIO[Option[ClaimedInterviewWorkflowCommand]] =
    RepositoryIO.lift(IO.randomUUID).flatMap { token =>
      transactionRunner.run { session =>
        loadWorkflow(command.workflowId, session).flatMap {
          case None                                                  => RepositoryIO.fromEither(Right(None))
          case Some(workflow) if !eligibleCommand(workflow, command) => RepositoryIO.fromEither(Right(None))
          case Some(workflow)                                        =>
            for {
              _ <- fenceSubjects(session, List(workflow.candidateId, workflow.recruiterId, workflow.initiatedBy))
              current <- RepositoryIO.lift(
                MongoSessionOperations.findOne(
                  commands,
                  session,
                  MongoFilter.eq(MongoFields.Id, commandId(command.workflowId, command.stepId))
                )
              )
              claimed <- current match {
                case Some(document) if document.containsKey("result") => RepositoryIO.fromEither(Right(None))
                case Some(document)
                    if Option(document.getString(CommandStateField)).contains(
                      InterviewWorkflowCommandState.Executing.toString
                    ) &&
                      Option(document.getDate(LeaseUntilField)).exists(_.toInstant.isAfter(now)) =>
                  RepositoryIO.fromEither(Left(RepositoryError.Conflict))
                case Some(document) =>
                  RepositoryIO.fromEither(decodeCommandRecord(document)).flatMap { stored =>
                    if (!eligibleCommand(workflow, stored)) RepositoryIO.fromEither(Right(None))
                    else
                      guardWrite(
                        commands,
                        session,
                        MongoFilter.and(
                          MongoFilter.eq(MongoFields.Id, commandId(command.workflowId, command.stepId)),
                          MongoFilter.eq(RevisionField, stored.revision),
                          MongoFilter.eq(CommandStateField, stored.state.toString)
                        ),
                        MongoUpdate.combine(
                          MongoUpdate.set(CommandStateField, InterviewWorkflowCommandState.Executing.toString),
                          MongoUpdate.set(OwnerField, workerId),
                          MongoUpdate.set(FencingTokenField, token.toString),
                          MongoUpdate.set(LeaseUntilField, Date.from(leaseUntil))
                        )
                      )
                        .as(Some(ClaimedInterviewWorkflowCommand(stored, workerId, token, leaseUntil)))
                  }
                case None => RepositoryIO.fromEither(Right(None))
              }
            } yield claimed
        }
      }
    }

  private def eligibleCommand(workflow: InterviewWorkflow, command: InterviewWorkflowCommandRecord): Boolean = {
    command.state != InterviewWorkflowCommandState.Superseded &&
    InterviewWorkflow.commandIsApplicable(workflow, command.revision, command.command)
  }

  override def authorizePublication(claim: ClaimedInterviewWorkflowCommand, now: Instant): RepositoryIO[Boolean] =
    transactionRunner.run { session =>
      for {
        workflow <- loadWorkflow(claim.record.workflowId, session).subflatMap(_.toRight(RepositoryError.Conflict))
        _ <- fenceSubjects(session, List(workflow.candidateId, workflow.recruiterId, workflow.initiatedBy))
        valid = claim.record.result.nonEmpty || eligibleCommand(workflow, claim.record)
        _ <- guardWrite(
          commands,
          session,
          claimFilter(claim, now),
          if (valid) MongoUpdate.set("publicationCheckedAt", Date.from(now))
          else MongoUpdate.set(CommandStateField, InterviewWorkflowCommandState.Published.toString)
        )
      } yield valid
    }

  override def attemptCount(workflowId: InterviewWorkflowId, command: InterviewWorkflowCommand): RepositoryIO[Long] = {
    val kinds = command match {
      case InterviewWorkflowCommand.ReserveCalendarSlot(_) | InterviewWorkflowCommand.LookupCalendarReservation(_) =>
        List("reserveCalendar", "lookupCalendar")
      case InterviewWorkflowCommand.CommitAcceptedToInterview(_) |
          InterviewWorkflowCommand.LookupStatusCommitReceipt(_) =>
        List("commitInterview", "lookupStatusCommit")
      case InterviewWorkflowCommand.ReleaseCalendarSlot(_) => List("releaseCalendar")
      case InterviewWorkflowCommand.Notify(_, _) | InterviewWorkflowCommand.LookupNotificationReceipt(_) =>
        List("notify", "lookupNotification")
      case _ => List("requireRepair")
    }
    val key = command match {
      case InterviewWorkflowCommand.Notify(_, value)                 => Some(value)
      case InterviewWorkflowCommand.LookupNotificationReceipt(value) => Some(value)
      case _                                                         => None
    }
    RepositoryIO
      .lift(MongoSessionOperations.findOne(workflows, None, MongoFilter.eq(MongoFields.Id, workflowId.value.toString)))
      .flatMap { stored =>
        val epoch = stored
          .flatMap(doc =>
            Option(doc.get("attemptEpochRevision")).collect { case number: java.lang.Number => number.longValue }
          )
          .getOrElse(0L)
        val filters = List(
          MongoFilter.eq(WorkflowIdField, workflowId.value.toString),
          MongoFilter.gte(RevisionField, epoch),
          MongoFilter.in(s"$CommandField.kind", kinds)
        ) ++ key.toList.map(value => MongoFilter.eq(s"$CommandField.$IdempotencyKeyField", value))
        RepositoryIO.lift(
          commands.flatMap(_.count(MongoFilter.and(filters*).bson, new com.mongodb.client.model.CountOptions()))
        )
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
                MongoUpdate.set(MongoFields.AvailableAt, Date.from(now))
              )
            )
      }
    }

  override def findRequest(
      recruiterId: UserId,
      requestKey: UUID,
      fingerprint: MutationReceiptFingerprint
  ): RepositoryIO[Option[InterviewWorkflow]] =
    requestReceipt(requestReceiptId(recruiterId, requestKey)).flatMap {
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
                      MongoFilter.eq(MongoFields.Id, workflow.id.value.toString),
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
                jobId <- RepositoryIO.fromEither(string(application, MongoFields.JobId))
                _ <- guardWrite(
                  jobs,
                  session,
                  MongoFilter.and(
                    MongoFilter.eq(MongoFields.Id, jobId),
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
                    MongoFilter.eq(MongoFields.Status, ApplicationStatus.Accepted.toString)
                  ),
                  MongoUpdate.combine(
                    MongoUpdate.set(MongoFields.Status, ApplicationStatus.Interview.toString),
                    MongoUpdate.set(MongoFields.UpdatedAt, Date.from(now))
                  )
                )
                eventId = UUID.nameUUIDFromBytes(
                  s"${workflow.id.value}:status".getBytes(java.nio.charset.StandardCharsets.UTF_8)
                )
                event = new Document(MongoFields.Id, eventId.toString)
                  .append(MongoFields.ApplicationId, workflow.applicationId.value.toString)
                  .append(MongoFields.PreviousStatus, ApplicationStatus.Accepted.toString)
                  .append(MongoFields.NewStatus, ApplicationStatus.Interview.toString)
                  .append(MongoFields.ActorId, workflow.initiatedBy.value.toString)
                  .append(MongoFields.OccurredAt, Date.from(now))
                _ <- insert(
                  session,
                  Mongo4catsCollections.documents(database, MongoCollections.ApplicationEvents),
                  event
                )
                decoded <- RepositoryIO.fromEither(
                  MongoHiringCodecs
                    .readApplication(application)
                    .toEither
                    .leftMap(_ => RepositoryError.InvalidStoredData)
                )
                statusEvent = ApplicationEvent(
                  ApplicationEventId(eventId),
                  workflow.applicationId,
                  Some(ApplicationStatus.Accepted),
                  ApplicationStatus.Interview,
                  workflow.initiatedBy,
                  now,
                  None,
                  None
                )
                _ <- insertOperationalEvents(
                  Mongo4catsCollections.documents(database, MongoCollections.EventOutbox),
                  session,
                  List(
                    OperationalEvents.statusChanged(
                      eventId,
                      decoded.copy(status = ApplicationStatus.Interview, updatedAt = now),
                      statusEvent
                    )
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
                    MongoUpdate.set(RevisionField, next._1.revision),
                    MongoUpdate.set("phase", next._1.phase.toString)
                  )
                )
                _ <- next._2.zipWithIndex.traverse_ { case (command, ordinal) =>
                  insert(
                    session,
                    commands,
                    commandDocument(
                      InterviewWorkflowCommandRecord(
                        workflow.id,
                        stepId(workflow.id, next._1.revision, ordinal),
                        next._1.revision,
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
          RepositoryIO.fromEither(
            long(receipt, RevisionField)
              .flatMap(revision => Either.cond(revision == expectedRevision + 1L, workflow, RepositoryError.Conflict))
          )
        case None => {
          if (workflow.revision != expectedRevision || workflow.phase != InterviewWorkflowPhase.RepairRequired)
            RepositoryIO.fromEither(Left(RepositoryError.Conflict))
          else
            hasHiringReceipt(workflow.id).flatMap { committed =>
              val phase =
                if (committed) InterviewWorkflowPhase.NotificationsPending
                else InterviewWorkflowPhase.ReservationPending
              val command =
                if (committed)
                  List(InterviewParticipant.Candidate, InterviewParticipant.Recruiter)
                    .filterNot(workflow.notified)
                    .map(participant =>
                      InterviewWorkflowCommand.LookupNotificationReceipt(s"${workflow.id.value}:notify:$participant")
                    )
                else List(InterviewWorkflowCommand.LookupCalendarReservation(workflow.id))
              val next = workflow.copy(revision = expectedRevision + 1L, phase = phase)
              advance(next, expectedRevision, s"repair:$requestKey", command, now, auditActorId = Some(actorId))
                .flatMap {
                  case InterviewWorkflowAdvanceResult.Applied             => RepositoryIO.fromEither(Right(next))
                  case InterviewWorkflowAdvanceResult.Duplicate(existing) => RepositoryIO.fromEither(Right(existing))
                  case InterviewWorkflowAdvanceResult.StaleRevision       =>
                    RepositoryIO.fromEither(Left(RepositoryError.Conflict))
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
      attempts = 0,
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
      inboxMessageId: String,
      emittedCommands: List[InterviewWorkflowCommand],
      occurredAt: Instant,
      availableAt: Option[Instant] = None,
      auditActorId: Option[UserId] = None
  ): RepositoryIO[InterviewWorkflowAdvanceResult] =
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
          InterviewWorkflowCommandState.Pending,
          attempts = 0,
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
                          if (inboxMessageId.startsWith("repair:"))
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
                            .append("auditActorId", auditActorId.map(_.value.toString).orNull)
                        )
                        _ <-
                          if (inboxMessageId.startsWith("repair:"))
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
        MongoUpdate.set(LeaseUntilField, Date.from(leaseUntil)),
        MongoUpdate.inc(MongoFields.Attempts, java.lang.Integer.valueOf(1))
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
          MongoUpdate.set(MongoFields.AvailableAt, Date.from(availableAt)),
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
  ): RepositoryIO[Unit] =
    if (!safeFailureCode(failureCode)) RepositoryIO.fromEither(Left(RepositoryError.InvalidStoredData))
    else
      transactionRunner.run { session =>
        for {
          workflow <- loadWorkflow(claim.record.workflowId, session).subflatMap(_.toRight(RepositoryError.Conflict))
          _ <- RepositoryIO.fromEither(
            Either.cond(eligibleCommand(workflow, claim.record), (), RepositoryError.Conflict)
          )
          _ <- fenceSubjects(session, List(workflow.candidateId, workflow.recruiterId, workflow.initiatedBy))
          _ <- guardWrite(
            commands,
            session,
            claimFilter(claim, now),
            MongoUpdate.combine(
              MongoUpdate.set(CommandStateField, InterviewWorkflowCommandState.RepairRequired.toString),
              MongoUpdate.set(FailureCodeField, failureCode),
              MongoUpdate.set("finishedAt", Date.from(now)),
              MongoUpdate.unset(OwnerField),
              MongoUpdate.unset(FencingTokenField),
              MongoUpdate.unset(LeaseUntilField)
            )
          )
          _ <- guardWrite(
            workflows,
            session,
            MongoFilter.eq(MongoFields.Id, workflow.id.value.toString),
            MongoUpdate.combine(
              MongoUpdate.set("phase", InterviewWorkflowPhase.RepairRequired.toString),
              MongoUpdate.inc(RevisionField, 1L)
            )
          )
        } yield ()
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
    val write = transactionRunner.run { session =>
      for {
        _ <- fenceSubjects(session, participants)
        _ <- fenceExecution(session, reservation.workflowId, execution)
        workflow <- loadWorkflow(reservation.workflowId, session).subflatMap(_.toRight(RepositoryError.Conflict))
        actualNow <- RepositoryIO.lift(IO.realTimeInstant)
        _ <- RepositoryIO.fromEither(
          Either.cond(actualNow.isBefore(workflow.preCommitDeadline), (), RepositoryError.Conflict)
        )
        _ <- RepositoryIO.fromEither(
          Either.cond(
            workflow.candidateId == reservation.candidateId &&
              workflow.recruiterId == reservation.recruiterId && workflow.interval == reservation.interval,
            (),
            RepositoryError.Conflict
          )
        )
        _ <- guardWrite(
          workflows,
          session,
          MongoFilter.and(
            MongoFilter.eq(MongoFields.Id, workflow.id.value.toString),
            MongoFilter.eq("phase", InterviewWorkflowPhase.ReservationPending.toString)
          ),
          MongoUpdate.inc("providerFence", 1L)
        )
        _ <- participants.traverse_(id => touchParticipantLock(session, id))
        existing <- RepositoryIO.lift(
          MongoSessionOperations.findOne(
            reservations,
            session,
            MongoFilter.eq(MongoFields.Id, reservation.workflowId.value.toString)
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
            val overlap = MongoFilter.and(
              MongoFilter.in("participants", participants.map(_.value.toString)),
              MongoFilter.lt(StartField, Date.from(reservation.interval.endsAt)),
              MongoFilter.gt(EndField, Date.from(reservation.interval.startsAt)),
              MongoFilter.exists(ReleasedAtField, false)
            )
            RepositoryIO.lift(MongoSessionOperations.findOne(reservations, session, overlap)).flatMap {
              case Some(_) => RepositoryIO.fromEither(Left(RepositoryError.Conflict))
              case None    =>
                insert(session, reservations, reservationDocument(reservation)).as(reservation)
            }
        }
      } yield saved
    }
    mapProvider(write)
  }

  override def find(workflowId: InterviewWorkflowId): InterviewProviderIO[Option[InterviewCalendarReservation]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "interviewCalendar.find") {
        RepositoryIO
          .lift(
            MongoSessionOperations
              .findOne(reservations, None, MongoFilter.eq(MongoFields.Id, workflowId.value.toString))
          )
          .flatMap(_.traverse(document => RepositoryIO.fromEither(decodeReservation(document))))
      }(_ => Left(RepositoryError.Unavailable))
      .leftMap(_ => InterviewProviderError.Unavailable)

  override def release(
      idempotencyKey: String,
      at: Instant,
      execution: Option[ClaimedInterviewWorkflowCommand] = None
  ): InterviewProviderIO[Unit] = {
    val releaseFilter = MongoFilter.and(
      MongoFilter.eq("releaseKey", idempotencyKey),
      MongoFilter.exists(ReleasedAtField, false)
    )
    val update = MongoUpdate.set(ReleasedAtField, Date.from(at))
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
                _ <- fenceSubjects(session, List(reservation.candidateId, reservation.recruiterId))
                _ <- List(reservation.candidateId, reservation.recruiterId).distinct
                  .traverse_(touchParticipantLock(session, _))
                _ <- guardWrite(reservations, session, releaseFilter, update)
              } yield ()
          }
      }
    }(_ => Left(RepositoryError.Unavailable))
    result.leftMap(_ => InterviewProviderError.Unavailable)
  }

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
    val expires = now.plusMillis(completedEvidenceRetention.toMillis)
    val related = MongoFilter.or(
      MongoFilter.eq(MongoFields.Id, workflow.id.value.toString),
      MongoFilter.eq(WorkflowIdField, workflow.id.value.toString),
      MongoFilter.eq(RequestWorkflowField, workflow.id.value.toString)
    )
    val calendarExpires = workflow.interval.endsAt.plusMillis(completedEvidenceRetention.toMillis)
    List(workflows, commands, inbox, notificationReceipts).traverse_(collection =>
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
    ) *>
      RepositoryIO
        .lift(
          MongoSessionOperations.updateMany(
            reservations,
            session,
            related,
            MongoUpdate.set(
              MongoFields.RetentionExpiresAt,
              Date.from(if (calendarExpires.isAfter(expires)) calendarExpires else expires)
            )
          )
        )
        .void
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
                _ <- guardWrite(
                  workflows,
                  session,
                  MongoFilter.and(
                    MongoFilter.eq(MongoFields.Id, workflow.id.value.toString),
                    MongoFilter.eq("phase", InterviewWorkflowPhase.NotificationsPending.toString)
                  ),
                  MongoUpdate.inc("providerFence", 1L)
                )
                receiptExists <- RepositoryIO.lift(
                  MongoSessionOperations
                    .findOne(inbox, session, MongoFilter.eq(MongoFields.Id, s"${workflow.id.value}:hiring"))
                )
                _ <- RepositoryIO.fromEither(Either.cond(receiptExists.nonEmpty, (), RepositoryError.Conflict))
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

  private def workflowDocument(value: InterviewWorkflow): Document =
    new Document(MongoFields.Id, value.id.value.toString)
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

  private def requestReceiptId(recruiterId: UserId, requestKey: UUID): String =
    s"request:${recruiterId.value}:$requestKey"

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
    new Document(MongoFields.Id, commandId(record.workflowId, record.stepId))
      .append(DocumentTypeField, "command")
      .append(WorkflowIdField, record.workflowId.value.toString)
      .append(StepIdField, record.stepId)
      .append(RevisionField, record.revision)
      .append(CommandField, encodeCommand(record.command))
      .append(CommandStateField, record.state.toString)
      .append(MongoFields.Attempts, record.attempts)
      .append(MongoFields.AvailableAt, Date.from(record.availableAt))
      .append(MongoFields.OccurredAt, Date.from(record.occurredAt))

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
    case InterviewWorkflowCommand.LookupNotificationReceipt(key) =>
      new Document("kind", "lookupNotification").append(IdempotencyKeyField, key)
    case InterviewWorkflowCommand.Retry(step)           => new Document("kind", "retry").append(StepIdField, step)
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
    for {
      workflowId <- string(document, WorkflowIdField).flatMap(uuid).map(InterviewWorkflowId.apply)
      step <- string(document, StepIdField)
      revision <- long(document, RevisionField)
      commandDoc <- Option(document.get(CommandField))
        .collect { case value: Document => value }
        .toRight(RepositoryError.InvalidStoredData)
      command <- decodeCommand(commandDoc)
      stateName <- string(document, CommandStateField)
      state <- InterviewWorkflowCommandState.values
        .find(_.toString == stateName)
        .toRight(RepositoryError.InvalidStoredData)
      attempts <- integer(document, MongoFields.Attempts)
      availableAt <- instant(document, MongoFields.AvailableAt)
      occurredAt <- instant(document, MongoFields.OccurredAt)
      result <- Option(document.get("result")) match {
        case None               => Right(None)
        case Some(name: String) =>
          InterviewCommandResult.values.find(_.toString == name).map(Some(_)).toRight(RepositoryError.InvalidStoredData)
        case _ => Left(RepositoryError.InvalidStoredData)
      }
    } yield InterviewWorkflowCommandRecord(
      workflowId,
      step,
      revision,
      command,
      state,
      attempts,
      availableAt,
      occurredAt,
      result
    )

  private def decodeCommand(document: Document): Either[RepositoryError, InterviewWorkflowCommand] =
    string(document, "kind").flatMap {
      case "reserveCalendar" =>
        string(document, IdempotencyKeyField).map(InterviewWorkflowCommand.ReserveCalendarSlot.apply)
      case "lookupCalendar" =>
        string(document, WorkflowIdField)
          .flatMap(uuid)
          .map(id => InterviewWorkflowCommand.LookupCalendarReservation(InterviewWorkflowId(id)))
      case "commitInterview" =>
        string(document, "expectedStatus")
          .flatMap(enumStatus)
          .map(InterviewWorkflowCommand.CommitAcceptedToInterview.apply)
      case "lookupStatusCommit" =>
        string(document, WorkflowIdField)
          .flatMap(uuid)
          .map(id => InterviewWorkflowCommand.LookupStatusCommitReceipt(InterviewWorkflowId(id)))
      case "releaseCalendar" =>
        string(document, IdempotencyKeyField).map(InterviewWorkflowCommand.ReleaseCalendarSlot.apply)
      case "notify" =>
        for {
          participantName <- string(document, "participant")
          participant <- InterviewParticipant.values
            .find(_.toString == participantName)
            .toRight(RepositoryError.InvalidStoredData)
          key <- string(document, IdempotencyKeyField)
        } yield InterviewWorkflowCommand.Notify(participant, key)
      case "lookupNotification" =>
        string(document, IdempotencyKeyField).map(InterviewWorkflowCommand.LookupNotificationReceipt.apply)
      case "retry"         => string(document, StepIdField).map(InterviewWorkflowCommand.Retry.apply)
      case "requireRepair" => string(document, "reason").map(InterviewWorkflowCommand.RequireRepair.apply)
      case _               => Left(RepositoryError.InvalidStoredData)
    }

  private def reservationDocument(value: InterviewCalendarReservation): Document =
    new Document(MongoFields.Id, value.workflowId.value.toString)
      .append(WorkflowIdField, value.workflowId.value.toString)
      .append("reserveKey", value.idempotencyKey)
      .append("releaseKey", s"${value.workflowId.value}:release")
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
      .append("deliveredAt", Date.from(value.deliveredAt))

  private def decodeNotification(document: Document): Either[RepositoryError, InterviewNotificationReceipt] =
    for {
      workflowId <- string(document, WorkflowIdField).flatMap(uuid).map(InterviewWorkflowId.apply)
      recipient <- string(document, "recipientId").flatMap(uuid).map(UserId.apply)
      participantName <- string(document, "participant")
      participant <- InterviewParticipant.values
        .find(_.toString == participantName)
        .toRight(RepositoryError.InvalidStoredData)
      key <- string(document, MongoFields.Id)
      delivered <- instant(document, "deliveredAt")
    } yield InterviewNotificationReceipt(workflowId, recipient, participant, key, delivered)

  private def sameNotification(left: InterviewNotificationReceipt, right: InterviewNotificationReceipt): Boolean =
    left.workflowId == right.workflowId && left.recipientId == right.recipientId &&
      left.participant == right.participant && left.idempotencyKey == right.idempotencyKey

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
      initiator
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
      .collect { case value: java.lang.Number => value.intValue }
      .toRight(RepositoryError.InvalidStoredData)

  private def uuid(value: String): Either[RepositoryError, UUID] =
    Either.catchNonFatal(UUID.fromString(value)).leftMap(_ => RepositoryError.InvalidStoredData)

  private def enumStatus(value: String): Either[RepositoryError, ApplicationStatus] =
    ApplicationStatus.values.find(_.toString == value).toRight(RepositoryError.InvalidStoredData)

  private def safeFailureCode(value: String): Boolean =
    value.nonEmpty && value.length <= 128 && value.forall(char => char.isLetterOrDigit || char == '_' || char == '-')
}

object MongoInterviewWorkflowRepository {
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
