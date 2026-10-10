package com.example.graphQL.cats.repository.mongo

import cats.data.EitherT
import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.RepositoryError
import com.mongodb.client.model.UpdateOptions
import mongo4cats.client.ClientSession
import org.bson.Document

import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.jdk.CollectionConverters.*

/** Durable calendar reservations: holds, cancellations and releases fenced against the workflow. */
final class MongoInterviewCalendarLedger(store: MongoInterviewWorkflowStore) extends InterviewCalendarLedger {
  import MongoInterviewFields.*
  import MongoInterviewWorkflowCodec.*
  import store.*

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
          workflow <- loadWorkflow(reservation.workflowId, session).orConflict
          actualNow <- RepositoryIO.lift(clock.realTimeInstant)
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
          _ <- bumpProviderFence(session, workflow.id, workflow.phase)
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
                MongoFilter.lt(StartField, reservation.interval.endsAt.toDate),
                MongoFilter.gt(EndField, reservation.interval.startsAt.toDate),
                MongoFilter.exists(ReleasedAtField, false)
              )
              RepositoryIO.lift(MongoSessionOperations.findOne(reservations, session, overlap)).flatMap {
                case Some(_) => RepositoryIO.raise(RepositoryError.Conflict)
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
          }
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
            case None                                                    => ().pure[RepositoryIO]
            case Some(document) if document.containsKey(ReleasedAtField) => ().pure[RepositoryIO]
            case Some(document)                                          =>
              for {
                reservation <- RepositoryIO.fromEither(decodeReservation(document))
                _ <- fenceExecution(session, reservation.workflowId, execution)
                receipt <- RepositoryIO.lift(
                  MongoSessionOperations.findById(inbox, session, s"${reservation.workflowId.value}:hiring")
                )
                _ <- RepositoryIO.fromEither(Either.cond(receipt.isEmpty, (), RepositoryError.Conflict))
                _ <- bumpProviderFence(session, reservation.workflowId, InterviewWorkflowPhase.CompensationPending)
                _ <- markReleased(session, reservation, at)
              } yield ()
          }
      }
    }
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
                case None =>
                  (InterviewCalendarCancellation.UnknownReservation: InterviewCalendarCancellation).pure[RepositoryIO]
                case Some(document) =>
                  RepositoryIO.fromEither(decodeReservation(document)).flatMap { reservation =>
                    reservation.releasedAt match {
                      case Some(released) =>
                        (InterviewCalendarCancellation.AlreadyCancelled(released): InterviewCalendarCancellation)
                          .pure[RepositoryIO]
                      case None =>
                        for {
                          _ <- fenceExecution(session, workflowId, execution)
                          workflow <- loadWorkflow(workflowId, session).orConflict
                          // The provider only cancels what the workflow currently requires cancelled.
                          _ <- RepositoryIO.fromEither(
                            Either.cond(
                              InterviewCalendarFence.cancellableGeneration(workflow).contains(generation),
                              (),
                              RepositoryError.Conflict
                            )
                          )
                          _ <- bumpProviderFence(session, workflow.id, workflow.phase)
                          _ <- markReleased(session, reservation, released)
                        } yield InterviewCalendarCancellation.Cancelled(released)
                    }
                  }
              }
          }
        }
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
        MongoUpdate.set(ReleasedAtField, at.toDate)
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
      .void
  }

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
      .append(StartField, value.interval.startsAt.toDate)
      .append(EndField, value.interval.endsAt.toDate)
      .append("reservedAt", value.reservedAt.toDate)

  private def sameReservation(left: InterviewCalendarReservation, right: InterviewCalendarReservation): Boolean =
    left.workflowId == right.workflowId && left.idempotencyKey == right.idempotencyKey &&
      left.candidateId == right.candidateId && left.recruiterId == right.recruiterId && left.interval == right.interval
}
