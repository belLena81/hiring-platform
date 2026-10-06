package com.example.graphQL.cats.service.application

import cats.effect.{Clock, IO}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.{ApplicationStatus, UserRole}
import com.example.graphQL.cats.domain.model.Identifiers.ApplicationId
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.{ActorContext, SearchError, UseCaseError}
import com.example.graphQL.cats.service.auth.ActorAuthorization
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.protocol.{UseCaseIO, UseCaseIO as UseCase}
import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.FiniteDuration

final class InterviewSchedulingService(
    users: UserRepository,
    jobs: JobRepository,
    applications: ApplicationRepository,
    workflows: InterviewWorkflowRepository,
    preCommitWindow: FiniteDuration
) {
  private val authorization = ActorAuthorization(users)

  def schedule(
      actor: ActorContext,
      applicationId: ApplicationId,
      startsAt: Instant,
      endsAt: Instant,
      idempotencyKey: UUID
  ): UseCaseIO[InterviewWorkflow] =
    for {
      user <- authorization.resolve(actor)
      application <- UseCase
        .repository(applications.find(applicationId))
        .subflatMap(_.toRight(UseCaseError.Domain(DomainError.NotFound("application"))))
      job <- UseCase
        .repository(jobs.find(application.jobId))
        .subflatMap(_.toRight(UseCaseError.Domain(DomainError.NotFound("job"))))
      _ <- UseCase.fromEither(
        Either.cond(authorization.canManage(user, job), (), UseCaseError.Domain(DomainError.Forbidden))
      )
      canonicalStart = startsAt.truncatedTo(java.time.temporal.ChronoUnit.MILLIS)
      canonicalEnd = endsAt.truncatedTo(java.time.temporal.ChronoUnit.MILLIS)
      fingerprint = MutationReceiptFingerprint.fromCanonicalInput(
        s"${applicationId.value}|$canonicalStart|$canonicalEnd"
      )
      replay <- UseCase.repository(workflows.findRequest(user.id, idempotencyKey, fingerprint))
      saved <- replay match {
        case Some(existing) => UseCase.pure(existing)
        case None           =>
          for {
            now <- UseCase.liftIO(Clock[IO].realTimeInstant)
            interval <- UseCase.fromEither(
              InterviewInterval
                .validate(startsAt, endsAt, now)
                .leftMap(_ => UseCaseError.Search(SearchError.InvalidFilter("interviewInterval")))
            )
            id <- UseCase.liftIO(IO.randomUUID.map(InterviewWorkflowId.apply))
            acceptedDeadline = now.plusMillis(preCommitWindow.toMillis)
            deadline = if (acceptedDeadline.isBefore(interval.startsAt)) acceptedDeadline else interval.startsAt
            workflow <- UseCase.fromEither(
              InterviewWorkflow
                .create(
                  id,
                  application.id,
                  application.candidateId,
                  job.recruiterId,
                  interval,
                  deadline,
                  idempotencyKey,
                  application.status,
                  Some(user.id)
                )
                .leftMap(_ =>
                  UseCaseError.Domain(
                    DomainError.InvalidStatusTransition(application.status, ApplicationStatus.Interview)
                  )
                )
            )
            result <- UseCase.repository(
              workflows.create(workflow, InterviewWorkflow.initialCommand(workflow), idempotencyKey, fingerprint, now)
            )
            saved <- result match {
              case InterviewWorkflowAdvanceResult.Applied             => UseCase.pure(workflow)
              case InterviewWorkflowAdvanceResult.Duplicate(existing) => UseCase.pure(existing)
              case InterviewWorkflowAdvanceResult.StaleRevision       =>
                UseCase.left(UseCaseError.Repository(RepositoryError.Conflict))
            }
          } yield saved
      }
    } yield saved

  def inspect(actor: ActorContext, id: InterviewWorkflowId): UseCaseIO[InterviewWorkflow] =
    for {
      user <- authorization.resolve(actor)
      value <- UseCase.repository(
        if (user.role == UserRole.Admin) workflows.findForAdmin(id)
        else workflows.findForActor(id, InterviewWorkflowAccess(user.id, user.role))
      )
      workflow <- UseCase.fromEither(value.toRight(UseCaseError.Domain(DomainError.NotFound("interviewWorkflow"))))
    } yield workflow

  def repair(
      actor: ActorContext,
      id: InterviewWorkflowId,
      expectedRevision: Long,
      idempotencyKey: UUID
  ): UseCaseIO[InterviewWorkflow] =
    for {
      user <- authorization.resolve(actor)
      _ <- UseCase.fromEither(Either.cond(user.role == UserRole.Admin, (), UseCaseError.Domain(DomainError.Forbidden)))
      workflow <- inspect(actor, id)
      now <- UseCase.liftIO(Clock[IO].realTimeInstant)
      repaired <- UseCase.repository(workflows.repair(workflow, expectedRevision, idempotencyKey, now, user.id))
    } yield repaired
}
