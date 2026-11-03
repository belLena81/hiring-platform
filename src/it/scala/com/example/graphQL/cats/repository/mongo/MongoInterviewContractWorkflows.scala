package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.{ApplicationStatus, UserRole}
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, UserId}
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.port.MutationWriteContext
import java.time.Instant
import java.util.UUID

/** Stores a fresh interview workflow with its participants so a provider contract can address it. */
object MongoInterviewContractWorkflows {
  def create(
      fixture: MongoAccessEvaluationSupport.Fixture,
      repository: MongoInterviewWorkflowRepository,
      candidate: UserId,
      recruiter: UserId,
      interval: InterviewInterval
  ): IO[InterviewWorkflow] = {
    val now = Instant.now()
    for {
      workflow <- IO.fromEither(
        InterviewWorkflow
          .create(
            InterviewWorkflowId(UUID.randomUUID()),
            ApplicationId(UUID.randomUUID()),
            candidate,
            recruiter,
            interval,
            now.plusSeconds(300),
            UUID.randomUUID(),
            ApplicationStatus.Accepted,
            recruiter,
            UserRole.Recruiter
          )
          .leftMap(error => new AssertionError(s"Invalid workflow: $error"))
      )
      _ <- InterviewSchedulingFixtures.seed(fixture.database, List(workflow), now)
      _ <- repository
        .create(workflow, InterviewWorkflow.initialCommand(workflow), now, MutationWriteContext.directWrite)
        .value
        .flatMap(_.fold(error => IO.raiseError(new AssertionError(s"create failed: $error")), _ => IO.unit))
    } yield workflow
  }
}
