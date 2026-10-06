package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.JobId
import com.example.graphQL.cats.domain.workflow.InterviewWorkflow
import mongo4cats.database.MongoDatabase
import java.time.Instant
import java.util.UUID

private[mongo] object InterviewSchedulingFixtures {
  def seed(database: MongoDatabase[IO], workflows: List[InterviewWorkflow], now: Instant): IO[Unit] = {
    val recruiters = workflows.map(_.recruiterId).toSet
    val users = workflows.flatMap(workflow => List(workflow.candidateId, workflow.recruiterId)).distinct.map { id =>
      val role = if (recruiters.contains(id)) UserRole.Recruiter else UserRole.Candidate
      val profile =
        if (role == UserRole.Recruiter) UserProfile.Recruiter(RecruiterProfile("Scheduling fixture", None))
        else UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None))
      User(id, None, s"Interview-${id.value}", role, Some(profile), now)
    }
    users.traverse_(user =>
      Mongo4catsCollections
        .documents(database, MongoCollections.Users)
        .flatMap(_.insertOne(MongoHiringCodecs.user(user)))
    ) *>
      workflows.traverse_ { workflow =>
        val job = Job(
          JobId(UUID.randomUUID()),
          workflow.recruiterId,
          "Interview engineer",
          "Scheduling evidence",
          List("Scala"),
          Set("Scala"),
          Location("Cyprus", "Nicosia", false),
          JobStatus.Open,
          now,
          now
        )
        val application =
          Application(workflow.applicationId, workflow.candidateId, job.id, ApplicationStatus.Accepted, now, now)
        Mongo4catsCollections
          .documents(database, MongoCollections.Jobs)
          .flatMap(_.insertOne(MongoHiringCodecs.job(job))) *>
          Mongo4catsCollections
            .documents(database, MongoCollections.Applications)
            .flatMap(_.insertOne(MongoHiringCodecs.application(application)))
            .void
      }
  }
}
