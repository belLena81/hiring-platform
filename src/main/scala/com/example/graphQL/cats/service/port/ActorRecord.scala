package com.example.graphQL.cats.service.port

import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.{AccountStatus, Job, JobStatus, User, UserRole}

/** The actor projection: the only facts about a user that authorization needs. It carries no profile, credential or
  * embedding, so a per-request authorization read never transfers or decodes them.
  */
final case class ActorRecord(id: UserId, role: UserRole, accountStatus: AccountStatus, adminSingleton: Boolean)

object ActorRecord {
  def of(user: User): ActorRecord = ActorRecord(user.id, user.role, user.accountStatus, user.role == UserRole.Admin)
}

/** The ownership projection of a job: who manages it and whether it is open. It carries no description or embedding. */
final case class JobOwner(id: JobId, recruiterId: UserId, status: JobStatus)

object JobOwner {
  def of(job: Job): JobOwner = JobOwner(job.id, job.recruiterId, job.status)
}
