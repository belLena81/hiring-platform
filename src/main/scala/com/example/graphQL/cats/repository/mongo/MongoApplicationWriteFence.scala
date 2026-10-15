package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.JobId
import com.example.graphQL.cats.domain.model.UserRole
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.port.*
import com.mongodb.client.model.{Projections, UpdateOptions}
import mongo4cats.client.ClientSession
import mongo4cats.database.MongoDatabase

/** Carries the acting user's predicate into an application write transaction.
  *
  * Inside the caller's transaction it (1) reads the actor and requires an active account in the claimed role (the
  * singleton Admin for `Admin`), (2) bumps the actor's subject fence, the same `outbox_subject_fences` document account
  * deletion upserts, and (3) for a Recruiter requires that the job belongs to them. Account deletion commits the
  * tombstone and the fence together, so a write that read the actor before the deletion committed conflicts on the
  * fence, is retried by the transaction runner, and then sees the deleted account. A refused predicate is
  * `AuthorityRevoked`. `users.version` is deliberately not touched, so unrelated user writes never conflict with it.
  */
private[mongo] final class MongoApplicationWriteFence(database: MongoDatabase[IO]) {
  private def users = Mongo4catsCollections.documents(database, MongoCollections.Users)
  private def jobs = Mongo4catsCollections.documents(database, MongoCollections.Jobs)
  private def subjectFences = Mongo4catsCollections.documents(database, MongoCollections.OutboxSubjectFences)

  /** Submission: the actor must be an active Candidate and, when the job exists, its recruiter must be an active
    * account. A deleted recruiter's jobs stay open until their follow-up closure completes, and a candidate must not be
    * able to apply to them in that window. A missing job is left to the status guard of the write.
    */
  def forSubmit(
      session: Option[ClientSession[IO]],
      actor: ApplicationWriteActor,
      jobId: JobId
  ): RepositoryF[IO, Unit] =
    if (actor.role != UserRole.Candidate) RepositoryF.at[IO].raise(RepositoryError.AuthorityRevoked)
    else requireActive(session, actor) *> bumpFence(session, actor) *> requireJobRecruiterActive(session, jobId)

  private def requireJobRecruiterActive(session: Option[ClientSession[IO]], jobId: JobId): RepositoryF[IO, Unit] =
    RepositoryF
      .lift(
        MongoSessionOperations.findOne(
          jobs,
          session,
          MongoFilter.eq(MongoFields.Id, jobId.value.toString),
          Some(Projections.include(MongoFields.RecruiterId))
        )
      )
      .flatMap {
        case None      => RepositoryF.at[IO].pure(())
        case Some(job) =>
          Option(job.getString(MongoFields.RecruiterId)).fold[RepositoryF[IO, Unit]](
            RepositoryF.at[IO].raise(RepositoryError.InvalidStoredData)
          ) { recruiterId =>
            RepositoryF
              .lift(
                MongoSessionOperations.findOne(
                  users,
                  session,
                  MongoFilter.and(
                    MongoFilter.eq(MongoFields.Id, recruiterId),
                    MongoFilter.eq(MongoFields.AccountStatus, "Active")
                  ),
                  Some(Projections.include(MongoFields.Id))
                )
              )
              .subflatMap(found => Either.cond(found.nonEmpty, (), RepositoryError.Conflict))
          }
      }

  /** Status change: an active Recruiter owning `jobId`, or the active singleton Admin (ownership bypass). */
  def forStatusChange(
      session: Option[ClientSession[IO]],
      actor: ApplicationWriteActor,
      jobId: JobId
  ): RepositoryF[IO, Unit] = actor.role match {
    case UserRole.Candidate => RepositoryF.at[IO].raise(RepositoryError.AuthorityRevoked)
    case UserRole.Admin     => requireActive(session, actor) *> bumpFence(session, actor)
    case UserRole.Recruiter =>
      requireActive(session, actor) *> bumpFence(session, actor) *> requireOwnership(session, actor, jobId)
  }

  private def requireActive(session: Option[ClientSession[IO]], actor: ApplicationWriteActor): RepositoryF[IO, Unit] = {
    val base = List(
      MongoFilter.eq(MongoFields.Id, actor.id.value.toString),
      MongoFilter.eq(MongoFields.Role, actor.role.toString),
      MongoFilter.eq(MongoFields.AccountStatus, "Active")
    )
    val admin =
      Option.when(actor.role == UserRole.Admin)(MongoFilter.eq(MongoFields.AdminSingletonKey, "singleton-admin"))
    RepositoryF
      .lift(
        MongoSessionOperations
          .findOne(users, session, MongoFilter.and((base ++ admin.toList)*), Some(Projections.include(MongoFields.Id)))
      )
      .subflatMap(found => Either.cond(found.nonEmpty, (), RepositoryError.AuthorityRevoked))
  }

  private def bumpFence(session: Option[ClientSession[IO]], actor: ApplicationWriteActor): RepositoryF[IO, Unit] =
    RepositoryF
      .lift(
        MongoSessionOperations.updateOne(
          subjectFences,
          session,
          MongoFilter.eq(MongoFields.Id, actor.id.value.toString),
          MongoUpdate.inc(MongoFields.FencingVersion, 1L),
          new UpdateOptions().upsert(true)
        )
      )
      .void

  private def requireOwnership(
      session: Option[ClientSession[IO]],
      actor: ApplicationWriteActor,
      jobId: JobId
  ): RepositoryF[IO, Unit] =
    RepositoryF
      .lift(
        MongoSessionOperations.findOne(
          jobs,
          session,
          MongoFilter.and(
            MongoFilter.eq(MongoFields.Id, jobId.value.toString),
            MongoFilter.eq(MongoFields.RecruiterId, actor.id.value.toString)
          ),
          Some(Projections.include(MongoFields.Id))
        )
      )
      .subflatMap(found => Either.cond(found.nonEmpty, (), RepositoryError.AuthorityRevoked))
}
