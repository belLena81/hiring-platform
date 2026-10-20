package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.port.*
import com.mongodb.client.model.{Projections, UpdateOptions}
import mongo4cats.client.ClientSession
import mongo4cats.database.MongoDatabase

/** The subject fence: a write transaction proves an account is active by reading it, then advances the account's
  * `outbox_subject_fences.fencingVersion`, the same document account deletion upserts together with the tombstone. A
  * transaction that read the account before the deletion committed conflicts on the fence, is retried and then sees the
  * deleted account. `users.version` (the content revision) is never touched, so unrelated profile writes never conflict
  * with it.
  */
private[mongo] final class MongoSubjectFence(database: MongoDatabase[IO]) {
  private def users = Mongo4catsCollections.documents(database, MongoCollections.Users)
  private def subjectFences = Mongo4catsCollections.documents(database, MongoCollections.OutboxSubjectFences)

  /** Advances the fence of `subjectId` (created on first use). */
  def bump(session: Option[ClientSession[IO]], subjectId: String): RepositoryF[IO, Unit] =
    RepositoryF
      .lift(
        MongoSessionOperations.updateOne(
          subjectFences,
          session,
          MongoFilter.eq(MongoFields.Id, subjectId),
          MongoUpdate.inc(MongoFields.FencingVersion, 1L),
          new UpdateOptions().upsert(true)
        )
      )
      .void

  /** Reads the active account with the extra predicates inside the session (`mismatch` when none matches), then bumps
    * its fence.
    */
  def requireActiveAndBump(
      session: Option[ClientSession[IO]],
      subjectId: String,
      extra: List[MongoFilter],
      mismatch: RepositoryError
  ): RepositoryF[IO, Unit] =
    RepositoryF
      .lift(
        MongoSessionOperations.findOne(
          users,
          session,
          MongoFilter.and(
            (MongoFilter.eq(MongoFields.Id, subjectId) ::
              MongoFilter.eq(MongoFields.AccountStatus, "Active") :: extra)*
          ),
          Some(Projections.include(MongoFields.Id))
        )
      )
      .subflatMap(found => Either.cond(found.nonEmpty, (), mismatch)) *> bump(session, subjectId)
}
