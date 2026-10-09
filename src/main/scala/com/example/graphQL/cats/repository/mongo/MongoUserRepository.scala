package com.example.graphQL.cats.repository.mongo

import cats.data.EitherT
import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.events.{OperationalEventType, OperationalEvents}
import com.example.graphQL.cats.service.Diagnostics
import com.mongodb.client.model.{Filters, UpdateOptions}
import com.mongodb.client.result.UpdateResult
import mongo4cats.client.{ClientSession, MongoClient}
import mongo4cats.database.MongoDatabase
import org.bson.Document

import com.example.graphQL.cats.service.read.*
import java.time.Instant
import scala.util.chaining.*
import java.util.Date

final class MongoUserRepository(
    database: MongoDatabase[IO],
    transactionRunner: MongoTransactionRunner,
    embeddingWork: MongoEmbeddingWorkEnqueuer,
    diagnostics: Diagnostics
) extends UserRepository
    with UserAccountRepository
    with MongoOperationalEventInsertion {
  private def collection = Mongo4catsCollections.documents(database, MongoCollections.Users)
  private def registry = Mongo4catsCollections.documents(database, MongoCollections.AccountRegistry)
  private def outbox = Mongo4catsCollections.documents(database, MongoCollections.EventOutbox)
  private def subjectFences = Mongo4catsCollections.documents(database, MongoCollections.OutboxSubjectFences)
  private val MaxJobsClosedByAccountDeletion = 1000

  def insert(user: User): RepositoryIO[Unit] =
    if (!user.roleProfileIsValid) RepositoryIO.fromEither(Left(RepositoryError.Conflict))
    else
      MongoRepositorySupport
        .repositoryGuard(diagnostics, "MongoUserRepository.insert")(
          RepositoryIO
            .lift(MongoSessionOperations.insertOne(collection, None, MongoHiringCodecs.user(user)))
            .subflatMap(MongoRepositorySupport.writeResult(_).void)
        )(MongoErrors.duplicateAsConflict)

  override def find(id: UserId): RepositoryIO[Option[User]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "MongoUserRepository.find")(
        RepositoryIO
          .lift(MongoSessionOperations.findById(collection, None, id.value.toString))
          .subflatMap(document => MongoStoredDocumentDecoding.repository(document.traverse(MongoHiringCodecs.readUser)))
      )

  override def findVersioned(id: UserId): RepositoryIO[Option[Versioned[User]]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "MongoUserRepository.findVersioned")(
        RepositoryIO
          .lift(MongoSessionOperations.findById(collection, None, id.value.toString))
          .subflatMap(document =>
            MongoStoredDocumentDecoding.repository(document.traverse(MongoHiringCodecs.readVersionedUser))
          )
      )

  private def findWithSession(
      id: UserId,
      session: Option[ClientSession[IO]]
  ): RepositoryIO[Option[User]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "MongoUserRepository.findWithSession")(
        RepositoryIO
          .lift(MongoSessionOperations.findOne(collection, session, MongoFilter.eq(MongoFields.Id, id.value.toString)))
          .subflatMap(document => MongoStoredDocumentDecoding.repository(document.traverse(MongoHiringCodecs.readUser)))
      )

  override def findMany(ids: List[UserId]): RepositoryIO[List[User]] =
    MongoKeysetPaging.byId(collection, ids.map(_.value.toString))(MongoHiringCodecs.readUser)(diagnostics)

  override def relatedUsers(scope: HiringReadScope, keys: List[UserRelationKey]): RepositoryIO[List[RelatedUser]] = {
    import MongoAuthorizedReadQueries.*
    val distinct = keys.distinct
    val candidates = distinct.collect { case key: UserRelationKey.ApplicationCandidate => key }
    val recruiters = distinct.collect { case key: UserRelationKey.JobRecruiter => key }
    def select[K <: UserRelationKey](
        requested: List[K],
        parentCollection: String,
        relatedField: String,
        predicate: K => org.bson.conversions.Bson,
        access: List[Document],
        matches: (K, Document) => Boolean
    ): RepositoryIO[List[RelatedUser]] =
      if (requested.isEmpty) RepositoryIO.fromEither(Right(Nil))
      else {
        val pipeline = List(matching(Filters.or(requested.map(predicate)*))) ++ actor(scope) ++ access ++ List(
          lookup(MongoCollections.Users, relatedField, MongoFields.Id, "related"),
          unwind("related")
        )
        documents(database, parentCollection, pipeline, requested.size, diagnostics).subflatMap(values =>
          MongoStoredDocumentDecoding.values(
            values.flatMap(document =>
              requested
                .find(key => matches(key, document))
                .map(key =>
                  MongoHiringCodecs
                    .readUser(document.get("related", classOf[Document]))
                    .map(user => RelatedUser(key, user))
                )
            )
          )
        )
      }
    for {
      candidateUsers <- select[UserRelationKey.ApplicationCandidate](
        candidates,
        MongoCollections.Applications,
        MongoFields.CandidateId,
        key =>
          Filters.and(
            Filters.eq(MongoFields.Id, key.applicationId.value.toString),
            Filters.eq(MongoFields.CandidateId, key.userId.value.toString)
          ),
        applicationAccess(scope),
        (key, document) =>
          document.getString(MongoFields.Id) == key.applicationId.value.toString && document.getString(
            MongoFields.CandidateId
          ) == key.userId.value.toString
      )
      recruiterUsers <- select[UserRelationKey.JobRecruiter](
        recruiters,
        MongoCollections.Jobs,
        MongoFields.RecruiterId,
        key =>
          Filters.and(
            Filters.eq(MongoFields.Id, key.jobId.value.toString),
            Filters.eq(MongoFields.RecruiterId, key.userId.value.toString)
          ),
        jobAccess(scope),
        (key, document) =>
          document.getString(MongoFields.Id) == key.jobId.value.toString && document.getString(
            MongoFields.RecruiterId
          ) == key.userId.value.toString
      )
    } yield candidateUsers ++ recruiterUsers
  }

  override def updateEmbedding(id: UserId, embedding: EntityEmbedding): RepositoryIO[Unit] =
    findVersioned(id).flatMap {
      case Some(user) => updateEmbedding(user, embedding)
      case None       => EitherT.leftT[IO, Unit](RepositoryError.Conflict)
    }

  override def updateEmbedding(
      observed: Versioned[User],
      embedding: EntityEmbedding
  ): RepositoryIO[Unit] = {
    val encoded = MongoHiringCodecs.embeddingDocument(embedding)
    RepositoryIO
      .lift(
        MongoSessionOperations
          .updateOne(
            collection,
            None,
            MongoFilter.and(
              MongoFilter.eq(MongoFields.Id, observed.value.id.value.toString),
              MongoFilter.eq(MongoFields.Version, observed.version),
              MongoFilter.eq(MongoFields.AccountStatus, AccountStatus.Active.toString),
              MongoFilter.eq(MongoFields.Role, UserRole.Candidate.toString),
              MongoFilter.lt(MongoFields.Version, Long.MaxValue)
            ),
            MongoUpdate.combine(
              MongoUpdate.set(MongoFields.Embedding, encoded.get(MongoFields.Embedding)),
              MongoUpdate.set(MongoFields.EmbeddingMeta, encoded.get(MongoFields.EmbeddingMeta)),
              MongoUpdate.inc(MongoFields.Version, 1L)
            )
          )
      )
      .subflatMap(MongoRepositorySupport.matchedOne(_))
      .pipe(effect =>
        MongoRepositorySupport
          .repositoryGuard(diagnostics, "MongoUserRepository.updateEmbedding")(effect)(MongoErrors.duplicateAsConflict)
      )
  }

  override def initialized: RepositoryIO[Boolean] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "MongoUserRepository.initialized")(
        RepositoryIO
          .lift(MongoSessionOperations.findById(registry, None, "user-account-registry"))
          .map(document => document.exists(value => Option(value.getString(MongoFields.State)).contains("Initialized")))
      )

  override def bootstrap(
      user: User,
      passwordHash: PasswordHash,
      context: MutationWriteContext
  ): RepositoryIO[Unit] =
    if (!user.roleProfileIsValid || user.role != UserRole.Admin || !user.adminSingleton)
      EitherT.leftT[IO, Unit](RepositoryError.Conflict)
    else
      MongoMutationWriteContext
        .run(context, transactionRunner, transactionRequired = true) { session =>
          bootstrapWithSession(user, passwordHash, session)
        }
        .pipe(effect =>
          MongoRepositorySupport
            .repositoryGuard(diagnostics, "MongoUserRepository.write")(effect)(MongoErrors.duplicateAsConflict)
        )

  private def bootstrapWithSession(
      user: User,
      passwordHash: PasswordHash,
      session: Option[ClientSession[IO]]
  ): RepositoryIO[Unit] =
    val stateFilter = MongoFilter.and(
      MongoFilter.eq(MongoFields.Id, "user-account-registry"),
      MongoFilter.eq(MongoFields.State, "Uninitialized")
    )
    for {
      state <- RepositoryIO.lift(MongoSessionOperations.findOne(registry, session, stateFilter))
      _ <- RepositoryIO.fromEither(state.toRight(RepositoryError.Conflict))
      existing <- RepositoryIO.lift(MongoSessionOperations.findOne(collection, session, MongoFilter.and()))
      _ <- RepositoryIO.fromEither(Either.cond(existing.isEmpty, (), RepositoryError.Conflict))
      inserted <- RepositoryIO.lift(
        MongoSessionOperations.insertOne(collection, session, MongoHiringCodecs.userWithPassword(user, passwordHash))
      )
      _ <- RepositoryIO.fromEither(MongoRepositorySupport.writeResult(inserted))
      updated <- RepositoryIO.lift(
        MongoSessionOperations.updateOne(
          registry,
          session,
          stateFilter,
          MongoUpdate.combine(
            MongoUpdate.set(MongoFields.State, "Initialized"),
            MongoUpdate.set(MongoFields.AdminId, user.id.value.toString)
          )
        )
      )
      _ <- RepositoryIO.fromEither(MongoRepositorySupport.writeResult(updated))
    } yield ()

  private def writeAccountSession(
      user: User,
      passwordHash: PasswordHash,
      now: Instant,
      session: Option[ClientSession[IO]]
  ): RepositoryIO[Unit] =
    if (!user.roleProfileIsValid || user.role == UserRole.Admin)
      RepositoryIO.fromEither(Left(RepositoryError.Conflict))
    else
      {
        val stateFilter = MongoFilter.and(
          MongoFilter.eq(MongoFields.Id, "user-account-registry"),
          MongoFilter.eq(MongoFields.State, "Initialized")
        )
        for {
          state <- RepositoryIO.lift(MongoSessionOperations.findOne(registry, session, stateFilter))
          _ <- RepositoryIO.fromEither(state.toRight(RepositoryError.Conflict))
          inserted <- RepositoryIO.lift(
            MongoSessionOperations
              .insertOne(collection, session, MongoHiringCodecs.userWithPassword(user, passwordHash))
          )
          _ <- RepositoryIO.fromEither(MongoRepositorySupport.writeResult(inserted))
          _ <- {
            if (user.role == UserRole.Candidate)
              embeddingWork.enqueue(
                session,
                EmbeddingWorkKey(EmbeddingWorkKind.CandidateProfile, user.id.value.toString),
                now
              )
            else RepositoryIO.fromEither(Right(()))
          }
        } yield ()
      }.pipe(effect =>
        MongoRepositorySupport.transactionGuard(diagnostics, "MongoUserRepository.write", session)(effect)(
          MongoErrors.duplicateAsConflict
        )
      )

  override def createAccount(
      user: User,
      passwordHash: PasswordHash,
      now: Instant,
      context: MutationWriteContext
  ): RepositoryIO[Unit] =
    MongoMutationWriteContext
      .run(
        context,
        transactionRunner,
        transactionRequired = user.role == UserRole.Candidate && embeddingWork.requiresTransaction
      )(session => writeAccountSession(user, passwordHash, now, session))
      .pipe(effect =>
        MongoRepositorySupport.repositoryGuard(diagnostics, "MongoUserRepository.write")(effect)(
          MongoErrors.duplicateAsConflict
        )
      )

  override def findByCanonicalName(nameCanonical: String): RepositoryIO[Option[AccountCredentials]] =
    RepositoryIO
      .lift(collection.flatMap(_.find(Filters.eq(MongoFields.NameCanonical, nameCanonical)).first))
      .subflatMap { document =>
        MongoStoredDocumentDecoding.repository(document.traverse(MongoHiringCodecs.readCredentials)).map(_.flatten)
      }
      .pipe(effect =>
        MongoRepositorySupport
          .repositoryGuard(diagnostics, "MongoUserRepository.read")(effect)
      )

  override def updateProfile(
      userId: UserId,
      profile: UserProfile,
      now: Instant,
      context: MutationWriteContext
  ): RepositoryIO[User] =
    profile match {
      case _: UserProfile.Candidate =>
        MongoMutationWriteContext
          .run(context, transactionRunner, transactionRequired = embeddingWork.requiresTransaction) { session =>
            updateCandidateProfileWithEmbeddingWork(userId, profile, now, session) *>
              findWithSession(userId, session).subflatMap(_.toRight(RepositoryError.MissingStoredResult))
          }
          .pipe(effect =>
            MongoRepositorySupport.repositoryGuard(diagnostics, "MongoUserRepository.write")(effect)(
              MongoErrors.duplicateAsConflict
            )
          )
      case _ =>
        MongoMutationWriteContext
          .run(context, transactionRunner, transactionRequired = false) { session =>
            updateProfileDirect(userId, profile, now, session)
          }
          .pipe(effect =>
            MongoRepositorySupport.repositoryGuard(diagnostics, "MongoUserRepository.write")(effect)(
              MongoErrors.duplicateAsConflict
            )
          )
    }

  private def updateProfileDirect(
      userId: UserId,
      profile: UserProfile,
      now: Instant,
      session: Option[ClientSession[IO]]
  ): RepositoryIO[User] =
    RepositoryIO
      .lift(
        MongoSessionOperations.updateOne(
          collection,
          session,
          MongoFilter.and(
            MongoFilter.eq(MongoFields.Id, userId.value.toString),
            MongoFilter.eq(MongoFields.AccountStatus, AccountStatus.Active.toString),
            MongoFilter.lt(MongoFields.Version, Long.MaxValue)
          ),
          MongoUpdate.combine(
            MongoUpdate.set(MongoFields.Profile, MongoHiringCodecs.profile(profile)),
            MongoUpdate.set(MongoFields.UpdatedAt, Date.from(now)),
            MongoUpdate.inc(MongoFields.Version, 1L)
          )
        )
      )
      .flatMap {
        case Some(result) if result.getMatchedCount == 1L =>
          findWithSession(userId, session).subflatMap(_.toRight(RepositoryError.MissingStoredResult))
        case Some(_) => RepositoryIO.fromEither(Left(RepositoryError.Conflict))
        case None    => RepositoryIO.fromEither(Left(RepositoryError.MissingWriteResult))
      }
      .pipe(effect =>
        MongoRepositorySupport.repositoryGuard(diagnostics, "MongoUserRepository.write")(effect)(
          MongoErrors.duplicateAsConflict
        )
      )

  private def updateCandidateProfileWithEmbeddingWork(
      userId: UserId,
      profile: UserProfile,
      now: Instant,
      session: Option[ClientSession[IO]]
  ): RepositoryIO[Unit] = {
    val filter = MongoFilter.and(
      MongoFilter.eq(MongoFields.Id, userId.value.toString),
      MongoFilter.eq(MongoFields.Role, UserRole.Candidate.toString),
      MongoFilter.eq(MongoFields.AccountStatus, AccountStatus.Active.toString),
      MongoFilter.lt(MongoFields.Version, Long.MaxValue)
    )
    val update = MongoUpdate.combine(
      MongoUpdate.set(MongoFields.Profile, MongoHiringCodecs.profile(profile)),
      MongoUpdate.set(MongoFields.UpdatedAt, Date.from(now)),
      MongoUpdate.inc(MongoFields.Version, 1L)
    )
    RepositoryIO
      .lift(MongoSessionOperations.updateOne(collection, session, filter, update))
      .flatMap {
        case Some(result) if result.getMatchedCount == 1L =>
          embeddingWork.enqueue(
            session,
            EmbeddingWorkKey(EmbeddingWorkKind.CandidateProfile, userId.value.toString),
            now
          )
        case Some(_) => RepositoryIO.fromEither(Left(RepositoryError.Conflict))
        case None    => RepositoryIO.fromEither(Left(RepositoryError.MissingWriteResult))
      }
      .pipe(effect =>
        MongoRepositorySupport.transactionGuard(diagnostics, "MongoUserRepository.write", session)(effect)(
          MongoErrors.duplicateAsConflict
        )
      )
  }

  override def listAccounts(page: UserPageRequest): RepositoryIO[List[User]] = {
    val filters = List(
      Some(MongoFilter.eq(MongoFields.AccountStatus, page.status.toString)),
      page.role.map(role => MongoFilter.eq(MongoFields.Role, role.toString)),
      page.cursor.map(cursor =>
        MongoFilter.beforeCursor(MongoFields.CreatedAt, cursor.createdAt, cursor.id.value.toString)
      )
    ).flatten
    MongoKeysetPaging.page(collection, MongoFilter.and(filters*), MongoFields.CreatedAt, page.pageSize)(
      MongoHiringCodecs.readUser
    )(diagnostics)
  }

  override def deleteAccount(
      userId: UserId,
      now: Instant,
      tombstone: String,
      context: MutationWriteContext
  ): RepositoryIO[Unit] =
    MongoMutationWriteContext
      .run(context, transactionRunner, transactionRequired = true) { session =>
        deleteAccountWithSession(userId, now, tombstone, session)
      }
      .pipe(effect =>
        MongoRepositorySupport.repositoryGuard(diagnostics, "MongoUserRepository.write")(effect)(
          MongoErrors.duplicateAsConflict
        )
      )

  private def deleteAccountWithSession(
      userId: UserId,
      now: Instant,
      tombstone: String,
      session: Option[ClientSession[IO]]
  ): RepositoryIO[Unit] = {
    val userFilter =
      MongoFilter.and(
        MongoFilter.eq(MongoFields.Id, userId.value.toString),
        MongoFilter.eq(MongoFields.AccountStatus, AccountStatus.Active.toString),
        MongoFilter.lt(MongoFields.Version, Long.MaxValue)
      )
    val userUpdate = MongoUpdate.combine(
      MongoUpdate.set(MongoFields.AccountStatus, AccountStatus.Deleted.toString),
      MongoUpdate.set(MongoFields.DeletedAt, Date.from(now)),
      MongoUpdate.set(MongoFields.Name, tombstone),
      MongoUpdate.set(MongoFields.NameCanonical, AccountName.canonical(tombstone)),
      MongoUpdate.unset(MongoFields.PasswordHash),
      MongoUpdate.unset(MongoFields.Profile),
      MongoUpdate.unset(MongoFields.Embedding),
      MongoUpdate.unset(MongoFields.EmbeddingMeta),
      MongoUpdate.unset(MongoFields.RecruiterProfile),
      MongoUpdate.unset(MongoFields.Email),
      MongoUpdate.unset(MongoFields.EmailCanonical),
      MongoUpdate.inc(MongoFields.Version, 1L)
    )
    val userWrite = MongoSessionOperations.updateOne(collection, session, userFilter, userUpdate)
    RepositoryIO.lift(userWrite).flatMap {
      case Some(result) if result.getMatchedCount == 1L =>
        markSubjectFenceDeleted(session, userId, now) *>
          new MongoInterviewSubjectCleanup(database).enqueue(userId, now, session) *>
          closeRecruiterJobs(userId, now, session)
      case Some(_) => RepositoryIO.fromEither(Left(RepositoryError.Conflict))
      case None    => RepositoryIO.fromEither(Left(RepositoryError.MissingWriteResult))
    }
  }

  private def markSubjectFenceDeleted(
      session: Option[ClientSession[IO]],
      userId: UserId,
      now: Instant
  ): RepositoryIO[Unit] = {
    val filter = MongoFilter.eq(MongoFields.Id, userId.value.toString)
    val update = MongoUpdate.combine(
      MongoUpdate.setOnInsert(MongoFields.Id, userId.value.toString),
      MongoUpdate.set(MongoFields.Deleted, true),
      MongoUpdate.set(MongoFields.DeletedAt, Date.from(now))
    )
    val operation =
      MongoSessionOperations.updateOne(subjectFences, session, filter, update, new UpdateOptions().upsert(true))
    RepositoryIO
      .lift(operation)
      .subflatMap(MongoRepositorySupport.writeResult(_).void)
      .pipe(effect =>
        MongoRepositorySupport
          .repositoryGuard(diagnostics, "MongoUserRepository.read")(effect)
      )
  }

  private def closeRecruiterJobs(
      userId: UserId,
      now: Instant,
      session: Option[ClientSession[IO]]
  ): RepositoryIO[Unit] = {
    val jobFilter = MongoFilter.and(
      MongoFilter.eq(MongoFields.RecruiterId, userId.value.toString),
      MongoFilter.eq(MongoFields.Status, JobStatus.Open.toString)
    )
    val jobs = Mongo4catsCollections.documents(database, MongoCollections.Jobs)
    def closeJobs(afterId: Option[String]): RepositoryIO[Unit] = {
      val batchFilter =
        MongoFilter.and(List(Some(jobFilter), afterId.map(id => MongoFilter.gt(MongoFields.Id, id))).flatten*)
      RepositoryIO
        .lift(MongoSessionOperations.findManyById(jobs, session, batchFilter, MaxJobsClosedByAccountDeletion))
        .subflatMap(documents => MongoStoredDocumentDecoding.values(documents.map(MongoHiringCodecs.readVersionedJob)))
        .flatMap {
          case Nil           => RepositoryIO.fromEither(Right(()))
          case versionedJobs =>
            val openJobs = versionedJobs.map(_.value)
            val closedJobs =
              openJobs.map(job => job.copy(status = JobStatus.Closed, closedAt = Some(now), updatedAt = now))
            val closeWrites = versionedJobs.zip(closedJobs).traverse_ { case (observed, closed) =>
              for {
                nextVersion <- RepositoryIO.fromEither(
                  Versioned.nextVersion(observed.version).toRight(RepositoryError.Conflict)
                )
                result <- RepositoryIO.lift(
                  MongoSessionOperations.replaceOne(
                    jobs,
                    session,
                    MongoFilter.and(
                      MongoFilter.eq(MongoFields.Id, observed.value.id.value.toString),
                      MongoFilter.eq(MongoFields.Version, observed.version)
                    ),
                    MongoHiringCodecs.job(closed, nextVersion)
                  )
                )
                _ <- RepositoryIO.fromEither(MongoUserRepository.classifyJobClose(result))
              } yield ()
            }
            val closeEvents = closedJobs.traverse { closed =>
              OperationalEvents.jobEvent(
                OperationalEventType.JOB_CLOSED,
                java.util.UUID.nameUUIDFromBytes(
                  s"job:${closed.id.value}:JOB_CLOSED:${closed.updatedAt.toEpochMilli}"
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8)
                ),
                closed,
                userId,
                now
              )
            }
            for {
              validEvents <- RepositoryIO.fromEither(closeEvents.leftMap(_ => RepositoryError.InvalidEvent))
              _ <- closeWrites
              _ <- insertOperationalEvents(outbox, session, validEvents, now, diagnostics)
              _ <- closeJobs(Some(openJobs.last.id.value.toString))
            } yield ()
        }
    }
    closeJobs(None)
  }
}

object MongoUserRepository {
  private[mongo] def classifyJobClose(result: Option[UpdateResult]): Either[RepositoryError, Unit] =
    MongoRepositorySupport.matchedOne(result)

  def transactional(
      database: MongoDatabase[IO],
      client: MongoClient[IO],
      embeddingWork: MongoEmbeddingWorkEnqueuer,
      diagnostics: Diagnostics
  ): MongoUserRepository =
    new MongoUserRepository(
      database,
      MongoTransactionRunner.sessions(client, RepositoryError.Conflict, diagnostics = diagnostics),
      embeddingWork,
      diagnostics
    )
}
