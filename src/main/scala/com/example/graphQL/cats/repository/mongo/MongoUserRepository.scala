package com.example.graphQL.cats.repository.mongo

import cats.data.EitherT
import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.events.{OperationalEventType, OperationalEvents}
import com.example.graphQL.cats.service.Diagnostics
import com.mongodb.client.model.{Filters, Sorts, UpdateOptions}
import com.mongodb.client.result.UpdateResult
import mongo4cats.client.{ClientSession, MongoClient}
import mongo4cats.collection.MongoCollection
import mongo4cats.database.MongoDatabase
import org.bson.Document

import java.time.Instant
import scala.util.chaining.*
import java.util.Date

final class MongoUserRepository(
    database: MongoDatabase[IO],
    transactionRunner: MongoTransactionRunner,
    embeddingWork: MongoEmbeddingWorkEnqueuer,
    diagnostics: Diagnostics = Diagnostics.noop
) extends UserRepository
    with UserAccountRepository
    with MongoConflictWriteMapping
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
          MongoSessionOperations
            .insertOne(collection, None, MongoHiringCodecs.user(user))
            .map(MongoRepositorySupport.writeResult(_).void)
        )(mapWrite)

  override def find(id: UserId): RepositoryIO[Option[User]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "MongoUserRepository.find")(
        collection
          .flatMap(_.find(Filters.eq(MongoFields.Id, id.value.toString)).first)
          .map(document => MongoStoredDocumentDecoding.repository(document.traverse(MongoHiringCodecs.readUser)))
      )(_ => Left(RepositoryError.Unavailable))

  override def findVersioned(id: UserId): RepositoryIO[Option[Versioned[User]]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "MongoUserRepository.findVersioned")(
        collection
          .flatMap(_.find(Filters.eq(MongoFields.Id, id.value.toString)).first)
          .map(document =>
            MongoStoredDocumentDecoding.repository(document.traverse(MongoHiringCodecs.readVersionedUser))
          )
      )(_ => Left(RepositoryError.Unavailable))

  private def findWithSession(
      id: UserId,
      session: Option[ClientSession[IO]]
  ): IO[Either[RepositoryError, Option[User]]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "MongoUserRepository.findWithSession")(
        MongoSessionOperations
          .findOne(collection, session, MongoFilter.eq(MongoFields.Id, id.value.toString))
          .map(document => MongoStoredDocumentDecoding.repository(document.traverse(MongoHiringCodecs.readUser)))
      )(_ => Left(RepositoryError.Unavailable))
      .value

  override def findMany(ids: List[UserId]): RepositoryIO[List[User]] =
    MongoKeysetPaging.byId(collection, ids.map(_.value.toString))(MongoHiringCodecs.readUser)(diagnostics)

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
    MongoSessionOperations
      .updateOne(
        collection,
        None,
        MongoFilter.and(
          MongoFilter.eq(MongoFields.Id, observed.value.id.value.toString),
          MongoFilter.eq(MongoFields.Version, observed.version),
          MongoFilter.lt(MongoFields.Version, Long.MaxValue)
        ),
        MongoUpdate.combine(
          MongoUpdate.set(MongoFields.Embedding, encoded.get(MongoFields.Embedding)),
          MongoUpdate.set(MongoFields.EmbeddingMeta, encoded.get(MongoFields.EmbeddingMeta)),
          MongoUpdate.inc(MongoFields.Version, 1L)
        )
      )
      .map {
        case Some(result) if result.getMatchedCount == 1L => Right(())
        case Some(_)                                      => Left(RepositoryError.Conflict)
        case None                                         => Left(RepositoryError.MissingWriteResult)
      }
      .pipe(effect =>
        MongoRepositorySupport
          .repositoryGuard(diagnostics, "MongoUserRepository.updateEmbedding")(effect)(mapWrite)
      )
  }

  override def initialized: RepositoryIO[Boolean] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "MongoUserRepository.initialized")(
        registry
          .flatMap(_.find(Filters.eq(MongoFields.Id, "user-account-registry")).first)
          .map(document => Right(document.exists(_.getString(MongoFields.State, "") == "Initialized")))
      )(_ => Left(RepositoryError.Unavailable))

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
          MongoRepositorySupport.repositoryGuard(diagnostics, "MongoUserRepository.write")(effect)(mapWrite)
        )

  private def bootstrapWithSession(
      user: User,
      passwordHash: PasswordHash,
      session: Option[ClientSession[IO]]
  ): IO[Either[RepositoryError, Unit]] =
    val stateFilter = MongoFilter.and(
      MongoFilter.eq(MongoFields.Id, "user-account-registry"),
      MongoFilter.eq(MongoFields.State, "Uninitialized")
    )
    val findRegistry = MongoSessionOperations.findOne(registry, session, stateFilter)
    val findAnyUser = MongoSessionOperations.findOne(collection, session, MongoFilter.and())
    findRegistry.flatMap {
      case None    => IO.pure(Left(RepositoryError.Conflict))
      case Some(_) =>
        findAnyUser.flatMap {
          case Some(_) => IO.pure(Left(RepositoryError.Conflict))
          case None    =>
            MongoSessionOperations
              .insertOne(collection, session, MongoHiringCodecs.userWithPassword(user, passwordHash))
              .flatMap {
                case None    => IO.pure(Left(RepositoryError.MissingWriteResult))
                case Some(_) =>
                  MongoSessionOperations
                    .updateOne(
                      registry,
                      session,
                      stateFilter,
                      MongoUpdate.combine(
                        MongoUpdate.set(MongoFields.State, "Initialized"),
                        MongoUpdate.set(MongoFields.AdminId, user.id.value.toString)
                      )
                    )
                    .map {
                      case Some(_) => Right(())
                      case None    => Left(RepositoryError.MissingWriteResult)
                    }
              }
        }
    }

  private def writeAccountSession(
      user: User,
      passwordHash: PasswordHash,
      now: Instant,
      session: Option[ClientSession[IO]]
  ): IO[Either[RepositoryError, Unit]] =
    if (!user.roleProfileIsValid || user.role == UserRole.Admin) IO.pure(Left(RepositoryError.Conflict))
    else
      {
        val stateFilter = MongoFilter.and(
          MongoFilter.eq(MongoFields.Id, "user-account-registry"),
          MongoFilter.eq(MongoFields.State, "Initialized")
        )
        MongoSessionOperations.findOne(registry, session, stateFilter).flatMap {
          case None    => IO.pure(Left(RepositoryError.Conflict))
          case Some(_) =>
            MongoSessionOperations
              .insertOne(collection, session, MongoHiringCodecs.userWithPassword(user, passwordHash))
              .flatMap {
                case None    => IO.pure(Left(RepositoryError.MissingWriteResult))
                case Some(_) =>
                  if (user.role == UserRole.Candidate)
                    embeddingWork.enqueue(
                      session,
                      EmbeddingWorkKey(EmbeddingWorkKind.CandidateProfile, user.id.value.toString),
                      now
                    )
                  else IO.pure(Right(()))
              }
        }
      }.pipe(effect =>
        MongoRepositorySupport.repositoryGuard(diagnostics, "MongoUserRepository.write")(effect)(mapWrite).value
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
          MongoRepositorySupport.repositoryGuard(diagnostics, "MongoUserRepository.write")(effect)(mapWrite)
      )

  override def findByCanonicalName(nameCanonical: String): RepositoryIO[Option[AccountCredentials]] =
    collection
      .flatMap(_.find(Filters.eq(MongoFields.NameCanonical, nameCanonical)).first)
      .map { document =>
        MongoStoredDocumentDecoding.repository(document.traverse(MongoHiringCodecs.readCredentials)).map(_.flatten)
      }
      .pipe(effect =>
        MongoRepositorySupport
          .repositoryGuard(diagnostics, "MongoUserRepository.read")(effect)(_ => Left(RepositoryError.Unavailable))
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
            updateCandidateProfileWithEmbeddingWork(userId, profile, now, session).flatMap {
              case Right(()) => findWithSession(userId, session).map(_.flatMap(_.toRight(RepositoryError.MissingStoredResult)))
              case Left(error) => IO.pure(Left(error))
            }
          }
          .pipe(effect =>
            MongoRepositorySupport.repositoryGuard(diagnostics, "MongoUserRepository.write")(effect)(mapWrite)
          )
      case _ =>
        MongoMutationWriteContext
          .run(context, transactionRunner, transactionRequired = false) { session =>
            updateProfileDirect(userId, profile, now, session)
          }
          .pipe(effect =>
            MongoRepositorySupport.repositoryGuard(diagnostics, "MongoUserRepository.write")(effect)(mapWrite)
          )
    }

  private def updateProfileDirect(
      userId: UserId,
      profile: UserProfile,
      now: Instant,
      session: Option[ClientSession[IO]]
  ): IO[Either[RepositoryError, User]] =
    MongoSessionOperations
      .updateOne(
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
      .flatMap {
        case Some(result) if result.getMatchedCount == 1L =>
          findWithSession(userId, session).map(_.flatMap(_.toRight(RepositoryError.MissingStoredResult)))
        case Some(_) => IO.pure(Left(RepositoryError.Conflict))
        case None    => IO.pure(Left(RepositoryError.MissingWriteResult))
      }
      .pipe(effect =>
        MongoRepositorySupport.repositoryGuard(diagnostics, "MongoUserRepository.write")(effect)(mapWrite).value
      )

  private def updateCandidateProfileWithEmbeddingWork(
      userId: UserId,
      profile: UserProfile,
      now: Instant,
      session: Option[ClientSession[IO]]
  ): IO[Either[RepositoryError, Unit]] = {
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
    MongoSessionOperations
      .updateOne(collection, session, filter, update)
      .flatMap {
        case Some(result) if result.getMatchedCount == 1L =>
          embeddingWork.enqueue(
            session,
            EmbeddingWorkKey(EmbeddingWorkKind.CandidateProfile, userId.value.toString),
            now
          )
        case Some(_) => IO.pure(Left(RepositoryError.Conflict))
        case None    => IO.pure(Left(RepositoryError.MissingWriteResult))
      }
      .pipe(effect =>
        MongoRepositorySupport.repositoryGuard(diagnostics, "MongoUserRepository.write")(effect)(mapWrite).value
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
        MongoRepositorySupport.repositoryGuard(diagnostics, "MongoUserRepository.write")(effect)(mapWrite)
      )

  private def deleteAccountWithSession(
      userId: UserId,
      now: Instant,
      tombstone: String,
      session: Option[ClientSession[IO]]
  ): IO[Either[RepositoryError, Unit]] = {
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
      MongoUpdate.unset(MongoFields.RecruiterProfile),
      MongoUpdate.unset(MongoFields.Email),
      MongoUpdate.unset(MongoFields.EmailCanonical),
      MongoUpdate.inc(MongoFields.Version, 1L)
    )
    val userWrite = MongoSessionOperations.updateOne(collection, session, userFilter, userUpdate)
    userWrite.flatMap {
      case Some(result) if result.getMatchedCount == 1L =>
        markSubjectFenceDeleted(session, userId, now).flatMap {
          case Left(error) => IO.pure(Left(error))
          case Right(())   => closeRecruiterJobs(userId, now, session)
        }
      case Some(_) => IO.pure(Left(RepositoryError.Conflict))
      case None    => IO.pure(Left(RepositoryError.MissingWriteResult))
    }
  }

  private def markSubjectFenceDeleted(
      session: Option[ClientSession[IO]],
      userId: UserId,
      now: Instant
  ): IO[Either[RepositoryError, Unit]] = {
    val filter = MongoFilter.eq(MongoFields.Id, userId.value.toString)
    val update = MongoUpdate.combine(
      MongoUpdate.setOnInsert(MongoFields.Id, userId.value.toString),
      MongoUpdate.set(MongoFields.Deleted, true),
      MongoUpdate.set(MongoFields.DeletedAt, Date.from(now))
    )
    val operation =
      MongoSessionOperations.updateOne(subjectFences, session, filter, update, new UpdateOptions().upsert(true))
    operation
      .map {
        case Some(_) => Right(())
        case None    => Left(RepositoryError.MissingWriteResult)
      }
      .pipe(effect =>
        MongoRepositorySupport
          .repositoryGuard(diagnostics, "MongoUserRepository.read")(effect)(_ => Left(RepositoryError.Unavailable))
          .value
      )
  }

  private def closeRecruiterJobs(
      userId: UserId,
      now: Instant,
      session: Option[ClientSession[IO]]
  ): IO[Either[RepositoryError, Unit]] = {
    val jobFilter = MongoFilter.and(
      MongoFilter.eq(MongoFields.RecruiterId, userId.value.toString),
      MongoFilter.eq(MongoFields.Status, JobStatus.Open.toString)
    )
    val jobs = Mongo4catsCollections.documents(database, MongoCollections.Jobs)
    def closeJobs(afterId: Option[String]): IO[Either[RepositoryError, Unit]] = {
      val batchFilter =
        MongoFilter.and(List(Some(jobFilter), afterId.map(id => MongoFilter.gt(MongoFields.Id, id))).flatten*)
      val findJobs = findManyById(session, jobs, batchFilter, MaxJobsClosedByAccountDeletion)
      findJobs.flatMap { documents =>
        MongoStoredDocumentDecoding.values(documents.map(MongoHiringCodecs.readVersionedJob)) match {
          case Left(error)          => IO.pure(Left(error))
          case Right(Nil)           => IO.pure(Right(()))
          case Right(versionedJobs) =>
            val openJobs = versionedJobs.map(_.value)
            val closedJobs =
              openJobs.map(job => job.copy(status = JobStatus.Closed, closedAt = Some(now), updatedAt = now))
            val closeWrites = versionedJobs.zip(closedJobs).traverse_ { case (observed, closed) =>
              Versioned.nextVersion(observed.version) match {
                case None              => EitherT.leftT[IO, Unit](RepositoryError.Conflict)
                case Some(nextVersion) =>
                  EitherT(
                    MongoSessionOperations
                      .replaceOne(
                        jobs,
                        session,
                        MongoFilter.and(
                          MongoFilter.eq(MongoFields.Id, observed.value.id.value.toString),
                          MongoFilter.eq(MongoFields.Version, observed.version)
                        ),
                        MongoHiringCodecs.job(closed, nextVersion)
                      )
                      .map(MongoUserRepository.classifyJobClose)
                  )
              }
            }
            val closeEvents = closedJobs.map { closed =>
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
            (for {
              _ <- closeWrites
              _ <- EitherT(insertOperationalEvents(outbox, session, closeEvents, now, diagnostics))
              _ <- EitherT(closeJobs(Some(openJobs.last.id.value.toString)))
            } yield ()).value
        }
      }
    }
    closeJobs(None)
  }

  private def findManyById(
      session: Option[ClientSession[IO]],
      target: IO[MongoCollection[IO, Document]],
      filter: MongoFilter,
      limit: Int
  ): IO[List[Document]] =
    target.flatMap(collection =>
      session.fold(
        collection
          .find(filter.bson)
          .sort(Sorts.ascending(MongoFields.Id))
          .limit(limit)
          .boundedStream(limit)
          .compile
          .toList
      )(active =>
        collection
          .find(active, filter.sessionFilter)
          .sort(Sorts.ascending(MongoFields.Id))
          .limit(limit)
          .boundedStream(limit)
          .compile
          .toList
      )
    )
}

object MongoUserRepository {
  private[mongo] def classifyJobClose(result: Option[UpdateResult]): Either[RepositoryError, Unit] =
    result match {
      case Some(value) if value.getMatchedCount == 1L => Right(())
      case Some(_)                                    => Left(RepositoryError.Conflict)
      case None                                       => Left(RepositoryError.MissingWriteResult)
    }

  def transactional(
      database: MongoDatabase[IO],
      client: MongoClient[IO],
      embeddingWork: MongoEmbeddingWorkEnqueuer,
      diagnostics: Diagnostics = Diagnostics.noop
  ): MongoUserRepository =
    new MongoUserRepository(
      database,
      MongoTransactionRunner.sessions(client, RepositoryError.Conflict, diagnostics = diagnostics),
      embeddingWork,
      diagnostics
    )
}
