package com.example.graphQL.cats.repository.mongo

import cats.data.EitherT
import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.repository.protocol.*
import com.example.graphQL.cats.shared.events.{OperationalEventType, OperationalEvents}
import com.mongodb.client.model.{Filters, Sorts, Updates}
import com.mongodb.client.result.UpdateResult
import com.mongodb.reactivestreams.client.{ClientSession, MongoClient, MongoCollection, MongoDatabase}
import org.bson.Document
import org.bson.conversions.Bson

import java.time.Instant
import java.util.Date

final class MongoUserRepository(
    database: MongoDatabase,
    transactionRunner: MongoTransactionRunner = MongoTransactionRunner.noTransaction,
    embeddingWork: Option[MongoEmbeddingWorkRepository] = None
) extends UserRepository
    with UserAccountRepository
    with MongoConflictWriteMapping
    with MongoOperationalEventInsertion {
  private val collection = database.getCollection("users")
  private val registry = database.getCollection("account_registry")
  private val outbox = database.getCollection("event_outbox")
  private val MaxJobsClosedByAccountDeletion = 1000

  def insert(user: User): IO[Either[RepositoryError, Unit]] =
    if (!user.roleProfileIsValid) IO.pure(Left(RepositoryError.Conflict))
    else PublisherBridge.first(collection.insertOne(MongoHiringCodecs.user(user))).as(Right(())).handleError(mapWrite)

  override def find(id: UserId): IO[Either[RepositoryError, Option[User]]] =
    PublisherBridge
      .first(collection.find(Filters.eq("_id", id.value.toString)))
      .map(document => MongoStoredDocumentDecoding.repository(document.traverse(MongoHiringCodecs.readUser)))
      .handleError(_ => Left(RepositoryError.Unavailable))

  override def findVersioned(id: UserId): IO[Either[RepositoryError, Option[Versioned[User]]]] =
    PublisherBridge
      .first(collection.find(Filters.eq("_id", id.value.toString)))
      .map(document => MongoStoredDocumentDecoding.repository(document.traverse(MongoHiringCodecs.readVersionedUser)))
      .handleError(_ => Left(RepositoryError.Unavailable))

  private def findWithSession(id: UserId, session: Option[ClientSession]): IO[Either[RepositoryError, Option[User]]] =
    findOne(session, collection, Filters.eq("_id", id.value.toString))
      .map(document => MongoStoredDocumentDecoding.repository(document.traverse(MongoHiringCodecs.readUser)))
      .handleError(_ => Left(RepositoryError.Unavailable))

  override def findMany(ids: List[UserId]): IO[Either[RepositoryError, List[User]]] =
    MongoKeysetPaging.byId(collection, ids.map(_.value.toString))(MongoHiringCodecs.readUser)

  override def updateEmbedding(id: UserId, embedding: EntityEmbedding): IO[Either[RepositoryError, Unit]] =
    findVersioned(id).flatMap {
      case Right(Some(user)) => updateEmbedding(user, embedding)
      case Right(None)       => IO.pure(Left(RepositoryError.Conflict))
      case Left(error)       => IO.pure(Left(error))
    }

  override def updateEmbedding(
      observed: Versioned[User],
      embedding: EntityEmbedding
  ): IO[Either[RepositoryError, Unit]] = {
    val encoded = MongoHiringCodecs.embeddingDocument(embedding)
    PublisherBridge
      .first(
        collection.updateOne(
          MongoObservedStateFilters.candidateEmbedding(observed),
          Updates.combine(
            Updates.set("embedding", encoded.get("embedding")),
            Updates.set("embeddingMeta", encoded.get("embeddingMeta")),
            Updates.inc("version", 1L)
          )
        )
      )
      .map {
        case Some(result) if result.getMatchedCount == 1L => Right(())
        case Some(_)                                      => Left(RepositoryError.Conflict)
        case None                                         => Left(RepositoryError.Unavailable)
      }
      .handleError(mapWrite)
  }

  override def initialized: IO[Either[RepositoryError, Boolean]] =
    PublisherBridge
      .first(registry.find(Filters.eq("_id", "user-account-registry")))
      .map(document => Right(document.exists(_.getString("state", "") == "Initialized")))
      .handleError(_ => Left(RepositoryError.Unavailable))

  override def bootstrap(
      user: User,
      passwordHash: String,
      context: MutationWriteContext
  ): IO[Either[RepositoryError, Unit]] =
    if (!user.roleProfileIsValid || user.role != UserRole.Admin || !user.adminSingleton)
      IO.pure(Left(RepositoryError.Conflict))
    else
      MongoMutationWriteContext
        .run(context, transactionRunner, transactionRequired = true) { session =>
          bootstrapWithSession(user, passwordHash, session)
        }
        .handleError(mapWrite)

  private def bootstrapWithSession(
      user: User,
      passwordHash: String,
      session: Option[ClientSession]
  ): IO[Either[RepositoryError, Unit]] =
    val stateFilter = Filters.and(Filters.eq("_id", "user-account-registry"), Filters.eq("state", "Uninitialized"))
    val findRegistry = findOne(session, registry, stateFilter)
    val findAnyUser = findOne(session, collection, new Document())
    findRegistry.flatMap {
      case None    => IO.pure(Left(RepositoryError.Conflict))
      case Some(_) =>
        findAnyUser.flatMap {
          case Some(_) => IO.pure(Left(RepositoryError.Conflict))
          case None    =>
            insertOne(session, collection, MongoHiringCodecs.userWithPassword(user, passwordHash)) *>
              updateOne(
                session,
                registry,
                stateFilter,
                Updates.combine(Updates.set("state", "Initialized"), Updates.set("adminId", user.id.value.toString))
              ).void.as(Right(()))
        }
    }

  private def writeAccountSession(
      user: User,
      passwordHash: String,
      now: Instant,
      session: Option[ClientSession]
  ): IO[Either[RepositoryError, Unit]] =
    if (!user.roleProfileIsValid || user.role == UserRole.Admin) IO.pure(Left(RepositoryError.Conflict))
    else
      {
        val stateFilter = Filters.and(Filters.eq("_id", "user-account-registry"), Filters.eq("state", "Initialized"))
        findOne(session, registry, stateFilter).flatMap {
          case None    => IO.pure(Left(RepositoryError.Conflict))
          case Some(_) =>
            insertOne(session, collection, MongoHiringCodecs.userWithPassword(user, passwordHash)).flatMap {
              case None    => IO.pure(Left(RepositoryError.Unavailable))
              case Some(_) =>
                embeddingWork
                  .filter(_ => user.role == UserRole.Candidate)
                  .fold(
                    IO.pure(Right(()): Either[RepositoryError, Unit])
                  )(
                    _.enqueue(
                      session,
                      EmbeddingWorkKey(EmbeddingWorkKind.CandidateProfile, user.id.value.toString),
                      now
                    )
                  )
            }
        }
      }.handleError(mapWrite)

  override def createAccount(
      user: User,
      passwordHash: String,
      now: Instant,
      context: MutationWriteContext
  ): IO[Either[RepositoryError, Unit]] =
    MongoMutationWriteContext
      .run(
        context,
        transactionRunner,
        transactionRequired = embeddingWork.nonEmpty && user.role == UserRole.Candidate
      )(session => writeAccountSession(user, passwordHash, now, session))
      .handleError(mapWrite)

  override def findByCanonicalName(nameCanonical: String): IO[Either[RepositoryError, Option[AccountCredentials]]] =
    PublisherBridge
      .first(collection.find(Filters.eq("nameCanonical", nameCanonical)))
      .map { document =>
        MongoStoredDocumentDecoding.repository(document.traverse(MongoHiringCodecs.readCredentials)).map(_.flatten)
      }
      .handleError(_ => Left(RepositoryError.Unavailable))

  override def updateProfile(
      userId: UserId,
      profile: UserProfile,
      now: Instant,
      context: MutationWriteContext
  ): IO[Either[RepositoryError, User]] =
    profile match {
      case _: UserProfile.Candidate if embeddingWork.nonEmpty =>
        MongoMutationWriteContext
          .run(context, transactionRunner, transactionRequired = true) { session =>
            updateCandidateProfileWithEmbeddingWork(userId, profile, now, session).flatMap {
              case Right(()) => findWithSession(userId, session).map(_.flatMap(_.toRight(RepositoryError.Unavailable)))
              case Left(error) => IO.pure(Left(error))
            }
          }
          .handleError(mapWrite)
      case _ =>
        MongoMutationWriteContext
          .run(context, transactionRunner, transactionRequired = false) { session =>
            updateProfileDirect(userId, profile, now, session)
          }
          .handleError(mapWrite)
    }

  private def updateProfileDirect(
      userId: UserId,
      profile: UserProfile,
      now: Instant,
      session: Option[ClientSession]
  ): IO[Either[RepositoryError, User]] =
    updateOne(
      session,
      collection,
      Filters.and(
        Filters.eq("_id", userId.value.toString),
        Filters.eq("accountStatus", AccountStatus.Active.toString),
        Filters.lt("version", Long.MaxValue)
      ),
      Updates.combine(
        Updates.set("profile", MongoHiringCodecs.profile(profile)),
        Updates.set("updatedAt", Date.from(now)),
        Updates.inc("version", 1L)
      )
    ).flatMap {
      case Some(result) if result.getMatchedCount == 1L =>
        findWithSession(userId, session).map(_.flatMap(_.toRight(RepositoryError.Unavailable)))
      case Some(_) => IO.pure(Left(RepositoryError.Conflict))
      case None    => IO.pure(Left(RepositoryError.Unavailable))
    }.handleError(mapWrite)

  private def updateCandidateProfileWithEmbeddingWork(
      userId: UserId,
      profile: UserProfile,
      now: Instant,
      session: Option[ClientSession]
  ): IO[Either[RepositoryError, Unit]] =
    embeddingWork.fold(IO.pure(Left(RepositoryError.Unavailable): Either[RepositoryError, Unit])) { work =>
      {
        val filter = Filters.and(
          Filters.eq("_id", userId.value.toString),
          Filters.eq("role", UserRole.Candidate.toString),
          Filters.eq("accountStatus", AccountStatus.Active.toString),
          Filters.lt("version", Long.MaxValue)
        )
        val update = Updates.combine(
          Updates.set("profile", MongoHiringCodecs.profile(profile)),
          Updates.set("updatedAt", Date.from(now)),
          Updates.inc("version", 1L)
        )
        updateOne(session, collection, filter, update).flatMap {
          case Some(result) if result.getMatchedCount == 1L =>
            work.enqueue(session, EmbeddingWorkKey(EmbeddingWorkKind.CandidateProfile, userId.value.toString), now)
          case Some(_) => IO.pure(Left(RepositoryError.Conflict))
          case None    => IO.pure(Left(RepositoryError.Unavailable))
        }
      }.handleError(mapWrite)
    }

  override def listAccounts(page: UserPageRequest): IO[Either[RepositoryError, List[User]]] = {
    val filters = List(
      Some(Filters.eq("accountStatus", page.status.toString)),
      page.role.map(role => Filters.eq("role", role.toString)),
      page.cursor.map(cursor => MongoKeysetPaging.beforeCursor("createdAt", cursor.createdAt, cursor.id.value.toString))
    ).flatten
    val filter = Filters.and(filters*)
    PublisherBridge
      .collectWithin(
        collection
          .find(filter)
          .sort(Sorts.orderBy(Sorts.descending("createdAt"), Sorts.descending("_id")))
          .limit(page.pageSize.value),
        page.pageSize.value
      )
      .map(documents => MongoStoredDocumentDecoding.values(documents.map(MongoHiringCodecs.readUser)))
      .handleError(_ => Left(RepositoryError.Unavailable))
  }

  override def deleteAccount(
      userId: UserId,
      now: Instant,
      tombstone: String,
      context: MutationWriteContext
  ): IO[Either[RepositoryError, Unit]] =
    MongoMutationWriteContext
      .run(context, transactionRunner, transactionRequired = true) { session =>
        deleteAccountWithSession(userId, now, tombstone, session)
      }
      .handleError(mapWrite)

  private def deleteAccountWithSession(
      userId: UserId,
      now: Instant,
      tombstone: String,
      session: Option[ClientSession]
  ): IO[Either[RepositoryError, Unit]] = {
    val userFilter =
      Filters.and(
        Filters.eq("_id", userId.value.toString),
        Filters.eq("accountStatus", AccountStatus.Active.toString),
        Filters.lt("version", Long.MaxValue)
      )
    val userUpdate = Updates.combine(
      Updates.set("accountStatus", AccountStatus.Deleted.toString),
      Updates.set("deletedAt", Date.from(now)),
      Updates.set("name", tombstone),
      Updates.set("nameCanonical", AccountName.canonical(tombstone)),
      Updates.unset("passwordHash"),
      Updates.unset("profile"),
      Updates.unset("recruiterProfile"),
      Updates.unset("email"),
      Updates.unset("emailCanonical"),
      Updates.inc("version", 1L)
    )
    val userWrite = updateOne(session, collection, userFilter, userUpdate)
    userWrite.flatMap {
      case Some(result) if result.getMatchedCount == 1L =>
        val jobFilter =
          Filters.and(Filters.eq("recruiterId", userId.value.toString), Filters.eq("status", JobStatus.Open.toString))
        val jobs = database.getCollection("jobs")
        def closeJobs(afterId: Option[String]): IO[Either[RepositoryError, Unit]] = {
          val batchFilter = Filters.and(
            List(
              Some(jobFilter),
              afterId.map(id => Filters.gt("_id", id))
            ).flatten*
          )
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
                        replaceOne(
                          session,
                          jobs,
                          MongoObservedStateFilters.jobReplacement(observed),
                          MongoHiringCodecs.job(closed, nextVersion)
                        ).map(MongoUserRepository.classifyJobClose)
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
                  _ <- EitherT(insertOperationalEvents(outbox, session, closeEvents, now))
                  _ <- EitherT(closeJobs(Some(openJobs.last.id.value.toString)))
                } yield ()).value
            }
          }
        }
        closeJobs(None)
      case Some(_) => IO.pure(Left(RepositoryError.Conflict))
      case None    => IO.pure(Left(RepositoryError.Unavailable))
    }
  }

  private def findOne(
      session: Option[ClientSession],
      target: MongoCollection[Document],
      filter: Bson
  ): IO[Option[Document]] =
    session.fold(PublisherBridge.first(target.find(filter)))(active =>
      PublisherBridge.first(target.find(active, filter))
    )

  private def insertOne(
      session: Option[ClientSession],
      target: MongoCollection[Document],
      document: Document
  ) =
    session.fold(PublisherBridge.first(target.insertOne(document)))(active =>
      PublisherBridge.first(target.insertOne(active, document))
    )

  private def updateOne(
      session: Option[ClientSession],
      target: MongoCollection[Document],
      filter: Bson,
      update: Bson
  ) =
    session.fold(PublisherBridge.first(target.updateOne(filter, update)))(active =>
      PublisherBridge.first(target.updateOne(active, filter, update))
    )

  private def replaceOne(
      session: Option[ClientSession],
      target: MongoCollection[Document],
      filter: Bson,
      document: Document
  ) =
    session.fold(PublisherBridge.first(target.replaceOne(filter, document)))(active =>
      PublisherBridge.first(target.replaceOne(active, filter, document))
    )

  private def findManyById(
      session: Option[ClientSession],
      target: MongoCollection[Document],
      filter: Bson,
      limit: Int
  ): IO[List[Document]] =
    session.fold(
      PublisherBridge.collectWithin(target.find(filter).sort(Sorts.ascending("_id")).limit(limit), limit)
    )(active =>
      PublisherBridge.collectWithin(target.find(active, filter).sort(Sorts.ascending("_id")).limit(limit), limit)
    )
}

object MongoUserRepository {
  private[mongo] def classifyJobClose(result: Option[UpdateResult]): Either[RepositoryError, Unit] =
    result match {
      case Some(value) if value.getMatchedCount == 1L => Right(())
      case Some(_)                                    => Left(RepositoryError.Conflict)
      case None                                       => Left(RepositoryError.Unavailable)
    }

  def standalone(database: MongoDatabase): MongoUserRepository =
    new MongoUserRepository(database)

  def transactional(
      database: MongoDatabase,
      client: MongoClient,
      embeddingWork: Option[MongoEmbeddingWorkRepository] = None
  ): MongoUserRepository =
    new MongoUserRepository(database, MongoTransactionRunner.sessions(client, RepositoryError.Conflict), embeddingWork)
}
