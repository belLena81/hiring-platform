package com.example.graphQL.cats.repository.mongo

import cats.data.{NonEmptyList, ValidatedNel}
import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.repository.protocol.*
import com.example.graphQL.cats.repository.protocol.RepositoryError
import com.example.graphQL.cats.shared.crypto.SourceHash
import com.example.graphQL.cats.shared.events.{OperationalEventEnvelope, OperationalEventType, OperationalEvents}
import com.example.graphQL.cats.shared.pagination.*
import com.example.graphQL.cats.shared.search.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.*
import com.mongodb.{MongoCommandException, MongoException, MongoWriteException}
import com.mongodb.client.model.{Filters, Sorts, Updates}
import com.mongodb.client.result.InsertOneResult
import com.mongodb.reactivestreams.client.{ClientSession, MongoClient, MongoCollection, MongoDatabase}
import org.bson.Document
import org.bson.conversions.Bson
import retry.*
import retry.RetryPolicies.*

import java.time.Instant
import scala.concurrent.duration.*
import java.util.Date
import scala.jdk.CollectionConverters.*

private[mongo] trait MongoTransactionRunner {
  def run[A](operation: Option[ClientSession] => IO[Either[RepositoryError, A]]): IO[Either[RepositoryError, A]]
}

private[mongo] trait MongoConflictWriteMapping {
  protected final def mapWrite[A](error: Throwable): Either[RepositoryError, A] =
    error match {
      case write: MongoWriteException if write.getError.getCode == 11000 => Left(RepositoryError.Conflict)
      case _ => Left(RepositoryError.Unavailable)
    }
}

private[mongo] trait MongoApplicationEventInsertion {
  protected final def insertApplicationEvent(
      events: MongoCollection[Document],
      session: Option[ClientSession],
      event: ApplicationEvent
  ): IO[Option[InsertOneResult]] =
    session.fold(
      PublisherBridge.first(events.insertOne(MongoHiringCodecs.event(event)))
    ) { active =>
      PublisherBridge.first(events.insertOne(active, MongoHiringCodecs.event(event)))
    }
}

private[mongo] trait MongoOperationalEventInsertion {
  protected final def insertOperationalEvents(
      outbox: MongoCollection[Document],
      session: Option[ClientSession],
      events: List[OperationalEventEnvelope],
      now: Instant
  ): IO[Either[RepositoryError, Unit]] =
    events.traverse_ { event =>
      val document = MongoHiringCodecs.outboxRecord(event, now)
      session.fold(PublisherBridge.first(outbox.insertOne(document)))(active =>
        PublisherBridge.first(outbox.insertOne(active, document))
      )
    }.as(Right(())).handleError {
      case write: MongoWriteException if write.getError.getCode == 11000 => Left(RepositoryError.Conflict)
      case _ => Left(RepositoryError.Unavailable)
    }
}

private[mongo] object MongoTransactionRunner {
  final case class RetryPolicy(
      maxTransactionAttempts: Int = 3,
      maxCommitAttempts: Int = 3,
      initialDelay: FiniteDuration = 25.millis,
      maxDelay: FiniteDuration = 250.millis
  )

  enum RetryStage {
    case Operation, Commit
  }

  enum RetryDecision {
    case RetryTransaction, RetryCommit, Fail
  }

  object RetryDecision {
    def decide(stage: RetryStage, labels: Set[String]): RetryDecision =
      stage match {
        case RetryStage.Operation if labels.contains("TransientTransactionError") =>
          RetryTransaction
        case RetryStage.Commit if labels.contains("UnknownTransactionCommitResult") =>
          RetryCommit
        case RetryStage.Commit if labels.contains("TransientTransactionError") =>
          RetryTransaction
        case _ => Fail
      }
  }

  private enum CommitOutcome[+A] {
    case Completed(result: Either[RepositoryError, A])
    case RetryTransaction(error: Throwable)
    case RetryCommit(error: Throwable)
  }

  val noTransaction: MongoTransactionRunner =
    new MongoTransactionRunner {
      override def run[A](operation: Option[ClientSession] => IO[Either[RepositoryError, A]]): IO[Either[RepositoryError, A]] =
        operation(None)
    }

  def sessions(
      client: MongoClient,
      duplicateKeyError: RepositoryError,
      retryPolicy: RetryPolicy = RetryPolicy()
  ): MongoTransactionRunner =
    new MongoTransactionRunner {
      override def run[A](operation: Option[ClientSession] => IO[Either[RepositoryError, A]]): IO[Either[RepositoryError, A]] = {
      def withSession[A](use: ClientSession => IO[A]): IO[A] =
        Resource.make(PublisherBridge.first(client.startSession()).flatMap {
          case Some(session) => IO.pure(session)
          case None => IO.raiseError(new IllegalStateException("Mongo startSession returned no session"))
        })(session => IO.blocking(session.close())).use(use)

      def abort(session: ClientSession): IO[Unit] =
        PublisherBridge.first(session.abortTransaction()).attempt.void

      val transactionBackoff = backoff[CommitOutcome[A]](retryPolicy, retryPolicy.maxTransactionAttempts)
      val commitBackoff = backoff[CommitOutcome[A]](retryPolicy, retryPolicy.maxCommitAttempts)

      def labels(error: MongoException): Set[String] =
        Set("TransientTransactionError", "UnknownTransactionCommitResult").filter(error.hasErrorLabel)

      def commit(session: ClientSession, result: A): IO[CommitOutcome[A]] = {
        val commitAttempt = PublisherBridge.first(session.commitTransaction()).as(CommitOutcome.Completed(Right(result))).handleError {
          case error: MongoException => RetryDecision.decide(RetryStage.Commit, labels(error)) match {
            case RetryDecision.RetryCommit => CommitOutcome.RetryCommit(error)
            case RetryDecision.RetryTransaction => CommitOutcome.RetryTransaction(error)
            case RetryDecision.Fail => CommitOutcome.Completed(mapWrite(error, duplicateKeyError))
          }
          case error => CommitOutcome.Completed(mapWrite(error, duplicateKeyError))
        }

        retryingOnFailures(commitAttempt)(
          policy = commitBackoff,
          valueHandler = (result, _) => IO.pure(result match {
            case CommitOutcome.RetryCommit(_) => HandlerDecision.Continue
            case _ => HandlerDecision.Stop
          })
        ).map(_.fold(identity, identity))
      }

      def attemptOnce: IO[CommitOutcome[A]] = withSession { session =>
        IO.delay(session.startTransaction()) *> operation(Some(session)).attempt.flatMap {
          case Left(error) =>
            error match {
              case mongo: MongoException if RetryDecision.decide(RetryStage.Operation, labels(mongo)) == RetryDecision.RetryTransaction =>
                abort(session).as(CommitOutcome.RetryTransaction(mongo))
              case _ => abort(session).as(CommitOutcome.Completed(mapWrite(error, duplicateKeyError)))
            }
          case Right(Left(error)) =>
            abort(session).as(CommitOutcome.Completed(Left(error)))
          case Right(Right(result)) =>
            commit(session, result).flatMap {
              case retry @ CommitOutcome.RetryTransaction(_) => abort(session).as(retry)
              case CommitOutcome.RetryCommit(error) => abort(session).as(CommitOutcome.Completed(mapWrite(error, duplicateKeyError)))
              case completed @ CommitOutcome.Completed(_) => IO.pure(completed)
            }
        }
      }

      retryingOnFailures(attemptOnce)(
        policy = transactionBackoff,
        valueHandler = (result, _) => IO.pure(result match {
          case CommitOutcome.RetryTransaction(_) => HandlerDecision.Continue
          case _ => HandlerDecision.Stop
        })
      ).map(_.fold(toResult, toResult))
      }
    }

  private def toResult[A](outcome: CommitOutcome[A]): Either[RepositoryError, A] = outcome match {
    case CommitOutcome.Completed(result) => result
    case CommitOutcome.RetryTransaction(_) => Left(RepositoryError.Conflict)
    case CommitOutcome.RetryCommit(_) => Left(RepositoryError.Unavailable)
  }

  private[mongo] def backoff[A](policy: RetryPolicy, maxAttempts: Int): retry.RetryPolicy[IO, A] =
    limitRetries[IO](math.max(maxAttempts - 1, 0)) join
      capDelay(policy.maxDelay, exponentialBackoff[IO](policy.initialDelay))

  private def mapWrite[A](error: Throwable, duplicateKeyError: RepositoryError): Either[RepositoryError, A] =
    error match {
      case write: MongoWriteException if write.getError.getCode == 11000 => Left(duplicateKeyError)
      case mongo: MongoException if isTransientTransactionError(mongo) => Left(RepositoryError.Conflict)
      case _ => Left(RepositoryError.Unavailable)
    }

  private[mongo] def isWriteConflict(error: MongoCommandException): Boolean =
    error.getErrorCode == 112 || error.hasErrorLabel("TransientTransactionError")

  private def isTransientTransactionError(error: Throwable): Boolean = error match {
    case mongo: MongoException => mongo.hasErrorLabel("TransientTransactionError")
    case _ => false
  }

}

private[mongo] object MongoStoredDocumentDecoding {
  def repository[A](decoded: ValidatedNel[MongoHiringCodecs.StoredDocumentError, A]): Either[RepositoryError, A] =
    decoded.toEither.leftMap(_ => RepositoryError.Unavailable)

  def optional[A](decoded: ValidatedNel[MongoHiringCodecs.StoredDocumentError, Option[A]]): Either[RepositoryError, Option[A]] =
    repository(decoded)

  def values[A](decoded: List[ValidatedNel[MongoHiringCodecs.StoredDocumentError, A]]): Either[RepositoryError, List[A]] =
    repository(decoded.sequence)
}

private[mongo] object MongoKeysetPaging {
  def byId[A](collection: MongoCollection[Document], ids: List[String])(read: Document => ValidatedNel[MongoHiringCodecs.StoredDocumentError, A]): IO[Either[RepositoryError, List[A]]] =
    if (ids.isEmpty) IO.pure(Right(Nil))
    else PublisherBridge.collectWithin(collection.find(Filters.in("_id", ids.distinct*)), ids.distinct.size)
      .map(documents => MongoStoredDocumentDecoding.values(documents.map(read)))
      .handleError(_ => Left(RepositoryError.Unavailable))

  def page[A](collection: MongoCollection[Document], filter: Bson, timestampField: String, pageSize: PageSize)(
      read: Document => ValidatedNel[MongoHiringCodecs.StoredDocumentError, A]
  ): IO[Either[RepositoryError, List[A]]] =
    PublisherBridge.collectWithin(
      collection.find(filter)
        .sort(Sorts.orderBy(Sorts.descending(timestampField), Sorts.descending("_id")))
        .limit(pageSize.value),
      pageSize.value
    ).map(documents => MongoStoredDocumentDecoding.values(documents.map(read)))
      .handleError(_ => Left(RepositoryError.Unavailable))

  def filter(filters: List[Option[Bson]]): Bson =
    Filters.and(filters.flatten*)

  def beforeCursor(timestampField: String, occurredAt: Instant, id: String): Bson =
    Filters.or(
      Filters.lt(timestampField, java.util.Date.from(occurredAt)),
      Filters.and(Filters.eq(timestampField, java.util.Date.from(occurredAt)), Filters.lt("_id", id))
    )
}

final class MongoUserRepository(
    database: MongoDatabase,
    transactionRunner: MongoTransactionRunner = MongoTransactionRunner.noTransaction,
    embeddingWork: Option[MongoEmbeddingWorkRepository] = None
) extends UserRepository with UserAccountRepository with MongoConflictWriteMapping with MongoOperationalEventInsertion {
  private val collection = database.getCollection("users")
  private val registry = database.getCollection("account_registry")
  private val outbox = database.getCollection("event_outbox")
  private val MaxJobsClosedByAccountDeletion = 1000

  def insert(user: User): IO[Either[RepositoryError, Unit]] =
    if (!user.roleProfileIsValid) IO.pure(Left(RepositoryError.Conflict))
    else PublisherBridge.first(collection.insertOne(MongoHiringCodecs.user(user))).as(Right(())).handleError(mapWrite)

  override def find(id: UserId): IO[Either[RepositoryError, Option[User]]] =
    PublisherBridge.first(collection.find(Filters.eq("_id", id.value.toString)))
      .map(document => MongoStoredDocumentDecoding.repository(document.traverse(MongoHiringCodecs.readUser)))
      .handleError(_ => Left(RepositoryError.Unavailable))

  override def findMany(ids: List[UserId]): IO[Either[RepositoryError, List[User]]] =
    MongoKeysetPaging.byId(collection, ids.map(_.value.toString))(MongoHiringCodecs.readUser)

  override def updateEmbedding(id: UserId, embedding: EntityEmbedding): IO[Either[RepositoryError, Unit]] = {
    val encoded = MongoHiringCodecs.embeddingDocument(embedding)
    PublisherBridge.first(collection.updateOne(
      Filters.eq("_id", id.value.toString),
      Updates.combine(
        Updates.set("embedding", encoded.get("embedding")),
        Updates.set("embeddingMeta", encoded.get("embeddingMeta"))
      )
    )).map {
      case Some(result) if result.getMatchedCount == 1L => Right(())
      case Some(_) => Left(RepositoryError.Conflict)
      case None => Left(RepositoryError.Unavailable)
    }.handleError(mapWrite)
  }


  override def initialized: IO[Either[RepositoryError, Boolean]] =
    PublisherBridge.first(registry.find(Filters.eq("_id", "user-account-registry")))
      .map(document => Right(document.exists(_.getString("state", "") == "Initialized")))
      .handleError(_ => Left(RepositoryError.Unavailable))

  override def bootstrap(user: User, passwordHash: String): IO[Either[RepositoryError, Unit]] =
    if (!user.roleProfileIsValid || user.role != UserRole.Admin || !user.adminSingleton) IO.pure(Left(RepositoryError.Conflict))
    else transactionRunner.run { session =>
      val stateFilter = Filters.and(Filters.eq("_id", "user-account-registry"), Filters.eq("state", "Uninitialized"))
      val findRegistry = session.fold(PublisherBridge.first(registry.find(stateFilter)))(active => PublisherBridge.first(registry.find(active, stateFilter)))
      val findAnyUser = session.fold(PublisherBridge.first(collection.find().first()))(active => PublisherBridge.first(collection.find(active).first()))
      findRegistry.flatMap {
        case None => IO.pure(Left(RepositoryError.Conflict))
        case Some(_) => findAnyUser.flatMap {
          case Some(_) => IO.pure(Left(RepositoryError.Conflict))
          case None =>
            val insert = session.fold(PublisherBridge.first(collection.insertOne(MongoHiringCodecs.userWithPassword(user, passwordHash))))(active => PublisherBridge.first(collection.insertOne(active, MongoHiringCodecs.userWithPassword(user, passwordHash))))
            insert *> session.fold(
              PublisherBridge.first(registry.updateOne(stateFilter, Updates.combine(Updates.set("state", "Initialized"), Updates.set("adminId", user.id.value.toString)))).void
            )(active => PublisherBridge.first(registry.updateOne(active, stateFilter, Updates.combine(Updates.set("state", "Initialized"), Updates.set("adminId", user.id.value.toString)))).void).as(Right(()))
        }
      }
    }.handleError(mapWrite)

  private def createAccountDirect(user: User, passwordHash: String): IO[Either[RepositoryError, Unit]] =
    if (!user.roleProfileIsValid || user.role == UserRole.Admin) IO.pure(Left(RepositoryError.Conflict))
    else transactionRunner.run { session =>
      val stateFilter = Filters.and(Filters.eq("_id", "user-account-registry"), Filters.eq("state", "Initialized"))
      val registryReady = session.fold(PublisherBridge.first(registry.find(stateFilter)))(active => PublisherBridge.first(registry.find(active, stateFilter)))
      registryReady.flatMap {
        case None => IO.pure(Left(RepositoryError.Conflict))
        case Some(_) =>
          val insert = session match {
            case None => PublisherBridge.first(collection.insertOne(MongoHiringCodecs.userWithPassword(user, passwordHash)))
            case Some(active) => PublisherBridge.first(collection.insertOne(active, MongoHiringCodecs.userWithPassword(user, passwordHash)))
          }
          insert.as(Right(())).handleError(mapWrite)
      }
    }.handleError(mapWrite)

  override def createAccount(user: User, passwordHash: String, now: Instant): IO[Either[RepositoryError, Unit]] =
    if (embeddingWork.nonEmpty && user.role == UserRole.Candidate) createAccountWithEmbeddingWork(user, passwordHash, now)
    else createAccountDirect(user, passwordHash)

  /** Candidate account creation and its embedding work share the same transaction. */
  def createAccountWithEmbeddingWork(
      user: User,
      passwordHash: String,
      now: Instant
  ): IO[Either[RepositoryError, Unit]] =
    embeddingWork.fold(IO.pure(Left(RepositoryError.Unavailable): Either[RepositoryError, Unit])) { work =>
      if (!user.roleProfileIsValid || user.role != UserRole.Candidate) IO.pure(Left(RepositoryError.Conflict))
      else transactionRunner.run { session =>
        val stateFilter = Filters.and(Filters.eq("_id", "user-account-registry"), Filters.eq("state", "Initialized"))
        findOne(session, registry, stateFilter).flatMap {
          case None => IO.pure(Left[RepositoryError, Unit](RepositoryError.Conflict))
          case Some(_) =>
            insertOne(session, collection, MongoHiringCodecs.userWithPassword(user, passwordHash)).flatMap {
              case Some(_) => work.enqueue(session, EmbeddingWorkKey(EmbeddingWorkKind.CandidateProfile, user.id.value.toString), now)
              case None => IO.pure(Left(RepositoryError.Unavailable))
            }
        }
      }.handleError(mapWrite)
    }

  override def findByCanonicalName(nameCanonical: String): IO[Either[RepositoryError, Option[AccountCredentials]]] =
    PublisherBridge.first(collection.find(Filters.eq("nameCanonical", nameCanonical))).map { document =>
      MongoStoredDocumentDecoding.repository(document.traverse(MongoHiringCodecs.readCredentials)).map(_.flatten)
    }.handleError(_ => Left(RepositoryError.Unavailable))

  override def updateProfile(
      userId: UserId,
      profile: UserProfile,
      now: Instant
  ): IO[Either[RepositoryError, User]] =
    profile match {
      case _: UserProfile.Candidate if embeddingWork.nonEmpty => updateCandidateProfileWithEmbeddingWork(userId, profile, now)
      case _ => updateProfileDirect(userId, profile, now)
    }

  private def updateProfileDirect(
      userId: UserId,
      profile: UserProfile,
      now: Instant
  ): IO[Either[RepositoryError, User]] =
    PublisherBridge.first(collection.updateOne(
      Filters.and(Filters.eq("_id", userId.value.toString), Filters.eq("accountStatus", AccountStatus.Active.toString)),
      Updates.combine(
        Updates.set("profile", MongoHiringCodecs.profile(profile)),
        Updates.set("updatedAt", Date.from(now))
      )
    )).flatMap {
      case Some(result) if result.getMatchedCount == 1L => find(userId).map(_.flatMap(_.toRight(RepositoryError.Unavailable)))
      case Some(_) => IO.pure(Left(RepositoryError.Conflict))
      case None => IO.pure(Left(RepositoryError.Unavailable))
    }.handleError(mapWrite)

  /** Candidate profile changes and their reindex request are one durable transaction. */
  def updateCandidateProfileWithEmbeddingWork(
      userId: UserId,
      profile: UserProfile,
      now: Instant
  ): IO[Either[RepositoryError, User]] =
    embeddingWork.fold(IO.pure(Left(RepositoryError.Unavailable): Either[RepositoryError, User])) { work =>
      transactionRunner.run { session =>
        val filter = Filters.and(
          Filters.eq("_id", userId.value.toString),
          Filters.eq("role", UserRole.Candidate.toString),
          Filters.eq("accountStatus", AccountStatus.Active.toString)
        )
        val update = Updates.combine(
          Updates.set("profile", MongoHiringCodecs.profile(profile)),
          Updates.set("updatedAt", Date.from(now))
        )
        updateOne(session, collection, filter, update).flatMap {
          case Some(result) if result.getMatchedCount == 1L =>
            work.enqueue(session, EmbeddingWorkKey(EmbeddingWorkKind.CandidateProfile, userId.value.toString), now)
          case Some(_) => IO.pure(Left(RepositoryError.Conflict))
          case None => IO.pure(Left(RepositoryError.Unavailable))
        }
      }.flatMap {
        case Right(()) => find(userId).map(_.flatMap(_.toRight(RepositoryError.Unavailable)))
        case Left(error) => IO.pure(Left(error))
      }.handleError(mapWrite)
    }

  override def listAccounts(page: UserPageRequest): IO[Either[RepositoryError, List[User]]] = {
    val filters = List(
      Some(Filters.eq("accountStatus", page.status.toString)),
      page.role.map(role => Filters.eq("role", role.toString)),
      page.cursor.map(cursor => MongoKeysetPaging.beforeCursor("createdAt", cursor.createdAt, cursor.id.value.toString))
    ).flatten
    val filter = Filters.and(filters*)
    PublisherBridge.collectWithin(
      collection.find(filter).sort(Sorts.orderBy(Sorts.descending("createdAt"), Sorts.descending("_id"))).limit(page.pageSize.value),
      page.pageSize.value
    ).map(documents => MongoStoredDocumentDecoding.values(documents.map(MongoHiringCodecs.readUser)))
      .handleError(_ => Left(RepositoryError.Unavailable))
  }

  override def deleteAccount(userId: UserId, now: Instant, tombstone: String): IO[Either[RepositoryError, Unit]] =
    transactionRunner.run { session =>
      val userFilter = Filters.and(Filters.eq("_id", userId.value.toString), Filters.eq("accountStatus", AccountStatus.Active.toString))
      val userUpdate = Updates.combine(
        Updates.set("accountStatus", AccountStatus.Deleted.toString),
        Updates.set("deletedAt", Date.from(now)),
        Updates.set("name", tombstone),
        Updates.set("nameCanonical", AccountName.canonical(tombstone)),
        Updates.unset("passwordHash"), Updates.unset("profile"), Updates.unset("recruiterProfile"), Updates.unset("email"), Updates.unset("emailCanonical")
      )
      val userWrite = session.fold(PublisherBridge.first(collection.updateOne(userFilter, userUpdate)))(active => PublisherBridge.first(collection.updateOne(active, userFilter, userUpdate)))
      userWrite.flatMap {
        case Some(result) if result.getMatchedCount == 1L =>
          val jobFilter = Filters.and(Filters.eq("recruiterId", userId.value.toString), Filters.eq("status", JobStatus.Open.toString))
          val jobs = database.getCollection("jobs")
          def closeJobs(afterId: Option[String]): IO[Either[RepositoryError, Unit]] = {
            val batchFilter = Filters.and(List(
              Some(jobFilter),
              afterId.map(id => Filters.gt("_id", id))
            ).flatten*)
            val findJobs = session.fold(
              PublisherBridge.collectWithin(
                jobs.find(batchFilter).sort(Sorts.ascending("_id")).limit(MaxJobsClosedByAccountDeletion),
                MaxJobsClosedByAccountDeletion
              )
            ) { active =>
              PublisherBridge.collectWithin(
                jobs.find(active, batchFilter).sort(Sorts.ascending("_id")).limit(MaxJobsClosedByAccountDeletion),
                MaxJobsClosedByAccountDeletion
              )
            }
            findJobs.flatMap { documents =>
              MongoStoredDocumentDecoding.values(documents.map(MongoHiringCodecs.readJob)) match {
                case Left(error) => IO.pure(Left(error))
                case Right(Nil) => IO.pure(Right(()))
                case Right(openJobs) =>
                  val closedJobs = openJobs.map(job => job.copy(status = JobStatus.Closed, closedAt = Some(now), updatedAt = now))
                  openJobs.zip(closedJobs).traverse_ { case (job, closed) =>
                    val filter = Filters.and(
                      Filters.eq("_id", job.id.value.toString),
                      Filters.eq("status", JobStatus.Open.toString)
                    )
                    session.fold(
                      PublisherBridge.first(jobs.replaceOne(filter, MongoHiringCodecs.job(closed)))
                    )(active => PublisherBridge.first(jobs.replaceOne(active, filter, MongoHiringCodecs.job(closed)))).flatMap {
                      case Some(jobResult) if jobResult.getMatchedCount == 1L => IO.unit
                      case Some(_) => IO.raiseError(new IllegalStateException("job changed while closing account"))
                      case None => IO.raiseError(new IllegalStateException("job close write returned no result"))
                    }
                  }.attempt.flatMap {
                    case Left(_) => IO.pure(Left(RepositoryError.Conflict))
                    case Right(_) =>
                      val closeEvents = closedJobs.map { closed =>
                        OperationalEvents.jobEvent(
                          OperationalEventType.JOB_CLOSED,
                          java.util.UUID.nameUUIDFromBytes(s"job:${closed.id.value}:JOB_CLOSED:${closed.updatedAt.toEpochMilli}".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                          closed,
                          userId,
                          now
                        )
                      }
                      insertOperationalEvents(outbox, session, closeEvents, now).flatMap {
                        case Left(error) => IO.pure(Left(error))
                        case Right(()) => closeJobs(Some(openJobs.last.id.value.toString))
                      }
                  }
              }
            }
          }
          closeJobs(None)
        case Some(_) => IO.pure(Left(RepositoryError.Conflict))
        case None => IO.pure(Left(RepositoryError.Unavailable))
      }
    }.handleError(mapWrite)

  private def findOne(
      session: Option[ClientSession],
      target: MongoCollection[Document],
      filter: Bson
  ): IO[Option[Document]] =
    session.fold(PublisherBridge.first(target.find(filter)))(active => PublisherBridge.first(target.find(active, filter)))

  private def insertOne(
      session: Option[ClientSession],
      target: MongoCollection[Document],
      document: Document
  ) =
    session.fold(PublisherBridge.first(target.insertOne(document)))(active => PublisherBridge.first(target.insertOne(active, document)))

  private def updateOne(
      session: Option[ClientSession],
      target: MongoCollection[Document],
      filter: Bson,
      update: Bson
  ) =
    session.fold(PublisherBridge.first(target.updateOne(filter, update)))(active => PublisherBridge.first(target.updateOne(active, filter, update)))
}

object MongoUserRepository {
  def standalone(database: MongoDatabase): MongoUserRepository =
    new MongoUserRepository(database)

  def transactional(
      database: MongoDatabase,
      client: MongoClient,
      embeddingWork: Option[MongoEmbeddingWorkRepository] = None
  ): MongoUserRepository =
    new MongoUserRepository(database, MongoTransactionRunner.sessions(client, RepositoryError.Conflict), embeddingWork)
}

final class MongoJobRepository(
    database: MongoDatabase,
    transactionRunner: MongoTransactionRunner = MongoTransactionRunner.noTransaction,
    embeddingWork: Option[MongoEmbeddingWorkRepository] = None
) extends JobRepository with MongoConflictWriteMapping with MongoOperationalEventInsertion {
  private val collection = database.getCollection("jobs")
  private val outbox = database.getCollection("event_outbox")

  override def find(id: JobId): IO[Either[RepositoryError, Option[Job]]] =
    PublisherBridge.first(collection.find(Filters.eq("_id", id.value.toString)))
      .map(document => MongoStoredDocumentDecoding.repository(document.traverse(MongoHiringCodecs.readJob)))
      .handleError(_ => Left(RepositoryError.Unavailable))

  override def findMany(ids: List[JobId]): IO[Either[RepositoryError, List[Job]]] =
    MongoKeysetPaging.byId(collection, ids.map(_.value.toString))(MongoHiringCodecs.readJob)

  override def findOpen(filter: JobSearchFilter, page: JobPageRequest): IO[Either[RepositoryError, List[Job]]] =
    findMany(baseSearchFilter(filter, page), page)

  override def findAll(page: JobPageRequest): IO[Either[RepositoryError, List[Job]]] =
    findMany(baseJobFilter(List(page.status.map(status => Filters.eq("status", status.toString))), page), page)

  override def findByRecruiter(recruiterId: UserId, page: JobPageRequest): IO[Either[RepositoryError, List[Job]]] =
    findMany(baseJobFilter(List(Some(Filters.eq("recruiterId", recruiterId.value.toString)), page.status.map(status => Filters.eq("status", status.toString))), page), page)

  private def createDirect(job: Job): IO[Either[RepositoryError, Unit]] =
    PublisherBridge.first(collection.insertOne(MongoHiringCodecs.job(job))).as(Right(())).handleError(mapWrite)

  override def create(job: Job, now: Instant): IO[Either[RepositoryError, Unit]] =
    if (embeddingWork.nonEmpty) createWithEmbeddingWork(job, now) else createDirect(job)

  override def createWithEvents(job: Job, now: Instant, events: List[OperationalEventEnvelope]): IO[Either[RepositoryError, Unit]] =
    if (events.isEmpty) create(job, now)
    else transactionRunner.run { session =>
      insertOne(session, MongoHiringCodecs.job(job)).flatMap {
        case Some(_) =>
          embeddingWork.fold(IO.pure(Right(()): Either[RepositoryError, Unit]))(
            _.enqueue(session, EmbeddingWorkKey(EmbeddingWorkKind.Job, job.id.value.toString), now)
          ).flatMap {
            case Right(()) => insertOperationalEvents(outbox, session, events, now)
            case Left(error) => IO.pure(Left(error))
          }
        case None => IO.pure(Left(RepositoryError.Unavailable))
      }
    }.handleError(mapWrite)

  /** Job creation and the coalesced reindex request commit together. */
  def createWithEmbeddingWork(job: Job, now: Instant): IO[Either[RepositoryError, Unit]] =
    embeddingWork.fold(IO.pure(Left(RepositoryError.Unavailable): Either[RepositoryError, Unit])) { work =>
      transactionRunner.run { session =>
        insertOne(session, MongoHiringCodecs.job(job)).flatMap {
          case Some(_) => work.enqueue(session, EmbeddingWorkKey(EmbeddingWorkKind.Job, job.id.value.toString), now)
          case None => IO.pure(Left(RepositoryError.Unavailable))
        }
      }.handleError(mapWrite)
    }

  private def updateDirect(job: Job): IO[Either[RepositoryError, Job]] = {
    val persisted = job
    PublisherBridge.first(collection.replaceOne(
      Filters.and(Filters.eq("_id", job.id.value.toString), Filters.eq("recruiterId", job.recruiterId.value.toString)),
      MongoHiringCodecs.job(persisted)
    )).map {
      case Some(result) if result.getMatchedCount == 1L => Right(persisted)
      case Some(_) => Left(RepositoryError.Conflict)
      case None => Left(RepositoryError.Unavailable)
    }.handleError(mapWrite)
  }

  override def update(job: Job, now: Instant): IO[Either[RepositoryError, Job]] =
    if (embeddingWork.nonEmpty) updateWithEmbeddingWork(job, now) else updateDirect(job)

  override def updateWithEvents(job: Job, now: Instant, events: List[OperationalEventEnvelope]): IO[Either[RepositoryError, Job]] =
    if (events.isEmpty) update(job, now)
    else {
      val persisted = job
      transactionRunner.run { session =>
        replaceOne(
          session,
          Filters.and(Filters.eq("_id", job.id.value.toString), Filters.eq("recruiterId", job.recruiterId.value.toString)),
          MongoHiringCodecs.job(persisted)
        ).flatMap {
          case Some(result) if result.getMatchedCount == 1L =>
            embeddingWork.fold(IO.pure(Right(()): Either[RepositoryError, Unit]))(
              _.enqueue(session, EmbeddingWorkKey(EmbeddingWorkKind.Job, job.id.value.toString), now)
            ).flatMap {
              case Right(()) => insertOperationalEvents(outbox, session, events, now)
              case Left(error) => IO.pure(Left(error))
            }
          case Some(_) => IO.pure(Left(RepositoryError.Conflict))
          case None => IO.pure(Left(RepositoryError.Unavailable))
        }
      }.map(_.as(persisted)).handleError(mapWrite)
    }

  /** Job replacement and the coalesced reindex request commit together. */
  def updateWithEmbeddingWork(job: Job, now: Instant): IO[Either[RepositoryError, Job]] = {
    val persisted = job
    embeddingWork.fold(IO.pure(Left(RepositoryError.Unavailable): Either[RepositoryError, Job])) { work =>
      transactionRunner.run { session =>
        replaceOne(
          session,
          Filters.and(Filters.eq("_id", job.id.value.toString), Filters.eq("recruiterId", job.recruiterId.value.toString)),
          MongoHiringCodecs.job(persisted)
        ).flatMap {
          case Some(result) if result.getMatchedCount == 1L =>
            work.enqueue(session, EmbeddingWorkKey(EmbeddingWorkKind.Job, job.id.value.toString), now)
          case Some(_) => IO.pure(Left(RepositoryError.Conflict))
          case None => IO.pure(Left(RepositoryError.Unavailable))
        }
      }.map(_.as(persisted)).handleError(mapWrite)
    }
  }

  override def updateEmbedding(id: JobId, embedding: EntityEmbedding): IO[Either[RepositoryError, Unit]] = {
    val encoded = MongoHiringCodecs.embeddingDocument(embedding)
    PublisherBridge.first(collection.updateOne(
      Filters.eq("_id", id.value.toString),
      Updates.combine(
        Updates.set("embedding", encoded.get("embedding")),
        Updates.set("embeddingMeta", encoded.get("embeddingMeta"))
      )
    )).map {
      case Some(result) if result.getMatchedCount == 1L => Right(())
      case Some(_) => Left(RepositoryError.Conflict)
      case None => Left(RepositoryError.Unavailable)
    }.handleError(mapWrite)
  }


  private def findMany(filter: Bson, page: JobPageRequest): IO[Either[RepositoryError, List[Job]]] =
    MongoKeysetPaging.page(collection, filter, "createdAt", page.pageSize)(MongoHiringCodecs.readJob)

  private def baseSearchFilter(filter: JobSearchFilter, page: JobPageRequest): Bson =
    baseJobFilter(List(
      Some(Filters.eq("status", JobStatus.Open.toString)),
      filter.city.map(city => Filters.eq("location.city", city)),
      Option.when(filter.skills.nonEmpty)(Filters.all("skills", filter.skills.toList.sorted*)),
      filter.createdAfter.map(createdAfter => Filters.gte("createdAt", java.util.Date.from(createdAfter)))
    ), page)

  private def baseJobFilter(filters: List[Option[Bson]], page: JobPageRequest): Bson = {
    val cursorFilter = page.cursor.map(cursor =>
      MongoKeysetPaging.beforeCursor("createdAt", cursor.createdAt, cursor.id.value.toString)
    )
    val activeFilters = (filters :+ cursorFilter).flatten
    if (activeFilters.isEmpty) new Document() else Filters.and(activeFilters*)
  }

  private def insertOne(session: Option[ClientSession], document: Document) =
    session.fold(PublisherBridge.first(collection.insertOne(document)))(active => PublisherBridge.first(collection.insertOne(active, document)))

  private def replaceOne(session: Option[ClientSession], filter: Bson, document: Document) =
    session.fold(PublisherBridge.first(collection.replaceOne(filter, document)))(active => PublisherBridge.first(collection.replaceOne(active, filter, document)))
}

object MongoJobRepository {
  def standalone(database: MongoDatabase): MongoJobRepository =
    new MongoJobRepository(database)

  def transactional(
      database: MongoDatabase,
      client: MongoClient,
      embeddingWork: Option[MongoEmbeddingWorkRepository] = None
  ): MongoJobRepository =
    new MongoJobRepository(database, MongoTransactionRunner.sessions(client, RepositoryError.Conflict), embeddingWork)
}

final class MongoSemanticSearchRepository(
    database: MongoDatabase,
    jobVectorIndex: String,
    candidateVectorIndex: String,
    jobLexicalIndex: String,
    numCandidates: Int
) extends SemanticSearchRepository {
  private val jobs = database.getCollection("jobs")
  private val users = database.getCollection("users")

  override def searchJobs(query: VectorSearchQuery): IO[Either[RepositoryError, List[RankedJob]]] =
    rankedJobs(query, query.filter)

  override def recommendedJobs(query: VectorSearchQuery): IO[Either[RepositoryError, List[RankedJob]]] =
    rankedJobs(query, JobSearchFilter(None, Set.empty, None))

  override def candidateMatches(query: VectorSearchQuery): IO[Either[RepositoryError, List[RankedCandidate]]] = {
    val filter = Filters.and(
      Filters.eq("role", UserRole.Candidate.toString),
      Filters.exists("profile"),
      Filters.eq("embeddingMeta.model", query.model)
    )
    val pipeline = List(vectorSearchStage(candidateVectorIndex, query.vector, filter, query.first.value), scoreStage).asJava
    PublisherBridge.collectWithin(users.aggregate(pipeline), query.first.value).map { documents =>
      MongoSemanticSearchResult.rankedCandidates(documents, query).map(_.flatten)
    }.handleError(_ => Left(RepositoryError.Unavailable))
  }

  private def rankedJobs(query: VectorSearchQuery, filter: JobSearchFilter): IO[Either[RepositoryError, List[RankedJob]]] = {
    val mongoFilter = jobFilter(query, filter)
    query.lexicalQuery.filter(_ => query.mode == SearchMode.HYBRID) match {
      case Some(text) =>
        val vector = vectorJobs(query, mongoFilter, numCandidates)
        val lexical = lexicalJobs(query, text, mongoFilter)
        (vector, lexical).parMapN { (vectorResults, lexicalResults) =>
          (vectorResults, lexicalResults) match {
            case (Right(vectorHits), Right(lexicalHits)) =>
              Right(HybridRankFusion.jobs(vectorHits, lexicalHits, query.first.value))
            case _ => Left(RepositoryError.Unavailable)
          }
        }
      case None => vectorJobs(query, mongoFilter, query.first.value)
    }
  }

  private def vectorJobs(query: VectorSearchQuery, filter: Bson, limit: Int): IO[Either[RepositoryError, List[RankedJob]]] = {
    val pipeline = List(vectorSearchStage(jobVectorIndex, query.vector, filter, limit), scoreStage).asJava
    PublisherBridge.collectWithin(jobs.aggregate(pipeline), limit).map { documents =>
      MongoSemanticSearchResult.rankedJobs(documents, query).map(_.flatten)
    }.handleError(_ => Left(RepositoryError.Unavailable))
  }

  private def lexicalJobs(
      query: VectorSearchQuery,
      text: String,
      filter: Bson
  ): IO[Either[RepositoryError, List[RankedJob]]] = {
    val search = new Document("index", jobLexicalIndex)
      .append("text", new Document("query", text)
        .append("path", List("title", "description", "requirements", "skills").asJava))
    val pipeline = List(
      new Document("$search", search),
      new Document("$match", filter),
      new Document("$limit", java.lang.Integer.valueOf(numCandidates)),
      new Document("$set", new Document("score", new Document("$meta", "searchScore")))
    ).asJava
    PublisherBridge.collectWithin(jobs.aggregate(pipeline), numCandidates).map { documents =>
      MongoSemanticSearchResult.rankedJobs(documents, query).map(_.flatten)
    }.handleError(_ => Left(RepositoryError.Unavailable))
  }

  private def jobFilter(query: VectorSearchQuery, filter: JobSearchFilter): Bson =
    MongoKeysetPaging.filter(List(
      Some(Filters.eq("status", JobStatus.Open.toString)),
      Some(Filters.eq("embeddingMeta.model", query.model)),
      filter.city.map(city => Filters.eq("location.city", city)),
      skillsFilter(filter.skills),
      filter.createdAfter.map(createdAfter => Filters.gte("createdAt", java.util.Date.from(createdAfter)))
    ))

  private def skillsFilter(skills: Set[String]): Option[Bson] =
    Option.when(skills.nonEmpty)(Filters.and(skills.toList.sorted.map(skill => Filters.eq("skills", skill))*))

  private def vectorSearchStage(index: String, vector: List[Float], filter: Bson, limit: Int): Document =
    new Document("$vectorSearch", new Document("index", index)
      .append("path", "embedding")
      .append("queryVector", vector.map(float => java.lang.Double.valueOf(float.toDouble)).asJava)
      .append("numCandidates", java.lang.Integer.valueOf(numCandidates))
      .append("limit", java.lang.Integer.valueOf(limit))
      .append("filter", filter))

  private val scoreStage: Document =
    new Document("$set", new Document("score", new Document("$meta", "vectorSearchScore")))
}

private[mongo] object MongoSemanticSearchResult {
  def rankedJobs(documents: List[Document], query: VectorSearchQuery): Either[RepositoryError, List[Option[RankedJob]]] =
    documents.traverse(rankedJob(_, query)).leftMap(_ => RepositoryError.Unavailable)

  def rankedCandidates(documents: List[Document], query: VectorSearchQuery): Either[RepositoryError, List[Option[RankedCandidate]]] =
    documents.traverse(rankedCandidate(_, query)).leftMap(_ => RepositoryError.Unavailable)

  def rankedJob(document: Document, query: VectorSearchQuery): Either[NonEmptyList[MongoHiringCodecs.StoredDocumentError], Option[RankedJob]] =
    val stored = new Document(document)
    stored.remove("score")
    MongoHiringCodecs.readJob(stored).toEither.map { job =>
      for {
      embedding <- job.embedding
      if embedding.meta.model == query.model
      if embedding.meta.sourceHash == SourceHash.sha256(SearchableText.job(job))
      score <- Option(document.get("score")).collect { case value: Number => value }
    } yield RankedJob(job, score.doubleValue, query.mode, embedding.meta, query.searchId)
    }

  def rankedCandidate(document: Document, query: VectorSearchQuery): Either[NonEmptyList[MongoHiringCodecs.StoredDocumentError], Option[RankedCandidate]] =
    val stored = new Document(document)
    stored.remove("score")
    MongoHiringCodecs.readUser(stored).toEither.map { candidate =>
      for {
      profile <- candidate.candidateProfile
      embedding <- candidate.embedding
      if embedding.meta.model == query.model
      if embedding.meta.sourceHash == SourceHash.sha256(SearchableText.candidate(profile))
      score <- Option(document.get("score")).collect { case value: Number => value }
    } yield RankedCandidate(candidate, score.doubleValue, query.mode, embedding.meta, query.searchId)
    }
}

final class MongoApplicationRepository private (
    database: MongoDatabase,
    transactionRunner: MongoTransactionRunner
) extends ApplicationRepository with MongoApplicationEventInsertion with MongoOperationalEventInsertion {
  private val collection = database.getCollection("applications")
  private val events = database.getCollection("application_events")
  private val jobs = database.getCollection("jobs")
  private val outbox = database.getCollection("event_outbox")

  override def find(id: ApplicationId): IO[Either[RepositoryError, Option[Application]]] =
    PublisherBridge.first(collection.find(Filters.eq("_id", id.value.toString)))
      .map(document => MongoStoredDocumentDecoding.repository(document.traverse(MongoHiringCodecs.readApplication)))
      .handleError(_ => Left(RepositoryError.Unavailable))

  override def findByCandidate(candidateId: UserId, page: ApplicationPageRequest): IO[Either[RepositoryError, List[Application]]] =
    findMany(baseFilter("candidateId", candidateId.value.toString, page), page)

  override def findByJob(jobId: JobId, page: ApplicationPageRequest): IO[Either[RepositoryError, List[Application]]] =
    findMany(baseFilter("jobId", jobId.value.toString, page), page)

  override def history(applicationId: ApplicationId, page: ApplicationEventPageRequest): IO[Either[RepositoryError, List[ApplicationEvent]]] =
    MongoKeysetPaging.page(events, eventFilter(applicationId, page), "occurredAt", page.pageSize)(MongoHiringCodecs.readEvent)

  override def createForOpenJob(
      observedJob: Job,
      application: Application,
      initialEvent: ApplicationEvent
  ): IO[Either[RepositoryError, Unit]] =
    if (!isConsistentSubmit(observedJob, application, initialEvent)) IO.pure(Left(RepositoryError.Conflict))
    else submitWithRetry(observedJob, application, initialEvent, remainingRetries = 2)

  override def createForOpenJobWithEvents(
      observedJob: Job,
      application: Application,
      initialEvent: ApplicationEvent,
      operationalEvents: List[OperationalEventEnvelope]
  ): IO[Either[RepositoryError, Unit]] =
    if (!isConsistentSubmit(observedJob, application, initialEvent)) IO.pure(Left(RepositoryError.Conflict))
    else submitWithRetry(observedJob, application, initialEvent, operationalEvents, remainingRetries = 2)

  override def updateStatus(application: Application, event: ApplicationEvent): IO[Either[RepositoryError, Unit]] =
    updateStatusWithEvents(application, event, Nil)

  override def updateStatusWithEvents(
      application: Application,
      event: ApplicationEvent,
      operationalEvents: List[OperationalEventEnvelope]
  ): IO[Either[RepositoryError, Unit]] =
    transactionRunner.run { session =>
      val statusGuard = event.previousStatus.map(status => Filters.eq("status", status.toString)).getOrElse(Filters.exists("status"))
      val filter = Filters.and(Filters.eq("_id", application.id.value.toString), statusGuard)
      val update = session.fold(
        PublisherBridge.first(collection.replaceOne(filter, MongoHiringCodecs.application(application)))
      ) { active =>
        PublisherBridge.first(collection.replaceOne(active, filter, MongoHiringCodecs.application(application)))
      }
      update.flatMap {
        case Some(result) if result.getMatchedCount == 1L =>
          insertApplicationEvent(events, session, event).attempt.flatMap {
            case Right(_) => insertOperationalEvents(outbox, session, operationalEvents, application.updatedAt)
            case Left(error) => IO.pure(mapDuplicateAs(RepositoryError.Conflict)(error))
          }
        case Some(_) => IO.pure(Left(RepositoryError.Conflict))
        case None => IO.pure(Left(RepositoryError.Unavailable))
      }
    }.handleError(mapWrite)

  private def findMany(filter: Bson, page: ApplicationPageRequest): IO[Either[RepositoryError, List[Application]]] =
    MongoKeysetPaging.page(collection, filter, "createdAt", page.pageSize)(MongoHiringCodecs.readApplication)

  private def baseFilter(field: String, id: String, page: ApplicationPageRequest): Bson = {
    val statusFilter = page.status.map(status => Filters.eq("status", status.toString))
    val cursorFilter = page.cursor.map(cursor =>
      MongoKeysetPaging.beforeCursor("createdAt", cursor.createdAt, cursor.id.value.toString)
    )
    MongoKeysetPaging.filter(List(Some(Filters.eq(field, id)), statusFilter, cursorFilter))
  }

  private def eventFilter(applicationId: ApplicationId, page: ApplicationEventPageRequest): Bson = {
    val cursorFilter = page.cursor.map(cursor =>
      MongoKeysetPaging.beforeCursor("occurredAt", cursor.occurredAt, cursor.id.value.toString)
    )
    MongoKeysetPaging.filter(List(Some(Filters.eq("applicationId", applicationId.value.toString)), cursorFilter))
  }

  private def submitWithRetry(
      observedJob: Job,
      application: Application,
      initialEvent: ApplicationEvent,
      remainingRetries: Int
  ): IO[Either[RepositoryError, Unit]] =
    submitWithRetry(observedJob, application, initialEvent, Nil, remainingRetries)

  private def submitWithRetry(
      observedJob: Job,
      application: Application,
      initialEvent: ApplicationEvent,
      operationalEvents: List[OperationalEventEnvelope],
      remainingRetries: Int
  ): IO[Either[RepositoryError, Unit]] =
    submitOnce(observedJob, application, initialEvent, operationalEvents).flatMap {
      case Left(RepositoryError.Conflict) if remainingRetries > 0 =>
        currentOpenJob(observedJob.id).flatMap {
          case Right(Some(current)) => submitWithRetry(current, application, initialEvent, operationalEvents, remainingRetries - 1)
          case Right(None) => IO.pure(Left(RepositoryError.Conflict))
          case Left(error) => IO.pure(Left(error))
        }
      case result => IO.pure(result)
    }

  private def isConsistentSubmit(observedJob: Job, application: Application, initialEvent: ApplicationEvent): Boolean =
    observedJob.id == application.jobId &&
      initialEvent.applicationId == application.id &&
      initialEvent.previousStatus.isEmpty &&
      initialEvent.newStatus == ApplicationStatus.Created &&
      initialEvent.actorId == application.candidateId

  private def submitOnce(
      observedJob: Job,
      application: Application,
      initialEvent: ApplicationEvent,
      operationalEvents: List[OperationalEventEnvelope]
  ): IO[Either[RepositoryError, Unit]] =
    transactionRunner.run { session =>
      val guardFilter = Filters.and(
        Filters.eq("_id", observedJob.id.value.toString),
        Filters.eq("status", JobStatus.Open.toString)
      )
      val guard = session.fold(
        PublisherBridge.first(jobs.updateOne(guardFilter, Updates.set("updatedAt", Date.from(application.createdAt))))
      ) { active =>
        PublisherBridge.first(jobs.updateOne(active, guardFilter, Updates.set("updatedAt", Date.from(application.createdAt))))
      }
      guard.flatMap {
        case Some(result) if result.getMatchedCount == 1L =>
          insertApplicationAndEvent(session, application, initialEvent, operationalEvents)
        case Some(_) => IO.pure(Left(RepositoryError.Conflict))
        case None => IO.pure(Left(RepositoryError.Unavailable))
      }
    }.handleError(mapWrite)

  private def insertApplicationAndEvent(
      session: Option[ClientSession],
      application: Application,
      initialEvent: ApplicationEvent,
      operationalEvents: List[OperationalEventEnvelope]
  ): IO[Either[RepositoryError, Unit]] = {
    val insertApplication = session.fold(
      PublisherBridge.first(collection.insertOne(MongoHiringCodecs.application(application)))
    ) { active =>
      PublisherBridge.first(collection.insertOne(active, MongoHiringCodecs.application(application)))
    }
    val insertEvent = insertApplicationEvent(events, session, initialEvent)
    insertApplication.attempt.flatMap {
      case Left(error) => IO.pure(mapDuplicateAs(RepositoryError.DuplicateApplication)(error))
      case Right(_) =>
        insertEvent.attempt.flatMap {
          case Right(_) => insertOperationalEvents(outbox, session, operationalEvents, application.createdAt)
          case Left(error) => IO.pure(mapDuplicateAs(RepositoryError.Conflict)(error))
        }
    }
  }

  private def currentOpenJob(id: JobId): IO[Either[RepositoryError, Option[Job]]] =
    PublisherBridge.first(jobs.find(Filters.eq("_id", id.value.toString))).map(
      document => MongoStoredDocumentDecoding.repository(document.traverse(MongoHiringCodecs.readJob)).map(_.filter(_.status == JobStatus.Open))
    ).handleError(_ => Left(RepositoryError.Unavailable))

  private def mapWrite(error: Throwable): Either[RepositoryError, Unit] =
    error match {
      case write: MongoWriteException if write.getError.getCode == 11000 => Left(RepositoryError.DuplicateApplication)
      case command: MongoCommandException if MongoTransactionRunner.isWriteConflict(command) => Left(RepositoryError.Conflict)
      case _ => Left(RepositoryError.Unavailable)
    }

  private def mapDuplicateAs(error: RepositoryError)(throwable: Throwable): Either[RepositoryError, Unit] =
    throwable match {
      case write: MongoWriteException if write.getError.getCode == 11000 => Left(error)
      case command: MongoCommandException if MongoTransactionRunner.isWriteConflict(command) => Left(RepositoryError.Conflict)
      case _ => Left(RepositoryError.Unavailable)
    }
}

object MongoApplicationRepository {
  def standalone(database: MongoDatabase): MongoApplicationRepository =
    new MongoApplicationRepository(database, MongoTransactionRunner.noTransaction)

  def transactional(database: MongoDatabase, client: MongoClient): MongoApplicationRepository =
    new MongoApplicationRepository(database, MongoTransactionRunner.sessions(client, RepositoryError.DuplicateApplication))
}
