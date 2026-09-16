package com.example.graphQL.cats.infrastructure.mongo

import cats.effect.{IO, Resource}
import com.example.graphQL.cats.application.port.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.{Application, ApplicationEvent, ApplicationStatus, Job, JobStatus, User, UserRole}
import com.mongodb.{MongoCommandException, MongoWriteException}
import com.mongodb.client.model.{Filters, Sorts, Updates}
import com.mongodb.reactivestreams.client.{ClientSession, MongoClient, MongoDatabase}
import org.bson.Document

private[mongo] trait MongoTransactionRunner {
  def run(operation: Option[ClientSession] => IO[Either[RepositoryError, Unit]]): IO[Either[RepositoryError, Unit]]
}

private[mongo] object MongoTransactionRunner {
  val noTransaction: MongoTransactionRunner =
    operation => operation(None)

  def sessions(client: MongoClient): MongoTransactionRunner =
    operation =>
      Resource.make(PublisherBridge.first(client.startSession()).flatMap {
        case Some(session) => IO.pure(session)
        case None => IO.raiseError(new IllegalStateException("Mongo startSession returned no session"))
      })(session => IO.blocking(session.close())).use { session =>
        IO.delay(session.startTransaction()) *> operation(Some(session)).attempt.flatMap {
          case Left(error) =>
            PublisherBridge.first(session.abortTransaction()).attempt.as(mapWrite(error))
          case Right(Left(error)) =>
            PublisherBridge.first(session.abortTransaction()).attempt.as(Left(error))
          case Right(Right(())) =>
            PublisherBridge.first(session.commitTransaction()).as(Right(())).handleErrorWith { error =>
              PublisherBridge.first(session.abortTransaction()).attempt.as(mapWrite(error))
            }
        }
      }

  private def mapWrite(error: Throwable): Either[RepositoryError, Unit] =
    error match {
      case write: MongoWriteException if write.getError.getCode == 11000 => Left(RepositoryError.DuplicateApplication)
      case command: MongoCommandException if isWriteConflict(command) => Left(RepositoryError.Conflict)
      case _ => Left(RepositoryError.Unavailable)
    }

  private[mongo] def isWriteConflict(error: MongoCommandException): Boolean =
    error.getErrorCode == 112 || error.hasErrorLabel("TransientTransactionError")
}

final class MongoUserRepository(database: MongoDatabase) extends UserRepository[IO] {
  private val collection = database.getCollection("users")

  def insert(user: User): IO[Either[RepositoryError, Unit]] =
    if (user.role == UserRole.Admin && !user.adminSingleton) IO.pure(Left(RepositoryError.Conflict))
    else PublisherBridge.first(collection.insertOne(MongoHiringCodecs.user(user))).as(Right(())).handleError(mapWrite)

  override def find(id: UserId): IO[Option[User]] =
    PublisherBridge.first(collection.find(Filters.eq("_id", id.value.toString))).map(_.map(MongoHiringCodecs.readUser))

  private def mapWrite(error: Throwable): Either[RepositoryError, Unit] =
    error match {
      case write: MongoWriteException if write.getError.getCode == 11000 => Left(RepositoryError.Conflict)
      case _ => Left(RepositoryError.Unavailable)
    }
}

final class MongoJobRepository(database: MongoDatabase) extends JobRepository[IO] {
  private val collection = database.getCollection("jobs")

  override def find(id: JobId): IO[Option[Job]] =
    PublisherBridge.first(collection.find(Filters.eq("_id", id.value.toString))).map(_.map(MongoHiringCodecs.readJob))

  override def create(job: Job): IO[Either[RepositoryError, Unit]] =
    PublisherBridge.first(collection.insertOne(MongoHiringCodecs.job(job))).as(Right(())).handleError(mapWrite)

  override def update(job: Job): IO[Either[RepositoryError, Job]] = {
    val persisted = job.copy(version = job.version + 1L)
    PublisherBridge.first(collection.replaceOne(
      Filters.and(Filters.eq("_id", job.id.value.toString), Filters.eq("version", job.version)),
      MongoHiringCodecs.job(persisted)
    )).map {
      case Some(result) if result.getMatchedCount == 1L => Right(persisted)
      case Some(_) => Left(RepositoryError.Conflict)
      case None => Left(RepositoryError.Unavailable)
    }.handleError(mapWrite)
  }

  private def mapWrite[A](error: Throwable): Either[RepositoryError, A] =
    error match {
      case write: MongoWriteException if write.getError.getCode == 11000 => Left(RepositoryError.Conflict)
      case _ => Left(RepositoryError.Unavailable)
    }
}

final class MongoApplicationRepository private (
    database: MongoDatabase,
    transactionRunner: MongoTransactionRunner
) extends ApplicationRepository[IO] {
  private val collection = database.getCollection("applications")
  private val events = database.getCollection("application_events")
  private val jobs = database.getCollection("jobs")

  override def find(id: ApplicationId): IO[Option[Application]] =
    PublisherBridge.first(collection.find(Filters.eq("_id", id.value.toString))).map(_.map(MongoHiringCodecs.readApplication))

  override def findByCandidate(candidateId: UserId, page: ApplicationPageRequest): IO[List[Application]] =
    findMany(baseFilter("candidateId", candidateId.value.toString, page), page)

  override def findByJob(jobId: JobId, page: ApplicationPageRequest): IO[List[Application]] =
    findMany(baseFilter("jobId", jobId.value.toString, page), page)

  override def createForOpenJob(
      observedJob: Job,
      application: Application,
      initialEvent: ApplicationEvent
  ): IO[Either[RepositoryError, Unit]] =
    if (!isConsistentSubmit(observedJob, application, initialEvent)) IO.pure(Left(RepositoryError.Conflict))
    else submitWithRetry(observedJob, application, initialEvent, remainingRetries = 2)

  override def updateStatus(application: Application, event: ApplicationEvent): IO[Either[RepositoryError, Unit]] =
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
          session.fold(
            PublisherBridge.first(events.insertOne(MongoHiringCodecs.event(event)))
          ) { active =>
            PublisherBridge.first(events.insertOne(active, MongoHiringCodecs.event(event)))
          }.attempt.map {
            case Right(_) => Right(())
            case Left(error) => mapDuplicateAs(RepositoryError.Conflict)(error)
          }
        case Some(_) => IO.pure(Left(RepositoryError.Conflict))
        case None => IO.pure(Left(RepositoryError.Unavailable))
      }
    }.handleError(mapWrite)

  private def findMany(filter: org.bson.conversions.Bson, page: ApplicationPageRequest): IO[List[Application]] =
    PublisherBridge.all(
      collection.find(filter)
        .sort(Sorts.orderBy(Sorts.descending("createdAt"), Sorts.descending("_id")))
        .limit(page.pageSize.value)
    ).map(_.map(MongoHiringCodecs.readApplication))

  private def baseFilter(field: String, id: String, page: ApplicationPageRequest): org.bson.conversions.Bson = {
    val statusFilter = page.status.map(status => Filters.eq("status", status.toString))
    val cursorFilter = page.cursor.map(cursor =>
      Filters.or(
        Filters.lt("createdAt", java.util.Date.from(cursor.createdAt)),
        Filters.and(Filters.eq("createdAt", java.util.Date.from(cursor.createdAt)), Filters.lt("_id", cursor.id.value.toString))
      )
    )
    Filters.and((List(Some(Filters.eq(field, id)), statusFilter, cursorFilter).flatten)*)
  }

  private def submitWithRetry(
      observedJob: Job,
      application: Application,
      initialEvent: ApplicationEvent,
      remainingRetries: Int
  ): IO[Either[RepositoryError, Unit]] =
    submitOnce(observedJob, application, initialEvent).flatMap {
      case Left(RepositoryError.Conflict) if remainingRetries > 0 =>
        currentOpenJob(observedJob.id).flatMap {
          case Some(current) => submitWithRetry(current, application, initialEvent, remainingRetries - 1)
          case None => IO.pure(Left(RepositoryError.Conflict))
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
      initialEvent: ApplicationEvent
  ): IO[Either[RepositoryError, Unit]] =
    transactionRunner.run { session =>
      val guardFilter = Filters.and(
        Filters.eq("_id", observedJob.id.value.toString),
        Filters.eq("status", JobStatus.Open.toString),
        Filters.eq("version", observedJob.version)
      )
      val guard = session.fold(
        PublisherBridge.first(jobs.updateOne(guardFilter, Updates.inc("version", 1L)))
      ) { active =>
        PublisherBridge.first(jobs.updateOne(active, guardFilter, Updates.inc("version", 1L)))
      }
      guard.flatMap {
        case Some(result) if result.getMatchedCount == 1L =>
          insertApplicationAndEvent(session, application, initialEvent)
        case Some(_) => IO.pure(Left(RepositoryError.Conflict))
        case None => IO.pure(Left(RepositoryError.Unavailable))
      }
    }.handleError(mapWrite)

  private def insertApplicationAndEvent(
      session: Option[ClientSession],
      application: Application,
      initialEvent: ApplicationEvent
  ): IO[Either[RepositoryError, Unit]] = {
    val insertApplication = session.fold(
      PublisherBridge.first(collection.insertOne(MongoHiringCodecs.application(application)))
    ) { active =>
      PublisherBridge.first(collection.insertOne(active, MongoHiringCodecs.application(application)))
    }
    val insertEvent = session.fold(
      PublisherBridge.first(events.insertOne(MongoHiringCodecs.event(initialEvent)))
    ) { active =>
      PublisherBridge.first(events.insertOne(active, MongoHiringCodecs.event(initialEvent)))
    }
    insertApplication.attempt.flatMap {
      case Left(error) => IO.pure(mapDuplicateAs(RepositoryError.DuplicateApplication)(error))
      case Right(_) =>
        insertEvent.attempt.map {
          case Right(_) => Right(())
          case Left(error) => mapDuplicateAs(RepositoryError.Conflict)(error)
        }
    }
  }

  private def currentOpenJob(id: JobId): IO[Option[Job]] =
    PublisherBridge.first(jobs.find(Filters.eq("_id", id.value.toString))).map(
      _.map(MongoHiringCodecs.readJob).filter(_.status == JobStatus.Open)
    )

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
    new MongoApplicationRepository(database, MongoTransactionRunner.sessions(client))
}
