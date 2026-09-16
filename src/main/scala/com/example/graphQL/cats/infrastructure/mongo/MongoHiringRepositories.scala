package com.example.graphQL.cats.infrastructure.mongo

import cats.effect.{IO, Resource}
import com.example.graphQL.cats.application.port.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.{Application, ApplicationEvent, Job, User}
import com.mongodb.MongoWriteException
import com.mongodb.client.model.{Filters, Sorts}
import com.mongodb.reactivestreams.client.{ClientSession, MongoClient, MongoDatabase}
import org.bson.Document

final class MongoUserRepository(database: MongoDatabase) extends UserRepository[IO] {
  private val collection = database.getCollection("users")

  def insert(user: User): IO[Either[RepositoryError, Unit]] =
    PublisherBridge.first(collection.insertOne(MongoHiringCodecs.user(user))).as(Right(())).handleError(mapWrite)

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

  override def update(job: Job): IO[Either[RepositoryError, Unit]] =
    PublisherBridge.first(collection.replaceOne(Filters.eq("_id", job.id.value.toString), MongoHiringCodecs.job(job)))
      .map {
        case Some(result) if result.getMatchedCount == 1L => Right(())
        case Some(_) => Left(RepositoryError.Conflict)
        case None => Left(RepositoryError.Unavailable)
      }
      .handleError(mapWrite)

  private def mapWrite(error: Throwable): Either[RepositoryError, Unit] =
    error match {
      case write: MongoWriteException if write.getError.getCode == 11000 => Left(RepositoryError.Conflict)
      case _ => Left(RepositoryError.Unavailable)
    }
}

final class MongoApplicationRepository(database: MongoDatabase, client: Option[MongoClient] = None) extends ApplicationRepository[IO] {
  private val collection = database.getCollection("applications")
  private val events = database.getCollection("application_events")

  override def find(id: ApplicationId): IO[Option[Application]] =
    PublisherBridge.first(collection.find(Filters.eq("_id", id.value.toString))).map(_.map(MongoHiringCodecs.readApplication))

  override def findByCandidate(candidateId: UserId, page: ApplicationPageRequest): IO[List[Application]] =
    findMany(baseFilter("candidateId", candidateId.value.toString, page), page)

  override def findByJob(jobId: JobId, page: ApplicationPageRequest): IO[List[Application]] =
    findMany(baseFilter("jobId", jobId.value.toString, page), page)

  override def create(application: Application, initialEvent: ApplicationEvent): IO[Either[RepositoryError, Unit]] =
    withOptionalTransaction { session =>
      session.fold(
        PublisherBridge.first(collection.insertOne(MongoHiringCodecs.application(application))).flatMap { _ =>
          PublisherBridge.first(events.insertOne(MongoHiringCodecs.event(initialEvent)))
        }
      ) { active =>
        PublisherBridge.first(collection.insertOne(active, MongoHiringCodecs.application(application))).flatMap { _ =>
          PublisherBridge.first(events.insertOne(active, MongoHiringCodecs.event(initialEvent)))
        }
      }.as(Right(()))
    }.handleError(mapWrite)

  override def updateStatus(application: Application, event: ApplicationEvent): IO[Either[RepositoryError, Unit]] =
    withOptionalTransaction { session =>
      val update = session.fold(
        PublisherBridge.first(collection.replaceOne(Filters.eq("_id", application.id.value.toString), MongoHiringCodecs.application(application)))
      ) { active =>
        PublisherBridge.first(collection.replaceOne(active, Filters.eq("_id", application.id.value.toString), MongoHiringCodecs.application(application)))
      }
      update.flatMap {
        case Some(result) if result.getMatchedCount == 1L =>
          session.fold(
            PublisherBridge.first(events.insertOne(MongoHiringCodecs.event(event)))
          ) { active =>
            PublisherBridge.first(events.insertOne(active, MongoHiringCodecs.event(event)))
          }.as(Right(()))
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

  private def mapWrite(error: Throwable): Either[RepositoryError, Unit] =
    error match {
      case write: MongoWriteException if write.getError.getCode == 11000 => Left(RepositoryError.DuplicateApplication)
      case _ => Left(RepositoryError.Unavailable)
    }

  private def withOptionalTransaction(operation: Option[ClientSession] => IO[Either[RepositoryError, Unit]]): IO[Either[RepositoryError, Unit]] =
    client match {
      case None => operation(None)
      case Some(mongoClient) =>
        Resource.make(PublisherBridge.first(mongoClient.startSession()).flatMap {
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
    }
}
