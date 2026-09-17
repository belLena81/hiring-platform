package com.example.graphQL.cats.infrastructure.mongo

import cats.effect.{IO, Resource}
import com.example.graphQL.cats.application.port.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.{Application, ApplicationEvent, ApplicationStatus, EntityEmbedding, Job, JobStatus, User, UserRole}
import com.mongodb.{MongoCommandException, MongoWriteException}
import com.mongodb.client.model.{Filters, Sorts, Updates}
import com.mongodb.client.result.InsertOneResult
import com.mongodb.reactivestreams.client.{ClientSession, MongoClient, MongoCollection, MongoDatabase}
import org.bson.Document
import org.bson.conversions.Bson

import java.time.Instant
import scala.jdk.CollectionConverters.*

private[mongo] trait MongoTransactionRunner {
  def run(operation: Option[ClientSession] => IO[Either[RepositoryError, Unit]]): IO[Either[RepositoryError, Unit]]
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

private[mongo] object MongoKeysetPaging {
  def byId[A](collection: MongoCollection[Document], ids: List[String])(read: Document => A): IO[List[A]] =
    if (ids.isEmpty) IO.pure(Nil)
    else PublisherBridge.all(collection.find(Filters.in("_id", ids.distinct*))).map(_.map(read))

  def page[A](collection: MongoCollection[Document], filter: Bson, timestampField: String, pageSize: PageSize)(
      read: Document => A
  ): IO[List[A]] =
    PublisherBridge.all(
      collection.find(filter)
        .sort(Sorts.orderBy(Sorts.descending(timestampField), Sorts.descending("_id")))
        .limit(pageSize.value)
    ).map(_.map(read))

  def filter(filters: List[Option[Bson]]): Bson =
    Filters.and(filters.flatten*)

  def beforeCursor(timestampField: String, occurredAt: Instant, id: String): Bson =
    Filters.or(
      Filters.lt(timestampField, java.util.Date.from(occurredAt)),
      Filters.and(Filters.eq(timestampField, java.util.Date.from(occurredAt)), Filters.lt("_id", id))
    )
}

final class MongoUserRepository(database: MongoDatabase) extends UserRepository[IO] with MongoConflictWriteMapping {
  private val collection = database.getCollection("users")

  def insert(user: User): IO[Either[RepositoryError, Unit]] =
    if (user.role == UserRole.Admin && !user.adminSingleton) IO.pure(Left(RepositoryError.Conflict))
    else PublisherBridge.first(collection.insertOne(MongoHiringCodecs.user(user))).as(Right(())).handleError(mapWrite)

  override def find(id: UserId): IO[Option[User]] =
    PublisherBridge.first(collection.find(Filters.eq("_id", id.value.toString))).map(_.map(MongoHiringCodecs.readUser))

  override def findMany(ids: List[UserId]): IO[List[User]] =
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
}

final class MongoJobRepository(database: MongoDatabase) extends JobRepository[IO] with MongoConflictWriteMapping {
  private val collection = database.getCollection("jobs")

  override def find(id: JobId): IO[Option[Job]] =
    PublisherBridge.first(collection.find(Filters.eq("_id", id.value.toString))).map(_.map(MongoHiringCodecs.readJob))

  override def findMany(ids: List[JobId]): IO[List[Job]] =
    MongoKeysetPaging.byId(collection, ids.map(_.value.toString))(MongoHiringCodecs.readJob)

  override def findOpen(filter: JobSearchFilter, page: JobPageRequest): IO[List[Job]] =
    findMany(baseSearchFilter(filter, page), page)

  override def findAll(page: JobPageRequest): IO[List[Job]] =
    findMany(baseJobFilter(List(page.status.map(status => Filters.eq("status", status.toString))), page), page)

  override def findByRecruiter(recruiterId: UserId, page: JobPageRequest): IO[List[Job]] =
    findMany(baseJobFilter(List(Some(Filters.eq("recruiterId", recruiterId.value.toString)), page.status.map(status => Filters.eq("status", status.toString))), page), page)

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

  private def findMany(filter: Bson, page: JobPageRequest): IO[List[Job]] =
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
}

final class MongoSemanticSearchRepository(
    database: MongoDatabase,
    jobVectorIndex: String,
    candidateVectorIndex: String,
    numCandidates: Int
) extends SemanticSearchRepository[IO] {
  private val jobs = database.getCollection("jobs")
  private val users = database.getCollection("users")

  override def searchJobs(query: VectorSearchQuery): IO[Either[RepositoryError, List[RankedJob]]] =
    rankedJobs(query, jobFilter(query, query.filter))

  override def recommendedJobs(query: VectorSearchQuery): IO[Either[RepositoryError, List[RankedJob]]] =
    rankedJobs(query, jobFilter(query, JobSearchFilter(None, Set.empty, None)))

  override def candidateMatches(query: VectorSearchQuery): IO[Either[RepositoryError, List[RankedCandidate]]] = {
    val filter = Filters.and(
      Filters.eq("role", UserRole.Candidate.toString),
      Filters.exists("profile"),
      Filters.eq("embeddingMeta.model", query.model),
      Filters.eq("embeddingMeta.version", query.version)
    )
    val pipeline = List(vectorSearchStage(candidateVectorIndex, query.vector, filter, query.first), scoreStage).asJava
    PublisherBridge.all(users.aggregate(pipeline)).map { documents =>
      Right(documents.flatMap { document =>
        for {
          embedding <- MongoHiringCodecs.readUser(document).embedding
          score <- Option(document.get("score", classOf[Number]))
        } yield RankedCandidate(MongoHiringCodecs.readUser(document), score.doubleValue, query.mode, embedding.meta, query.searchId)
      })
    }.handleError(_ => Left(RepositoryError.Unavailable))
  }

  private def rankedJobs(query: VectorSearchQuery, filter: Bson): IO[Either[RepositoryError, List[RankedJob]]] = {
    val pipeline = List(vectorSearchStage(jobVectorIndex, query.vector, filter, query.first), scoreStage).asJava
    PublisherBridge.all(jobs.aggregate(pipeline)).map { documents =>
      Right(documents.flatMap { document =>
        val job = MongoHiringCodecs.readJob(document)
        for {
          embedding <- job.embedding
          score <- Option(document.get("score", classOf[Number]))
        } yield RankedJob(job, score.doubleValue, query.mode, embedding.meta, query.searchId)
      })
    }.handleError(_ => Left(RepositoryError.Unavailable))
  }

  private def jobFilter(query: VectorSearchQuery, filter: JobSearchFilter): Bson =
    MongoKeysetPaging.filter(List(
      Some(Filters.eq("status", JobStatus.Open.toString)),
      Some(Filters.eq("embeddingMeta.model", query.model)),
      Some(Filters.eq("embeddingMeta.version", query.version)),
      filter.city.map(city => Filters.eq("location.city", city)),
      skillsFilter(filter.skills),
      filter.createdAfter.map(createdAfter => Filters.gte("createdAt", java.util.Date.from(createdAfter)))
    ))

  private def skillsFilter(skills: Set[String]): Option[Bson] =
    Option.when(skills.nonEmpty)(Filters.and(skills.toList.sorted.map(skill => Filters.eq("skills", skill))*))

  private def vectorSearchStage(index: String, vector: List[Float], filter: Bson, first: PageSize): Document =
    new Document("$vectorSearch", new Document("index", index)
      .append("path", "embedding")
      .append("queryVector", vector.map(float => java.lang.Double.valueOf(float.toDouble)).asJava)
      .append("numCandidates", java.lang.Integer.valueOf(math.max(numCandidates, first.value)))
      .append("limit", java.lang.Integer.valueOf(first.value))
      .append("filter", filter))

  private val scoreStage: Document =
    new Document("$set", new Document("score", new Document("$meta", "vectorSearchScore")))
}

final class MongoApplicationRepository private (
    database: MongoDatabase,
    transactionRunner: MongoTransactionRunner
) extends ApplicationRepository[IO] with MongoApplicationEventInsertion {
  private val collection = database.getCollection("applications")
  private val events = database.getCollection("application_events")
  private val jobs = database.getCollection("jobs")

  override def find(id: ApplicationId): IO[Option[Application]] =
    PublisherBridge.first(collection.find(Filters.eq("_id", id.value.toString))).map(_.map(MongoHiringCodecs.readApplication))

  override def findByCandidate(candidateId: UserId, page: ApplicationPageRequest): IO[List[Application]] =
    findMany(baseFilter("candidateId", candidateId.value.toString, page), page)

  override def findByJob(jobId: JobId, page: ApplicationPageRequest): IO[List[Application]] =
    findMany(baseFilter("jobId", jobId.value.toString, page), page)

  override def history(applicationId: ApplicationId, page: ApplicationEventPageRequest): IO[List[ApplicationEvent]] =
    MongoKeysetPaging.page(events, eventFilter(applicationId, page), "occurredAt", page.pageSize)(MongoHiringCodecs.readEvent)

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
          insertApplicationEvent(events, session, event).attempt.map {
            case Right(_) => Right(())
            case Left(error) => mapDuplicateAs(RepositoryError.Conflict)(error)
          }
        case Some(_) => IO.pure(Left(RepositoryError.Conflict))
        case None => IO.pure(Left(RepositoryError.Unavailable))
      }
    }.handleError(mapWrite)

  private def findMany(filter: Bson, page: ApplicationPageRequest): IO[List[Application]] =
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
    val insertEvent = insertApplicationEvent(events, session, initialEvent)
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
