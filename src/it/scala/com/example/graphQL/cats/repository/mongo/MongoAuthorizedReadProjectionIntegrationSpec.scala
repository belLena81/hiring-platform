package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.pagination.{ApplicationPageRequest, PageSize}
import com.example.graphQL.cats.service.{ActorContext, Diagnostics}
import com.example.graphQL.cats.service.auth.ActorAuthorization
import com.example.graphQL.cats.service.port.{ActorRecord, JobOwner, MutationWriteContext}
import com.example.graphQL.cats.service.read.HiringReadScope
import com.mongodb.client.model.{Filters, Updates}
import java.time.Instant
import java.util.UUID
import org.bson.{BsonDocument, Document}
import scala.jdk.CollectionConverters.*

/** Read shapes of the authorization and list paths: what is projected, and how the candidate page is ordered. */
final class MongoAuthorizedReadProjectionIntegrationSpec extends MongoIntegrationSuite {
  private val now = Instant.parse("2026-10-19T12:00:00Z")
  private val vector = EntityEmbedding(
    List.fill(64)(0.25f),
    EmbeddingMeta("test-model", "0" * 64, now)
  )
  private val vectorAndCredential = Set("embedding", "embeddingMeta", "passwordHash")

  private def candidate(name: String): User = User(
    UserId(UUID.randomUUID()),
    None,
    name,
    UserRole.Candidate,
    Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None))),
    now,
    embedding = Some(vector)
  )

  private def recruiter(name: String): User = User(
    UserId(UUID.randomUUID()),
    None,
    name,
    UserRole.Recruiter,
    Some(UserProfile.Recruiter(RecruiterProfile("Acme", None))),
    now
  )

  private def job(owner: UserId): Job = Job(
    JobId(UUID.randomUUID()),
    owner,
    "Scala developer",
    "Build backend services",
    List("Scala"),
    Set("Scala"),
    Location("Ukraine", Some("Kyiv"), remote = true),
    JobStatus.Open,
    now,
    now,
    embedding = Some(vector)
  )

  private final case class Repositories(
      users: MongoUserRepository,
      jobs: MongoJobRepository,
      applications: MongoApplicationRepository
  )

  private def repositories(fixture: MongoAccessEvaluationSupport.Fixture): Repositories =
    Repositories(
      MongoUserRepository.transactional(
        fixture.database,
        fixture.client,
        MongoEmbeddingWorkEnqueuer.disabled,
        Diagnostics.noop[IO]
      ),
      MongoJobRepository.transactional(
        fixture.database,
        fixture.client,
        MongoEmbeddingWorkEnqueuer.disabled,
        Diagnostics.noop[IO]
      ),
      MongoApplicationRepository.transactional(fixture.database, fixture.client, Diagnostics.noop[IO])
    )

  private def insertUser(fixture: MongoAccessEvaluationSupport.Fixture, user: User): IO[Unit] =
    MongoRepositoryTestSupport.insertOne(
      fixture.database,
      MongoCollections.Users,
      MongoHiringCodecs.userWithPassword(user, PasswordHash.fromEncoded("stored-password-hash"))
    )

  private def insertJob(fixture: MongoAccessEvaluationSupport.Fixture, value: Job): IO[Unit] =
    MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.Jobs, MongoHiringCodecs.job(value))

  private def fieldNames(projection: Document): Set[String] = projection.keySet.asScala.toSet

  private def issued(fixture: MongoAccessEvaluationSupport.Fixture, name: String, collection: String) =
    fixture.commands.snapshot.map(_.filter { command =>
      command.containsKey(name) && command.get(name).isString && command.getString(name).getValue == collection
    })

  private def projectionOf(command: BsonDocument): Document =
    Document.parse(command.getDocument("projection").toJson)

  test("AC-01.3 the actor read projects exactly the authorization fields and returns no profile or vector") {
    mongoResource.use { fixture =>
      val repository = repositories(fixture).users
      val subject = candidate("Projected actor")
      for {
        _ <- insertUser(fixture, subject)
        _ <- fixture.commands.clear
        actor <- repository.findActor(subject.id).value
        finds <- issued(fixture, "find", MongoCollections.Users)
      } yield {
        assertEquals(actor, Right(Some(ActorRecord(subject.id, UserRole.Candidate, AccountStatus.Active, false))))
        assertEquals(finds.size, 1)
        assertEquals(fieldNames(projectionOf(finds.head)), Set("_id", "role", "accountStatus", "adminSingletonKey"))
      }
    }
  }

  test("AC-01.3 the profile read and the account listing exclude the vector and the credential") {
    mongoResource.use { fixture =>
      val repository = repositories(fixture).users
      val subject = candidate("Projected profile")
      for {
        _ <- insertUser(fixture, subject)
        _ <- fixture.commands.clear
        found <- repository.find(subject.id).value
        listed <- repository
          .listAccounts(
            com.example.graphQL.cats.domain.model.UserPageRequest(
              AccountStatus.Active,
              None,
              None,
              PageSize.fromInt(10).toOption.get
            )
          )
          .value
        finds <- issued(fixture, "find", MongoCollections.Users)
      } yield {
        assertEquals(found.map(_.map(_.id)), Right(Some(subject.id)))
        assertEquals(found.map(_.flatMap(_.embedding)), Right(None))
        assertEquals(listed.map(_.flatMap(_.embedding)), Right(Nil))
        assertEquals(finds.size, 2)
        finds.foreach(command => assertEquals(fieldNames(projectionOf(command)), vectorAndCredential))
      }
    }
  }

  test("AC-01.3 only the embedding pipeline read keeps the vector, and it never reads the credential") {
    mongoResource.use { fixture =>
      val repository = repositories(fixture).users
      val subject = candidate("Embedded profile")
      for {
        _ <- insertUser(fixture, subject)
        _ <- fixture.commands.clear
        full <- repository.findVersioned(subject.id).value
        finds <- issued(fixture, "find", MongoCollections.Users)
      } yield {
        assertEquals(full.map(_.flatMap(_.value.embedding)), Right(Some(vector)))
        assertEquals(fieldNames(projectionOf(finds.head)), Set("passwordHash"))
      }
    }
  }

  test("AC-01.4 the owner read projects only the recruiter and status of a job; display reads drop the vector") {
    mongoResource.use { fixture =>
      val repository = repositories(fixture).jobs
      val owner = recruiter("Job owner")
      val stored = job(owner.id)
      for {
        _ <- insertJob(fixture, stored)
        _ <- fixture.commands.clear
        ownership <- repository.findOwner(stored.id).value
        display <- repository.find(stored.id).value
        open <- repository
          .findByRecruiter(
            owner.id,
            com.example.graphQL.cats.domain.pagination.JobPageRequest(None, None, PageSize.fromInt(10).toOption.get)
          )
          .value
        finds <- issued(fixture, "find", MongoCollections.Jobs)
      } yield {
        assertEquals(ownership, Right(Some(JobOwner(stored.id, owner.id, JobStatus.Open))))
        assertEquals(display.map(_.map(_.title)), Right(Some(stored.title)))
        assertEquals(display.map(_.flatMap(_.embedding)), Right(None))
        assertEquals(open.map(_.flatMap(_.embedding)), Right(Nil))
        assertEquals(
          finds.map(command => fieldNames(projectionOf(command))),
          List(
            Set("_id", "recruiterId", "status"),
            Set("embedding", "embeddingMeta"),
            Set("embedding", "embeddingMeta")
          )
        )
      }
    }
  }

  test("AC-01.5 related users and jobs exclude the vector and the credential inside their lookups") {
    mongoResource.use { fixture =>
      val Repositories(users, jobs, applications) = repositories(fixture)
      val subject = candidate("Related candidate")
      val owner = recruiter("Related recruiter")
      val stored = job(owner.id)
      val application = Application.create(ApplicationId(UUID.randomUUID()), subject.id, stored.id, now)
      for {
        _ <- insertUser(fixture, subject)
        _ <- insertUser(fixture, owner)
        _ <- insertJob(fixture, stored)
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.Applications,
          MongoHiringCodecs.application(application)
        )
        scope <- IO.fromEither(
          HiringReadScope
            .validated(
              ActorContext(subject.id, subject.role),
              ActorRecord.of(subject),
              ActorAuthorization(users)
            )
            .leftMap(error => new AssertionError(error.toString))
        )
        _ <- fixture.commands.clear
        relatedUsers <- users
          .relatedUsers(
            scope,
            List(com.example.graphQL.cats.service.read.UserRelationKey.ApplicationCandidate(application.id, subject.id))
          )
          .value
        relatedJobs <- jobs
          .relatedJobs(
            scope,
            List(com.example.graphQL.cats.service.read.JobRelationKey(application.id, stored.id))
          )
          .value
        commands <- fixture.commands.snapshot
      } yield {
        assertEquals(relatedUsers.map(_.flatMap(_.value.embedding)), Right(Nil))
        assertEquals(relatedUsers.map(_.map(_.value.id)), Right(List(subject.id)))
        assertEquals(relatedJobs.map(_.map(_.value.id)), Right(List(stored.id)))
        assertEquals(relatedJobs.map(_.flatMap(_.value.embedding)), Right(Nil))
        val pipelines = commands.filter(_.containsKey("aggregate")).map(_.toJson)
        assertEquals(pipelines.size, 2)
        assert(
          pipelines.exists(p => p.contains("\"passwordHash\": 0") && p.contains("\"embedding\": 0")),
          pipelines.toString
        )
        assert(
          pipelines.exists(p => p.contains("\"embedding\": 0") && p.contains("\"embeddingMeta\": 0")),
          pipelines.toString
        )
        val _ = applications
      }
    }
  }

  test("AC-01.6 a profile update returns the updated profile from one findAndModify and no second read") {
    mongoResource.use { fixture =>
      val repository = repositories(fixture).users
      val subject = recruiter("Profile recruiter")
      for {
        _ <- insertUser(fixture, subject)
        _ <- fixture.commands.clear
        updated <- repository
          .updateProfile(
            subject.id,
            UserProfile.Recruiter(RecruiterProfile("Updated organization", Some("Lead"))),
            now.plusSeconds(60),
            MutationWriteContext.directWrite
          )
          .value
        commands <- fixture.commands.snapshot
      } yield {
        assertEquals(
          updated.map(_.profile),
          Right(Some(UserProfile.Recruiter(RecruiterProfile("Updated organization", Some("Lead")))))
        )
        assertEquals(commands.map(_.getFirstKey), List("findAndModify"))
      }
    }
  }

  test("AC-01.6 an embedding write reads only the revision before updating") {
    mongoResource.use { fixture =>
      val repository = repositories(fixture).users
      val subject = candidate("Embedding candidate").copy(embedding = None)
      for {
        _ <- insertUser(fixture, subject)
        _ <- fixture.commands.clear
        written <- repository.updateEmbedding(subject.id, vector).value
        finds <- issued(fixture, "find", MongoCollections.Users)
        reread <- repository.findVersioned(subject.id).value
      } yield {
        assertEquals(written, Right(()))
        assertEquals(finds.map(command => fieldNames(projectionOf(command))), List(Set("_id", "version")))
        // Derived data never advances the content revision.
        assertEquals(reread.map(_.map(_.version)), Right(Some(0L)))
        assertEquals(reread.map(_.flatMap(_.value.embedding)), Right(Some(vector)))
      }
    }
  }

  test("AC-03.1 the candidate page is ordered by the candidate index before the actor check, with no SORT stage") {
    mongoResource.use { fixture =>
      val Repositories(users, _, applications) = repositories(fixture)
      val subject = candidate("Paging candidate")
      val owner = recruiter("Paging recruiter")
      val perJobTime = (i: Int) => now.plusSeconds(i.toLong)
      val own = (0 until 1000).toList.map { i =>
        val stored = job(owner.id)
        Application.create(ApplicationId(UUID.randomUUID()), subject.id, stored.id, perJobTime(i))
      }
      val others = (0 until 500).toList.map(i =>
        Application.create(
          ApplicationId(UUID.randomUUID()),
          UserId(UUID.randomUUID()),
          JobId(UUID.randomUUID()),
          perJobTime(i)
        )
      )
      val page = ApplicationPageRequest(None, None, PageSize.fromInt(20).toOption.get)
      def explain(pipeline: java.util.List[Document]): IO[String] =
        MongoAccessEvaluationSupport
          .command(
            fixture.database,
            new Document(
              "explain",
              new Document("aggregate", MongoCollections.Applications)
                .append("pipeline", pipeline)
                .append("cursor", new Document())
            ).append("verbosity", "executionStats")
          )
          .map(_.toJson)
      for {
        _ <- MongoHiringIndexSetup.create(fixture.database)
        _ <- insertUser(fixture, subject)
        collection <- MongoRepositoryTestSupport.collection(fixture.database, MongoCollections.Applications)
        _ <- collection.insertMany((own ++ others).map(MongoHiringCodecs.application))
        scope <- IO.fromEither(
          HiringReadScope
            .validated(ActorContext(subject.id, subject.role), ActorRecord.of(subject), ActorAuthorization(users))
            .leftMap(error => new AssertionError(error.toString))
        )
        _ <- fixture.commands.clear
        firstPage <- applications.findByCandidate(scope, page).value
        aggregates <- issued(fixture, "aggregate", MongoCollections.Applications)
        issuedPipeline = Document.parse(aggregates.head.toJson).get("pipeline", classOf[java.util.List[Document]])
        after <- explain(issuedPipeline)
        // The shape before the reorder: the actor lookup ran on every matched row, then sort, then limit.
        legacyPipeline = {
          import MongoAuthorizedReadQueries.*
          val matched = Document.parse(aggregates.head.toJson).get("pipeline", classOf[java.util.List[Document]]).get(0)
          (List(matched) ++ actor(scope) ++ List(
            new Document("$sort", new Document("createdAt", -1).append("_id", -1)),
            new Document("$limit", 20)
          )).asJava
        }
        before <- explain(legacyPipeline)
      } yield {
        assertEquals(firstPage.map(_.size), Right(20))
        assertEquals(firstPage.map(_.map(_.createdAt)), Right(own.map(_.createdAt).sorted.reverse.take(20)))
        println(s"AC-03.1 before: ${summary(before)}")
        println(s"AC-03.1 after: ${summary(after)}")
        val winning = winningPlan(after)
        assert(winning.contains("IXSCAN"), winning)
        assert(!winning.contains("\"stage\": \"SORT\""), winning)
        assert(!winning.contains("COLLSCAN"), winning)
        assert(docsExamined(after) <= 20L, summary(after))
        assert(docsExamined(before) >= 1000L, summary(before))
      }
    }
  }

  private def markDeleted(fixture: MongoAccessEvaluationSupport.Fixture, id: UserId): IO[Unit] =
    MongoRepositoryTestSupport
      .collection(fixture.database, MongoCollections.Users)
      .flatMap(
        _.updateOne(
          com.mongodb.client.model.Filters.eq("_id", id.value.toString),
          com.mongodb.client.model.Updates.set("accountStatus", "Deleted")
        )
      )
      .void

  private def scopeOf(user: User, users: MongoUserRepository): IO[HiringReadScope] =
    IO.fromEither(
      HiringReadScope
        .validated(ActorContext(user.id, user.role), ActorRecord.of(user), ActorAuthorization(users))
        .leftMap(error => new AssertionError(error.toString))
    )

  private val firstPage = ApplicationPageRequest(None, None, PageSize.fromInt(10).toOption.get)

  private def seededApplication(candidateId: UserId, jobId: JobId): Application =
    Application.create(ApplicationId(UUID.randomUUID()), candidateId, jobId, now)

  test("AC-03.2 a deleted-account candidate and a scope of another role get an empty candidate page") {
    mongoResource.use { fixture =>
      val Repositories(users, _, applications) = repositories(fixture)
      val subject = candidate("Deleted paging candidate")
      val owner = recruiter("Wrong-role recruiter")
      val stored = job(owner.id)
      for {
        _ <- MongoHiringIndexSetup.create(fixture.database)
        _ <- insertUser(fixture, subject)
        _ <- insertUser(fixture, owner)
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.Applications,
          MongoHiringCodecs.application(seededApplication(subject.id, stored.id))
        )
        activeScope <- scopeOf(subject, users)
        active <- applications.findByCandidate(activeScope, firstPage).value
        recruiterScope <- scopeOf(owner, users)
        wrongRole <- applications.findByCandidate(recruiterScope, firstPage).value
        _ <- markDeleted(fixture, subject.id)
        deleted <- applications.findByCandidate(activeScope, firstPage).value
      } yield {
        assertEquals(active.map(_.size), Right(1))
        assertEquals(wrongRole, Right(Nil))
        assertEquals(deleted, Right(Nil))
      }
    }
  }

  test("AC-03.3 an actor deleted after its scope was granted is refused by every pipeline predicate") {
    mongoResource.use { fixture =>
      val Repositories(users, jobs, applications) = repositories(fixture)
      val subject = candidate("Cached candidate")
      val owner = recruiter("Cached recruiter")
      val stored = job(owner.id)
      val application = seededApplication(subject.id, stored.id)
      for {
        _ <- MongoHiringIndexSetup.create(fixture.database)
        _ <- insertUser(fixture, subject)
        _ <- insertUser(fixture, owner)
        _ <- insertJob(fixture, stored)
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.Applications,
          MongoHiringCodecs.application(application)
        )
        recruiterScope <- scopeOf(owner, users)
        candidateScope <- scopeOf(subject, users)
        key = com.example.graphQL.cats.service.read.UserRelationKey.ApplicationCandidate(application.id, subject.id)
        before <- applications.findByJob(recruiterScope, stored.id, firstPage).value
        beforeRelated <- users.relatedUsers(recruiterScope, List(key)).value
        _ <- markDeleted(fixture, owner.id)
        byJob <- applications.findByJob(recruiterScope, stored.id, firstPage).value
        related <- users.relatedUsers(recruiterScope, List(key)).value
        _ <- markDeleted(fixture, subject.id)
        byCandidate <- applications.findByCandidate(candidateScope, firstPage).value
        relatedByCandidate <- users.relatedUsers(candidateScope, List(key)).value
        _ = jobs
      } yield {
        assertEquals(before.map(_.size), Right(1))
        assertEquals(beforeRelated.map(_.size), Right(1))
        assertEquals(byJob, Right(Nil))
        assertEquals(related, Right(Nil))
        assertEquals(byCandidate, Right(Nil))
        assertEquals(relatedByCandidate, Right(Nil))
      }
    }
  }

  private def transactionalUsers(fixture: MongoAccessEvaluationSupport.Fixture): MongoUserRepository =
    MongoUserRepository.transactional(
      fixture.database,
      fixture.client,
      new MongoEmbeddingWorkRepository(fixture.database, Diagnostics.noop[IO]),
      Diagnostics.noop[IO]
    )

  private val candidateProfile = UserProfile.Candidate(CandidateProfile(Set("Scala", "Cats"), Some("Updated"), None))

  test("AC-01.7 a candidate profile update is one findAndModify with embedding work enqueued in the same transaction") {
    mongoResource.use { fixture =>
      val repository = transactionalUsers(fixture)
      val subject = candidate("Updating candidate")
      for {
        _ <- insertUser(fixture, subject)
        _ <- fixture.commands.clear
        updated <- repository
          .updateProfile(subject.id, candidateProfile, now.plusSeconds(60), MutationWriteContext.directWrite)
          .value
        commands <- fixture.commands.snapshot
        work <- MongoRepositoryTestSupport.count(fixture.database, MongoCollections.EmbeddingWork)
      } yield {
        assertEquals(updated.map(_.profile), Right(Some(candidateProfile)))
        assertEquals(updated.map(_.embedding), Right(None))
        assertEquals(commands.count(_.containsKey("findAndModify")), 1)
        assertEquals(
          commands.count(command =>
            command.containsKey("find") && command.getString("find").getValue == MongoCollections.Users
          ),
          0
        )
        assertEquals(work, 1L)
        assert(commands.exists(_.containsKey("commitTransaction")), commands.map(_.getFirstKey).toString)
        // The enqueue and the user write share one transaction: every data command carries the same session.
        val sessions = commands
          .filter(c => c.containsKey("findAndModify") || c.containsKey("update") && !c.containsKey("findAndModify"))
          .map(_.get("lsid"))
        assertEquals(sessions.distinct.size, 1)
      }
    }
  }

  test("AC-01.7 a lost guard on a profile update is a Conflict and writes no embedding work") {
    mongoResource.use { fixture =>
      val repository = transactionalUsers(fixture)
      val deleted = candidate("Deleted updater")
      val owner = recruiter("Role mismatch updater")
      for {
        _ <- insertUser(fixture, deleted)
        _ <- insertUser(fixture, owner)
        _ <- markDeleted(fixture, deleted.id)
        lost <- repository.updateProfile(deleted.id, candidateProfile, now, MutationWriteContext.directWrite).value
        mismatch <- repository.updateProfile(owner.id, candidateProfile, now, MutationWriteContext.directWrite).value
        absent <- repository
          .updateProfile(
            UserId(UUID.randomUUID()),
            UserProfile.Recruiter(RecruiterProfile("Acme", None)),
            now,
            MutationWriteContext.directWrite
          )
          .value
        work <- MongoRepositoryTestSupport.count(fixture.database, MongoCollections.EmbeddingWork)
      } yield {
        assertEquals(lost.map(_.id), Left(com.example.graphQL.cats.service.RepositoryError.Conflict))
        assertEquals(mismatch.map(_.id), Left(com.example.graphQL.cats.service.RepositoryError.Conflict))
        assertEquals(absent.map(_.id), Left(com.example.graphQL.cats.service.RepositoryError.Conflict))
        assertEquals(work, 0L)
      }
    }
  }

  test("AC-01.8 an embedding write is a Conflict for an absent document, a deleted account and a stale revision") {
    mongoResource.use { fixture =>
      val repository = repositories(fixture).users
      val subject = candidate("Stale embedding").copy(embedding = None)
      val gone = candidate("Gone embedding").copy(embedding = None)
      val conflict = Left(com.example.graphQL.cats.service.RepositoryError.Conflict)
      for {
        _ <- insertUser(fixture, subject)
        _ <- insertUser(fixture, gone)
        absent <- repository.updateEmbedding(UserId(UUID.randomUUID()), vector).value
        observed <- repository.findVersioned(subject.id).value.map(_.toOption.flatten.get)
        // Another writer edits the content (its revision advances); an embedding write never does.
        _ <- MongoRepositoryTestSupport
          .collection(fixture.database, MongoCollections.Users)
          .flatMap(
            _.updateOne(Filters.eq(MongoFields.Id, subject.id.value.toString), Updates.inc(MongoFields.Version, 1L))
          )
        stale <- repository.updateEmbedding(observed, vector).value
        _ <- markDeleted(fixture, gone.id)
        deleted <- repository.updateEmbedding(gone.id, vector).value
      } yield {
        assertEquals(absent, conflict)
        assertEquals(stale, conflict)
        assertEquals(deleted, conflict)
      }
    }
  }

  /** The plan the server chose for the first pipeline stages (rejected plans may legitimately contain a SORT). */
  private def winningPlan(explainJson: String): String =
    Document
      .parse(explainJson)
      .getList("stages", classOf[Document])
      .get(0)
      .get("$cursor", classOf[Document])
      .get("queryPlanner", classOf[Document])
      .get("winningPlan", classOf[Document])
      .toJson

  private def docsExamined(explainJson: String): Long =
    "\"totalDocsExamined\": (?:\\{\"\\$numberLong\": \")?(\\d+)".r
      .findAllMatchIn(explainJson)
      .map(_.group(1).toLong)
      .maxOption
      .getOrElse(-1L)

  private def summary(explainJson: String): String =
    s"docsExamined=${docsExamined(explainJson)} winningPlan=${winningPlan(explainJson)}"
}
