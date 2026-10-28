package com.example.graphQL.cats.repository.mongo

import io.github.iltotore.iron.autoRefine
import cats.effect.IO
import cats.effect.std.Semaphore
import cats.syntax.all.*
import com.example.graphQL.cats.config.{AdminSeedConfig, PasswordHashConfig}
import com.example.graphQL.cats.infrastructure.auth.Argon2PasswordHasher
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.pagination.{ApplicationPageRequest, PageSize}
import com.example.graphQL.cats.service.{ActorContext, Diagnostics}
import com.example.graphQL.cats.service.auth.ActorAuthorization
import com.example.graphQL.cats.service.port.ActorRecord
import com.example.graphQL.cats.service.read.HiringReadScope
import io.circe.Json
import io.circe.parser.parse
import java.time.Instant
import java.util.UUID
import org.bson.{BsonDocument, Document}
import scala.concurrent.duration.*

/** The recruiter dashboard reads application counts per status for a page of jobs: one authorized read, scoped to the
  * jobs the actor manages, instead of one list read per job.
  */
final class MongoJobApplicationCountsIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes

  // Declared before the tests that use it: tests are registered while the class is constructed.
  private val measurement = new munit.Tag("measurement")

  private val now = Instant.parse("2026-10-28T12:00:00Z")
  private val jobCount = 100
  private val applicationsPerJob = 50

  private def user(name: String, role: UserRole): User = User(
    UserId(UUID.randomUUID()),
    None,
    name,
    role,
    Some(role match {
      case UserRole.Recruiter => UserProfile.Recruiter(RecruiterProfile("Acme", None))
      case _                  => UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None))
    }),
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
    JobState.Open,
    now,
    now
  )

  private def applicationsFor(target: Job, count: Int): List[Application] =
    (0 until count).toList.map { index =>
      Application
        .create(ApplicationId(UUID.randomUUID()), UserId(UUID.randomUUID()), target.id, now.plusSeconds(index.toLong))
        .copy(status = ApplicationStatus.values(index % ApplicationStatus.values.length))
    }

  private def failed(error: com.example.graphQL.cats.service.RepositoryError): Throwable =
    new AssertionError(error.toString)

  private def scopeOf(subject: User, users: MongoUserRepository): IO[HiringReadScope] =
    IO.fromEither(
      HiringReadScope
        .validated(ActorContext(subject.id, subject.role), ActorRecord.of(subject), ActorAuthorization(users))
        .leftMap(error => new AssertionError(error.toString))
    )

  private def seed(
      fixture: MongoAccessEvaluationSupport.Fixture,
      users: List[User],
      jobs: List[Job],
      apps: List[Application]
  ): IO[Unit] = seedAfter(fixture, users, jobs, apps)(IO.unit)

  /** `provision` runs after the database setup and before any other account exists, as the startup seed does. */
  private def seedAfter[A](
      fixture: MongoAccessEvaluationSupport.Fixture,
      users: List[User],
      jobs: List[Job],
      apps: List[Application]
  )(provision: IO[A]): IO[A] =
    for {
      _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
      provisioned <- provision
      _ <- users.traverse_(subject =>
        MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.Users,
          MongoHiringCodecs.userWithPassword(subject, PasswordHash.fromEncoded("stored-password-hash"))
        )
      )
      jobsCollection <- MongoRepositoryTestSupport.collection(fixture.database, MongoCollections.Jobs)
      _ <- jobsCollection.insertMany(jobs.map(MongoHiringCodecs.job(_)))
      applicationsCollection <- MongoRepositoryTestSupport.collection(fixture.database, MongoCollections.Applications)
      _ <- IO.whenA(apps.nonEmpty)(applicationsCollection.insertMany(apps.map(MongoHiringCodecs.application)).void)
    } yield provisioned

  private def repositories(fixture: MongoAccessEvaluationSupport.Fixture) =
    (
      MongoUserRepository.transactional(
        fixture.database,
        fixture.client,
        MongoEmbeddingWorkEnqueuer.disabled,
        Diagnostics.noop[IO]
      ),
      MongoApplicationRepository.transactional(fixture.database, fixture.client, Diagnostics.noop[IO])
    )

  test("AC-14.9 counts cover every status, match the stored applications and only report managed jobs") {
    mongoResource.use { fixture =>
      val owner = user("Owner", UserRole.Recruiter)
      val stranger = user("Stranger", UserRole.Recruiter)
      val reader = user("Candidate", UserRole.Candidate)
      val owned = job(owner.id)
      val foreign = job(stranger.id)
      val apps = applicationsFor(owned, 14) ++ applicationsFor(foreign, 3)
      val (users, applications) = repositories(fixture)
      for {
        _ <- seed(fixture, List(owner, stranger, reader), List(owned, foreign), apps)
        ownerScope <- scopeOf(owner, users)
        strangerScope <- scopeOf(stranger, users)
        candidateScope <- scopeOf(reader, users)
        asOwner <- applications.applicationCounts(ownerScope, List(owned.id, foreign.id)).value
        asStranger <- applications.applicationCounts(strangerScope, List(owned.id)).value
        asCandidate <- applications.applicationCounts(candidateScope, List(owned.id, foreign.id)).value
        unknown <- applications.applicationCounts(ownerScope, List(JobId(UUID.randomUUID()))).value
      } yield {
        val own = asOwner.toOption.get
        assertEquals(own.map(_.jobId), List(owned.id))
        assertEquals(own.head.total, 14)
        // 14 applications cycle through six statuses: the first two statuses get three, the rest two
        assertEquals(
          ApplicationStatus.values.toList.map(own.head.byStatus),
          List(3, 3, 2, 2, 2, 2)
        )
        assertEquals(asStranger, Right(Nil))
        assertEquals(asCandidate, Right(Nil))
        assertEquals(unknown, Right(Nil))
      }
    }
  }

  /** The real singleton Admin, provisioned through the startup seed as in production, read back for a read scope. */
  private def seededAdmin(
      fixture: MongoAccessEvaluationSupport.Fixture,
      users: MongoUserRepository
  ): IO[User] =
    for {
      permits <- Semaphore[IO](1)
      _ <- Argon2PasswordHasher.create(PasswordHashConfig(1, 8192, 1), permits).flatMap { hasher =>
        MongoAdminSeed.run(
          fixture.database,
          users,
          hasher,
          AdminSeedConfig(true, Some("Seed Admin"), Some("original-password"))
        )
      }
      found <- MongoAccessEvaluationSupport.command(
        fixture.database,
        new Document("find", MongoCollections.Users)
          .append("filter", new Document(MongoFields.AdminSingletonKey, "singleton-admin"))
          .append("limit", 1)
      )
      id = UserId(
        UUID.fromString(
          found
            .get("cursor", classOf[Document])
            .getList("firstBatch", classOf[Document])
            .get(0)
            .getString(MongoFields.Id)
        )
      )
      admin <- users.find(id).value.flatMap(read => IO.fromEither(read.leftMap(failed)))
    } yield admin.getOrElse(throw new AssertionError("the seeded Admin is missing"))

  test("AC-14.12 the seeded singleton Admin reads the counts of every recruiter's jobs") {
    mongoResource.use { fixture =>
      val first = user("First owner", UserRole.Recruiter)
      val second = user("Second owner", UserRole.Recruiter)
      val firstJob = job(first.id)
      val secondJob = job(second.id)
      val (users, applications) = repositories(fixture)
      for {
        admin <- seedAfter(
          fixture,
          List(first, second),
          List(firstJob, secondJob),
          applicationsFor(firstJob, 5) ++ applicationsFor(secondJob, 2)
        )(seededAdmin(fixture, users))
        scope <- scopeOf(admin, users)
        counts <- applications.applicationCounts(scope, List(firstJob.id, secondJob.id)).value
      } yield {
        assertEquals(admin.role, UserRole.Admin)
        assertEquals(
          counts.map(_.map(c => c.jobId -> c.total).toMap),
          Right(Map(firstJob.id -> 5, secondJob.id -> 2))
        )
      }
    }
  }

  test("AC-14.9 an unreadable stored status fails the counts closed instead of dropping the application") {
    mongoResource.use { fixture =>
      val owner = user("Corrupt data owner", UserRole.Recruiter)
      val owned = job(owner.id)
      val (users, applications) = repositories(fixture)
      val corrupt = MongoHiringCodecs.application(applicationsFor(owned, 1).head)
      corrupt.put(MongoFields.Status, "NotAStatus")
      for {
        _ <- seed(fixture, List(owner), List(owned), Nil)
        _ <- MongoRepositoryTestSupport.insertUnvalidated(fixture.database, MongoCollections.Applications, corrupt)
        scope <- scopeOf(owner, users)
        counts <- applications.applicationCounts(scope, List(owned.id)).value
      } yield assertEquals(counts, Left(com.example.graphQL.cats.service.RepositoryError.InvalidStoredData))
    }
  }

  test("AC-14.9 an owner whose account is gone after the scope was granted reads no counts") {
    mongoResource.use { fixture =>
      val owner = user("Soon deleted", UserRole.Recruiter)
      val owned = job(owner.id)
      val (users, applications) = repositories(fixture)
      for {
        _ <- seed(fixture, List(owner), List(owned), applicationsFor(owned, 4))
        scope <- scopeOf(owner, users)
        before <- applications.applicationCounts(scope, List(owned.id)).value
        collection <- MongoRepositoryTestSupport.collection(fixture.database, MongoCollections.Users)
        // the collection validator forbids a half-deleted user document, so the account simply disappears
        _ <- collection.deleteOne(com.mongodb.client.model.Filters.eq(MongoFields.Id, owner.id.value.toString))
        after <- applications.applicationCounts(scope, List(owned.id)).value
      } yield {
        assertEquals(before.map(_.map(_.total)), Right(List(4)))
        assertEquals(after, Right(Nil))
      }
    }
  }

  test("AC-14.10 a page of one hundred jobs is one aggregate that examines no application document") {
    mongoResource.use { fixture =>
      val owner = user("Dashboard owner", UserRole.Recruiter)
      val jobs = List.fill(jobCount)(job(owner.id))
      val (users, applications) = repositories(fixture)
      for {
        _ <- seed(fixture, List(owner), jobs, jobs.flatMap(applicationsFor(_, applicationsPerJob)))
        scope <- scopeOf(owner, users)
        _ <- fixture.commands.clear
        counts <- applications.applicationCounts(scope, jobs.map(_.id)).value
        issued <- fixture.commands.snapshot.map(_.filter(_.getFirstKey == "aggregate"))
        explained <- explainOf(fixture, issued.head)
      } yield {
        assertEquals(counts.map(_.size), Right(jobCount))
        assertEquals(counts.map(_.forall(_.total == applicationsPerJob)), Right(true))
        assertEquals(issued.size, 1)
        // the lookup is answered from the (jobId, status, ...) index alone
        assertEquals(metric(explained, "totalDocsExamined").sum, 0L)
      }
    }
  }

  // Opt-in: HIRING_JOB_COUNTS_MEASURE=1 prints the before/after dashboard measurement recorded in the spec.
  test("AC-14.10 measurement: one counts read against one list read per job on a dashboard page".tag(measurement)) {
    mongoResource.use { fixture =>
      val owner = user("Dashboard owner", UserRole.Recruiter)
      val jobs = List.fill(jobCount)(job(owner.id))
      val (users, applications) = repositories(fixture)
      val page = ApplicationPageRequest(None, None, PageSize.fromInt(5).toOption.get)
      def millis(task: IO[Unit]): IO[Double] =
        IO.monotonic.flatMap(start => task *> IO.monotonic.map(end => (end - start).toNanos / 1e6))
      def nested(scope: HiringReadScope): IO[Unit] =
        jobs.traverse_(target =>
          applications.findByJob(scope, target.id, page).value.flatMap(read => IO.fromEither(read.leftMap(failed)))
        )
      def batched(scope: HiringReadScope): IO[Unit] =
        applications
          .applicationCounts(scope, jobs.map(_.id))
          .value
          .flatMap(read => IO.fromEither(read.leftMap(failed)))
          .void
      def aggregates: IO[Int] = fixture.commands.snapshot.map(_.count(_.getFirstKey == "aggregate"))
      def summary(label: String, runs: List[Double], reads: Int): String = {
        val sorted = runs.sorted
        f"$label: aggregates/run=$reads median=${sorted(sorted.size / 2)}%.1fms p95=${sorted(math.min(sorted.size - 1, (sorted.size * 0.95).toInt))}%.1fms"
      }
      for {
        _ <- seed(fixture, List(owner), jobs, jobs.flatMap(applicationsFor(_, applicationsPerJob)))
        scope <- scopeOf(owner, users)
        _ <- (nested(scope), batched(scope)).tupled.replicateA_(3) // warm-up
        _ <- fixture.commands.clear
        nestedMs <- (1 to 10).toList.traverse(_ => millis(nested(scope)))
        nestedReads <- aggregates
        _ <- fixture.commands.clear
        batchedMs <- (1 to 10).toList.traverse(_ => millis(batched(scope)))
        batchedReads <- aggregates
      } yield println(
        s"""JOB-COUNTS-MEASUREMENT jobs=$jobCount applicationsPerJob=$applicationsPerJob runs=10
           |JOB-COUNTS-MEASUREMENT ${summary("nested list reads", nestedMs, nestedReads / 10)}
           |JOB-COUNTS-MEASUREMENT ${summary("batched counts", batchedMs, batchedReads / 10)}""".stripMargin
      )
    }
  }

  override def munitTestTransforms: List[TestTransform] =
    super.munitTestTransforms :+ new TestTransform(
      "measurement is opt-in",
      test =>
        if (test.tags.contains(measurement) && sys.env.get("HIRING_JOB_COUNTS_MEASURE").isEmpty) test.tag(munit.Ignore)
        else test
    )

  private def explainOf(fixture: MongoAccessEvaluationSupport.Fixture, observed: BsonDocument): IO[Json] = {
    val command = Document.parse(observed.toJson)
    List("$db", "lsid", "txnNumber", "startTransaction", "autocommit", "readConcern", "writeConcern", "maxTimeMS")
      .foreach(command.remove)
    MongoAccessEvaluationSupport
      .command(fixture.database, new Document("explain", command).append("verbosity", "executionStats"))
      .map(result => parse(result.toJson).toOption.getOrElse(Json.Null))
  }

  private def metric(json: Json, key: String): List[Long] =
    json.asObject.toList.flatMap { fields =>
      fields(key).flatMap(_.asNumber).flatMap(_.toLong).toList ++ fields.values.toList.flatMap(metric(_, key))
    } ++ json.asArray.toList.flatMap(_.toList.flatMap(metric(_, key)))
}
