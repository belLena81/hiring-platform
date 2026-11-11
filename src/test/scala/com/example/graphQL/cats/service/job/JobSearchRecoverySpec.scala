package com.example.graphQL.cats.service.job

import cats.effect.{IO, Ref}
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.{AccountStatus, Job, User, UserRole}
import com.example.graphQL.cats.domain.pagination.{JobCursor, JobPageRequest, PageSize}
import com.example.graphQL.cats.service.{ActorContext, Diagnostics, LogEvent, LogField, RepositoryError}
import com.example.graphQL.cats.service.mutation.TestIdempotency
import com.example.graphQL.cats.service.search.TestEmbeddingWorkPublisher
import com.example.graphQL.cats.service.ServiceFixtures.*
import com.example.graphQL.cats.service.port.{JobRepository, RepositoryF}
import com.example.graphQL.cats.service.search.{JobSearchFilter, OpenJobsPage, RelaxedFilter}
import java.lang.reflect.{InvocationTargetException, Proxy}
import java.util.UUID
import munit.CatsEffectSuite

/** GDF-01: an empty filtered job search is rerun with one filter relaxed at a time, and says which. */
final class JobSearchRecoverySpec extends CatsEffectSuite {
  private val page = JobPageRequest(None, None, PageSize.fromInt(10).toOption.get)
  private val candidateActor = ActorContext(candidateId, UserRole.Candidate)

  private def jobWith(index: Int, skills: Set[String]): Job =
    openJob.copy(id = JobId(UUID.fromString(f"00000000-0000-0000-0000-0000000001$index%02d")), skills = skills)

  private val scalaAndKafka = jobWith(1, Set("Scala", "Kafka"))
  private val scalaOnly = jobWith(2, Set("Scala"))

  private final class Fixture(
      val service: JobService[IO],
      val searches: Ref[IO, Vector[JobPageRequest]],
      val admissions: Ref[IO, Int],
      val logged: Ref[IO, Vector[(LogEvent, Map[LogField, String])]]
  ) {
    def admit(answer: Boolean): IO[Boolean] = admissions.update(_ + 1).as(answer)
  }

  private def fixture(
      stored: List[Job],
      users: List[User] = List(candidate),
      wrap: (JobRepository[IO], Ref[IO, Int]) => JobRepository[IO] = (repository, _) => repository
  ): IO[Fixture] =
    for {
      usersRef <- Ref.of[IO, Map[UserId, User]](users.map(user => user.id -> user).toMap)
      jobsRef <- Ref.of[IO, Map[JobId, Job]](stored.map(job => job.id -> job).toMap)
      searches <- Ref.of[IO, Vector[JobPageRequest]](Vector.empty)
      admissions <- Ref.of[IO, Int](0)
      logged <- Ref.of[IO, Vector[(LogEvent, Map[LogField, String])]](Vector.empty)
      searchCount <- Ref.of[IO, Int](0)
      jobs = wrap(new InMemoryJobs(jobsRef, pageRequests = Some(searches)), searchCount)
      diagnostics = new Diagnostics[IO] {
        def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]): IO[Unit] =
          logged.update(_ :+ ((event, fields)))
      }
      service = new JobService[IO](
        new InMemoryUsers(usersRef),
        jobs,
        TestEmbeddingWorkPublisher.noop,
        TestIdempotency.noop,
        diagnostics,
        cats.effect.Clock[IO],
        cats.effect.std.UUIDGen[IO]
      )
    } yield new Fixture(service, searches, admissions, logged)

  private def search(
      f: Fixture,
      filter: JobSearchFilter,
      admit: Boolean = true,
      request: JobPageRequest = page,
      actor: ActorContext = candidateActor
  ) = f.service.searchOpenJobs(actor, filter, request, f.admit(admit)).value

  test("GDF-01.1 Ana's skills Scala and Kafka in a window that returns nothing relax createdAfter and say so") {
    val window = later // the job was created at `now`, before the bound
    val filter = JobSearchFilter(None, Set("Scala", "Kafka"), Some(window))
    for {
      f <- fixture(List(scalaAndKafka))
      result <- search(f, filter)
      searches <- f.searches.get
    } yield {
      assertEquals(result, Right(OpenJobsPage(List(scalaAndKafka), List(RelaxedFilter.CreatedAfterDropped(window)))))
      assertEquals(searches.size, 2)
    }
  }

  test("GDF-01.1 when dropping createdAfter is not enough the skills are widened to any-of, one filter at a time") {
    val filter = JobSearchFilter(None, Set("Scala", "Kafka"), Some(now))
    for {
      f <- fixture(List(scalaOnly))
      result <- search(f, filter)
      searches <- f.searches.get
      admissions <- f.admissions.get
    } yield {
      assertEquals(
        result,
        Right(OpenJobsPage(List(scalaOnly), List(RelaxedFilter.SkillsAllOfToAnyOf(Set("Scala", "Kafka")))))
      )
      assertEquals(searches.size, 3)
      assertEquals(admissions, 2)
    }
  }

  test("GDF-01.1 the relaxation order is deterministic: createdAfter is tried before skills") {
    // Both relaxations would find a job; the first non-empty page wins and it is the createdAfter one.
    val filter = JobSearchFilter(None, Set("Scala", "Kafka"), Some(later))
    for {
      f <- fixture(List(scalaAndKafka, scalaOnly))
      result <- search(f, filter)
    } yield assertEquals(
      result.map(_.relaxedFilters),
      Right(List(RelaxedFilter.CreatedAfterDropped(later)))
    )
  }

  test("GDF-01.2 a non-empty page is returned as it is: nothing is relaxed and no extra search is admitted") {
    for {
      f <- fixture(List(scalaAndKafka, scalaOnly))
      result <- search(f, JobSearchFilter(None, Set("Scala"), Some(now)))
      searches <- f.searches.get
      admissions <- f.admissions.get
    } yield {
      assertEquals(result.map(_.relaxedFilters), Right(Nil))
      assertEquals(result.map(_.jobs.size), Right(2))
      assertEquals(searches.size, 1)
      assertEquals(admissions, 0)
    }
  }

  test("GDF-01.2 an unfiltered search that finds nothing stays empty after a single search") {
    for {
      f <- fixture(Nil)
      result <- search(f, JobSearchFilter(None, Set.empty, None))
      searches <- f.searches.get
    } yield {
      assertEquals(result, Right(OpenJobsPage(Nil, Nil)))
      assertEquals(searches.size, 1)
    }
  }

  test("GDF-01.2 a city that matches nothing is never relaxed") {
    for {
      f <- fixture(List(scalaAndKafka))
      result <- search(f, JobSearchFilter(Some("Atlantis"), Set.empty, None))
      searches <- f.searches.get
    } yield {
      assertEquals(result, Right(OpenJobsPage(Nil, Nil)))
      assertEquals(searches.size, 1)
    }
  }

  test("GDF-01.2 a later page (a cursor was sent) is never relaxed") {
    val cursor = Some(JobCursor(later.plusSeconds(1), scalaOnly.id))
    for {
      f <- fixture(List(scalaOnly))
      result <- search(
        f,
        JobSearchFilter(None, Set("Scala", "Kafka"), Some(later)),
        request = page.copy(cursor = cursor)
      )
      searches <- f.searches.get
    } yield {
      assertEquals(result, Right(OpenJobsPage(Nil, Nil)))
      assertEquals(searches.size, 1)
    }
  }

  test("GDF-01.3 a query still empty after both relaxations returns an empty page after at most two extra searches") {
    val filter = JobSearchFilter(None, Set("Scala", "Rust"), Some(later))
    for {
      f <- fixture(List(scalaAndKafka, scalaOnly))
      result <- search(f, filter)
      searches <- f.searches.get
      admissions <- f.admissions.get
    } yield {
      assertEquals(result, Right(OpenJobsPage(Nil, Nil)))
      assertEquals(searches.size, 1 + 2)
      assertEquals(admissions, 2) // each extra search was admitted (priced) before it ran
    }
  }

  test("GDF-01.3 a refused admission stops the recovery and returns the empty base page") {
    for {
      f <- fixture(List(scalaAndKafka))
      result <- search(f, JobSearchFilter(None, Set("Scala", "Kafka"), Some(later)), admit = false)
      searches <- f.searches.get
      admissions <- f.admissions.get
    } yield {
      assertEquals(result, Right(OpenJobsPage(Nil, Nil)))
      assertEquals(searches.size, 1)
      assertEquals(admissions, 1)
    }
  }

  test("GDF-08 an actor who may not search makes zero searches and zero admissions, relaxation included") {
    val filter = JobSearchFilter(None, Set("Scala", "Kafka"), Some(later))
    for {
      f <- fixture(List(scalaAndKafka), users = Nil)
      unknown <- search(f, filter)
      wrongRole <- search(f, filter, actor = ActorContext(candidateId, UserRole.Recruiter))
      searches <- f.searches.get
      admissions <- f.admissions.get
    } yield {
      assert(unknown.isLeft, unknown.toString)
      assert(wrongRole.isLeft, wrongRole.toString)
      assertEquals(searches.size, 0)
      assertEquals(admissions, 0)
    }
  }

  test("GDF-08 a deleted Candidate account makes zero searches and zero admissions, relaxation included") {
    val filter = JobSearchFilter(None, Set("Scala", "Kafka"), Some(later))
    val deleted = candidate.copy(state = accountStateOf(AccountStatus.Deleted))
    for {
      f <- fixture(List(scalaAndKafka), users = List(deleted))
      result <- search(f, filter)
      searches <- f.searches.get
      admissions <- f.admissions.get
    } yield {
      assert(result.isLeft, result.toString)
      assertEquals(searches.size, 0)
      assertEquals(admissions, 0)
    }
  }

  test("GDF-08 a failing extra search keeps the base result: the empty page is returned, not an error") {
    for {
      f <- fixture(List(scalaAndKafka), wrap = failingFromSecondSearch)
      result <- search(f, JobSearchFilter(None, Set("Scala", "Kafka"), Some(later)))
    } yield assertEquals(result, Right(OpenJobsPage(Nil, Nil)))
  }

  test("GDF-08 a failing admission check keeps the base result") {
    for {
      f <- fixture(List(scalaAndKafka))
      result <- f.service
        .searchOpenJobs(
          candidateActor,
          JobSearchFilter(None, Set("Scala", "Kafka"), Some(later)),
          page,
          IO.raiseError(new IllegalStateException("budget store down"))
        )
        .value
    } yield assertEquals(result, Right(OpenJobsPage(Nil, Nil)))
  }

  test("GDF-08 a failing base search is still an error: only the optional relaxation is forgiven") {
    for {
      f <- fixture(List(scalaAndKafka), wrap = (repository, calls) => failingFrom(repository, calls, 1))
      result <- search(f, JobSearchFilter(None, Set("Scala", "Kafka"), Some(later)))
      admissions <- f.admissions.get
    } yield {
      assert(result.isLeft, result.toString)
      assertEquals(admissions, 0)
    }
  }

  private def assertSafe(logged: Vector[(LogEvent, Map[LogField, String])], code: String): Unit = {
    assertEquals(logged.map(_._1), Vector(LogEvent.JobSearchRelaxationFailed))
    val fields = logged.head._2
    assertEquals(fields.get(LogField.ErrorCode), Some(code))
    assert(fields.keySet.subsetOf(Set(LogField.ErrorCode, LogField.ErrorType, LogField.ErrorLocation)), fields.toString)
    val text = fields.values.mkString(" ")
    List("Atlantis", "Scala", "Kafka", candidateId.value.toString).foreach(secret =>
      assert(!text.contains(secret), s"$secret leaked into $text")
    )
  }

  test("GDF-08 a failing extra search is logged with only a safe code and the base page is kept") {
    for {
      f <- fixture(List(scalaAndKafka), wrap = failingFromSecondSearch)
      result <- search(f, JobSearchFilter(None, Set("Scala", "Kafka"), Some(later)))
      logged <- f.logged.get
    } yield {
      assertEquals(result, Right(OpenJobsPage(Nil, Nil)))
      assertSafe(logged, "UNAVAILABLE")
    }
  }

  test("GDF-08 a throwing admission check is logged with the failure class and location only") {
    for {
      f <- fixture(List(scalaAndKafka))
      result <- f.service
        .searchOpenJobs(
          candidateActor,
          JobSearchFilter(Some("Atlantis"), Set("Scala", "Kafka"), Some(later)),
          page,
          IO.raiseError(new IllegalStateException("budget store down for Scala"))
        )
        .value
      logged <- f.logged.get
    } yield {
      assertEquals(result, Right(OpenJobsPage(Nil, Nil)))
      assertSafe(logged, "RELAXATION_ADMISSION_FAILED")
      assertEquals(logged.head._2.get(LogField.ErrorType), Some("java.lang.IllegalStateException"))
    }
  }

  test("GDF-08 a thrown extra search is logged as a search failure with the class only") {
    for {
      f <- fixture(List(scalaAndKafka), wrap = throwingFromSecondSearch)
      result <- search(f, JobSearchFilter(None, Set("Scala", "Kafka"), Some(later)))
      logged <- f.logged.get
    } yield {
      assertEquals(result, Right(OpenJobsPage(Nil, Nil)))
      assertSafe(logged, "RELAXATION_SEARCH_FAILED")
    }
  }

  test("GDF-08 a refused admission and a successful relaxation log nothing") {
    for {
      refused <- fixture(List(scalaAndKafka))
      _ <- search(refused, JobSearchFilter(None, Set("Scala", "Kafka"), Some(later)), admit = false)
      ok <- fixture(List(scalaAndKafka))
      _ <- search(ok, JobSearchFilter(None, Set("Scala", "Kafka"), Some(later)))
      refusedLog <- refused.logged.get
      okLog <- ok.logged.get
    } yield {
      assertEquals(refusedLog, Vector.empty)
      assertEquals(okLog, Vector.empty)
    }
  }

  /** A repository whose second `findOpen` raises a throwable instead of a typed repository error. */
  private def throwingFromSecondSearch(delegate: JobRepository[IO], calls: Ref[IO, Int]): JobRepository[IO] =
    Proxy
      .newProxyInstance(
        getClass.getClassLoader,
        Array[Class[?]](classOf[JobRepository[?]]),
        (_, method, args) =>
          if (method.getName == "findOpen")
            RepositoryF
              .lift(calls.updateAndGet(_ + 1))
              .flatMap(count =>
                if (count >= 2) RepositoryF.lift(IO.raiseError[List[Job]](new IllegalStateException("driver down")))
                else delegate.findOpen(args(0).asInstanceOf[JobSearchFilter], args(1).asInstanceOf[JobPageRequest])
              )
          else
            try method.invoke(delegate, Option(args).getOrElse(Array.empty[AnyRef])*)
            catch { case error: InvocationTargetException => throw error.getCause }
      )
      .asInstanceOf[JobRepository[IO]]

  private def failingFromSecondSearch(repository: JobRepository[IO], calls: Ref[IO, Int]): JobRepository[IO] =
    failingFrom(repository, calls, 2)

  /** A repository whose `findOpen` fails from the n-th call on; every other operation is the delegate's. */
  private def failingFrom(delegate: JobRepository[IO], calls: Ref[IO, Int], from: Int): JobRepository[IO] =
    Proxy
      .newProxyInstance(
        getClass.getClassLoader,
        Array[Class[?]](classOf[JobRepository[?]]),
        (_, method, args) =>
          if (method.getName == "findOpen")
            RepositoryF
              .lift(calls.updateAndGet(_ + 1))
              .flatMap(count =>
                if (count >= from) RepositoryF.at[IO].raise(RepositoryError.Unavailable)
                else delegate.findOpen(args(0).asInstanceOf[JobSearchFilter], args(1).asInstanceOf[JobPageRequest])
              )
          else
            try method.invoke(delegate, Option(args).getOrElse(Array.empty[AnyRef])*)
            catch { case error: InvocationTargetException => throw error.getCause }
      )
      .asInstanceOf[JobRepository[IO]]
}
