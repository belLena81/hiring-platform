package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.comcast.ip4s.{Host, Port}
import com.example.graphQL.cats.api.auth.JwtActorAuthenticator
import com.example.graphQL.cats.api.graphql.{CursorCodec, HiringGraphQLServices, TestGraphQLSupport}
import com.example.graphQL.cats.api.http.HiringApiRoutes
import com.example.graphQL.cats.config.JwtAuthConfig
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.*
import com.example.graphQL.cats.infrastructure.auth.JwtAccessTokenIssuer
import com.example.graphQL.cats.runtime.HiringPlatformServer
import com.example.graphQL.cats.service.{Diagnostics, HealthService, HiringReadService, ProbeResult}
import com.example.graphQL.cats.service.application.ApplicationService
import com.example.graphQL.cats.service.auth.UserAuthenticationService
import com.example.graphQL.cats.service.job.JobService
import com.example.graphQL.cats.service.mutation.Idempotent
import com.example.graphQL.cats.service.search.EmbeddingWorkPublisher
import io.circe.Json
import io.circe.parser.parse
import org.http4s.{Method, Request, Uri}
import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder
import org.typelevel.ci.CIString
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.Instant
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Loopback HTTP baseline: authenticated structured GraphQL reads backed by disposable MongoDB. */
final class HiringStructuredAccessEvaluationIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes
  private val support = MongoAccessEvaluationSupport
  private val fixtureTime = Instant.parse("2026-10-05T12:00:00Z")
  private val warmup = 8
  private val samples = 100
  private val pageSize = 7
  private val auth = JwtAuthConfig("synthetic-http-evaluation-key-0123456789", "synthetic", "synthetic")
  private val publisher = new EmbeddingWorkPublisher { def wake: IO[Unit] = IO.unit }
  private def user(index: Int, role: UserRole): User =
    User(
      UserId(support.deterministicId(s"http-user:$index")),
      None,
      s"Synthetic $role $index",
      role,
      role match {
        case UserRole.Candidate => Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None)))
        case UserRole.Recruiter => Some(UserProfile.Recruiter(RecruiterProfile("Synthetic organization", None)))
        case UserRole.Admin     => None
      },
      fixtureTime
    )
  private val candidates = (0 until 32).toList.map(user(_, UserRole.Candidate))
  private val recruiters = List(user(32, UserRole.Recruiter), user(33, UserRole.Recruiter))
  private val jobs = (0 until 128).toList.map { index =>
    Job(
      JobId(support.deterministicId(s"http-job:$index")),
      recruiters(index % 2).id,
      s"Synthetic Scala job $index",
      "Synthetic description",
      List("Synthetic requirement"),
      Set("Scala"),
      Location("Cyprus", "Nicosia", remote = false),
      if (index % 4 == 3) JobStatus.Closed else JobStatus.Open,
      fixtureTime.minusSeconds(index.toLong),
      fixtureTime
    )
  }
  private val applications = (0 until 128).toList.map { index =>
    Application(
      ApplicationId(support.deterministicId(s"http-application:$index")),
      candidates(index % 32).id,
      jobs(index / 8).id,
      ApplicationStatus.Created,
      fixtureTime.minusSeconds(index.toLong),
      fixtureTime
    )
  }
  private final case class Operation(
      name: String,
      actor: User,
      query: String,
      field: String,
      expectedCount: Int,
      forbidden: Boolean = false,
      targetP95Millis: Option[Int] = None,
      targetP99Millis: Option[Int] = None
  )
  private final case class Observation(millis: Double, returned: Int, error: Option[String])

  private val operations = List(
    Operation(
      "jobDiscovery",
      candidates.head,
      s"{ jobs(first: $pageSize) { edges { node { id title status recruiter { id } } } pageInfo { hasNextPage endCursor } } }",
      "jobs",
      7,
      targetP95Millis = Some(150),
      targetP99Millis = Some(300)
    ),
    Operation(
      "jobDetail",
      candidates.head,
      s"{ job(id: \"${jobs.head.id.value}\") { id title description status recruiter { id } } }",
      "job",
      1,
      targetP95Millis = Some(100)
    ),
    Operation(
      "candidateApplications",
      candidates.head,
      s"{ myApplications(first: $pageSize) { edges { node { id status job { id title } } } pageInfo { hasNextPage endCursor } } }",
      "myApplications",
      4,
      targetP95Millis = Some(150)
    ),
    Operation(
      "recruiterApplications",
      recruiters.head,
      s"{ jobApplications(jobId: \"${jobs.head.id.value}\", first: $pageSize) { edges { node { id status candidate { id } job { id title } } } pageInfo { hasNextPage endCursor } } }",
      "jobApplications",
      7,
      targetP95Millis = Some(150)
    ),
    Operation(
      "foreignRecruiterApplicationsForbidden",
      recruiters(1),
      s"{ jobApplications(jobId: \"${jobs.head.id.value}\", first: $pageSize) { edges { node { id candidate { id } } } } }",
      "jobApplications",
      0,
      forbidden = true
    ),
    Operation(
      "candidateRecruiterApplicationsForbidden",
      candidates.head,
      s"{ jobApplications(jobId: \"${jobs.head.id.value}\", first: $pageSize) { edges { node { id candidate { id } } } } }",
      "jobApplications",
      0,
      forbidden = true
    )
  )

  private def server(fixture: support.Fixture): Resource[IO, Int] = {
    val users = MongoUserRepository.transactional(
      fixture.database,
      fixture.client,
      MongoEmbeddingWorkEnqueuer.disabled,
      Diagnostics.noop
    )
    val jobRepository = MongoJobRepository.transactional(
      fixture.database,
      fixture.client,
      MongoEmbeddingWorkEnqueuer.disabled,
      Diagnostics.noop
    )
    val applicationRepository =
      MongoApplicationRepository.transactional(fixture.database, fixture.client, Diagnostics.noop)
    val receipts = MongoMutationReceiptRepository.transactional(fixture.database, fixture.client, Diagnostics.noop)
    val idempotent = Idempotent(receipts)
    val services = HiringGraphQLServices(
      HiringReadService(users, jobRepository, applicationRepository),
      JobService.live(users, jobRepository, publisher, idempotent, Diagnostics.noop),
      ApplicationService.live(users, jobRepository, applicationRepository, idempotent),
      CursorCodec.keyFromSecret(auth.hmacSecret),
      TestGraphQLSupport.accountService,
      TestGraphQLSupport.interactions,
      TestGraphQLSupport.searchSessions
    )
    val authenticator = new JwtActorAuthenticator(auth, UserAuthenticationService(users), cats.effect.Clock[IO])
    val probe = MongoDatabaseProbe.fromDatabase(fixture.database, Map.empty, Diagnostics.noop)
    for {
      dependencies <- TestGraphQLSupport.dependencies(services, authenticator.authenticate, IO.pure(ProbeResult.Ready))
      app <- Resource.eval(
        new HiringApiRoutes(new HealthService(probe, Diagnostics.noop), Diagnostics.noop, dependencies)
          .httpApp(HiringApiRoutes.HttpConfig(16, 10.seconds))
      )
      bound <- HiringPlatformServer.resource(
        Host.fromString("127.0.0.1").getOrElse(fail("invalid host")),
        Port.fromInt(0).getOrElse(fail("invalid port")),
        app,
        Diagnostics.noop
      )
    } yield bound.address.getPort
  }

  private def request(client: Client[IO], port: Int, token: String, operation: Operation): IO[Int] = {
    val uri = Uri.fromString(s"http://127.0.0.1:$port/graphql").toOption.getOrElse(fail("invalid fixture URI"))
    val request = Request[IO](Method.POST, uri)
      .withEntity(Json.obj("query" -> Json.fromString(operation.query)).noSpaces)
      .putHeaders(
        org.http4s.Header.Raw(CIString("Content-Type"), "application/json"),
        org.http4s.Header.Raw(CIString("Authorization"), s"Bearer $token")
      )
    client.run(request).use { response =>
      response.as[String].flatMap { payload =>
        IO {
          assertEquals(response.status.code, 200)
          val body = parse(payload).toOption.getOrElse(fail("response was not JSON"))
          val cursor = body.hcursor.downField("data").downField(operation.field)
          if (operation.forbidden) {
            assertEquals(
              body.hcursor.downField("errors").downArray.downField("extensions").get[String]("code"),
              Right("FORBIDDEN")
            )
            assert(cursor.focus.forall(_.isNull), "forbidden result must not contain private data")
            0
          } else {
            assert(!body.hcursor.downField("errors").succeeded, "successful operation returned GraphQL errors")
            val count = if (operation.field == "job") {
              assertEquals(cursor.get[String]("id"), Right(jobs.head.id.value.toString))
              assertEquals(cursor.downField("recruiter").get[String]("id"), Right(jobs.head.recruiterId.value.toString))
              1
            } else {
              val edges = cursor.get[List[Json]]("edges").toOption.getOrElse(fail("missing connection edges"))
              assert(edges.forall(_.hcursor.downField("node").get[String]("id").isRight))
              val allowedIds = operation.field match {
                case "jobs"           => jobs.filter(_.status == JobStatus.Open).map(_.id.value.toString).toSet
                case "myApplications" =>
                  applications.filter(_.candidateId == operation.actor.id).map(_.id.value.toString).toSet
                case "jobApplications" => applications.filter(_.jobId == jobs.head.id).map(_.id.value.toString).toSet
                case _                 => Set.empty[String]
              }
              assert(
                edges.forall(edge => edge.hcursor.downField("node").get[String]("id").exists(allowedIds.contains)),
                "connection returned records outside the actor's authorized fixture scope"
              )
              edges.foreach { edge =>
                val node = edge.hcursor.downField("node")
                if (operation.field == "jobs") {
                  val expected = jobs.find(job => node.get[String]("id").contains(job.id.value.toString))
                  assert(
                    expected.exists(job =>
                      node.downField("recruiter").get[String]("id").contains(job.recruiterId.value.toString)
                    )
                  )
                  assertEquals(node.get[String]("status"), Right("OPEN"))
                } else {
                  val expected =
                    applications.find(application => node.get[String]("id").contains(application.id.value.toString))
                  assert(
                    expected.exists(application =>
                      node.downField("job").get[String]("id").contains(application.jobId.value.toString)
                    )
                  )
                  if (operation.field == "jobApplications")
                    assert(
                      expected.exists(application =>
                        node.downField("candidate").get[String]("id").contains(application.candidateId.value.toString)
                      )
                    )
                }
              }
              assertEquals(
                cursor.downField("pageInfo").get[Boolean]("hasNextPage"),
                Right(operation.field != "myApplications")
              )
              assert(cursor.downField("pageInfo").get[String]("endCursor").exists(_.nonEmpty))
              edges.size
            }
            assertEquals(count, operation.expectedCount)
            count
          }
        }
      }
    }
  }

  private def observe(client: Client[IO], port: Int, operation: Operation, concurrency: Int): IO[Json] = for {
    now <- IO.realTimeInstant
    token = JwtAccessTokenIssuer.issue(auth, operation.actor.id, now).value
    _ <- fs2.Stream
      .range(0, warmup)
      .covary[IO]
      .parEvalMap(concurrency)(_ => request(client, port, token, operation))
      .compile
      .drain
    started <- IO.monotonic
    observations <- fs2.Stream
      .range(0, samples)
      .covary[IO]
      .parEvalMap(concurrency) { _ =>
        for {
          before <- IO.monotonic
          result <- request(client, port, token, operation).attempt
          after <- IO.monotonic
        } yield Observation(
          (after - before).toNanos.toDouble / 1000000d,
          result.toOption.getOrElse(0),
          result.left.toOption.map(_.getClass.getSimpleName)
        )
      }
      .compile
      .toList
    ended <- IO.monotonic
    sorted = observations.map(_.millis).sorted
    errors = observations.count(_.error.nonEmpty)
    report = Json.obj(
      "operation" -> Json.fromString(operation.name),
      "concurrency" -> Json.fromInt(concurrency),
      "elapsedMillis" -> Json.fromDoubleOrNull((ended - started).toNanos.toDouble / 1e6),
      "warmupRequests" -> Json.fromInt(warmup),
      "measuredRequests" -> Json.fromInt(samples),
      "p50Millis" -> Json.fromDoubleOrNull(percentile(sorted, 0.5)),
      "p95Millis" -> Json.fromDoubleOrNull(percentile(sorted, 0.95)),
      "p99Millis" -> Json.fromDoubleOrNull(percentile(sorted, 0.99)),
      "errors" -> Json.fromInt(errors),
      "expectedForbiddenResponses" -> Json.fromInt(if (operation.forbidden) samples else 0),
      "returnedRecords" -> Json.fromInt(observations.map(_.returned).sum),
      "throughputRequestsPerSecond" -> Json.fromDoubleOrNull(samples / (ended - started).toNanos.toDouble * 1e9),
      "referenceP95TargetMillis" -> operation.targetP95Millis.fold(Json.Null)(Json.fromInt),
      "referenceP99TargetMillis" -> operation.targetP99Millis.fold(Json.Null)(Json.fromInt)
    )
    _ <- IO(assertEquals(errors, 0, s"unexpected errors for ${operation.name}"))
  } yield report

  private def percentile(sorted: List[Double], quantile: Double): Double =
    sorted.lift(math.ceil(sorted.size * quantile).toInt - 1).getOrElse(0d)

  private def fingerprint: IO[String] = IO.blocking {
    val digest = java.security.MessageDigest.getInstance("SHA-256")
    List("src/main", "src/test", "src/it").foreach { directory =>
      val stream = Files.walk(Paths.get(directory))
      try
        stream
          .iterator()
          .asScala
          .filter(path => Files.isRegularFile(path) && path.toString.endsWith(".scala"))
          .toList
          .sortBy(_.toString)
          .foreach { path =>
            digest.update(path.toString.getBytes(StandardCharsets.UTF_8)); digest.update(0.toByte)
            digest.update(Files.readAllBytes(path))
          }
      finally stream.close()
    }
    digest.digest().map(value => f"${value & 0xff}%02x").mkString
  }

  test("authenticated structured HTTP reads record bounded local latency with verified access outcomes") {
    mongoResource.use { fixture =>
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- (candidates ++ recruiters).traverse_(value =>
          MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.Users, MongoHiringCodecs.user(value))
        )
        _ <- jobs.traverse_(value =>
          MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.Jobs, MongoHiringCodecs.job(value))
        )
        _ <- applications.traverse_(value =>
          MongoRepositoryTestSupport.insertOne(
            fixture.database,
            MongoCollections.Applications,
            MongoHiringCodecs.application(value)
          )
        )
        started <- IO.realTimeInstant
        mongoVersion <- support
          .command(fixture.database, new org.bson.Document("buildInfo", 1))
          .map(_.getString("version"))
        digest <- fingerprint
        revision <- IO.blocking(scala.sys.process.Process(Seq("git", "rev-parse", "HEAD")).!!.trim)
        rows <- server(fixture).use { port =>
          EmberClientBuilder
            .default[IO]
            .withTimeout(15.seconds)
            .build
            .use(client =>
              List(1, 8).traverse(level => operations.traverse(observe(client, port, _, level))).map(_.flatten)
            )
        }
        ended <- IO.realTimeInstant
        report = Json.obj(
          "startedAt" -> Json.fromString(started.toString),
          "finishedAt" -> Json.fromString(ended.toString),
          "seed" -> Json.fromLong(20261005L),
          "mongoVersion" -> Json.fromString(mongoVersion),
          "elapsedMillis" -> Json.fromLong(java.time.Duration.between(started, ended).toMillis),
          "revision" -> Json.fromString(revision),
          "revisionScope" -> Json.fromString("Git HEAD; checked-out source may contain uncommitted changes"),
          "sourceState" -> Json.fromString("working-tree"),
          "sourceSha256" -> Json.fromString(digest),
          "fingerprintScope" -> Json.fromString(
            "Sorted relative paths and bytes of .scala files under src/main, src/test and src/it; includes tests and harness; excludes build, configuration and documentation"
          ),
          "javaVersion" -> Json.fromString(System.getProperty("java.version")),
          "processors" -> Json.fromInt(Runtime.getRuntime.availableProcessors()),
          "jobs" -> Json.fromInt(jobs.size),
          "applications" -> Json.fromInt(applications.size),
          "candidates" -> Json.fromInt(candidates.size),
          "recruiters" -> Json.fromInt(recruiters.size),
          "pageSize" -> Json.fromInt(pageSize),
          "measurementBoundary" -> Json.fromString(
            "Loopback HTTP client send through full UTF-8 body consumption, JSON decode and access/result assertions; JWT verification, GraphQL, services and real Mongo included"
          ),
          "limitations" -> Json.fromString(
            "Bounded warm local synthetic workload; loopback network only; no browser, external network, Atlas, provider, Kafka workers or production load. Account mutations are not exercised. CPU/memory are not sampled for this HTTP harness; repository baseline records resources separately. Reference targets are not acceptance assertions."
          ),
          "measurements" -> Json.fromValues(rows)
        )
        _ <- IO.blocking {
          val directory = Paths.get(".local/data/mongodb-access-evaluation")
          val _ = Files.createDirectories(directory)
          val path = directory.resolve(s"structured-http-${ended.toEpochMilli}-${java.util.UUID.randomUUID()}.json")
          val _ = Files.writeString(path, report.spaces2, StandardCharsets.UTF_8)
          println(s"Structured HTTP evidence: $path")
        }
      } yield ()
    }
  }
}
