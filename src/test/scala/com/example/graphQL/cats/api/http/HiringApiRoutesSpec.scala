package com.example.graphQL.cats.api.http

import cats.effect.{Deferred, IO, Ref, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.api.auth.JwtActorAuthenticator
import com.example.graphQL.cats.FixedTestClock
import com.example.graphQL.cats.api.graphql.{GraphQLRequest, HiringGraphQLSchema, HiringGraphQLServices, TestGraphQLSupport}
import com.example.graphQL.cats.api.http.HiringApiRoutes
import com.example.graphQL.cats.service.{DatabaseProbe, Diagnostics, HealthService, HiringReadService, LogEvent, LogField, LogFields, ProbeResult}
import com.example.graphQL.cats.service.auth.UserAuthenticationService
import com.example.graphQL.cats.service.application.ApplicationService
import com.example.graphQL.cats.service.job.JobService
import com.example.graphQL.cats.service.ServiceFixtures
import com.example.graphQL.cats.config.{AuthRateLimitConfig, JwtAuthConfig, TrustedProxyConfig}
import com.comcast.ip4s.{Cidr, SocketAddress}
import com.example.graphQL.cats.domain.model.UserRole
import io.circe.Json
import munit.CatsEffectSuite
import org.http4s.*
import org.http4s.circe.*
import org.http4s.headers.{`Cache-Control`, `Retry-After`, `WWW-Authenticate`}
import org.typelevel.ci.CIString
import pdi.jwt.JwtCirce

import scala.concurrent.duration.*

final class HiringApiRoutesSpec extends CatsEffectSuite {
  private val DefaultAdmissionPermits = 16L

  private def app(
      effect: IO[ProbeResult],
      diagnostics: Diagnostics = Diagnostics.noop,
      admissionPermits: Long = DefaultAdmissionPermits
  ): IO[HttpApp[IO]] =
    val probe = new DatabaseProbe { def check: IO[ProbeResult] = effect }
    buildRoutes(new HealthService(probe, diagnostics), diagnostics)
      .flatMap(_.httpApp(HiringApiRoutes.HttpConfig(admissionPermits, 5.seconds)))

  private def buildRoutes(
      service: HealthService,
      diagnostics: Diagnostics,
      hiring: HiringGraphQLServices = TestGraphQLSupport.emptyServices,
      authenticate: Request[IO] => IO[Either[com.example.graphQL.cats.api.auth.AuthFailure, Option[com.example.graphQL.cats.service.ActorContext]]] =
        _ => IO.pure(Right(None)),
      hiringReady: IO[ProbeResult] = IO.pure(ProbeResult.Ready),
      authRateLimit: AuthRateLimitConfig = AuthRateLimitConfig(60, 100, 1000),
      trustedProxy: TrustedProxyConfig = TrustedProxyConfig(Nil),
  ): IO[HiringApiRoutes] =
    TestGraphQLSupport.dependencies(hiring, authenticate, hiringReady, authRateLimit, trustedProxy).allocated
      .map { case (dependencies, _) => new HiringApiRoutes(service, diagnostics, dependencies) }

  private def defaultApp(routes: HiringApiRoutes): IO[HttpApp[IO]] =
    routes.httpApp(HiringApiRoutes.HttpConfig(16L, 5.seconds))

  private def request(query: String): Request[IO] =
    Request[IO](Method.POST, Uri.unsafeFromString("/graphql"))
      .withEntity(Json.obj("query" -> Json.fromString(query)))

  private def request(query: String, operationName: String): Request[IO] =
    Request[IO](Method.POST, Uri.unsafeFromString("/graphql"))
      .withEntity(Json.obj("query" -> Json.fromString(query), "operationName" -> Json.fromString(operationName)))

  private def proxiedRequest(query: String, peer: String, forwarded: String): Request[IO] = {
    proxiedHeaderRequest(query, peer, "Forwarded", forwarded)
  }

  private def proxiedHeaderRequest(query: String, peer: String, headerName: String, value: String): Request[IO] = {
    val socket = if (peer.contains(':')) s"[$peer]:12345" else s"$peer:12345"
    request(query)
      .putHeaders(Header.Raw(CIString(headerName), value))
      .withAttribute(Request.Keys.ConnectionInfo, Request.Connection(
        SocketAddress.fromStringIp("127.0.0.1:8080").get,
        SocketAddress.fromStringIp(socket).get,
        secure = false
      ))
  }

  private val health = request("{ health { status } }")
  private val jwtSecret = "01234567890123456789012345678901"
  private val jwtConfig = JwtAuthConfig(jwtSecret, "hiring-platform-local", "hiring-graphql-api")

  private def requestId(response: Response[IO]): Option[String] =
    response.headers.get(CIString("X-Request-ID")).map(_.head.value)

  private def isUuid(value: String): Boolean = scala.util.Try(java.util.UUID.fromString(value)).isSuccess

  test("authentication repository unavailability returns a sanitized service-unavailable response") {
    for {
      probe = new DatabaseProbe { def check: IO[ProbeResult] = IO.pure(ProbeResult.Ready) }
      http <- buildRoutes(
        new HealthService(probe, Diagnostics.noop),
        Diagnostics.noop,
        authenticate = _ => IO.pure(Left(com.example.graphQL.cats.api.auth.AuthFailure.Unavailable))
      ).flatMap(defaultApp)
      response <- http(request("{ health { status } }"))
      body <- response.as[Json]
    } yield {
      assertEquals(response.status, Status.ServiceUnavailable)
      assertEquals(body.hcursor.downField("errors").downArray.get[String]("message"), Right("Service unavailable"))
    }
  }

  private type DiagnosticRecord = (LogEvent, Option[String], Map[LogField, String])

  private def spanEvent(event: LogEvent): Boolean = event match {
    case LogEvent.SpanSucceeded | LogEvent.SpanFailed | LogEvent.SpanCancelled => true
    case _ => false
  }

  private def capture(records: Ref[IO, Vector[DiagnosticRecord]]): Diagnostics = new Diagnostics {
    def event(event: LogEvent, id: Option[String], fields: => Map[LogField, String]): IO[Unit] =
      records.update(_ :+ ((event, id, fields)))
  }

  private def signedToken(
      userId: com.example.graphQL.cats.domain.model.Identifiers.UserId,
      forgedRole: UserRole
  ): String =
    JwtCirce.encode(
      Json.obj("alg" -> Json.fromString("HS256")),
      Json.obj(
        "sub" -> Json.fromString(userId.value.toString),
        "iss" -> Json.fromString(jwtConfig.issuer),
        "aud" -> Json.fromString(jwtConfig.audience),
        "exp" -> Json.fromLong(ServiceFixtures.now.plusSeconds(300).getEpochSecond),
        "role" -> Json.fromString(forgedRole.toString)
      ),
      jwtSecret
    )

  test("served GraphQL hiring workflow derives ActorContext from a signed bearer token") {
    val mutation =
      s"""mutation {
         |  submitApplication(input: { jobId: "${ServiceFixtures.jobId.value}" }) {
         |    __typename
         |    ... on Application { status }
         |  }
         |}""".stripMargin
    val applicationsQuery =
      """query {
        |  myApplications(first: 10) {
        |    edges { node { status job { id } candidate { id } } }
        | __typename
        |  }
        |}""".stripMargin
    for {
      usersRef <- Ref.of[IO, Map[com.example.graphQL.cats.domain.model.Identifiers.UserId, com.example.graphQL.cats.domain.model.User]](
        Map(ServiceFixtures.candidateId -> ServiceFixtures.candidate, ServiceFixtures.recruiterId -> ServiceFixtures.recruiter)
      )
      jobsRef <- Ref.of[IO, Map[com.example.graphQL.cats.domain.model.Identifiers.JobId, com.example.graphQL.cats.domain.model.Job]](
        Map(ServiceFixtures.jobId -> ServiceFixtures.openJob)
      )
      applicationsRef <- Ref.of[IO, Map[com.example.graphQL.cats.domain.model.Identifiers.ApplicationId, com.example.graphQL.cats.domain.model.Application]](Map.empty)
      eventsRef <- Ref.of[IO, Vector[com.example.graphQL.cats.domain.model.ApplicationEvent]](Vector.empty)
      createErrorRef <- Ref.of[IO, Option[com.example.graphQL.cats.repository.protocol.RepositoryError]](None)
      probe = new DatabaseProbe { def check: IO[ProbeResult] = IO.pure(ProbeResult.Ready) }
      users = ServiceFixtures.InMemoryUsers(usersRef)
      jobs = ServiceFixtures.InMemoryJobs(jobsRef)
      applications = ServiceFixtures.InMemoryApplications(applicationsRef, eventsRef, createErrorRef)
      services = HiringGraphQLServices(HiringReadService(users, jobs, applications), JobService(users, jobs), ApplicationService(users, jobs, applications), TestGraphQLSupport.cursorKey, TestGraphQLSupport.accountService)
      authenticator = JwtActorAuthenticator(jwtConfig, UserAuthenticationService[IO](users), FixedTestClock.at(ServiceFixtures.now))
      http <- buildRoutes(new HealthService(probe, Diagnostics.noop), Diagnostics.noop, hiring = services,
        authenticate = authenticator.authenticateDetailed).flatMap(defaultApp)
      token = signedToken(ServiceFixtures.candidateId, UserRole.Admin)
      submitted <- http(request(mutation).putHeaders(Header.Raw(CIString("Authorization"), s"Bearer $token"))).flatMap(_.as[Json])
      listed <- http(request(applicationsQuery).putHeaders(Header.Raw(CIString("Authorization"), s"Bearer $token"))).flatMap(_.as[Json])
    } yield {
      val payload = submitted.hcursor.downField("data").downField("submitApplication")
      assertEquals(payload.get[String]("status"), Right("CREATED"))
      val edge = listed.hcursor.downField("data").downField("myApplications").downField("edges").downArray
      assertEquals(edge.downField("node").get[String]("status"), Right("CREATED"))
      assertEquals(edge.downField("node").downField("job").get[String]("id"), Right(ServiceFixtures.jobId.value.toString))
      assertEquals(edge.downField("node").downField("candidate").get[String]("id"), Right(ServiceFixtures.candidateId.value.toString))
      assert(!submitted.noSpaces.contains("Admin"))
    }
  }

  test("served GraphQL hiring setup failure returns typed service-not-ready instead of unauthorized") {
    val mutation =
      s"""mutation {
         |  submitApplication(input: { jobId: "${ServiceFixtures.jobId.value}" }) {
         |    __typename
         | __typename
         |  }
         |}""".stripMargin
    for {
      usersRef <- Ref.of[IO, Map[com.example.graphQL.cats.domain.model.Identifiers.UserId, com.example.graphQL.cats.domain.model.User]](
        Map(ServiceFixtures.candidateId -> ServiceFixtures.candidate)
      )
      jobsRef <- Ref.of[IO, Map[com.example.graphQL.cats.domain.model.Identifiers.JobId, com.example.graphQL.cats.domain.model.Job]](
        Map(ServiceFixtures.jobId -> ServiceFixtures.openJob)
      )
      applicationsRef <- Ref.of[IO, Map[com.example.graphQL.cats.domain.model.Identifiers.ApplicationId, com.example.graphQL.cats.domain.model.Application]](Map.empty)
      eventsRef <- Ref.of[IO, Vector[com.example.graphQL.cats.domain.model.ApplicationEvent]](Vector.empty)
      createErrorRef <- Ref.of[IO, Option[com.example.graphQL.cats.repository.protocol.RepositoryError]](None)
      probe = new DatabaseProbe { def check: IO[ProbeResult] = IO.pure(ProbeResult.Ready) }
      users = ServiceFixtures.InMemoryUsers(usersRef)
      jobs = ServiceFixtures.InMemoryJobs(jobsRef)
      applications = ServiceFixtures.InMemoryApplications(applicationsRef, eventsRef, createErrorRef)
      services = HiringGraphQLServices(HiringReadService(users, jobs, applications), JobService(users, jobs), ApplicationService(users, jobs, applications), TestGraphQLSupport.cursorKey, TestGraphQLSupport.accountService)
      authenticator = JwtActorAuthenticator(jwtConfig, UserAuthenticationService[IO](users), FixedTestClock.at(ServiceFixtures.now))
      http <- buildRoutes(new HealthService(probe, Diagnostics.noop), Diagnostics.noop, hiring = services,
        authenticate = authenticator.authenticateDetailed, hiringReady = IO.pure(ProbeResult.Unavailable)).flatMap(defaultApp)
      token = signedToken(ServiceFixtures.candidateId, UserRole.Candidate)
      response <- http(request(mutation).putHeaders(Header.Raw(CIString("Authorization"), s"Bearer $token")))
      body <- response.as[Json]
      applicationsAfter <- applicationsRef.get
    } yield {
      assertEquals(response.status, Status.Ok)
      val error = body.hcursor.downField("errors").downArray
      assertEquals(error.downField("extensions").get[String]("code"), Right("SERVICE_NOT_READY"))
      assertEquals(error.get[String]("message"), Right("Service not ready"))
      assert(!body.noSpaces.contains("UNAUTHORIZED"))
      assertEquals(applicationsAfter, Map.empty)
    }
  }

  test("authenticated public GraphQL health and readiness bypass hiring setup readiness") {
    for {
      usersRef <- Ref.of[IO, Map[com.example.graphQL.cats.domain.model.Identifiers.UserId, com.example.graphQL.cats.domain.model.User]](
        Map(ServiceFixtures.candidateId -> ServiceFixtures.candidate)
      )
      jobsRef <- Ref.of[IO, Map[com.example.graphQL.cats.domain.model.Identifiers.JobId, com.example.graphQL.cats.domain.model.Job]](Map.empty)
      applicationsRef <- Ref.of[IO, Map[com.example.graphQL.cats.domain.model.Identifiers.ApplicationId, com.example.graphQL.cats.domain.model.Application]](Map.empty)
      eventsRef <- Ref.of[IO, Vector[com.example.graphQL.cats.domain.model.ApplicationEvent]](Vector.empty)
      createErrorRef <- Ref.of[IO, Option[com.example.graphQL.cats.repository.protocol.RepositoryError]](None)
      setupChecks <- Ref.of[IO, Int](0)
      probe = new DatabaseProbe { def check: IO[ProbeResult] = IO.pure(ProbeResult.Unavailable) }
      users = ServiceFixtures.InMemoryUsers(usersRef)
      jobs = ServiceFixtures.InMemoryJobs(jobsRef)
      applications = ServiceFixtures.InMemoryApplications(applicationsRef, eventsRef, createErrorRef)
      services = HiringGraphQLServices(HiringReadService(users, jobs, applications), JobService(users, jobs), ApplicationService(users, jobs, applications), TestGraphQLSupport.cursorKey, TestGraphQLSupport.accountService)
      authenticator = JwtActorAuthenticator(jwtConfig, UserAuthenticationService[IO](users), FixedTestClock.at(ServiceFixtures.now))
      http <- buildRoutes(new HealthService(probe, Diagnostics.noop), Diagnostics.noop, hiring = services,
        authenticate = authenticator.authenticateDetailed,
        hiringReady = setupChecks.update(_ + 1).as(ProbeResult.Unavailable)).flatMap(defaultApp)
      token = signedToken(ServiceFixtures.candidateId, UserRole.Candidate)
      authenticatedHealth <- http(health.putHeaders(Header.Raw(CIString("Authorization"), s"Bearer $token")))
      healthBody <- authenticatedHealth.as[Json]
      authenticatedReadiness <- http(request("{ readiness { status } }").putHeaders(Header.Raw(CIString("Authorization"), s"Bearer $token")))
      readinessBody <- authenticatedReadiness.as[Json]
      setupCount <- setupChecks.get
    } yield {
      assertEquals(authenticatedHealth.status, Status.Ok)
      assertEquals(healthBody.hcursor.downField("data").downField("health").get[String]("status"), Right("UP"))
      assertEquals(authenticatedReadiness.status, Status.Ok)
      assertEquals(readinessBody.hcursor.downField("data").downField("readiness").get[String]("status"), Right("NOT_READY"))
      assertEquals(setupCount, 0)
    }
  }

  test("served GraphQL hiring operation without a valid token returns typed unauthorized payload") {
    val mutation =
      s"""mutation {
         |  submitApplication(input: { jobId: "${ServiceFixtures.jobId.value}" }) {
         |    __typename
         | __typename
         |  }
         |}""".stripMargin
    for {
      probe = new DatabaseProbe { def check: IO[ProbeResult] = IO.pure(ProbeResult.Ready) }
      http <- buildRoutes(new HealthService(probe, Diagnostics.noop), Diagnostics.noop).flatMap(defaultApp)
      response <- http(request(mutation))
      body <- response.as[Json]
    } yield {
      assertEquals(response.status, Status.Ok)
      assertEquals(body.hcursor.downField("errors").downArray.downField("extensions").get[String]("code"),
        Right("UNAUTHORIZED"))
    }
  }

  test("login and signup requests are rate limited by remote address and operation") {
    val login =
      """mutation {
        |  login(input: { name: "Candidate", password: "password-password" }) {
        | __typename
        |  }
        |}""".stripMargin
    val signup =
      """mutation {
        |  signUp(input: { name: "Candidate", role: CANDIDATE, password: "password-password", skills: ["Scala"] }) {
        | __typename
        |  }
        |}""".stripMargin

    for {
      records <- Ref.of[IO, Vector[DiagnosticRecord]](Vector.empty)
      probe = new DatabaseProbe { def check: IO[ProbeResult] = IO.pure(ProbeResult.Ready) }
      diagnostics = capture(records)
      http <- buildRoutes(new HealthService(probe, diagnostics), diagnostics,
        authRateLimit = AuthRateLimitConfig(windowSeconds = 60, attempts = 1, maxBuckets = 100)).flatMap(defaultApp)
      firstLogin <- http(request(login))
      limitedLogin <- http(request(login))
      firstSignup <- http(request(signup))
      limitedSignup <- http(request(signup))
      limitedBody <- limitedLogin.as[Json]
      captured <- records.get
    } yield {
      assertEquals(firstLogin.status, Status.Ok)
      assertEquals(limitedLogin.status, Status.TooManyRequests)
      assertEquals(limitedSignup.status, Status.TooManyRequests)
      assertEquals(firstSignup.status, Status.Ok)
      assert(limitedLogin.headers.get[`Retry-After`].exists(_.retry.exists(value => value >= 1 && value <= 60)))
      assertEquals(limitedLogin.contentType.map(_.mediaType), Some(MediaType.application.json))
      assertEquals(limitedBody, Json.obj("errors" -> Json.arr(Json.obj("message" -> Json.fromString("Too many authentication attempts")))))
      assert(captured.exists { case (event, _, fields) =>
        event == LogEvent.RequestRejected && fields.get(LogField.Reason).contains("RATE_LIMITED")
      })
    }
  }

  test("authentication failure returns a typed bearer challenge") {
    for {
      probe = new DatabaseProbe { def check: IO[ProbeResult] = IO.pure(ProbeResult.Ready) }
      http <- buildRoutes(new HealthService(probe, Diagnostics.noop), Diagnostics.noop,
        authenticate = _ => IO.pure(Left(com.example.graphQL.cats.api.auth.AuthFailure.InvalidToken)))
          .flatMap(defaultApp)
      response <- http(health)
    } yield {
      assertEquals(response.status, Status.Unauthorized)
      assertEquals(response.headers.get[`WWW-Authenticate`].map(_.values.head),
        Some(Challenge("Bearer", "hiring")))
    }
  }

  test("responses include the typed shared security headers") {
    for {
      http <- app(IO.pure(ProbeResult.Ready))
      response <- http(health)
    } yield {
      assertEquals(response.headers.get[`Cache-Control`].map(_.values.toList), Some(List(CacheDirective.`no-store`)))
      assertEquals(response.headers.get(CIString("X-Content-Type-Options")).map(_.head.value), Some("nosniff"))
      assertEquals(response.headers.get(CIString("Content-Security-Policy")).map(_.head.value),
        Some("default-src 'none'; frame-ancestors 'none'; base-uri 'none'"))
    }
  }

  test("account admission rejects multiple selected sensitive root fields before authentication or execution") {
    val repeatedAliases =
      """mutation {
        |  first: login(input: { name: "Candidate", password: "password-password" }) { __typename }
        |  second: login(input: { name: "Candidate", password: "password-password" }) { __typename }
        |}""".stripMargin
    val mixedOperations =
      """mutation {
        |  login(input: { name: "Candidate", password: "password-password" }) { __typename }
        |  signUp(input: { name: "Candidate", role: CANDIDATE, password: "password-password", skills: ["Scala"] }) { __typename }
        |}""".stripMargin
    val cyclicFragments =
      """mutation {
        |  ...First
        |}
        |fragment First on Mutation {
        |  first: login(input: { name: "Candidate", password: "password-password" }) { __typename }
        |  ...Second
        |}
        |fragment Second on Mutation {
        |  second: signUp(input: { name: "Candidate", role: CANDIDATE, password: "password-password", skills: ["Scala"] }) { __typename }
        |  ...First
        |}""".stripMargin

    for {
      authenticationCalls <- Ref.of[IO, Int](0)
      probe = new DatabaseProbe { def check: IO[ProbeResult] = IO.pure(ProbeResult.Ready) }
      http <- buildRoutes(new HealthService(probe, Diagnostics.noop), Diagnostics.noop,
        authenticate = _ => authenticationCalls.updateAndGet(_ + 1).as(Right(None))).flatMap(defaultApp)
      aliasResponse <- http(request(repeatedAliases))
      mixedResponse <- http(request(mixedOperations))
      cyclicResponse <- http(request(cyclicFragments))
      aliasBody <- aliasResponse.as[Json]
      calls <- authenticationCalls.get
    } yield {
      assertEquals(aliasResponse.status, Status.BadRequest)
      assertEquals(mixedResponse.status, Status.BadRequest)
      assertEquals(cyclicResponse.status, Status.BadRequest)
      assertEquals(aliasBody, Json.obj("errors" -> Json.arr(Json.obj("message" -> Json.fromString("Invalid GraphQL query")))))
      assertEquals(calls, 3)
    }
  }

  test("account admission only inspects the selected operation") {
    val document =
      """mutation UnselectedSensitive {
        |  first: login(input: { name: "Candidate", password: "password-password" }) { __typename }
        |  second: login(input: { name: "Candidate", password: "password-password" }) { __typename }
        |}
        |mutation Harmless {
        |  createJob(input: {
        |    title: "Platform developer"
        |    description: "Build platform services"
        |    requirements: ["Scala"]
        |    skills: ["Scala"]
        |    country: "Cyprus"
        |    city: "Nicosia"
        |    remote: false
        |  }) { __typename }
        |}
        |mutation SelectedSingle {
        |  login(input: { name: "Candidate", password: "password-password" }) { __typename }
        |}""".stripMargin

    for {
      probe = new DatabaseProbe { def check: IO[ProbeResult] = IO.pure(ProbeResult.Ready) }
      http <- buildRoutes(new HealthService(probe, Diagnostics.noop), Diagnostics.noop,
        authRateLimit = AuthRateLimitConfig(windowSeconds = 60, attempts = 1, maxBuckets = 100)).flatMap(defaultApp)
      harmlessFirst <- http(request(document, "Harmless"))
      harmlessSecond <- http(request(document, "Harmless"))
      sensitiveFirst <- http(request(document, "SelectedSingle"))
      sensitiveSecond <- http(request(document, "SelectedSingle"))
    } yield {
      assertEquals(harmlessFirst.status, Status.Ok)
      assertEquals(harmlessSecond.status, Status.Ok)
      assertEquals(sensitiveFirst.status, Status.Ok)
      assertEquals(sensitiveSecond.status, Status.TooManyRequests)
    }
  }

  test("auth rate limiting resolves named and inline fragments") {
    val namedLogin =
      """mutation {
        |  ...LoginFragment
        |}
        |fragment LoginFragment on Mutation {
        |  login(input: { name: "Candidate", password: "password-password" }) {
        | __typename
        |  }
        |}""".stripMargin
    val inlineSignup =
      """mutation {
        |  ... on Mutation {
        |    signUp(input: { name: "Candidate", role: CANDIDATE, password: "password-password", skills: ["Scala"] }) {
        | __typename
        |    }
        |  }
        |}""".stripMargin
    val aliasedBootstrap =
      """mutation {
        |  ...BootstrapFragment
        |}
        |fragment BootstrapFragment on Mutation {
        |  firstAdmin: bootstrapAdmin(input: { name: "Admin", password: "password-password" }) {
        | __typename
        |  }
        |}""".stripMargin

    for {
      probe = new DatabaseProbe { def check: IO[ProbeResult] = IO.pure(ProbeResult.Ready) }
      http <- buildRoutes(new HealthService(probe, Diagnostics.noop), Diagnostics.noop,
        authRateLimit = AuthRateLimitConfig(windowSeconds = 60, attempts = 1, maxBuckets = 100)).flatMap(defaultApp)
      firstNamedLogin <- http(request(namedLogin))
      limitedNamedLogin <- http(request(namedLogin))
      firstInlineSignup <- http(request(inlineSignup))
      limitedInlineSignup <- http(request(inlineSignup))
      firstBootstrap <- http(request(aliasedBootstrap))
      limitedBootstrap <- http(request(aliasedBootstrap))
    } yield {
      assertEquals(firstNamedLogin.status, Status.Ok)
      assertEquals(limitedNamedLogin.status, Status.TooManyRequests)
      assertEquals(firstInlineSignup.status, Status.Ok)
      assertEquals(limitedInlineSignup.status, Status.TooManyRequests)
      assertEquals(firstBootstrap.status, Status.Ok)
      assertEquals(limitedBootstrap.status, Status.TooManyRequests)
    }
  }

  test("account admission rejects duplicate fragment expansion without exponential traversal") {
    val fragmentCount = 32
    val fragments = (0 until fragmentCount).map {
      case 0 =>
        """fragment Fragment0 on Mutation {
          |  login(input: { name: "Candidate", password: "password-password" }) { __typename }
          |}""".stripMargin
      case index =>
        s"""fragment Fragment$index on Mutation {
           |  ...Fragment${index - 1}
           |  ...Fragment${index - 1}
           |}""".stripMargin
    }
    val fragmentBomb = s"""mutation { ...Fragment${fragmentCount - 1} }
                            |${fragments.mkString("\n")}""".stripMargin

    for {
      probe = new DatabaseProbe { def check: IO[ProbeResult] = IO.pure(ProbeResult.Ready) }
      http <- buildRoutes(new HealthService(probe, Diagnostics.noop), Diagnostics.noop,
        authRateLimit = AuthRateLimitConfig(windowSeconds = 60, attempts = 1, maxBuckets = 100)).flatMap(defaultApp)
      first <- http(request(fragmentBomb))
      second <- http(request(fragmentBomb))
    } yield {
      assert(fragmentBomb.length < 64 * 1024)
      assertEquals(first.status, Status.BadRequest)
      assertEquals(second.status, Status.BadRequest)
    }
  }

  test("auth rate limiting ignores auth words outside top-level fields") {
    val createJob =
      """mutation {
        |  createJob(input: {
        |    title: "Senior login signUp platform developer"
        |    description: "Build platform services"
        |    requirements: ["Scala"]
        |    skills: ["Scala"]
        |    country: "Cyprus"
        |    city: "Nicosia"
        |    remote: false
        |  }) {
        | __typename
        |  }
        |}""".stripMargin
    val operationNamedLogin =
      """query login {
        |  health { status }
        |}""".stripMargin

    for {
      probe = new DatabaseProbe { def check: IO[ProbeResult] = IO.pure(ProbeResult.Ready) }
      http <- buildRoutes(new HealthService(probe, Diagnostics.noop), Diagnostics.noop,
        authRateLimit = AuthRateLimitConfig(windowSeconds = 60, attempts = 1, maxBuckets = 100)).flatMap(defaultApp)
      firstCreate <- http(request(createJob))
      secondCreate <- http(request(createJob))
      firstNamed <- http(request(operationNamedLogin))
      secondNamed <- http(request(operationNamedLogin))
    } yield {
      assertEquals(firstCreate.status, Status.Ok)
      assertEquals(secondCreate.status, Status.Ok)
      assertEquals(firstNamed.status, Status.Ok)
      assertEquals(secondNamed.status, Status.Ok)
    }
  }

  test("trusted Forwarded client addresses isolate auth rate-limit buckets") {
    val login =
      """mutation {
        |  login(input: { name: "Candidate", password: "password-password" }) { __typename }
        |}""".stripMargin
    val signup =
      """mutation {
        |  signUp(input: { name: "Candidate", role: CANDIDATE, password: "password-password", skills: ["Scala"] }) { __typename }
        |}""".stripMargin
    val trustedProxy = TrustedProxyConfig(List(Cidr.fromString("10.0.0.0/8").get))

    for {
      probe = new DatabaseProbe { def check: IO[ProbeResult] = IO.pure(ProbeResult.Ready) }
      http <- buildRoutes(new HealthService(probe, Diagnostics.noop), Diagnostics.noop,
        authRateLimit = AuthRateLimitConfig(windowSeconds = 60, attempts = 1, maxBuckets = 100),
        trustedProxy = trustedProxy).flatMap(defaultApp)
      firstClientLogin <- http(proxiedRequest(login, "10.0.0.5", "for=203.0.113.10"))
      limitedClientLogin <- http(proxiedRequest(login, "10.0.0.5", "for=203.0.113.10"))
      secondClientLogin <- http(proxiedRequest(login, "10.0.0.5", "for=203.0.113.11"))
      firstClientSignup <- http(proxiedRequest(signup, "10.0.0.5", "for=203.0.113.10"))
      limitedClientSignup <- http(proxiedRequest(signup, "10.0.0.5", "for=203.0.113.10"))
      secondClientSignup <- http(proxiedRequest(signup, "10.0.0.5", "for=203.0.113.11"))
    } yield {
      assertEquals(firstClientLogin.status, Status.Ok)
      assertEquals(limitedClientLogin.status, Status.TooManyRequests)
      assertEquals(secondClientLogin.status, Status.Ok)
      assertEquals(firstClientSignup.status, Status.Ok)
      assertEquals(limitedClientSignup.status, Status.TooManyRequests)
      assertEquals(secondClientSignup.status, Status.Ok)
    }
  }

  test("untrusted peers cannot evade auth rate limits with Forwarded") {
    val login =
      """mutation {
        |  login(input: { name: "Candidate", password: "password-password" }) { __typename }
        |}""".stripMargin

    for {
      probe = new DatabaseProbe { def check: IO[ProbeResult] = IO.pure(ProbeResult.Ready) }
      http <- buildRoutes(new HealthService(probe, Diagnostics.noop), Diagnostics.noop,
        authRateLimit = AuthRateLimitConfig(windowSeconds = 60, attempts = 1, maxBuckets = 100)).flatMap(defaultApp)
      first <- http(proxiedRequest(login, "198.51.100.10", "for=203.0.113.10"))
      limited <- http(proxiedRequest(login, "198.51.100.10", "for=203.0.113.11"))
    } yield {
      assertEquals(first.status, Status.Ok)
      assertEquals(limited.status, Status.TooManyRequests)
    }
  }

  test("trusted X-Forwarded-For client addresses isolate auth rate-limit buckets") {
    val login =
      """mutation {
        |  login(input: { name: "Candidate", password: "password-password" }) { __typename }
        |}""".stripMargin
    val trustedProxy = TrustedProxyConfig(List(Cidr.fromString("10.0.0.0/8").get))

    for {
      probe = new DatabaseProbe { def check: IO[ProbeResult] = IO.pure(ProbeResult.Ready) }
      http <- buildRoutes(new HealthService(probe, Diagnostics.noop), Diagnostics.noop,
        authRateLimit = AuthRateLimitConfig(windowSeconds = 60, attempts = 1, maxBuckets = 100),
        trustedProxy = trustedProxy).flatMap(defaultApp)
      first <- http(proxiedHeaderRequest(login, "10.0.0.5", "X-Forwarded-For", "203.0.113.10"))
      limited <- http(proxiedHeaderRequest(login, "10.0.0.5", "X-Forwarded-For", "203.0.113.10"))
      second <- http(proxiedHeaderRequest(login, "10.0.0.5", "X-Forwarded-For", "203.0.113.11"))
    } yield {
      assertEquals(first.status, Status.Ok)
      assertEquals(limited.status, Status.TooManyRequests)
      assertEquals(second.status, Status.Ok)
    }
  }

  test("validated Sangria field errors retain their response and emit correlated completion diagnostics") {
    for {
      records <- Ref.of[IO, Vector[DiagnosticRecord]](Vector.empty)
      executionRecords <- Ref.of[IO, Vector[DiagnosticRecord]](Vector.empty)
      executionFailure <- Deferred[IO, Unit]
      sink = new Diagnostics {
        def event(event: LogEvent, id: Option[String], fields: => Map[LogField, String]): IO[Unit] =
          records.update(_ :+ ((event, id, fields)))
      }
      executionSink = new Diagnostics {
        def event(event: LogEvent, id: Option[String], fields: => Map[LogField, String]): IO[Unit] =
          executionRecords.update(_ :+ ((event, id, fields))) *>
            (if event == LogEvent.RuntimeFailed then executionFailure.complete(()).void else IO.unit)
      }
      parsed <- IO.fromEither(Json.obj(
        "query" -> Json.fromString("query Selected($include: Boolean!) { readiness @include(if: $include) { status } } # synthetic-field-comment-secret"),
        "operationName" -> Json.fromString("Selected"),
        "variables" -> Json.obj("include" -> Json.True, "password" -> Json.fromString("synthetic-field-value-secret"))
      ).as[GraphQLRequest].leftMap(error => new IllegalArgumentException("Invalid test query", error)))
      execution <- TestGraphQLSupport.context(IO.raiseError[ProbeResult](new IllegalStateException("synthetic-resolver-secret")), diagnostics = executionSink,
        requestId = Some("00000000-0000-0000-0000-000000000901")).use { context =>
        HiringGraphQLSchema.executeInContext(parsed, context).flatTap(_ => executionFailure.get.timeout(5.seconds))
      }
      result <- IO.fromEither(execution.left.map(failure => new AssertionError(s"Expected field error result: $failure")))
      probe = new DatabaseProbe { def check: IO[ProbeResult] = IO.pure(ProbeResult.Ready) }
      id <- IO.randomUUID.map(_.toString)
      response <- TestGraphQLSupport.dependencies().use { dependencies =>
        new GraphQLHttpRoutes(new HealthService(probe, sink), sink, dependencies).completedGraphQL(parsed, result, id)
      }
      body <- response.as[Json]
      captured <- records.get
      executionCaptured <- executionRecords.get
    } yield {
      assertEquals(response.status, Status.Ok)
      assertEquals(body, result)
      val errors = body.hcursor.downField("errors").as[Vector[Json]]
      assertEquals(errors.map(_.size), Right(1))
      val error = errors.toOption.get.head
      assertEquals(error.hcursor.get[String]("message"), Right("Execution failed"))
      assertEquals(error.hcursor.get[Vector[String]]("path"), Right(Vector("readiness")))
      assert(error.hcursor.downField("locations").succeeded)
      assertEquals(captured.map(_._1), Vector(LogEvent.GraphQLCompleted))
      assert(captured.forall(_._2.contains(id)))
      assert(captured.filter(_._1 == LogEvent.GraphQLCompleted).forall(_._3.get(LogField.Outcome).contains("FIELD_ERROR")))
      val runtimeFailures = executionCaptured.filter(_._1 == LogEvent.RuntimeFailed)
      assertEquals(runtimeFailures.map(_._1), Vector(LogEvent.RuntimeFailed))
      assertEquals(runtimeFailures.head._2, Some("00000000-0000-0000-0000-000000000901"))
      assert(runtimeFailures.head._3.get(LogField.ErrorLocation).exists(LogFields.validPublic(LogField.ErrorLocation, _)))
      assert(!body.noSpaces.contains("Request context is closed"))
    }
  }

  test("throwing cancellation sinks preserve cleanup and cancellation") {
    List(true, false).traverse_ { synchronous =>
      val attempts = new java.util.concurrent.atomic.AtomicReference(Vector.empty[(DiagnosticRecord, Boolean)])
      val finalized = new java.util.concurrent.atomic.AtomicBoolean(false)
      val sink = new Diagnostics {
        def event(event: LogEvent, id: Option[String], fields: => Map[LogField, String]): IO[Unit] = {
          def record(): Unit = {
            val _ = attempts.updateAndGet(_ :+ (((event, id, fields), finalized.get())))
          }
          if (synchronous) {
            record()
            throw new IllegalStateException("synthetic-cancellation-secret")
          } else IO.delay(record()) *> IO.raiseError(new IllegalStateException("synthetic-cancellation-secret"))
        }
      }
      for {
        entered <- Deferred[IO, Unit]
        finalizing <- Deferred[IO, Unit]
        finish <- Deferred[IO, Unit]
        probe = new DatabaseProbe { def check: IO[ProbeResult] = IO.pure(ProbeResult.Ready) }
        http <- buildRoutes(new HealthService(probe, sink), sink).flatMap(defaultApp)
        slow = health.withBodyStream(fs2.Stream.eval(entered.complete(()) *> IO.never[Byte])
          .onFinalize(finalizing.complete(()) *> finish.get *> IO.delay(finalized.set(true))))
        _ <- Resource.make(http(slow).start)(fiber => finish.complete(()).void *> fiber.cancel).use { fiber =>
              entered.get.timeout(2.seconds) *> fiber.cancel.background.use { joined =>
                (for {
                  _ <- finalizing.get.timeout(2.seconds)
                  during <- IO(attempts.get())
                  _ <- IO(assert(during.isEmpty))
                  _ <- finish.complete(())
                  _ <- joined.flatMap(_.embedNever)
                  outcome <- fiber.join
                  recovered <- http(Request[IO](Method.GET, Uri.unsafeFromString("/health")))
                  after <- IO(attempts.get())
                } yield {
                  assert(outcome.isCanceled)
                  assertEquals(recovered.status, Status.Ok)
                  assertEquals(after, Vector.empty)
                }).guarantee(finish.complete(()).void)
              }
            }
      } yield ()
    }
  }

  test("HTTP rejection diagnostics remain correlated without synthetic completion events") {
    for {
      records <- Ref.of[IO, Vector[DiagnosticRecord]](Vector.empty)
      http <- app(IO.pure(ProbeResult.Ready), capture(records))
      response <- http(Request[IO](Method.fromString("SYNTHETIC").getOrElse(fail("Invalid test method")),
        Uri.unsafeFromString("/synthetic-path-secret?password=synthetic-query-secret")))
      body <- response.as[Json]
      captured <- records.get
    } yield {
      val id = requestId(response)
      assertEquals(response.status, Status.NotFound)
      assert(id.exists(isUuid))
      assertEquals(body, Json.obj("errors" -> Json.arr(Json.obj("message" -> Json.fromString("Not found")))))
      assert(captured.filterNot(record => spanEvent(record._1)).forall(_._2 == id))
      assert(captured.exists(record => record._1 == LogEvent.RequestRejected &&
        record._3.get(LogField.Reason).contains("NOT_FOUND")))
      assert(!captured.exists(record => record._1.toString == "REQUEST_COMPLETED" || record._1.toString == "REQUEST_CANCELLED"))
      assert(captured.forall(_._3.forall { case (field, value) => LogFields.validPublic(field, value) }))
      assert(!captured.toString.contains("synthetic"))
    }
  }

  test("GraphQL completion shares the HTTP and readiness diagnostic request ID") {
    for {
      records <- Ref.of[IO, Vector[DiagnosticRecord]](Vector.empty)
      http <- app(IO.pure(ProbeResult.Unavailable), capture(records))
      response <- http(request("query LocalCheck { readiness { status } }"))
      captured <- records.get
    } yield {
      val id = requestId(response)
      assertEquals(response.status, Status.Ok)
      assertEquals(captured.filterNot(record => spanEvent(record._1)).map(_._1), Vector(LogEvent.MongoUnavailable, LogEvent.GraphQLCompleted))
      assert(captured.filterNot(record => spanEvent(record._1)).forall(_._2 == id))
      assert(captured.filter(_._1 == LogEvent.GraphQLCompleted).forall { case (_, _, fields) =>
        fields.get(LogField.OperationName).contains("LocalCheck") && fields.get(LogField.Outcome).contains("COMPLETED")
      })
    }
  }

  test("an inbound request ID is not used as application correlation") {
    val inboundId = "00000000-0000-0000-0000-000000000123"
    for {
      records <- Ref.of[IO, Vector[DiagnosticRecord]](Vector.empty)
      http <- app(IO.pure(ProbeResult.Ready), capture(records))
      first <- http(health.putHeaders(Header.Raw(CIString("X-Request-ID"), inboundId)))
      second <- http(health.putHeaders(Header.Raw(CIString("X-Request-ID"), inboundId)))
      captured <- records.get
    } yield {
      assert(requestId(first).exists(isUuid))
      assert(requestId(second).exists(isUuid))
      assert(requestId(first).forall(_ != inboundId))
      assert(requestId(second).forall(_ != inboundId))
      assertEquals(captured.map(_._1), Vector(LogEvent.GraphQLCompleted, LogEvent.GraphQLCompleted))
      assertEquals(captured.map(_._2), Vector(requestId(first), requestId(second)))
    }
  }

  test("HTTP cancellation does not emit duplicate application lifecycle diagnostics") {
    for {
      records <- Ref.of[IO, Vector[DiagnosticRecord]](Vector.empty)
      entered <- Deferred[IO, Unit]
      finalizing <- Deferred[IO, Unit]
      finish <- Deferred[IO, Unit]
      sink = new Diagnostics {
        def event(event: LogEvent, id: Option[String], fields: => Map[LogField, String]): IO[Unit] =
          records.update(_ :+ ((event, id, fields)))
      }
      http <- app(IO.pure(ProbeResult.Ready), sink)
      slow = health.withBodyStream(fs2.Stream.eval(entered.complete(()) *> IO.never[Byte])
        .onFinalize(finalizing.complete(()) *> finish.get))
      _ <- Resource.make(http(slow).start)(fiber => finish.complete(()).void *> fiber.cancel).use { fiber =>
        entered.get.timeout(2.seconds) *> fiber.cancel.background.use { joined =>
          (for {
            _ <- finalizing.get.timeout(2.seconds)
            during <- records.get
            _ <- IO(assertEquals(during, Vector.empty))
            _ <- finish.complete(())
            _ <- joined.flatMap(_.embedNever)
            outcome <- fiber.join
            after <- records.get
          } yield {
            assert(outcome.isCanceled)
            assertEquals(after, Vector.empty)
          }).guarantee(finish.complete(()).void)
        }
      }
    } yield ()
  }

  test("sync and effectful sink failures preserve successful, rejected and unexpected-error responses") {
    List(true, false).traverse_ { synchronous =>
      val sink = new Diagnostics {
        def event(event: LogEvent, id: Option[String], fields: => Map[LogField, String]): IO[Unit] =
          if (synchronous) throw new IllegalStateException("synthetic-diagnostics-secret")
          else IO.raiseError(new IllegalStateException("synthetic-diagnostics-secret"))
      }
      for {
        http <- app(IO.pure(ProbeResult.Unavailable), sink)
        success <- http(request("{ readiness { status } }"))
        rejected <- http(request("{ missingField }"))
        unexpected <- http(health.withBodyStream(fs2.Stream.raiseError[IO](new IllegalStateException("synthetic-body-secret"))))
        bodies <- List(success, rejected, unexpected).traverse(_.as[String])
      } yield {
        assertEquals(List(success.status, rejected.status, unexpected.status), List(Status.Ok, Status.BadRequest, Status.InternalServerError))
        assert(bodies.forall(body => !body.contains("synthetic-")))
      }
    }
  }

  test("Accept negotiates both GraphQL response media types") {
    val cases = List(
      ("application/graphql-response+json", Status.Ok, "application/graphql-response+json"),
      ("application/graphql-response+json;q=0.5", Status.Ok, "application/graphql-response+json"),
      ("application/json, application/graphql-response+json", Status.Ok, "application/graphql-response+json"),
      ("application/json", Status.Ok, "application/json"),
      ("text/html, application/json;q=1", Status.Ok, "application/json")
    )
    app(IO.pure(ProbeResult.Ready)).flatMap { http =>
      cases.traverse_ { case (accept, expected, mediaType) =>
        http(health.putHeaders(Header.Raw(CIString("Accept"), accept)))
          .map { response =>
            assertEquals(response.status, expected, accept)
            assertEquals(response.contentType.map(header => s"${header.mediaType.mainType}/${header.mediaType.subType}"), Some(mediaType), accept)
          }
      }
    }
  }

  test("Accept resolves each offered representation by its most-specific matching range") {
    val cases = List(
      ("application/*;q=1, application/graphql-response+json;q=0", Status.Ok, "application/json"),
      ("*/*;q=1, application/json;q=0", Status.Ok, "application/graphql-response+json"),
      ("application/*;q=1, application/graphql-response+json;q=0, application/json;q=0", Status.NotAcceptable, "application/json")
    )
    app(IO.pure(ProbeResult.Ready)).flatMap { http =>
      cases.traverse_ { case (accept, expectedStatus, expectedMediaType) =>
        http(health.putHeaders(Header.Raw(CIString("Accept"), accept))).map { response =>
          assertEquals(response.status, expectedStatus, accept)
          assertEquals(response.contentType.map(header => s"${header.mediaType.mainType}/${header.mediaType.subType}"),
            Some(expectedMediaType), accept)
        }
      }
    }
  }

  test("malformed Accept segments are ignored independently") {
    val accepted = List(
      "text/html;q=invalid, application/graphql-response+json",
      "application/graphql-response+json, text/html;q=invalid"
    )
    val rejected = List(
      "application/graphql-response+json;q=0;q=1",
      "text/html;q=invalid"
    )
    app(IO.pure(ProbeResult.Ready)).flatMap { http =>
      accepted.traverse_ { accept =>
        http(health.putHeaders(Header.Raw(CIString("Accept"), accept)))
          .map(response => assertEquals(response.status, Status.Ok, accept))
      } *> rejected.traverse_ { accept =>
        http(health.putHeaders(Header.Raw(CIString("Accept"), accept)))
          .flatMap { response =>
            response.as[Json].map { body =>
              assertEquals(response.status, Status.NotAcceptable, accept)
              assertEquals(body.hcursor.downField("errors").downArray.get[String]("message"),
                Right("Expected application/graphql-response+json or application/json"), accept)
            }
          }
      }
    }
  }

  test("readiness bypasses the GraphQL concurrency cap") {
    for {
      graphqlEntered <- Deferred[IO, Unit]
      readinessEntered <- Deferred[IO, Unit]
      calls <- Ref.of[IO, Int](0)
      probe = new DatabaseProbe {
        def check: IO[ProbeResult] = calls.updateAndGet(_ + 1).flatMap {
          case 1 => graphqlEntered.complete(()) *> IO.never[ProbeResult]
          case _ => readinessEntered.complete(()) *> IO.pure(ProbeResult.Ready)
        }
      }
      routes <- buildRoutes(new HealthService(probe, Diagnostics.noop), Diagnostics.noop)
      http <- routes.httpApp(HiringApiRoutes.HttpConfig(1L, 5.seconds))
      graphqlFiber <- http(request("{ readiness { status } }")).start
      _ <- graphqlEntered.get.timeout(2.seconds)
      readiness <- http(Request[IO](Method.GET, Uri.unsafeFromString("/ready")))
      _ <- readinessEntered.get.timeout(2.seconds)
      _ <- graphqlFiber.cancel
      _ <- IO(assertEquals(readiness.status, Status.Ok))
    } yield ()
  }

  test("API schema download matches the served schema without database access") {
    for {
      http <- app(IO.raiseError(new IllegalStateException("Schema must not query MongoDB")))
      response <- http(Request[IO](Method.GET, Uri.unsafeFromString("/schema.graphql")))
      schema <- response.as[String]
    } yield {
      assertEquals(response.status, Status.Ok)
      assertEquals(schema, HiringGraphQLSchema.sdl)
      assert(schema.contains("users(first: Int!"))
    }
  }

  test("health contract, correlation, and no database access") {
    for {
      count <- Ref.of[IO, Int](0)
      http <- app(count.update(_ + 1).as(ProbeResult.Ready))
      response <- http(health)
      body <- response.as[Json]
      calls <- count.get
    } yield {
      assertEquals(response.status, Status.Ok)
      assertEquals(body.hcursor.downField("data").downField("health").get[String]("status"), Right("UP"))
      assertEquals(calls, 0)
      assert(requestId(response).exists(isUuid))
      assert(response.headers.get(CIString("Access-Control-Allow-Origin")).isEmpty)
    }
  }

  test("readiness aliases share a ping, recover, and retain HTTP200 on GraphQL failure") {
    for {
      count <- Ref.of[IO, Int](0)
      result <- Ref.of[IO, ProbeResult](ProbeResult.Unavailable)
      http <- app(count.update(_ + 1) *> result.get)
      response <- http(request("{ first: readiness { status } second: readiness { status } }"))
      body <- response.as[Json]
      firstCount <- count.get
      _ <- result.set(ProbeResult.Ready)
      recovered <- http(Request[IO](Method.GET, Uri.unsafeFromString("/ready")))
      ready <- recovered.as[Json]
    } yield {
      assertEquals(response.status, Status.Ok)
      assertEquals(body.hcursor.downField("data").downField("first").get[String]("status"), Right("NOT_READY"))
      assertEquals(firstCount, 1)
      assertEquals(recovered.status, Status.Ok)
      assertEquals(ready.hcursor.get[String]("status"), Right("READY"))
    }
  }

  test("JSON envelope and GraphQL failures are sanitized, before probe execution") {
    val malformed = List(
      "not-json", "[]", "{\"query\":1}", "{\"query\":\"{ health { status } }\",\"variables\":[]}",
      "{\"query\":\"{ health { status } }\",\"operationName\":2}"
    ).map(body => health.withEntity(body).putHeaders(Header.Raw(CIString("Content-Type"), "application/json")))
    val queries = List("{ privateSecret }", "{ users { email } }", "query A { health { status } } query B { health { status } }",
      "query Test($include: Boolean!) { readiness @include(if: $include) { status } }", "{ health").map(request)
    for {
      count <- Ref.of[IO, Int](0)
      http <- app(count.update(_ + 1).as(ProbeResult.Ready))
      responses <- (malformed ++ queries).traverse(http.run)
      bodies <- responses.traverse(_.as[String])
      calls <- count.get
    } yield {
      assert(responses.forall(_.status == Status.BadRequest))
      assert(bodies.forall(!_.contains("privateSecret")))
      assertEquals(calls, 0)
    }
  }

  test("GraphQL execution rejects excessive depth through Sangria reducers before resolver work") {
    val nestedType =
      List.fill(20)("ofType { ").mkString + "name" + List.fill(20)(" }").mkString
    val query = s"{ __type(name: \"Job\") { fields { type { $nestedType } } } }"
    for {
      calls <- Ref.of[IO, Int](0)
      http <- app(calls.update(_ + 1).as(ProbeResult.Ready))
      response <- http(request(query))
      body <- response.as[String]
      count <- calls.get
    } yield {
      assertEquals(response.status, Status.BadRequest)
      assertEquals(count, 0)
      assert(!body.contains("ofType"))
      assert(!body.contains("Job"))
    }
  }

  test("GraphQL execution rejects excessive complexity through Sangria reducers before resolver work") {
    val selections = (0 to 1000).map(index => s"r$index: readiness { status }").mkString(" ")
    val query = s"{ $selections }"
    for {
      calls <- Ref.of[IO, Int](0)
      http <- app(calls.update(_ + 1).as(ProbeResult.Ready))
      response <- http(request(query))
      body <- response.as[String]
      count <- calls.get
    } yield {
      assertEquals(response.status, Status.BadRequest)
      assertEquals(count, 0)
      assert(!body.contains("readiness"))
      assert(!body.contains("complexity"))
    }
  }

  test("named operation, variables, explicit nulls, methods and media negotiation") {
    val selected = health.withEntity(Json.obj(
      "query" -> Json.fromString("query First { readiness { status } } query Second($include: Boolean!) { health @include(if: $include) { status } }"),
      "operationName" -> Json.fromString("Second"), "variables" -> Json.obj("include" -> Json.True)))
    for {
      http <- app(IO.raiseError(new IllegalStateException("probe must not execute")))
      named <- http(selected)
      nulls <- http(health.withEntity(Json.obj("query" -> Json.fromString("{health{status}}"),
        "operationName" -> Json.Null, "variables" -> Json.Null)))
      missingOperation <- http(selected.withEntity(Json.obj("query" -> Json.fromString("query Named { health { status } }"),
        "operationName" -> Json.fromString("Missing"))))
      method <- http(Request[IO](Method.GET, Uri.unsafeFromString("/graphql")))
      media <- http(health.withEntity("query"))
      accept <- http(health.putHeaders(Header.Raw(CIString("Accept"), "text/html")))
      zeroQuality <- http(health.putHeaders(Header.Raw(CIString("Accept"), "application/json;q=0")))
      charset <- http(health.putHeaders(Header.Raw(CIString("Content-Type"), "application/json; charset=utf-8")))
    } yield {
      assertEquals(named.status, Status.Ok)
      assertEquals(nulls.status, Status.Ok)
      assertEquals(missingOperation.status, Status.BadRequest)
      assertEquals(method.status, Status.MethodNotAllowed)
      assertEquals(media.status, Status.UnsupportedMediaType)
      assertEquals(accept.status, Status.NotAcceptable)
      assertEquals(zeroQuality.status, Status.NotAcceptable)
      assertEquals(charset.status, Status.Ok)
    }
  }

  test("invalid field in an unselected operation rejects a valid selected health operation") {
    val selected = health.withEntity(Json.obj(
      "query" -> Json.fromString("query Selected { health { status } } query Unselected { invalidField readiness { status } }"),
      "operationName" -> Json.fromString("Selected")
    ))
    for {
      calls <- Ref.of[IO, Int](0)
      http <- app(calls.update(_ + 1).as(ProbeResult.Ready))
      response <- http(selected)
      body <- response.as[String]
      count <- calls.get
    } yield {
      assertEquals(response.status, Status.BadRequest)
      assertEquals(count, 0)
      assert(!body.contains("invalidField"))
    }
  }

  test("rejection diagnostics match the sanitized response request ID without synthetic secrets") {
    val secret = "synthetic-rejection-secret"
    for {
      events <- Ref.of[IO, List[(LogEvent, Option[String])]](Nil)
      diagnostics = new Diagnostics {
        def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]): IO[Unit] = events.update(_ :+ (event -> requestId))
      }
      http <- app(IO.pure(ProbeResult.Ready), diagnostics)
      response <- http(health.withEntity(Json.obj(
        "query" -> Json.fromString("{ unknownField }"),
        "variables" -> Json.obj("password" -> Json.fromString(secret))
      )).putHeaders(Header.Raw(CIString("X-Request-ID"), secret)))
      body <- response.as[String]
      captured <- events.get
    } yield {
      val correlationId = requestId(response)
      assertEquals(response.status, Status.BadRequest)
      assert(correlationId.exists(isUuid))
      assertEquals(captured.filterNot(record => spanEvent(record._1)), List(LogEvent.RequestRejected -> correlationId))
      assert(!body.contains(secret))
      assert(!response.headers.toString.contains(secret))
      assert(!captured.toString.contains(secret))
    }
  }

  test("unexpected body stream failure returns a sanitized HTTP500 response") {
    val secret = "synthetic-body-failure-secret"
    for {
      events <- Ref.of[IO, List[(LogEvent, Option[String])]](Nil)
      diagnostics = new Diagnostics {
        def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]): IO[Unit] = events.update(_ :+ (event -> requestId))
      }
      http <- app(IO.pure(ProbeResult.Ready), diagnostics)
      response <- http(health.withBodyStream(fs2.Stream.raiseError[IO](new IllegalStateException(secret))))
      body <- response.as[Json]
      captured <- events.get
    } yield {
      assertEquals(response.status, Status.InternalServerError)
      assertEquals(body, Json.obj("errors" -> Json.arr(Json.obj("message" -> Json.fromString("Request failed")))))
      val correlationId = requestId(response)
      assertEquals(captured.filterNot(record => spanEvent(record._1)), List(LogEvent.RequestRejected -> correlationId))
      assert(!body.noSpaces.contains(secret))
      assert(!response.headers.toString.contains(secret))
      assert(!captured.toString.contains(secret))
    }
  }

  test("a failing readiness diagnostic preserves the GraphQL result and HTTP200") {
    val secret = "synthetic-resolver-failure-secret"
    for {
      calls <- Ref.of[IO, Int](0)
      events <- Ref.of[IO, List[(LogEvent, Option[String])]](Nil)
      diagnostics = new Diagnostics {
        def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]): IO[Unit] =
          events.update(_ :+ (event -> requestId)) *>
            (if (event == LogEvent.MongoUnavailable) IO.raiseError(new IllegalStateException(secret)) else IO.unit)
      }
      http <- app(calls.update(_ + 1).as(ProbeResult.Unavailable), diagnostics)
      response <- http(request("{ readiness { status } }"))
      body <- response.as[Json]
      count <- calls.get
      captured <- events.get
    } yield {
      assertEquals(response.status, Status.Ok)
      assertEquals(count, 1)
      assertEquals(body.hcursor.downField("data").downField("readiness").get[String]("status"), Right("NOT_READY"))
      assert(!body.hcursor.downField("errors").succeeded)
      val correlationId = requestId(response)
      assertEquals(captured.filterNot(record => spanEvent(record._1)), List(
        LogEvent.MongoUnavailable -> correlationId,
        LogEvent.GraphQLCompleted -> correlationId
      ))
      assert(!body.noSpaces.contains(secret))
      assert(!response.headers.toString.contains(secret))
      assert(!captured.toString.contains(secret))
    }
  }

  test("streamed size limit, slow body deadline and cancellation release capacity") {
    for {
      records <- Ref.of[IO, Vector[DiagnosticRecord]](Vector.empty)
      http <- app(IO.pure(ProbeResult.Ready), capture(records))
      large <- http(health.withBodyStream(fs2.Stream.repeatEval(IO.pure(32.toByte)).take(65537)))
      finalized <- Deferred[IO, Unit]
      slow = health.withBodyStream(fs2.Stream.eval(IO.never[Byte]).onFinalize(finalized.complete(()).void))
      response <- http(slow)
      _ <- finalized.get.timeout(1.second)
      successful <- http(health)
      captured <- records.get
    } yield {
      assertEquals(large.status, Status.PayloadTooLarge)
      assertEquals(response.status, Status.GatewayTimeout)
      assertEquals(successful.status, Status.Ok)
      val id = requestId(response)
      val deadline = captured.filter(record => !spanEvent(record._1) && record._2 == id)
      assertEquals(deadline.map(_._1), Vector(LogEvent.RequestRejected))
      assert(deadline.filter(_._1 == LogEvent.RequestRejected).forall(_._3.get(LogField.Reason).contains("DEADLINE_EXCEEDED")))
    }
  }

  test("saturation rejects work and preserves probe liveness") {
    for {
      started <- Ref.of[IO, Int](0)
      allStarted <- Deferred[IO, Unit]
      release <- Deferred[IO, Unit]
      http <- app(
        started.updateAndGet(_ + 1).flatMap(count => if (count == 16) allStarted.complete(()).void else IO.unit) *>
          release.get.as(ProbeResult.Ready))
      _ <- List.fill(16)(Resource.make(http(request("{ readiness { status } }")).start)(_.cancel)).sequence.use { fibers =>
        for {
          _ <- allStarted.get.timeout(5.seconds)
          rejected <- http(health)
          liveness <- http(Request[IO](Method.GET, Uri.unsafeFromString("/health")))
          _ <- release.complete(())
          _ <- fibers.traverse_(_.joinWithNever)
          _ <- IO {
            assertEquals(rejected.status, Status.ServiceUnavailable)
            assertEquals(liveness.status, Status.Ok)
          }
        } yield ()
      }
    } yield ()
  }

  test("configured admission permit count controls saturation threshold") {
    for {
      started <- Ref.of[IO, Int](0)
      bothStarted <- Deferred[IO, Unit]
      release <- Deferred[IO, Unit]
      http <- app(
        started.updateAndGet(_ + 1).flatMap(count => if (count == 2) bothStarted.complete(()).void else IO.unit) *>
          release.get *> IO.pure(ProbeResult.Ready), admissionPermits = 2)
      _ <- List.fill(2)(Resource.make(http(request("{ readiness { status } }")).start)(_.cancel)).sequence.use { fibers =>
        (for {
          _ <- bothStarted.get.timeout(2.seconds)
          overflow <- http(health)
          _ <- IO(assertEquals(overflow.status, Status.ServiceUnavailable))
          _ <- release.complete(())
          completed <- fibers.traverse(_.joinWithNever)
          _ <- IO(assert(completed.forall(_.status == Status.Ok)))
        } yield ()).guarantee(release.complete(()).void)
      }
    } yield ()
  }

  test("UI and assets are absent, and probes remain live") {
    for {
      probe = new DatabaseProbe { def check: IO[ProbeResult] = IO.pure(ProbeResult.Ready) }
      http <- buildRoutes(new HealthService(probe, Diagnostics.noop), Diagnostics.noop).flatMap(defaultApp)
      ui <- http(Request[IO](Method.GET, Uri.unsafeFromString("/graphiql")))
      asset <- http(Request[IO](Method.GET, Uri.unsafeFromString("/graphiql/assets/graphiql.js")))
      work <- http(health)
      live <- http(Request[IO](Method.GET, Uri.unsafeFromString("/health")))
    } yield {
      assertEquals(ui.status, Status.NotFound)
      assertEquals(asset.status, Status.NotFound)
      assertEquals(work.status, Status.Ok)
      assertEquals(live.status, Status.Ok)
    }
  }
}
