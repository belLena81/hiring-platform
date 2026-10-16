package com.example.graphQL.cats.api.http

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.comcast.ip4s.{Cidr, SocketAddress}
import com.example.graphQL.cats.api.graphql.TestGraphQLSupport
import com.example.graphQL.cats.config.{CostBudgetConfig, TrustedProxyConfig}
import com.example.graphQL.cats.domain.model.UserRole
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.{ActorContext, DatabaseProbe, Diagnostics, HealthService, ProbeResult}
import io.circe.Json
import munit.CatsEffectSuite
import org.http4s.*
import org.http4s.circe.*
import org.typelevel.ci.CIString
import scala.concurrent.duration.*

/** ARR-14: GraphQL complexity is charged to a per-actor or per-client budget before any resolver runs. */
final class GraphQLCostBudgetSpec extends CatsEffectSuite {
  // Each `me { id }` selection weighs 2, so this request costs 8.
  private val spend =
    "{ a: me { id } b: me { id } c: me { id } d: me { id } }"
  private val cheap = "{ me { id } }"
  private val login =
    """mutation {
      |  login(input: { idempotencyKey: "00000000-0000-0000-0000-000000000021", name: "Candidate", password: "password-password" }) { __typename }
      |}""".stripMargin
  private val proxies = TrustedProxyConfig(List(Cidr.fromString("10.0.0.0/8").get))
  private val actorHeader = CIString("X-Test-Actor")

  private def actor(name: String): ActorContext =
    ActorContext(UserId(java.util.UUID.nameUUIDFromBytes(name.getBytes)), UserRole.Candidate)

  private def build(budget: CostBudgetConfig): IO[(HttpApp[IO], Ref[IO, Int])] =
    for {
      probeCalls <- Ref.of[IO, Int](0)
      probe = new DatabaseProbe[IO] {
        def check(requestId: Option[String]): IO[ProbeResult] = IO.pure(ProbeResult.Ready)
      }
      built <- TestGraphQLSupport
        .dependencies(
          // A resolver that starts reaches the hiring-readiness check once per request, so this counts requests that
          // got past the budget.
          hiringReady = probeCalls.update(_ + 1).as(ProbeResult.Ready),
          authenticate = request => IO.pure(Right(request.headers.get(actorHeader).map(h => actor(h.head.value)))),
          trustedProxy = proxies,
          costBudget = budget
        )
        .allocated
      routes = new HiringApiRoutes(new HealthService(probe, Diagnostics.noop), Diagnostics.noop, built._1)
      http <- routes.httpApp(HiringApiRoutes.HttpConfig(16L, 5.seconds))
    } yield (http, probeCalls)

  private def post(query: String, actorName: Option[String] = None, clientIp: Option[String] = None): Request[IO] = {
    val base = Request[IO](Method.POST, Uri.unsafeFromString("/graphql"))
      .withEntity(Json.obj("query" -> Json.fromString(query)))
    val withActor = actorName.fold(base)(name => base.putHeaders(Header.Raw(actorHeader, name)))
    clientIp.fold(withActor) { ip =>
      withActor
        .putHeaders(Header.Raw(CIString("X-Forwarded-For"), ip))
        .withAttribute(
          Request.Keys.ConnectionInfo,
          Request.Connection(
            SocketAddress.fromStringIp("127.0.0.1:8080").get,
            SocketAddress.fromStringIp("10.0.0.5:12345").get,
            secure = false
          )
        )
    }
  }

  private val tight = CostBudgetConfig(windowSeconds = 60, maxCost = 9, maxActorBuckets = 100, maxClientBuckets = 100)

  test("ARR-14.1 a request within the budget succeeds and runs its resolvers") {
    for {
      (http, calls) <- build(tight)
      response <- http(post(cheap))
      body <- response.as[Json]
      count <- calls.get
    } yield {
      assertEquals(response.status, Status.Ok)
      assertEquals(body.hcursor.downField("data").downField("me").focus, Some(Json.Null), clue(body))
      assertEquals(count, 1)
    }
  }

  test(
    "ARR-14.1 after expensive queries spend the budget the next request is refused with 429 before any resolver runs"
  ) {
    for {
      (http, calls) <- build(tight)
      spent <- http(post(spend))
      afterSpend <- calls.get
      refused <- http(post(cheap))
      body <- refused.as[Json]
      afterRefusal <- calls.get
    } yield {
      assertEquals(spent.status, Status.Ok)
      assertEquals(afterSpend, 1)
      assertEquals(refused.status, Status.TooManyRequests)
      assertEquals(refused.headers.get(CIString("Retry-After")).map(_.head.value), Some("60"))
      val error = body.hcursor.downField("errors").downArray.downField("extensions")
      assertEquals(error.get[String]("code"), Right("RATE_LIMITED"), clue(body))
      assertEquals(error.get[Long]("retryAfter"), Right(60L), clue(body))
      assertEquals(afterRefusal, afterSpend)
    }
  }

  test("ARR-14.1 a mutation is charged and refused too once the budget is spent") {
    for {
      (http, calls) <- build(tight)
      _ <- http(post(spend))
      refused <- http(post(login))
      count <- calls.get
    } yield {
      assertEquals(refused.status, Status.TooManyRequests)
      assertEquals(count, 1)
    }
  }

  test("ARR-14.1 a query whose cost exceeds the whole budget is refused without running") {
    for {
      (http, calls) <- build(CostBudgetConfig(60, 5, 100))
      refused <- http(post(spend))
      count <- calls.get
    } yield {
      assertEquals(refused.status, Status.TooManyRequests)
      assertEquals(count, 0)
    }
  }

  test("ARR-14.2 authenticated callers are keyed by actor id, whatever their client address") {
    for {
      (http, _) <- build(tight)
      _ <- http(post(spend, Some("alice"), Some("203.0.113.1")))
      aliceElsewhere <- http(post(cheap, Some("alice"), Some("203.0.113.2")))
      bobSameAddress <- http(post(cheap, Some("bob"), Some("203.0.113.1")))
    } yield {
      assertEquals(aliceElsewhere.status, Status.TooManyRequests)
      assertEquals(bobSameAddress.status, Status.Ok)
    }
  }

  test("ARR-14.2 anonymous callers are keyed by the resolved client address, separately from actors") {
    for {
      (http, _) <- build(tight)
      _ <- http(post(spend, clientIp = Some("203.0.113.1")))
      sameClient <- http(post(cheap, clientIp = Some("203.0.113.1")))
      otherClient <- http(post(cheap, clientIp = Some("203.0.113.2")))
      actorOnSameClient <- http(post(cheap, Some("alice"), Some("203.0.113.1")))
    } yield {
      assertEquals(sameClient.status, Status.TooManyRequests)
      assertEquals(otherClient.status, Status.Ok)
      assertEquals(actorOnSameClient.status, Status.Ok)
    }
  }

  test("ARR-14.2 flooding distinct clients past max-buckets does not reset a victim's exhausted budget") {
    for {
      (http, _) <- build(CostBudgetConfig(60, 9, 3, 3))
      _ <- http(post(spend, clientIp = Some("203.0.113.1")))
      _ <- http(post(cheap, clientIp = Some("203.0.113.1")))
      _ <- (1 to 30).toList.foldLeft(IO.unit)((done, index) =>
        // Bodies are drained so each response releases its admission permit.
        done *> http(post(cheap, clientIp = Some(s"198.51.100.$index"))).flatMap(_.as[String]).void
      )
      victim <- http(post(cheap, clientIp = Some("203.0.113.1")))
    } yield assertEquals(victim.status, Status.TooManyRequests)
  }

  test("ARR-01.1 a refused page size does not charge the cost budget") {
    for {
      (http, calls) <- build(tight)
      refusals <- (1 to 5).toList.traverse(_ =>
        http(post("{ jobs(first: -1) { edges { cursor } } }")).flatMap(_.as[Json])
      )
      afterRefusals <- calls.get
      spent <- http(post(spend))
    } yield {
      assert(refusals.forall(_.noSpaces.contains("VALIDATION_FAILED")), refusals.toString)
      assertEquals(afterRefusals, 0)
      assertEquals(spent.status, Status.Ok, "the whole budget is still available after the refusals")
    }
  }

  private def cost(body: Json): Option[Json] = body.hcursor.downField("extensions").downField("cost").focus

  private def costOf(body: Json): (Option[Int], Option[Int], Option[Long], Option[Long]) = {
    val c = body.hcursor.downField("extensions").downField("cost")
    (
      c.get[Int]("requested").toOption,
      c.get[Int]("limit").toOption,
      c.get[Long]("remaining").toOption,
      c.get[Long]("resetsInSeconds").toOption
    )
  }

  test("AIR-08.1 two charged queries report extensions.cost with remaining decreasing by the requested cost") {
    for {
      (http, _) <- build(tight)
      first <- http(post(cheap, Some("alice"))).flatMap(_.as[Json])
      second <- http(post(cheap, Some("alice"))).flatMap(_.as[Json])
    } yield {
      assertEquals(costOf(first), (Some(2), Some(9), Some(7L), Some(60L)), clue(first))
      assertEquals(costOf(second), (Some(2), Some(9), Some(5L), Some(60L)), clue(second))
    }
  }

  test("AIR-08.1 a request rejected before the budget is charged carries no cost object") {
    for {
      (http, _) <- build(tight)
      pageSize <- http(post("{ jobs(first: -1) { edges { cursor } } }")).flatMap(_.as[Json])
    } yield assertEquals(cost(pageSize), None, clue(pageSize))
  }

  test("AIR-08.2 the 429 body carries the cost object with remaining 0 and Retry-After is unchanged") {
    for {
      (http, _) <- build(tight)
      _ <- http(post(spend, Some("alice")))
      refused <- http(post(cheap, Some("alice")))
      body <- refused.as[Json]
    } yield {
      assertEquals(refused.status, Status.TooManyRequests)
      assertEquals(refused.headers.get(CIString("Retry-After")).map(_.head.value), Some("60"))
      assertEquals(costOf(body), (Some(2), Some(9), Some(0L), Some(60L)), clue(body))
      assertEquals(
        body.hcursor.downField("errors").downArray.downField("extensions").get[Long]("retryAfter"),
        Right(60L)
      )
    }
  }

  test("AIR-08.3 a caller charged to a shared overflow counter gets no cost object, on success or on 429") {
    for {
      (http, _) <- build(CostBudgetConfig(60, 9, 3, 3))
      _ <- (1 to 3).toList.traverse(index =>
        http(post(cheap, clientIp = Some(s"198.51.100.$index"))).flatMap(_.as[String])
      )
      admitted <- http(post(cheap, clientIp = Some("203.0.113.9")))
      admittedBody <- admitted.as[Json]
      refused <- http(post(spend, clientIp = Some("203.0.113.9")))
      refusedBody <- refused.as[Json]
    } yield {
      assertEquals(admitted.status, Status.Ok)
      assert(admittedBody.hcursor.downField("extensions").failed, clue(admittedBody))
      assertEquals(refused.status, Status.TooManyRequests)
      assert(refusedBody.hcursor.downField("extensions").failed, clue(refusedBody))
      assertEquals(refused.headers.get(CIString("Retry-After")).map(_.head.value), Some("60"))
    }
  }

  test("AIR-08.3 the cost receipt keeps the extensions keys a response already carries") {
    val receipt = com.example.graphQL.cats.api.graphql.CostReceipt(8, 100, 92, 30)
    val withExtensions = Json.obj("data" -> Json.Null, "extensions" -> Json.obj("tracing" -> Json.fromString("kept")))
    val merged = com.example.graphQL.cats.api.graphql.HiringGraphQLSchema.withCost(Some(receipt))(withExtensions)
    val extensions = merged.hcursor.downField("extensions")
    assertEquals(extensions.get[String]("tracing"), Right("kept"))
    assertEquals(extensions.downField("cost").get[Int]("requested"), Right(8))
    assertEquals(extensions.downField("cost").get[Long]("resetsInSeconds"), Right(30L))
    val plain = Json.obj("data" -> Json.Null)
    assertEquals(
      com.example.graphQL.cats.api.graphql.HiringGraphQLSchema.withCost(None)(withExtensions),
      withExtensions
    )
    assert(
      com.example.graphQL.cats.api.graphql.HiringGraphQLSchema
        .withCost(Some(receipt))(plain)
        .hcursor
        .downField("extensions")
        .downField("cost")
        .succeeded
    )
  }
}
