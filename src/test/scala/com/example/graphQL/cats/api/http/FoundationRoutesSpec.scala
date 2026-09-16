package com.example.graphQL.cats.api.http

import cats.effect.{Deferred, IO, Ref, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.api.graphql.{FoundationSchema, GraphQLRequest, RequestContext}
import com.example.graphQL.cats.application.{DatabaseProbe, Diagnostics, HealthService, LogEvent, LogField, LogFields, ProbeResult}
import io.circe.Json
import munit.CatsEffectSuite
import org.http4s.*
import org.http4s.circe.*
import org.typelevel.ci.CIString
import scala.concurrent.duration.*

final class FoundationRoutesSpec extends CatsEffectSuite {
  private def app(effect: IO[ProbeResult], diagnostics: Diagnostics = Diagnostics.noop): IO[HttpApp[IO]] =
    Admission.create.map { admission =>
      val probe = new DatabaseProbe { def check: IO[ProbeResult] = effect }
      new FoundationRoutes(new HealthService(probe, diagnostics), diagnostics, admission).app
    }

  private def request(query: String): Request[IO] =
    Request[IO](Method.POST, Uri.unsafeFromString("/graphql"))
      .withEntity(Json.obj("query" -> Json.fromString(query)))

  private val health = request("{ health { status } }")

  private type DiagnosticRecord = (LogEvent, Option[String], Map[LogField, String])

  private def capture(records: Ref[IO, Vector[DiagnosticRecord]]): Diagnostics = new Diagnostics {
    def event(event: LogEvent, id: Option[String], fields: Map[LogField, String]): IO[Unit] =
      records.update(_ :+ ((event, id, fields)))
  }

  test("validated Sangria field errors retain their response and emit correlated filtered payloads") {
    for {
      records <- Ref.of[IO, Vector[DiagnosticRecord]](Vector.empty)
      sink = new Diagnostics {
        override def payloadsEnabled: Boolean = true
        def event(event: LogEvent, id: Option[String], fields: Map[LogField, String]): IO[Unit] =
          records.update(_ :+ ((event, id, fields)))
      }
      parsed <- IO.fromOption(GraphQLRequest.parseBody(Json.obj(
        "query" -> Json.fromString("query Selected($include: Boolean!) { readiness @include(if: $include) { status } } # synthetic-field-comment-secret"),
        "operationName" -> Json.fromString("Selected"),
        "variables" -> Json.obj("include" -> Json.True, "password" -> Json.fromString("synthetic-field-value-secret"))
      ).noSpaces))(new IllegalArgumentException("Invalid test query"))
      closed <- RequestContext.resource(IO.pure(ProbeResult.Ready)).use(IO.pure)
      execution <- FoundationSchema.executeInContext(parsed, closed)
      result <- IO.fromEither(execution.left.map(failure => new AssertionError(s"Expected field error result: $failure")))
      admission <- Admission.create
      probe = new DatabaseProbe { def check: IO[ProbeResult] = IO.pure(ProbeResult.Ready) }
      routes = new FoundationRoutes(new HealthService(probe, sink), sink, admission)
      id <- IO.randomUUID.map(_.toString)
      response <- routes.completedGraphQL(parsed, result, id)
      body <- response.as[Json]
      captured <- records.get
    } yield {
      assertEquals(response.status, Status.Ok)
      assertEquals(body, result)
      assertEquals(body.hcursor.get[Json]("errors"), Right(Json.arr(Json.obj("message" -> Json.fromString("Execution failed")))))
      assertEquals(captured.map(_._1), Vector(LogEvent.GraphQLCompleted, LogEvent.RequestPayload))
      assert(captured.forall(_._2.contains(id)))
      assert(captured.filter(_._1 == LogEvent.GraphQLCompleted).forall(_._3.get(LogField.Outcome).contains("FIELD_ERROR")))
      captured.filter(_._1 == LogEvent.RequestPayload).foreach { case (_, _, fields) =>
        val serialized = fields.getOrElse(LogField.RequestPayload, fail("Missing field error payload"))
        val payload = io.circe.parser.parse(serialized).getOrElse(fail("Invalid payload JSON"))
        assertEquals(payload.hcursor.downField("variables").get[Boolean]("include"), Right(true))
        assertEquals(payload.hcursor.downField("variables").get[String]("password"), Right("[REDACTED]"))
        assert(!serialized.contains("synthetic-"))
      }
      assert(!body.noSpaces.contains("Request context is closed"))
    }
  }

  test("throwing cancellation sinks preserve cleanup, cancellation and admission capacity") {
    List(true, false).traverse_ { synchronous =>
      val attempts = new java.util.concurrent.atomic.AtomicReference(Vector.empty[(DiagnosticRecord, Boolean)])
      val finalized = new java.util.concurrent.atomic.AtomicBoolean(false)
      val sink = new Diagnostics {
        def event(event: LogEvent, id: Option[String], fields: Map[LogField, String]): IO[Unit] = {
          def record(): Unit = {
            val _ = attempts.updateAndGet(_ :+ (((event, id, fields), finalized.get())))
          }
          if (event != LogEvent.RequestCancelled) IO.unit
          else if (synchronous) {
            record()
            throw new IllegalStateException("synthetic-cancellation-secret")
          } else IO.delay(record()) *> IO.raiseError(new IllegalStateException("synthetic-cancellation-secret"))
        }
      }
      for {
        entered <- Deferred[IO, Unit]
        finalizing <- Deferred[IO, Unit]
        finish <- Deferred[IO, Unit]
        admission <- Admission.create
        probe = new DatabaseProbe { def check: IO[ProbeResult] = IO.pure(ProbeResult.Ready) }
        http = new FoundationRoutes(new HealthService(probe, sink), sink, admission).app
        slow = health.withBodyStream(fs2.Stream.eval(entered.complete(()) *> IO.never[Byte])
          .onFinalize(finalizing.complete(()) *> finish.get *> IO.delay(finalized.set(true))))
        _ <- List.fill(15)(admission.permit).sequence.use { held =>
          IO(assert(held.forall(identity))) *>
            Resource.make(http(slow).start)(fiber => finish.complete(()).void *> fiber.cancel).use { fiber =>
              entered.get.timeout(2.seconds) *> fiber.cancel.background.use { joined =>
                (for {
                  _ <- finalizing.get.timeout(2.seconds)
                  during <- IO(attempts.get())
                  saturated <- http(health)
                  _ <- IO {
                    assert(during.isEmpty)
                    assertEquals(saturated.status, Status.ServiceUnavailable)
                  }
                  _ <- finish.complete(())
                  _ <- joined.flatMap(_.embedNever)
                  outcome <- fiber.join
                  recovered <- http(health)
                  after <- IO(attempts.get())
                } yield {
                  assert(outcome.isCanceled)
                  assertEquals(recovered.status, Status.Ok)
                  assertEquals(after.size, 1)
                  assert(after.forall { case ((event, id, fields), cleaned) =>
                    cleaned && event == LogEvent.RequestCancelled && id.nonEmpty &&
                      !fields.contains(LogField.Status) && fields.get(LogField.Outcome).contains("CANCELLED")
                  })
                }).guarantee(finish.complete(()).void)
              }
            }
        }
      } yield ()
    }
  }

  test("payload policy is never consulted before envelope, schema, operation and variable validation succeed") {
    val policyReads = new java.util.concurrent.atomic.AtomicInteger()
    val variableQuery = "query Selected($include: Boolean!) { health @include(if: $include) { status } }"
    val invalid = List(
      health.withEntity("not-json").putHeaders(Header.Raw(CIString("Content-Type"), "application/json")),
      request("{ invalidField }"),
      request("{ health"),
      health.withEntity(Json.obj("query" -> Json.fromString(variableQuery), "variables" -> Json.obj("include" -> Json.fromString("invalid")))),
      health.withEntity(Json.obj("query" -> Json.fromString(variableQuery))),
      health.withEntity(Json.obj("query" -> Json.fromString("query Named { health { status } }"),
        "operationName" -> Json.fromString("Missing"))),
      request("query One { health { status } } query Two { health { status } }"),
      health.withEntity(Json.obj("query" -> Json.fromString("query Selected { health { status } } query Other { invalidField }"),
        "operationName" -> Json.fromString("Selected")))
    )
    for {
      records <- Ref.of[IO, Vector[DiagnosticRecord]](Vector.empty)
      sink = new Diagnostics {
        override def payloadsEnabled: Boolean = { val _ = policyReads.incrementAndGet(); true }
        def event(event: LogEvent, id: Option[String], fields: Map[LogField, String]): IO[Unit] =
          records.update(_ :+ ((event, id, fields)))
      }
      http <- app(IO.pure(ProbeResult.Ready), sink)
      responses <- invalid.traverse(http.run)
      captured <- records.get
    } yield {
      assert(responses.forall(_.status == Status.BadRequest))
      assertEquals(policyReads.get(), 0)
      assert(!captured.exists(_._1 == LogEvent.RequestPayload))
    }
  }

  test("payload opt-in captures only filtered validated data and shares the generated request ID") {
    List(false, true).traverse_ { enabled =>
      for {
        records <- Ref.of[IO, Vector[DiagnosticRecord]](Vector.empty)
        sink = new Diagnostics {
          override def payloadsEnabled: Boolean = enabled
          def event(event: LogEvent, id: Option[String], fields: Map[LogField, String]): IO[Unit] =
            records.update(_ :+ ((event, id, fields)))
        }
        http <- app(IO.pure(ProbeResult.Ready), sink)
        response <- http(health.withEntity(Json.obj(
          "query" -> Json.fromString("query Selected($include: Boolean!) { health @include(if: $include) { status } } query Other { health { status } } # synthetic-comment-secret"),
          "operationName" -> Json.fromString("Selected"),
          "variables" -> Json.obj("include" -> Json.True, "password" -> Json.fromString("synthetic-variable-secret"))
        )))
        captured <- records.get
      } yield {
        assertEquals(response.status, Status.Ok)
        val payloads = captured.filter(_._1 == LogEvent.RequestPayload)
        assertEquals(payloads.size, if (enabled) 1 else 0)
        payloads.foreach { case (_, id, fields) =>
          assertEquals(id, response.headers.get(CIString("X-Request-ID")).map(_.head.value))
          val serialized = fields.getOrElse(LogField.RequestPayload, fail("Missing filtered payload"))
          val payload = io.circe.parser.parse(serialized).getOrElse(fail("Payload is not JSON"))
          assertEquals(payload.hcursor.downField("variables").get[Boolean]("include"), Right(true))
          assertEquals(payload.hcursor.downField("variables").get[String]("password"), Right("[REDACTED]"))
          assert(!serialized.contains("synthetic-"))
          assert(!serialized.contains("Other"))
        }
      }
    }
  }

  test("throwing payload policy and payload sink leave a validated response unchanged") {
    List(true, false).traverse_ { policyThrows =>
      val sink = new Diagnostics {
        override def payloadsEnabled: Boolean =
          if (policyThrows) throw new IllegalStateException("synthetic-policy-secret") else true
        def event(event: LogEvent, id: Option[String], fields: Map[LogField, String]): IO[Unit] =
          if (event == LogEvent.RequestPayload) throw new IllegalStateException("synthetic-payload-sink-secret") else IO.unit
      }
      for {
        http <- app(IO.pure(ProbeResult.Ready), sink)
        response <- http(health)
        body <- response.as[Json]
      } yield {
        assertEquals(response.status, Status.Ok)
        assertEquals(body.hcursor.downField("data").downField("health").get[String]("status"), Right("UP"))
      }
    }
  }

  test("response creation logs one normalized completion with rejection reason, status and timing") {
    for {
      records <- Ref.of[IO, Vector[DiagnosticRecord]](Vector.empty)
      http <- app(IO.pure(ProbeResult.Ready), capture(records))
      response <- http(Request[IO](Method.fromString("SYNTHETIC").getOrElse(fail("Invalid test method")),
        Uri.unsafeFromString("/synthetic-path-secret?password=synthetic-query-secret")))
      captured <- records.get
    } yield {
      val id = response.headers.get(CIString("X-Request-ID")).map(_.head.value)
      val completions = captured.filter(_._1 == LogEvent.RequestCompleted)
      assertEquals(response.status, Status.NotFound)
      assertEquals(completions.size, 1)
      assert(captured.forall(_._2 == id))
      assert(captured.exists(record => record._1 == LogEvent.RequestRejected &&
        record._3.get(LogField.Reason).contains("NOT_FOUND")))
      assert(completions.forall { case (_, _, fields) =>
        fields.get(LogField.Route).contains("_unmatched") && fields.get(LogField.Method).contains("OTHER") &&
          fields.get(LogField.Status).contains("404") && fields.get(LogField.DurationMs).flatMap(_.toLongOption).exists(_ >= 0)
      })
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
      val id = response.headers.get(CIString("X-Request-ID")).map(_.head.value)
      assertEquals(response.status, Status.Ok)
      assertEquals(captured.map(_._1), Vector(LogEvent.MongoUnavailable, LogEvent.GraphQLCompleted, LogEvent.RequestCompleted))
      assert(captured.forall(_._2 == id))
      assert(captured.filter(_._1 == LogEvent.GraphQLCompleted).forall { case (_, _, fields) =>
        fields.get(LogField.OperationName).contains("LocalCheck") && fields.get(LogField.Outcome).contains("COMPLETED")
      })
    }
  }

  test("HTTP cancellation is recorded after blocked cleanup without a completion or invented status") {
    for {
      records <- Ref.of[IO, Vector[DiagnosticRecord]](Vector.empty)
      entered <- Deferred[IO, Unit]
      finalizing <- Deferred[IO, Unit]
      finish <- Deferred[IO, Unit]
      finalized <- Ref.of[IO, Boolean](false)
      sink = new Diagnostics {
        def event(event: LogEvent, id: Option[String], fields: Map[LogField, String]): IO[Unit] =
          (if (event == LogEvent.RequestCancelled) finalized.get.flatMap(done => IO(assert(done))) else IO.unit) *>
            records.update(_ :+ ((event, id, fields)))
      }
      http <- app(IO.pure(ProbeResult.Ready), sink)
      slow = health.withBodyStream(fs2.Stream.eval(entered.complete(()) *> IO.never[Byte])
        .onFinalize(finalizing.complete(()) *> finish.get *> finalized.set(true)))
      _ <- Resource.make(http(slow).start)(fiber => finish.complete(()).void *> fiber.cancel).use { fiber =>
        entered.get.timeout(2.seconds) *> fiber.cancel.background.use { joined =>
          (for {
            _ <- finalizing.get.timeout(2.seconds)
            during <- records.get
            _ <- IO(assert(during.isEmpty))
            _ <- finish.complete(())
            _ <- joined.flatMap(_.embedNever)
            outcome <- fiber.join
            after <- records.get
          } yield {
            assert(outcome.isCanceled)
            assertEquals(after.map(_._1), Vector(LogEvent.RequestCancelled))
            assert(after.forall { case (_, id, fields) =>
              id.nonEmpty && !fields.contains(LogField.Status) && fields.get(LogField.Outcome).contains("CANCELLED") &&
                fields.get(LogField.DurationMs).flatMap(_.toLongOption).exists(_ >= 0)
            })
          }).guarantee(finish.complete(()).void)
        }
      }
    } yield ()
  }

  test("sync and effectful sink failures preserve successful, rejected and unexpected-error responses") {
    List(true, false).traverse_ { synchronous =>
      val sink = new Diagnostics {
        def event(event: LogEvent, id: Option[String], fields: Map[LogField, String]): IO[Unit] =
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

  test("Accept uses the most specific range and highest quality only among equal specificity") {
    val cases = List(
      "*/*" -> Status.Ok,
      "application/*" -> Status.Ok,
      "application/json;q=0, */*;q=1" -> Status.NotAcceptable,
      "*/*;q=1, application/json;q=0" -> Status.NotAcceptable,
      "application/*;q=0, */*;q=1" -> Status.NotAcceptable,
      "application/json;q=0.001, application/*;q=0" -> Status.Ok,
      "application/json;q=0, application/json;q=0.5" -> Status.Ok,
      "application/json;q=0.5, application/json;q=0" -> Status.Ok,
      "application/json;q=0, application/json" -> Status.Ok,
      "application/*;q=0, application/*;q=0.2" -> Status.Ok,
      "text/html;q=1, */*;q=0.1" -> Status.Ok,
      "application/json;q=1.000" -> Status.Ok,
      "application/json;q=0.000" -> Status.NotAcceptable,
      "application/json;q=1." -> Status.Ok,
      "application/json;q=0." -> Status.NotAcceptable,
      "APPLICATION/JSON; Q = 0.5" -> Status.Ok
    )
    app(IO.pure(ProbeResult.Ready)).flatMap { http =>
      cases.traverse_ { case (accept, expected) =>
        http(health.putHeaders(Header.Raw(CIString("Accept"), accept)))
          .map(response => assertEquals(response.status, expected, accept))
      }
    }
  }

  test("malformed or duplicate Accept quality parameters reject the whole header") {
    val invalid = List("-0.1", "1.001", "2", "0.0001", "1.0000", ".5", "01", "NaN", "Infinity", "1e0", "", "\"0.5\"")
      .map(value => s"application/json;q=$value, */*;q=1") ++ List(
        "application/json;q=0;q=1", "application/json;q=1;Q=1", "application/json;q",
        "application/json, text/html;q=invalid"
      )
    app(IO.pure(ProbeResult.Ready)).flatMap { http =>
      invalid.traverse_ { accept =>
        http(health.putHeaders(Header.Raw(CIString("Accept"), accept)))
          .map(response => assertEquals(response.status, Status.NotAcceptable, accept))
      }
    }
  }

  test("GraphQL permit remains occupied until resolver cancellation finalizers finish") {
    for {
      admission <- Admission.create
      entered <- Deferred[IO, Unit]
      finalizing <- Deferred[IO, Unit]
      finishFinalizer <- Deferred[IO, Unit]
      cancelled <- Deferred[IO, Unit]
      probe = new DatabaseProbe {
        def check: IO[ProbeResult] = (entered.complete(()) *> IO.never[ProbeResult])
          .onCancel(finalizing.complete(()) *> finishFinalizer.get)
      }
      http = new FoundationRoutes(new HealthService(probe, Diagnostics.noop), Diagnostics.noop, admission).app
      _ <- List.fill(15)(admission.permit).sequence.use { held =>
        for {
          _ <- IO(assert(held.forall(identity)))
          _ <- Resource.make(http(request("{ readiness { status } }")).start) { requestFiber =>
            finishFinalizer.complete(()).void *> requestFiber.cancel
          }.use { requestFiber =>
            for {
              _ <- entered.get.timeout(2.seconds)
              _ <- (requestFiber.cancel *> cancelled.complete(()).void).background.use { _ =>
                (for {
                  _ <- finalizing.get.timeout(2.seconds)
                  rejected <- http(health)
                  completed <- cancelled.tryGet
                  _ <- IO {
                    assertEquals(rejected.status, Status.ServiceUnavailable)
                    assertEquals(completed, None)
                  }
                  _ <- finishFinalizer.complete(())
                  _ <- cancelled.get.timeout(2.seconds)
                  recovered <- http(health)
                  _ <- IO(assertEquals(recovered.status, Status.Ok))
                } yield ()).guarantee(finishFinalizer.complete(()).void)
              }
            } yield ()
          }
        } yield ()
      }
    } yield ()
  }

  test("API schema download matches the served schema without database access") {
    for {
      http <- app(IO.raiseError(new IllegalStateException("Schema must not query MongoDB")))
      response <- http(Request[IO](Method.GET, Uri.unsafeFromString("/schema.graphql")))
      schema <- response.as[String]
    } yield {
      assertEquals(response.status, Status.Ok)
      assertEquals(schema, com.example.graphQL.cats.api.graphql.FoundationSchema.sdl)
      assert(!schema.contains("users"))
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
      assert(response.headers.get(CIString("X-Request-ID")).isDefined)
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

  test("rejection diagnostics match the generated response request ID without synthetic secrets") {
    val secret = "synthetic-rejection-secret"
    for {
      events <- Ref.of[IO, List[(LogEvent, Option[String])]](Nil)
      diagnostics = new Diagnostics {
        def event(event: LogEvent, requestId: Option[String], fields: Map[LogField, String]): IO[Unit] = events.update(_ :+ (event -> requestId))
      }
      http <- app(IO.pure(ProbeResult.Ready), diagnostics)
      response <- http(health.withEntity(Json.obj(
        "query" -> Json.fromString("{ unknownField }"),
        "variables" -> Json.obj("password" -> Json.fromString(secret))
      )).putHeaders(Header.Raw(CIString("X-Request-ID"), secret)))
      body <- response.as[String]
      captured <- events.get
    } yield {
      val requestId = response.headers.get(CIString("X-Request-ID")).map(_.head.value)
      assertEquals(response.status, Status.BadRequest)
      assert(requestId.exists(value => scala.util.Try(java.util.UUID.fromString(value)).isSuccess))
      assertEquals(captured, List(LogEvent.RequestRejected -> requestId, LogEvent.RequestCompleted -> requestId))
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
        def event(event: LogEvent, requestId: Option[String], fields: Map[LogField, String]): IO[Unit] = events.update(_ :+ (event -> requestId))
      }
      http <- app(IO.pure(ProbeResult.Ready), diagnostics)
      response <- http(health.withBodyStream(fs2.Stream.raiseError[IO](new IllegalStateException(secret))))
      body <- response.as[Json]
      captured <- events.get
    } yield {
      assertEquals(response.status, Status.InternalServerError)
      assertEquals(body, Json.obj("errors" -> Json.arr(Json.obj("message" -> Json.fromString("Request failed")))))
      val requestId = response.headers.get(CIString("X-Request-ID")).map(_.head.value)
      assertEquals(captured, List(LogEvent.RequestRejected -> requestId, LogEvent.RequestCompleted -> requestId))
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
        def event(event: LogEvent, requestId: Option[String], fields: Map[LogField, String]): IO[Unit] =
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
      val requestId = response.headers.get(CIString("X-Request-ID")).map(_.head.value)
      assertEquals(captured, List(LogEvent.MongoUnavailable -> requestId, LogEvent.GraphQLCompleted -> requestId,
        LogEvent.RequestCompleted -> requestId))
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
      val id = response.headers.get(CIString("X-Request-ID")).map(_.head.value)
      val deadline = captured.filter(_._2 == id)
      assertEquals(deadline.map(_._1), Vector(LogEvent.RequestRejected, LogEvent.RequestCompleted))
      assert(deadline.filter(_._1 == LogEvent.RequestRejected).forall(_._3.get(LogField.Reason).contains("DEADLINE_EXCEEDED")))
      assert(deadline.filter(_._1 == LogEvent.RequestCompleted).forall { case (_, _, fields) =>
        fields.get(LogField.Status).contains("504") && fields.get(LogField.DurationMs).flatMap(_.toLongOption).exists(_ >= 5000)
      })
    }
  }

  test("saturation rejects work, preserves liveness, and cancellation returns every permit") {
    for {
      started <- Ref.of[IO, Int](0)
      allStarted <- Deferred[IO, Unit]
      canceled <- Ref.of[IO, Int](0)
      http <- app(IO.pure(ProbeResult.Ready))
      slow = health.withBodyStream(fs2.Stream.eval(
        started.updateAndGet(_ + 1).flatMap(count => if (count == 16) allStarted.complete(()).void else IO.unit) *>
          IO.never[Byte]).onFinalize(canceled.update(_ + 1)))
      _ <- List.fill(16)(Resource.make(http(slow).start)(_.cancel)).sequence.use { fibers =>
        for {
          _ <- allStarted.get.timeout(2.seconds)
          rejected <- http(health)
          liveness <- http(Request[IO](Method.GET, Uri.unsafeFromString("/health")))
          _ <- fibers.traverse_(_.cancel)
          canceledCount <- canceled.get
          _ <- IO {
            assertEquals(rejected.status, Status.ServiceUnavailable)
            assertEquals(liveness.status, Status.Ok)
            assertEquals(canceledCount, 16)
          }
        } yield ()
      }
      replacementCount <- Ref.of[IO, Int](0)
      replacementsEntered <- Deferred[IO, Unit]
      releaseReplacements <- Deferred[IO, Unit]
      replacement = health.withBodyStream(fs2.Stream.eval(
        replacementCount.updateAndGet(_ + 1).flatMap { count =>
          if (count == 16) replacementsEntered.complete(()).void else IO.unit
        } *> releaseReplacements.get).drain ++ health.body)
      _ <- List.fill(16)(Resource.make(http(replacement).start)(_.cancel)).sequence.use { fibers =>
        (for {
          _ <- replacementsEntered.get.timeout(2.seconds)
          enteredCount <- replacementCount.get
          overflow <- http(health)
          _ <- IO {
            assertEquals(enteredCount, 16)
            assertEquals(overflow.status, Status.ServiceUnavailable)
          }
          _ <- releaseReplacements.complete(())
          successful <- fibers.traverse(_.joinWithNever)
          _ <- IO(assert(successful.forall(_.status == Status.Ok)))
        } yield ()).guarantee(releaseReplacements.complete(()).void)
      }
    } yield ()
  }

  test("UI and assets are absent, and closing admission preserves liveness") {
    for {
      admission <- Admission.create
      probe = new DatabaseProbe { def check: IO[ProbeResult] = IO.pure(ProbeResult.Ready) }
      http = new FoundationRoutes(new HealthService(probe, Diagnostics.noop), Diagnostics.noop, admission).app
      ui <- http(Request[IO](Method.GET, Uri.unsafeFromString("/graphiql")))
      asset <- http(Request[IO](Method.GET, Uri.unsafeFromString("/graphiql/assets/graphiql.js")))
      _ <- admission.close
      work <- http(health)
      live <- http(Request[IO](Method.GET, Uri.unsafeFromString("/health")))
    } yield {
      assertEquals(ui.status, Status.NotFound)
      assertEquals(asset.status, Status.NotFound)
      assertEquals(work.status, Status.ServiceUnavailable)
      assertEquals(live.status, Status.Ok)
    }
  }
}
