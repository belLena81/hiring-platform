package com.example.graphQL.cats.infrastructure.embedding

import cats.effect.{IO, Ref, Resource}
import fs2.Stream
import cats.data.Kleisli
import cats.syntax.all.*
import com.example.graphQL.cats.domain.policy.CircuitBreakerPolicy
import com.example.graphQL.cats.service.resilience.CircuitBreaker
import com.example.graphQL.cats.service.port.{EmbeddingError, EmbeddingInput, EmbeddingInputType, EmbeddingVector}
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField, LogFields}
import io.circe.Json
import munit.CatsEffectSuite
import org.http4s.{Header, HttpApp, Method, Request, Response, Status, Uri}
import org.http4s.client.Client
import org.http4s.circe.*
import org.typelevel.ci.CIString

import scala.concurrent.duration.*

final class VoyageEmbeddingServiceSpec extends CatsEffectSuite {
  private val input = EmbeddingInput("Scala", EmbeddingInputType.Document)
  private val endpoint = Uri.unsafeFromString("https://example.test/embed")

  test("encodes the Voyage request and decodes a successful response") {
    val app: HttpApp[IO] = Kleisli { (request: Request[IO]) =>
      for {
        body <- request.as[String]
        _ = assertEquals(request.method, Method.POST)
        _ = assert(request.headers.get(CIString("Authorization")).nonEmpty)
        _ = assertEquals(
          body,
          """{"input":"Scala","model":"voyage-4-lite","input_type":"document","output_dimension":2,"output_dtype":"float","truncation":true}"""
        )
      } yield Response[IO](Status.Ok).withEntity(
        Json.obj(
          "data" -> Json.arr(Json.obj("embedding" -> Json.arr(Json.fromFloatOrNull(0.1f), Json.fromFloatOrNull(0.2f)))),
          "model" -> Json.fromString("voyage-4-lite")
        )
      )(using jsonEncoderOf[IO, Json])
    }
    val client = Client.fromHttpApp[IO](app)
    val service = new VoyageEmbeddingService(
      client,
      "test-key",
      endpoint,
      "voyage-4-lite",
      2,
      1.second,
      diagnostics = com.example.graphQL.cats.service.Diagnostics.noop
    )

    service.embed(input).map { result =>
      assertEquals(result, Right(EmbeddingVector(List(0.1f, 0.2f), "voyage-4-lite", 2)))
    }
  }

  test("maps non-success provider responses to provider unavailability") {
    val app: HttpApp[IO] = Kleisli { (_: Request[IO]) => IO.pure(Response[IO](Status.TooManyRequests)) }
    val client = Client.fromHttpApp[IO](app)
    val service = new VoyageEmbeddingService(
      client,
      "test-key",
      endpoint,
      "voyage-4-lite",
      2,
      1.second,
      diagnostics = com.example.graphQL.cats.service.Diagnostics.noop
    )

    service.embed(input).map(result => assertEquals(result, Left(EmbeddingError.ProviderUnavailable)))
  }

  List(EmbeddingInputType.Document, EmbeddingInputType.Query).foreach { kind =>
    test(s"rejects a same-dimensional response from another model for $kind") {
      val app: HttpApp[IO] = Kleisli { (_: Request[IO]) =>
        IO.pure(jsonResponse(Status.Ok, """{"data":[{"embedding":[0.1,0.2]}],"model":"another-model"}"""))
      }
      val service = new VoyageEmbeddingService(
        Client.fromHttpApp[IO](app),
        "test-key",
        endpoint,
        "voyage-4-lite",
        2,
        1.second,
        diagnostics = Diagnostics.noop
      )
      service
        .embed(input.copy(inputType = kind))
        .map(result => assertEquals(result, Left(EmbeddingError.InvalidResponse)))
    }
  }

  test("an absent response model preserves the configured model") {
    val app: HttpApp[IO] = Kleisli { (_: Request[IO]) =>
      IO.pure(jsonResponse(Status.Ok, """{"data":[{"embedding":[0.1,0.2]}]}"""))
    }
    val service = new VoyageEmbeddingService(
      Client.fromHttpApp[IO](app),
      "test-key",
      endpoint,
      "voyage-4-lite",
      2,
      1.second,
      diagnostics = Diagnostics.noop
    )
    service
      .embed(input)
      .map(result => assertEquals(result, Right(EmbeddingVector(List(0.1f, 0.2f), "voyage-4-lite", 2))))
  }

  test("maps malformed or dimension-mismatched responses to invalid response") {
    val malformedApp: HttpApp[IO] = Kleisli { (_: Request[IO]) =>
      IO.pure(jsonResponse(Status.Ok, "not-json"))
    }
    val wrongDimensionApp: HttpApp[IO] = Kleisli { (_: Request[IO]) =>
      IO.pure(jsonResponse(Status.Ok, """{"data":[{"embedding":[0.1]}],"model":"voyage-4-lite"}"""))
    }
    val malformed = Client.fromHttpApp[IO](malformedApp)
    val wrongDimension = Client.fromHttpApp[IO](wrongDimensionApp)
    val malformedService = new VoyageEmbeddingService(
      malformed,
      "test-key",
      endpoint,
      "voyage-4-lite",
      2,
      1.second,
      diagnostics = com.example.graphQL.cats.service.Diagnostics.noop
    )
    val wrongDimensionService =
      new VoyageEmbeddingService(
        wrongDimension,
        "test-key",
        endpoint,
        "voyage-4-lite",
        2,
        1.second,
        diagnostics = com.example.graphQL.cats.service.Diagnostics.noop
      )

    for {
      malformedResult <- malformedService.embed(input)
      wrongDimensionResult <- wrongDimensionService.embed(input)
    } yield {
      assertEquals(malformedResult, Left(EmbeddingError.InvalidResponse))
      assertEquals(wrongDimensionResult, Left(EmbeddingError.InvalidResponse))
    }
  }

  List("null", "1e100", "-1e100").foreach { invalidValue =>
    test(s"rejects nonfinite vector component $invalidValue for document and query inputs") {
      val app: HttpApp[IO] = Kleisli { (_: Request[IO]) =>
        IO.pure(jsonResponse(Status.Ok, s"""{"data":[{"embedding":[$invalidValue,0.2]}]}"""))
      }
      val service = new VoyageEmbeddingService(
        Client.fromHttpApp[IO](app),
        "test-key",
        endpoint,
        "voyage-4-lite",
        2,
        1.second,
        diagnostics = com.example.graphQL.cats.service.Diagnostics.noop
      )

      for {
        document <- service.embed(input)
        query <- service.embed(input.copy(inputType = EmbeddingInputType.Query))
      } yield {
        assertEquals(document, Left(EmbeddingError.InvalidResponse))
        assertEquals(query, Left(EmbeddingError.InvalidResponse))
      }
    }
  }

  test("maps client timeouts to provider unavailability") {
    val app: HttpApp[IO] = Kleisli { (_: Request[IO]) => IO.never[Response[IO]] }
    val client = Client.fromHttpApp[IO](app)
    val service = new VoyageEmbeddingService(
      client,
      "test-key",
      endpoint,
      "voyage-4-lite",
      2,
      20.millis,
      diagnostics = com.example.graphQL.cats.service.Diagnostics.noop
    )

    service.embed(input).map(result => assertEquals(result, Left(EmbeddingError.ProviderUnavailable)))
  }

  test("reports a provider exception with sanitized failure fields") {
    val app: HttpApp[IO] = Kleisli { (_: Request[IO]) =>
      IO.raiseError[Response[IO]](new IllegalStateException("private provider detail"))
    }
    for {
      observed <- Ref.of[IO, List[(LogEvent, Map[LogField, String])]](Nil)
      diagnostics = new Diagnostics[IO] {
        override def event(
            event: LogEvent,
            requestId: Option[String],
            fields: => Map[LogField, String]
        ): IO[Unit] = observed.update(_ :+ (event -> fields))
      }
      service = new VoyageEmbeddingService(
        Client.fromHttpApp[IO](app),
        "test-key",
        endpoint,
        "voyage-4-lite",
        2,
        1.second,
        diagnostics = diagnostics
      )
      result <- service.embed(input)
      events <- observed.get
    } yield {
      assertEquals(result, Left(EmbeddingError.ProviderUnavailable))
      assertEquals(events.map(_._1), List(LogEvent.EmbeddingProviderFailed))
      assertEquals(events.headOption.flatMap(_._2.get(LogField.ErrorType)), Some("java.lang.IllegalStateException"))
      assertEquals(events.headOption.map(_._2.keySet), Some(LogFields.failure(new IllegalStateException()).keySet))
      assert(!events.exists(_._2.values.exists(_.contains("private provider detail"))))
    }
  }

  test("builds and releases the provider resource without contacting the endpoint") {
    VoyageEmbeddingService
      .resource("test-key", endpoint, "voyage-4-lite", 2, 1.second, diagnostics = Diagnostics.noop)
      .use(_ => IO.unit)
  }

  test("rejects multiple vectors for a single embedding input") {
    val app: HttpApp[IO] = Kleisli { (_: Request[IO]) =>
      IO.pure(jsonResponse(Status.Ok, """{"data":[{"embedding":[0.1,0.2]},{"embedding":[0.3,0.4]}]}"""))
    }
    provider(Client.fromHttpApp[IO](app))
      .embed(input)
      .map(result => assertEquals(result, Left(EmbeddingError.InvalidResponse)))
  }

  test("accepts a chunked response exactly at the byte limit") {
    val valid = """{"data":[{"embedding":[0.1,0.2]}]}"""
    val limit = 64 * 1024 + 32 * 2
    val body = valid + " " * (limit - valid.length)
    val app: HttpApp[IO] = Kleisli { (_: Request[IO]) =>
      IO.pure(
        jsonResponse(Status.Ok, body).withBodyStream(
          Stream.emits(body.getBytes(java.nio.charset.StandardCharsets.UTF_8)).covary[IO].chunkN(127).unchunks
        )
      )
    }
    provider(Client.fromHttpApp[IO](app))
      .embed(input)
      .map(result => assertEquals(result, Right(EmbeddingVector(List(0.1f, 0.2f), "voyage-4-lite", 2))))
  }

  test("rejects oversized chunked JSON before decoding and releases the response") {
    val valid = """{"data":[{"embedding":[0.1,0.2]}]}"""
    val limit = 64 * 1024 + 32 * 2
    for {
      released <- Ref.of[IO, Boolean](false)
      body = Stream
        .emits((valid + " " * (limit * 2)).getBytes(java.nio.charset.StandardCharsets.UTF_8))
        .covary[IO]
        .chunkN(127)
        .unchunks ++ Stream.raiseError[IO](new AssertionError("read beyond response budget"))
      client = Client[IO](_ =>
        Resource.make(
          IO.pure(jsonResponse(Status.Ok, "").withBodyStream(body))
        )(_ => released.set(true))
      )
      result <- provider(client).embed(input)
      wasReleased <- released.get
    } yield {
      assertEquals(result, Left(EmbeddingError.InvalidResponse))
      assert(wasReleased)
    }
  }

  private val breakerSettings = CircuitBreakerPolicy.Settings(2, 10.seconds, 100.millis, 1.second, 1)

  private def breakerFixture(responses: Ref[IO, List[Response[IO]]], calls: Ref[IO, Int]) =
    CircuitBreaker.create[IO](breakerSettings, _ => IO.unit).map { breaker =>
      val app: HttpApp[IO] = Kleisli { (_: Request[IO]) =>
        // The last scripted response repeats.
        calls.update(_ + 1) *> responses.modify {
          case head :: (tail @ (_ :: _)) => (tail, head)
          case other => (other, other.headOption.getOrElse(Response[IO](Status.InternalServerError)))
        }
      }
      breaker -> new VoyageEmbeddingService(
        Client.fromHttpApp[IO](app),
        "test-key",
        endpoint,
        "voyage-4-lite",
        2,
        1.second,
        diagnostics = Diagnostics.noop,
        breaker = breaker
      )
    }

  private def good = jsonResponse(Status.Ok, """{"data":[{"embedding":[0.1,0.2]}],"model":"voyage-4-lite"}""")

  test("CBR-03.1 after threshold provider failures the provider is not called and the error is unchanged") {
    for {
      responses <- Ref.of[IO, List[Response[IO]]](List(Response[IO](Status.ServiceUnavailable)))
      calls <- Ref.of[IO, Int](0)
      pair <- breakerFixture(responses, calls)
      (_, service) = pair
      first <- service.embed(input)
      second <- service.embed(input)
      third <- service.embed(input)
      count <- calls.get
    } yield {
      assertEquals(
        List(first, second, third),
        List(
          Left(EmbeddingError.ProviderUnavailable),
          Left(EmbeddingError.ProviderUnavailable),
          Left(EmbeddingError.CircuitOpen)
        )
      )
      assertEquals(count, 2)
    }
  }

  test("CBR-03.2 a 4xx response other than 429 is not counted") {
    for {
      responses <- Ref.of[IO, List[Response[IO]]](List(Response[IO](Status.Unauthorized)))
      calls <- Ref.of[IO, Int](0)
      pair <- breakerFixture(responses, calls)
      (breaker, service) = pair
      results <- List.fill(5)(service.embed(input)).sequence
      count <- calls.get
      state <- breaker.current
    } yield {
      assert(results.forall(_ == Left(EmbeddingError.ProviderUnavailable)))
      assertEquals(count, 5)
      assertEquals(state, CircuitBreakerPolicy.initial)
    }
  }

  test("CBR-03.1 429 counts and an invalid response does not") {
    for {
      invalid <- Ref.of[IO, List[Response[IO]]](List(jsonResponse(Status.Ok, """{"data":[]}""")))
      invalidCalls <- Ref.of[IO, Int](0)
      invalidPair <- breakerFixture(invalid, invalidCalls)
      invalidResults <- List.fill(4)(invalidPair._2.embed(input)).sequence
      invalidState <- invalidPair._1.current
      limited <- Ref.of[IO, List[Response[IO]]](List(Response[IO](Status.TooManyRequests)))
      limitedCalls <- Ref.of[IO, Int](0)
      limitedPair <- breakerFixture(limited, limitedCalls)
      _ <- List.fill(3)(limitedPair._2.embed(input)).sequence
      limitedCount <- limitedCalls.get
    } yield {
      assert(invalidResults.forall(_ == Left(EmbeddingError.InvalidResponse)))
      assertEquals(invalidState, CircuitBreakerPolicy.initial)
      assertEquals(limitedCount, 2)
    }
  }

  test("CBR-03.3 after the open period one successful probe restores normal calls") {
    for {
      responses <- Ref.of[IO, List[Response[IO]]](
        List(Response[IO](Status.InternalServerError), Response[IO](Status.InternalServerError), good)
      )
      calls <- Ref.of[IO, Int](0)
      pair <- breakerFixture(responses, calls)
      (breaker, service) = pair
      _ <- service.embed(input)
      _ <- service.embed(input)
      rejected <- service.embed(input)
      _ <- IO.sleep(200.millis)
      probe <- service.embed(input)
      again <- service.embed(input)
      state <- breaker.current
    } yield {
      assertEquals(rejected, Left(EmbeddingError.CircuitOpen))
      assertEquals(probe, Right(EmbeddingVector(List(0.1f, 0.2f), "voyage-4-lite", 2)))
      assertEquals(again, probe)
      assertEquals(state, CircuitBreakerPolicy.initial)
    }
  }

  private def provider(client: Client[IO]): VoyageEmbeddingService =
    new VoyageEmbeddingService(
      client,
      "test-key",
      endpoint,
      "voyage-4-lite",
      2,
      1.second,
      diagnostics = Diagnostics.noop
    )

  private def jsonResponse(status: Status, body: String): Response[IO] =
    Response[IO](status).withEntity(body).putHeaders(Header.Raw(CIString("Content-Type"), "application/json"))
}
