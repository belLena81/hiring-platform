package com.example.graphQL.cats.infrastructure.embedding

import cats.effect.{IO, Ref}
import cats.data.Kleisli
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
        body <- request.as[Json]
        _ = assertEquals(request.method, Method.POST)
        _ = assert(request.headers.get(CIString("Authorization")).nonEmpty)
        _ = assertEquals(body.hcursor.get[String]("input"), Right("Scala"))
        _ = assertEquals(body.hcursor.get[String]("input_type"), Right("document"))
        _ = assertEquals(body.hcursor.get[Int]("output_dimension"), Right(2))
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
      diagnostics = new Diagnostics {
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

  test("rejects an invalid endpoint while constructing the provider resource") {
    VoyageEmbeddingService
      .resource(
        "test-key",
        "not a URI",
        "voyage-4-lite",
        2,
        1.second,
        diagnostics = com.example.graphQL.cats.service.Diagnostics.noop
      )
      .use(_ => IO.raiseError[Unit](new AssertionError("invalid endpoint was accepted")))
      .attempt
      .map(result => assert(result.isLeft))
  }

  private def jsonResponse(status: Status, body: String): Response[IO] =
    Response[IO](status).withEntity(body).putHeaders(Header.Raw(CIString("Content-Type"), "application/json"))
}
