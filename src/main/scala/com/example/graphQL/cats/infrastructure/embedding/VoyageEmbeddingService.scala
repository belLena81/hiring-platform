package com.example.graphQL.cats.infrastructure.embedding

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.repository.protocol.{EmbeddingError, EmbeddingInput, EmbeddingInputType, EmbeddingService, EmbeddingVector}
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.deriveDecoder
import org.http4s.{AuthScheme, Credentials, Headers, Method, Request, Uri}
import org.http4s.client.Client
import org.http4s.circe.*
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.headers.Authorization
import org.typelevel.otel4s.context.propagation.TextMapUpdater
import org.typelevel.otel4s.trace.Tracer
import org.typelevel.ci.CIString

import scala.concurrent.duration.*

final class VoyageEmbeddingService(
    client: Client[IO],
    apiKey: String,
    endpoint: Uri,
    model: String,
    dimension: Int,
    timeout: FiniteDuration,
    tracer: Tracer[IO] = Tracer.noop[IO]
) extends EmbeddingService[IO] {
  private given TextMapUpdater[Headers] with
    def updated(headers: Headers, key: String, value: String): Headers =
      headers.put(org.http4s.Header.Raw(CIString(key), value))

  override def embed(input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]] = {
    val request = Request[IO](Method.POST, endpoint)
      .withHeaders(Authorization(Credentials.Token(AuthScheme.Bearer, apiKey)))
      .withEntity(VoyageEmbeddingRequest(
        input = input.text,
        model = model,
        inputType = inputType(input.inputType),
        outputDimension = dimension,
        outputDtype = "float",
        truncation = true
      ))(using jsonEncoderOf[IO, VoyageEmbeddingRequest])

    tracer.span("voyage.embeddings").surround {
      tracer.propagate(request.headers).flatMap { propagatedHeaders =>
      client.run(request.withHeaders(propagatedHeaders)).use { response =>
      if (!response.status.isSuccess)
        IO.pure(Left(EmbeddingError.ProviderUnavailable))
      else
        response.attemptAs[VoyageEmbeddingResponse](using jsonOf[IO, VoyageEmbeddingResponse]).value.map { decoded =>
          decoded.leftMap(_ => EmbeddingError.InvalidResponse).flatMap(validate)
        }
      }.timeout(timeout).handleError(_ => Left(EmbeddingError.ProviderUnavailable))
      }
    }
  }

  private def validate(response: VoyageEmbeddingResponse): Either[EmbeddingError, EmbeddingVector] =
    response.data.headOption
      .map { item =>
        val values = item.embedding
        Either.cond(
          values.size == dimension,
          EmbeddingVector(values, response.model.getOrElse(model), values.size),
          EmbeddingError.InvalidResponse
        )
      }
      .getOrElse(Left(EmbeddingError.InvalidResponse))

  private def inputType(inputType: EmbeddingInputType): String =
    inputType match {
      case EmbeddingInputType.Query => "query"
      case EmbeddingInputType.Document => "document"
    }
}

private final case class VoyageEmbeddingRequest(
    input: String,
    model: String,
    inputType: String,
    outputDimension: Int,
    outputDtype: String,
    truncation: Boolean
)

private object VoyageEmbeddingRequest {
  given Encoder[VoyageEmbeddingRequest] = Encoder.forProduct6(
    "input", "model", "input_type", "output_dimension", "output_dtype", "truncation"
  )(request => (
    request.input,
    request.model,
    request.inputType,
    request.outputDimension,
    request.outputDtype,
    request.truncation
  ))
}

private final case class VoyageEmbeddingItem(embedding: List[Float])

private object VoyageEmbeddingItem {
  given Decoder[VoyageEmbeddingItem] = deriveDecoder
}

private final case class VoyageEmbeddingResponse(
    data: List[VoyageEmbeddingItem],
    model: Option[String]
)

private object VoyageEmbeddingResponse {
  given Decoder[VoyageEmbeddingResponse] = deriveDecoder
}

object VoyageEmbeddingService {
  def resource(
      apiKey: String,
      endpoint: String,
      model: String,
      dimension: Int,
      timeout: FiniteDuration,
      tracer: Tracer[IO] = Tracer.noop[IO]
  ): Resource[IO, EmbeddingService[IO]] =
    Resource.eval(IO.fromEither(
      Uri.fromString(endpoint).leftMap(_ => new IllegalArgumentException("Invalid Voyage embedding endpoint"))
    )).flatMap { uri =>
      EmberClientBuilder.default[IO]
        .withTimeout(timeout)
        .build
        .map(client => new VoyageEmbeddingService(client, apiKey, uri, model, dimension, timeout, tracer))
    }

}
