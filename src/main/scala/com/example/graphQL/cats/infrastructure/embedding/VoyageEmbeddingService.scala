package com.example.graphQL.cats.infrastructure.embedding

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import com.example.graphQL.cats.service.port.{
  EmbeddingError,
  EmbeddingInput,
  EmbeddingInputType,
  EmbeddingService,
  EmbeddingVector
}
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogFields}
import com.example.graphQL.cats.service.Diagnostics.*
import com.example.graphQL.cats.service.resilience.CircuitBreaker
import scala.util.control.NoStackTrace
import io.circe.{Decoder, Encoder}
import io.circe.derivation.{Configuration, ConfiguredEncoder}
import org.http4s.{AuthScheme, Credentials, Headers, Method, Request, Uri}
import org.http4s.client.Client
import org.http4s.circe.*
import org.http4s.circe.CirceEntityEncoder.circeEntityEncoder
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
    tracer: Tracer[IO] = Tracer.noop[IO],
    diagnostics: Diagnostics[IO],
    breaker: CircuitBreaker[IO] = CircuitBreaker.disabled[IO]
) extends EmbeddingService[IO] {
  private given TextMapUpdater[Headers] with
    def updated(headers: Headers, key: String, value: String): Headers =
      headers.put(org.http4s.Header.Raw(CIString(key), value))

  override def embed(input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]] = {
    val request = Request[IO](Method.POST, endpoint)
      .withHeaders(Authorization(Credentials.Token(AuthScheme.Bearer, apiKey)))
      .withEntity(
        VoyageEmbeddingRequest(
          input = input.text,
          model = model,
          inputType = input.inputType,
          outputDimension = dimension,
          outputDtype = "float",
          truncation = true
        )
      )(using circeEntityEncoder[IO, VoyageEmbeddingRequest])

    tracer.span("voyage.embeddings").surround {
      tracer.propagate(request.headers).flatMap { propagatedHeaders =>
        // Only timeouts, transport failures, 5xx and 429 are raised, so only they count against the breaker.
        val call = client
          .run(request.withHeaders(propagatedHeaders))
          .use { response =>
            if (response.status.code == 429 || response.status.code >= 500)
              IO.raiseError(VoyageEmbeddingService.ProviderOverloaded)
            else if (!response.status.isSuccess)
              IO.pure(Left(EmbeddingError.ProviderUnavailable))
            else
              response.body.take(maximumResponseBytes + 1L).compile.to(Chunk).flatMap { bytes =>
                if (bytes.size.toLong > maximumResponseBytes) IO.pure(Left(EmbeddingError.InvalidResponse))
                else
                  response
                    .withBodyStream(Stream.chunk(bytes).covary[IO])
                    .attemptAs[VoyageEmbeddingResponse](using jsonOf[IO, VoyageEmbeddingResponse])
                    .value
                    .map { decoded =>
                      decoded.leftMap(_ => EmbeddingError.InvalidResponse).flatMap(validate)
                    }
              }
          }
          .timeout(timeout)
        breaker
          .protect(call)(CircuitBreaker.classifier(_ => true))
          .map(_.leftMap(_ => EmbeddingError.CircuitOpen).flatten)
          .handleErrorWith(error =>
            diagnostics
              .emit(LogEvent.EmbeddingProviderFailed, fields = LogFields.failure(error))
              .as(Left(EmbeddingError.ProviderUnavailable))
          )
      }
    }
  }

  // Headroom over ~25 JSON characters per float for the single embedding plus response metadata.
  private val maximumResponseBytes: Long = 64L * 1024L + 32L * dimension.toLong

  private def validate(response: VoyageEmbeddingResponse): Either[EmbeddingError, EmbeddingVector] =
    response.data match {
      case item :: Nil =>
        val values = item.embedding
        Either.cond(
          values.size == dimension && values.forall(_.isFinite) && response.model.forall(_ == model),
          EmbeddingVector(values, response.model.getOrElse(model), values.size),
          EmbeddingError.InvalidResponse
        )
      case _ => Left(EmbeddingError.InvalidResponse)
    }
}

/** Field names are the provider's snake_case wire names; `outputDtype` and `truncation` are fixed by the contract. */
private final case class VoyageEmbeddingRequest(
    input: String,
    model: String,
    inputType: EmbeddingInputType,
    outputDimension: Int,
    outputDtype: String,
    truncation: Boolean
) derives ConfiguredEncoder

private object VoyageEmbeddingRequest {
  private given Configuration = Configuration.default.withSnakeCaseMemberNames
  given Encoder[EmbeddingInputType] = Encoder[String].contramap(_.toString.toLowerCase)
}

private final case class VoyageEmbeddingItem(embedding: List[Float]) derives Decoder

private final case class VoyageEmbeddingResponse(
    data: List[VoyageEmbeddingItem],
    model: Option[String]
) derives Decoder

object VoyageEmbeddingService {

  /** The provider answered 429 or 5xx; raised inside the breaker so it counts, then mapped to unavailable. */
  private case object ProviderOverloaded extends RuntimeException("embedding provider overloaded") with NoStackTrace

  def resource(
      apiKey: String,
      endpoint: Uri,
      model: String,
      dimension: Int,
      timeout: FiniteDuration,
      tracer: Tracer[IO] = Tracer.noop[IO],
      diagnostics: Diagnostics[IO],
      breaker: CircuitBreaker[IO] = CircuitBreaker.disabled[IO]
  ): Resource[IO, EmbeddingService[IO]] =
    EmberClientBuilder
      .default[IO]
      .withTimeout(timeout)
      .build
      .map(client =>
        new VoyageEmbeddingService(client, apiKey, endpoint, model, dimension, timeout, tracer, diagnostics, breaker)
      )
}
