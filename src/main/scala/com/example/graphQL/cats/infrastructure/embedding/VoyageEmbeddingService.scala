package com.example.graphQL.cats.infrastructure.embedding

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.repository.protocol.{EmbeddingError, EmbeddingInput, EmbeddingInputType, EmbeddingService, EmbeddingVector}
import io.circe.Json
import io.circe.parser.parse
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration

final class VoyageEmbeddingService(
    apiKey: String,
    endpoint: String,
    model: String,
    dimension: Int,
    timeoutMillis: Int,
    client: HttpClient = HttpClient.newHttpClient()
) extends EmbeddingService[IO] {
  override def embed(input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]] = {
    val request = HttpRequest.newBuilder(URI.create(endpoint))
      .timeout(Duration.ofMillis(timeoutMillis.toLong))
      .header("Content-Type", "application/json")
      .header("Authorization", s"Bearer $apiKey")
      .POST(HttpRequest.BodyPublishers.ofString(payload(input).noSpaces))
      .build()

    IO.blocking(client.send(request, HttpResponse.BodyHandlers.ofString()))
      .map(response => decode(response.statusCode(), response.body()))
      .handleError(_ => Left(EmbeddingError.ProviderUnavailable))
  }

  private def payload(input: EmbeddingInput): Json =
    Json.obj(
      "input" -> Json.fromString(input.text),
      "model" -> Json.fromString(model),
      "input_type" -> Json.fromString(inputType(input.inputType)),
      "output_dimension" -> Json.fromInt(dimension),
      "output_dtype" -> Json.fromString("float"),
      "truncation" -> Json.fromBoolean(true)
    )

  private def inputType(inputType: EmbeddingInputType): String =
    inputType match {
      case EmbeddingInputType.Query => "query"
      case EmbeddingInputType.Document => "document"
    }

  private def decode(status: Int, body: String): Either[EmbeddingError, EmbeddingVector] =
    if (status < 200 || status >= 300) Left(EmbeddingError.ProviderUnavailable)
    else {
      parse(body).flatMap { json =>
        val cursor = json.hcursor
        for {
          data <- cursor.downField("data").as[List[Json]]
          first <- data.headOption.toRight(io.circe.DecodingFailure("missing embedding", cursor.history))
          values <- first.hcursor.downField("embedding").as[List[Float]]
          responseModel <- cursor.downField("model").as[Option[String]]
        } yield EmbeddingVector(values, responseModel.getOrElse(model), values.size)
      }.leftMap(_ => EmbeddingError.InvalidResponse).flatMap { vector =>
        Either.cond(vector.dimension == dimension, vector, EmbeddingError.InvalidResponse)
      }
    }
}
