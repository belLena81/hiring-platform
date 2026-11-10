package com.example.graphQL.cats.service.search

import cats.effect.{IO, Resource}
import com.example.graphQL.cats.infrastructure.embedding.{MeteredEmbedding, MeteredEmbeddingService}
import com.example.graphQL.cats.service.port.{EmbeddingError, EmbeddingInput, EmbeddingInputType}
import com.typesafe.config.Config
import io.circe.Json
import org.http4s.Uri

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.time.Instant
import java.util.Locale
import scala.concurrent.duration.*
import scala.util.Try

/** Why a provider-embedding evaluation capture was refused or stopped. Every case has a fixed message: no message,
  * `toString` or record can carry the key, an input text, a vector or an exception text.
  */
enum ProviderEmbeddingRefusal(val message: String) {
  case NotAuthorized
      extends ProviderEmbeddingRefusal(
        "Provider embeddings need the explicit --authorize-provider-embeddings flag; no provider call was made."
      )
  case MissingAbsoluteP95Budget
      extends ProviderEmbeddingRefusal(
        "Provider embeddings need --absolute-p95-budget-ms (whole milliseconds, 2000 to 5000); no provider call was made."
      )
  case BudgetOutOfRange
      extends ProviderEmbeddingRefusal(
        "--absolute-p95-budget-ms must be a whole number of milliseconds from 2000 to 5000; no provider call was made."
      )
  case MissingNoiseAllowance
      extends ProviderEmbeddingRefusal(
        "Provider embeddings need --latency-noise-allowance-ms (whole milliseconds, 0 to 60000); no provider call was made."
      )
  case InvalidOption
      extends ProviderEmbeddingRefusal(
        "A provider-embedding option is missing its value or has an invalid value; no provider call was made."
      )
  case RepeatedOption
      extends ProviderEmbeddingRefusal(
        "A provider-embedding option was given more than once; no provider call was made."
      )
  case MissingApiKey
      extends ProviderEmbeddingRefusal(
        "Provider embeddings need vector-search.voyage.api-key (an unscoped model API key); no provider call was made."
      )
  case InvalidEndpoint
      extends ProviderEmbeddingRefusal(
        "vector-search.voyage.endpoint must be an https URL on ai.mongodb.com or api.voyageai.com; no provider call was made."
      )
  case MissingAttestation
      extends ProviderEmbeddingRefusal(
        "Provider embeddings need vector-search.voyage.data-collection-attestation = on|off (the owner's Atlas " +
          "data-collection setting); no provider call was made."
      )
  case InvalidConfiguration
      extends ProviderEmbeddingRefusal(
        "The vector-search.voyage configuration is incomplete or invalid; no provider call was made."
      )
  case DimensionMismatch
      extends ProviderEmbeddingRefusal("The provider returned a vector of an unexpected dimension or model.")
  case EmbeddingFailed
      extends ProviderEmbeddingRefusal("The provider could not embed an input; the capture was stopped.")
  case EvidenceNotWritten
      extends ProviderEmbeddingRefusal(
        "The provider requests were billed but the evidence record could not be written; stopped before Atlas, " +
          "check the provider usage dashboard."
      )
  case UnexpectedFailure
      extends ProviderEmbeddingRefusal("The provider capture failed unexpectedly; details were suppressed.")
}

/** The owner's Atlas data-collection setting: an attestation recorded in configuration and in the evidence record. It
  * is recorded, not enforced and not verified by the system.
  */
enum DataCollectionAttestation(val text: String) {
  case On extends DataCollectionAttestation("on")
  case Off extends DataCollectionAttestation("off")
}

/** Policy values and pacing for a provider-generated capture. There are no defaults for the budget or the allowance. */
final case class ProviderEmbeddingOptions(
    absoluteP95Budget: FiniteDuration,
    latencyNoiseAllowanceMillis: Double,
    requestPause: FiniteDuration
)

object ProviderEmbeddingOptions {
  val AuthorizationFlag = "--authorize-provider-embeddings"
  private val BudgetFlag = "--absolute-p95-budget-ms"
  private val AllowanceFlag = "--latency-noise-allowance-ms"
  private val PauseFlag = "--embedding-pause-ms"
  private val flags = Set(AuthorizationFlag, BudgetFlag, AllowanceFlag, PauseFlag)

  /** Owner's range for the absolute p95 budget, inclusive, in whole milliseconds. */
  val BudgetRangeMillis: (Long, Long) = (2000L, 5000L)
  val AllowanceMaxMillis: Long = 60000L
  val PauseMaxMillis: Long = 600000L

  /** ASCII digits with an optional leading minus: a leading plus and non-ASCII digits are refused. */
  private def wholeMillis(text: String): Option[Long] =
    Option.when(text.matches("-?[0-9]+"))(text).flatMap(_.toLongOption)

  /** Splits the provider options from the rest of the arguments. `None` means provider mode was not requested; any
    * provider option without the authorization flag is refused, so an option can never half-enable a provider call.
    * Values are whole milliseconds; a repeated flag is refused rather than resolved first-wins.
    */
  def extract(
      args: List[String]
  ): Either[ProviderEmbeddingRefusal, (Option[ProviderEmbeddingOptions], List[String])] = {
    def valueOf(flag: String): Either[ProviderEmbeddingRefusal, Option[String]] =
      args.indexOf(flag) match {
        case -1                                                                 => Right(None)
        case index if index + 1 < args.size && !flags.contains(args(index + 1)) => Right(Some(args(index + 1)))
        case _ => Left(ProviderEmbeddingRefusal.InvalidOption)
      }
    def stripped: List[String] = {
      def loop(rest: List[String]): List[String] = rest match {
        case flag :: _ :: tail if flag == BudgetFlag || flag == AllowanceFlag || flag == PauseFlag => loop(tail)
        case AuthorizationFlag :: tail                                                             => loop(tail)
        case other :: tail => other :: loop(tail)
        case Nil           => Nil
      }
      loop(args)
    }
    def millis(text: String, min: Long, max: Long): Either[ProviderEmbeddingRefusal, Long] =
      wholeMillis(text).filter(value => value >= min && value <= max).toRight(ProviderEmbeddingRefusal.InvalidOption)
    val requested = args.exists(flags.contains)
    if (!requested) Right((None, args))
    else if (!args.contains(AuthorizationFlag)) Left(ProviderEmbeddingRefusal.NotAuthorized)
    else if (flags.exists(flag => args.count(_ == flag) > 1)) Left(ProviderEmbeddingRefusal.RepeatedOption)
    else
      for {
        budgetText <- valueOf(BudgetFlag)
        allowanceText <- valueOf(AllowanceFlag)
        pauseText <- valueOf(PauseFlag)
        budget <- budgetText
          .toRight(ProviderEmbeddingRefusal.MissingAbsoluteP95Budget)
          .flatMap(text =>
            wholeMillis(text)
              .toRight(ProviderEmbeddingRefusal.InvalidOption)
              .filterOrElse(
                value => value >= BudgetRangeMillis._1 && value <= BudgetRangeMillis._2,
                ProviderEmbeddingRefusal.BudgetOutOfRange
              )
          )
        allowance <- allowanceText
          .toRight(ProviderEmbeddingRefusal.MissingNoiseAllowance)
          .flatMap(millis(_, 0L, AllowanceMaxMillis))
        pause <- pauseText.fold[Either[ProviderEmbeddingRefusal, Long]](Right(0L))(millis(_, 0L, PauseMaxMillis))
      } yield (
        Some(ProviderEmbeddingOptions(budget.millis, allowance.toDouble, pause.millis)),
        stripped
      )
  }
}

/** Provider settings read from configuration. The key is never part of `toString`. */
final class ProviderEmbeddingConfig private (
    val apiKey: String,
    val endpoint: Uri,
    val model: String,
    val dimension: Int,
    val timeout: FiniteDuration,
    val attestation: DataCollectionAttestation
) {
  override def toString: String =
    s"ProviderEmbeddingConfig(${endpoint.host.fold("?")(_.renderString)}, $model, $dimension, [REDACTED])"
}

object ProviderEmbeddingConfig {
  private val Section = "vector-search.voyage"

  /** The only hosts the key may be sent to. */
  val AllowedHosts: Set[String] = Set("ai.mongodb.com", "api.voyageai.com")

  /** Reads `vector-search.voyage.*` and `vector-search.embedding.timeout-ms`. The attestation key
    * `data-collection-attestation` (`on` or `off`) is required for a provider-generated capture: a missing or invalid
    * value is a typed refusal, never a default. It is absent from `application.conf` by default. The endpoint must be
    * https, on an allowed host and without user information.
    */
  /** The text between `://` and the first `/`, `?` or `#`: where a host (or hidden user information) would sit. */
  private def authorityOf(value: String): String =
    value.dropWhile(_ != ':').drop(3).takeWhile(c => c != '/' && c != '?' && c != '#')

  def read(config: Config): Either[ProviderEmbeddingRefusal, ProviderEmbeddingConfig] = {
    def text(key: String): Option[String] =
      Try(config.getString(s"$Section.$key")).toOption.map(_.trim).filter(_.nonEmpty)
    for {
      key <- text("api-key").toRight(ProviderEmbeddingRefusal.MissingApiKey)
      endpoint <- text("endpoint")
        .filter(value => !authorityOf(value).contains('%')) // a percent-encoded host is refused, not decoded
        .flatMap(value => Uri.fromString(value).toOption)
        .filter(uri =>
          uri.scheme.exists(_.value.toLowerCase(Locale.ROOT) == "https") &&
            uri.host.exists { host =>
              val name = host.renderString
              name.forall(_ < 128) && AllowedHosts.contains(name.toLowerCase(Locale.ROOT))
            } &&
            uri.authority.forall(_.userInfo.isEmpty)
        )
        .toRight(ProviderEmbeddingRefusal.InvalidEndpoint)
      model <- text("model").toRight(ProviderEmbeddingRefusal.InvalidConfiguration)
      dimension <- Try(config.getInt(s"$Section.dimension")).toOption
        .filter(_ > 0)
        .toRight(ProviderEmbeddingRefusal.InvalidConfiguration)
      timeout <- Try(config.getInt("vector-search.embedding.timeout-ms")).toOption
        .filter(_ > 0)
        .toRight(ProviderEmbeddingRefusal.InvalidConfiguration)
      attestation <- text("data-collection-attestation")
        .toRight(ProviderEmbeddingRefusal.MissingAttestation)
        .flatMap {
          _.toLowerCase(Locale.ROOT) match {
            case "on"  => Right(DataCollectionAttestation.On)
            case "off" => Right(DataCollectionAttestation.Off)
            case _     => Left(ProviderEmbeddingRefusal.MissingAttestation)
          }
        }
    } yield new ProviderEmbeddingConfig(key, endpoint, model, dimension, timeout.millis, attestation)
  }
}

/** One provider input: a corpus document or a captured query text. `id` is the entity or query identifier. */
final case class ProviderEmbeddingRequest(id: String, text: String, inputType: EmbeddingInputType)

/** The provider-reported token count for one answered input; `None` when the response carried no usable usage. */
final case class ProviderEmbeddingUsage(id: String, inputType: EmbeddingInputType, totalTokens: Option[Long])

final case class ProviderEmbeddingResult(
    vectors: Map[(EmbeddingInputType, String), List[Float]],
    usage: List[ProviderEmbeddingUsage]
)

/** A stopped capture with the progress made so far. `config` is absent, and `requestsMade` is 0, when the capture was
  * refused before any provider request. `requestsMade` counts every attempted request, including the failing one;
  * `usage` holds the inputs the provider answered, so billed tokens are not lost on a failed run.
  */
final case class ProviderEmbeddingFailure(
    refusal: ProviderEmbeddingRefusal,
    config: Option[ProviderEmbeddingConfig],
    requestsMade: Int,
    usage: List[ProviderEmbeddingUsage]
)

object ProviderEmbeddingFailure {
  def refused(refusal: ProviderEmbeddingRefusal): ProviderEmbeddingFailure =
    ProviderEmbeddingFailure(refusal, None, 0, Nil)
}

object ProviderEmbeddingCapture {
  private final case class Progress(
      attempted: Int,
      usage: List[ProviderEmbeddingUsage],
      vectors: Map[(EmbeddingInputType, String), List[Float]]
  )

  /** `answered` is the provider-reported token count when the provider answered, even if the vector was then refused.
    */
  private final case class Attempt(
      vector: Either[ProviderEmbeddingRefusal, List[Float]],
      answered: Option[Option[Long]]
  )

  /** Embeds every request in order, one provider call each, and stops at the first failure. The configuration is read
    * inside `IO`, and the provider client is opened only after authorization, so a refusal makes no call. A vector
    * whose dimension or model differs from the configured one fails closed. `pause` runs between two requests when the
    * configured pause is above zero.
    */
  def embedAll(
      options: Option[ProviderEmbeddingOptions],
      configuration: IO[Either[ProviderEmbeddingRefusal, ProviderEmbeddingConfig]],
      open: ProviderEmbeddingConfig => Resource[IO, MeteredEmbeddingService],
      requests: List[ProviderEmbeddingRequest],
      pause: FiniteDuration => IO[Unit] = IO.sleep(_)
  ): IO[Either[ProviderEmbeddingFailure, (ProviderEmbeddingConfig, ProviderEmbeddingResult)]] =
    options match {
      case None         => IO.pure(Left(ProviderEmbeddingFailure.refused(ProviderEmbeddingRefusal.NotAuthorized)))
      case Some(chosen) =>
        configuration.flatMap {
          case Left(refusal)   => IO.pure(Left(ProviderEmbeddingFailure.refused(refusal)))
          case Right(provider) =>
            open(provider).use { embedder =>
              def loop(
                  rest: List[ProviderEmbeddingRequest],
                  progress: Progress
              ): IO[Either[ProviderEmbeddingFailure, Progress]] =
                rest match {
                  case Nil             => IO.pure(Right(progress))
                  case request :: tail =>
                    embedOne(embedder, provider, request).flatMap { attempt =>
                      val next = progress.copy(
                        attempted = progress.attempted + 1,
                        usage = progress.usage ++ attempt.answered.map(
                          ProviderEmbeddingUsage(request.id, request.inputType, _)
                        )
                      )
                      attempt.vector match {
                        case Left(refusal) =>
                          IO.pure(Left(ProviderEmbeddingFailure(refusal, Some(provider), next.attempted, next.usage)))
                        case Right(vector) =>
                          val stored = next.copy(vectors = next.vectors + ((request.inputType, request.id) -> vector))
                          (if (tail.nonEmpty && chosen.requestPause > Duration.Zero) pause(chosen.requestPause)
                           else IO.unit) *> loop(tail, stored)
                      }
                    }
                }
              loop(requests, Progress(0, Nil, Map.empty))
                .map(_.map(done => provider -> ProviderEmbeddingResult(done.vectors, done.usage)))
            }
        }
    }

  private def embedOne(
      embedder: MeteredEmbeddingService,
      provider: ProviderEmbeddingConfig,
      request: ProviderEmbeddingRequest
  ): IO[Attempt] =
    embedder
      .embedMetered(EmbeddingInput(request.text, request.inputType))
      .handleError(_ => Left(EmbeddingError.ProviderUnavailable))
      .map {
        case Left(_)                                 => Attempt(Left(ProviderEmbeddingRefusal.EmbeddingFailed), None)
        case Right(MeteredEmbedding(vector, tokens)) =>
          Attempt(
            Either.cond(
              vector.dimension == provider.dimension && vector.values.size == provider.dimension &&
                vector.model == provider.model,
              vector.values,
              ProviderEmbeddingRefusal.DimensionMismatch
            ),
            Some(tokens)
          )
      }
}

/** The evidence record for a provider-generated capture. It holds counts, identifiers and the owner's attestation, and
  * never the key, a vector, an input text or an exception text.
  */
object ProviderEmbeddingEvidence {
  final case class Totals(requests: Int, totalTokens: Long, inputsWithoutUsage: Int)

  def totals(usage: List[ProviderEmbeddingUsage]): Totals =
    Totals(usage.size, usage.flatMap(_.totalTokens).sum, usage.count(_.totalTokens.isEmpty))

  def render(
      config: ProviderEmbeddingConfig,
      options: ProviderEmbeddingOptions,
      usage: List[ProviderEmbeddingUsage],
      captureBound: Int,
      k: Int
  ): Json = record(config, options, "Completed", None, usage.size, usage, captureBound, k)

  /** The record of a fully answered embedding run. It is written right after the last provider answer, before any
    * database exists, so a later stop cannot lose the billed-token evidence.
    */
  def writeCompleted(
      directory: Path,
      concurrency: Int,
      timestamp: Instant,
      config: ProviderEmbeddingConfig,
      options: ProviderEmbeddingOptions,
      usage: List[ProviderEmbeddingUsage],
      captureBound: Int,
      k: Int
  ): IO[Path] =
    IO.blocking {
      Files.createDirectories(directory)
      Files.writeString(
        directory.resolve(s"provider-embedding-evidence-$concurrency-${timestamp.toEpochMilli}.json"),
        render(config, options, usage, captureBound, k).spaces2,
        StandardCharsets.UTF_8
      )
    }

  /** Writes the completed record first and only then runs `continue`. If the write fails, `continue` never runs (no
    * Atlas work after unrecorded billing) and the result is the fixed `EvidenceNotWritten` refusal. A failure of
    * `continue` propagates unchanged; the record already exists.
    */
  def recordThen[A](write: IO[Path], continue: IO[A]): IO[Either[ProviderEmbeddingRefusal, A]] =
    write.attempt.flatMap {
      case Left(_)  => IO.pure(Left(ProviderEmbeddingRefusal.EvidenceNotWritten))
      case Right(_) => continue.map(Right(_))
    }

  /** The counts-only record of a capture that stopped after at least one request. */
  def renderFailure(
      config: ProviderEmbeddingConfig,
      options: ProviderEmbeddingOptions,
      failure: ProviderEmbeddingFailure,
      captureBound: Int,
      k: Int
  ): Json =
    record(
      config,
      options,
      failure.refusal.toString,
      Some(failure.refusal.message),
      failure.requestsMade,
      failure.usage,
      captureBound,
      k
    )

  def writeFailure(
      directory: Path,
      concurrency: Int,
      timestamp: Instant,
      config: ProviderEmbeddingConfig,
      options: ProviderEmbeddingOptions,
      failure: ProviderEmbeddingFailure,
      captureBound: Int,
      k: Int
  ): IO[Path] =
    IO.blocking {
      Files.createDirectories(directory)
      Files.writeString(
        directory.resolve(s"provider-embedding-evidence-failed-$concurrency-${timestamp.toEpochMilli}.json"),
        renderFailure(config, options, failure, captureBound, k).spaces2,
        StandardCharsets.UTF_8
      )
    }

  private def record(
      config: ProviderEmbeddingConfig,
      options: ProviderEmbeddingOptions,
      outcome: String,
      failureMessage: Option[String],
      requests: Int,
      usage: List[ProviderEmbeddingUsage],
      captureBound: Int,
      k: Int
  ): Json = {
    val summary = totals(usage)
    def entries(kind: EmbeddingInputType): Json = Json.arr(
      usage
        .filter(_.inputType == kind)
        .map(entry =>
          Json.obj(
            "id" -> Json.fromString(entry.id),
            "totalTokens" -> entry.totalTokens.fold(Json.Null)(Json.fromLong)
          )
        )*
    )
    Json.obj(
      "outcome" -> Json.fromString(outcome),
      "failureMessage" -> failureMessage.fold(Json.Null)(Json.fromString),
      "embeddingProvenance" -> Json.fromString(SearchEvaluationEmbeddingProvenance.ProviderGenerated.toString),
      "endpointHost" -> Json.fromString(config.endpoint.host.fold("unknown")(_.renderString)),
      "model" -> Json.fromString(config.model),
      "dimension" -> Json.fromInt(config.dimension),
      "dataCollectionAttestation" -> Json.fromString(config.attestation.text),
      "dataCollectionAttestationKind" -> Json.fromString("owner attestation, recorded and not verified"),
      "absoluteP95BudgetMillis" -> Json.fromLong(options.absoluteP95Budget.toMillis),
      "latencyNoiseAllowanceMillis" -> Json.fromDoubleOrNull(options.latencyNoiseAllowanceMillis),
      "requestPauseMillis" -> Json.fromLong(options.requestPause.toMillis),
      "captureBound" -> Json.fromInt(captureBound),
      "k" -> Json.fromInt(k),
      "providerRequests" -> Json.fromInt(requests),
      "totalTokens" -> Json.fromLong(summary.totalTokens),
      "inputsWithoutUsage" -> Json.fromInt(summary.inputsWithoutUsage),
      "documentTokens" -> entries(EmbeddingInputType.Document),
      "queryTokens" -> entries(EmbeddingInputType.Query)
    )
  }
}
