package com.example.hiring.analytics.config

import com.example.hiring.analytics.domain.{AnalyticsReplayRequestId, AnalyticsTopic, RunId, WriterDisposition}
import com.example.hiring.analytics.errors.AnalyticsError

import cats.data.{Chain, NonEmptyChain, ValidatedNec}
import cats.syntax.all.*
import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.any.Not
import io.github.iltotore.iron.constraint.string.Blank
import io.github.iltotore.iron.pureconfig.given
import _root_.pureconfig.*
import _root_.pureconfig.error.{
  CannotConvert,
  ConfigReaderFailure,
  ConfigReaderFailures,
  ConvertFailure,
  ExceptionThrown,
  UserValidationFailed
}

import scala.concurrent.duration.*

/** Iron constraints for `FiniteDuration` settings; iron-pureconfig turns each refined alias into a bounded reader. */
object AnalyticsDurationConstraints {
  final class PositiveDuration
  final class AtLeastOneMillisecond
  final class StreamingTriggerBound

  object PositiveDuration {
    class PositiveDurationConstraint extends Constraint[FiniteDuration, PositiveDuration] {
      override inline def test(inline value: FiniteDuration): Boolean = value > Duration.Zero
      override inline def message: String = "Should be strictly positive"
    }
    inline given PositiveDurationConstraint = new PositiveDurationConstraint
  }

  /** Mongo leases and timeouts are stored at millisecond resolution, so shorter values would round to zero. */
  object AtLeastOneMillisecond {
    class AtLeastOneMillisecondConstraint extends Constraint[FiniteDuration, AtLeastOneMillisecond] {
      override inline def test(inline value: FiniteDuration): Boolean = value.toMillis > 0L
      override inline def message: String = "Should be at least one millisecond"
    }
    inline given AtLeastOneMillisecondConstraint = new AtLeastOneMillisecondConstraint
  }

  object StreamingTriggerBound {
    val Ceiling: FiniteDuration = 10.seconds

    class StreamingTriggerBoundConstraint extends Constraint[FiniteDuration, StreamingTriggerBound] {
      override inline def test(inline value: FiniteDuration): Boolean = value > Duration.Zero && value <= Ceiling
      override inline def message: String = "Should be strictly positive and at most 10 seconds"
    }
    inline given StreamingTriggerBoundConstraint = new StreamingTriggerBoundConstraint
  }
}

type AnalyticsPositiveDuration = FiniteDuration :| AnalyticsDurationConstraints.PositiveDuration
type AnalyticsMillisecondDuration = FiniteDuration :| AnalyticsDurationConstraints.AtLeastOneMillisecond
type StreamingTriggerInterval = FiniteDuration :| AnalyticsDurationConstraints.StreamingTriggerBound

/** Readers shared by every analytics HOCON section plus the one decode boundary that reports failing paths. */
private[analytics] object AnalyticsConfigReaders {
  given ConfigReader[AnalyticsNonBlank] =
    ConfigReader[String].emap(value =>
      value.refineEither[Not[Blank]].leftMap(_ => UserValidationFailed("must be non-empty"))
    )

  private def validated[A](parse: String => Either[String, A]): ConfigReader[A] =
    ConfigReader[String].emap(parse(_).leftMap(UserValidationFailed.apply))

  given ConfigReader[AnalyticsTopic] = validated(AnalyticsTopic.from)
  given ConfigReader[RunId] = validated(RunId.from)
  given ConfigReader[AnalyticsReplayRequestId] = validated(AnalyticsReplayRequestId.from)

  given ConfigReader[WriterDisposition] = ConfigReader[String].emap {
    case "stopped"        => Right(WriterDisposition.Stopped)
    case "access-revoked" => Right(WriterDisposition.AccessRevoked)
    case "active"         => Right(WriterDisposition.Active)
    case "unknown"        => Right(WriterDisposition.Unknown)
    case _                => Left(UserValidationFailed("must be stopped, access-revoked, active, or unknown"))
  }

  /** `${?NAME}` placeholders set to an empty string mean "not configured", so empty optional strings read as `None`. */
  given ConfigReader[Option[AnalyticsNonBlank]] with ReadsMissingKeys {
    override def from(cursor: ConfigCursor): ConfigReader.Result[Option[AnalyticsNonBlank]] =
      if (cursor.isUndefined || cursor.isNull) Right(None)
      else
        ConfigReader[String].from(cursor).flatMap { value =>
          if (value.trim.isEmpty) Right(None)
          else ConfigReader[AnalyticsNonBlank].from(cursor).map(Some(_))
        }
  }

  given ConfigReader[AnalyticsHmacSettings] =
    ConfigReader.forProduct4("secret-base64", "key-id", "previous-key-id", "previous-secret-base64")(
      AnalyticsHmacSettings.apply
    )

  given ConfigReader[AnalyticsRetentionSettings] =
    ConfigReader
      .forProduct12(
        "bronze-days",
        "quarantine-days",
        "silver-days",
        "stage-times-terminal-days",
        "stage-times-open-days",
        "published-snapshot-days",
        "deletion-marker-days",
        "delta-vacuum-safety",
        "delta-log-retention",
        "erasure-completion-budget",
        "kafka-retention",
        "kafka-segment-roll"
      )(
        (
            bronzeDays: AnalyticsPositiveInt,
            quarantineDays: AnalyticsPositiveInt,
            silverDays: AnalyticsPositiveInt,
            stageTimesTerminalDays: StageTimesRetentionDays,
            stageTimesOpenDays: StageTimesRetentionDays,
            publishedSnapshotDays: AnalyticsPositiveInt,
            deletionMarkerDays: AnalyticsPositiveInt,
            deltaVacuumSafety: AnalyticsPositiveDuration,
            deltaLogRetention: AnalyticsPositiveDuration,
            erasureCompletionBudget: AnalyticsPositiveDuration,
            kafkaRetention: AnalyticsPositiveDuration,
            kafkaSegmentRoll: AnalyticsPositiveDuration
        ) =>
          AnalyticsRetentionSettings(
            bronzeDays,
            quarantineDays,
            silverDays,
            stageTimesTerminalDays,
            stageTimesOpenDays,
            publishedSnapshotDays,
            deletionMarkerDays,
            deltaVacuumSafety,
            deltaLogRetention,
            erasureCompletionBudget,
            kafkaRetention,
            kafkaSegmentRoll
          )
      )
      .emap { retention =>
        val worstCase = retention.worstCaseErasureCompletion
        val budget = retention.erasureCompletionBudget
        val cap = AnalyticsRetentionSettings.MaximumErasureCompletionBudget
        if (budget > cap)
          Left(UserValidationFailed(s"erasure-completion-budget $budget exceeds the one-month response period $cap"))
        else
          Either.cond(
            worstCase <= budget,
            retention,
            UserValidationFailed(s"worst-case erasure completion $worstCase exceeds erasure-completion-budget $budget")
          )
      }

  given ConfigReader[AnalyticsErasureWorkerTimings] =
    ConfigReader.forProduct5(
      "lease-duration",
      "delivery-timeout",
      "poll-interval",
      "reclamation-recheck-interval",
      "backlog-log-interval"
    )(
      (
          leaseDuration: AnalyticsMillisecondDuration,
          deliveryTimeout: AnalyticsMillisecondDuration,
          pollInterval: AnalyticsMillisecondDuration,
          reclamationRecheckInterval: AnalyticsMillisecondDuration,
          backlogLogInterval: AnalyticsMillisecondDuration
      ) =>
        AnalyticsErasureWorkerTimings(
          leaseDuration,
          deliveryTimeout,
          pollInterval,
          reclamationRecheckInterval,
          backlogLogInterval
        )
    )

  given ConfigReader[AnalyticsMongoStartupSettings] =
    ConfigReader
      .forProduct4("check-timeout", "initial-backoff", "max-backoff", "max-wait")(
        (
            checkTimeout: AnalyticsMillisecondDuration,
            initialBackoff: AnalyticsMillisecondDuration,
            maxBackoff: AnalyticsMillisecondDuration,
            maxWait: AnalyticsMillisecondDuration
        ) => AnalyticsMongoStartupSettings(checkTimeout, initialBackoff, maxBackoff, maxWait)
      )
      .emap(settings =>
        Either.cond(
          settings.initialBackoff <= settings.maxBackoff,
          settings,
          UserValidationFailed("initial-backoff must not exceed max-backoff")
        )
      )

  given ConfigReader[AnalyticsOperationalSettings] =
    ConfigReader.forProduct6(
      "retention",
      "report-reservation-ttl",
      "mongo-transaction-window",
      "maximum-erasure-evidence-files",
      "mongo-publisher-buffer-size",
      "erasure-worker"
    )(
      (
          retention: AnalyticsRetentionSettings,
          reportReservationTtl: AnalyticsPositiveDuration,
          mongoTransactionWindow: AnalyticsPositiveDuration,
          maximumErasureEvidenceFiles: MaximumErasureEvidenceFiles,
          mongoPublisherBufferSize: MongoPublisherBufferSize,
          erasureWorkerTimings: AnalyticsErasureWorkerTimings
      ) =>
        AnalyticsOperationalSettings(
          retention,
          reportReservationTtl,
          mongoTransactionWindow,
          maximumErasureEvidenceFiles,
          mongoPublisherBufferSize,
          erasureWorkerTimings
        )
    )

  /** Decodes one section of `source`, keeping one redacted diagnostic per failing path. */
  def decode[A: ConfigReader](source: ConfigSource, path: String): ValidatedNec[String, A] =
    source.at(path).load[A].toValidated.leftMap(describe)

  def complete[A](value: ValidatedNec[String, A]): Either[AnalyticsError, A] =
    value.toEither.leftMap(AnalyticsError.fromProblems)

  private def describe(failures: ConfigReaderFailures): NonEmptyChain[String] =
    NonEmptyChain.fromChainPrepend(describe(failures.head), Chain.fromSeq(failures.tail.map(describe)))

  /** Conversion failures carry the rejected value in their description, so only the target type is reported. */
  private def describe(failure: ConfigReaderFailure): String = {
    val detail = failure match {
      case ConvertFailure(CannotConvert(_, toType, _), _, path) => s"$path: cannot convert the value to $toType"
      case ConvertFailure(ExceptionThrown(_), _, path)          => s"$path: the value could not be read"
      case ConvertFailure(reason, _, path)                      => s"$path: ${reason.description}"
      case other                                                => other.description
    }
    val origin = failure.origin.fold("")(origin => s" (${origin.description})")
    redactConfigDiagnostic(detail + origin)
  }

  private val SensitiveAssignment =
    "(?i)((?:previous-)?secret-base64|(?:sasl-)?(?:username|password)|mongo(?:db)?-uri)\\s*[:=]\\s*(?!Key not found:\\s*')(\"(?:\\\\.|[^\"])*\"|[^,\\s}]+)".r
  private val MongoCredentials = "(?i)(mongodb(?:\\+srv)?://)[^/@\\s]+@".r
  private val LongEncodedSecret = "(?<![A-Za-z0-9])[A-Za-z0-9+/]{32,}={0,2}(?![A-Za-z0-9])".r

  private def redactConfigDiagnostic(message: String): String =
    LongEncodedSecret.replaceAllIn(
      MongoCredentials.replaceAllIn(
        SensitiveAssignment.replaceAllIn(message, matched => s"${matched.group(1)}=[REDACTED]"),
        matched => s"${matched.group(1)}[REDACTED]@"
      ),
      "[REDACTED_SECRET]"
    )
}
