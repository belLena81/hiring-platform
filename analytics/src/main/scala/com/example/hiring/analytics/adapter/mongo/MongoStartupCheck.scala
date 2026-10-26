package com.example.hiring.analytics.adapter.mongo

import com.example.hiring.analytics.config.AnalyticsMongoStartupSettings
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.errors.AnalyticsError.MongoUnreachableReason

import cats.Applicative
import cats.effect.Async
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.mongodb.{ConnectionString, MongoClientSettings, ReadPreference, ServerAddress}
import mongo4cats.client.MongoClient
import org.bson.{BsonDocument, BsonInt32}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import retry.{ResultHandler, RetryDetails, RetryPolicies, retryingOnErrors}

import java.net.InetAddress
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.*

/** Checks that MongoDB answers before an analytics process builds a Spark session, so an unreachable database costs a
  * short check instead of a full start. Long-running commands wait with capped exponential backoff; the others fail
  * after one check. Logged lines and errors never carry a host, URI or credentials.
  */
private[analytics] object MongoStartupCheck {
  enum Mode {
    case FailFast
    case AwaitReachable
  }

  /** One DNS resolution plus one bounded server-selection ping, for the real driver. */
  def check[F[_]: Async](uri: String, settings: AnalyticsMongoStartupSettings): F[Unit] =
    probe(
      uri,
      host => Either.catchNonFatal(InetAddress.getAllByName(host).toVector),
      ping(uri, settings)
    )

  /** The check under the mode's policy; each failed attempt of a waiting command logs one sanitized WARN line. */
  def ensureReachable[F[_]: Async](
      mode: Mode,
      uri: String,
      settings: AnalyticsMongoStartupSettings
  ): F[Unit] = {
    val logger = Slf4jLogger.getLogger[F]
    ensureReachableUsing(mode, settings, check(uri, settings), line => logger.warn(line))
  }

  private[analytics] def ensureReachableUsing[F[_]: Async](
      mode: Mode,
      settings: AnalyticsMongoStartupSettings,
      checkOnce: F[Unit],
      warn: String => F[Unit]
  ): F[Unit] = mode match {
    case Mode.FailFast       => checkOnce
    case Mode.AwaitReachable =>
      val policy = RetryPolicies.limitRetriesByCumulativeDelay[F, Throwable](
        settings.maxWait,
        RetryPolicies.capDelay(settings.maxBackoff, RetryPolicies.exponentialBackoff[F](settings.initialBackoff))
      )
      retryingOnErrors(checkOnce)(
        policy,
        ResultHandler.retryOnSomeErrors[F, Unit](
          {
            case _: AnalyticsError.MongoUnreachable => true
            case _                                  => false
          },
          (error, details) => warnAttempt(error, details, warn)
        )
      ).adaptError { case AnalyticsError.MongoUnreachable(reason, None) =>
        AnalyticsError.MongoUnreachable(reason, Some(settings.maxWait))
      }
  }

  private def warnAttempt[F[_]: Applicative](
      error: Throwable,
      details: RetryDetails,
      warn: String => F[Unit]
  ): F[Unit] =
    (error, details.nextStepIfUnsuccessful) match {
      case (AnalyticsError.MongoUnreachable(reason, _), RetryDetails.NextStep.DelayAndRetry(delay)) =>
        warn(
          s"MongoDB is not reachable (${reason.text}); attempt ${details.retriesSoFar + 1}, retrying in ${delay.toMillis} ms"
        )
      case _ => Applicative[F].unit
    }

  /** DNS first (a missing host is a fast, distinct failure), then the ping; both failures are fixed-text errors. */
  private[analytics] def probe[F[_]: Async](
      uri: String,
      resolveAddresses: String => Either[Throwable, Vector[InetAddress]],
      pingOnce: F[Unit]
  ): F[Unit] =
    Async[F]
      .fromEither(
        Either
          .catchNonFatal(new ConnectionString(uri))
          .leftMap(_ => AnalyticsError.InvalidConfiguration("MONGODB_URI is invalid"))
      )
      .flatMap { connection =>
        val hosts = connection.getHosts.asScala.toList.map(new ServerAddress(_).getHost)
        // A mongodb+srv name is a DNS SRV record, not an address; the ping resolves it.
        // The JDK lookup blocks, so it runs on the blocking pool, as in KafkaConnection.preflight.
        val resolves =
          if (connection.isSrvProtocol) Async[F].pure(true)
          else Async[F].blocking(hosts.exists(host => resolveAddresses(host).exists(_.nonEmpty)))
        resolves.flatMap(
          Async[F].raiseUnless(_)(AnalyticsError.MongoUnreachable(MongoUnreachableReason.DnsResolution))
        ) *>
          pingOnce.adaptError { case scala.util.control.NonFatal(_) =>
            AnalyticsError.MongoUnreachable(MongoUnreachableReason.ServerSelection)
          }
      }

  private def ping[F[_]: Async](uri: String, settings: AnalyticsMongoStartupSettings): F[Unit] =
    Async[F]
      .fromEither(
        Either
          .catchNonFatal(
            MongoClientSettings
              .builder()
              .applyConnectionString(new ConnectionString(uri))
              .applyToClusterSettings { cluster =>
                val _ = cluster.serverSelectionTimeout(settings.checkTimeout.toMillis, TimeUnit.MILLISECONDS)
              }
              .build()
          )
          .leftMap(_ => AnalyticsError.InvalidConfiguration("MONGODB_URI is invalid"))
      )
      .flatMap(driverSettings =>
        MongoClient
          .create[F](driverSettings)
          .use(
            _.getDatabase("admin")
              .flatMap(_.runCommand(new BsonDocument("ping", new BsonInt32(1)), ReadPreference.primaryPreferred()))
          )
          .timeout(settings.checkTimeout * 2)
          .void
      )
}
