package com.example.hiring.analytics.cli

import com.example.hiring.analytics.errors.AnalyticsError
import cats.effect.{Clock, IO}
import scala.concurrent.duration.*

/** Calendar substitution is available only on the test classpath and one nonce-bound disposable fixture. */
private[analytics] object HmacRetirementProofCalendar {
  private val ShiftMillis = 32.days.toMillis

  def shift(
      root: String,
      database: String,
      topic: String,
      retiringKeyId: String,
      ownedNonce: Option[String] = sys.env.get("HIRING_HMAC_ROTATION_ISOLATED_TEST_NONCE")
  ): Either[AnalyticsError, Long] =
    ownedNonce match {
      case None        => Right(0L)
      case Some(nonce) =>
        Either.cond(
          nonce.matches("[a-f0-9]{16}") && database == s"hiring_hmac_rotation_test_$nonce" &&
            topic == s"hiring.hmac.rotation.test.$nonce" && retiringKeyId == s"rotation-old-$nonce" &&
            root.matches(s"file:///[^?#]*[.]local/data/hmac-key-retirement/isolated-$nonce/lakehouse/?"),
          ShiftMillis,
          AnalyticsError.InvalidConfiguration("isolated retirement calendar does not match its owned nonce fixture")
        )
    }

  def clock(shiftMillis: Long): Clock[IO] = new Clock[IO] {
    override def applicative = summon[cats.Applicative[IO]]
    override def monotonic: IO[FiniteDuration] = Clock[IO].monotonic
    override def realTime: IO[FiniteDuration] = Clock[IO].realTime.map(_ + shiftMillis.millis)
  }
}
