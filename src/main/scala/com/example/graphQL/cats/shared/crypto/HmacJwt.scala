package com.example.graphQL.cats.shared.crypto

import java.time.{Clock as JavaClock, Instant, ZoneOffset}
import javax.crypto.SecretKey
import pdi.jwt.{JwtAlgorithm, JwtCirce, JwtClaim, JwtOptions}

/** The one HS256 verification policy: signature, expiry and not-before are enforced with no leeway. */
object HmacJwt {
  private val Algorithms = Seq(JwtAlgorithm.HS256)
  private val Options = JwtOptions(signature = true, expiration = true, notBefore = true, leeway = 0)

  def decode(token: String, key: SecretKey, now: Instant, issuer: String, audience: String): Option[JwtClaim] = {
    given clock: JavaClock = JavaClock.fixed(now, ZoneOffset.UTC)
    JwtCirce(clock)
      .decode(token, key, Algorithms, Options)
      .toOption
      .filter(_.isValid(issuer, audience))
  }
}
