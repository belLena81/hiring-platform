package com.example.hiring.analytics

import com.example.hiring.analytics.domain.SubjectToken

import munit.FunSuite

import java.nio.charset.StandardCharsets
import java.util.Base64
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

class SubjectPseudonymizerExceptionSafetySpec extends FunSuite {
  private val keyId = "hmac-v1"
  private val secret = "a-test-secret-that-is-long-enough".getBytes(StandardCharsets.UTF_8)
  private val pseudonymizer = AnalyticsTestSubjectPseudonymizer.fromKeyRing(keyId, secret, Vector.empty)

  test("HMAC token construction preserves the validated token wire format") {
    val subjectId = "candidate-123"
    val mac = Mac.getInstance("HmacSHA256")
    mac.init(new SecretKeySpec(secret, "HmacSHA256"))
    val digest = mac.doFinal(subjectId.getBytes(StandardCharsets.UTF_8))
    val expected = s"${keyId}_${Base64.getUrlEncoder.withoutPadding().encodeToString(digest)}"

    val token = pseudonymizer.typedToken(subjectId)

    assertEquals(token.map(_.value), Right(expected))
    assertEquals(token.flatMap(value => SubjectToken.fromHmac(value.value)), token)
  }

  test("public subject ID validation remains typed") {
    assertEquals(pseudonymizer.typedToken(null), Left("subject id must be non-empty"))
    assertEquals(pseudonymizer.typedToken(""), Left("subject id must be non-empty"))
    assertEquals(pseudonymizer.matchingTokens(""), Left("subject id must be non-empty"))
  }
}
