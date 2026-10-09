package com.example.graphQL.cats.shared

import com.example.graphQL.cats.shared.crypto.{Hmac, HmacJwt}
import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import munit.FunSuite
import pdi.jwt.{JwtAlgorithm, JwtCirce, JwtClaim}

final class SharedHelpersSpec extends FunSuite {
  test("CauseChain lists the error and its causes outermost first and stops at a self-referencing cause") {
    val root = new IllegalStateException("root")
    val outer = new RuntimeException("outer", new RuntimeException("middle", root))
    assertEquals(CauseChain(outer).map(_.getMessage).toList, List("outer", "middle", "root"))
    val selfCaused = new RuntimeException("self") { override def getCause: Throwable = this }
    assertEquals(CauseChain(selfCaused).size, 1)
  }

  test("Hmac.sha256 matches the RFC 4231 test vector") {
    assertEquals(
      Hmac.hex(Hmac.sha256("Jefe".getBytes(UTF_8), "what do ya want for nothing?")),
      "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
    )
  }

  test("HmacJwt accepts only a valid, unexpired token for the expected issuer and audience") {
    val key = Hmac.secretKey("synthetic-signing-key-material-0123456789".getBytes(UTF_8))
    val now = Instant.parse("2026-01-01T00:00:00Z")
    val claim = JwtClaim().by("issuer").to("audience").about("subject").expiresAt(now.plusSeconds(60).getEpochSecond)
    val token = JwtCirce.encode(claim, key, JwtAlgorithm.HS256)
    assertEquals(HmacJwt.decode(token, key, now, "issuer", "audience").flatMap(_.subject), Some("subject"))
    assertEquals(HmacJwt.decode(token, key, now, "other", "audience"), None)
    assertEquals(HmacJwt.decode(token, key, now.plusSeconds(61), "issuer", "audience"), None)
  }
}
