package com.example.graphQL.cats.infrastructure.auth

import com.example.graphQL.cats.service.port.MutationReceiptFingerprint
import munit.FunSuite

final class HmacAuthenticationFingerprintSpec extends FunSuite {
  private val first = new HmacAuthenticationFingerprint("synthetic-authentication-secret-one")
  private val second = new HmacAuthenticationFingerprint("synthetic-authentication-secret-two")
  private val digest = MutationReceiptFingerprint.fromCanonicalInput("password-bearing-input")

  test("fingerprints are tagged, deterministic, and separated by secret, operation and actor") {
    val result = first.protect("login", "public:recruiter", digest)
    assert(result.value.matches("hmac-sha256:[0-9a-f]{64}"))
    assertNotEquals(result, digest)
    assertEquals(result, first.protect("login", "public:recruiter", digest))
    assertNotEquals(result, second.protect("login", "public:recruiter", digest))
    assertNotEquals(result, first.protect("signUp", "public:recruiter", digest))
    assertNotEquals(result, first.protect("login", "public:other", digest))
    assertNotEquals(first.protect("login:public", "recruiter", digest), result)
    assertNotEquals(first.keyId, second.keyId)
    assert(!first.toString.contains("synthetic"))
  }
}
