package com.example.graphQL.cats.infrastructure.auth

import com.example.graphQL.cats.service.auth.AuthenticationFingerprint
import com.example.graphQL.cats.service.port.MutationReceiptFingerprint
import com.example.graphQL.cats.shared.crypto.Hmac
import io.circe.Json
import java.nio.charset.StandardCharsets.UTF_8

final class HmacAuthenticationFingerprint(secret: String) extends AuthenticationFingerprint {
  private val key = Hmac.sha256(secret.getBytes(UTF_8), "hiring-platform:auth-receipt")
  override val keyId: String = Hmac.hex(Hmac.sha256(key, "hiring-platform:auth-receipt:key-id"))
  override def protect(
      operation: String,
      actorScope: String,
      digest: MutationReceiptFingerprint
  ): MutationReceiptFingerprint =
    MutationReceiptFingerprint.stored(
      "hmac-sha256:" + Hmac.hex(
        Hmac.sha256(
          key,
          Json.arr(Json.fromString(operation), Json.fromString(actorScope), Json.fromString(digest.value)).noSpaces
        )
      )
    )
  override def toString: String = "HmacAuthenticationFingerprint([REDACTED])"
}
