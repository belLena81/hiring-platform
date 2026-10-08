package com.example.graphQL.cats.infrastructure.auth

import com.example.graphQL.cats.service.auth.AuthenticationFingerprint
import com.example.graphQL.cats.service.port.MutationReceiptFingerprint
import io.circe.Json
import java.nio.charset.StandardCharsets.UTF_8
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

final class HmacAuthenticationFingerprint(secret: String) extends AuthenticationFingerprint {
  private def hmac(key: Array[Byte], value: String): Array[Byte] = {
    val mac = Mac.getInstance("HmacSHA256")
    mac.init(new SecretKeySpec(key, "HmacSHA256"))
    mac.doFinal(value.getBytes(UTF_8))
  }
  private def hex(value: Array[Byte]): String = java.util.HexFormat.of().formatHex(value)
  private val key = hmac(secret.getBytes(UTF_8), "hiring-platform:auth-receipt")
  override val keyId: String = hex(hmac(key, "hiring-platform:auth-receipt:key-id"))
  override def protect(
      operation: String,
      actorScope: String,
      digest: MutationReceiptFingerprint
  ): MutationReceiptFingerprint =
    MutationReceiptFingerprint.stored(
      "hmac-sha256:" + hex(
        hmac(
          key,
          Json.arr(Json.fromString(operation), Json.fromString(actorScope), Json.fromString(digest.value)).noSpaces
        )
      )
    )
  override def toString: String = "HmacAuthenticationFingerprint([REDACTED])"
}
