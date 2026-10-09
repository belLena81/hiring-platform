package com.example.graphQL.cats.shared.crypto

import java.nio.charset.StandardCharsets.UTF_8
import java.util.HexFormat
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

/** HMAC-SHA256 primitives shared by cursor signing, receipt fingerprints and token verification. */
object Hmac {
  private val Algorithm = "HmacSHA256"

  def secretKey(bytes: Array[Byte]): SecretKeySpec = new SecretKeySpec(bytes, Algorithm)

  def sha256(key: Array[Byte], value: Array[Byte]): Array[Byte] = {
    val mac = Mac.getInstance(Algorithm)
    mac.init(secretKey(key))
    mac.doFinal(value)
  }

  def sha256(key: Array[Byte], value: String): Array[Byte] = sha256(key, value.getBytes(UTF_8))

  def hex(bytes: Array[Byte]): String = HexFormat.of().formatHex(bytes)
}
