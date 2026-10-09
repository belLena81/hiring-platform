package com.example.hiring.analytics.domain

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

/** Shared lowercase hexadecimal SHA-256 encoding for analytics identities and fingerprints. */
private[analytics] object AnalyticsDigest {
  def sha256Hex(bytes: Array[Byte]): String =
    java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes))

  /** Digest of the UTF-8 encoding of `text`. */
  def sha256Hex(text: String): String = sha256Hex(text.getBytes(StandardCharsets.UTF_8))
}
