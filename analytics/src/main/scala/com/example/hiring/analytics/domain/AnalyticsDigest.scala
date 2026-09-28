package com.example.hiring.analytics.domain

import java.security.MessageDigest

/** Shared lowercase hexadecimal SHA-256 encoding for analytics identities and fingerprints. */
private[analytics] object AnalyticsDigest {
  def sha256Hex(bytes: Array[Byte]): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(bytes)
      .map(byte => f"${byte & 0xff}%02x")
      .mkString
}
