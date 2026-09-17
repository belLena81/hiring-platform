package com.example.graphQL.cats.shared.crypto

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

object SourceHash {
  def sha256(value: String): String = {
    val digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8))
    digest.map(byte => f"${byte & 0xff}%02x").mkString
  }
}
