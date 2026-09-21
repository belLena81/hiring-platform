package com.example.graphQL.cats.shared.crypto

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.HexFormat

object SourceHash {
  def sha256(value: String): String = {
    val digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8))
    HexFormat.of().formatHex(digest)
  }
}
