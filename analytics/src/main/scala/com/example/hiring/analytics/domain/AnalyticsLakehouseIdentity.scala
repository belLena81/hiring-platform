package com.example.hiring.analytics.domain

import java.net.URI
import java.nio.charset.StandardCharsets

/** Canonical privacy-safe identity shared by lakehouse ownership and streaming admission. */
object AnalyticsLakehouseIdentity {
  def from(root: String): Either[String, String] =
    scala.util.Try {
      val uri = new URI(root).normalize()
      require(
        uri.getScheme != null && uri.getRawUserInfo == null && uri.getRawQuery == null && uri.getRawFragment == null
      )
      require(!uri.isOpaque)
      val rawPath = Option(uri.getRawPath).getOrElse("")
      val authority = Option(uri.getRawAuthority).fold("")(value => s"//${value.toLowerCase(java.util.Locale.ROOT)}")
      val withRootSlash = if (authority.nonEmpty && rawPath.isEmpty) "/" else rawPath
      val canonicalPath =
        if (withRootSlash.length > 1) withRootSlash.reverse.dropWhile(_ == '/').reverse else withRootSlash
      val canonical = s"${uri.getScheme.toLowerCase(java.util.Locale.ROOT)}:$authority$canonicalPath"
      AnalyticsDigest.sha256Hex(canonical.getBytes(StandardCharsets.UTF_8))
    }.toEither match {
      case Right(identity) => Right(identity)
      case Left(_)         => Left("analytics lakehouse root is invalid")
    }
}
