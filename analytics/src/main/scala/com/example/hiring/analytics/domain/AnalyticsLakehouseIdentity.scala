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
      if (uri.getScheme.equalsIgnoreCase("file")) {
        require(uri.getAuthority == null && Option(uri.getPath).exists(_.startsWith("/")))
        // Keep the established raw-path identity. Reject escaped aliases instead of
        // silently assigning a different ownership hash to the same local directory.
        val canonicalFile = new URI("file", null, uri.getPath, null, null).normalize().toASCIIString
        require(uri.getRawPath == new URI(canonicalFile).getRawPath)
      }
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
