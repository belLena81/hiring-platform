package com.example.hiring.analytics.domain

import cats.syntax.all.*

import java.net.URI

/** Canonical privacy-safe identity shared by lakehouse ownership and streaming admission. */
object AnalyticsLakehouseIdentity {
  private val Invalid = "analytics lakehouse root is invalid"

  def from(root: String): Either[String, String] =
    for {
      uri <- Either.catchNonFatal(new URI(root).normalize()).leftMap(_ => Invalid)
      _ <- Either.cond(
        uri.getScheme != null && uri.getRawUserInfo == null && uri.getRawQuery == null && uri.getRawFragment == null &&
          !uri.isOpaque,
        (),
        Invalid
      )
      _ <- if (uri.getScheme.equalsIgnoreCase("file")) requireCanonicalFile(uri) else Right(())
    } yield {
      val rawPath = Option(uri.getRawPath).getOrElse("")
      val authority = Option(uri.getRawAuthority).fold("")(value => s"//${value.toLowerCase(java.util.Locale.ROOT)}")
      val withRootSlash = if (authority.nonEmpty && rawPath.isEmpty) "/" else rawPath
      val canonicalPath =
        if (withRootSlash.length > 1) withRootSlash.reverse.dropWhile(_ == '/').reverse else withRootSlash
      AnalyticsDigest.sha256Hex(s"${uri.getScheme.toLowerCase(java.util.Locale.ROOT)}:$authority$canonicalPath")
    }

  /** Keeps the established raw-path identity: escaped aliases are rejected instead of silently getting a different
    * ownership hash for the same local directory.
    */
  private def requireCanonicalFile(uri: URI): Either[String, Unit] =
    for {
      _ <- Either.cond(uri.getAuthority == null && Option(uri.getPath).exists(_.startsWith("/")), (), Invalid)
      canonicalRawPath <- Either
        .catchNonFatal(new URI(new URI("file", null, uri.getPath, null, null).normalize().toASCIIString).getRawPath)
        .leftMap(_ => Invalid)
      _ <- Either.cond(uri.getRawPath == canonicalRawPath, (), Invalid)
    } yield ()
}
