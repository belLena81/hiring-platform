package com.example.graphQL.cats.service.search

import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.JobId
import com.example.graphQL.cats.shared.Parsing
import java.nio.charset.StandardCharsets
import java.util.Base64

enum NearbyCursorError {
  case InvalidEncoding, InvalidDistance, InvalidIdentity, CriteriaMismatch

  def message: String = this match {
    case CriteriaMismatch => "Nearby cursor does not match criteria"
    case _                => "Invalid nearby cursor"
  }
}

object NearbyJobCursorCodec {
  private[search] def fingerprint(query: NearbyJobsQuery): String =
    com.example.graphQL.cats.shared.crypto.SourceHash.sha256(
      io.circe.Json
        .arr(
          io.circe.Json.fromDoubleOrNull(query.center.latitude),
          io.circe.Json.fromDoubleOrNull(query.center.longitude),
          io.circe.Json.fromDoubleOrNull(query.radiusKm),
          query.filter.city.map(value => io.circe.Json.fromString(value.trim)).getOrElse(io.circe.Json.Null),
          io.circe.Json.fromValues(
            query.filter.skills.toList.map(_.trim).filter(_.nonEmpty).distinct.sorted.map(io.circe.Json.fromString)
          ),
          query.filter.createdAfter
            .map(value => io.circe.Json.fromString(value.toString))
            .getOrElse(io.circe.Json.Null),
          io.circe.Json.fromString("distanceKm,id")
        )
        .noSpaces
    )

  def encode(distanceKm: Double, jobId: JobId, query: NearbyJobsQuery): String =
    Base64.getUrlEncoder
      .withoutPadding()
      .encodeToString(
        s"$distanceKm|${jobId.value}|${query.fingerprint}".getBytes(StandardCharsets.UTF_8)
      )

  def decode(value: String, query: NearbyJobsQuery): Either[NearbyCursorError, NearbyJobCursor] =
    Either
      .catchNonFatal(new String(Base64.getUrlDecoder.decode(value), StandardCharsets.UTF_8))
      .leftMap(_ => NearbyCursorError.InvalidEncoding)
      .flatMap { decoded =>
        decoded.split("\\|", -1).toList match {
          case distanceText :: jobId :: fingerprint :: Nil =>
            for {
              distance <- Either.catchNonFatal(distanceText.toDouble).leftMap(_ => NearbyCursorError.InvalidDistance)
              identity <- Parsing.parseUuid(jobId).leftMap(_ => NearbyCursorError.InvalidIdentity).map(JobId.apply)
              cursor <- validate(NearbyJobCursor(distance, identity, fingerprint), query)
            } yield cursor
          case _ => Left(NearbyCursorError.InvalidEncoding)
        }
      }

  def validate(cursor: NearbyJobCursor, query: NearbyJobsQuery): Either[NearbyCursorError, NearbyJobCursor] =
    for {
      _ <- Either.cond(cursor.distanceKm.isFinite && cursor.distanceKm >= 0d, (), NearbyCursorError.InvalidDistance)
      _ <- Either.cond(cursor.queryFingerprint == query.fingerprint, (), NearbyCursorError.CriteriaMismatch)
    } yield cursor
}
