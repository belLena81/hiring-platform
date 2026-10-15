package com.example.graphQL.cats.shared.crypto

import io.circe.Json

/** Canonical digest of search criteria; the compact JSON array layout is part of the persisted cursor contract. */
object CriteriaDigest {
  def sha256(
      lat: Double,
      lon: Double,
      radius: Double,
      city: Option[String],
      skills: List[String],
      createdAfter: Option[String]
  ): String =
    SourceHash.sha256(
      Json
        .arr(
          Json.fromDoubleOrNull(lat),
          Json.fromDoubleOrNull(lon),
          Json.fromDoubleOrNull(radius),
          city.map(Json.fromString).getOrElse(Json.Null),
          Json.fromValues(skills.map(Json.fromString)),
          createdAfter.map(Json.fromString).getOrElse(Json.Null),
          Json.fromString("distanceKm,id")
        )
        .noSpaces
    )
}
