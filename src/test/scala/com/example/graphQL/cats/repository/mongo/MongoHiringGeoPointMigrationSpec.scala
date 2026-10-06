package com.example.graphQL.cats.repository.mongo

import munit.FunSuite
import org.bson.Document
import scala.jdk.CollectionConverters.*

class MongoHiringGeoPointMigrationSpec extends FunSuite {
  test("accepts valid GeoJSON points in longitude latitude order") {
    val point = new Document("type", "Point")
      .append("coordinates", List(-180d, 90d).asJava)

    assert(MongoHiringMigrations.isValidJobGeoPoint(point))
  }

  test("rejects unsupported geometry, malformed coordinates, and out of range values") {
    val unsupportedType = new Document("type", "LineString")
      .append("coordinates", List(0d, 0d).asJava)
    val tooFewCoordinates = new Document("type", "Point")
      .append("coordinates", List(0d).asJava)
    val latitudeOutOfRange = new Document("type", "Point")
      .append("coordinates", List(0d, 90.1d).asJava)
    val nonFinite = new Document("type", "Point")
      .append("coordinates", List(Double.NaN, 0d).asJava)

    assert(!MongoHiringMigrations.isValidJobGeoPoint(unsupportedType))
    assert(!MongoHiringMigrations.isValidJobGeoPoint(tooFewCoordinates))
    assert(!MongoHiringMigrations.isValidJobGeoPoint(latitudeOutOfRange))
    assert(!MongoHiringMigrations.isValidJobGeoPoint(nonFinite))
  }
}
