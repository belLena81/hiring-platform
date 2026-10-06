package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import mongo4cats.database.MongoDatabase
import com.mongodb.MongoCommandException
import org.bson.Document

import scala.jdk.CollectionConverters.*

/** Collection validators are maintained beside their collection setup behavior. */
private[mongo] object MongoHiringValidators {
  def createOutboxValidator(database: MongoDatabase[IO]): IO[Unit] = {
    val subjectIds = new Document("bsonType", "array")
      .append("minItems", 1)
      .append("uniqueItems", true)
      .append("items", new Document("bsonType", "string"))
    val version = new Document("bsonType", "int").append("enum", List(1).asJava)
    val schema = new Document("bsonType", "object")
      .append("required", List(MongoFields.SubjectIds, MongoFields.SubjectRefsVersion).asJava)
      .append(
        "properties",
        new Document(MongoFields.SubjectIds, subjectIds).append(MongoFields.SubjectRefsVersion, version)
      )
    val command = new Document("collMod", MongoCollections.EventOutbox)
      .append("validator", new Document("$jsonSchema", schema))
      .append("validationLevel", "strict")
      .append("validationAction", "error")
    database.runCommand(command).void
  }

  def createUserValidator(database: MongoDatabase[IO]): IO[Unit] = {
    def active(role: String, required: String) = new Document("required", List(required).asJava).append(
      "properties",
      new Document()
        .append(MongoFields.Role, new Document("enum", List(role).asJava))
        .append(MongoFields.AccountStatus, new Document("enum", List("Active").asJava))
    )
    val candidateProfile = new Document("bsonType", "object").append(
      "properties",
      new Document()
        .append(MongoFields.RecruiterSearchOptIn, new Document("bsonType", "bool"))
        .append(MongoFields.AvailabilityStatus, new Document("enum", List("AVAILABLE_NOW", "UNAVAILABLE").asJava))
        .append(
          MongoFields.CurrentResidence,
          new Document("bsonType", "object")
            .append("required", List(MongoFields.Country, MongoFields.CountryCanonical).asJava)
            .append(
              "properties",
              new Document()
                .append(
                  MongoFields.Country,
                  new Document("bsonType", "string").append("minLength", 1).append("maxLength", 256)
                )
                .append(
                  MongoFields.City,
                  new Document("bsonType", "string").append("minLength", 1).append("maxLength", 256)
                )
                .append(
                  MongoFields.CountryCanonical,
                  new Document("bsonType", "string").append("minLength", 1).append("maxLength", 256)
                )
                .append(
                  MongoFields.CityCanonical,
                  new Document("bsonType", "string").append("minLength", 1).append("maxLength", 256)
                )
            )
        )
    )
    val candidate = active("Candidate", MongoFields.Profile)
    candidate.get("properties", classOf[Document]).append(MongoFields.Profile, candidateProfile)
    val admin = active("Admin", MongoFields.AdminSingletonKey)
    admin
      .get("properties", classOf[Document])
      .append(MongoFields.AdminSingletonKey, new Document("enum", List("singleton-admin").asJava))
    val schema = new Document(
      "$jsonSchema",
      new Document("bsonType", "object")
        .append("required", List(MongoFields.Role, MongoFields.AccountStatus, MongoFields.Version).asJava)
        .append("properties", new Document(MongoFields.Version, new Document("bsonType", "long").append("minimum", 0L)))
        .append(
          "oneOf",
          List(
            candidate,
            active("Recruiter", MongoFields.Profile),
            admin,
            new Document(
              "properties",
              new Document(MongoFields.AccountStatus, new Document("enum", List("Deleted").asJava))
            )
          ).asJava
        )
    )
    database.createCollection(MongoCollections.Users).void.recoverWith {
      case error: MongoCommandException if error.getErrorCode == 48 => IO.unit
    } *> database
      .runCommand(
        new Document("collMod", MongoCollections.Users)
          .append("validator", schema)
          .append("validationLevel", "strict")
          .append("validationAction", "error")
      )
      .void
  }

  def createJobValidator(database: MongoDatabase[IO]): IO[Unit] = {
    val geoPoint = new Document("bsonType", "object")
      .append("required", List("type", "coordinates").asJava)
      .append(
        "properties",
        new Document("type", new Document("bsonType", "string").append("enum", List("Point").asJava))
          .append(
            "coordinates",
            new Document("bsonType", "array")
              .append("minItems", 2)
              .append("maxItems", 2)
              .append("items", new Document("bsonType", "number"))
          )
      )
    val location = new Document("bsonType", "object")
      .append("properties", new Document("point", geoPoint))
    val schema = new Document(
      "$jsonSchema",
      new Document("bsonType", "object")
        .append("required", List(MongoFields.Version).asJava)
        .append(
          "properties",
          new Document(MongoFields.Version, new Document("bsonType", "long").append("minimum", 0L))
            .append(MongoFields.Location, location)
        )
    )
    val longitude = new Document("$arrayElemAt", List("$location.point.coordinates", 0).asJava)
    val latitude = new Document("$arrayElemAt", List("$location.point.coordinates", 1).asJava)
    val coordinateBounds = new Document(
      "$and",
      List(
        new Document("$isNumber", List(longitude).asJava),
        new Document("$isNumber", List(latitude).asJava),
        new Document("$gte", List(longitude, -180d).asJava),
        new Document("$lte", List(longitude, 180d).asJava),
        new Document("$gte", List(latitude, -90d).asJava),
        new Document("$lte", List(latitude, 90d).asJava)
      ).asJava
    )
    val coordinatesAreArray = new Document("$isArray", List("$location.point.coordinates").asJava)
    val validCoordinates = new Document(
      "$cond",
      List(
        coordinatesAreArray,
        coordinateBounds,
        false
      ).asJava
    )
    val pointIsAbsent = new Document("$eq", List(new Document("$type", "$location.point"), "missing").asJava)
    val geoPredicate = new Document(
      "$expr",
      new Document("$cond", List(pointIsAbsent, true, validCoordinates).asJava)
    )
    database.createCollection(MongoCollections.Jobs).void.recoverWith {
      case error: MongoCommandException if error.getErrorCode == 48 => IO.unit
    } *> database
      .runCommand(
        new Document("collMod", MongoCollections.Jobs)
          .append("validator", new Document("$and", List(schema, geoPredicate).asJava))
          .append("validationLevel", "strict")
          .append("validationAction", "error")
      )
      .void
  }
}
