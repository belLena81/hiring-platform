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
    val schema = new Document(
      "$jsonSchema",
      new Document("bsonType", "object")
        .append("required", List(MongoFields.Version).asJava)
        .append("properties", new Document(MongoFields.Version, new Document("bsonType", "long").append("minimum", 0L)))
    )
    database.createCollection(MongoCollections.Jobs).void.recoverWith {
      case error: MongoCommandException if error.getErrorCode == 48 => IO.unit
    } *> database
      .runCommand(
        new Document("collMod", MongoCollections.Jobs)
          .append("validator", schema)
          .append("validationLevel", "strict")
          .append("validationAction", "error")
      )
      .void
  }
}
