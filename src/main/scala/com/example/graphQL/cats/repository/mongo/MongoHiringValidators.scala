package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.mongodb.MongoClientSettings
import org.bson.{BsonDocument, BsonString, Document}

import scala.jdk.CollectionConverters.*

/** JSON-schema fragments shared by every strict collection validator. */
private[mongo] object MongoValidatorSchemas {
  val UuidPattern = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
  val AnchoredUuidPattern: String = s"^$UuidPattern$$"
  val ShortTextMaxLength = 256

  def enumSchema(values: String*): Document = new Document("enum", values.toList.asJava)
  def objectSchema(required: List[String], properties: Document): Document =
    new Document("bsonType", "object").append("required", required.asJava).append("properties", properties)
  def shortText: Document =
    new Document("bsonType", "string").append("minLength", 1).append("maxLength", ShortTextMaxLength)
  def stringUuid: Document = shortText.append("pattern", AnchoredUuidPattern)
  def nonNegativeLong: Document = new Document("bsonType", "long").append("minimum", 0L)
  def nonNegativeInt: Document = new Document("bsonType", "int").append("minimum", 0)
  def date: Document = new Document("bsonType", "date")
}

/** Collection validators are maintained beside their collection setup behavior: one `collMod` builder, one installer
  * and one exact-definition check serve every strict validator.
  */
private[mongo] object MongoHiringValidators {
  import MongoHiringSetup.SetupDatabase

  private val bsonRegistry = MongoClientSettings.getDefaultCodecRegistry
  private val StrictLevel = "strict"
  private val ErrorAction = "error"

  /** The only `collMod` shape the platform installs: strict level, error action, the given validator. */
  def strictValidation(collection: String, validator: Document): Document =
    new Document("collMod", collection)
      .append("validator", validator)
      .append("validationLevel", StrictLevel)
      .append("validationAction", ErrorAction)

  def install(database: SetupDatabase, collection: String, validator: Document): IO[Unit] =
    database.ensureCollection(collection) *> database.runCommand(strictValidation(collection, validator)).void

  /** Current `options` of the listed collections, keyed by name; absent collections are absent from the map. */
  def collectionOptions(database: SetupDatabase, collections: List[String]): IO[Map[String, BsonDocument]] =
    database
      .runCommand(
        new Document("listCollections", 1)
          .append("filter", new Document("name", new Document("$in", collections.asJava)))
      )
      .map { result =>
        Option(result.getDocument("cursor", null)).toList
          .flatMap(cursor => Option(cursor.getArray("firstBatch", null)).toList.flatMap(_.getValues.asScala))
          .collect { case entry: BsonDocument => entry }
          .flatMap { entry =>
            Option(entry.getString("name", null))
              .map(name => name.getValue -> Option(entry.getDocument("options", null)).getOrElse(new BsonDocument()))
          }
          .toMap
      }

  /** Pure exact-definition check: strict level, error action and a validator equal to the expected document. */
  def strictValidatorMatches(options: Option[BsonDocument], expected: Document): Boolean =
    options.exists { value =>
      Option(value.get("validationLevel")).contains(new BsonString(StrictLevel)) &&
      Option(value.get("validationAction")).contains(new BsonString(ErrorAction)) &&
      Option(value.get("validator")).contains(expected.toBsonDocument(classOf[Document], bsonRegistry))
    }

  def validatorMatches(database: SetupDatabase, collection: String, expected: Document): IO[Boolean] =
    collectionOptions(database, List(collection)).map(options =>
      strictValidatorMatches(options.get(collection), expected)
    )

  /** Fails closed with the first drifted collection; one round trip verifies every listed definition. */
  def assertStrictValidators(database: SetupDatabase, expected: List[(String, Document)]): IO[Unit] =
    collectionOptions(database, expected.map(_._1)).flatMap { options =>
      expected.traverse_ { case (collection, validator) =>
        IO.raiseUnless(strictValidatorMatches(options.get(collection), validator))(
          MigrationError.ValidatorMismatch(collection)
        )
      }
    }

  def outboxValidator: Document = {
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
    new Document("$jsonSchema", schema)
  }

  def createOutboxValidator(database: SetupDatabase): IO[Unit] =
    database.runCommand(strictValidation(MongoCollections.EventOutbox, outboxValidator)).void

  def userValidator: Document = {
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
              "oneOf",
              List(
                new Document("required", List(MongoFields.City, MongoFields.CityCanonical).asJava),
                new Document(
                  "allOf",
                  List(MongoFields.City, MongoFields.CityCanonical)
                    .map(field => new Document("not", new Document("required", List(field).asJava)))
                    .asJava
                )
              ).asJava
            )
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
    new Document(
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
  }

  def userValidatorMatches(database: SetupDatabase): IO[Boolean] =
    validatorMatches(database, MongoCollections.Users, userValidator)

  def createUserValidator(database: SetupDatabase): IO[Unit] =
    install(database, MongoCollections.Users, userValidator)

  def jobValidator: Document = {
    val geoPoint = new Document("bsonType", "object")
      .append("required", List("type", MongoFields.Coordinates).asJava)
      .append(
        "properties",
        new Document("type", new Document("bsonType", "string").append("enum", List("Point").asJava))
          .append(
            MongoFields.Coordinates,
            new Document("bsonType", "array")
              .append("minItems", 2)
              .append("maxItems", 2)
              .append("items", new Document("bsonType", "number"))
          )
      )
    val location = new Document("bsonType", "object")
      .append("properties", new Document(MongoFields.Point, geoPoint))
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
    new Document("$and", List(schema, geoPredicate).asJava)
  }

  def createJobValidator(database: SetupDatabase): IO[Unit] =
    install(database, MongoCollections.Jobs, jobValidator)
}
