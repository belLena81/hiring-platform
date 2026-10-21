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
  import MongoValidatorSchemas.*

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

  /** `true` when the collection carries exactly one of the given definitions. */
  def validatorMatchesAny(database: SetupDatabase, collection: String, accepted: List[Document]): IO[Boolean] =
    collectionOptions(database, List(collection)).map(options =>
      accepted.exists(strictValidatorMatches(options.get(collection), _))
    )

  /** Passes when the collection carries exactly one of the accepted definitions: a completed proof accepts the
    * validator it completed against and every documented successor, and still fails closed on any other drift.
    */
  def assertAnyStrictValidator(database: SetupDatabase, collection: String, accepted: List[Document]): IO[Unit] =
    collectionOptions(database, List(collection)).flatMap { options =>
      IO.raiseUnless(accepted.exists(strictValidatorMatches(options.get(collection), _)))(
        MigrationError.ValidatorMismatch(collection)
      )
    }

  /** The outbox validator `013_hiring_workflow_integrity` installed; kept so its audit and completed proof still hold
    * until the documented successor ([[outboxValidator]]) is installed by `026_outbox_envelope_source`.
    */
  def outboxValidatorV1: Document = outboxSchema(successor = false)

  /** The current outbox validator: `envelopeBytes` is the only stored copy of the event body (`payload` is forbidden),
    * and the lease fields are absent or typed, never `null`.
    */
  def outboxValidator: Document = outboxSchema(successor = true)

  private def outboxSchema(successor: Boolean): Document = {
    val subjectIds = new Document("bsonType", "array")
      .append("minItems", 1)
      .append("uniqueItems", true)
      .append("items", new Document("bsonType", "string"))
    val version = new Document("bsonType", "int").append("enum", List(1).asJava)
    val baseline = new Document(MongoFields.SubjectIds, subjectIds).append(MongoFields.SubjectRefsVersion, version)
    val baselineRequired = List(MongoFields.SubjectIds, MongoFields.SubjectRefsVersion)
    val schema =
      if (!successor) objectSchema(baselineRequired, baseline)
      else
        objectSchema(
          baselineRequired :+ MongoFields.EnvelopeBytes,
          baseline
            .append(MongoFields.EnvelopeBytes, new Document("bsonType", "binData"))
            .append(MongoFields.LeaseOwner, new Document("bsonType", "string"))
            .append(MongoFields.LeaseToken, new Document("bsonType", "string"))
            .append(MongoFields.LeaseUntil, date)
        ).append("not", new Document("required", List(MongoFields.Payload).asJava))
    new Document("$jsonSchema", schema)
  }

  def createOutboxValidator(database: SetupDatabase): IO[Unit] =
    database.runCommand(strictValidation(MongoCollections.EventOutbox, outboxValidator)).void

  /** The users validator `016_candidate_residence_integrity` completed against; kept so its proof still holds until the
    * documented successor is installed.
    */
  def userValidatorV1: Document = userSchema(1)

  /** The users validator `025_operational_collection_validators` installed: typed identity and date fields, and no
    * personal data on a Deleted account. Kept for the 025 audit and install; [[userValidator]] is its successor.
    */
  def userValidatorV2: Document = userSchema(2)

  /** The current users validator: V2 plus an `embedding` that is a BSON binary vector (`028_binary_embedding_vectors`).
    */
  def userValidator: Document = userSchema(3)

  /** Every users definition a completed earlier proof may find installed. */
  def userValidatorHistory: List[Document] = List(userValidatorV1, userValidatorV2, userValidator)

  private val deletedAccountPersonalFields: List[String] = List(
    MongoFields.Email,
    MongoFields.EmailCanonical,
    MongoFields.PasswordHash,
    MongoFields.Profile,
    MongoFields.Embedding,
    MongoFields.EmbeddingMeta
  )

  private def userSchema(level: Int): Document = {
    val successor = level >= 2
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
    val deletedProperties = new Document(
      "properties",
      new Document(MongoFields.AccountStatus, new Document("enum", List("Deleted").asJava))
    )
    val deleted =
      if (successor)
        deletedProperties.append(
          "not",
          new Document(
            "anyOf",
            deletedAccountPersonalFields.map(field => new Document("required", List(field).asJava)).asJava
          )
        )
      else deletedProperties
    val required =
      if (successor)
        List(
          MongoFields.Role,
          MongoFields.AccountStatus,
          MongoFields.Version,
          MongoFields.Name,
          MongoFields.NameCanonical,
          MongoFields.CreatedAt,
          MongoFields.UpdatedAt
        )
      else List(MongoFields.Role, MongoFields.AccountStatus, MongoFields.Version)
    val topProperties = new Document(MongoFields.Version, new Document("bsonType", "long").append("minimum", 0L))
    if (level >= 3) { val _ = topProperties.append(MongoFields.Embedding, binaryVector) }
    if (successor)
      List(
        MongoFields.Name -> new Document("bsonType", "string"),
        MongoFields.NameCanonical -> new Document("bsonType", "string"),
        MongoFields.Email -> new Document("bsonType", "string"),
        MongoFields.EmailCanonical -> new Document("bsonType", "string"),
        MongoFields.PasswordHash -> new Document("bsonType", "string"),
        MongoFields.CreatedAt -> MongoValidatorSchemas.date,
        MongoFields.UpdatedAt -> MongoValidatorSchemas.date,
        MongoFields.DeletedAt -> MongoValidatorSchemas.date
      ).foreach { case (field, schema) => topProperties.append(field, schema) }
    new Document(
      "$jsonSchema",
      new Document("bsonType", "object")
        .append("required", required.asJava)
        .append("properties", topProperties)
        .append("oneOf", List(candidate, active("Recruiter", MongoFields.Profile), admin, deleted).asJava)
    )
  }

  def userValidatorMatches(database: SetupDatabase): IO[Boolean] =
    validatorMatches(database, MongoCollections.Users, userValidator)

  def createUserValidator(database: SetupDatabase): IO[Unit] =
    install(database, MongoCollections.Users, userValidator)

  /** Stored vectors are BSON binary (`binData`) vectors; the float64 arrays they replaced are rejected. */
  private def binaryVector: Document = new Document("bsonType", "binData")

  /** The jobs validator `021_job_admission_fence` installed. */
  def jobValidatorV2: Document = jobSchema(2)

  /** `027_job_canonical_filters`: V2 plus the canonical filter fields, paired with their display fields. */
  def jobValidatorV3: Document = jobSchema(3)

  /** The current jobs validator: V3 plus an `embedding` that is a BSON binary vector (`028`). */
  def jobValidator: Document = jobSchema(4)

  /** Every jobs definition a completed earlier proof may find installed. */
  def jobValidatorHistory: List[Document] = List(jobValidatorV2, jobValidatorV3, jobValidator)

  /** The part of [[jobValidatorV3]] / [[jobValidator]] that a migration audit checks: everything except the geo-point
    * predicate, so a legacy job with an invalid point still fails at its own geo-point proof (`006`) with its own typed
    * error instead of failing the audit of a later step.
    */
  def jobCanonicalAudit: Document = jobSchema(3, geo = false)
  def jobBinaryVectorAudit: Document = jobSchema(4, geo = false)

  private def jobSchema(level: Int, geo: Boolean = true): Document = {
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
    // A canonical form can be longer than its 256-character display value: lower-casing expands only U+0130 (to two
    // characters), so twice the display bound is the exact worst case.
    val canonicalText =
      new Document("bsonType", "string").append("minLength", 1).append("maxLength", 2 * ShortTextMaxLength)
    val location =
      if (level < 3) new Document("bsonType", "object").append("properties", new Document(MongoFields.Point, geoPoint))
      else
        // A city and its canonical form are both present or both absent.
        new Document("bsonType", "object")
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
            new Document(MongoFields.Point, geoPoint)
              .append(MongoFields.City, shortText)
              .append(MongoFields.CityCanonical, canonicalText)
          )
    val properties = new Document(MongoFields.Version, new Document("bsonType", "long").append("minimum", 0L))
      .append(MongoFields.AdmissionFence, MongoValidatorSchemas.nonNegativeLong)
      .append(MongoFields.Location, location)
    if (level >= 3) {
      val textArray = (items: Document) => new Document("bsonType", "array").append("items", items)
      val _ = properties
        .append(MongoFields.Skills, textArray(new Document("bsonType", "string")))
        .append(MongoFields.SkillsCanonical, textArray(canonicalText))
    }
    if (level >= 4) { val _ = properties.append(MongoFields.Embedding, binaryVector) }
    val required =
      if (level < 3) List(MongoFields.Version)
      else List(MongoFields.Version, MongoFields.Skills, MongoFields.SkillsCanonical, MongoFields.Location)
    val schema = new Document(
      "$jsonSchema",
      new Document("bsonType", "object")
        .append("required", required.asJava)
        .append("properties", properties)
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
    // `$toLower` is ASCII-only in MongoDB, so the equality of each canonical value with its display value cannot be
    // checked here; the validator pins the shape (pairing, types, one canonical value per skill). The writers and the
    // 027 backfill derive both from `SearchTerm.canonical` in one update.
    val canonicalPairing = new Document(
      "$expr",
      new Document(
        "$cond",
        List(
          new Document(
            "$and",
            List(
              new Document("$isArray", List(s"$$${MongoFields.Skills}").asJava),
              new Document("$isArray", List(s"$$${MongoFields.SkillsCanonical}").asJava)
            ).asJava
          ),
          new Document(
            "$eq",
            List(
              new Document("$size", s"$$${MongoFields.Skills}"),
              new Document("$size", s"$$${MongoFields.SkillsCanonical}")
            ).asJava
          ),
          false
        ).asJava
      )
    )
    new Document(
      "$and",
      (List(schema) ++ Option.when(geo)(geoPredicate).toList ++ Option.when(level >= 3)(canonicalPairing).toList).asJava
    )
  }

  def createJobValidator(database: SetupDatabase): IO[Unit] =
    install(database, MongoCollections.Jobs, jobValidator)
}
