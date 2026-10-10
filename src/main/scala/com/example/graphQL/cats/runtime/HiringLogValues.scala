package com.example.graphQL.cats.runtime

import com.mongodb.*
import com.example.graphQL.cats.config.ConfigError
import com.example.graphQL.cats.repository.mongo.{MigrationError, MongoSetupError}
import com.example.graphQL.cats.service.PublicLogValues

/** The startup allow-lists that let diagnostics log adapter and driver failure types and configuration keys. */
object HiringLogValues {
  private val driverErrorTypes: Set[String] = Set(
    classOf[MongoSecurityException],
    classOf[MongoTimeoutException],
    classOf[MongoSocketException],
    classOf[MongoSocketOpenException],
    classOf[MongoSocketReadException],
    classOf[MongoSocketReadTimeoutException],
    classOf[MongoCommandException],
    classOf[MongoWriteException],
    classOf[MongoBulkWriteException],
    classOf[MongoException],
    classOf[MongoSetupError]
  ).map(_.getName)

  val allowed: PublicLogValues =
    PublicLogValues(name => driverErrorTypes(name) || MigrationError.isPublicType(name), ConfigError.publicKeys)
}
