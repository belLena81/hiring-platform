package com.example.graphQL.cats.repository.mongo

import scala.util.control.NoStackTrace

/** A fixed, sanitized startup or infrastructure failure of the Mongo adapters (Admin seed, Atlas Search, bounds). */
final case class MongoSetupError(message: String) extends RuntimeException(message) with NoStackTrace
