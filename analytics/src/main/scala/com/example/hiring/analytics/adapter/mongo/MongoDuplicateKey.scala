package com.example.hiring.analytics.adapter.mongo

import com.mongodb.MongoException

/** Matches the Mongo duplicate-key write failure (server error code 11000). */
private[analytics] object MongoDuplicateKey {
  def unapply(error: Throwable): Boolean = error match {
    case mongo: MongoException => mongo.getCode == 11000
    case _                     => false
  }
}
