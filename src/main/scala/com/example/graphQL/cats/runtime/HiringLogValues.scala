package com.example.graphQL.cats.runtime

import com.example.graphQL.cats.config.ConfigError
import com.example.graphQL.cats.repository.mongo.MigrationError
import com.example.graphQL.cats.service.PublicLogValues

/** The startup allow-lists that let diagnostics log adapter failure types and configuration keys. */
object HiringLogValues {
  val allowed: PublicLogValues = PublicLogValues(MigrationError.publicTypeNames, ConfigError.publicKeys)
}
