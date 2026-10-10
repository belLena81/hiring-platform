package com.example.graphQL.cats.service.port

/** A repository snapshot paired with the revision required for compare-and-set writes. */
final case class Versioned[+A](value: A, version: Long)

object Versioned {
  def nextVersion(version: Long): Option[Long] = Option.when(version >= 0L && version < Long.MaxValue)(version + 1L)
}
