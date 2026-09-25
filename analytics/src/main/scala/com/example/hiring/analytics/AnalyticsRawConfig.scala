package com.example.hiring.analytics

import pureconfig.ConfigReader

/** Shared HOCON input records used by the analytics batch/worker and the key-retirement audit. */
private[analytics] object AnalyticsRawConfig {
  final case class Mongo(uri: Option[String], database: Option[String]) derives ConfigReader
  final case class Spark(master: Option[String]) derives ConfigReader
  final case class Lakehouse(root: Option[String]) derives ConfigReader
}
