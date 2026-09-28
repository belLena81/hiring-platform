package com.example.hiring.analytics.config
import com.example.hiring.analytics.service.keyretirement.*
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.errors.*
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.config.*
import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.adapter.mongo.*
import com.example.hiring.analytics.adapter.kafka.*
import com.example.hiring.analytics.adapter.local.*
import com.example.hiring.analytics.service.erasure.*

import pureconfig.ConfigReader

/** Shared HOCON input records used by the analytics batch/worker and the key-retirement audit. */
private[analytics] object AnalyticsRawConfig {
  final case class Mongo(uri: Option[String], database: Option[String]) derives ConfigReader
  final case class Spark(master: Option[String]) derives ConfigReader
  final case class Lakehouse(root: Option[String]) derives ConfigReader
}
