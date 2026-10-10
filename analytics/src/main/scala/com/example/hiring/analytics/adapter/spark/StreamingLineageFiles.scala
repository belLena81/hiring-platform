package com.example.hiring.analytics.adapter.spark

/** Suffixes of the write-once control files kept per stream under the lakehouse lineage directory. */
private[analytics] object StreamingLineageFiles {
  val IdentitySuffix = ".identity"
  val EstablishedSuffix = ".established"
  val QueryIdSuffix = ".query-id"
}
