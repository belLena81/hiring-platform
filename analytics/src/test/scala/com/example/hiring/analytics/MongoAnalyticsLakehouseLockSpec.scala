package com.example.hiring.analytics

import com.example.hiring.analytics.mongo.MongoAnalyticsLakehouseLock
import munit.FunSuite

final class MongoAnalyticsLakehouseLockSpec extends FunSuite {
  test("lakehouse mutex keys canonicalize URI scheme, authority casing, and trailing slashes") {
    val first = MongoAnalyticsLakehouseLock.lockId("s3a://BUCKET-a/lakehouse/")
    val equivalent = MongoAnalyticsLakehouseLock.lockId("S3A://bucket-a/lakehouse")
    val different = MongoAnalyticsLakehouseLock.lockId("s3a://bucket-a/another-lakehouse")

    assertEquals(first, equivalent)
    assertNotEquals(first, different)
    assertEquals(
      MongoAnalyticsLakehouseLock.lockId("s3a://bucket-a"),
      MongoAnalyticsLakehouseLock.lockId("s3a://bucket-a/")
    )
    val originalLocale = java.util.Locale.getDefault
    try {
      java.util.Locale.setDefault(new java.util.Locale("tr", "TR"))
      assertEquals(
        MongoAnalyticsLakehouseLock.lockId("HDFS://NAMENODE/lakehouse"),
        MongoAnalyticsLakehouseLock.lockId("hdfs://namenode/lakehouse")
      )
    } finally java.util.Locale.setDefault(originalLocale)
  }

  test("lakehouse mutex keys reject URI credentials and query strings") {
    assert(MongoAnalyticsLakehouseLock.lockId("s3a://user:secret@bucket/lakehouse").isLeft)
    assert(MongoAnalyticsLakehouseLock.lockId("s3a://bucket/lakehouse?token=secret").isLeft)
  }
}
