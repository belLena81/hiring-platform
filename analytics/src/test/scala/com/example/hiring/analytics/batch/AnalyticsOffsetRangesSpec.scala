package com.example.hiring.analytics.adapter.spark
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

import com.example.hiring.analytics.domain.PartitionOffsetRange

import munit.ScalaCheckSuite
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

final class AnalyticsOffsetRangesSpec extends ScalaCheckSuite {
  property("a complete dense offset interval accepts exactly its requested observation") {
    forAll(Gen.chooseNum(0L, 1000000000L), Gen.chooseNum(1L, 1000000L)) { (start: Long, length: Long) =>
      val end = start + length
      val range = PartitionOffsetRange.unsafe("hiring.operational-events", 0, start, end)
      val observed = AnalyticsOffsetRanges.Observed(length, start, end - 1L)

      AnalyticsOffsetRanges.complete(range, Some(observed)).isRight
    }
  }

  property("offset availability accepts intervals within broker retention bounds") {
    forAll(
      Gen.chooseNum(0L, 1000000000L),
      Gen.chooseNum(1L, 1000000L),
      Gen.chooseNum(0L, 1000000L),
      Gen.chooseNum(0L, 1000000L)
    ) { (start: Long, length: Long, earlier: Long, later: Long) =>
      val end = start + length
      val range = PartitionOffsetRange.unsafe("hiring.operational-events", 0, start, end)

      AnalyticsOffsetRanges.available(range, start - earlier, end + later).isRight
    }
  }
}
