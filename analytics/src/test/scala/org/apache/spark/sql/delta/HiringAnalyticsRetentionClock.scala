package org.apache.spark.sql.delta

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.{Clock, SystemClock}

/** Test-scoped interop for Spark's package-private clock required by Delta's cleanup overload. */
object HiringAnalyticsRetentionClock {
  def forTable(spark: SparkSession, path: String, shiftMillis: Long): DeltaLog = {
    val clock = new Clock {
      private val system = new SystemClock()
      override def getTimeMillis(): Long = system.getTimeMillis() + shiftMillis
      override def nanoTime(): Long = system.nanoTime()
      override def waitTillTime(target: Long): Long = system.waitTillTime(target - shiftMillis) + shiftMillis
    }
    // Delta caches by path; a preceding table read may have cached the system clock.
    DeltaLog.invalidateCache(spark, new Path(path))
    val log = DeltaLog.forTable(spark, new Path(path), clock)
    require(log.clock.eq(clock), "Delta cleanup must use the controlled test calendar")
    log
  }
}
