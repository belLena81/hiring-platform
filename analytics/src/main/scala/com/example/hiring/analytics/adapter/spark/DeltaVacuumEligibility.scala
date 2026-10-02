package com.example.hiring.analytics.adapter.spark

import org.apache.hadoop.fs.{LocalFileSystem, RawLocalFileSystem}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog

import java.util.concurrent.TimeUnit
import java.nio.file.{Files, LinkOption, Path}
import java.nio.file.attribute.BasicFileAttributes
import scala.util.control.NonFatal

/** A conservative proof that native FULL VACUUM cannot select a file or directory in a fresh flat local table. Anything
  * this probe cannot establish goes through the existing native command and its validation.
  */
private[analytics] object DeltaVacuumEligibility {
  private[analytics] def roundedRetentionMillis(hours: Double): Option[Long] =
    Option.when(hours.isFinite && hours >= 0d)(TimeUnit.HOURS.toMillis(math.round(hours)))

  private[analytics] def fileIsFresh(modifiedAt: Long, cutoff: Long): Boolean = modifiedAt >= cutoff

  def canSkip(spark: SparkSession, path: String, hours: Double, maximumEntries: Int): Boolean = {
    try {
      val log = DeltaLogFactory.system(spark, path)
      val snapshot = log.update()
      log.protocolWrite(snapshot.protocol)
      val rounded = roundedRetentionMillis(hours)
      val safetyEnabled = spark.conf.get("spark.databricks.delta.retentionDurationCheck.enabled", "true").toBoolean
      val liteEnabled = spark.conf.get("spark.databricks.delta.vacuum.lite.enabled", "false").toBoolean
      val knownProtocol = snapshot.protocol.readerFeatures.forall(_.isEmpty) &&
        snapshot.protocol.writerFeatures.forall(_.isEmpty)
      if (
        snapshot.version < 0L || snapshot.isCatalogOwned || liteEnabled || !knownProtocol || maximumEntries <= 0 ||
        rounded.isEmpty || (safetyEnabled && rounded.get < DeltaLog.tombstoneRetentionMillis(snapshot.metadata))
      ) false
      else {
        val fs = log.dataPath.getFileSystem(log.newDeltaHadoopConf())
        // NIO provides bounded directory iteration and link attributes for these known local filesystems.
        val knownFileSystem = fs.getClass == classOf[LocalFileSystem] || fs.getClass == classOf[RawLocalFileSystem]
        if (!knownFileSystem) false
        else {
          val directory = Path.of(fs.makeQualified(log.dataPath).toUri)
          var ancestor = Option(directory)
          var safe = true
          while (safe && ancestor.nonEmpty) {
            val status = Files.readAttributes(ancestor.get, classOf[BasicFileAttributes], LinkOption.NOFOLLOW_LINKS)
            safe = status.isDirectory && !status.isSymbolicLink
            ancestor = Option(ancestor.get.getParent)
          }
          // SQL/API VACUUM invokes gc with its default SystemClock, not DeltaLog's transaction clock.
          val cutoff = System.currentTimeMillis() - rounded.get
          var seen = 0
          var logDirectoryFound = false
          if (safe) {
            val stream = Files.newDirectoryStream(directory)
            try {
              val entries = stream.iterator()
              while (safe && entries.hasNext) {
                if (Thread.currentThread().isInterrupted)
                  throw new InterruptedException("Delta eligibility probe interrupted")
                if (seen >= maximumEntries) safe = false
                else {
                  val entry = entries.next()
                  seen += 1
                  val status = Files.readAttributes(entry, classOf[BasicFileAttributes], LinkOption.NOFOLLOW_LINKS)
                  if (status.isSymbolicLink) safe = false
                  else if (entry.getFileName.toString == "_delta_log") {
                    logDirectoryFound = true
                    safe = status.isDirectory
                  } else safe = status.isRegularFile && fileIsFresh(status.lastModifiedTime().toMillis, cutoff)
                }
              }
            } finally stream.close()
          }
          safe && logDirectoryFound
        }
      }
    } catch {
      // Fallback retains native errors; interruption/fatal errors are excluded from NonFatal.
      case NonFatal(_) => false
    }
  }
}
