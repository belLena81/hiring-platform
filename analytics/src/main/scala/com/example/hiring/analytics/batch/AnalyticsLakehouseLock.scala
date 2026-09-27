package com.example.hiring.analytics.batch

import cats.effect.{IO, Resource}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Semaphore

/** Serializes every repository-managed batch, erasure, and retention operation for one lakehouse root. */
private[analytics] trait AnalyticsLakehouseLock {
  def resource(root: String): Resource[IO, Unit]
}

/** Process-local fallback for unit tests only. Production entry points must inject the Mongo implementation. */
private[analytics] object AnalyticsLakehouseLock {
  private val locks = new ConcurrentHashMap[String, Semaphore]()

  val processLocal: AnalyticsLakehouseLock = (root: String) =>
    Resource.make(IO.blocking {
      val lock = locks.computeIfAbsent(root, _ => new Semaphore(1))
      lock.acquire()
    })(_ => IO.blocking(locks.get(root).release()))

  def resource(root: String): Resource[IO, Unit] = processLocal.resource(root)
}
