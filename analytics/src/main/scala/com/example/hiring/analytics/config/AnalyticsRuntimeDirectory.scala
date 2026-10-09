package com.example.hiring.analytics.config

import java.nio.file.{Path, Paths}
import scala.jdk.CollectionConverters.*

/** The runtime directory the analytics container owns. Local runs use `.local/data/analytics` under the checkout. */
object AnalyticsRuntimeDirectory {
  val ContainerRoot: Path = Paths.get("/var/lib/hiring-analytics")
  val SparkTemp: String = "spark-temp"
  val Checkpoints: String = "checkpoints"

  private val LocalRootSegments: Vector[String] = Vector(".local", "data", "analytics")

  def container(category: String): Path = ContainerRoot.resolve(category)

  /** True when the absolute `path` lies in the owned container or local directory for `category`. The category
    * directory itself counts only when `allowCategoryRoot` is set; otherwise a child entry is required.
    */
  def owns(path: Path, category: String, allowCategoryRoot: Boolean): Boolean = {
    val containerCategory = container(category)
    val localCategory = LocalRootSegments :+ category
    val segments = path.iterator().asScala.map(_.toString).toVector
    val inLocalCheckout = segments.sliding(localCategory.size).zipWithIndex.exists { case (window, index) =>
      window == localCategory && (allowCategoryRoot || segments.size > index + localCategory.size)
    }
    val inContainer = path.startsWith(containerCategory) && (allowCategoryRoot || path != containerCategory)
    path.isAbsolute && (inContainer || inLocalCheckout)
  }
}
