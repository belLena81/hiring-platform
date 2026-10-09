package com.example.hiring.analytics.cli

import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.all.*
import com.example.hiring.analytics.app.AppModule
import com.example.hiring.analytics.config.AnalyticsRuntimeConfig
import org.apache.spark.SparkThrowable
import scala.jdk.CollectionConverters.*

/** Operator-only production batch retry with bounded structural failure diagnostics. */
object HiringAnalyticsBatchDiagnosticMain extends IOApp {
  private def label(value: String): String =
    Option(value).filter(_.matches("[A-Za-z0-9_.$-]{1,160}")).getOrElse("UNAVAILABLE")

  private def causes(error: Throwable): Vector[Throwable] = {
    def next(current: Throwable, seen: Vector[Throwable]): Vector[Throwable] =
      if (current == null || seen.size >= 8 || seen.exists(_ eq current)) seen
      else next(current.getCause, seen :+ current)
    next(error, Vector.empty)
  }

  private def diagnose(error: Throwable): IO[Unit] =
    causes(error).zipWithIndex.traverse_ { case (cause, index) =>
      val structural = cause match {
        case spark: SparkThrowable =>
          val keys = Option(spark.getMessageParameters).toVector
            .flatMap(_.keySet().asScala)
            .map(label)
            .distinct
            .sorted
            .take(32)
            .mkString(",")
          s" condition=${label(spark.getCondition)} sqlState=${label(spark.getSqlState)} parameterKeys=$keys"
        case _ => ""
      }
      val owners = cause.getStackTrace.toVector
        .filter(_.getClassName.startsWith("com.example.hiring.analytics."))
        .take(8)
        .map(frame => s"${label(frame.getClassName)}.${label(frame.getMethodName)}:${frame.getLineNumber}")
        .mkString(",")
      IO.println(
        s"BATCH_RUNTIME_DIAGNOSTIC cause=$index class=${label(cause.getClass.getName)}$structural owners=$owners"
      )
    }

  override def run(args: List[String]): IO[ExitCode] =
    (for {
      _ <- IO.raiseUnless(args.isEmpty)(new IllegalArgumentException("batch diagnostic accepts no arguments"))
      settings <- AnalyticsRuntimeConfig.loadBatch[IO]
      _ <- AppModule.batch[IO](settings).use(identity)
      _ <- IO.println("BATCH_RUNTIME_DIAGNOSTIC_SUCCESS")
    } yield ExitCode.Success).handleErrorWith(error => diagnose(error).as(ExitCode.Error))
}
