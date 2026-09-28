package com.example.hiring.analytics.cli
import com.example.hiring.analytics.service.keyretirement.*
import com.example.hiring.analytics.service.batch.*

import cats.effect.{ExitCode, IO, IOApp}

object HiringAnalyticsBatchMain extends IOApp {
  override def run(args: List[String]): IO[ExitCode] =
    com.example.hiring.analytics.adapter.spark.HiringAnalyticsBatchMain.run(args)
}

object AnalyticsErasureWorkerMain extends IOApp {
  override def run(args: List[String]): IO[ExitCode] =
    com.example.hiring.analytics.adapter.spark.AnalyticsErasureWorkerMain.run(args)
}

object AnalyticsErasureRepairMain extends IOApp {
  override def run(args: List[String]): IO[ExitCode] =
    com.example.hiring.analytics.adapter.spark.AnalyticsErasureRepairMain.run(args)
}
