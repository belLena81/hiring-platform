package com.example.hiring.analytics.cli

import cats.effect.{Deferred, ExitCode, IO, IOApp, Ref, Resource}
import cats.syntax.all.*
import com.example.hiring.analytics.AnalyticsTestOperationalConfig
import com.example.hiring.analytics.adapter.mongo.MongoAnalyticsLakehouseLock
import com.example.hiring.analytics.adapter.spark.{StreamingQueryFactory, StreamingQueryHandle, StreamingQueryLifecycle}
import com.example.hiring.analytics.app.AppModule
import com.example.hiring.analytics.errors.AnalyticsError
import com.mongodb.client.MongoClients
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, LinkOption, Path, StandardOpenOption}
import java.nio.file.attribute.PosixFilePermissions
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Forked JVM ownership proof. It does not grant streaming activation or prove business admission. */
object StreamingProcessTerminationProofMain extends IOApp {
  private final case class Settings(mongoUri: String, database: String, nonce: String) {
    override def toString: String = "StreamingProcessTerminationProofSettings([REDACTED])"
    val root: Path = Path.of(".local", "data", s"hiring-streaming-signal-proof-$nonce").toAbsolutePath.normalize()
    val spill: Path = root.resolve("spill")
    val ready: Path =
      Path.of(".local", "logs", s"hiring-streaming-signal-proof-$nonce.ready").toAbsolutePath.normalize()
    val result: Path =
      Path.of(".local", "logs", s"hiring-streaming-signal-proof-$nonce.result").toAbsolutePath.normalize()
  }

  private def settings: IO[Settings] = IO
    .blocking {
      val config = ConfigFactory.load().getConfig("streaming-signal-proof")
      val value = Settings(config.getString("mongo-uri"), config.getString("mongo-database"), config.getString("nonce"))
      require(value.nonce.matches("[a-f0-9]{16}"))
      require(value.database == s"hiring_signal_proof_${value.nonce}")
      require(value.mongoUri.nonEmpty)
      value
    }
    .handleErrorWith(_ =>
      IO.raiseError(AnalyticsError.InvalidConfiguration("streaming signal proof configuration is invalid"))
    )

  private def assertSafeAncestors(path: Path): Unit = {
    var ancestor = Option(path)
    while (ancestor.nonEmpty) {
      require(!Files.isSymbolicLink(ancestor.get), "signal proof paths must not contain symbolic links")
      ancestor = Option(ancestor.get.getParent)
    }
  }

  private def write(path: Path, value: String): IO[Unit] = IO.blocking {
    Files.createFile(path, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")))
    Files.writeString(path, value, UTF_8, StandardOpenOption.WRITE)
    ()
  }

  private def lockCount(value: Settings): IO[Long] = IO.blocking {
    val client = MongoClients.create(value.mongoUri)
    try client.getDatabase(value.database).getCollection("analytics_lakehouse_mutexes").countDocuments()
    finally client.close()
  }

  private def ownedSpill(value: Settings): Resource[IO, Path] = Resource.make(IO.blocking {
    Vector(value.root, value.ready, value.result).foreach(assertSafeAncestors)
    require(!Files.exists(value.root, LinkOption.NOFOLLOW_LINKS), "signal proof root must be fresh")
    require(!Files.exists(value.ready, LinkOption.NOFOLLOW_LINKS), "signal proof ready file must be fresh")
    require(!Files.exists(value.result, LinkOption.NOFOLLOW_LINKS), "signal proof result file must be fresh")
    val privateDirectory = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------"))
    Files.createDirectories(value.root.getParent, privateDirectory)
    Files.createDirectories(value.ready.getParent, privateDirectory)
    Files.createDirectory(value.root, privateDirectory)
    Files.createDirectory(value.spill, privateDirectory)
  })(spill =>
    IO.blocking {
      // Remove only this freshly acquired child directory, without following symbolic links.
      val paths = Files.walk(spill)
      try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.delete)
      finally paths.close()
    }
  )

  private def program(value: Settings): IO[Unit] = for {
    original <- Ref.of[IO, Option[(SparkSession, StreamingQuery)]](None)
    acquired <- Deferred[IO, (SparkSession, StreamingQuery)]
    readinessFailure <- Deferred[IO, Throwable]
    maintenanceReleased <- Ref.of[IO, Boolean](false)
    _ <- lockCount(value).flatMap(count =>
      IO.raiseUnless(count == 0L)(
        new IllegalStateException("signal proof database already contains an owned mutex")
      )
    )
    owned = ownedSpill(value).flatMap(spill =>
      AppModule
        .sparkMongo[IO](
          value.mongoUri,
          "local[2]",
          "hiring-streaming-signal-proof",
          sparkUiEnabled = Some(false),
          sparkLocalDirectory = spill.toString
        )
        .flatMap { case (spark, client, execution) =>
          Resource.eval(client.getDatabase(value.database)).flatMap { database =>
            new MongoAnalyticsLakehouseLock[IO](database, AnalyticsTestOperationalConfig.streams)
              .resource(value.root.toUri.toString)
              .flatMap { _ =>
                val factory = new StreamingQueryFactory[IO] {
                  override def start: IO[StreamingQueryHandle[IO]] = execution {
                    spark.readStream
                      .format("rate")
                      .option("rowsPerSecond", 1)
                      .load()
                      .writeStream
                      .foreachBatch((batch: DataFrame, _: Long) => { batch.count(); () })
                      .option("checkpointLocation", value.root.resolve("checkpoint").toString)
                      .start()
                  }.flatTap(query => original.set(Some((spark, query))) *> acquired.complete((spark, query)).void).map {
                    query =>
                      new StreamingQueryHandle[IO] {
                        override def awaitTermination: IO[Unit] = IO.interruptibleMany(query.awaitTermination())
                        override def stop: IO[Unit] = execution(query.stop())
                      }
                  }
                }
                val maintenance = Resource
                  .make(IO.never[Unit].start)(fiber => fiber.cancel *> maintenanceReleased.set(true))
                  .as(IO.never[Unit])
                val ready = acquired.get
                  .flatMap { case (session, query) =>
                    def waitForProgress: IO[Unit] = execution(query.recentProgress.nonEmpty).flatMap {
                      case true  => IO.unit
                      case false => IO.sleep(100.millis) *> IO.defer(waitForProgress)
                    }
                    waitForProgress.timeout(60.seconds) *>
                      execution(
                        require(query.isActive && !session.sparkContext.isStopped, "original query must be active")
                      ) *>
                      lockCount(value).flatMap(count =>
                        IO.raiseUnless(count == 1L)(
                          new IllegalStateException("real Mongo mutex control absent")
                        )
                      ) *> write(
                        value.ready,
                        "READY originalQueryActive=true originalContextStopped=false mutexRows=1\n"
                      )
                  }
                // Startup blocks in the lifecycle; readiness is a joined child owned by this Resource.
                Resource.make(ready.handleErrorWith(error => readinessFailure.complete(error).void).start)(_.cancel) *>
                  StreamingQueryLifecycle
                    .resource(factory, readinessFailure.get.flatMap(IO.raiseError[Unit]), maintenance)
              }
          }
        }
    )
    _ <- StreamingProcessTermination.run(owned.use(_ => IO.unit))
    captured <- original.get.flatMap(_.liftTo[IO](new IllegalStateException("original query was never acquired")))
    _ <- IO.blocking {
      require(captured._1.sparkContext.isStopped, "original Spark context remains active")
      require(!captured._2.isActive, "original streaming query remains active")
      require(!Files.exists(value.spill, LinkOption.NOFOLLOW_LINKS), "owned spill remains")
    }
    released <- maintenanceReleased.get
    _ <- IO.raiseUnless(released)(new IllegalStateException("maintenance finalizer was not joined"))
    count <- lockCount(value)
    _ <- IO.raiseUnless(count == 0L)(new IllegalStateException("real Mongo mutex remains"))
    _ <- write(
      value.result,
      "PASS originalContextStopped=true originalQueryActive=false ownedSpillExists=false mutexRows=0 maintenanceReleased=true\n"
    )
  } yield ()

  override def run(args: List[String]): IO[ExitCode] = AnalyticsCliProgram.runProgram(
    IO.raiseUnless(args.isEmpty)(
      AnalyticsError.InvalidConfiguration("signal proof accepts only private HOCON settings")
    ) *>
      settings.flatMap(program)
  )
}
