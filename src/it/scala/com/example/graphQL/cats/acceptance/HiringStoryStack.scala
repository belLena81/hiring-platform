package com.example.graphQL.cats.acceptance

import cats.effect.{IO, Resource}
import com.example.hiring.testing.{KafkaTestNamespace, LocalTestServices, RecoveryApiProcess}
import io.circe.parser.parse
import java.net.{InetAddress, ServerSocket, URI}
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.file.attribute.PosixFilePermissions
import java.nio.file.{Files, Path}
import java.time.Duration
import java.util.UUID
import scala.concurrent.duration.*

/** The real `Main` started as a child JVM against the wrapper-owned Mongo and Kafka services.
  *
  * The Admin is provisioned only through the explicit `auth.admin-seed` configuration of a private, ignored
  * configuration file, as the README describes. Vector search is disabled because the test stack has no Atlas search.
  */
object HiringStoryStack {
  val AdminName = "Story Admin"
  val AdminPassword = "story-admin-password-1"

  final case class Running(
      baseUrl: String,
      database: String,
      mongoUri: String,
      namespace: KafkaTestNamespace.Namespace,
      client: GraphQLClient
  )

  private val readyDeadline = 90.seconds
  // The HTTP bind port is restricted to 1024..32767, below the Linux ephemeral range that port 0 allocates from.
  private val configurablePortRange = 1024 to 32767
  private val maxPortAttempts = 200

  private[acceptance] def freePort: IO[Int] = IO.blocking {
    val address = InetAddress.getByName("127.0.0.1")
    val start = scala.util.Random.nextInt(configurablePortRange.size)
    Iterator
      .range(0, maxPortAttempts)
      .map(i => configurablePortRange((start + i) % configurablePortRange.size))
      .flatMap { port =>
        scala.util.Try { val socket = new ServerSocket(port, 1, address); socket.close(); port }.toOption
      }
      .nextOption()
      .getOrElse(throw new java.io.IOException(s"No free port found in $configurablePortRange"))
  }

  private[acceptance] val childClasspath: String = {
    val integrationClasses = Path.of(getClass.getProtectionDomain.getCodeSource.getLocation.toURI)
    val outputDirectory = integrationClasses.getParent
    val projectOutputs =
      List(integrationClasses, outputDirectory.resolve("classes"), outputDirectory.resolve("test-classes"))
        .filter(Files.isDirectory(_))
        .map(_.toString)
    (projectOutputs :+ System.getProperty("java.class.path")).mkString(java.io.File.pathSeparator)
  }

  private[acceptance] def hocon(value: String): String = "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\""

  private def configuration(
      manifest: LocalTestServices.Manifest,
      namespace: KafkaTestNamespace.Namespace,
      database: String,
      port: Int,
      jwtSecret: String,
      interview: Boolean
  ): String =
    s"""include classpath("application.conf")
       |mongo.uri = ${hocon(manifest.mongoUri)}
       |mongo.database = ${hocon(database)}
       |mongo.reset-on-start = false
       |http.host = "127.0.0.1"
       |http.port = $port
       |logging.level = "INFO"
       |auth.jwt.hs256-secret = ${hocon(jwtSecret)}
       |auth.admin-seed.enabled = true
       |auth.admin-seed.name = ${hocon(AdminName)}
       |auth.admin-seed.password = ${hocon(AdminPassword)}
       |auth.rate-limit.attempts = 1000
       |graphql.cost-budget.max-cost = 1000000
       |graphql.schema-exposure = false
       |vector-search.enabled = false
       |kafka.enabled = true
       |kafka.bootstrap-servers = ${hocon(manifest.kafkaBootstrap)}
       |kafka.sasl-security-protocol = "SASL_PLAINTEXT"
       |kafka.topic = ${hocon(namespace.events)}
       |kafka.consumer-group = ${hocon(namespace.eventGroup)}
       |kafka.publisher.sasl-username = "hiring_publisher_v2"
       |kafka.publisher.sasl-password = ${hocon(manifest.publisherPassword)}
       |kafka.publisher.poll-interval-ms = 100
       |kafka.consumer.enabled = false
       |kafka.interview.enabled = $interview
       |kafka.interview.commands-topic = ${hocon(namespace.commands)}
       |kafka.interview.results-topic = ${hocon(namespace.results)}
       |kafka.interview.worker-group = ${hocon(namespace.workers)}
       |kafka.interview.orchestrator-group = ${hocon(namespace.orchestrator)}
       |kafka.interview.orchestrator-username = "interview_command_publisher"
       |kafka.interview.orchestrator-password = ${hocon(manifest.orchestratorPassword)}
       |kafka.interview.worker-username = "interview_result_publisher"
       |kafka.interview.worker-password = ${hocon(manifest.workerPassword)}
       |kafka.interview.fencer-username = "interview_fencer"
       |kafka.interview.fencer-password = ${hocon(manifest.interviewFencerPassword)}
       |""".stripMargin

  def resource(
      manifest: LocalTestServices.Manifest,
      namespace: KafkaTestNamespace.Namespace,
      database: String,
      interview: Boolean
  ): Resource[IO, Running] =
    for {
      root <- runDirectory(manifest.workspace)
      port <- Resource.eval(freePort)
      jwtSecret = UUID.randomUUID().toString + UUID.randomUUID().toString
      baseUrl = s"http://127.0.0.1:$port"
      process <- apiProcess(manifest, namespace, database, port, jwtSecret, root, interview)
      client <- GraphQLClient.resource(baseUrl)
      _ <- Resource.eval(awaitReady(baseUrl, process))
    } yield Running(baseUrl, database, manifest.mongoUri, namespace, client)

  private[acceptance] def runDirectory(workspace: String): Resource[IO, Path] =
    Resource.make(IO.blocking {
      val parent = Path.of(workspace).resolve(".local/data/hiring-story-acceptance")
      val _ = Files.createDirectories(parent)
      Files.createTempDirectory(
        parent,
        "run-",
        PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------"))
      )
    })(root =>
      IO.blocking {
        val walk = Files.walk(root)
        try walk.sorted(java.util.Comparator.reverseOrder()).forEach(path => { val _ = Files.deleteIfExists(path); () })
        finally walk.close()
      }
    )

  private def apiProcess(
      manifest: LocalTestServices.Manifest,
      namespace: KafkaTestNamespace.Namespace,
      database: String,
      port: Int,
      jwtSecret: String,
      root: Path,
      interview: Boolean
  ): Resource[IO, Process] =
    launch(
      manifest.workspace,
      configuration(manifest, namespace, database, port, jwtSecret, interview),
      root,
      Nil,
      "hiring-story-acceptance"
    )

  /** Starts the real `Main` as a child JVM with a private, owner-only configuration file and a cleared environment. It
    * is stopped (TERM, then KILL after 15 s) when the resource is released.
    */
  private[acceptance] def launch(
      workspace: String,
      configText: String,
      root: Path,
      jvmOptions: List[String],
      logName: String
  ): Resource[IO, Process] =
    Resource.make(IO.blocking {
      val configFile = root.resolve("api.conf")
      val _ = Files.writeString(configFile, configText)
      Files.setPosixFilePermissions(configFile, PosixFilePermissions.fromString("rw-------"))
      val logs = Path.of(workspace).resolve(".local/logs/test-services")
      val _ = Files.createDirectories(logs)
      val builder = new ProcessBuilder(
        RecoveryApiProcess.command(
          List(
            Path.of(System.getProperty("java.home"), "bin", "java").toString,
            "-Dfile.encoding=UTF-8",
            "-Dotel.sdk.disabled=true"
          ) ++ jvmOptions ++ List(
            s"-Dconfig.file=${configFile.toAbsolutePath}",
            "-cp",
            childClasspath,
            "com.example.graphQL.cats.Main"
          )
        )*
      ).directory(Files.createDirectories(logs.resolve(logName)).toFile)
      // Inherited application or provider configuration cannot override the synthetic fixture configuration.
      builder.environment().clear()
      val _ = builder.redirectErrorStream(true).redirectOutput(logs.resolve(s"$logName-api.log").toFile)
      builder.start()
    })(process =>
      IO.blocking {
        process.destroy()
        if (!process.waitFor(15, java.util.concurrent.TimeUnit.SECONDS)) {
          val _ = process.destroyForcibly()
          val _ = process.waitFor()
        }
        ()
      }
    )

  private[acceptance] def awaitReady(
      baseUrl: String,
      process: Process,
      deadline: FiniteDuration = readyDeadline
  ): IO[Unit] = {
    val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build()
    val probe = IO
      .blocking {
        require(process.isAlive, "The real Main exited before it became ready; see the API log under .local/logs")
        val request =
          HttpRequest.newBuilder(URI.create(baseUrl + "/ready")).timeout(Duration.ofSeconds(2)).GET().build()
        parse(http.send(request, HttpResponse.BodyHandlers.ofString()).body()).toOption
          .exists(body => body.hcursor.get[String]("status") == Right("READY"))
      }
      .handleErrorWith(error => if (process.isAlive) IO.pure(false) else IO.raiseError(error))
    def loop: IO[Unit] = probe.flatMap(ready => if (ready) IO.unit else IO.sleep(500.millis) *> loop)
    loop.timeoutTo(deadline, IO.raiseError(new IllegalStateException("The real Main did not become ready")))
  }
}

/** The child JVM leaves its consumer groups when it stops, but the broker may take a moment to drop the members. The
  * namespace cleanup deletes the groups and fails while any member remains, so ownership waits for them to be empty.
  */
object ConsumerGroupDrain {
  private val deadline = 90.seconds

  def awaitEmpty(namespace: KafkaTestNamespace.Namespace): IO[Unit] = {
    val manifest = namespace.manifest
    val groups = java.util.List.of(namespace.workers, namespace.orchestrator, namespace.eventGroup)
    Resource
      .make(
        IO.blocking(
          org.apache.kafka.clients.admin.Admin
            .create(LocalTestServices.adminProperties(manifest, "broker", manifest.brokerPassword))
        )
      )(admin => IO.blocking(admin.close()))
      .use { admin =>
        val empty = IO.blocking {
          val described = admin.describeConsumerGroups(groups).all().get(20L, java.util.concurrent.TimeUnit.SECONDS)
          described.values().stream().allMatch(group => group.members().isEmpty)
        }
        def loop: IO[Unit] = empty.flatMap(done => if (done) IO.unit else IO.sleep(500.millis) *> loop)
        loop.timeoutTo(deadline, IO.unit)
      }
  }
}
