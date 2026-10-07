package com.example.hiring.testing

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.mongodb.ConnectionString
import com.github.dockerjava.api.model.Ulimit
import io.circe.{Decoder, Json}
import io.circe.generic.semiauto.*
import io.circe.parser.decode
import mongo4cats.client.MongoClient
import mongo4cats.database.MongoDatabase
import fs2.interop.reactivestreams.*
import org.apache.kafka.clients.admin.Admin
import org.bson.Document
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import java.nio.file.{Files, Path, StandardOpenOption}
import java.nio.file.attribute.PosixFilePermission
import java.time.Duration
import java.util.Properties
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Test-only infrastructure ownership. External service identity is checked before any database mutation. */
object LocalTestServices {
  val MongoImage = "mongo:8.0.32-noble@sha256:01354084d2ae665d2e79b79b0cdc50c2c0c98873618912d9a2c8c9cb5c3d24e6"
  final case class Manifest(
      schema: Int,
      workspace: String,
      nonce: String,
      project: String,
      mongoUri: String,
      replicaSet: String,
      kafkaBootstrap: String,
      kafkaClusterId: String,
      brokerPassword: String,
      publisherPassword: String,
      readerPassword: String,
      fencerPassword: String,
      orchestratorPassword: String,
      workerPassword: String,
      interviewFencerPassword: String
  ) { override def toString: String = "IsolatedTestServices([REDACTED])" }
  given Decoder[Manifest] = deriveDecoder

  final case class MongoEndpoint(uri: String, sampleResources: IO[Json])
  private final class ReplicaSet extends GenericContainer[ReplicaSet](DockerImageName.parse(MongoImage))
  private val unavailableResources = IO.pure(Json.obj("status" -> Json.fromString("unavailable")))

  def validateManifest(value: Manifest, workspace: Path): Either[String, Manifest] = {
    val checked = scala.util
      .Try {
        val mongo = new ConnectionString(value.mongoUri)
        val hosts = mongo.getHosts.asScala.toVector
        val prefix = "hiring-tests-" + sha(workspace.toRealPath().toString).take(12) + "-" + value.nonce.take(8)
        value.schema == 1 && value.nonce.matches("[a-f0-9]{32}") && value.project == prefix &&
        Path.of(value.workspace).toRealPath() == workspace.toRealPath() &&
        value.replicaSet == "hiring_test_" + value.nonce && !mongo.isSrvProtocol &&
        hosts.size == 1 && validEndpoint(hosts.head, 27017) &&
        Option(mongo.getRequiredReplicaSetName).contains(value.replicaSet) &&
        Option(mongo.isDirectConnection).contains(java.lang.Boolean.TRUE) &&
        Option(mongo.getDatabase).isEmpty && mongo.getCredential == null &&
        validEndpoint(value.kafkaBootstrap, 9092) && value.kafkaClusterId.matches("[A-Za-z0-9_-]{22}") &&
        Vector(
          value.brokerPassword,
          value.publisherPassword,
          value.readerPassword,
          value.fencerPassword,
          value.orchestratorPassword,
          value.workerPassword,
          value.interviewFencerPassword
        ).forall(_.matches("[a-f0-9]{48}"))
      }
      .toOption
      .contains(true)
    Either.cond(checked, value, "Invalid isolated test service identity")
  }

  private def validEndpoint(endpoint: String, forbiddenPort: Int): Boolean =
    endpoint.split(":", -1).toVector match {
      case Vector("127.0.0.1", port) =>
        port.toIntOption.exists(value => value > 1024 && value <= 65535 && value != forbiddenPort)
      case _ => false
    }

  private def sha(value: String): String = java.util.HexFormat
    .of()
    .formatHex(
      java.security.MessageDigest.getInstance("SHA-256").digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8))
    )

  def manifest: IO[Option[Manifest]] = sys.env.get("HIRING_TEST_MANIFEST") match {
    case None       => IO.pure(None)
    case Some(path) =>
      IO.blocking {
        val file = Path.of(path)
        require(!Files.isSymbolicLink(file), "Test manifest must not be a symlink")
        val permissions = Files.getPosixFilePermissions(file).asScala.toSet
        require(
          permissions == Set(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE),
          "Test manifest requires owner-only permissions"
        )
        val raw = Files.readString(file)
        val parsed = decode[Manifest](raw).left.map(_ => "Invalid test service manifest")
        val workspace = Path.of(sys.env.getOrElse("HIRING_TEST_WORKSPACE", System.getProperty("user.dir")))
        parsed
          .flatMap(validateManifest(_, workspace))
          .fold(message => throw new IllegalArgumentException(message), value => Option(value))
      }.flatTap(_.traverse_(verifyRunOwner))
  }

  private def verifyRunOwner(value: Manifest): IO[Unit] = IO.blocking {
    val path = sys.env
      .get("HIRING_TEST_RUN_REGISTRY")
      .map(Path.of(_))
      .getOrElse(
        throw new IllegalArgumentException(
          "Reusable test services require a locked run registry; use scripts/run-local-tests.sh"
        )
      )
    require(
      !Files.isSymbolicLink(path) && path.getFileName.toString.matches("[a-f0-9]{32}"),
      "Invalid test run registry"
    )
    val parent = Path.of(value.workspace).resolve(".local/data/test-services/runs").toRealPath()
    require(path.toRealPath().getParent == parent, "Test run registry must belong to this workspace")
    val identity = path.resolve("service.json")
    val lock = path.resolve("owner.lock")
    require(
      !Files.isSymbolicLink(identity) && !Files.isSymbolicLink(lock) && Files.isRegularFile(lock),
      "Invalid run ownership files"
    )
    val marker = io.circe.parser.parse(Files.readString(identity)).toOption
    val expected = Json.obj("nonce" -> Json.fromString(value.nonce), "project" -> Json.fromString(value.project))
    require(marker.contains(expected), "Test run registry service identity mismatch")
    val status = new ProcessBuilder("flock", "-n", lock.toString, "true").start().waitFor()
    require(status == 1, "Test run registry requires a live owner lock")
  }

  def command(database: MongoDatabase[IO], value: Document): IO[Document] =
    IO.delay(database.underlying.runCommand(value, classOf[Document]))
      .flatMap(_.toStreamBuffered[IO](1).compile.lastOrError)

  def verifiedMongo(value: Manifest): IO[Unit] =
    MongoClient.fromConnectionString[IO](value.mongoUri).use { client =>
      for {
        admin <- client.getDatabase("admin")
        hello <- command(admin, new Document("hello", 1))
        _ <- IO.raiseUnless(
          Option(hello.getString("setName")).contains(value.replicaSet) && hello.getBoolean("isWritablePrimary", false)
        )(new IllegalArgumentException("Test Mongo identity or primary mismatch"))
        control <- client.getDatabase("hiring_test_control")
        marker <- command(
          control,
          new Document("find", "identity").append("filter", new Document("_id", value.nonce)).append("limit", 1)
        )
        batch = marker.get("cursor", classOf[Document]).getList("firstBatch", classOf[Document]).asScala
        _ <- IO.raiseUnless(batch.size == 1 && Option(batch.head.getString("project")).contains(value.project))(
          new IllegalArgumentException("Test Mongo marker mismatch")
        )
      } yield ()
    }

  def adminProperties(value: Manifest, username: String, password: String): Properties = {
    val properties = new Properties()
    properties.put("bootstrap.servers", value.kafkaBootstrap)
    properties.put("security.protocol", "SASL_PLAINTEXT")
    properties.put("sasl.mechanism", "PLAIN")
    properties.put(
      "sasl.jaas.config",
      s"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$username\" password=\"$password\";"
    )
    properties.put("default.api.timeout.ms", "10000")
    properties.put("request.timeout.ms", "10000")
    properties
  }

  def verifiedKafka(value: Manifest): IO[Unit] =
    Resource
      .make(IO.blocking(Admin.create(adminProperties(value, "broker", value.brokerPassword))))(admin =>
        IO.blocking(admin.close())
      )
      .use(admin =>
        IO.blocking(admin.describeCluster().clusterId().get(10L, java.util.concurrent.TimeUnit.SECONDS)).flatMap { id =>
          IO.raiseUnless(id == value.kafkaClusterId)(
            new IllegalArgumentException("Test Kafka cluster identity mismatch")
          )
        }
      )

  /** Suite-scoped service endpoint; dedicated suites never attach to reusable services. */
  def mongoEndpoint(dedicated: Boolean = false, testCommands: Boolean = false): Resource[IO, MongoEndpoint] =
    Resource.eval(if (dedicated) IO.pure(None) else manifest).flatMap {
      case Some(value) => Resource.eval(verifiedMongo(value)).as(MongoEndpoint(value.mongoUri, unavailableResources))
      case None        => disposableMongo(testCommands)
    }

  private def disposableMongo(testCommands: Boolean): Resource[IO, MongoEndpoint] =
    Resource
      .make(IO.blocking {
        val container = new ReplicaSet
        val command = Vector("mongod", "--bind_ip_all", "--replSet", "rs0", "--wiredTigerCacheSizeGB", "0.25") ++
          (if (testCommands) Vector("--setParameter", "enableTestCommands=1") else Vector.empty)
        val _ = container
          .withExposedPorts(27017)
          .withCreateContainerCmdModifier(command => {
            val _ = command.getHostConfig.withUlimits(Array(new Ulimit("nofile", 65536L, 65536L)))
            val _ = command.getHostConfig.withMemory(3L * 1024L * 1024L * 1024L)
          })
          .withTmpFs(Map("/data/db" -> "rw,size=2g", "/data/configdb" -> "rw").asJava)
          .withCommand(command*)
          .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(90)))
        try {
          container.start()
          val result = container.execInContainer(
            "mongosh",
            "--quiet",
            "--eval",
            "rs.initiate({_id:'rs0',members:[{_id:0,host:'localhost:27017'}]})"
          )
          require(result.getExitCode == 0, "Test replica set initiation failed")
          container
        } catch { case error: Throwable => container.stop(); throw error }
      })(container => IO.blocking(container.stop()))
      .evalTap { container =>
        def primary(remaining: Int): IO[Unit] = IO
          .blocking(container.execInContainer("mongosh", "--quiet", "--eval", "db.hello().isWritablePrimary"))
          .flatMap { result =>
            if (result.getExitCode == 0 && result.getStdout.trim == "true") IO.unit
            else if (remaining > 0) IO.sleep(250.millis) *> primary(remaining - 1)
            else IO.raiseError(new IllegalStateException("Test replica set primary unavailable"))
          }
        primary(60)
      }
      .map { container =>
        val sample = IO
          .blocking(
            container.execInContainer("sh", "-c", "cat /sys/fs/cgroup/cpu.stat && cat /sys/fs/cgroup/memory.current")
          )
          .map { result =>
            val lines = result.getStdout.linesIterator.toVector
            val cpu = lines.collectFirst {
              case line if line.startsWith("usage_usec ") => line.drop(11).trim.toLongOption
            }.flatten
            val memory = lines.lastOption.flatMap(_.trim.toLongOption)
            Json.obj(
              "status" -> Json.fromString(
                if (result.getExitCode == 0 && cpu.nonEmpty && memory.nonEmpty) "available" else "unavailable"
              ),
              "cpuUsageMicros" -> cpu.fold(Json.Null)(Json.fromLong),
              "memoryCurrentBytes" -> memory.fold(Json.Null)(Json.fromLong)
            )
          }
          .handleError(_ => Json.obj("status" -> Json.fromString("unavailable")))
        MongoEndpoint(
          s"mongodb://${container.getHost}:${container.getMappedPort(27017)}/?replicaSet=rs0&directConnection=true",
          sample
        )
      }

  /** Registration precedes database creation, allowing exact owned orphan cleanup after a killed JVM. */
  def database(client: MongoClient[IO]): Resource[IO, MongoDatabase[IO]] =
    Resource
      .eval(IO.randomUUID.map(id => "hiring_test_" + id.toString.replace("-", "")))
      .flatMap(databaseNamed(client, _))

  def databaseNamed(client: MongoClient[IO], name: String): Resource[IO, MongoDatabase[IO]] = {
    require(name.matches("hiring_test_[a-f0-9]{32}"), "Only generated test database names are permitted")
    val registered = sys.env.get("HIRING_TEST_RUN_REGISTRY") match {
      case Some(path) =>
        IO.blocking {
          val root = Path.of(path).toRealPath()
          require(!Files.isSymbolicLink(Path.of(path)), "Test run registry must not be a symlink")
          val file = root.resolve(name + ".database")
          val _ = Files.writeString(file, name, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
          file
        }.map(Some(_))
      case None => IO.pure(None)
    }
    Resource.eval(registered).flatMap { marker =>
      Resource.make(client.getDatabase(name))(database =>
        command(database, new Document("dropDatabase", 1)).void *>
          marker.traverse_(path => IO.blocking(Files.deleteIfExists(path)).void)
      )
    }
  }
}
