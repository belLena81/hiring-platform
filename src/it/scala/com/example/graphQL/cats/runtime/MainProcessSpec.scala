package com.example.graphQL.cats.runtime

import cats.effect.{IO, Resource}
import io.circe.Json
import io.circe.parser.parse
import java.net.{InetAddress, ServerSocket}
import java.nio.file.{Files, Path}
import java.time.Instant
import java.util.concurrent.TimeUnit
import munit.CatsEffectSuite
import scala.concurrent.duration.*

class MainProcessSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 45.seconds

  private final case class ChildResult(exitCode: Int, stdout: String, stderr: String)

  private val secret = "synthetic-secret"
  private val mainClass = "com.example.graphQL.cats.Main"

  private def listeningSocket(host: String = "127.0.0.1"): Resource[IO, ServerSocket] =
    Resource.make(IO.blocking(new ServerSocket(0, 1, InetAddress.getByName(host)))) { socket =>
      IO.blocking(socket.close())
    }

  private def outputDirectory: Resource[IO, Path] =
    Resource.make(IO.blocking(Files.createTempDirectory("hiring-main-process-"))) { directory =>
      IO.blocking {
        val _ = Files.deleteIfExists(directory.resolve("stdout.log"))
        val _ = Files.deleteIfExists(directory.resolve("stderr.log"))
        val _ = Files.deleteIfExists(directory.resolve("local.conf"))
        val _ = Files.deleteIfExists(directory)
      }
    }

  private def terminate(process: Process): IO[Unit] = IO.blocking {
    if (process.isAlive) {
      process.destroy()
      if (!process.waitFor(5, TimeUnit.SECONDS)) {
        val _ = process.destroyForcibly()
        assert(process.waitFor(5, TimeUnit.SECONDS), "Child JVM did not terminate")
      }
    }
  }

  private def runChild(entryPoint: String, environment: Map[String, String], arguments: List[String] = Nil): IO[ChildResult] =
    outputDirectory.use { directory =>
      val stdout = directory.resolve("stdout.log")
      val stderr = directory.resolve("stderr.log")
      Resource.make(IO.blocking {
        val configFile = directory.resolve("local.conf")
        val defaults = Map(
          "HTTP_HOST" -> "127.0.0.1",
          "HTTP_PORT" -> "8080",
          "MONGODB_URI" -> "${MONGODB_URI}",
          "MONGODB_DATABASE" -> "hiring_test",
          "LOG_LEVEL" -> "ERROR",
          "AUTH_JWT_HS256_SECRET" -> "disabled",
          "VOYAGE_API_KEY" -> "disabled",
          "VOYAGE_MODEL" -> "voyage-4-lite"
        )
        val configEntries = defaults ++ environment.removed("MONGODB_URI")
        Files.writeString(configFile, configEntries.toList.sortBy(_._1).map { case (key, value) => s"$key=$value" }.mkString("", "\n", "\n"))
        val builder = new ProcessBuilder((List(
          Path.of(System.getProperty("java.home"), "bin", "java").toString,
          "-Dfile.encoding=UTF-8",
          "-Djdk.httpclient.allowRestrictedHeaders=connection",
          "-cp", System.getProperty("java.class.path"), entryPoint
        ) ++ arguments)*).directory(directory.toFile).redirectOutput(stdout.toFile).redirectError(stderr.toFile)
        val childEnvironment = builder.environment()
        childEnvironment.clear()
        (Map(
          "MONGODB_URI" -> s"mongodb://test-user:$secret@127.0.0.1:1/?authSource=admin"
        ) ++ environment.filter { case (key, _) => key == "MONGODB_URI" }).foreach { case (key, value) =>
          val _ = childEnvironment.put(key, value)
        }
        builder.start()
      })(terminate).use { process =>
        IO.blocking {
          assert(process.waitFor(25, TimeUnit.SECONDS), "Child JVM exceeded the 25-second harness deadline")
          assert(Files.size(stdout) <= 65536, "Unexpectedly large child stdout")
          assert(Files.size(stderr) <= 65536, "Unexpectedly large child stderr")
          ChildResult(process.exitValue(), Files.readString(stdout), Files.readString(stderr))
        }
      }
    }

  private def assertSanitized(result: ChildResult, expectedCategory: String): Vector[Json] = {
    val captured = result.stdout + result.stderr
    assert(!captured.contains(secret), "Synthetic secret appeared in child output")
    assert(!captured.contains("mongodb://"), "Mongo URI appeared in child output")
    assertEquals(result.stderr, "")
    val lines = result.stdout.linesIterator.filter(_.nonEmpty).toVector
    assert(lines.nonEmpty, "Expected structured diagnostic output")
    val events = lines.map { line =>
      val event = parse(line).toOption.getOrElse(fail("Child stdout contains non-JSON output"))
      assertEquals(event.asObject.map(_.keys.toSet), Some(Set("timestamp", "severity", "category", "requestId",
        "marker", "component", "message", "masking", "details")))
      val category = event.hcursor.get[String]("category").toOption.getOrElse(fail("Missing category"))
      val component = event.hcursor.get[String]("component").toOption.getOrElse(fail("Missing component"))
      assertEquals(event.hcursor.get[String]("marker"), Right(s"HP.$component.$category"))
      assert(event.hcursor.get[String]("message").exists(_.nonEmpty))
      assert(event.hcursor.get[String]("masking").exists(Set("enabled", "disabled-local").contains))
      assert(event.hcursor.downField("details").focus.flatMap(_.asObject).exists(_.size <= 12))
      assert(line.getBytes(java.nio.charset.StandardCharsets.UTF_8).length <= 8192)
      val timestamp = event.hcursor.get[String]("timestamp").toOption.getOrElse(fail("Missing UTC timestamp"))
      assert(timestamp.endsWith("Z"))
      assert(Instant.parse(timestamp).isBefore(Instant.now().plusSeconds(1)))
      event
    }
    val matching = events.filter(_.hcursor.get[String]("category") == Right(expectedCategory))
    assert(matching.nonEmpty, clues(expectedCategory, events))
    matching.foreach { event =>
      assertEquals(event.hcursor.get[String]("severity"), Right("ERROR"))
      assertEquals(event.hcursor.downField("requestId").focus, Some(Json.Null))
    }
    events
  }

  test("P1-AC01/09 actual Main rejects secret-bearing invalid config with nonzero exit and sanitized output") {
    runChild(mainClass, Map("MONGODB_URI" -> s"invalid-uri-$secret")).map { result =>
      assert(result.exitCode != 0)
      val events = assertSanitized(result, "CONFIG_INVALID")
      assertEquals(events.size, 1)
      assertEquals(events.headOption.map(_.hcursor.downField("details").get[String]("configKey")),
        Some(Right("MONGODB_URI")))
      assert(events.forall(_.hcursor.get[String]("masking") == Right("enabled")))
    }
  }

  test("P1-AC02/09 actual Main exits nonzero on an occupied port without raw background failure output") {
    listeningSocket().use { socket =>
      runChild(mainClass, Map("HTTP_PORT" -> socket.getLocalPort.toString)).map { result =>
        assert(result.exitCode != 0)
        val _ = assertSanitized(result, "STARTUP_FAILED")
        assert(!result.stdout.contains("STARTED"))
        assert(!socket.isClosed)
      }
    }
  }

  test("P1-AC09 actual Main runtime reporter emits only a safe event for a synthetic-secret failure") {
    for {
      port <- listeningSocket().use(socket => IO.pure(socket.getLocalPort))
      result <- runChild("com.example.graphQL.cats.runtime.MainReporterProcess",
        Map("HTTP_PORT" -> port.toString, "LOG_LEVEL" -> "INFO"),
        List("--exercise-payload", "--test-http-host=127.0.0.1", s"--test-http-port=$port"))
    } yield {
      assertEquals(result.exitCode, 0)
      val events = assertSanitized(result, "RUNTIME_FAILED")
      val categories = events.flatMap(_.hcursor.get[String]("category").toOption)
      assertEquals(categories.count(_ == "RUNTIME_FAILED"), 1)
      assert(categories.indexOf("STARTED") >= 0)
      assert(categories.indexOf("STARTED") < categories.indexOf("RUNTIME_FAILED"))
      assert(!categories.contains("STARTUP_FAILED"))
      assert(!categories.contains("LOCAL_UNMASKED"))
      assert(!categories.contains("LOCAL_PAYLOADS_ENABLED"))
      assert(!categories.contains("REQUEST_PAYLOAD"))
      assert(events.forall(_.hcursor.get[String]("masking") == Right("enabled")))
      val started = events.find(_.hcursor.get[String]("category") == Right("STARTED")).getOrElse(fail("Missing startup"))
      assertEquals(started.hcursor.downField("details").get[String]("httpHost"), Right("[REDACTED]"))
      assertEquals(started.hcursor.downField("details").get[String]("httpPort"), Right(port.toString))
    }
  }

  List(
    ("masked payload capture", Map("LOG_REQUEST_PAYLOADS" -> "true"), "LOG_REQUEST_PAYLOADS"),
    ("IPv4 wildcard unmasking", Map("HTTP_HOST" -> "0.0.0.0", "LOG_MASK_SENSITIVE" -> "false"), "LOG_MASK_SENSITIVE"),
    ("IPv6 wildcard unmasking", Map("HTTP_HOST" -> "::", "LOG_MASK_SENSITIVE" -> "false"), "LOG_MASK_SENSITIVE"),
    ("IPv6 nonloopback unmasking", Map("HTTP_HOST" -> "2001:db8::1", "LOG_MASK_SENSITIVE" -> "false"), "LOG_MASK_SENSITIVE"),
    ("invalid masking flag", Map("LOG_MASK_SENSITIVE" -> "FALSE"), "LOG_MASK_SENSITIVE"),
    ("invalid payload flag", Map("LOG_REQUEST_PAYLOADS" -> "synthetic-secret"), "LOG_REQUEST_PAYLOADS")
  ).foreach { case (label, environment, key) =>
    test(s"LOG-03 actual Main rejects $label before emitting local diagnostics") {
      runChild(mainClass, environment).map { result =>
        assert(result.exitCode != 0)
        val events = assertSanitized(result, "CONFIG_INVALID")
        assertEquals(events.size, 1)
        events.foreach { event =>
          assertEquals(event.hcursor.get[String]("masking"), Right("enabled"))
          assertEquals(event.hcursor.downField("details").get[String]("configKey"), Right(key))
        }
      }
    }
  }

  List("127.42.10.8", "0:0:0:0:0:0:0:1").foreach { host =>
    test(s"LOG-03 actual Main accepts local loopback $host while its runtime reporter stays masked") {
      for {
        port <- listeningSocket(host).use(socket => IO.pure(socket.getLocalPort))
        result <- runChild("com.example.graphQL.cats.runtime.MainReporterProcess", Map(
          "HTTP_HOST" -> host, "HTTP_PORT" -> port.toString, "LOG_LEVEL" -> "INFO",
          "LOG_MASK_SENSITIVE" -> "false"
        ), List("--exercise-payload", s"--test-http-host=$host", s"--test-http-port=$port"))
      } yield {
        assertEquals(result.exitCode, 0)
        val events = assertSanitized(result, "RUNTIME_FAILED")
        assertEquals(events.count(_.hcursor.get[String]("category") == Right("LOCAL_UNMASKED")), 1)
        assert(!events.exists(_.hcursor.get[String]("category") == Right("LOCAL_PAYLOADS_ENABLED")))
        assert(!events.exists(_.hcursor.get[String]("category") == Right("REQUEST_PAYLOAD")))
        events.foreach { event =>
          val runtimeFailure = event.hcursor.get[String]("category") == Right("RUNTIME_FAILED")
          assertEquals(event.hcursor.get[String]("masking"), Right(if (runtimeFailure) "enabled" else "disabled-local"))
        }
        val started = events.find(_.hcursor.get[String]("category") == Right("STARTED")).getOrElse(fail("Missing startup"))
        assertEquals(started.hcursor.downField("details").get[String]("httpHost"), Right(host))
        val failure = events.find(_.hcursor.get[String]("category") == Right("RUNTIME_FAILED")).getOrElse(fail("Missing failure"))
        assertEquals(failure.hcursor.downField("details").get[String]("errorType"), Right("java.lang.RuntimeException"))
      }
    }
  }

  List(false, true).foreach { payloads =>
    test(s"LOG-03 local startup warnings bypass ERROR with payloads=$payloads and have no duplicates") {
      listeningSocket().use { socket =>
        runChild(mainClass, Map("HTTP_PORT" -> socket.getLocalPort.toString,
          "LOG_MASK_SENSITIVE" -> "false", "LOG_REQUEST_PAYLOADS" -> payloads.toString)).map { result =>
          assert(result.exitCode != 0)
          val events = assertSanitized(result, "STARTUP_FAILED")
          assertEquals(events.count(_.hcursor.get[String]("category") == Right("LOCAL_UNMASKED")), 1)
          assertEquals(events.count(_.hcursor.get[String]("category") == Right("LOCAL_PAYLOADS_ENABLED")), if (payloads) 1 else 0)
          events.filter(_.hcursor.get[String]("category").exists(_.startsWith("LOCAL_"))).foreach { warning =>
            assertEquals(warning.hcursor.get[String]("severity"), Right("WARN"))
            assertEquals(warning.hcursor.get[String]("masking"), Right("disabled-local"))
          }
          assert(!events.exists(_.hcursor.get[String]("category") == Right("REQUEST_PAYLOAD")))
        }
      }
    }
  }

  test("LOG-04 actual local payload capture excludes literal values, credentials, comments and exception messages") {
    for {
      port <- listeningSocket().use(socket => IO.pure(socket.getLocalPort))
      result <- runChild("com.example.graphQL.cats.runtime.MainReporterProcess", Map(
        "HTTP_PORT" -> port.toString, "LOG_LEVEL" -> "INFO",
        "LOG_MASK_SENSITIVE" -> "false", "LOG_REQUEST_PAYLOADS" -> "true"
      ), List("--exercise-payload", "--test-http-host=127.0.0.1", s"--test-http-port=$port"))
    } yield {
      assertEquals(result.exitCode, 0)
      val events = assertSanitized(result, "RUNTIME_FAILED")
      assertEquals(events.count(_.hcursor.get[String]("category") == Right("LOCAL_UNMASKED")), 1)
      assertEquals(events.count(_.hcursor.get[String]("category") == Right("LOCAL_PAYLOADS_ENABLED")), 1)
      val captured = events.filter(_.hcursor.get[String]("category") == Right("REQUEST_PAYLOAD"))
      assertEquals(captured.size, 1)
      captured.foreach { event =>
        assertEquals(event.hcursor.get[String]("masking"), Right("disabled-local"))
        val payload = event.hcursor.downField("details").get[String]("requestPayload").toOption.getOrElse(fail("Missing filtered payload"))
        assert(parse(payload).isRight, "Filtered payload must be valid JSON")
        assert(!payload.contains("synthetic-comment"))
      }
    }
  }
}
