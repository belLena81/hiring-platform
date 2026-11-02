package com.example.hiring.testing

import cats.effect.{IO, Resource}
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpsConfigurator, HttpsServer}
import io.circe.Json
import io.circe.parser.parse
import java.io.FileInputStream
import java.net.{InetAddress, InetSocketAddress}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.nio.file.attribute.PosixFilePermissions
import java.security.KeyStore
import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import javax.net.ssl.{KeyManagerFactory, SSLContext}

/** A throw-away TLS identity for the embedding stub. The application accepts only `https://` provider endpoints, so the
  * stub speaks TLS with a self-signed certificate for 127.0.0.1 and the child JVM trusts exactly that certificate
  * through `-Djavax.net.ssl.trustStore`. Nothing leaves the machine and no production code is relaxed.
  */
final case class StubTls(keystore: Path, truststore: Path, passphrase: String) {

  /** JVM options that make a process trust this stub certificate and nothing else. The trust store holds only the
    * public certificate, which the JDK reads without a password (it then skips the integrity check), so no passphrase
    * is put on the command line of the child process.
    */
  def clientJvmOptions: List[String] =
    List(s"-Djavax.net.ssl.trustStore=${truststore.toAbsolutePath}", "-Djavax.net.ssl.trustStoreType=JKS")

  def serverContext: SSLContext = {
    val store = KeyStore.getInstance("PKCS12")
    val input = new FileInputStream(keystore.toFile)
    try store.load(input, passphrase.toCharArray)
    finally input.close()
    val keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    keys.init(store, passphrase.toCharArray)
    val context = SSLContext.getInstance("TLS")
    context.init(keys.getKeyManagers, null, null)
    context
  }

  /** The stub certificate as a client trust anchor, for in-process clients. */
  def clientContext: SSLContext = {
    val store = KeyStore.getInstance("JKS")
    val input = new FileInputStream(truststore.toFile)
    // No password, exactly as a child JVM started with `clientJvmOptions` reads the trust store.
    try store.load(input, null)
    finally input.close()
    val trust = javax.net.ssl.TrustManagerFactory.getInstance(javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm)
    trust.init(store)
    val context = SSLContext.getInstance("TLS")
    context.init(null, trust.getTrustManagers, null)
    context
  }
}

object StubTls {
  private val keytoolTimeoutSeconds = 60L
  private val passphraseVariable = "STUB_TLS_PASSPHRASE"

  /** Generates a key pair and a separate trust store with the JDK `keytool` in an owner-only directory under `parent`.
    * The passphrase is random, lives only in this process and is handed to `keytool` through its environment.
    */
  def resource(parent: Path): Resource[IO, StubTls] =
    Resource.make(IO.blocking(generate(parent)))(tls => IO.blocking(deleteTree(tls.keystore.getParent)))

  private def generate(parent: Path): StubTls = {
    val _ = Files.createDirectories(parent)
    val directory = Files.createTempDirectory(
      parent,
      "stub-tls-",
      PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------"))
    )
    val passphrase = UUID.randomUUID().toString.replace("-", "")
    val keystore = directory.resolve("stub-keystore.p12")
    val truststore = directory.resolve("stub-truststore.jks")
    val certificate = directory.resolve("stub.pem")
    val keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString
    def run(arguments: String*): Unit = {
      val builder = new ProcessBuilder((keytool +: arguments)*).redirectErrorStream(true)
      builder.environment().put(passphraseVariable, passphrase)
      val process = builder.start()
      val output = new String(process.getInputStream.readAllBytes(), StandardCharsets.UTF_8)
      if (!process.waitFor(keytoolTimeoutSeconds, TimeUnit.SECONDS)) {
        process.destroyForcibly()
        throw new IllegalStateException("keytool did not finish within its time limit")
      }
      if (process.exitValue() != 0) throw new IllegalStateException(s"keytool failed: ${output.take(300)}")
    }
    run(
      "-genkeypair",
      "-alias",
      "stub",
      "-keyalg",
      "RSA",
      "-keysize",
      "2048",
      "-validity",
      "2",
      "-dname",
      "CN=localhost",
      "-ext",
      "SAN=ip:127.0.0.1,dns:localhost",
      "-storetype",
      "PKCS12",
      "-keystore",
      keystore.toString,
      "-storepass:env",
      passphraseVariable,
      "-keypass:env",
      passphraseVariable
    )
    run(
      "-exportcert",
      "-alias",
      "stub",
      "-rfc",
      "-keystore",
      keystore.toString,
      "-storepass:env",
      passphraseVariable,
      "-file",
      certificate.toString
    )
    run(
      "-importcert",
      "-noprompt",
      "-alias",
      "stub",
      "-file",
      certificate.toString,
      "-storetype",
      "JKS",
      "-keystore",
      truststore.toString,
      "-storepass:env",
      passphraseVariable
    )
    StubTls(keystore, truststore, passphrase)
  }

  private def deleteTree(root: Path): Unit = {
    val walk = Files.walk(root)
    try walk.sorted(java.util.Comparator.reverseOrder()).forEach(path => { val _ = Files.deleteIfExists(path); () })
    finally walk.close()
  }
}

/** A local stand-in for the Voyage embeddings endpoint (`POST /v1/embeddings`). It accepts the application's request
  * (`input`, `model`, `output_dimension`), answers with a deterministic unit vector, counts the calls it served and can
  * be stopped to simulate a provider outage. No network access, no paid provider and no API key beyond a non-empty
  * bearer token.
  *
  * Vectors come from `DeterministicEmbeddingStub.embed`: hashed words, so texts sharing words are close.
  */
final class DeterministicEmbeddingStub private (server: HttpsServer, val endpoint: String) {
  private val served = new AtomicInteger(0)
  private val stopped = new java.util.concurrent.atomic.AtomicBoolean(false)

  /** Number of well-formed embedding requests answered so far. */
  def callCount: Int = served.get()

  private[testing] def record(): Unit = { val _ = served.incrementAndGet(); () }

  /** Stops answering: the port is closed, so clients see a refused connection. Idempotent. */
  def stop(): Unit =
    if (stopped.compareAndSet(false, true)) server.stop(0)
}

object DeterministicEmbeddingStub {
  val Dimension: Int = 1024
  private val maxRequestBytes = 256 * 1024
  private val EndpointPath = "/v1/embeddings"

  private val Tokens = "[^\\p{L}\\p{N}]+".r

  /** FNV-1a 64-bit hash of the token's UTF-8 bytes: stable across JVMs and runs, unlike `String.hashCode` reasoning. */
  private def fnv1a(token: String): Long =
    token
      .getBytes(StandardCharsets.UTF_8)
      .foldLeft(0xcbf29ce484222325L)((hash, byte) => (hash ^ (byte & 0xff)) * 0x100000001b3L)

  /** Bag of hashed lowercase words, term-frequency weighted and L2 normalized. All components are non-negative, so the
    * cosine of two texts grows with the words they share. Empty text yields the unit vector on the first axis.
    */
  def embed(text: String, dimension: Int = Dimension): Vector[Float] = {
    val counts = new Array[Double](dimension)
    Tokens.split(text.toLowerCase(java.util.Locale.ROOT)).filter(_.nonEmpty).foreach { token =>
      counts(Math.floorMod(fnv1a(token), dimension.toLong).toInt) += 1.0
    }
    val norm = math.sqrt(counts.map(value => value * value).sum)
    if (norm == 0.0) Vector.tabulate(dimension)(index => if (index == 0) 1.0f else 0.0f)
    else counts.iterator.map(value => (value / norm).toFloat).toVector
  }

  def cosine(left: Vector[Float], right: Vector[Float]): Double =
    left.iterator.zip(right.iterator).map((a, b) => a.toDouble * b.toDouble).sum

  /** Starts the stub on a free loopback port; it is stopped when the resource is released. */
  def resource(tls: StubTls): Resource[IO, DeterministicEmbeddingStub] =
    Resource.make(IO.blocking(start(tls)))(stub => IO.blocking(stub.stop()))

  private def start(tls: StubTls): DeterministicEmbeddingStub = {
    val loopback = InetAddress.getByName("127.0.0.1")
    val server = HttpsServer.create(new InetSocketAddress(loopback, 0), 16)
    server.setHttpsConfigurator(new HttpsConfigurator(tls.serverContext))
    val stub = new DeterministicEmbeddingStub(server, s"https://127.0.0.1:${server.getAddress.getPort}$EndpointPath")
    server.createContext(EndpointPath, handler(stub))
    server.setExecutor(
      Executors.newFixedThreadPool(
        4,
        runnable => { val t = new Thread(runnable, "embedding-stub"); t.setDaemon(true); t }
      )
    )
    server.start()
    stub
  }

  private def handler(stub: DeterministicEmbeddingStub): HttpHandler = (exchange: HttpExchange) =>
    try {
      val bearer = Option(exchange.getRequestHeaders.getFirst("Authorization")).exists(_.startsWith("Bearer "))
      val body = new String(exchange.getRequestBody.readNBytes(maxRequestBytes + 1), StandardCharsets.UTF_8)
      if (exchange.getRequestMethod != "POST") reply(exchange, 405, Json.obj())
      else if (!bearer) reply(exchange, 401, Json.obj())
      else if (body.length > maxRequestBytes) reply(exchange, 413, Json.obj())
      else
        parse(body).toOption.flatMap(request => request.hcursor.get[String]("input").toOption.map(request -> _)) match {
          case None                  => reply(exchange, 400, Json.obj())
          case Some((request, text)) =>
            val model = request.hcursor.get[String]("model").getOrElse("stub-model")
            val dimension = request.hcursor.get[Int]("output_dimension").getOrElse(Dimension)
            if (dimension < 1 || dimension > 2048) reply(exchange, 400, Json.obj())
            else {
              stub.record()
              reply(
                exchange,
                200,
                Json.obj(
                  "object" -> Json.fromString("list"),
                  "data" -> Json.arr(
                    Json.obj(
                      "object" -> Json.fromString("embedding"),
                      "embedding" -> Json.fromValues(embed(text, dimension).map(Json.fromFloatOrNull)),
                      "index" -> Json.fromInt(0)
                    )
                  ),
                  "model" -> Json.fromString(model),
                  "usage" -> Json.obj("total_tokens" -> Json.fromInt(text.length))
                )
              )
            }
        }
    } finally exchange.close()

  private def reply(exchange: HttpExchange, status: Int, json: Json): Unit = {
    val bytes = json.noSpaces.getBytes(StandardCharsets.UTF_8)
    exchange.getResponseHeaders.add("Content-Type", "application/json")
    exchange.sendResponseHeaders(status, bytes.length.toLong)
    val out = exchange.getResponseBody
    try out.write(bytes)
    finally out.close()
  }
}
