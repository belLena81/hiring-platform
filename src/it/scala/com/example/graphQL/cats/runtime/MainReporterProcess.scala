package com.example.graphQL.cats.runtime

import cats.effect.unsafe.IORuntime
import com.example.graphQL.cats.Main
import java.io.{ByteArrayOutputStream, OutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration
import java.util.concurrent.CountDownLatch

object MainReporterProcess {
  def main(args: Array[String]): Unit = {
    val started = new CountDownLatch(1)
    val reported = new CountDownLatch(1)
    val original = System.out
    val forwarding = new OutputStream {
      private val line = new ByteArrayOutputStream()

      override def write(value: Int): Unit = this.synchronized {
        original.write(value)
        if (value == '\n') {
          original.flush()
          val text = line.toString(StandardCharsets.UTF_8)
          line.reset()
          if (text.contains("\"category\":\"STARTED\"")) started.countDown()
          if (text.contains("\"category\":\"RUNTIME_FAILED\"")) reported.countDown()
        } else if (line.size() < 65536) line.write(value)
      }

      override def flush(): Unit = this.synchronized(original.flush())
    }
    System.setOut(new PrintStream(forwarding, true, StandardCharsets.UTF_8))
    val injector = new Thread(() => {
      started.await()
      if (args.contains("--exercise-payload")) {
        val host = sys.env("HTTP_HOST")
        val authority = if (host.contains(':')) s"[$host]" else host
        val query = """{"query":"query LocalHealth($include: Boolean = true) { health @include(if: $include) { status } __type(name: \"synthetic-secret\") { name } } # synthetic-comment", "variables":{"include":true,"password":"synthetic-secret","api-key":"synthetic-secret"}}"""
        val request = HttpRequest.newBuilder(URI.create(s"http://$authority:${sys.env("HTTP_PORT")}/graphql"))
          .timeout(Duration.ofSeconds(8))
          .header("Content-Type", "application/json")
          .header("Connection", "close")
          .header("Authorization", "Bearer synthetic-secret")
          .header("Cookie", "session=synthetic-secret")
          .POST(HttpRequest.BodyPublishers.ofString(query)).build()
        val response = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build()
          .send(request, HttpResponse.BodyHandlers.ofString())
        if (response.statusCode() != 200) System.exit(2)
      }
      IORuntime.global.compute.reportFailure(new RuntimeException("mongodb://user:synthetic-secret@host/private"))
      reported.await()
      System.exit(0)
    }, "test-runtime-reporter")
    injector.setDaemon(true)
    injector.start()
    Main.main(args)
  }
}
