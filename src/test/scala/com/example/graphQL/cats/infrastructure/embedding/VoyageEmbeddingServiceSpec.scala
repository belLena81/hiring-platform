package com.example.graphQL.cats.infrastructure.embedding

import cats.effect.IO
import com.example.graphQL.cats.repository.protocol.{EmbeddingInput, EmbeddingInputType}
import java.net.{Authenticator, CookieHandler, ProxySelector}
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration
import java.util.Optional
import java.util.concurrent.{CompletableFuture, CountDownLatch, Executor, TimeUnit}
import java.util.concurrent.atomic.AtomicReference
import javax.net.ssl.{SSLContext, SSLParameters}
import munit.CatsEffectSuite

final class VoyageEmbeddingServiceSpec extends CatsEffectSuite {
  test("cancelling an embedding request cancels the underlying HTTP future") {
    for {
      client = new HangingHttpClient
      service = new VoyageEmbeddingService("test-key", "https://example.test/embed", "voyage-4-lite", 2, 1000, client)
      fiber <- service.embed(EmbeddingInput("Scala", EmbeddingInputType.Document)).start
      future <- client.awaitFuture
      _ <- fiber.cancel
      _ = assert(future.isCancelled)
    } yield ()
  }

  test("an invalid configured endpoint is reported as provider unavailability") {
    val service = new VoyageEmbeddingService("test-key", "not a URI", "voyage-4-lite", 2, 1000)
    service.embed(EmbeddingInput("Scala", EmbeddingInputType.Document)).map { result =>
      assertEquals(result, Left(com.example.graphQL.cats.repository.protocol.EmbeddingError.ProviderUnavailable))
    }
  }

  private final class HangingHttpClient extends HttpClient {
    val future = new AtomicReference[CompletableFuture[HttpResponse[String]]]()
    private val started = new CountDownLatch(1)

    def awaitFuture: IO[CompletableFuture[HttpResponse[String]]] = IO.blocking {
      if (!started.await(3, TimeUnit.SECONDS))
        throw new AssertionError("HTTP request did not start")
      Option(future.get).getOrElse(throw new AssertionError("HTTP future was not published"))
    }

    override def cookieHandler: Optional[CookieHandler] = Optional.empty()
    override def connectTimeout: Optional[Duration] = Optional.empty()
    override def followRedirects: HttpClient.Redirect = HttpClient.Redirect.NEVER
    override def proxy: Optional[ProxySelector] = Optional.empty()
    override def sslContext: SSLContext = SSLContext.getDefault
    override def sslParameters: SSLParameters = new SSLParameters()
    override def authenticator: Optional[Authenticator] = Optional.empty()
    override def version: HttpClient.Version = HttpClient.Version.HTTP_1_1
    override def executor: Optional[Executor] = Optional.empty()

    override def send[T](request: HttpRequest, handler: HttpResponse.BodyHandler[T]): HttpResponse[T] =
      throw new UnsupportedOperationException("synchronous send is not used")

    override def sendAsync[T](request: HttpRequest, handler: HttpResponse.BodyHandler[T]): CompletableFuture[HttpResponse[T]] = {
      val future = new CompletableFuture[HttpResponse[String]]()
      this.future.set(future)
      started.countDown()
      future.asInstanceOf[CompletableFuture[HttpResponse[T]]]
    }

    override def sendAsync[T](
        request: HttpRequest,
        handler: HttpResponse.BodyHandler[T],
        pushHandler: HttpResponse.PushPromiseHandler[T]
    ): CompletableFuture[HttpResponse[T]] = sendAsync(request, handler)
  }
}
