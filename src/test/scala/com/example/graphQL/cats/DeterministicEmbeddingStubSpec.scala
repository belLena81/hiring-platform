package com.example.graphQL.cats

import cats.effect.IO
import com.example.graphQL.cats.infrastructure.embedding.VoyageEmbeddingService
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.port.{EmbeddingError, EmbeddingInput, EmbeddingInputType}
import com.example.hiring.testing.{DeterministicEmbeddingStub, StubTls}
import com.example.hiring.testing.HiringSearchStoryData.*
import fs2.io.net.Network
import java.nio.file.{Files, Path}
import munit.CatsEffectSuite
import org.http4s.Uri
import org.http4s.ember.client.EmberClientBuilder
import scala.concurrent.duration.*

/** The deterministic embedding stub behind the search part of the hiring story: its vectors, the ranking oracle that
  * the Tier B expectations rest on, and the wire contract with the production Voyage client.
  */
final class DeterministicEmbeddingStubSpec extends CatsEffectSuite {
  private def vector(text: String) = DeterministicEmbeddingStub.embed(text)
  private def similarity(query: String, seed: JobSeed): Double =
    DeterministicEmbeddingStub.cosine(vector(query), vector(seed.searchableText))

  test("a text always embeds to the same unit vector of the configured dimension") {
    val first = vector("Functional JVM backend")
    assertEquals(first, vector("Functional JVM backend"))
    assertEquals(first.size, DeterministicEmbeddingStub.Dimension)
    assertEqualsDouble(DeterministicEmbeddingStub.cosine(first, first), 1.0, 1e-6)
    assertEquals(vector("functional jvm BACKEND"), first) // case and punctuation do not matter
  }

  test("empty text still embeds to a valid unit vector") {
    assertEqualsDouble(DeterministicEmbeddingStub.cosine(vector(""), vector("")), 1.0, 1e-6)
  }

  test("HEA-02.1 oracle: the query 'functional JVM backend' is closest to J1 among the open jobs") {
    val query = "functional JVM backend"
    val open = List(j2, j4).map(similarity(query, _))
    assert(open.forall(_ < similarity(query, j1)), s"J1 must beat J2 and J4, got J1=${similarity(query, j1)} $open")
  }

  test("HEA-02.1 oracle: the hidden J3 and J5 would outrank the open J2 and J4, so their absence proves visibility") {
    val query = "functional JVM backend"
    val hidden = List(j3, j5).map(similarity(query, _))
    val visibleOthers = List(j2, j4).map(similarity(query, _))
    assert(hidden.min > visibleOthers.max, s"hidden $hidden must exceed visible $visibleOthers")
  }

  test("HEA-06.1 oracle: Ana's profile is closest to J1 among all five jobs") {
    val profile = vector(ana.searchableText)
    val scores = List(j1, j2, j3, j4, j5).map(job =>
      job.key -> DeterministicEmbeddingStub.cosine(profile, vector(job.searchableText))
    )
    assertEquals(scores.maxBy(_._2)._1, "J1", scores.toString)
  }

  test("HEA-10.1 oracle: for J1, Ana ranks first, then the opted-out Scala candidate, then Bo and Cy") {
    val job = vector(j1.searchableText)
    val order = candidates.sortBy(seed => -DeterministicEmbeddingStub.cosine(job, vector(seed.searchableText)))
    assertEquals(order.take(2).map(_.key), List(ana.key, dee.key), order.toString)
  }

  test("the production Voyage client embeds through the stub over TLS and sees an outage after the stub stops") {
    // The temporary directory lives under the ignored .local/ tree and is removed afterwards.
    val parent = Path.of(sys.props("user.dir")).resolve(".local/data/embedding-stub-spec")
    val text = "Scala backend services"
    // A client-local TLS context that trusts only the stub certificate (read without a password, as a child JVM does),
    // so the JVM default context is never changed. The service class is the production one.
    val outcome = StubTls
      .resource(parent)
      .flatMap(tls => DeterministicEmbeddingStub.resource(tls).map(tls -> _))
      .use { (tls, stub) =>
        EmberClientBuilder
          .default[IO]
          .withTLSContext(Network[IO].tlsContext.fromSSLContext(tls.clientContext))
          .withTimeout(5.seconds)
          .build
          .use { client =>
            val provider = new VoyageEmbeddingService(
              client,
              "stub-key",
              Uri.unsafeFromString(stub.endpoint),
              "voyage-4-lite",
              DeterministicEmbeddingStub.Dimension,
              5.seconds,
              diagnostics = Diagnostics.noop[IO]
            )
            for {
              answered <- provider.embed(EmbeddingInput(text, EmbeddingInputType.Query))
              callsAfterAnswer = stub.callCount
              _ <- IO.blocking(stub.stop())
              refused <- provider.embed(EmbeddingInput(text, EmbeddingInputType.Query))
            } yield (answered, callsAfterAnswer, refused)
          }
      }
      .guarantee(IO.blocking { val _ = Files.deleteIfExists(parent); () }.handleError(_ => ()))
    outcome.map { (answered, calls, refused) =>
      assertEquals(answered.map(_.values), Right(vector(text).toList))
      assertEquals(answered.map(_.model), Right("voyage-4-lite"))
      assertEquals(calls, 1)
      assertEquals(refused.left.toOption, Some(EmbeddingError.ProviderUnavailable))
    }
  }
}
