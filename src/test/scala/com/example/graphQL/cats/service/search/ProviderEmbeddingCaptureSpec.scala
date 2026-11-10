package com.example.graphQL.cats.service.search

import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.infrastructure.embedding.{MeteredEmbedding, MeteredEmbeddingService}
import com.example.graphQL.cats.service.port.{EmbeddingError, EmbeddingInput, EmbeddingInputType, EmbeddingVector}
import com.typesafe.config.{Config, ConfigFactory}
import io.circe.parser
import munit.CatsEffectSuite

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.Instant
import scala.concurrent.duration.*

final class ProviderEmbeddingCaptureSpec extends CatsEffectSuite {
  private val SecretKey = "sk-super-secret-key-value"
  private val Flag = ProviderEmbeddingOptions.AuthorizationFlag
  private val Budget = "--absolute-p95-budget-ms"
  private val Allowance = "--latency-noise-allowance-ms"
  private val Pause = "--embedding-pause-ms"
  private val fullArgs = List(Flag, Budget, "3000", Allowance, "5")

  private def args(budget: String = "3000", allowance: String = "5", extra: List[String] = Nil): List[String] =
    List(Flag, Budget, budget, Allowance, allowance) ++ extra

  private def config(overrides: String = ""): Config =
    ConfigFactory
      .parseString(overrides)
      .withFallback(
        ConfigFactory.parseString(s"""
          vector-search.embedding.timeout-ms = 5000
          vector-search.voyage {
            api-key = "$SecretKey"
            endpoint = "https://ai.mongodb.com/v1/embeddings"
            model = "voyage-4-lite"
            dimension = 3
            data-collection-attestation = off
          }""")
      )
      .resolve()

  private val options = ProviderEmbeddingOptions(3.seconds, 5.0, 0.millis)
  private val provider = ProviderEmbeddingConfig.read(config()).toOption.get
  private val configured: IO[Either[ProviderEmbeddingRefusal, ProviderEmbeddingConfig]] = IO.pure(Right(provider))
  private val requests = List(
    ProviderEmbeddingRequest("job-1", "text one", EmbeddingInputType.Document),
    ProviderEmbeddingRequest("q-1", "query one", EmbeddingInputType.Query)
  )

  private def fakeEmbedder(
      calls: Ref[IO, Int],
      reply: (Int, EmbeddingInput) => IO[Either[EmbeddingError, MeteredEmbedding]]
  ): Resource[IO, MeteredEmbeddingService] =
    Resource.pure(new MeteredEmbeddingService {
      def embedMetered(input: EmbeddingInput) = calls.updateAndGet(_ + 1).flatMap(reply(_, input))
    })

  private def always(
      reply: Either[EmbeddingError, MeteredEmbedding]
  ): (Int, EmbeddingInput) => IO[Either[EmbeddingError, MeteredEmbedding]] =
    (_, _) => IO.pure(reply)

  private def vector(size: Int = 3, model: String = "voyage-4-lite") =
    EmbeddingVector(List.fill(size)(0.5f), model, size)

  private def optionsOf(parsed: List[String]) = ProviderEmbeddingOptions.extract(parsed).toOption.flatMap(_._1)

  test("HDQ-08 G4 a provider option without the authorization flag is refused and nothing is opened") {
    val refused = ProviderEmbeddingOptions.extract(List(Budget, "3000"))
    assertEquals(refused.left.toOption, Some(ProviderEmbeddingRefusal.NotAuthorized))
    assertEquals(ProviderEmbeddingOptions.extract(Nil).map(_._1), Right(None))
    for {
      calls <- Ref.of[IO, Int](0)
      opened <- Ref.of[IO, Int](0)
      read <- Ref.of[IO, Int](0)
      result <- ProviderEmbeddingCapture.embedAll(
        None,
        read.update(_ + 1).as(Right(provider)),
        _ =>
          Resource.eval(opened.update(_ + 1)) *> fakeEmbedder(calls, always(Right(MeteredEmbedding(vector(), None)))),
        requests
      )
      count <- calls.get
      openCount <- opened.get
      readCount <- read.get
    } yield {
      assertEquals(result.left.toOption.map(_.refusal), Some(ProviderEmbeddingRefusal.NotAuthorized))
      assertEquals((count, openCount, readCount), (0, 0, 0))
    }
  }

  test("HDQ-08 G9 the budget and the noise allowance are required and have no default") {
    assertEquals(
      ProviderEmbeddingOptions.extract(List(Flag, Allowance, "5")).left.toOption,
      Some(ProviderEmbeddingRefusal.MissingAbsoluteP95Budget)
    )
    assertEquals(
      ProviderEmbeddingOptions.extract(List(Flag, Budget, "3000")).left.toOption,
      Some(ProviderEmbeddingRefusal.MissingNoiseAllowance)
    )
    val parsed = ProviderEmbeddingOptions.extract("--other" :: fullArgs)
    assertEquals(parsed.map(_._2), Right(List("--other")))
    assertEquals(parsed.toOption.flatMap(_._1).map(_.absoluteP95Budget), Some(3.seconds))
  }

  test("HDQ-08 G9 the budget is whole milliseconds in the owner's inclusive range 2000 to 5000") {
    assertEquals(optionsOf(args(budget = "2000")).map(_.absoluteP95Budget), Some(2.seconds))
    assertEquals(optionsOf(args(budget = "5000")).map(_.absoluteP95Budget), Some(5.seconds))
    List("1999", "5001", "0", "-1", "-3000", "9223372036854775807").foreach { outOfRange =>
      assertEquals(
        ProviderEmbeddingOptions.extract(args(budget = outOfRange)).left.toOption,
        Some(ProviderEmbeddingRefusal.BudgetOutOfRange),
        outOfRange
      )
    }
    // Not a whole number of milliseconds, or too large for any integer: refused without an exception.
    List("0.5", "3000.5", "3e3", "NaN", "Infinity", "abc", "", "99999999999999999999999", "1e400").foreach { invalid =>
      assertEquals(
        ProviderEmbeddingOptions.extract(args(budget = invalid)).left.toOption,
        Some(ProviderEmbeddingRefusal.InvalidOption),
        invalid
      )
    }
  }

  test("HDQ-08 G9 the noise allowance and the pause are whole milliseconds within their bounds") {
    assertEquals(optionsOf(args(allowance = "0")).map(_.latencyNoiseAllowanceMillis), Some(0.0))
    assertEquals(optionsOf(args(allowance = "60000")).map(_.latencyNoiseAllowanceMillis), Some(60000.0))
    List("-1", "60001", "5.5", "0.4", "abc", "99999999999999999999999").foreach { bad =>
      assertEquals(
        ProviderEmbeddingOptions.extract(args(allowance = bad)).left.toOption,
        Some(ProviderEmbeddingRefusal.InvalidOption),
        bad
      )
    }
    assertEquals(optionsOf(args(extra = List(Pause, "600000"))).map(_.requestPause), Some(600.seconds))
    assertEquals(optionsOf(args()).map(_.requestPause), Some(Duration.Zero))
    List("-1", "600001", "1.5").foreach { bad =>
      assertEquals(
        ProviderEmbeddingOptions.extract(args(extra = List(Pause, bad))).left.toOption,
        Some(ProviderEmbeddingRefusal.InvalidOption),
        bad
      )
    }
  }

  test("HDQ-08 G9 a repeated provider flag is refused, never resolved first-wins") {
    List(
      args() ++ List(Budget, "4000"),
      args() ++ List(Allowance, "6"),
      args() ++ List(Pause, "1", Pause, "2"),
      args() :+ Flag
    ).foreach { repeated =>
      assertEquals(
        ProviderEmbeddingOptions.extract(repeated).left.toOption,
        Some(ProviderEmbeddingRefusal.RepeatedOption)
      )
    }
  }

  test("HDQ-08 G6 a missing or invalid data-collection attestation is a typed failure, never a default") {
    val without = ConfigFactory.parseString(
      s"""vector-search.embedding.timeout-ms = 5000
      vector-search.voyage { api-key = "$SecretKey", endpoint = "https://ai.mongodb.com/e", model = m, dimension = 3 }"""
    )
    assertEquals(ProviderEmbeddingConfig.read(without).left.toOption, Some(ProviderEmbeddingRefusal.MissingAttestation))
    // Only on|off are accepted: no true/false aliases.
    List("maybe", "true", "false", "yes").foreach { invalid =>
      assertEquals(
        ProviderEmbeddingConfig
          .read(config(s"vector-search.voyage.data-collection-attestation = $invalid"))
          .left
          .toOption,
        Some(ProviderEmbeddingRefusal.MissingAttestation),
        invalid
      )
    }
    assertEquals(
      ProviderEmbeddingConfig.read(config("vector-search.voyage.data-collection-attestation = on")).map(_.attestation),
      Right(DataCollectionAttestation.On)
    )
    for {
      opened <- Ref.of[IO, Int](0)
      result <- ProviderEmbeddingCapture.embedAll(
        Some(options),
        IO.pure(ProviderEmbeddingConfig.read(without)),
        _ => Resource.eval(opened.update(_ + 1)) *> Resource.eval(IO.never),
        requests
      )
      openCount <- opened.get
    } yield {
      assertEquals(result.left.toOption.map(_.refusal), Some(ProviderEmbeddingRefusal.MissingAttestation))
      assertEquals(result.left.toOption.map(_.requestsMade), Some(0))
      assertEquals(openCount, 0)
    }
  }

  test("HDQ-08 G6 a missing key or a non-https endpoint is refused") {
    val noKey = ConfigFactory.parseString(
      """vector-search.embedding.timeout-ms = 5000
      vector-search.voyage { endpoint = "https://ai.mongodb.com/e", model = m, dimension = 3, data-collection-attestation = on }"""
    )
    assertEquals(ProviderEmbeddingConfig.read(noKey).left.toOption, Some(ProviderEmbeddingRefusal.MissingApiKey))
    assertEquals(
      ProviderEmbeddingConfig
        .read(config("""vector-search.voyage.endpoint = "http://ai.mongodb.com/e""""))
        .left
        .toOption,
      Some(ProviderEmbeddingRefusal.InvalidEndpoint)
    )
  }

  test("HDQ-08 G6 the key is only sent to ai.mongodb.com or api.voyageai.com over https") {
    List("https://ai.mongodb.com/v1/embeddings", "https://api.voyageai.com/v1/embeddings").foreach { allowed =>
      assert(ProviderEmbeddingConfig.read(config(s"""vector-search.voyage.endpoint = "$allowed"""")).isRight, allowed)
    }
    List(
      "https://ai.example.test/v1/embeddings",
      "https://evil.example.test/v1/embeddings",
      "https://ai.mongodb.com.evil.test/v1/embeddings",
      "https://user:pw@ai.mongodb.com/v1/embeddings",
      "http://api.voyageai.com/v1/embeddings",
      "ftp://api.voyageai.com/v1/embeddings"
    ).foreach { refused =>
      assertEquals(
        ProviderEmbeddingConfig.read(config(s"""vector-search.voyage.endpoint = "$refused"""")).left.toOption,
        Some(ProviderEmbeddingRefusal.InvalidEndpoint),
        refused
      )
    }
  }

  test("HDQ-08 G6 the allow-list compares a lower-cased ASCII host; an explicit port is accepted") {
    List(
      "https://AI.MongoDB.com/v1/embeddings",
      "HTTPS://ai.mongodb.com/v1/embeddings",
      "https://ai.mongodb.com:443/v1/embeddings",
      "https://api.voyageai.com:8443/v1/embeddings"
    ).foreach { accepted =>
      assert(ProviderEmbeddingConfig.read(config(s"""vector-search.voyage.endpoint = "$accepted"""")).isRight, accepted)
    }
    List(
      "https://ai.mongodb.com./v1/embeddings",
      "https://ai%2Emongodb.com/v1/embeddings",
      "https://a%69.mongodb.com/v1/embeddings",
      "https://evil.test#@ai.mongodb.com",
      "https://evil.test/#@ai.mongodb.com/",
      "https://aİ.mongodb.com/v1/embeddings",
      "https://ai.mongodb.comK/v1/embeddings"
    ).foreach { refused =>
      assertEquals(
        ProviderEmbeddingConfig.read(config(s"""vector-search.voyage.endpoint = "$refused"""")).left.toOption,
        Some(ProviderEmbeddingRefusal.InvalidEndpoint),
        refused
      )
    }
  }

  test("HDQ-08 G9 a leading plus or a non-ASCII digit is refused in every whole-millisecond option") {
    List("+3000", "+0", "٣000").foreach { bad =>
      assertEquals(
        ProviderEmbeddingOptions.extract(args(budget = bad)).left.toOption,
        Some(ProviderEmbeddingRefusal.InvalidOption),
        bad
      )
      assertEquals(
        ProviderEmbeddingOptions.extract(args(allowance = bad)).left.toOption,
        Some(ProviderEmbeddingRefusal.InvalidOption),
        bad
      )
      assertEquals(
        ProviderEmbeddingOptions.extract(args(extra = List(Pause, bad))).left.toOption,
        Some(ProviderEmbeddingRefusal.InvalidOption),
        bad
      )
    }
  }

  private def completedWrite(directory: java.nio.file.Path, writes: Ref[IO, Int]): IO[java.nio.file.Path] =
    writes.update(_ + 1) *>
      ProviderEmbeddingEvidence.writeCompleted(
        directory,
        8,
        Instant.ofEpochMilli(99L),
        provider,
        options,
        List(ProviderEmbeddingUsage("job-1", EmbeddingInputType.Document, Some(7L))),
        7,
        7
      )

  test("HDQ-08 G8 the completed record is written once, before the downstream stage runs") {
    val directory = Files.createTempDirectory("provider-evidence-completed")
    for {
      writes <- Ref.of[IO, Int](0)
      seen <- Ref.of[IO, Boolean](false)
      result <- ProviderEmbeddingEvidence.recordThen(
        completedWrite(directory, writes),
        IO(Files.exists(directory.resolve("provider-embedding-evidence-8-99.json")))
          .flatMap(seen.set)
          .as("done")
      )
      count <- writes.get
      existedBefore <- seen.get
    } yield {
      val path = directory.resolve("provider-embedding-evidence-8-99.json")
      val json = parser.parse(Files.readString(path, StandardCharsets.UTF_8)).toOption.get
      assertEquals(result, Right("done"))
      assertEquals(count, 1)
      assert(existedBefore)
      assertEquals(json.hcursor.get[String]("outcome"), Right("Completed"))
      assertEquals(json.hcursor.get[Long]("totalTokens"), Right(7L))
      assertEquals(directory.toFile.list().toList, List("provider-embedding-evidence-8-99.json"))
    }
  }

  test("HDQ-08 G8 a downstream failure or the truncation-guard stop after embedding keeps the completed record") {
    List[Throwable](
      new IllegalStateException("Atlas connect timeout"),
      new RuntimeException("Possibly truncated capture")
    ).traverse { stop =>
      val directory = Files.createTempDirectory("provider-evidence-stop")
      for {
        writes <- Ref.of[IO, Int](0)
        outcome <- ProviderEmbeddingEvidence
          .recordThen(completedWrite(directory, writes), IO.raiseError[Unit](stop))
          .attempt
        count <- writes.get
      } yield {
        assertEquals(outcome.left.toOption, Some(stop))
        assertEquals(count, 1)
        assertEquals(directory.toFile.list().toList, List("provider-embedding-evidence-8-99.json"))
      }
    }.void
  }

  test("HDQ-08 G8 if the completed record cannot be written, nothing downstream runs and the message is fixed") {
    val blocker = Files.createTempFile("provider-evidence-not-a-directory", ".tmp")
    for {
      ran <- Ref.of[IO, Boolean](false)
      writes <- Ref.of[IO, Int](0)
      result <- ProviderEmbeddingEvidence.recordThen(completedWrite(blocker, writes), ran.set(true))
      started <- ran.get
    } yield {
      assertEquals(result, Left(ProviderEmbeddingRefusal.EvidenceNotWritten))
      assert(!started)
      assert(!ProviderEmbeddingRefusal.EvidenceNotWritten.message.contains(blocker.toString))
      Files.deleteIfExists(blocker)
    }
  }

  test("HDQ-08 G8 token counts are aggregated per input and in total, and a missing count is reported") {
    for {
      calls <- Ref.of[IO, Int](0)
      result <- ProviderEmbeddingCapture.embedAll(
        Some(options),
        configured,
        _ =>
          fakeEmbedder(
            calls,
            (_, input) =>
              IO.pure(
                Right(MeteredEmbedding(vector(), Option.when(input.inputType == EmbeddingInputType.Document)(7L)))
              )
          ),
        requests :+ ProviderEmbeddingRequest("job-2", "text two", EmbeddingInputType.Document)
      )
      count <- calls.get
    } yield {
      val (_, captured) = result.toOption.get
      assertEquals(count, 3)
      assertEquals(ProviderEmbeddingEvidence.totals(captured.usage), ProviderEmbeddingEvidence.Totals(3, 14L, 1))
      assertEquals(
        captured.vectors.keySet,
        Set(
          (EmbeddingInputType.Document, "job-1"),
          (EmbeddingInputType.Query, "q-1"),
          (EmbeddingInputType.Document, "job-2")
        )
      )
      val record = ProviderEmbeddingEvidence.render(provider, options, captured.usage, 7, 7)
      assertEquals(record.hcursor.get[Long]("totalTokens"), Right(14L))
      assertEquals(record.hcursor.get[Int]("providerRequests"), Right(3))
      assertEquals(record.hcursor.get[Int]("inputsWithoutUsage"), Right(1))
      assertEquals(record.hcursor.get[String]("outcome"), Right("Completed"))
      assertEquals(record.hcursor.get[String]("dataCollectionAttestation"), Right("off"))
      assertEquals(record.hcursor.get[String]("embeddingProvenance"), Right("ProviderGenerated"))
      assertEquals(record.hcursor.get[Long]("absoluteP95BudgetMillis"), Right(3000L))
    }
  }

  test("HDQ-08 G8 the record lists the token count of every document and every query") {
    for {
      calls <- Ref.of[IO, Int](0)
      result <- ProviderEmbeddingCapture.embedAll(
        Some(options),
        configured,
        _ =>
          fakeEmbedder(
            calls,
            (call, _) => IO.pure(Right(MeteredEmbedding(vector(), Option.when(call != 2)(call.toLong * 10))))
          ),
        requests :+ ProviderEmbeddingRequest("job-2", "text two", EmbeddingInputType.Document)
      )
    } yield {
      val record = ProviderEmbeddingEvidence.render(provider, options, result.toOption.get._2.usage, 7, 7)
      val documents = record.hcursor.downField("documentTokens").as[List[io.circe.Json]].toOption.get
      val queries = record.hcursor.downField("queryTokens").as[List[io.circe.Json]].toOption.get
      assertEquals(documents.map(_.hcursor.get[String]("id")), List(Right("job-1"), Right("job-2")))
      assertEquals(documents.map(_.hcursor.get[Option[Long]]("totalTokens")), List(Right(Some(10L)), Right(Some(30L))))
      assertEquals(queries.map(_.hcursor.get[String]("id")), List(Right("q-1")))
      assertEquals(queries.map(_.hcursor.get[Option[Long]]("totalTokens")), List(Right(None)))
    }
  }

  test("HDQ-08 G4 a dimension or model mismatch fails closed, stops the capture and keeps the billed tokens") {
    List(
      vector(size = 2),
      vector(model = "another-model"),
      EmbeddingVector(List(0.5f, 0.5f, 0.5f), "voyage-4-lite", 4)
    ).traverse { bad =>
      for {
        calls <- Ref.of[IO, Int](0)
        result <- ProviderEmbeddingCapture.embedAll(
          Some(options),
          configured,
          _ => fakeEmbedder(calls, always(Right(MeteredEmbedding(bad, Some(1L))))),
          requests
        )
        count <- calls.get
      } yield {
        val failure = result.left.toOption.get
        assertEquals(failure.refusal, ProviderEmbeddingRefusal.DimensionMismatch)
        assertEquals(failure.requestsMade, 1)
        assertEquals(failure.usage.flatMap(_.totalTokens).sum, 1L)
        assertEquals(count, 1)
      }
    }.void
  }

  test("HDQ-08 G4 a provider failure stops the capture with a fixed message") {
    for {
      calls <- Ref.of[IO, Int](0)
      result <- ProviderEmbeddingCapture.embedAll(
        Some(options),
        configured,
        _ => fakeEmbedder(calls, always(Left(EmbeddingError.ProviderUnavailable))),
        requests
      )
      count <- calls.get
    } yield {
      assertEquals(result.left.toOption.map(_.refusal), Some(ProviderEmbeddingRefusal.EmbeddingFailed))
      assertEquals(result.left.toOption.map(_.requestsMade), Some(1))
      assertEquals(count, 1)
    }
  }

  test("HDQ-08 G8 a provider failure on the third request keeps a counts-only record with the tokens already used") {
    val three = requests :+ ProviderEmbeddingRequest("job-2", "text two", EmbeddingInputType.Document)
    val directory = Files.createTempDirectory("provider-evidence")
    for {
      calls <- Ref.of[IO, Int](0)
      result <- ProviderEmbeddingCapture.embedAll(
        Some(options),
        configured,
        _ =>
          fakeEmbedder(
            calls,
            (call, _) =>
              IO.pure(
                if (call < 3) Right(MeteredEmbedding(vector(), Some(7L))) else Left(EmbeddingError.ProviderUnavailable)
              )
          ),
        three :+ ProviderEmbeddingRequest("job-3", "text three", EmbeddingInputType.Document)
      )
      failure = result.left.toOption.get
      path <- ProviderEmbeddingEvidence.writeFailure(
        directory,
        8,
        Instant.ofEpochMilli(1234L),
        failure.config.get,
        options,
        failure,
        7,
        7
      )
      count <- calls.get
    } yield {
      val text = Files.readString(path, StandardCharsets.UTF_8)
      val json = parser.parse(text).toOption.get
      assertEquals(path.getFileName.toString, "provider-embedding-evidence-failed-8-1234.json")
      assertEquals(count, 3)
      assertEquals(failure.refusal, ProviderEmbeddingRefusal.EmbeddingFailed)
      assertEquals(json.hcursor.get[String]("outcome"), Right("EmbeddingFailed"))
      assertEquals(json.hcursor.get[Int]("providerRequests"), Right(3))
      assertEquals(json.hcursor.get[Long]("totalTokens"), Right(14L))
      assertEquals(json.hcursor.get[Int]("inputsWithoutUsage"), Right(0))
      assertEquals(json.hcursor.get[String]("endpointHost"), Right("ai.mongodb.com"))
      assertEquals(json.hcursor.get[String]("model"), Right("voyage-4-lite"))
      assertEquals(json.hcursor.get[String]("dataCollectionAttestation"), Right("off"))
      assertEquals(json.hcursor.get[String]("embeddingProvenance"), Right("ProviderGenerated"))
      assertEquals(json.hcursor.get[Long]("absoluteP95BudgetMillis"), Right(3000L))
      assertEquals(
        json.hcursor.keys.map(_.toSet).getOrElse(Set.empty).filter(_.toLowerCase.contains("vector")),
        Set.empty[String]
      )
      List(SecretKey, "text one", "query one", "text two", "text three", "0.5").foreach(secret =>
        assert(!text.contains(secret), secret)
      )
      Files.deleteIfExists(path)
      Files.deleteIfExists(directory)
    }
  }

  test("HDQ-08 G6 a raised exception becomes EmbeddingFailed and its text reaches neither the failure nor the record") {
    val thrown = new RuntimeException(s"boom $SecretKey text one")
    for {
      calls <- Ref.of[IO, Int](0)
      result <- ProviderEmbeddingCapture.embedAll(
        Some(options),
        configured,
        _ => fakeEmbedder(calls, (_, _) => IO.raiseError(thrown)),
        requests
      )
    } yield {
      val failure = result.left.toOption.get
      assertEquals(failure.refusal, ProviderEmbeddingRefusal.EmbeddingFailed)
      assertEquals(failure.requestsMade, 1)
      val rendered = List(
        failure.toString,
        failure.refusal.message,
        ProviderEmbeddingEvidence.renderFailure(provider, options, failure, 7, 7).spaces2
      )
      rendered.foreach { text =>
        assert(!text.contains("boom"), text)
        assert(!text.contains(SecretKey), text)
        assert(!text.contains("text one"), text)
      }
    }
  }

  test("HDQ-08 G2 a pause above zero runs between requests and not after the last one") {
    for {
      calls <- Ref.of[IO, Int](0)
      pauses <- Ref.of[IO, List[FiniteDuration]](Nil)
      result <- ProviderEmbeddingCapture.embedAll(
        Some(options.copy(requestPause = 250.millis)),
        configured,
        _ => fakeEmbedder(calls, always(Right(MeteredEmbedding(vector(), Some(1L))))),
        requests :+ ProviderEmbeddingRequest("job-2", "text two", EmbeddingInputType.Document),
        pause = duration => pauses.update(_ :+ duration)
      )
      recorded <- pauses.get
      none <- Ref.of[IO, List[FiniteDuration]](Nil).flatMap { zero =>
        ProviderEmbeddingCapture
          .embedAll(
            Some(options),
            configured,
            _ => fakeEmbedder(calls, always(Right(MeteredEmbedding(vector(), None)))),
            requests,
            pause = d => zero.update(_ :+ d)
          )
          .flatMap(_ => zero.get)
      }
    } yield {
      assert(result.isRight)
      assertEquals(recorded, List(250.millis, 250.millis))
      assertEquals(none, Nil)
    }
  }

  test("HDQ-08 G6 the key appears in no rendered configuration, record or refusal message") {
    for {
      calls <- Ref.of[IO, Int](0)
      result <- ProviderEmbeddingCapture.embedAll(
        Some(options),
        configured,
        _ => fakeEmbedder(calls, always(Right(MeteredEmbedding(vector(), Some(2L))))),
        requests
      )
    } yield {
      val usage = result.toOption.get._2.usage
      val rendered = List(
        provider.toString,
        ProviderEmbeddingEvidence.render(provider, options, usage, 7, 7).spaces2,
        options.toString,
        usage.toString
      ) ++ ProviderEmbeddingRefusal.values.map(_.message)
      rendered.foreach { text =>
        assert(!text.contains(SecretKey))
        assert(!text.contains("text one"))
        assert(!text.contains("query one"))
      }
      assert(provider.toString.contains("[REDACTED]"))
    }
  }
}
