package com.example.graphQL.cats.service.search

import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.service.ServiceFixtures.*
import com.example.graphQL.cats.shared.crypto.SourceHash
import munit.FunSuite

final class EmbeddingPreparationSpec extends FunSuite {
  private val model = "voyage-4-lite"
  private val text = "Scala engineer"
  private val metadata = EmbeddingMeta(model, SourceHash.sha256(text), now)

  test("absent source and matching metadata need no work") {
    assertEquals(EmbeddingPreparation.prepare(None, None, model, 100), EmbeddingPreparation.NoWork)
    assertEquals(EmbeddingPreparation.prepare(Some(text), Some(metadata), model, 100), EmbeddingPreparation.NoWork)
  }

  test("prepared hash identifies exactly the canonical source") {
    assertEquals(
      EmbeddingPreparation.prepare(Some(text), None, model, text.length),
      EmbeddingPreparation.Prepared(text, metadata.sourceHash)
    )
    assertEquals(
      EmbeddingPreparation.prepare(Some(text), Some(metadata.copy(model = "previous")), model, 100),
      EmbeddingPreparation.Prepared(text, metadata.sourceHash)
    )
    assertEquals(
      EmbeddingPreparation.prepare(Some(text), Some(metadata.copy(sourceHash = "outdated")), model, 100),
      EmbeddingPreparation.Prepared(text, metadata.sourceHash)
    )
  }

  test("size rejection retains existing freshness-before-size precedence") {
    assertEquals(
      EmbeddingPreparation.prepare(Some(text), None, model, text.length - 1),
      EmbeddingPreparation.DocumentTooLarge
    )
    assertEquals(
      EmbeddingPreparation.prepare(Some(text), Some(metadata), model, text.length - 1),
      EmbeddingPreparation.NoWork
    )
  }

  test("private candidate attributes never alter prepared searchable source") {
    val profile = CandidateProfile(Set("Scala", "Kafka"), Some("Hiring platform engineer"), None)
    val changed = profile.copy(
      currentResidence = Some(CandidateResidence("Cyprus", Some("Nicosia"))),
      availabilityStatus = Some(CandidateAvailabilityStatus.AVAILABLE_NOW),
      recruiterSearchOptIn = !profile.recruiterSearchOptIn
    )
    assertEquals(SearchableText.candidate(changed), SearchableText.candidate(profile))
    assertEquals(
      EmbeddingPreparation
        .prepare(Some(SearchableText.candidate(changed)), None, model, SearchableText.DocumentMaxChars),
      EmbeddingPreparation.prepare(
        Some(SearchableText.candidate(profile)),
        None,
        model,
        SearchableText.DocumentMaxChars
      )
    )
  }
}
