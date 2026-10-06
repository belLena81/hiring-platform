package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.service.ServiceFixtures.*
import com.example.graphQL.cats.service.port.RepositoryError
import com.example.graphQL.cats.service.search.*
import com.example.graphQL.cats.shared.crypto.SourceHash
import munit.FunSuite
import org.bson.Document

final class MongoSearchEligibilityCodecsSpec extends FunSuite {
  private val profile = CandidateProfile(Set("Scala"), Some("Engineer"), Some("private-resume"))
  private val meta = EmbeddingMeta("voyage-4-lite", SourceHash.sha256(SearchableText.candidate(profile)), now)

  test("eligibility projections exclude vectors, credentials, email and resume references") {
    val candidateFields = MongoSearchEligibilityCodecs.candidateFields
    assert(!candidateFields.contains(MongoFields.Embedding))
    assert(!candidateFields.contains(MongoFields.Email))
    assert(!candidateFields.contains(MongoFields.PasswordHash))
    assert(!candidateFields.contains(MongoFields.Profile))
    assert(!candidateFields.exists(_.endsWith("resumeRef")))
    assert(!MongoSearchEligibilityCodecs.jobFields.contains(MongoFields.Embedding))
    assert(candidateFields.contains(MongoFields.EmbeddingMeta))
  }

  test("authoritative candidate decoding retains source and eligibility fields without hydrating vectors or resume") {
    val document = MongoHiringCodecs.user(
      candidate.copy(profile = Some(UserProfile.Candidate(profile)), embedding = Some(EntityEmbedding(List(1f), meta)))
    )
    document.remove(MongoFields.Embedding)
    document.remove(MongoFields.Email)
    val decoded = MongoSearchEligibilityCodecs.candidate(document)
    assertEquals(decoded.map(_.metadata), Right(Some(meta)))
    assertEquals(decoded.map(_.profile.flatMap(_.resumeRef)), Right(None))
    assertEquals(decoded.map(_.profile.map(SearchableText.candidate)), Right(Some(SearchableText.candidate(profile))))
  }

  test("projected jobs decode without vectors and preserve authoritative source metadata") {
    val jobMeta = meta.copy(sourceHash = SourceHash.sha256(SearchableText.job(openJob)))
    val document = MongoHiringCodecs.job(openJob.copy(embedding = Some(EntityEmbedding(List(1f), jobMeta))))
    document.remove(MongoFields.Embedding)
    val decoded = MongoSearchEligibilityCodecs.job(document)
    assertEquals(decoded.map(_.job.embedding), Right(None))
    assertEquals(decoded.map(_.metadata), Right(Some(jobMeta)))
    assertEquals(
      decoded.map(value =>
        SearchEligibilityPolicy.job(value, jobMeta, meta.model, JobSearchFilter(None, Set.empty, None))
      ),
      Right(true)
    )
  }

  test("malformed present metadata and availability fail closed") {
    val document = MongoHiringCodecs.user(
      candidate.copy(profile = Some(UserProfile.Candidate(profile)), embedding = Some(EntityEmbedding(List(1f), meta)))
    )
    document.get(MongoFields.Profile, classOf[Document]).append(MongoFields.AvailabilityStatus, "UNKNOWN")
    assertEquals(MongoSearchEligibilityCodecs.candidate(document), Left(RepositoryError.InvalidStoredData))
    val job = MongoHiringCodecs.job(openJob)
    job.append(MongoFields.EmbeddingMeta, new Document(MongoFields.Model, "model"))
    assertEquals(MongoSearchEligibilityCodecs.job(job), Left(RepositoryError.InvalidStoredData))
  }
}
