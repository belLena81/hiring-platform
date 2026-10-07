package com.example.graphQL.cats.service.search

import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.service.SearchError
import com.example.graphQL.cats.service.ServiceFixtures.*
import com.example.graphQL.cats.shared.crypto.SourceHash
import munit.FunSuite

final class ValidatedCandidateMatchFiltersSpec extends FunSuite {
  private def validated(request: CandidateMatchFilters): ValidatedCandidateMatchFilters =
    ValidatedCandidateMatchFilters.from(request).fold(errors => fail(errors.toString), identity)

  private def errors(request: CandidateMatchFilters): List[SearchError] =
    ValidatedCandidateMatchFilters.from(request).fold(_.toList, _ => Nil)

  test("canonical criteria preserve AND matching and the typed availability state") {
    val filters = validated(
      CandidateMatchFilters(
        List(" MongoDB ", "SCALA", "Scala"),
        Some(" CYPRUS "),
        Some(" Nicosia "),
        Some("AVAILABLE_NOW")
      )
    )
    assertEquals(filters.requiredSkills, List("mongodb", "scala"))
    assertEquals(filters.countryCanonical, Some("cyprus"))
    assertEquals(filters.cityCanonical, Some("nicosia"))
    assertEquals(filters.availabilityStatus, Some(CandidateAvailabilityStatus.AVAILABLE_NOW))
    assertEquals(validated(CandidateMatchFilters.empty), ValidatedCandidateMatchFilters.empty)
    assertEquals(
      filters,
      validated(CandidateMatchFilters(List("scala", "mongodb"), Some("cyprus"), Some("nicosia"), Some("AVAILABLE_NOW")))
    )
  }

  test("independent validation failures accumulate in the existing outward error order") {
    assertEquals(
      errors(CandidateMatchFilters(List(" "), None, Some("Nicosia"), Some("available_now"))),
      List(
        SearchError.InvalidFilter("requiredSkills"),
        SearchError.InvalidFilter("residence"),
        SearchError.InvalidFilter("availabilityStatus")
      )
    )
  }

  test("original skill counts and trimmed sizes validate before canonical deduplication") {
    assertEquals(
      errors(CandidateMatchFilters(List.fill(101)("Scala"), None, None, None)),
      List(SearchError.InvalidFilter("requiredSkills"))
    )
    assertEquals(
      errors(CandidateMatchFilters(List("a" * 257), None, None, None)),
      List(SearchError.InvalidFilter("requiredSkills"))
    )
    assertEquals(
      validated(CandidateMatchFilters(List.fill(100)(" Scala "), None, None, None)).requiredSkills,
      List("scala")
    )
    assertEquals(validated(CandidateMatchFilters(List("İ" * 256), None, None, None)).requiredSkills.head.length, 512)
  }

  test("absent residence is valid while blank values and city without country reject") {
    List(
      CandidateMatchFilters(Nil, Some(" "), None, None),
      CandidateMatchFilters(Nil, Some("Cyprus"), Some(" "), None),
      CandidateMatchFilters(Nil, None, Some("Nicosia"), None),
      CandidateMatchFilters(Nil, Some("a" * 257), None, None)
    ).foreach(request => assertEquals(errors(request), List(SearchError.InvalidFilter("residence"))))
    assertEquals(validated(CandidateMatchFilters.empty).countryCanonical, None)
  }

  test("canonical location lengths and Unicode casing use the stored Mongo predicate rules") {
    assertEquals(validated(CandidateMatchFilters(Nil, Some(" İ "), None, None)).countryCanonical, Some("i\u0307"))
    assertEquals(
      validated(CandidateMatchFilters(Nil, Some("İ" * 128), None, None)).countryCanonical.map(_.length),
      Some(256)
    )
    assertEquals(
      errors(CandidateMatchFilters(Nil, Some("İ" * 129), None, None)),
      List(SearchError.InvalidFilter("residence"))
    )
    assertEquals(
      errors(CandidateMatchFilters(Nil, None, None, Some(" AVAILABLE_NOW "))),
      List(SearchError.InvalidFilter("availabilityStatus"))
    )
  }

  test("authoritative skill and private residence matching use canonical Unicode equality") {
    val profile = CandidateProfile(Set("İ"), Some("Engineer"), None, Some(CandidateResidence("İ", None)), None, true)
    val meta = EmbeddingMeta("model", SourceHash.sha256(SearchableText.candidate(profile)), now)
    val current =
      CandidateSearchEligibility(candidateId, UserRole.Candidate, AccountStatus.Active, Some(profile), Some(meta))
    def eligible(request: CandidateMatchFilters): Boolean =
      SearchEligibilityPolicy.candidate(current, meta, "model", validated(request))
    assert(eligible(CandidateMatchFilters(List(" İ "), Some(" İ "), None, None)))
    assert(!eligible(CandidateMatchFilters(List("i"), None, None, None)))
    assert(!eligible(CandidateMatchFilters(Nil, Some("i"), None, None)))
    assert(!eligible(CandidateMatchFilters(List("İ", "scala"), None, None, None)))
  }

  test("consent bypasses private criteria while public skill criteria always apply") {
    val profile = CandidateProfile(Set(" Scala "), Some("Engineer"), None)
    val meta = EmbeddingMeta("model", SourceHash.sha256(SearchableText.candidate(profile)), now)
    val current =
      CandidateSearchEligibility(candidateId, UserRole.Candidate, AccountStatus.Active, Some(profile), Some(meta))
    val filters =
      validated(CandidateMatchFilters(List("SCALA"), Some("Cyprus"), Some("Nicosia"), Some("AVAILABLE_NOW")))
    assert(SearchEligibilityPolicy.candidate(current, meta, "model", filters))
    assert(
      !SearchEligibilityPolicy.candidate(
        current.copy(profile = Some(profile.copy(recruiterSearchOptIn = true))),
        meta,
        "model",
        filters
      )
    )
    assert(
      !SearchEligibilityPolicy.candidate(
        current,
        meta,
        "model",
        validated(CandidateMatchFilters(List("Java"), None, None, None))
      )
    )
  }
}
