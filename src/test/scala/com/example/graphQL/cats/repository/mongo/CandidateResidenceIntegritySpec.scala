package com.example.graphQL.cats.repository.mongo

import munit.FunSuite
import org.bson.Document

final class CandidateResidenceIntegritySpec extends FunSuite {
  private def user(residence: Document): Document =
    new Document("profile", new Document("currentResidence", residence))

  private def residence: Document =
    new Document("country", " Cyprus ").append("countryCanonical", "cyprus")

  test("absent residence and absent city preserve legitimate optional values") {
    assertEquals(CandidateResidenceIntegrity.validate(new Document()), Right(()))
    assertEquals(CandidateResidenceIntegrity.validate(new Document("profile", new Document())), Right(()))
    assertEquals(CandidateResidenceIntegrity.validate(user(residence)), Right(()))
  }

  test("paired city uses the application's Unicode and whitespace canonicalization") {
    val value = residence.append("city", "  ΛΕΥΚΩΣΊΑ  ").append("cityCanonical", "λευκωσία")
    assertEquals(CandidateResidenceIntegrity.validate(user(value)), Right(()))
  }

  test("city and canonical city must both exist and match") {
    List(
      residence.append("city", "Nicosia"),
      residence.append("cityCanonical", "nicosia"),
      residence.append("city", "Nicosia").append("cityCanonical", "limassol"),
      residence.append("city", " ").append("cityCanonical", ""),
      residence.append("city", "Nicosia").append("cityCanonical", null)
    ).foreach(value => assert(CandidateResidenceIntegrity.validate(user(value)).isLeft))
  }

  test("malformed residence, wrong types, country mismatches and oversized values fail typed validation") {
    List(
      new Document("profile", new Document("currentResidence", "invalid")),
      new Document("profile", new Document("currentResidence", null)),
      new Document("profile", "invalid"),
      user(new Document("country", "Cyprus")),
      user(new Document("country", "Cyprus").append("countryCanonical", "greece")),
      user(new Document("country", Int.box(1)).append("countryCanonical", "cyprus")),
      user(new Document("country", "x" * 257).append("countryCanonical", "x" * 257))
    ).foreach(value => assert(CandidateResidenceIntegrity.validate(value).isLeft))
  }
}
