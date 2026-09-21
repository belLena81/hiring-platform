package com.example.graphQL.cats.api.http

import munit.FunSuite
import org.http4s.MediaType
import org.http4s.headers.Accept

final class MediaTypeNegotiationSpec extends FunSuite {
  test("negotiation always returns a supported representation or no match") {
    val headers = List(
      "*/*",
      "application/*;q=0.5, application/json;q=1",
      "application/json;q=0, application/graphql-response+json;q=0",
      "text/html, application/json;q=0.8",
      "application/graphql-response+json;q=0, application/json;q=0.4",
      "application/graphql-response+json;q=0.2, application/json;q=0.9"
    )

    headers.foreach { raw =>
      val accept = Accept.parse(raw).fold(error => fail(s"invalid test Accept header $raw: $error"), identity)
      assert(MediaTypeNegotiation.selectResponseMediaType(Some(accept)).forall(MediaTypeNegotiation.supported.contains), raw)
    }
  }

  test("specific ranges outrank wildcards and zero quality excludes a representation") {
    val specific = Accept.parse("application/*;q=1, application/graphql-response+json;q=0").toOption.get
    val allZero = Accept.parse("application/json;q=0, application/graphql-response+json;q=0").toOption.get

    assertEquals(MediaTypeNegotiation.selectResponseMediaType(Some(specific)), Some(MediaType.application.json))
    assertEquals(MediaTypeNegotiation.selectResponseMediaType(Some(allZero)), None)
  }
}
