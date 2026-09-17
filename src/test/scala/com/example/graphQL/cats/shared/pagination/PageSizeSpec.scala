package com.example.graphQL.cats.shared.pagination

import com.example.graphQL.cats.domain.error.DomainValidationError.InvalidNumber
import munit.FunSuite

class PageSizeSpec extends FunSuite {
  test("page size validation bounds application repository list contracts") {
    assertEquals(PageSize.fromInt(1).map(_.value).toEither, Right(1))
    assertEquals(PageSize.fromInt(100).map(_.value).toEither, Right(100))
    assertEquals(PageSize.fromInt(0).leftMap(_.toList).toEither, Left(List(InvalidNumber("pageSize", 1, 100, 0))))
    assertEquals(PageSize.fromInt(101).leftMap(_.toList).toEither, Left(List(InvalidNumber("pageSize", 1, 100, 101))))
  }
}
