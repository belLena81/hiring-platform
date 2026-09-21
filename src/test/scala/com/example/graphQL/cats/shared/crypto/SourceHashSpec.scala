package com.example.graphQL.cats.shared.crypto

import munit.FunSuite

final class SourceHashSpec extends FunSuite {
  test("sha256 returns lowercase hexadecimal output") {
    assertEquals(
      SourceHash.sha256("hello"),
      "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    )
  }
}
