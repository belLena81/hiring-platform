package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import com.example.graphQL.cats.service.{Diagnostics, RepositoryError}
import com.example.graphQL.cats.service.port.RepositoryF
import com.mongodb.MongoTimeoutException
import munit.CatsEffectSuite

final class EligibilityWithoutDiscoveryPolicySpec extends CatsEffectSuite {
  private def guarded[A](query: IO[A]) =
    MongoSemanticSearchRepository
      .unmetered(MongoRepositorySupport.discoveryGuard(Diagnostics.noop[IO], "test")(RepositoryF.lift(query)))
      .value

  test("CBR-04.5 an eligibility query without a discovery policy still runs and returns its result") {
    guarded(IO.pure(List(1, 2))).map(result => assertEquals(result, Right(List(1, 2))))
  }

  test("CBR-04.5 an unreachable deployment without a discovery policy is Unavailable, not a thrown error") {
    guarded(IO.raiseError[Int](new MongoTimeoutException("down"))).map(result =>
      assertEquals(result, Left(RepositoryError.Unavailable))
    )
  }

  test("CBR-04.5 any other failure is mapped to Unavailable by the guard") {
    guarded(IO.raiseError[Int](new IllegalStateException("bug"))).map(result =>
      assertEquals(result, Left(RepositoryError.Unavailable))
    )
  }
}
