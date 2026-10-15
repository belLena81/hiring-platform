package com.example.graphQL.cats.service.auth

import cats.effect.{IO, Ref}
import cats.effect.testkit.TestControl
import com.example.graphQL.cats.service.{Diagnostics, RepositoryError}
import com.example.graphQL.cats.service.port.{RecruiterJobClosureProgress, RecruiterJobClosureRepository, RepositoryF}
import munit.CatsEffectSuite
import scala.concurrent.duration.*

/** ARR-07.1: the closure worker reports progress after each successful step and stays silent while steps fail. */
final class RecruiterJobClosureWorkerAliveSpec extends CatsEffectSuite {
  private def repository(step: RepositoryF[IO, RecruiterJobClosureProgress]) =
    new RecruiterJobClosureRepository[IO] { def closeNextPage = step }

  private def beats(step: RepositoryF[IO, RecruiterJobClosureProgress]): IO[Int] =
    Ref.of[IO, Int](0).flatMap { count =>
      new RecruiterJobClosureWorker[IO](
        repository(step),
        Diagnostics.noop[IO],
        RecruiterJobClosureSchedule.Default,
        count.update(_ + 1)
      ).resource.surround(IO.sleep(10.seconds)) *> count.get
    }

  test("ARR-07.1 an idle closure worker reports on every poll") {
    TestControl
      .executeEmbed(beats(RepositoryF.pure[IO, RecruiterJobClosureProgress](RecruiterJobClosureProgress.Idle)))
      .map(count => assert(count >= 9, clue(count)))
  }

  test("ARR-07.1 a closure worker whose steps fail reports no progress") {
    TestControl
      .executeEmbed(
        beats(RepositoryF.raise[IO, RecruiterJobClosureProgress](RepositoryError.Unavailable))
      )
      .map(count => assertEquals(count, 0))
  }
}
