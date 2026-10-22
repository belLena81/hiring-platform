package com.example.graphQL.cats.service.resilience

import cats.effect.Temporal
import cats.syntax.all.*
import fs2.Stream

import scala.concurrent.duration.*

/** Why a held lease can no longer be relied on while work runs under it. */
enum LeaseLost {

  /** The store reports the lease is no longer ours (expired, taken over, or the work row is gone). */
  case Expired

  /** The renewal itself could not be completed, so ownership is unknown. */
  case Unavailable
}

/** Runs work under a lease and renews it in the background: the one heartbeat loop of every leased worker. */
object LeaseHeartbeat {

  /** Renews every third of `lease` while `work` runs. The first renewal that reports a loss ends the guard and cancels
    * `work`, answering `Left` of that loss; otherwise the answer is `Right` of the work's result. `renewFirst` renews
    * once before starting the work, for callers whose claim may have spent most of the lease on loading and
    * authorization. A failure raised by `renew` or `work` propagates unchanged.
    */
  def guard[F[_], A](lease: FiniteDuration, renewFirst: Boolean = true)(
      renew: F[Either[LeaseLost, Unit]]
  )(work: F[A])(using F: Temporal[F]): F[Either[LeaseLost, A]] = {
    val lost: F[LeaseLost] =
      Stream
        .awakeEvery[F]((lease / 3).max(1.millis))
        .evalMap(_ => renew)
        .collectFirst { case Left(loss) => loss }
        .compile
        .lastOrError
    val started: F[Option[LeaseLost]] = if (renewFirst) renew.map(_.left.toOption) else F.pure(None)
    started.flatMap {
      case Some(loss) => F.pure(Left(loss))
      case None       =>
        F.race(work, lost).map {
          case Left(value) => Right(value)
          case Right(loss) => Left(loss)
        }
    }
  }
}
