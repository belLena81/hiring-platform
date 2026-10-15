package com.example.graphQL.cats.service.resilience

import cats.{Applicative, Functor}
import cats.effect.{Clock, MonadCancel, Ref}
import cats.effect.kernel.{Outcome, Temporal}
import cats.syntax.all.*
import cats.effect.syntax.all.*
import com.example.graphQL.cats.domain.policy.CircuitBreakerPolicy
import com.example.graphQL.cats.domain.policy.CircuitBreakerPolicy.{Admission, Completion, Settings, State, Transition}

import java.time.Instant

/** Observable breaker state, exposed for readiness and tests. */
type CircuitState = CircuitBreakerPolicy.State

/** A call refused because the dependency is considered unavailable; the effect never ran. */
final case class CircuitRejected(retryAt: Option[Instant])

/** Process-local breaker over any `F` with a clock. `protect` runs an effect only when admitted. */
trait CircuitBreaker[F[_]] {

  /** Runs `fa` when admitted and classifies its outcome; `Left` means rejected without evaluating `fa`. Classification
    * is an effect so a caller can consult what the protected call observed, for example a cancelled call that had
    * already seen the dependency fail.
    */
  def protect[A](fa: F[A])(classify: Outcome[F, Throwable, A] => F[Completion]): F[Either[CircuitRejected, A]]

  def current: F[CircuitState]

  /** The state as of now when a call would be admitted, and `None` when it would be rejected. Takes no probe slot; use
    * it to avoid claiming work that cannot run.
    */
  def admittedState: F[Option[CircuitState]]

  def admits(using Functor[F]): F[Boolean] = admittedState.map(_.nonEmpty)
}

object CircuitBreaker {

  /** Always admits; used when a breaker is disabled by configuration. */
  def disabled[F[_]](using F: MonadCancel[F, Throwable]): CircuitBreaker[F] = new CircuitBreaker[F] {
    def protect[A](fa: F[A])(classify: Outcome[F, Throwable, A] => F[Completion]): F[Either[CircuitRejected, A]] =
      fa.map(Right(_))
    def current: F[CircuitState] = F.pure(CircuitBreakerPolicy.initial)
    def admittedState: F[Option[CircuitState]] = F.pure(Some(CircuitBreakerPolicy.initial))
  }

  /** Each state change is reported once through `onTransition`, after the state is updated. */
  def create[F[_]](settings: Settings, onTransition: Transition => F[Unit])(using
      F: Temporal[F]
  ): F[CircuitBreaker[F]] =
    Ref.of[F, State](CircuitBreakerPolicy.initial).map { state =>
      // A failing listener must never leak a probe slot or fail the protected call.
      def announce(before: State, after: State): F[Unit] =
        CircuitBreakerPolicy.transition(before, after).traverse_(onTransition(_).attempt.void)

      new CircuitBreaker[F] {
        def protect[A](fa: F[A])(classify: Outcome[F, Throwable, A] => F[Completion]): F[Either[CircuitRejected, A]] =
          F.uncancelable { poll =>
            for {
              now <- Clock[F].realTimeInstant
              step <- state.modify { before =>
                val (after, admission) = CircuitBreakerPolicy.attempt(settings, before, now)
                (after, (before, after, admission))
              }
              (before, after, admission) = step
              _ <- announce(before, after)
              result <- admission match {
                case Admission.Reject(retryAt) => F.pure(Left(CircuitRejected(retryAt)))
                case Admission.Admit(probe)    =>
                  poll(fa)
                    .guaranteeCase(outcome => classify(outcome).flatMap(finish(probe, _)))
                    .map(Right(_))
              }
            } yield result
          }

        private def finish(probe: Option[Instant], completion: Completion): F[Unit] =
          for {
            now <- Clock[F].realTimeInstant
            change <- state.modify { before =>
              val after = CircuitBreakerPolicy.complete(settings, before, probe, completion, now)
              (after, (before, after))
            }
            _ <- announce(change._1, change._2)
          } yield ()

        def current: F[CircuitState] = state.get

        def admittedState: F[Option[CircuitState]] =
          (state.get, Clock[F].realTimeInstant)
            .mapN((current, now) => Option.when(CircuitBreakerPolicy.admits(settings, current, now))(current))
      }
    }

  /** Success for a value, Counted for errors `counted` recognises, Ignored for other errors and cancellation. */
  def classifier[F[_]: Applicative, A](counted: Throwable => Boolean): Outcome[F, Throwable, A] => F[Completion] = {
    case Outcome.Succeeded(_)             => Applicative[F].pure(Completion.Success)
    case Outcome.Errored(e) if counted(e) => Applicative[F].pure(Completion.CountedFailure)
    case _                                => Applicative[F].pure(Completion.IgnoredFailure)
  }
}
