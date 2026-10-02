package com.example.hiring.analytics.cli

import cats.effect.{Deferred, IO, Resource}
import cats.effect.std.Dispatcher
import cats.syntax.all.*
import com.example.hiring.analytics.errors.AnalyticsError
import sun.misc.{Signal, SignalHandler}

/** OS interop stays outside the streaming service and its resource owners. */
private[cli] trait StreamingProcessSignals {
  def install(name: String, requestStop: () => Unit): Resource[IO, Unit]
}

private[cli] object StreamingProcessSignals {
  val jvm: StreamingProcessSignals = new StreamingProcessSignals {
    override def install(name: String, requestStop: () => Unit): Resource[IO, Unit] =
      Resource
        .make(
          IO.delay {
            try {
              val signal = new Signal(name)
              val previous = Signal.handle(
                signal,
                new SignalHandler {
                  override def handle(received: Signal): Unit = requestStop()
                }
              )
              (signal, previous)
            } catch {
              case _: LinkageError =>
                throw AnalyticsError.InvalidConfiguration(
                  "streaming process signal handling is unavailable"
                )
            }
          }.handleErrorWith(_ =>
            IO.raiseError(
              AnalyticsError.InvalidConfiguration("streaming process signal handling is unavailable")
            )
          )
        ) { case (signal, previous) => IO.delay(Signal.handle(signal, previous)).void }
        .void
  }
}

private[cli] object StreamingProcessTermination {
  // Synchronization only protects the OS callback versus Dispatcher release. Each evaluation owns its gate.
  private final class StopRequests(dispatcher: Dispatcher[IO], stopped: Deferred[IO, Unit]) {
    private var accepting = true
    private var requested = false

    def request(): Unit = synchronized {
      if (accepting && !requested) {
        requested = true
        dispatcher.unsafeRunAndForget(stopped.complete(()).void)
      }
    }

    def close(): Unit = synchronized { accepting = false }
  }

  private[cli] def resource(signals: StreamingProcessSignals): Resource[IO, IO[Unit]] =
    for {
      stopped <- Resource.eval(Deferred[IO, Unit])
      dispatcher <- Dispatcher.sequential[IO]
      requests <- Resource.make(IO.delay(new StopRequests(dispatcher, stopped)))(gate => IO.delay(gate.close()))
      _ <- signals.install("TERM", () => requests.request())
      _ <- signals.install("INT", () => requests.request())
    } yield stopped.get

  def run(program: IO[Unit], signals: StreamingProcessSignals = StreamingProcessSignals.jvm): IO[Unit] =
    // race joins cancellation and every finalizer of the complete streaming Resource.use before restoring handlers.
    resource(signals).use(stopped => IO.race(program, stopped).void)
}
