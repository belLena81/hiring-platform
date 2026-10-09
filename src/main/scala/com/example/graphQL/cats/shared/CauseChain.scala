package com.example.graphQL.cats.shared

object CauseChain {

  /** The throwable followed by its causes, outermost first; a self-referencing cause ends the chain. */
  def apply(error: Throwable): Iterator[Throwable] =
    Iterator
      .iterate(Option(error))(_.flatMap(current => Option(current.getCause).filterNot(_ eq current)))
      .takeWhile(_.nonEmpty)
      .flatten
}
