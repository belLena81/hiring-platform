package com.example.graphQL.cats.repository.mongo

import cats.Monad
import cats.effect.IO
import cats.syntax.all.*
import fs2.Stream
import org.bson.Document

private[mongo] object MongoBoundedResults {

  /** Reads at most `maximum` values, failing with `exceeded` when the source holds more. */
  def collect[A](stream: Stream[IO, A], maximum: Int, exceeded: => Throwable): IO[List[A]] =
    stream.take(maximum.toLong + 1L).compile.toList.flatMap { values =>
      if (values.size > maximum) IO.raiseError(exceeded) else IO.pure(values)
    }
}

/** Pages a collection in ascending `_id` order so migrations and audits resume from a checkpoint. */
private[mongo] object MongoBatchScan {

  /** Page size shared by every migration and audit scan. */
  val BatchSize = 500

  /** `fetch` returns at most `batchSize` documents after the given key; `handle` runs per page and a full page
    * continues from `next` of its last document.
    */
  def byId[K](batchSize: Int, from: Option[K])(fetch: Option[K] => IO[List[Document]])(next: Document => IO[K])(
      handle: List[Document] => IO[Unit]
  ): IO[Unit] =
    Monad[IO].tailRecM(from) { after =>
      fetch(after).flatMap { rows =>
        handle(rows) *> rows.lastOption
          .filter(_ => rows.size == batchSize)
          .traverse(next)
          .map(_.fold[Either[Option[K], Unit]](Right(()))(key => Left(Some(key))))
      }
    }
}
