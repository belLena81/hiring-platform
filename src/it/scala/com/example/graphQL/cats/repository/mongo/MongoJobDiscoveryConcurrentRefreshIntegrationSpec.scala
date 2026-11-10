package com.example.graphQL.cats.repository.mongo

import cats.effect.{Deferred, IO}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.search.*
import com.mongodb.client.model.{Filters, Updates}
import mongo4cats.database.MongoDatabase
import org.bson.Document
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** HDQ-03 concurrent refresh: nearby jobs are paged with a cursor while jobs are closed, deleted, inserted or moved
  * between pages. The documented limitation is that a cursor is a position in (distance, id) order, not a snapshot: a
  * job changed behind the cursor is not revisited, a job changed ahead of it is read in its new state. A page never
  * repeats a job, never returns a job whose closure or deletion committed before the page was read, never fails, and
  * keeps the (distance, id) order. Mutations are released by explicit handshakes, not by sleeping.
  */
final class MongoJobDiscoveryConcurrentRefreshIntegrationSpec extends MongoJobDiscoveryFixture {
  private val query = NearbyJobsQuery(center, 50d, filter)

  /** Distinct, strictly increasing distances from the center: step n is about n * 111 m north. */
  private def at(step: Int): GeoPoint = GeoPoint(center.latitude + step * 0.001d, center.longitude)
  private def id(index: Int): String = job(index).id.value.toString

  private final class Env(db: MongoDatabase[IO], nearby: (NearbyJobsQuery, Int) => IO[List[NearbyJob]]) {
    def page(after: Option[NearbyJob], limit: Int): IO[List[NearbyJob]] =
      nearby(
        query.copy(after = after.map(hit => NearbyJobCursor(hit.distanceKm, hit.job.id, query.fingerprint))),
        limit
      )
    def insert(index: Int, point: GeoPoint): IO[Unit] =
      MongoRepositoryTestSupport.insertOne(db, MongoCollections.Jobs, MongoHiringCodecs.job(job(index, Some(point))))
    private def jobs = MongoRepositoryTestSupport.collection(db, MongoCollections.Jobs)
    def close(index: Int): IO[Unit] =
      jobs.flatMap(_.updateOne(Filters.eq("_id", id(index)), Updates.set("status", JobStatus.Closed.toString))).void
    def delete(index: Int): IO[Unit] = jobs.flatMap(_.deleteOne(Filters.eq("_id", id(index)))).void
    def move(index: Int, point: GeoPoint): IO[Unit] =
      jobs
        .flatMap(
          _.updateOne(
            Filters.eq("_id", id(index)),
            Updates.set(
              "location.point",
              new Document("type", "Point").append("coordinates", List(point.longitude, point.latitude).asJava)
            )
          )
        )
        .void

    /** Pages from `after` until an empty page; the final empty page is part of the result. */
    def drain(after: Option[NearbyJob], limit: Int): IO[List[List[NearbyJob]]] =
      page(after, limit).flatMap(current =>
        current.lastOption.fold(IO.pure(List(current)))(last => drain(Some(last), limit).map(current :: _))
      )
  }

  private def environment(body: Env => IO[Unit]): IO[Unit] =
    discoveryResource.use { case (fixture, policy, scope) =>
      val repository = MongoJobRepository.transactional(
        fixture.database,
        fixture.client,
        MongoEmbeddingWorkEnqueuer.disabled,
        Diagnostics.noop[IO],
        Some(policy)
      )
      MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO]) *>
        body(new Env(fixture.database, (q, limit) => success(repository.nearbyJobs(scope, q, limit))))
    }

  /** Reads the first page, releases `mutations` on another fiber, waits until they are committed, then reads on. */
  private def interleaved[A, B](first: IO[A], mutations: IO[Unit])(rest: A => IO[B]): IO[(A, B)] =
    for {
      firstRead <- Deferred[IO, Unit]
      committed <- Deferred[IO, Unit]
      mutator <- (firstRead.get *> mutations *> committed.complete(()).void).start
      a <- first
      _ <- firstRead.complete(())
      _ <- committed.get.timeout(30.seconds)
      b <- rest(a)
      _ <- mutator.joinWithNever
    } yield (a, b)

  private def ids(hits: List[NearbyJob]): List[Int] = hits.map(_.job.id.value.getLeastSignificantBits.toInt)
  private def assertOrdered(hits: List[NearbyJob]): Unit =
    hits.zip(hits.drop(1)).foreach { case (before, after) =>
      val ordered = before.distanceKm < after.distanceKm ||
        (before.distanceKm == after.distanceKm && before.job.id.value.toString < after.job.id.value.toString)
      assert(ordered, clue(before -> after))
    }

  test(
    "HDQ-03 jobs closed, deleted, inserted and moved between pages: no duplicates, committed exclusions hold, order is kept"
  ) {
    environment { env =>
      for {
        _ <- (1 to 12).toList.traverse_(index => env.insert(index, at(index)))
        result <- interleaved(
          env.page(None, 4),
          // Between pages: ahead of the cursor a job closes, one is deleted, one is inserted far away and one moves
          // to the far end; behind the cursor a job is inserted and one job moves back.
          List(
            env.close(6),
            env.delete(7),
            env.insert(13, at(13)),
            env.move(9, at(20)),
            env.insert(0, at(1)),
            env.move(8, at(2))
          ).parSequence_
        )(first => env.drain(first.lastOption, 4))
        (first, later) = result
        all = first ++ later.flatten
        _ = assertEquals(ids(first), List(1, 2, 3, 4))
        // 8 moved behind the cursor and 0 appeared behind it: neither is revisited (the documented limitation).
        _ = assertEquals(later.map(ids), List(List(5, 10, 11, 12), List(13, 9), Nil))
        _ = assertEquals(ids(all).distinct, ids(all))
        _ = assert(Set(0, 6, 7, 8).intersect(ids(all).toSet).isEmpty, clue(ids(all)))
        _ = assertOrdered(all)
      } yield ()
    }
  }

  test("HDQ-03 a cursor at the last item returns an empty page, and a later insert beyond it is read exactly once") {
    environment { env =>
      for {
        _ <- (1 to 3).toList.traverse_(index => env.insert(index, at(index)))
        initial <- env.page(None, 3)
        emptyAfterLast <- env.page(initial.lastOption, 3)
        _ = assertEquals(ids(initial), List(1, 2, 3))
        _ = assertEquals(emptyAfterLast, Nil)
        result <- interleaved(env.page(None, 3), env.insert(4, at(4)) *> env.insert(0, at(0)))(first =>
          env.drain(first.lastOption, 3)
        )
        (first, later) = result
        _ = assertEquals(ids(first), List(1, 2, 3))
        // The page read before the inserts was the first three jobs; a job inserted behind the cursor is skipped.
        _ = assertEquals(later.map(ids), List(List(4), Nil))
        _ = assertOrdered(first ++ later.flatten)
      } yield ()
    }
  }

  test("HDQ-03 deleting or closing every remaining job yields an empty page, and the old cursor stays usable") {
    environment { env =>
      for {
        _ <- (1 to 6).toList.traverse_(index => env.insert(index, at(index)))
        result <- interleaved(env.page(None, 3), List(env.delete(4), env.delete(5), env.close(6)).parSequence_)(first =>
          env.drain(first.lastOption, 3)
        )
        (first, later) = result
        _ = assertEquals(ids(first), List(1, 2, 3))
        _ = assertEquals(later, List(Nil))
        // The same cursor, replayed after a later insert beyond it, still works and returns only that job.
        _ <- env.insert(7, at(7))
        replay <- env.drain(first.lastOption, 3)
        _ = assertEquals(replay.map(ids), List(List(7), Nil))
      } yield ()
    }
  }

  test(
    "HDQ-03 equal-distance ties at a page boundary: closing tied jobs, even the cursor's own, never repeats or revives one"
  ) {
    environment { env =>
      for {
        _ <- (1 to 6).toList.traverse_(index => env.insert(index, at(3)))
        result <- interleaved(
          env.page(None, 3),
          // 4 is the next tied job and is closed; 3 is the tied job the cursor points at. 7 ties and sorts after the
          // cursor, 0 ties and sorts before it.
          List(env.close(4), env.close(3), env.insert(7, at(3)), env.insert(0, at(3))).parSequence_
        )(first => env.drain(first.lastOption, 3))
        (first, later) = result
        all = first ++ later.flatten
        _ = assertEquals(ids(first), List(1, 2, 3))
        _ = assertEquals(later.map(ids), List(List(5, 6, 7), Nil))
        _ = assertEquals(ids(all).distinct, ids(all))
        _ = assert(Set(0, 4).intersect(ids(all).toSet).isEmpty, clue(ids(all)))
        _ = assertEquals(all.map(_.distanceKm).distinct.size, 1)
        _ = assertOrdered(all)
      } yield ()
    }
  }

  test(
    "HDQ-03 paging while another fiber keeps inserting, closing and deleting jobs: untouched jobs appear once, in order"
  ) {
    environment { env =>
      val stable = (1 to 20).toList
      def churn(n: Int, started: Deferred[IO, Unit]): IO[Unit] = {
        val churned = GeoPoint(center.latitude + (1 + n % 20) * 0.001d + 0.0004d, center.longitude)
        env.insert(100 + n, churned) *> env.close(99 + n) *> env.delete(98 + n) *>
          started.complete(()).void >> churn(n + 1, started)
      }
      for {
        _ <- stable.traverse_(index => env.insert(index, at(index)))
        started <- Deferred[IO, Unit]
        mutator <- churn(1, started).start
        _ <- started.get.timeout(30.seconds)
        pages <- env.drain(None, 3).timeout(2.minutes).guarantee(mutator.cancel)
        all = pages.flatten
        _ = assertEquals(ids(all).distinct, ids(all))
        _ = assertEquals(ids(all).filter(_ < 100), stable)
        _ = assertOrdered(all)
        _ = assertEquals(pages.last, Nil)
      } yield ()
    }
  }
}
