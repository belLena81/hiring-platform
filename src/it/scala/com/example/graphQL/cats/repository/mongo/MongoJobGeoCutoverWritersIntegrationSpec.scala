package com.example.graphQL.cats.repository.mongo

import cats.effect.{Deferred, IO}
import cats.syntax.all.*
import com.example.graphQL.cats.service.Diagnostics
import com.mongodb.client.model.{Filters, Updates}
import org.bson.Document
import scala.jdk.CollectionConverters.*

/** HDQ-10 writers race: while the job geo-point migration is interrupted (ledger Running because a legacy point is
  * incompatible), many writers insert jobs at the same instant. The installed validator accepts every valid point,
  * including poles and the antimeridian, and rejects every invalid one; nothing is rewritten or invented; and after the
  * exact repair the repeated setup completes the migration with the geo index present.
  */
final class MongoJobGeoCutoverWritersIntegrationSpec extends MongoJobDiscoveryFixture {
  private def point(coordinates: Any*): Document =
    new Document("type", "Point").append("coordinates", coordinates.toList.asJava)

  private def jobDocument(index: Int, location: Option[Document]): Document = {
    val document = MongoHiringCodecs.job(job(index))
    val stored = document.get("location", classOf[Document])
    val _ = location.fold(stored.remove("point"))(stored.put("point", _))
    document
  }

  private val validPoints: List[(String, Option[Document])] = List(
    "north pole" -> Some(point(0d, 90d)),
    "south pole" -> Some(point(0d, -90d)),
    "antimeridian east" -> Some(point(180d, 0d)),
    "antimeridian west" -> Some(point(-180d, 0d)),
    "north-east corner" -> Some(point(180d, 90d)),
    "south-west corner" -> Some(point(-180d, -90d)),
    "origin" -> Some(point(0d, 0d)),
    "ordinary position" -> Some(point(center.longitude, center.latitude)),
    "no point (remote or unlocated job)" -> None
  )

  private val invalidPoints: List[(String, Document)] = List(
    "longitude just above 180" -> point(180.000001d, 0d),
    "longitude just below -180" -> point(-180.000001d, 0d),
    "latitude just above 90" -> point(0d, 90.000001d),
    "latitude just below -90" -> point(0d, -90.000001d),
    "NaN longitude" -> point(Double.NaN, 0d),
    "NaN latitude" -> point(0d, Double.NaN),
    "positive infinity" -> point(Double.PositiveInfinity, 0d),
    "negative infinity" -> point(0d, Double.NegativeInfinity),
    "non-numeric coordinates" -> point("33.3", "35.1"),
    "coordinates that are not an array" -> new Document("type", "Point").append("coordinates", "33.3,35.1"),
    "a point without coordinates" -> new Document("type", "Point"),
    "a single coordinate" -> point(33d)
  )

  private def ledgerState(db: mongo4cats.database.MongoDatabase[IO]): IO[Option[String]] =
    MongoRepositoryTestSupport
      .findOne(db, MongoCollections.HiringMigrationLedger, Filters.eq("_id", "006_job_geo_points"))
      .map(_.map(_.getString("state")))

  private def isValidationFailure(error: Throwable): Boolean = error match {
    case failure: com.mongodb.MongoException => failure.getCode == 121
    case _                                   => false
  }

  /** Starts every task behind one barrier so the writers and the repeated setup attempt contend. */
  private def together[A](tasks: List[IO[A]]): IO[List[A]] =
    for {
      go <- Deferred[IO, Unit]
      fibers <- tasks.traverse(task => (go.get *> task).start)
      _ <- go.complete(())
      results <- fibers.traverse(_.joinWithNever)
    } yield results

  test(
    "HDQ-10 concurrent invalid and valid job writers during an interrupted geo cutover: invalid rejected, valid kept untouched, repair then setup completes"
  ) {
    mongoResource.use { fixture =>
      val db = fixture.database
      val legacy = jobDocument(1, Some(point(181d, 0d)))
      val validIds = validPoints.indices.toList.map(_ + 200)
      val invalidIds = invalidPoints.indices.toList.map(_ + 100)
      for {
        jobs <- MongoRepositoryTestSupport.collection(db, MongoCollections.Jobs)
        _ <- jobs.insertOne(legacy)
        interrupted <- MongoHiringSetup.initialize(db, Diagnostics.noop[IO]).attempt
        _ = assert(interrupted.isLeft)
        _ <- ledgerState(db).map(state => assertEquals(state, Some("Running")))
        validWrites = validPoints.zip(validIds).map { case ((_, location), index) =>
          jobs.insertOne(jobDocument(index, location)).attempt
        }
        invalidWrites = invalidPoints.zip(invalidIds).map { case ((_, invalid), index) =>
          jobs.insertOne(jobDocument(index, Some(invalid))).attempt
        }
        retriedSetup = MongoHiringSetup.initialize(db, Diagnostics.noop[IO]).attempt
        outcomes <- together(retriedSetup :: (validWrites ++ invalidWrites))
        setup = outcomes.head
        results = outcomes.tail
        (valid, invalid) = results.splitAt(validWrites.size)
        _ = assert(setup.isLeft, "the incompatible legacy point must keep the migration failing")
        _ = valid.zip(validPoints).foreach { case (result, (name, _)) => assert(result.isRight, clue(name -> result)) }
        _ = invalid.zip(invalidPoints).foreach { case (result, (name, _)) =>
          assert(result.left.exists(isValidationFailure), clue(name -> result))
        }
        _ <- ledgerState(db).map(state => assertEquals(state, Some("Running")))
        total <- MongoRepositoryTestSupport.count(db, MongoCollections.Jobs)
        _ = assertEquals(total, 1L + validPoints.size)
        // No coordinate was invented, normalised or clamped: every stored point is exactly what its writer sent.
        _ <- validPoints.zip(validIds).traverse_ { case ((name, expected), index) =>
          MongoRepositoryTestSupport
            .findOne(db, MongoCollections.Jobs, Filters.eq("_id", job(index).id.value.toString))
            .map { stored =>
              val storedPoint =
                stored.flatMap(d => Option(d.get("location", classOf[Document]).get("point", classOf[Document])))
              assertEquals(storedPoint.map(_.get("coordinates")), expected.map(_.get("coordinates")), name)
            }
        }
        untouched <- MongoRepositoryTestSupport.findOne(
          db,
          MongoCollections.Jobs,
          Filters.eq("_id", job(1).id.value.toString)
        )
        _ = assertEquals(
          untouched.map(_.get("location", classOf[Document]).get("point", classOf[Document]).get("coordinates")),
          Some(point(181d, 0d).get("coordinates"))
        )
        // The exact repair of the one incompatible job, then repeated setup: the migration converges.
        _ <- jobs.updateOne(
          Filters.eq("_id", job(1).id.value.toString),
          Updates.set("location.point", point(center.longitude, center.latitude))
        )
        _ <- MongoHiringSetup.initialize(db, Diagnostics.noop[IO])
        _ <- MongoHiringSetup.initialize(db, Diagnostics.noop[IO])
        _ <- ledgerState(db).map(state => assertEquals(state, Some("Complete")))
        indexes <- jobs.listIndexes[Document]
        _ = assert(indexes.exists(_.getString("name") == MongoHiringSetup.JobsLocationPointIndex))
        // After completion the same invalid writers are still rejected and the extreme valid points survived the index build.
        afterwards <- together(invalidPoints.zip(invalidIds).map { case ((_, invalidPoint), index) =>
          jobs.insertOne(jobDocument(index + 1000, Some(invalidPoint))).attempt
        })
        _ = afterwards.foreach(result => assert(result.left.exists(isValidationFailure), clue(result)))
        finalTotal <- MongoRepositoryTestSupport.count(db, MongoCollections.Jobs)
        _ = assertEquals(finalTotal, 1L + validPoints.size)
      } yield ()
    }
  }
}
