package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.{GeoPoint, Job, JobState, Location}
import com.mongodb.MongoClientSettings
import munit.FunSuite
import org.bson.BsonDocument

import java.time.Instant
import java.util.UUID
import scala.jdk.CollectionConverters.*

class MongoJobContentUpdateSpec extends FunSuite {
  private val now = Instant.parse("2026-01-01T10:00:00Z")
  private val closedAt = Instant.parse("2026-02-01T10:00:00Z")

  private def job(location: Location, state: JobState): Job = Job(
    JobId(UUID.fromString("00000000-0000-0000-0000-000000000601")),
    UserId(UUID.fromString("00000000-0000-0000-0000-000000000602")),
    "Scala Developer",
    "Build services",
    List("Cats Effect"),
    Set("Scala", "MongoDB"),
    location,
    state,
    now,
    state.closedAt.getOrElse(now)
  )

  private def render(job: Job): BsonDocument =
    MongoJobContentUpdate(job).bson.toBsonDocument(classOf[BsonDocument], MongoClientSettings.getDefaultCodecRegistry)

  private def keys(document: BsonDocument, operator: String): Set[String] =
    Option(document.getDocument(operator, null)).fold(Set.empty[String])(_.keySet().asScala.toSet)

  test("MSC-01.1 an open job without city or point sets content, unsets absent optionals and advances version once") {
    val update = render(job(Location("Cyprus", None, remote = true), JobState.Open))
    assertEquals(
      keys(update, "$set"),
      Set(
        "recruiterId",
        "title",
        "description",
        "requirements",
        "skills",
        "location.country",
        "location.remote",
        "status",
        "updatedAt"
      )
    )
    assertEquals(keys(update, "$unset"), Set("location.city", "location.point", "closedAt"))
    assertEquals(update.getDocument("$inc").getInt64("version").getValue, 1L)
    assertEquals(keys(update, "$inc"), Set("version"))
  }

  test("MSC-01.1 a closed job with city and point sets them and never touches embedding, fence or createdAt") {
    val update = render(
      job(
        Location("Cyprus", Some("Nicosia"), remote = false, coordinates = Some(GeoPoint(33.4, 35.1))),
        JobState.Closed(closedAt)
      )
    )
    assert(keys(update, "$set").containsAll(Set("location.city", "location.point", "closedAt", "status")))
    assertEquals(keys(update, "$unset"), Set.empty[String])
    val touched = keys(update, "$set") ++ keys(update, "$unset") ++ keys(update, "$inc")
    assert(
      touched.intersect(Set("embedding", "embeddingMeta", "admissionFence", "createdAt", "_id")).isEmpty,
      clue(touched)
    )
    assertEquals(update.getDocument("$set").getDocument("location.point").getArray("coordinates").size(), 2)
  }

  private val NotContent = Set("_id", "createdAt", "version", "embedding", "embeddingMeta", "admissionFence")

  /** Stored content keys of a job document, with the nested location flattened to dotted paths. */
  private def storedContentKeys(job: Job): Set[String] = {
    val document = MongoHiringCodecs.job(job)
    val location = document.get("location", classOf[org.bson.Document])
    (document.keySet().asScala.toSet - "location" -- NotContent) ++
      location.keySet().asScala.map(key => s"location.$key")
  }

  test("MSC-01.1 drift guard: the content update covers exactly the content keys the job codec stores") {
    val full = job(
      Location("Cyprus", Some("Nicosia"), remote = false, coordinates = Some(GeoPoint(33.4, 35.1))),
      JobState.Closed(closedAt)
    )
    val minimal = job(Location("Cyprus", None, remote = true), JobState.Open)
    def written(job: Job): Set[String] = {
      val update = render(job)
      keys(update, "$set") ++ keys(update, "$unset")
    }
    // Everything stored for a fully populated job is written; absent optionals are unset, never left stale.
    assertEquals(storedContentKeys(full), written(full))
    assertEquals(written(minimal), written(full))
    assert(storedContentKeys(minimal).subsetOf(written(minimal)), clue(storedContentKeys(minimal)))
  }

  extension (keys: Set[String]) private def containsAll(expected: Set[String]): Boolean = expected.subsetOf(keys)
}
