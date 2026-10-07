package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.search.*
import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*

private[mongo] abstract class MongoJobDiscoveryFixture extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes
  protected val now = Instant.parse("2026-10-06T12:00:00Z")
  protected val center = GeoPoint(35.1856d, 33.3823d)
  protected val filter = JobSearchFilter(None, Set.empty, None)
  protected def job(index: Int, point: Option[GeoPoint] = Some(center), remote: Boolean = false): Job =
    Job(
      JobId(new UUID(0L, index.toLong)),
      UserId(new UUID(1L, 1L)),
      "Engineer",
      "Build hiring",
      List("Scala"),
      Set("Scala", s"Skill$index"),
      Location("Cyprus", "Nicosia", remote, point),
      JobStatus.Open,
      now,
      now
    )
  protected def discoveryResource = mongoResource.evalMap { fixture =>
    val actor = com.example.graphQL.cats.service.ServiceFixtures.candidate
    val users = MongoUserRepository.transactional(
      fixture.database,
      fixture.client,
      MongoEmbeddingWorkEnqueuer.disabled,
      Diagnostics.noop
    )
    for {
      _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
      _ <- MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.Users, MongoHiringCodecs.user(actor))
      policy <- DiscoveryQueryPolicy.create(2.seconds, 4)
      scope <- IO.fromEither(
        com.example.graphQL.cats.service.read.HiringReadScope
          .validated(
            com.example.graphQL.cats.service.ActorContext(actor.id, actor.role),
            actor,
            com.example.graphQL.cats.service.auth.ActorAuthorization(users)
          )
          .leftMap(error => new AssertionError(error.toString))
      )
    } yield (fixture, policy, scope)
  }
  protected def success[A](effect: RepositoryIO[A]): IO[A] =
    effect.value.flatMap(_.fold(error => IO.raiseError(new AssertionError(s"Repository failed: $error")), IO.pure))

}
