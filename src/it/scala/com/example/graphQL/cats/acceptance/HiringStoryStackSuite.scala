package com.example.graphQL.cats.acceptance

import cats.effect.{IO, Resource}
import com.example.graphQL.cats.repository.mongo.MongoDatabaseProbe
import com.example.hiring.testing.{KafkaTestNamespace, LocalTestServices}
import mongo4cats.database.MongoDatabase
import munit.CatsEffectSuite
import scala.concurrent.duration.*

/** A suite that runs against the real `Main` over HTTP on the wrapper-owned Mongo and Kafka test services.
  *
  * Without the wrapper's manifest the suite is skipped as BLOCKED, never reported as a pass.
  */
abstract class HiringStoryStackSuite extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes

  /** The interview workflow runs over Kafka and is off unless a suite asks for it. */
  protected def interviewEnabled: Boolean = false

  protected final case class Stack(
      running: HiringStoryStack.Running,
      operations: StoryOperations,
      database: MongoDatabase[IO]
  )

  private val stackFixture = ResourceSuiteLocalFixture(
    "hiring-story-stack",
    KafkaTestNamespace.resource.flatMap {
      case None            => Resource.pure[IO, Option[Stack]](None)
      case Some(namespace) =>
        for {
          _ <- Resource.eval(LocalTestServices.verifiedMongo(namespace.manifest))
          client <- MongoDatabaseProbe.clientResource(namespace.manifest.mongoUri)
          database <- LocalTestServices.database(client)
          _ <- Resource.onFinalize(ConsumerGroupDrain.awaitEmpty(namespace))
          _ <- OutboxDiagnostics.resource(database)
          running <- HiringStoryStack.resource(
            namespace.manifest,
            namespace,
            database.underlying.getName,
            interviewEnabled
          )
        } yield Some(Stack(running, new StoryOperations(running.client), database))
    }
  )

  override def munitFixtures: List[munit.AnyFixture[?]] = List(stackFixture)

  protected def stack: Stack = {
    val value = stackFixture()
    assume(value.nonEmpty, "BLOCKED: isolated test service manifest absent; use scripts/run-local-tests.sh")
    value.get
  }

  protected def operations: StoryOperations = stack.operations

  protected def adminActor: IO[StoryActor] =
    operations.login(HiringStoryStack.AdminName, HiringStoryStack.AdminPassword, "ADMIN")
}
