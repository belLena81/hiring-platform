package com.example.hiring.testing

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import io.circe.Json
import org.apache.kafka.clients.admin.{Admin, NewTopic}
import org.apache.kafka.common.acl.{AccessControlEntry, AclBinding, AclOperation, AclPermissionType}
import org.apache.kafka.common.resource.{PatternType, ResourcePattern, ResourceType}
import org.apache.kafka.common.errors.{GroupIdNotFoundException, UnknownTopicOrPartitionException}
import java.nio.file.{Files, Path, StandardOpenOption}
import scala.jdk.CollectionConverters.*

/** Exact per-scenario topics/ACLs/groups on the verified isolated broker. */
object KafkaTestNamespace {
  final case class Namespace(
      manifest: LocalTestServices.Manifest,
      commands: String,
      results: String,
      events: String,
      workers: String,
      orchestrator: String,
      eventGroup: String
  )

  def resource: Resource[IO, Option[Namespace]] = Resource.eval(LocalTestServices.manifest).flatMap {
    case None           => Resource.pure(None)
    case Some(manifest) =>
      for {
        _ <- Resource.eval(LocalTestServices.verifiedKafka(manifest))
        id <- Resource.eval(IO.randomUUID.map(_.toString.replace("-", "")))
        namespace = Namespace(
          manifest,
          "hiring.test.commands." + id,
          "hiring.test.results." + id,
          "hiring.test.events." + id,
          "hiring.test.workers." + id,
          "hiring.test.orchestrator." + id,
          "hiring.test.events-group." + id
        )
        admin <- Resource.make(
          IO.blocking(Admin.create(LocalTestServices.adminProperties(manifest, "broker", manifest.brokerPassword)))
        )(value => IO.blocking(value.close()))
        marker <- Resource.eval(register(namespace, id))
        // Register finalization before creation so partially created topics/ACLs are cleaned on acquisition failure.
        _ <- Resource.make(IO.unit)(_ =>
          cleanup(admin, namespace) *> marker.traverse_(file => IO.blocking(Files.deleteIfExists(file)).void)
        )
        _ <- Resource.eval(initialize(admin, namespace))
      } yield Some(namespace)
  }

  private def register(value: Namespace, id: String): IO[Option[Path]] = sys.env.get("HIRING_TEST_RUN_REGISTRY") match {
    case None       => IO.pure(None)
    case Some(path) =>
      IO.blocking {
        val directory = Path.of(path)
        require(!Files.isSymbolicLink(directory), "Test run registry must not be a symlink")
        val file = directory.toRealPath().resolve(id + ".kafka")
        val contents = Json
          .obj(
            "topics" -> Json.arr(Vector(value.commands, value.results, value.events).map(Json.fromString)*),
            "groups" -> Json.arr(Vector(value.workers, value.orchestrator, value.eventGroup).map(Json.fromString)*)
          )
          .noSpaces
        val _ = Files.writeString(file, contents, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
        Some(file)
      }
  }

  private def acls(value: Namespace): Vector[AclBinding] = {
    def grant(principal: String, kind: ResourceType, name: String, operations: AclOperation*): Vector[AclBinding] =
      operations.toVector.map(operation =>
        new AclBinding(
          new ResourcePattern(kind, name, PatternType.LITERAL),
          new AccessControlEntry("User:" + principal, "*", operation, AclPermissionType.ALLOW)
        )
      )
    import AclOperation.*
    import ResourceType.*
    grant("interview_command_publisher", TOPIC, value.commands, WRITE, DESCRIBE) ++
      grant("interview_command_publisher", TOPIC, value.results, READ, DESCRIBE) ++
      grant("interview_command_publisher", GROUP, value.orchestrator, READ) ++
      grant("interview_result_publisher", TOPIC, value.commands, READ, DESCRIBE) ++
      grant("interview_result_publisher", TOPIC, value.results, WRITE, DESCRIBE) ++
      grant("interview_result_publisher", GROUP, value.workers, READ) ++
      grant("hiring_publisher_v2", TOPIC, value.events, WRITE, DESCRIBE) ++
      grant("analytics_reader", TOPIC, value.events, READ, DESCRIBE) ++
      grant("analytics_reader", GROUP, value.eventGroup, READ)
  }

  private def initialize(admin: Admin, value: Namespace): IO[Unit] = IO.blocking {
    val topics = Vector(value.commands, value.results, value.events).map { name =>
      new NewTopic(name, 1, 1.toShort).configs(
        Map(
          "cleanup.policy" -> "delete",
          "retention.ms" -> "604800000",
          "max.message.bytes" -> (if (name == value.events) "1048576" else "65536"),
          "min.insync.replicas" -> "1"
        ).asJava
      )
    }
    admin.createTopics(topics.asJava).all().get(20L, java.util.concurrent.TimeUnit.SECONDS)
    val _ = admin.createAcls(acls(value).asJava).all().get(20L, java.util.concurrent.TimeUnit.SECONDS)
  }

  private def cleanup(admin: Admin, value: Namespace): IO[Unit] = IO.blocking {
    Vector(value.workers, value.orchestrator, value.eventGroup).foreach { group =>
      try admin.deleteConsumerGroups(List(group).asJava).all().get(20L, java.util.concurrent.TimeUnit.SECONDS)
      catch {
        case error: java.util.concurrent.ExecutionException if error.getCause.isInstanceOf[GroupIdNotFoundException] =>
          ()
      }
    }
    Vector(value.commands, value.results, value.events).foreach { topic =>
      try { val _ = admin.deleteTopics(List(topic).asJava).all().get(20L, java.util.concurrent.TimeUnit.SECONDS) }
      catch {
        case error: java.util.concurrent.ExecutionException
            if error.getCause.isInstanceOf[UnknownTopicOrPartitionException] =>
          ()
      }
    }
    val _ = admin.deleteAcls(acls(value).map(_.toFilter).asJava).all().get(20L, java.util.concurrent.TimeUnit.SECONDS)
  }
}
