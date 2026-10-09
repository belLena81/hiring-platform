package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.{IO, Resource}
import com.example.graphQL.cats.config.KafkaSaslSecurityProtocol
import com.example.graphQL.cats.domain.workflow.InterviewTopicPair
import com.github.dockerjava.api.model.{ExposedPort, PortBinding, Ports, Ulimit}
import org.apache.kafka.clients.admin.{Admin, NewTopic}
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import java.net.ServerSocket
import java.time.Duration
import java.util.{Properties, UUID}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Owns one disposable broker; restarting its child process preserves tmpfs logs and never addresses shared services.
  */
private[graphQL] object InterviewBrokerFixture {
  private val Image =
    "apache/kafka:3.9.2@sha256:05b4616e0702ef2729327705d54ad6b50ea70b271c4b730fabd2320789fb7b02"
  private final class Broker extends GenericContainer[Broker](DockerImageName.parse(Image))
  private val Supervisor = """
    set -eu
    broker_pid=''
    trap 'if [ -n "$broker_pid" ]; then kill -TERM "$broker_pid" 2>/dev/null || true; wait "$broker_pid" || true; fi; exit 0' TERM INT
    while true; do
      while [ -e /mnt/shared/config/interview.stopped ]; do sleep 0.1; done
      /etc/kafka/docker/run &
      broker_pid=$!
      echo "$broker_pid" > /mnt/shared/config/interview.pid
      wait "$broker_pid" || true
    done
    """

  final class Running private[InterviewBrokerFixture] (
      broker: Broker,
      val bootstrap: String,
      password: String,
      val topics: InterviewTopicPair
  ) {
    def config(worker: Boolean): InterviewKafkaConfig = InterviewKafkaConfig(
      bootstrap,
      if (worker) "interview_result_publisher" else "interview_command_publisher",
      password,
      KafkaSaslSecurityProtocol.Plaintext,
      worker,
      topics = topics,
      workerGroup = "interview-outage-workers",
      orchestratorGroup = "interview-outage-orchestrator"
    )

    def adminProperties: Properties = {
      val properties = new Properties()
      properties.put("bootstrap.servers", bootstrap)
      KafkaClientSettings
        .security(Some("broker"), Some(password), KafkaSaslSecurityProtocol.Plaintext)
        .foreach { case (key, value) => val _ = properties.put(key, value) }
      properties.put("request.timeout.ms", "2000")
      properties.put("default.api.timeout.ms", "3000")
      properties
    }

    private def admin: Resource[IO, Admin] =
      Resource.make(IO.blocking(Admin.create(adminProperties)))(value => IO.blocking(value.close()))

    def awaitReady: IO[Unit] = admin.use { client =>
      def poll: IO[Unit] = IO.blocking(client.describeCluster().nodes().get(3L, TimeUnit.SECONDS)).attempt.flatMap {
        case Right(nodes) if !nodes.isEmpty => IO.unit
        case _                              => IO.sleep(100.millis) *> poll
      }
      poll.timeout(60.seconds)
    }

    def initialize: IO[Unit] = awaitReady *> admin.use { client =>
      IO.blocking {
        client
          .createTopics(
            List(new NewTopic(topics.commands, 1, 1.toShort), new NewTopic(topics.results, 1, 1.toShort)).asJava
          )
          .all()
          .get(20L, TimeUnit.SECONDS)
      }.void
    }

    def stopProcess: IO[Unit] = IO.blocking {
      val result = broker.execInContainer(
        "bash",
        "-ec",
        """
        touch /mnt/shared/config/interview.stopped
        broker_pid=$(cat /mnt/shared/config/interview.pid)
        case "$broker_pid" in ''|*[!0-9]*) exit 1;; esac
        echo "$broker_pid" > /mnt/shared/config/interview.stopped-pid
        kill -TERM "$broker_pid"
        for attempt in {1..300}; do
          if ! kill -0 "$broker_pid" 2>/dev/null; then exit 0; fi
          sleep 0.1
        done
        exit 1
        """
      )
      assert(result.getExitCode == 0, "Owned broker process did not terminate")
      assert(broker.getDockerClient.inspectContainerCmd(broker.getContainerId).exec().getState.getRunning)
    }

    def restartProcess: IO[Unit] =
      IO.blocking {
        val result = broker.execInContainer("bash", "-ec", "rm /mnt/shared/config/interview.stopped")
        assert(result.getExitCode == 0, "Owned broker restart signal failed")
      } *> awaitReady *> IO.blocking {
        val result = broker.execInContainer(
          "bash",
          "-ec",
          """
          old_pid=$(cat /mnt/shared/config/interview.stopped-pid)
          current_pid=$(cat /mnt/shared/config/interview.pid)
          test "$old_pid" != "$current_pid"
          kill -0 "$current_pid"
          """
        )
        assert(result.getExitCode == 0, "Broker did not restart with a new process identity")
      }
  }

  def resource(topics: InterviewTopicPair): Resource[IO, Running] = {
    for {
      port <- Resource.eval(IO.blocking {
        val socket = new ServerSocket(0)
        try socket.getLocalPort
        finally socket.close()
      })
      password <- Resource.eval(IO.randomUUID.map(_.toString))
      broker <- Resource.make(IO.blocking {
        val instance = new Broker
        val jaas =
          s"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"broker\" password=\"$password\" user_broker=\"$password\" user_interview_command_publisher=\"$password\" user_interview_result_publisher=\"$password\";"
        val _ = instance
          .withExposedPorts(29092)
          .withCreateContainerCmdModifier(command => {
            val _ = command.getHostConfig.withPortBindings(
              new PortBinding(Ports.Binding.bindIpAndPort("127.0.0.1", port), new ExposedPort(29092))
            )
            val _ = command.getHostConfig.withMemory(768L * 1024L * 1024L)
            val _ = command.getHostConfig.withUlimits(Array(new Ulimit("nofile", 65536L, 65536L)))
            val _ = command.withEntrypoint("bash", "-ec")
          })
          .withTmpFs(
            Map(
              "/var/lib/kafka/data" -> "rw,uid=1000,gid=1000,size=512m",
              "/etc/kafka/secrets" -> "rw,uid=1000,gid=1000,size=16m",
              "/mnt/shared/config" -> "rw,uid=1000,gid=1000,size=16m"
            ).asJava
          )
          .withCommand(Array(Supervisor)*)
          .withEnv(
            Map(
              "CLUSTER_ID" -> org.apache.kafka.common.Uuid.randomUuid().toString,
              "KAFKA_NODE_ID" -> "1",
              "KAFKA_PROCESS_ROLES" -> "broker,controller",
              "KAFKA_CONTROLLER_QUORUM_VOTERS" -> "1@localhost:9093",
              "KAFKA_LISTENERS" -> "INTERNAL://:9092,EXTERNAL://:29092,CONTROLLER://:9093",
              "KAFKA_ADVERTISED_LISTENERS" -> s"INTERNAL://localhost:9092,EXTERNAL://127.0.0.1:$port",
              "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP" -> "INTERNAL:SASL_PLAINTEXT,EXTERNAL:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT",
              "KAFKA_CONTROLLER_LISTENER_NAMES" -> "CONTROLLER",
              "KAFKA_INTER_BROKER_LISTENER_NAME" -> "INTERNAL",
              "KAFKA_SASL_ENABLED_MECHANISMS" -> "PLAIN",
              "KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL" -> "PLAIN",
              "KAFKA_LISTENER_NAME_INTERNAL_PLAIN_SASL_JAAS_CONFIG" -> jaas,
              "KAFKA_LISTENER_NAME_EXTERNAL_PLAIN_SASL_JAAS_CONFIG" -> jaas,
              "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR" -> "1",
              "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR" -> "1",
              "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR" -> "1",
              "KAFKA_LOG_DIRS" -> "/var/lib/kafka/data",
              "KAFKA_HEAP_OPTS" -> "-Xms256m -Xmx256m"
            ).asJava
          )
          .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(90)))
        instance
      })(instance => IO.blocking(instance.stop()))
      _ <- Resource.eval(IO.blocking(broker.start()))
      running = new Running(broker, s"127.0.0.1:$port", password, topics)
      _ <- Resource.eval(running.initialize)
    } yield running
  }

  def uniqueTopics: InterviewTopicPair = {
    val suffix = UUID.randomUUID().toString.replace("-", "")
    InterviewTopicPair(s"interview.commands.$suffix", s"interview.results.$suffix")
  }
}
