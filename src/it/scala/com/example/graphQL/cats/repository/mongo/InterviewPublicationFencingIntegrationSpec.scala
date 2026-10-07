package com.example.graphQL.cats.repository.mongo

import cats.effect.{Deferred, IO, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.config.KafkaSaslSecurityProtocol
import com.example.graphQL.cats.domain.model.ApplicationStatus
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, UserId}
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.infrastructure.kafka.{
  InterviewKafkaConfig,
  InterviewKafkaRuntime,
  InterviewMessageCodec,
  InterviewProducerFencer
}
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.port.*
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.consumer.{CloseOptions, ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{
  AuthenticationException,
  AuthorizationException,
  InvalidProducerEpochException,
  ProducerFencedException
}
import org.apache.kafka.common.serialization.{
  ByteArrayDeserializer,
  ByteArraySerializer,
  StringDeserializer,
  StringSerializer
}
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.{Properties, UUID}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Opt-in evidence against the isolated SASL broker and a disposable transactional Mongo replica set. */
final class InterviewPublicationFencingIntegrationSpec extends KafkaIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes

  private case class Broker(values: Map[String, String]) {
    def bootstrap: String = values("INTERVIEW_KAFKA_BOOTSTRAP")
    def password(worker: Boolean): String =
      values(if (worker) "KAFKA_INTERVIEW_WORKER_PASSWORD" else "KAFKA_INTERVIEW_ORCHESTRATOR_PASSWORD")
    def username(worker: Boolean): String = if (worker) "interview_result_publisher" else "interview_command_publisher"
    def config(worker: Boolean): InterviewKafkaConfig =
      InterviewKafkaConfig(
        bootstrap,
        username(worker),
        password(worker),
        KafkaSaslSecurityProtocol.Plaintext,
        worker,
        topics = interviewTopics,
        workerGroup = kafkaNamespace.workers,
        orchestratorGroup = kafkaNamespace.orchestrator
      )
    def fencer: Resource[IO, InterviewPublisherFencer] = InterviewProducerFencer.resource(
      bootstrap,
      "interview_fencer",
      values("KAFKA_INTERVIEW_FENCER_PASSWORD"),
      KafkaSaslSecurityProtocol.Plaintext,
      Diagnostics.noop
    )
  }

  private def broker: IO[Broker] = IO.delay(Broker(kafkaEnvironment))

  private def success[A](operation: RepositoryIO[A]): IO[A] = operation.value.flatMap {
    case Right(value) => IO.pure(value)
    case Left(error)  => IO.raiseError(new AssertionError(s"Repository failure: $error"))
  }

  private def properties(broker: Broker, username: String, password: String): Properties = {
    val values = new Properties()
    val _ = values.setProperty("bootstrap.servers", broker.bootstrap)
    val _ = values.setProperty("security.protocol", "SASL_PLAINTEXT")
    val _ = values.setProperty("sasl.mechanism", "PLAIN")
    val _ = values.setProperty(
      "sasl.jaas.config",
      s"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$username\" password=\"$password\";"
    )
    val _ = values.setProperty("request.timeout.ms", "10000")
    val _ = values.setProperty("default.api.timeout.ms", "15000")
    values
  }

  private def producer(
      broker: Broker,
      username: String,
      password: String,
      transactionalId: String
  ): Resource[IO, KafkaProducer[String, Array[Byte]]] =
    Resource.make(IO.blocking {
      val values = properties(broker, username, password)
      val _ = values.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId)
      val _ = values.setProperty(ProducerConfig.ACKS_CONFIG, "all")
      val _ = values.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
      val _ = values.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "15000")
      val _ = values.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000")
      new KafkaProducer[String, Array[Byte]](values, new StringSerializer(), new ByteArraySerializer())
    })(value => IO.blocking(value.close(Duration.ofSeconds(5))))

  private def initializedProducer(
      broker: Broker,
      generation: InterviewPublisherGeneration
  ): Resource[IO, KafkaProducer[String, Array[Byte]]] = {
    val worker = generation.role == InterviewPublisherRole.Worker
    producer(broker, broker.username(worker), broker.password(worker), generation.transactionalId)
      .evalTap(value => IO.blocking(value.initTransactions()))
  }

  private def causes(error: Throwable): List[Throwable] =
    error :: Option(error.getCause).filterNot(_ eq error).toList.flatMap(causes)
  private def fenced(error: Throwable): Boolean = causes(error).exists {
    case _: ProducerFencedException | _: InvalidProducerEpochException => true
    case _                                                             => false
  }
  private def denied(error: Throwable): Boolean = causes(error).exists {
    case _: AuthorizationException | _: AuthenticationException => true
    case _                                                      => false
  }

  private case class Scheduled(
      workflow: InterviewWorkflow,
      claim: ClaimedInterviewWorkflowCommand,
      message: InterviewMessage
  )
  private def scheduled(
      database: mongo4cats.database.MongoDatabase[IO],
      repository: MongoInterviewWorkflowRepository,
      result: Boolean = false
  ): IO[Scheduled] =
    for {
      now <- IO.realTimeInstant
      value = InterviewWorkflow
        .create(
          InterviewWorkflowId(UUID.randomUUID()),
          ApplicationId(UUID.randomUUID()),
          UserId(UUID.randomUUID()),
          UserId(UUID.randomUUID()),
          InterviewInterval(now.plusSeconds(172800), now.plusSeconds(176400)),
          now.plusSeconds(3600),
          UUID.randomUUID(),
          ApplicationStatus.Accepted
        )
        .fold(error => fail(s"Invalid fixture: $error"), identity)
      _ <- InterviewSchedulingFixtures.seed(database, List(value), now)
      _ <- success(
        repository.create(
          value,
          InterviewWorkflow.initialCommand(value),
          value.idempotencyKey,
          MutationReceiptFingerprint.fromCanonicalInput(s"fencing:${value.id.value}"),
          now
        )
      )
      claims <- success(repository.claimDueCommands("fencing-proof", now, now.plusSeconds(600), 1))
      initial <- IO.fromOption(claims.headOption)(new AssertionError("missing command claim"))
      claim <-
        if (!result) IO.pure(initial)
        else
          for {
            outcome <- success(repository.claimExecution(initial.record, "result-proof", now, now.plusSeconds(600), 3))
            executing <- outcome match {
              case InterviewExecutionClaimOutcome.Acquired(value) => IO.pure(value)
              case other => IO.raiseError(new AssertionError(s"Missing execution claim: $other"))
            }
            _ <- success(repository.recordResult(executing, InterviewCommandResult.Succeeded, now))
            results <- success(repository.claimDueCommands("result-fencing-proof", now, now.plusSeconds(600), 1))
            publishing <- IO.fromOption(results.headOption)(new AssertionError("missing result claim"))
          } yield publishing
      message = InterviewMessage(
        UUID.nameUUIDFromBytes(claim.record.stepId.getBytes(StandardCharsets.UTF_8)),
        value.id.value,
        claim.record.stepId,
        InterviewStep.Reserve,
        0L,
        value.id.value,
        value.preCommitDeadline,
        claim.record.result.map(InterviewResult.fromCommandResult),
        claim.record.occurredAt
      )
    } yield Scheduled(value, claim, message)

  private def delete(
      database: mongo4cats.database.MongoDatabase[IO],
      client: mongo4cats.client.MongoClient[IO],
      subject: UserId
  ): IO[Vector[String]] = {
    val users = new MongoUserRepository(
      database,
      MongoTransactionRunner.sessions(client, RepositoryError.Conflict, diagnostics = Diagnostics.noop),
      MongoEmbeddingWorkEnqueuer.disabled,
      Diagnostics.noop
    )
    val cleanup = new MongoInterviewSubjectCleanup(database, topics = interviewTopics)
    for {
      now <- IO.realTimeInstant
      _ <- success(users.deleteAccount(subject, now, "deleted-account", MutationWriteContext.directWrite))
      request <- success(cleanup.find(subject))
      _ <- IO.fromOption(request)(new AssertionError("account deletion did not enqueue cleanup"))
      ids <- success(cleanup.producerBatch(subject))
    } yield ids
  }

  test("account deletion fences an authorized paused sender; a fresh generation works only for an unaffected subject") {
    broker.flatMap { kafka =>
      List(false, true).traverse_ { worker =>
        mongoResource.use { fixture =>
          val repository = MongoInterviewWorkflowRepository.live(fixture.database, fixture.client, Diagnostics.noop)
          (
            InterviewKafkaRuntime.publisherResource(kafka.config(worker)),
            InterviewKafkaRuntime.publisherResource(kafka.config(worker)),
            kafka.fencer
          ).tupled.use { case (transport, peer, fencer) =>
            for {
              _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop, interviewTopics)
              item <- scheduled(fixture.database, repository, result = worker)
              authorized <- Deferred[IO, ClaimedInterviewWorkflowCommand]
              release <- Deferred[IO, Unit]
              result <- Resource
                .make((for {
                  at <- IO.realTimeInstant
                  firstPermission <- success(
                    repository.authorizePublication(item.claim, transport.generationFor(item.message), at)
                  )
                  _ = assert(firstPermission)
                  _ <- success(repository.retry(item.claim, at, at, "publication_unavailable"))
                  nextClaims <- success(repository.claimDueCommands("fencing-proof-retry", at, at.plusSeconds(600), 1))
                  nextClaim <- IO.fromOption(nextClaims.headOption)(new AssertionError("missing retry claim"))
                  _ = assert(
                    nextClaim.fencingToken != item.claim.fencingToken,
                    "a later attempt must own a fresh claim token"
                  )
                  _ = assertEquals(nextClaim.record.publicationAttempts, 1)
                  secondPermission <- success(
                    repository.authorizePublication(nextClaim, peer.generationFor(item.message), at)
                  )
                  _ = assert(secondPermission)
                  stored <- success(repository.findCommand(item.workflow.id, nextClaim.record.stepId))
                  currentRecord <- IO.fromOption(stored)(new AssertionError("missing authorized command"))
                  currentClaim = nextClaim.copy(record = currentRecord)
                  _ = assertEquals(currentClaim.record.publicationAttempts, 2)
                  renewed <- success(repository.renewPublication(currentClaim, at, at.plusSeconds(600)))
                  _ = assert(renewed, "replacement denial must use a currently owned publication claim")
                  _ <- authorized.complete(currentClaim)
                  _ <- release.get
                  sent <- Vector(transport, peer).traverse(_.publish(item.message).attempt)
                } yield sent).start)(_.cancel)
                .use { fiber =>
                  for {
                    currentClaim <- authorized.get.timeout(30.seconds)
                    ids <- delete(fixture.database, fixture.client, item.workflow.candidateId)
                    _ = assert(
                      Vector(transport, peer)
                        .forall(value => ids.contains(value.generationFor(item.message).transactionalId))
                    )
                    _ <- success(fencer.fence(ids))
                    _ <- release.complete(())
                    outcome <- fiber.joinWithNever.timeout(30.seconds)
                  } yield (outcome, currentClaim)
                }
              _ = assert(result._1.forall(_.left.exists(_.isInstanceOf[InterviewProducerGenerationFenced])))
              _ <- InterviewKafkaRuntime.publisherResource(kafka.config(worker)).use { replacement =>
                for {
                  at <- IO.realTimeInstant
                  refused <- repository
                    .authorizePublication(result._2, replacement.generationFor(item.message), at)
                    .value
                  _ = assert(
                    refused.fold(_ => true, allowed => !allowed),
                    "new generation must reauthorize deleted subject"
                  )
                  unrelated <- scheduled(fixture.database, repository, result = worker)
                  permitted <- success(
                    repository.authorizePublication(unrelated.claim, replacement.generationFor(unrelated.message), at)
                  )
                  _ = assert(permitted)
                  _ <- replacement.publish(unrelated.message)
                  _ <- success(repository.markPublished(unrelated.claim, at))
                } yield ()
              }
            } yield ()
          }
        }
      }
    }
  }

  private def assertInvisible(kafka: Broker, metadata: RecordMetadata, workflowId: UUID): IO[Unit] = {
    val consumer = Resource.make(IO.blocking {
      val values = properties(kafka, kafka.username(true), kafka.password(true))
      val _ = values.setProperty(ConsumerConfig.GROUP_ID_CONFIG, kafkaNamespace.workers)
      val _ = values.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
      val _ = values.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
      new KafkaConsumer[String, Array[Byte]](values, new StringDeserializer(), new ByteArrayDeserializer())
    })(value => IO.blocking(value.close(CloseOptions.timeout(Duration.ofSeconds(5)))))
    consumer.use { value =>
      val partition = new TopicPartition(metadata.topic(), metadata.partition())
      def poll(remaining: Int): IO[Unit] = IO
        .blocking {
          val records = value.poll(Duration.ofMillis(500)).iterator().asScala.toList
          assert(
            !records.exists(_.key() == workflowId.toString),
            "aborted transaction became visible to read_committed"
          )
          value.position(partition) > metadata.offset()
        }
        .flatMap {
          case true                   => IO.unit
          case false if remaining > 0 => poll(remaining - 1)
          case false                  =>
            IO.raiseError(
              new AssertionError("consumer did not advance beyond fenced transaction; absence is unverified")
            )
        }
      IO.blocking { value.assign(List(partition).asJava); value.seek(partition, metadata.offset()) } *> poll(60)
    }
  }

  test("deletion fences an already open transaction and its acknowledged record remains invisible") {
    broker.flatMap { kafka =>
      mongoResource.use { fixture =>
        val repository = MongoInterviewWorkflowRepository.live(fixture.database, fixture.client, Diagnostics.noop)
        val generation = InterviewPublisherGeneration(InterviewPublisherRole.Orchestrator, UUID.randomUUID())
        (initializedProducer(kafka, generation), kafka.fencer).tupled.use { case (sender, fencer) =>
          for {
            _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop, interviewTopics)
            item <- scheduled(fixture.database, repository)
            at <- IO.realTimeInstant
            permitted <- success(repository.authorizePublication(item.claim, generation, at))
            _ = assert(permitted)
            metadata <- IO.blocking {
              sender.beginTransaction()
              sender
                .send(
                  new ProducerRecord(
                    interviewTopics.commands,
                    item.workflow.id.value.toString,
                    InterviewMessageCodec.bytes(item.message)
                  )
                )
                .get()
            }
            ids <- delete(fixture.database, fixture.client, item.workflow.candidateId)
            _ <- success(fencer.fence(ids))
            committed <- IO.blocking(sender.commitTransaction()).attempt
            _ = assert(committed.left.exists(fenced), "broker must reject the old producer epoch commit")
            _ <- assertInvisible(kafka, metadata, item.workflow.id.value)
          } yield ()
        }
      }
    }
  }

  test("concurrent generation registration and real deletion either deny authorization or capture the generation") {
    broker.flatMap { kafka =>
      mongoResource.use { fixture =>
        val repository = MongoInterviewWorkflowRepository.live(fixture.database, fixture.client, Diagnostics.noop)
        (InterviewKafkaRuntime.publisherResource(kafka.config(false)), kafka.fencer).tupled.use {
          case (transport, fencer) =>
            for {
              _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop, interviewTopics)
              item <- scheduled(fixture.database, repository)
              start <- Deferred[IO, Unit]
              at <- IO.realTimeInstant
              raced <- Resource
                .make(
                  (start.get *> repository
                    .authorizePublication(item.claim, transport.generationFor(item.message), at)
                    .value).start
                )(_.cancel)
                .use { authorize =>
                  Resource
                    .make((start.get *> delete(fixture.database, fixture.client, item.workflow.candidateId)).start)(
                      _.cancel
                    )
                    .use { deletion =>
                      start.complete(()) *> (authorize.joinWithNever, deletion.joinWithNever).tupled
                    }
                }
              (authorization, ids) = raced
              _ = assert(
                authorization != Right(true) || ids.contains(transport.generationFor(item.message).transactionalId),
                "authorized generation escaped atomic deletion capture"
              )
              refused <- repository.authorizePublication(item.claim, transport.generationFor(item.message), at).value
              _ = assert(refused.fold(_ => true, allowed => !allowed))
              _ <- success(fencer.fence(ids))
            } yield ()
        }
      }
    }
  }

  test("broker enforces producer topic and transactional prefix ACLs and rejects retired credentials") {
    broker.flatMap { kafka =>
      val wrongPrefixes = List(false, true).traverse_ { worker =>
        val wrongRole = if (worker) InterviewPublisherRole.Orchestrator else InterviewPublisherRole.Worker
        val generation = InterviewPublisherGeneration(wrongRole, UUID.randomUUID())
        producer(kafka, kafka.username(worker), kafka.password(worker), generation.transactionalId).use { value =>
          IO.blocking(value.initTransactions()).attempt.map(result => assert(result.left.exists(denied)))
        }
      }
      val wrongTopics = List(false, true).traverse_ { worker =>
        val role = if (worker) InterviewPublisherRole.Worker else InterviewPublisherRole.Orchestrator
        initializedProducer(kafka, InterviewPublisherGeneration(role, UUID.randomUUID())).use { value =>
          val topic = if (worker) interviewTopics.commands else interviewTopics.results
          IO.blocking {
            value.beginTransaction()
            value.send(new ProducerRecord(topic, UUID.randomUUID().toString, Array[Byte](1))).get()
          }.attempt
            .flatMap(result =>
              IO(assert(result.left.exists(denied))) *>
                IO.blocking(value.abortTransaction()).attempt.void
            )
        }
      }
      val retired = List("interview_orchestrator", "interview_worker").traverse_ { username =>
        producer(
          kafka,
          username,
          kafka.password(username == "interview_worker"),
          s"hiring-interview-orchestrator-${UUID.randomUUID()}"
        ).use { value =>
          IO.blocking(value.initTransactions()).attempt.map(result => assert(result.left.exists(denied)))
        }
      }
      val fencerRestricted = Resource
        .make(
          IO.blocking(
            Admin.create(properties(kafka, "interview_fencer", kafka.values("KAFKA_INTERVIEW_FENCER_PASSWORD")))
          )
        )(value => IO.blocking(value.close(Duration.ofSeconds(5))))
        .use { value =>
          IO.blocking(value.fenceProducers(List(s"hiring-publisher-${UUID.randomUUID()}").asJava).all().get())
            .attempt
            .map(result =>
              assert(result.left.exists(denied), "interview fencer must not own operational publisher prefix")
            )
        }
      val fencerTopics = List(interviewTopics.commands, interviewTopics.results).traverse_ { topic =>
        producer(
          kafka,
          "interview_fencer",
          kafka.values("KAFKA_INTERVIEW_FENCER_PASSWORD"),
          s"hiring-interview-orchestrator-${UUID.randomUUID()}"
        ).use { value =>
          IO.blocking {
            value.initTransactions()
            value.beginTransaction()
            value.send(new ProducerRecord(topic, UUID.randomUUID().toString, Array[Byte](1))).get()
          }.attempt
            .flatMap(result =>
              IO(assert(result.left.exists(denied), "fencer must have no topic publication rights")) *>
                IO.blocking(value.abortTransaction()).attempt.void
            )
        }
      }
      wrongPrefixes *> wrongTopics *> retired *> fencerRestricted *> fencerTopics
    }
  }
}
