package com.example.graphQL.cats.acceptance

import cats.effect.{IO, Resource}
import com.example.graphQL.cats.repository.mongo.MongoDatabaseProbe
import com.example.hiring.testing.{DeterministicEmbeddingStub, StubTls}
import fs2.interop.reactivestreams.*
import java.nio.file.Path
import java.util.UUID
import org.bson.Document
import scala.concurrent.duration.*

/** The real `Main` started as a child JVM against a disposable Atlas Local deployment with search enabled, for the
  * search part of the hiring story (HEA-02, 06, 10).
  *
  * The embedding provider is a local deterministic stub reached through configuration only: the provider endpoint and a
  * placeholder API key are application settings, and the child JVM trusts the stub's throw-away certificate through its
  * own trust store options. No production code path is replaced. Kafka publication is off because search does not need
  * it. The nonce database is dropped when the resource is released, including on failure and cancellation.
  */
object HiringSearchAtlasStack {
  val DatabasePrefix = "hiring_acceptance_search_"

  final case class Running(
      baseUrl: String,
      database: String,
      client: GraphQLClient,
      stub: DeterministicEmbeddingStub
  )

  // Creating and syncing the Atlas Search and Vector Search indexes of a fresh database is the slow part of startup.
  private val readyDeadline = 300.seconds
  private val placeholderApiKey = "local-stub-key"

  private def configuration(
      atlasUri: String,
      database: String,
      port: Int,
      jwtSecret: String,
      providerEndpoint: String
  ): String = {
    import HiringStoryStack.hocon
    s"""include classpath("application.conf")
       |mongo.uri = ${hocon(atlasUri)}
       |mongo.database = ${hocon(database)}
       |mongo.reset-on-start = false
       |http.host = "127.0.0.1"
       |http.port = $port
       |logging.level = "INFO"
       |auth.jwt.hs256-secret = ${hocon(jwtSecret)}
       |auth.admin-seed.enabled = true
       |auth.admin-seed.name = ${hocon(HiringStoryStack.AdminName)}
       |auth.admin-seed.password = ${hocon(HiringStoryStack.AdminPassword)}
       |auth.rate-limit.attempts = 1000
       |graphql.cost-budget.max-cost = 1000000
       |graphql.schema-exposure = false
       |kafka.enabled = false
       |kafka.consumer.enabled = false
       |vector-search.enabled = true
       |vector-search.voyage.api-key = ${hocon(placeholderApiKey)}
       |vector-search.voyage.endpoint = ${hocon(providerEndpoint)}
       |""".stripMargin
  }

  def resource(atlasUri: String, workspace: Path): Resource[IO, Running] =
    for {
      tls <- StubTls.resource(workspace.resolve(".local/data/hiring-search-acceptance"))
      stub <- DeterministicEmbeddingStub.resource(tls)
      database = DatabasePrefix + UUID.randomUUID().toString.replace("-", "")
      // Registered before the API starts, so it is released after the API has stopped.
      _ <- disposableDatabase(atlasUri, database)
      root <- HiringStoryStack.runDirectory(workspace.toString)
      port <- Resource.eval(HiringStoryStack.freePort)
      jwtSecret = UUID.randomUUID().toString + UUID.randomUUID().toString
      baseUrl = s"http://127.0.0.1:$port"
      process <- HiringStoryStack.launch(
        workspace.toString,
        configuration(atlasUri, database, port, jwtSecret, stub.endpoint),
        root,
        tls.clientJvmOptions,
        "hiring-search-acceptance"
      )
      client <- GraphQLClient.resource(baseUrl)
      _ <- Resource.eval(HiringStoryStack.awaitReady(baseUrl, process, readyDeadline))
    } yield Running(baseUrl, database, client, stub)

  /** Drops only the nonce database this run created, whatever the outcome. */
  private def disposableDatabase(atlasUri: String, name: String): Resource[IO, Unit] =
    MongoDatabaseProbe.clientResource(atlasUri).flatMap { client =>
      Resource.make(IO.unit)(_ =>
        client
          .getDatabase(name)
          .flatMap(db =>
            IO.delay(db.underlying.runCommand(new Document("dropDatabase", 1), classOf[Document]))
              .flatMap(_.toStreamBuffered[IO](1).compile.drain)
          )
          .void
          .handleErrorWith(error =>
            IO.blocking(
              System.err.println(s"Could not drop disposable database $name: ${error.getClass.getSimpleName}")
            )
          )
      )
    }
}
