package com.example.graphQL.cats.runtime

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.config.VectorSearchConfig
import com.example.graphQL.cats.repository.protocol.EmbeddingService
import com.example.graphQL.cats.service.search.EmbeddingWorkPublisher

private[runtime] sealed trait EmbeddingCapability[Work, Users, Jobs, Search] {
  def users: Users
  def jobs: Jobs
}

private[runtime] object EmbeddingCapability {
  final case class Disabled[Work, Users, Jobs, Search](users: Users, jobs: Jobs)
      extends EmbeddingCapability[Work, Users, Jobs, Search]

  final case class Enabled[Work, Users, Jobs, Search](
      work: Work,
      users: Users,
      jobs: Jobs,
      search: Search,
      embeddings: EmbeddingService,
      publisher: EmbeddingWorkPublisher,
      model: String
  ) extends EmbeddingCapability[Work, Users, Jobs, Search]

  def resource[Work, Users, Jobs, Search](
      config: VectorSearchConfig,
      makeWork: IO[Work],
      makeUsers: Option[Work] => IO[Users],
      makeJobs: Option[Work] => IO[Jobs],
      makeSearch: IO[Search],
      makeEmbeddings: (VectorSearchConfig, String) => Resource[IO, EmbeddingService],
      makePipeline: (Work, Users, Jobs, EmbeddingService) => Resource[IO, EmbeddingWorkPublisher]
  ): Resource[IO, EmbeddingCapability[Work, Users, Jobs, Search]] =
    if (!config.enabled) {
      Resource.eval((makeUsers(None), makeJobs(None)).mapN(Disabled(_, _)))
    } else {
      for {
        apiKey <- Resource.eval(
          IO.fromOption(config.voyageApiKey)(
            new IllegalArgumentException("VOYAGE_API_KEY is required when vector search is enabled")
          )
        )
        work <- Resource.eval(makeWork)
        users <- Resource.eval(makeUsers(Some(work)))
        jobs <- Resource.eval(makeJobs(Some(work)))
        search <- Resource.eval(makeSearch)
        embeddings <- makeEmbeddings(config, apiKey)
        publisher <- makePipeline(work, users, jobs, embeddings)
      } yield Enabled(work, users, jobs, search, embeddings, publisher, config.voyageModel)
    }
}
