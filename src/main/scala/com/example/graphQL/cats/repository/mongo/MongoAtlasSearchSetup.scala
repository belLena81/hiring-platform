package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.mongodb.client.model.{SearchIndexModel, SearchIndexType}
import org.bson.Document
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

private[mongo] object MongoAtlasSearchSetup {
  def provision(database: MongoHiringSetup.SetupDatabase, config: AtlasSearchIndexConfig): IO[Unit] = {

    def vectorDefinition(filters: List[String]): Document = new Document(
      "fields",
      (new Document("type", "vector")
        .append("path", MongoFields.Embedding)
        .append("numDimensions", Int.box(config.dimension))
        .append("similarity", "cosine") ::
        filters.map(path => new Document("type", MongoFields.Filter).append("path", path))).asJava
    )
    val lexicalDefinition = new Document(
      "mappings",
      new Document("dynamic", false).append(
        "fields",
        new Document()
          .append(MongoFields.Title, new Document("type", "string"))
          .append(MongoFields.Description, new Document("type", "string"))
          .append(MongoFields.Requirements, new Document("type", "string"))
          .append(MongoFields.Skills, new Document("type", "string"))
      )
    )
    def create(collectionName: String, model: SearchIndexModel): IO[Unit] =
      MongoAtlasSearchAdmin.createIndex(database.underlying, collectionName, model)
    List(
      create(
        MongoCollections.Jobs,
        new SearchIndexModel(
          config.jobVectorIndex,
          vectorDefinition(
            List(
              MongoFields.Status,
              MongoFields.LocationCity,
              MongoFields.Skills,
              MongoFields.CreatedAt,
              MongoFields.RecruiterId,
              MongoFields.EmbeddingMetaModel
            )
          ),
          SearchIndexType.vectorSearch()
        )
      ),
      create(
        MongoCollections.Users,
        new SearchIndexModel(
          config.candidateVectorIndex,
          vectorDefinition(
            List(
              MongoFields.Role,
              MongoFields.AccountStatus,
              MongoFields.EmbeddingMetaModel,
              MongoFields.ProfileSkillsCanonical,
              MongoFields.ProfileRecruiterSearchOptIn,
              MongoFields.ProfileCurrentResidenceCountryCanonical,
              MongoFields.ProfileCurrentResidenceCityCanonical,
              MongoFields.ProfileAvailabilityStatus
            )
          ),
          SearchIndexType.vectorSearch()
        )
      ),
      create(
        MongoCollections.Users,
        new SearchIndexModel(
          config.candidateLexicalIndex,
          new Document(
            "mappings",
            new Document("dynamic", false).append(
              "fields",
              new Document(
                MongoFields.Profile,
                new Document("type", "document").append(
                  "fields",
                  new Document()
                    .append(MongoFields.Skills, new Document("type", "string"))
                    .append(MongoFields.ExperienceSummary, new Document("type", "string"))
                )
              )
            )
          ),
          SearchIndexType.search()
        )
      ),
      create(
        MongoCollections.Jobs,
        new SearchIndexModel(config.jobLexicalIndex, lexicalDefinition, SearchIndexType.search())
      )
    ).sequence_.void *> awaitSearchIndexes(database, config)
  }

  private def awaitSearchIndexes(
      database: MongoHiringSetup.SetupDatabase,
      config: AtlasSearchIndexConfig
  ): IO[Unit] = {
    val candidateVectorFilters = Set(
      MongoFields.Role,
      MongoFields.AccountStatus,
      MongoFields.EmbeddingMetaModel,
      MongoFields.ProfileSkillsCanonical,
      MongoFields.ProfileRecruiterSearchOptIn,
      MongoFields.ProfileCurrentResidenceCountryCanonical,
      MongoFields.ProfileCurrentResidenceCityCanonical,
      MongoFields.ProfileAvailabilityStatus
    )
    val deadline = IO.monotonic.map(_ + config.readyTimeoutMillis.millis)
    def indexDocuments(collectionName: String): IO[List[Document]] =
      MongoAtlasSearchAdmin.listIndexes(database.underlying, collectionName, 100)
    def validVector(index: Document, requiredFilters: Set[String]): Boolean = {
      val definition = Option(index.get("latestDefinition", classOf[Document]))
        .orElse(Option(index.get("definition", classOf[Document])))
      val fields = definition
        .flatMap(value => Option(value.get("fields", classOf[java.util.List[Document]])))
        .toList
        .flatMap(_.asScala)
      val filters =
        fields.filter(_.getString("type") == MongoFields.Filter).flatMap(field => Option(field.getString("path"))).toSet
      val vector = fields.filter(_.getString("type") == "vector")
      requiredFilters.subsetOf(filters) && vector.size == 1 && vector.headOption.exists(field =>
        field.getString("path") == MongoFields.Embedding &&
          Option(field.getInteger("numDimensions")).contains(config.dimension) &&
          field.getString("similarity") == "cosine"
      )
    }
    def validLexical(index: Document): Boolean = {
      val definition = Option(index.get("latestDefinition", classOf[Document]))
        .orElse(Option(index.get("definition", classOf[Document])))
      val mappings = definition.flatMap(value => Option(value.get("mappings", classOf[Document])))
      val profile = mappings.flatMap(value =>
        Option(value.get("fields", classOf[Document])).flatMap(fields =>
          Option(fields.get(MongoFields.Profile, classOf[Document]))
        )
      )
      val profileFields = profile.flatMap(value => Option(value.get("fields", classOf[Document])))
      def stringField(fields: Document, name: String): Boolean =
        Option(fields.get(name, classOf[Document])).exists(_.getString("type") == "string")
      mappings.exists(_.getBoolean("dynamic", true) == false) &&
      profile.exists(_.getString("type") == "document") &&
      profileFields.exists(fields =>
        stringField(fields, MongoFields.Skills) && stringField(fields, MongoFields.ExperienceSummary)
      )
    }
    def await(name: String, collectionName: String, validate: Document => Boolean): IO[Unit] =
      deadline.flatMap { until =>
        def poll: IO[Unit] = indexDocuments(collectionName).flatMap { indexes =>
          indexes.find(_.getString("name") == name) match {
            case Some(index) if !validate(index) =>
              IO.raiseError(new IllegalStateException(s"Atlas Search index '$name' has an incompatible definition"))
            case Some(index) if index.getBoolean("queryable", java.lang.Boolean.FALSE).booleanValue() => IO.unit
            case _                                                                                    =>
              IO.monotonic.flatMap(now =>
                if (now >= until)
                  IO.raiseError(new IllegalStateException(s"Atlas Search index '$name' is not queryable"))
                else IO.sleep(config.pollIntervalMillis.millis) *> poll
              )
          }
        }
        poll
      }
    val jobFilters = Set(
      MongoFields.Status,
      MongoFields.LocationCity,
      MongoFields.Skills,
      MongoFields.CreatedAt,
      MongoFields.RecruiterId,
      MongoFields.EmbeddingMetaModel
    )
    def validJobLexical(index: Document): Boolean = {
      val definition = Option(index.get("latestDefinition", classOf[Document]))
        .orElse(Option(index.get("definition", classOf[Document])))
      val mappings = definition.flatMap(value => Option(value.get("mappings", classOf[Document])))
      val fields = mappings.flatMap(value => Option(value.get("fields", classOf[Document])))
      mappings.exists(_.getBoolean("dynamic", true) == false) && fields.exists(value =>
        List(MongoFields.Title, MongoFields.Description, MongoFields.Requirements, MongoFields.Skills)
          .forall(name => Option(value.get(name, classOf[Document])).exists(_.getString("type") == "string"))
      )
    }
    await(config.jobVectorIndex, MongoCollections.Jobs, index => validVector(index, jobFilters)) *>
      await(config.jobLexicalIndex, MongoCollections.Jobs, validJobLexical) *>
      await(config.candidateVectorIndex, MongoCollections.Users, index => validVector(index, candidateVectorFilters)) *>
      await(config.candidateLexicalIndex, MongoCollections.Users, validLexical)
  }
}
