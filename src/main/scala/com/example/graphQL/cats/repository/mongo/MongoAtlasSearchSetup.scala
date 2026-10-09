package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.mongodb.client.model.{SearchIndexModel, SearchIndexType}
import org.bson.Document
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

private[mongo] object MongoAtlasSearchSetup {
  private def token: Document = new Document("type", "token").append("normalizer", "none")
  private def string: Document = new Document("type", "string")
  private def document(fields: Document): Document = new Document("type", "document").append("fields", fields)
  private def lexical(fields: Document): Document =
    new Document("mappings", new Document("dynamic", false).append("fields", fields))

  private[mongo] def jobLexicalDefinition: Document = lexical(
    new Document()
      .append(MongoFields.Title, string)
      .append(MongoFields.Description, string)
      .append(MongoFields.Requirements, string)
      .append(MongoFields.Skills, List(string, token).asJava)
      .append(MongoFields.Status, token)
      .append(MongoFields.CreatedAt, new Document("type", "date"))
      .append(MongoFields.Location, document(new Document(MongoFields.City, token)))
      .append(MongoFields.EmbeddingMeta, document(new Document(MongoFields.Model, token)))
  )

  private[mongo] def candidateLexicalDefinition: Document = lexical(
    new Document()
      .append(MongoFields.Role, token)
      .append(MongoFields.AccountStatus, token)
      .append(MongoFields.EmbeddingMeta, document(new Document(MongoFields.Model, token)))
      .append(
        MongoFields.Profile,
        document(
          new Document()
            .append(MongoFields.Skills, string)
            .append(MongoFields.ExperienceSummary, string)
            .append(MongoFields.SkillsCanonical, token)
            .append(MongoFields.RecruiterSearchOptIn, new Document("type", "boolean"))
            .append(MongoFields.AvailabilityStatus, token)
            .append(
              MongoFields.CurrentResidence,
              document(
                new Document()
                  .append(MongoFields.CountryCanonical, token)
                  .append(MongoFields.CityCanonical, token)
              )
            )
        )
      )
  )

  private[mongo] def validLexical(index: Document, expected: Document): Boolean = {
    val definition = Option(index.get("latestDefinition", classOf[Document]))
      .orElse(Option(index.get("definition", classOf[Document])))
    def includes(actual: Object, required: Object): Boolean = (actual, required) match {
      case (a: Document, r: Document) =>
        r.entrySet()
          .asScala
          .forall(entry => Option(a.get(entry.getKey)).exists(value => includes(value, entry.getValue)))
      case (a: java.util.List[?], r: java.util.List[?]) =>
        r.asScala.forall(requiredEntry => a.asScala.exists(includes(_, requiredEntry)))
      case (a, r) => a == r
    }
    definition.exists(value => includes(value, expected))
  }

  def provision(database: MongoHiringSetup.SetupDatabase, config: AtlasSearchIndexConfig): IO[Unit] = {

    def vectorDefinition(filters: List[String]): Document = new Document(
      "fields",
      (new Document("type", "vector")
        .append("path", MongoFields.Embedding)
        .append("numDimensions", Int.box(config.dimension))
        .append("similarity", "cosine") ::
        filters.map(path => new Document("type", MongoFields.Filter).append("path", path))).asJava
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
          candidateLexicalDefinition,
          SearchIndexType.search()
        )
      ),
      create(
        MongoCollections.Jobs,
        new SearchIndexModel(config.jobLexicalIndex, jobLexicalDefinition, SearchIndexType.search())
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
    def await(name: String, collectionName: String, validate: Document => Boolean): IO[Unit] =
      deadline.flatMap { until =>
        def poll: IO[Unit] = indexDocuments(collectionName).flatMap { indexes =>
          indexes.find(_.getString("name") == name) match {
            case Some(index) if !validate(index) =>
              IO.raiseError(MongoSetupError(s"Atlas Search index '$name' has an incompatible definition"))
            case Some(index) if index.getBoolean("queryable", java.lang.Boolean.FALSE).booleanValue() => IO.unit
            case _                                                                                    =>
              IO.monotonic.flatMap(now =>
                if (now >= until)
                  IO.raiseError(MongoSetupError(s"Atlas Search index '$name' is not queryable"))
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
    await(config.jobVectorIndex, MongoCollections.Jobs, index => validVector(index, jobFilters)) *>
      await(config.jobLexicalIndex, MongoCollections.Jobs, index => validLexical(index, jobLexicalDefinition)) *>
      await(config.candidateVectorIndex, MongoCollections.Users, index => validVector(index, candidateVectorFilters)) *>
      await(
        config.candidateLexicalIndex,
        MongoCollections.Users,
        index => validLexical(index, candidateLexicalDefinition)
      )
  }
}
