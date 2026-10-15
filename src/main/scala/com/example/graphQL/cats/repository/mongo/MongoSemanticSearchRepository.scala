package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.Identifiers.parse as parseIdentifier
import com.example.graphQL.cats.domain.search.SearchFusionStrategy
import com.example.graphQL.cats.service.port.{RepositoryF, SemanticSearchRepository}
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.search.*
import com.example.graphQL.cats.service.read.HiringReadScope
import com.example.graphQL.cats.service.Diagnostics
import com.mongodb.client.model.Filters
import mongo4cats.database.MongoDatabase
import mongo4cats.collection.MongoCollection
import org.bson.Document
import org.bson.conversions.Bson

import fs2.Stream
import scala.jdk.CollectionConverters.*

final class MongoSemanticSearchRepository(
    database: MongoDatabase[IO],
    jobVectorIndex: String,
    candidateVectorIndex: String,
    jobLexicalIndex: String,
    candidateLexicalIndex: String,
    numCandidates: Int,
    branchResultLimit: Int = 0,
    fusionStrategy: SearchFusionStrategy = SearchFusionStrategy.ApplicationRrf,
    rerankEnabled: Boolean = false,
    rerankModel: String = "rerank-2.5-lite",
    diagnostics: Diagnostics[IO],
    discoveryPolicy: Option[DiscoveryQueryPolicy] = None
) extends SemanticSearchRepository[IO] {

  private def admitted[A](query: RepositoryF[IO, A]): RepositoryF[IO, A] =
    discoveryPolicy.fold(RepositoryF.at[IO].fromEither[A](Left(RepositoryError.Unavailable)))(policy =>
      RepositoryF.fromFEither(
        policy.run(query.value).recover {
          case error if DiscoveryQueryPolicy.isUnavailable(error) => Left(RepositoryError.Unavailable)
        }
      )
    )

  /** Eligibility is not a search root: with a policy it shares the search admission and breaker; without one it still
    * runs, as before, with unavailability mapped in place.
    */
  private def eligibilityAdmitted[A](query: RepositoryF[IO, A]): RepositoryF[IO, A] =
    discoveryPolicy.fold(MongoSemanticSearchRepository.unmetered(query))(_ => admitted(query))

  private val branchLimit = if (branchResultLimit > 0) branchResultLimit else numCandidates

  private def aggregate(source: MongoCollection[IO, Document], pipeline: Seq[Document]): Stream[IO, Document] = {
    val query = source.aggregate[Document](pipeline)
    discoveryPolicy.fold(query)(policy => query.maxTime(policy.maxTime)).boundedStream(32)
  }

  private def collectWithin[A](stream: Stream[IO, A], maximum: Int): IO[List[A]] =
    MongoBoundedResults.collect(stream, maximum, MongoSetupError("Mongo result exceeded configured limit"))

  private def metadataFilter(meta: EmbeddingMeta): Bson = Filters.and(
    Filters.eq(MongoFields.EmbeddingMetaModel, meta.model),
    Filters.eq(s"${MongoFields.EmbeddingMeta}.${MongoFields.SourceHash}", meta.sourceHash),
    Filters.eq(s"${MongoFields.EmbeddingMeta}.${MongoFields.UpdatedAt}", meta.updatedAt.toDate)
  )

  override def authorizedJobEligibility(
      scope: HiringReadScope,
      ids: List[JobId],
      queryCandidate: Option[CandidateSearchEligibility]
  ): RepositoryF[IO, List[JobSearchEligibility]] = {
    val queryGate = queryCandidate.toList.flatMap { candidate =>
      (candidate.profile, candidate.metadata) match {
        case (Some(profile), Some(meta)) =>
          List(
            MongoAuthorizedReadQueries.matching(
              Filters.and(
                metadataFilter(meta),
                Filters.eq(MongoFields.ProfileSkills, profile.skills.toList.sorted.asJava),
                Filters.eq(MongoFields.ProfileExperienceSummary, profile.experienceSummary.orNull)
              )
            )
          )
        case _ => List(MongoAuthorizedReadQueries.matching(Filters.expr(new Document("$eq", List(1, 0).asJava))))
      }
    }
    authorizedEligibility(
      MongoCollections.Users,
      ids.map(_.value.toString),
      List(
        MongoAuthorizedReadQueries.matching(
          Filters.and(
            Filters.eq(MongoFields.Id, scope.userId.value.toString),
            Filters.eq(MongoFields.Role, UserRole.Candidate.toString)
          )
        )
      ) ++
        MongoAuthorizedReadQueries.actor(scope) ++ queryGate,
      MongoCollections.Jobs,
      MongoSearchEligibilityCodecs.jobFields
    )(MongoSearchEligibilityCodecs.job)
  }

  override def authorizedCandidateEligibility(
      scope: HiringReadScope,
      queryJob: JobSearchEligibility,
      ids: List[UserId]
  ): RepositoryF[IO, List[CandidateSearchEligibility]] = {
    val source = queryJob.job
    val embeddingGate =
      queryJob.metadata.map(metadataFilter).getOrElse(Filters.expr(new Document("$eq", List(1, 0).asJava)))
    val parentGate = Filters.and(
      Filters.eq(MongoFields.Id, source.id.value.toString),
      MongoAuthorizedReadQueries.managedJob(scope),
      Filters.eq(MongoFields.Status, JobStatus.Open.toString),
      embeddingGate,
      Filters.eq(MongoFields.Title, source.title),
      Filters.eq(MongoFields.Description, source.description),
      Filters.eq(MongoFields.Requirements, source.requirements.asJava),
      Filters.eq(MongoFields.Skills, source.skills.toList.sorted.asJava)
    )
    authorizedEligibility(
      MongoCollections.Jobs,
      ids.map(_.value.toString),
      List(MongoAuthorizedReadQueries.matching(parentGate)) ++ MongoAuthorizedReadQueries.actor(scope),
      MongoCollections.Users,
      MongoSearchEligibilityCodecs.candidateFields
    )(MongoSearchEligibilityCodecs.candidate)
  }

  private def authorizedEligibility[A](
      parentCollection: String,
      ids: List[String],
      parentGate: List[Document],
      selectedCollection: String,
      fields: List[String]
  )(decode: Document => Either[RepositoryError, A]): RepositoryF[IO, List[A]] = {
    val selected = ids.distinct
    if (selected.size > branchLimit) RepositoryF.at[IO].raise(RepositoryError.Unavailable)
    else if (selected.isEmpty) RepositoryF.at[IO].pure(Nil)
    else
      // Eligibility queries run on the same search deployment, so they share its admission and breaker.
      eligibilityAdmitted(MongoRepositorySupport.discoveryGuard(diagnostics, "semanticSearch.authorizedEligibility") {
        val child = List(
          MongoAuthorizedReadQueries.matching(Filters.in(MongoFields.Id, selected.asJava)),
          new Document("$limit", selected.size),
          new Document("$project", MongoSearchEligibilityCodecs.projection(fields))
        )
        val pipeline = parentGate ++ List(
          new Document(
            "$lookup",
            new Document("from", selectedCollection)
              .append("pipeline", child.asJava)
              .append("as", "eligibleHits")
          ),
          MongoAuthorizedReadQueries.unwind("eligibleHits"),
          MongoAuthorizedReadQueries.replace("eligibleHits")
        )
        RepositoryF
          .lift(Mongo4catsCollections.documents(database, parentCollection).flatMap { collection =>
            collectWithin(aggregate(collection, pipeline), selected.size)
          })
          .subflatMap(_.traverse(decode))
      })
  }

  override def searchJobs(query: VectorSearchQuery): RepositoryF[IO, List[JobRetrievalHit]] =
    admitted(rankedJobs(query, query.filter))

  override def recommendedJobs(query: VectorSearchQuery): RepositoryF[IO, List[JobRetrievalHit]] =
    admitted(rankedJobs(query, JobSearchFilter(None, Set.empty, None)))

  override def candidateMatches(query: VectorSearchQuery): RepositoryF[IO, List[CandidateRetrievalHit]] = {
    val filter = candidateFilter(query, includeEmbeddingModel = true)
    val vectorLimit = branchLimit
    val jobVector = candidateVectorSearch(query, query.vector, filter, vectorLimit)
    val result = query.candidateQueryVector match {
      case None                                                                       => jobVector
      case Some(queryVector) if fusionStrategy != SearchFusionStrategy.ApplicationRrf =>
        nativeCandidateFusion(query, queryVector, filter)
      case Some(queryVector) =>
        val queryVectorResults = candidateVectorSearch(query, queryVector, filter, branchLimit)
        val lexicalResults = candidateLexicalSearch(query)
        MongoSemanticSearchResult.fuseCandidates(jobVector, queryVectorResults, lexicalResults, branchLimit)
    }
    admitted(result)
  }

  private def candidateVectorSearch(
      query: VectorSearchQuery,
      vector: List[Float],
      filter: Bson,
      limit: Int
  ): RepositoryF[IO, List[CandidateRetrievalHit]] = {
    val pipeline = candidateVectorPipeline(vector, filter, limit)
    MongoRepositorySupport.discoveryGuard(diagnostics, "semanticSearch.candidateVector") {
      RepositoryF
        .lift(
          Mongo4catsCollections.documents(database, MongoCollections.Users).flatMap { users =>
            collectWithin(aggregate(users, pipeline.asScala.toSeq), limit)
          }
        )
        .subflatMap(documents => MongoSemanticSearchResult.candidateHits(documents, query).map(_.flatten))
    }
  }

  private def candidateLexicalSearch(
      query: VectorSearchQuery
  ): RepositoryF[IO, List[CandidateRetrievalHit]] = {
    val pipeline = candidateLexicalPipeline(query, candidateLexicalFilter(query))
    MongoRepositorySupport.discoveryGuard(diagnostics, "semanticSearch.candidateLexical") {
      RepositoryF
        .lift(
          Mongo4catsCollections.documents(database, MongoCollections.Users).flatMap { users =>
            collectWithin(aggregate(users, pipeline.asScala.toSeq), branchLimit)
          }
        )
        .subflatMap(documents => MongoSemanticSearchResult.candidateHits(documents, query).map(_.flatten))
    }
  }

  private[mongo] def candidateFilter(query: VectorSearchQuery, includeEmbeddingModel: Boolean): Bson =
    MongoSearchEligibilityRendering.operational(SearchEligibilityCriteria.candidates(query, includeEmbeddingModel))

  private[mongo] def retrievalProjection: Document = new Document(
    "$project",
    new Document(MongoFields.Id, 1)
      .append(MongoFields.EmbeddingMeta, 1)
      .append(MongoFields.Score, 1)
      .append(MongoFields.RetrievalScore, 1)
  )

  private def rankedJobs(
      query: VectorSearchQuery,
      filter: JobSearchFilter
  ): RepositoryF[IO, List[JobRetrievalHit]] = {
    val mongoFilter = jobFilter(query, filter)
    query.lexicalQuery.filter(_ => query.mode == SearchMode.HYBRID) match {
      case Some(text) if fusionStrategy != SearchFusionStrategy.ApplicationRrf =>
        nativeJobFusion(query, text, mongoFilter)
      case Some(text) =>
        val vector = vectorJobs(query, mongoFilter, branchLimit)
        val lexical = lexicalJobs(query, text)
        MongoSemanticSearchResult.fuseJobs(vector, lexical, branchLimit)
      case None => vectorJobs(query, mongoFilter, branchLimit)
    }
  }

  private def nativeJobFusion(
      query: VectorSearchQuery,
      text: String,
      filter: Bson
  ): RepositoryF[IO, List[JobRetrievalHit]] = {
    val stages = nativeJobFusionStages(query, text, filter)
    MongoRepositorySupport.discoveryGuard(diagnostics, "semanticSearch.nativeJobs") {
      RepositoryF
        .lift(
          Mongo4catsCollections.documents(database, MongoCollections.Jobs).flatMap { jobs =>
            collectWithin(aggregate(jobs, stages), branchLimit)
          }
        )
        .subflatMap(documents => MongoSemanticSearchResult.jobHits(documents, query).map(_.flatten))
    }
  }

  private[mongo] def nativeJobFusionStages(
      query: VectorSearchQuery,
      text: String,
      filter: Bson
  ): List[Document] = {
    val pipelines = new Document()
      .append("vector", List(vectorSearchStage(jobVectorIndex, query.vector, filter, branchLimit)).asJava)
      .append("lexical", lexicalJobStages(text, jobLexicalFilter(query, query.filter)))
    val rerank = Option.when(rerankEnabled)(
      MongoSearchFusionPipeline
        .rerankStage(
          rerankModel,
          text,
          List(MongoFields.Title, MongoFields.Description, MongoFields.Requirements, MongoFields.Skills),
          branchLimit
        )
    )
    MongoSearchFusionPipeline.rankedStages(
      fusionStrategy,
      pipelines,
      List("vector", "lexical"),
      branchLimit,
      rerank,
      Nil,
      scoreFieldStage
    ) ++ List(retrievalProjection)
  }

  private[mongo] def lexicalJobStages(text: String, filter: Document): java.util.List[Document] =
    List(
      lexicalSearchStage(
        jobLexicalIndex,
        text,
        List(MongoFields.Title, MongoFields.Description, MongoFields.Requirements, MongoFields.Skills),
        filter
      ),
      new Document("$limit", Int.box(branchLimit))
    ).asJava

  private def lexicalSearchStage(index: String, text: String, paths: List[String], filter: Document): Document =
    MongoLexicalSearch.stage(index, text, paths, filter)

  private[mongo] def jobLexicalFilter(query: VectorSearchQuery, filter: JobSearchFilter): Document =
    MongoSearchEligibilityRendering.indexed(SearchEligibilityCriteria.jobs(query.model, filter))

  private[mongo] def candidateLexicalFilter(query: VectorSearchQuery): Document =
    MongoSearchEligibilityRendering.indexed(SearchEligibilityCriteria.candidates(query, includeEmbeddingModel = true))

  private val scoreFieldStage = new Document("$set", new Document(MongoFields.Score, new Document("$meta", "score")))

  private def nativeCandidateFusion(
      query: VectorSearchQuery,
      queryVector: List[Float],
      filter: Bson
  ): RepositoryF[IO, List[CandidateRetrievalHit]] = {
    val pipeline = nativeCandidateFusionStages(query, queryVector, filter)
    MongoRepositorySupport.discoveryGuard(diagnostics, "semanticSearch.nativeCandidates") {
      RepositoryF
        .lift(
          Mongo4catsCollections.documents(database, MongoCollections.Users).flatMap { users =>
            collectWithin(
              aggregate(users, pipeline),
              branchLimit
            )
          }
        )
        .subflatMap(documents => MongoSemanticSearchResult.candidateHits(documents, query).map(_.flatten))
    }
  }

  private[mongo] def nativeCandidateFusionStages(
      query: VectorSearchQuery,
      queryVector: List[Float],
      filter: Bson
  ): List[Document] = {
    val pipelines = new Document()
      .append("jobVector", List(vectorSearchStage(candidateVectorIndex, query.vector, filter, branchLimit)).asJava)
      .append("queryVector", List(vectorSearchStage(candidateVectorIndex, queryVector, filter, branchLimit)).asJava)
      .append("lexical", candidateLexicalStages(query, candidateLexicalFilter(query)))
    val rerank = Option.when(rerankEnabled)(
      MongoSearchFusionPipeline.rerankStage(
        rerankModel,
        query.lexicalQuery.getOrElse(""),
        List(MongoFields.ProfileSkills, MongoFields.ProfileExperienceSummary),
        branchLimit
      )
    )
    val rerankPreparation =
      if (rerankEnabled)
        List(
          new Document(
            "$set",
            new Document(
              "profile.experienceSummary",
              new Document("$ifNull", List("$profile.experienceSummary", "").asJava)
            )
          )
        )
      else Nil
    MongoSearchFusionPipeline.rankedStages(
      fusionStrategy,
      pipelines,
      List("jobVector", "queryVector", "lexical"),
      branchLimit,
      rerank,
      rerankPreparation,
      scoreFieldStage
    ) ++ List(retrievalProjection)
  }

  private[mongo] def candidateLexicalStages(query: VectorSearchQuery, filter: Document): java.util.List[Document] =
    List(
      lexicalSearchStage(
        candidateLexicalIndex,
        query.lexicalQuery.getOrElse(""),
        List(MongoFields.ProfileSkills, MongoFields.ProfileExperienceSummary),
        filter
      ),
      new Document("$limit", Int.box(branchLimit))
    ).asJava

  private def vectorJobs(
      query: VectorSearchQuery,
      filter: Bson,
      limit: Int
  ): RepositoryF[IO, List[JobRetrievalHit]] = {
    val pipeline = jobVectorPipeline(query.vector, filter, limit)
    MongoRepositorySupport.discoveryGuard(diagnostics, "semanticSearch.vectorJobs") {
      RepositoryF
        .lift(
          Mongo4catsCollections.documents(database, MongoCollections.Jobs).flatMap { jobs =>
            collectWithin(aggregate(jobs, pipeline.asScala.toSeq), limit)
          }
        )
        .subflatMap(documents => MongoSemanticSearchResult.jobHits(documents, query).map(_.flatten))
    }
  }

  private[mongo] def jobVectorPipeline(vector: List[Float], filter: Bson, limit: Int): java.util.List[Document] =
    List(vectorSearchStage(jobVectorIndex, vector, filter, limit), scoreStage, retrievalProjection).asJava

  private[mongo] def candidateVectorPipeline(vector: List[Float], filter: Bson, limit: Int): java.util.List[Document] =
    List(
      vectorSearchStage(candidateVectorIndex, vector, filter, limit),
      scoreStage,
      retrievalProjection
    ).asJava

  private[mongo] def candidateLexicalPipeline(query: VectorSearchQuery, filter: Document): java.util.List[Document] =
    (candidateLexicalStages(query, filter).asScala.toList ++ List(
      new Document("$set", new Document(MongoFields.Score, new Document("$meta", "searchScore"))),
      retrievalProjection
    )).asJava

  private def lexicalJobs(
      query: VectorSearchQuery,
      text: String
  ): RepositoryF[IO, List[JobRetrievalHit]] = {
    val pipeline = (lexicalJobStages(text, jobLexicalFilter(query, query.filter)).asScala.toList ++ List(
      new Document("$set", new Document(MongoFields.Score, new Document("$meta", "searchScore"))),
      retrievalProjection
    )).asJava
    MongoRepositorySupport.discoveryGuard(diagnostics, "semanticSearch.lexicalJobs") {
      RepositoryF
        .lift(
          Mongo4catsCollections.documents(database, MongoCollections.Jobs).flatMap { jobs =>
            collectWithin(aggregate(jobs, pipeline.asScala.toSeq), branchLimit)
          }
        )
        .subflatMap(documents => MongoSemanticSearchResult.jobHits(documents, query).map(_.flatten))
    }
  }

  private[mongo] def jobFilter(query: VectorSearchQuery, filter: JobSearchFilter): Bson =
    MongoSearchEligibilityRendering.operational(SearchEligibilityCriteria.jobs(query.model, filter))

  private def vectorSearchStage(index: String, vector: List[Float], filter: Bson, limit: Int): Document =
    new Document(
      "$vectorSearch",
      new Document("index", index)
        .append("path", MongoFields.Embedding)
        .append("queryVector", vector.map(float => java.lang.Double.valueOf(float.toDouble)).asJava)
        .append("numCandidates", java.lang.Integer.valueOf(numCandidates))
        .append("limit", java.lang.Integer.valueOf(limit))
        .append("filter", filter)
    )

  private val scoreStage: Document =
    new Document("$set", new Document(MongoFields.Score, new Document("$meta", "vectorSearchScore")))
}

private[mongo] object MongoSearchEligibilityRendering {
  private def path(field: SearchEligibilityField): String = field match {
    case SearchEligibilityField.JobStatus              => MongoFields.Status
    case SearchEligibilityField.EmbeddingModel         => MongoFields.EmbeddingMetaModel
    case SearchEligibilityField.JobCity                => MongoFields.LocationCity
    case SearchEligibilityField.JobSkill               => MongoFields.Skills
    case SearchEligibilityField.JobCreatedAt           => MongoFields.CreatedAt
    case SearchEligibilityField.CandidateRole          => MongoFields.Role
    case SearchEligibilityField.CandidateAccountStatus => MongoFields.AccountStatus
    case SearchEligibilityField.CandidateSkill         => MongoFields.ProfileSkillsCanonical
    case SearchEligibilityField.SearchConsent          => MongoFields.ProfileRecruiterSearchOptIn
    case SearchEligibilityField.ResidenceCountry       => MongoFields.ProfileCurrentResidenceCountryCanonical
    case SearchEligibilityField.ResidenceCity          => MongoFields.ProfileCurrentResidenceCityCanonical
    case SearchEligibilityField.Availability           => MongoFields.ProfileAvailabilityStatus
  }
  private def literal(value: SearchEligibilityValue): Object = value match {
    case SearchEligibilityValue.Text(value) => value
    case SearchEligibilityValue.Flag(value) => java.lang.Boolean.valueOf(value)
  }
  def operational(criteria: SearchEligibilityCriteria): Bson = criteria match {
    case SearchEligibilityCriteria.Equal(field, value)   => Filters.eq(path(field), literal(value))
    case SearchEligibilityCriteria.AtLeast(field, value) => Filters.gte(path(field), value.toDate)
    case SearchEligibilityCriteria.Missing(field)        => Filters.exists(path(field), false)
    case SearchEligibilityCriteria.All(values)           => Filters.and(values.map(operational)*)
    case SearchEligibilityCriteria.AnyOf(values)         => Filters.or(values.map(operational)*)
  }
  def indexed(criteria: SearchEligibilityCriteria): Document = criteria match {
    case SearchEligibilityCriteria.Equal(field, value) =>
      new Document("equals", new Document("path", path(field)).append("value", literal(value)))
    case SearchEligibilityCriteria.AtLeast(field, value) =>
      new Document("range", new Document("path", path(field)).append("gte", value.toDate))
    case SearchEligibilityCriteria.Missing(field) =>
      new Document(
        "compound",
        new Document("mustNot", List(new Document("exists", new Document("path", path(field)))).asJava)
      )
    case SearchEligibilityCriteria.All(values) =>
      new Document("compound", new Document("filter", values.map(indexed).asJava))
    case SearchEligibilityCriteria.AnyOf(values) =>
      new Document("compound", new Document("should", values.map(indexed).asJava).append("minimumShouldMatch", 1))
  }
}

private[mongo] object MongoSearchFusionPipeline {
  def fusionStage(strategy: SearchFusionStrategy, pipelines: Document, names: List[String]): Document = {
    val weights = names.foldLeft(new Document())((value, name) => value.append(name, 1.0d))
    val input = new Document("pipelines", pipelines)
    strategy match {
      case SearchFusionStrategy.MongoRankFusion =>
        new Document(
          "$rankFusion",
          new Document("input", input)
            .append("combination", new Document("weights", weights))
        )
      case SearchFusionStrategy.MongoScoreFusion =>
        input.append("normalization", "sigmoid")
        new Document(
          "$scoreFusion",
          new Document("input", input)
            .append("combination", new Document("weights", weights).append("method", "avg"))
        )
      case SearchFusionStrategy.ApplicationRrf => new Document()
    }
  }

  def rerankStage(model: String, query: String, paths: List[String], limit: Int): Document =
    new Document(
      "$rerank",
      new Document("query", new Document("text", query))
        .append("path", paths.asJava)
        .append("numDocsToRerank", Int.box(limit))
        .append("model", model)
    )

  def rankedStages(
      strategy: SearchFusionStrategy,
      pipelines: Document,
      names: List[String],
      limit: Int,
      rerank: Option[Document],
      rerankPreparation: List[Document],
      scoreStage: Document
  ): List[Document] = {
    val fusion = fusionStage(strategy, pipelines, names)
    rerank match {
      case Some(stage) =>
        List(
          fusion,
          new Document("$set", new Document(MongoFields.RetrievalScore, new Document("$meta", "score"))),
          new Document("$sort", new Document(MongoFields.RetrievalScore, -1).append(MongoFields.Id, 1)),
          new Document("$limit", Int.box(limit))
        ) ++ rerankPreparation ++ List(stage, scoreStage)
      case None =>
        List(
          fusion,
          scoreStage,
          new Document("$sort", new Document(MongoFields.Score, -1).append(MongoFields.Id, 1)),
          new Document("$limit", Int.box(limit))
        )
    }
  }
}

private[mongo] object MongoSemanticSearchResult {
  def fuseJobs(
      vector: RepositoryF[IO, List[JobRetrievalHit]],
      lexical: RepositoryF[IO, List[JobRetrievalHit]],
      limit: Int
  ): RepositoryF[IO, List[JobRetrievalHit]] =
    (vector, lexical)
      .parMapN((vectorHits, lexicalHits) =>
        HybridRankFusion.retrieval(List(vectorHits, lexicalHits), limit)(_.value.toString)
      )
      .leftMap(_ => RepositoryError.Unavailable)

  def fuseCandidates(
      jobVector: RepositoryF[IO, List[CandidateRetrievalHit]],
      queryVector: RepositoryF[IO, List[CandidateRetrievalHit]],
      lexical: RepositoryF[IO, List[CandidateRetrievalHit]],
      limit: Int
  ): RepositoryF[IO, List[CandidateRetrievalHit]] =
    (jobVector, queryVector, lexical)
      .parMapN((jobs, text, lexicalHits) =>
        HybridRankFusion.retrieval(List(jobs, text, lexicalHits), limit)(_.value.toString)
      )
      .leftMap(_ => RepositoryError.Unavailable)

  def jobHits(
      documents: List[Document],
      query: VectorSearchQuery
  ): Either[RepositoryError, List[Option[JobRetrievalHit]]] =
    documents.traverse(jobHit(_, query))

  def candidateHits(
      documents: List[Document],
      query: VectorSearchQuery
  ): Either[RepositoryError, List[Option[CandidateRetrievalHit]]] =
    documents.traverse(candidateHit(_, query))

  def jobHit(document: Document, query: VectorSearchQuery): Either[RepositoryError, Option[JobRetrievalHit]] =
    hit(document, query)(JobId.apply)

  def candidateHit(
      document: Document,
      query: VectorSearchQuery
  ): Either[RepositoryError, Option[CandidateRetrievalHit]] =
    hit(document, query)(UserId.apply)

  private def hit[Id](document: Document, query: VectorSearchQuery)(
      identify: java.util.UUID => Id
  ): Either[RepositoryError, Option[SearchRetrievalHit[Id]]] =
    for {
      raw <- Either.catchNonFatal(document.getString(MongoFields.Id)).leftMap(_ => RepositoryError.InvalidStoredData)
      id <- parseIdentifier(raw)(identify).toRight(RepositoryError.InvalidStoredData)
      score <- readScore(document, MongoFields.Score, required = true)
      retrievalScore <- readScore(document, MongoFields.RetrievalScore, required = false)
      metadata <- MongoSearchEligibilityCodecs.metadata(document)
    } yield metadata
      .filter(_.model == query.model)
      .map(meta => SearchRetrievalHit(id, score.getOrElse(0d), query.mode, meta, query.searchId, retrievalScore))

  private def readScore(
      document: Document,
      field: String,
      required: Boolean
  ): Either[RepositoryError, Option[Double]] =
    Option(document.get(field)) match {
      case None if required                                    => Left(RepositoryError.InvalidStoredData)
      case None                                                => Right(None)
      case Some(value: Number) if value.doubleValue().isFinite => Right(Some(value.doubleValue()))
      case Some(_)                                             => Left(RepositoryError.InvalidStoredData)
    }
}

/** The one `$search` shape for lexical retrieval; query text is only ever the value of `text.query` on fixed paths. */
private[mongo] object MongoLexicalSearch {
  def stage(index: String, text: String, paths: List[String], filter: Document): Document =
    new Document(
      "$search",
      new Document("index", index).append(
        "compound",
        new Document(
          "must",
          List(new Document("text", new Document("query", text).append("path", paths.asJava))).asJava
        )
          .append("filter", List(filter).asJava)
      )
    )
}

private[mongo] object MongoSemanticSearchRepository {

  /** Runs a discovery-guarded query without admission control, mapping unreachable-deployment failures to
    * `Unavailable`.
    */
  def unmetered[A](query: RepositoryF[IO, A]): RepositoryF[IO, A] =
    RepositoryF.fromFEither(query.value.recover {
      case error if DiscoveryQueryPolicy.isUnavailable(error) => Left(RepositoryError.Unavailable)
    })
}
