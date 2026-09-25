package com.example.graphQL.cats.repository.mongo

import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.repository.protocol.{RepositoryError, SemanticSearchRepository}
import com.example.graphQL.cats.shared.crypto.SourceHash
import com.example.graphQL.cats.shared.search.*
import com.mongodb.client.model.Filters
import com.mongodb.reactivestreams.client.MongoDatabase
import org.bson.Document
import org.bson.conversions.Bson

import scala.jdk.CollectionConverters.*

final class MongoSemanticSearchRepository(
    database: MongoDatabase,
    jobVectorIndex: String,
    candidateVectorIndex: String,
    jobLexicalIndex: String,
    candidateLexicalIndex: String,
    numCandidates: Int,
    fusionStrategy: SearchFusionStrategy = SearchFusionStrategy.ApplicationRrf,
    rerankEnabled: Boolean = false,
    rerankModel: String = "rerank-2.5-lite"
) extends SemanticSearchRepository {
  private lazy val jobs = database.getCollection("jobs")
  private lazy val users = database.getCollection("users")

  override def searchJobs(query: VectorSearchQuery): IO[Either[RepositoryError, List[RankedJob]]] =
    rankedJobs(query, query.filter)

  override def recommendedJobs(query: VectorSearchQuery): IO[Either[RepositoryError, List[RankedJob]]] =
    rankedJobs(query, JobSearchFilter(None, Set.empty, None))

  override def candidateMatches(query: VectorSearchQuery): IO[Either[RepositoryError, List[RankedCandidate]]] = {
    val filter = candidateFilter(query, includeEmbeddingModel = true)
    val jobVector = candidateVectorSearch(query, query.vector, filter)
    query.candidateQueryVector match {
      case None                                                                       => jobVector
      case Some(queryVector) if fusionStrategy != SearchFusionStrategy.ApplicationRrf =>
        nativeCandidateFusion(query, queryVector, filter)
      case Some(queryVector) =>
        val queryVectorResults = candidateVectorSearch(query, queryVector, filter)
        val lexicalResults = candidateLexicalSearch(query, filter)
        (jobVector, queryVectorResults, lexicalResults).parMapN { (jobHits, queryHits, lexicalHits) =>
          (jobHits, queryHits, lexicalHits) match {
            case (Right(jobs), Right(text), Right(lexical)) =>
              Right(HybridRankFusion.candidates(jobs, text, lexical, query.first.value))
            case _ => Left(RepositoryError.Unavailable)
          }
        }
    }
  }

  private def candidateVectorSearch(
      query: VectorSearchQuery,
      vector: List[Float],
      filter: Bson
  ): IO[Either[RepositoryError, List[RankedCandidate]]] = {
    val pipeline = candidateVectorPipeline(vector, filter)
    PublisherBridge
      .collectWithin(users.aggregate(pipeline), numCandidates)
      .map { documents =>
        MongoSemanticSearchResult.rankedCandidates(documents, query).map(_.flatten)
      }
      .handleError(_ => Left(RepositoryError.Unavailable))
  }

  private def candidateLexicalSearch(
      query: VectorSearchQuery,
      filter: Bson
  ): IO[Either[RepositoryError, List[RankedCandidate]]] = {
    val pipeline = candidateLexicalPipeline(query, filter)
    PublisherBridge
      .collectWithin(users.aggregate(pipeline), numCandidates)
      .map { documents =>
        MongoSemanticSearchResult.rankedCandidates(documents, query).map(_.flatten)
      }
      .handleError(_ => Left(RepositoryError.Unavailable))
  }

  private[mongo] def candidateFilter(query: VectorSearchQuery, includeEmbeddingModel: Boolean): Bson = {
    val filters = List(
      Some(Filters.eq("role", UserRole.Candidate.toString)),
      Some(Filters.eq("accountStatus", AccountStatus.Active.toString)),
      Option.when(includeEmbeddingModel)(Filters.eq("embeddingMeta.model", query.model)),
      Option.when(query.candidateFilters.requiredSkills.nonEmpty)(
        Filters.and(
          query.candidateFilters.requiredSkills
            .map(_.trim.toLowerCase(java.util.Locale.ROOT))
            .distinct
            .sorted
            .map(Filters.eq("profile.skillsCanonical", _))*
        )
      )
    ).flatten ++ privateCandidateFilters(query.candidateFilters)
    Filters.and(filters*)
  }

  private def privateCandidateFilters(filters: CandidateMatchFilters): List[Bson] = {
    val requested = List(
      filters.countryCanonical.map(value => Filters.eq("profile.currentResidence.countryCanonical", value)),
      filters.cityCanonical.map(value => Filters.eq("profile.currentResidence.cityCanonical", value)),
      filters.availabilityStatus.map(value => Filters.eq("profile.availabilityStatus", value))
    ).flatten
    Option
      .when(requested.nonEmpty)(
        Filters.or(
          Filters.eq("profile.recruiterSearchOptIn", false),
          Filters.exists("profile.recruiterSearchOptIn", false),
          Filters.and((Filters.eq("profile.recruiterSearchOptIn", true) :: requested)*)
        )
      )
      .toList
  }

  private val candidateProjection = new Document(
    "$project",
    new Document("_id", 1)
      .append("name", 1)
      .append("profile.skills", 1)
      .append("profile.experienceSummary", 1)
      .append("embeddingMeta", 1)
      .append("score", 1)
      .append("retrievalScore", 1)
  )

  private def rankedJobs(
      query: VectorSearchQuery,
      filter: JobSearchFilter
  ): IO[Either[RepositoryError, List[RankedJob]]] = {
    val mongoFilter = jobFilter(query, filter)
    query.lexicalQuery.filter(_ => query.mode == SearchMode.HYBRID) match {
      case Some(text) if fusionStrategy != SearchFusionStrategy.ApplicationRrf =>
        nativeJobFusion(query, text, mongoFilter)
      case Some(text) =>
        val vector = vectorJobs(query, mongoFilter, numCandidates)
        val lexical = lexicalJobs(query, text, mongoFilter)
        (vector, lexical).parMapN { (vectorResults, lexicalResults) =>
          (vectorResults, lexicalResults) match {
            case (Right(vectorHits), Right(lexicalHits)) =>
              Right(HybridRankFusion.jobs(vectorHits, lexicalHits, query.first.value))
            case _ => Left(RepositoryError.Unavailable)
          }
        }
      case None => vectorJobs(query, mongoFilter, query.first.value)
    }
  }

  private def nativeJobFusion(
      query: VectorSearchQuery,
      text: String,
      filter: Bson
  ): IO[Either[RepositoryError, List[RankedJob]]] = {
    val stages = nativeJobFusionStages(query, text, filter)
    PublisherBridge
      .collectWithin(jobs.aggregate(stages.asJava), query.first.value)
      .map { documents =>
        MongoSemanticSearchResult.rankedJobs(documents, query).map(_.flatten)
      }
      .handleError(_ => Left(RepositoryError.Unavailable))
  }

  private[mongo] def nativeJobFusionStages(
      query: VectorSearchQuery,
      text: String,
      filter: Bson
  ): List[Document] = {
    val pipelines = new Document()
      .append("vector", List(vectorSearchStage(jobVectorIndex, query.vector, filter, numCandidates)).asJava)
      .append("lexical", lexicalJobStages(text, filter))
    val rerank = Option.when(rerankEnabled)(
      MongoSearchFusionPipeline
        .rerankStage(rerankModel, text, List("title", "description", "requirements", "skills"), query.first.value)
    )
    MongoSearchFusionPipeline.rankedStages(
      fusionStrategy,
      pipelines,
      List("vector", "lexical"),
      query.first.value,
      rerank,
      Nil,
      scoreFieldStage
    )
  }

  private[mongo] def lexicalJobStages(text: String, filter: Bson): java.util.List[Document] = {
    val search = new Document("index", jobLexicalIndex).append(
      "text",
      new Document("query", text).append("path", List("title", "description", "requirements", "skills").asJava)
    )
    List(
      new Document("$search", search),
      new Document("$match", filter),
      new Document("$limit", Int.box(numCandidates))
    ).asJava
  }

  private val scoreFieldStage = new Document("$set", new Document("score", new Document("$meta", "score")))

  private def nativeCandidateFusion(
      query: VectorSearchQuery,
      queryVector: List[Float],
      filter: Bson
  ): IO[Either[RepositoryError, List[RankedCandidate]]] = {
    val pipeline = nativeCandidateFusionStages(query, queryVector, filter)
    PublisherBridge
      .collectWithin(users.aggregate(pipeline.asJava), query.first.value)
      .map { documents =>
        MongoSemanticSearchResult.rankedCandidates(documents, query).map(_.flatten)
      }
      .handleError(_ => Left(RepositoryError.Unavailable))
  }

  private[mongo] def nativeCandidateFusionStages(
      query: VectorSearchQuery,
      queryVector: List[Float],
      filter: Bson
  ): List[Document] = {
    val pipelines = new Document()
      .append("jobVector", List(vectorSearchStage(candidateVectorIndex, query.vector, filter, numCandidates)).asJava)
      .append("queryVector", List(vectorSearchStage(candidateVectorIndex, queryVector, filter, numCandidates)).asJava)
      .append("lexical", candidateLexicalStages(query, filter))
    val rerank = Option.when(rerankEnabled)(
      MongoSearchFusionPipeline.rerankStage(
        rerankModel,
        query.lexicalQuery.getOrElse(""),
        List("profile.skills", "profile.experienceSummary"),
        query.first.value
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
      query.first.value,
      rerank,
      rerankPreparation,
      scoreFieldStage
    ) ++ List(candidateProjection)
  }

  private[mongo] def candidateLexicalStages(query: VectorSearchQuery, filter: Bson): java.util.List[Document] = {
    val search = new Document("index", candidateLexicalIndex)
      .append(
        "text",
        new Document("query", query.lexicalQuery.getOrElse(""))
          .append("path", List("profile.skills", "profile.experienceSummary").asJava)
      )
    List(
      new Document("$search", search),
      new Document("$match", filter),
      new Document("$limit", Int.box(numCandidates))
    ).asJava
  }

  private def vectorJobs(
      query: VectorSearchQuery,
      filter: Bson,
      limit: Int
  ): IO[Either[RepositoryError, List[RankedJob]]] = {
    val pipeline = jobVectorPipeline(query.vector, filter, limit)
    PublisherBridge
      .collectWithin(jobs.aggregate(pipeline), limit)
      .map { documents =>
        MongoSemanticSearchResult.rankedJobs(documents, query).map(_.flatten)
      }
      .handleError(_ => Left(RepositoryError.Unavailable))
  }

  private[mongo] def jobVectorPipeline(vector: List[Float], filter: Bson, limit: Int): java.util.List[Document] =
    List(vectorSearchStage(jobVectorIndex, vector, filter, limit), scoreStage).asJava

  private[mongo] def candidateVectorPipeline(vector: List[Float], filter: Bson): java.util.List[Document] =
    List(
      vectorSearchStage(candidateVectorIndex, vector, filter, numCandidates),
      scoreStage,
      candidateProjection
    ).asJava

  private[mongo] def candidateLexicalPipeline(query: VectorSearchQuery, filter: Bson): java.util.List[Document] = {
    val search = new Document("index", candidateLexicalIndex)
      .append(
        "text",
        new Document("query", query.lexicalQuery.getOrElse(""))
          .append("path", List("profile.skills", "profile.experienceSummary").asJava)
      )
    List(
      new Document("$search", search),
      new Document("$match", filter),
      new Document("$limit", java.lang.Integer.valueOf(numCandidates)),
      new Document("$set", new Document("score", new Document("$meta", "searchScore"))),
      candidateProjection
    ).asJava
  }

  private def lexicalJobs(
      query: VectorSearchQuery,
      text: String,
      filter: Bson
  ): IO[Either[RepositoryError, List[RankedJob]]] = {
    val search = new Document("index", jobLexicalIndex)
      .append(
        "text",
        new Document("query", text)
          .append("path", List("title", "description", "requirements", "skills").asJava)
      )
    val pipeline = List(
      new Document("$search", search),
      new Document("$match", filter),
      new Document("$limit", java.lang.Integer.valueOf(numCandidates)),
      new Document("$set", new Document("score", new Document("$meta", "searchScore")))
    ).asJava
    PublisherBridge
      .collectWithin(jobs.aggregate(pipeline), numCandidates)
      .map { documents =>
        MongoSemanticSearchResult.rankedJobs(documents, query).map(_.flatten)
      }
      .handleError(_ => Left(RepositoryError.Unavailable))
  }

  private[mongo] def jobFilter(query: VectorSearchQuery, filter: JobSearchFilter): Bson =
    MongoKeysetPaging.filter(
      List(
        Some(Filters.eq("status", JobStatus.Open.toString)),
        Some(Filters.eq("embeddingMeta.model", query.model)),
        filter.city.map(city => Filters.eq("location.city", city)),
        skillsFilter(filter.skills),
        filter.createdAfter.map(createdAfter => Filters.gte("createdAt", java.util.Date.from(createdAfter)))
      )
    )

  private def skillsFilter(skills: Set[String]): Option[Bson] =
    Option.when(skills.nonEmpty)(Filters.and(skills.toList.sorted.map(skill => Filters.eq("skills", skill))*))

  private def vectorSearchStage(index: String, vector: List[Float], filter: Bson, limit: Int): Document =
    new Document(
      "$vectorSearch",
      new Document("index", index)
        .append("path", "embedding")
        .append("queryVector", vector.map(float => java.lang.Double.valueOf(float.toDouble)).asJava)
        .append("numCandidates", java.lang.Integer.valueOf(numCandidates))
        .append("limit", java.lang.Integer.valueOf(limit))
        .append("filter", filter)
    )

  private val scoreStage: Document =
    new Document("$set", new Document("score", new Document("$meta", "vectorSearchScore")))
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
          new Document("$set", new Document("retrievalScore", new Document("$meta", "score"))),
          new Document("$sort", new Document("retrievalScore", -1).append("_id", 1)),
          new Document("$limit", Int.box(limit))
        ) ++ rerankPreparation ++ List(stage, scoreStage)
      case None =>
        List(
          fusion,
          scoreStage,
          new Document("$sort", new Document("score", -1).append("_id", 1)),
          new Document("$limit", Int.box(limit))
        )
    }
  }
}

private[mongo] object MongoSemanticSearchResult {
  def rankedJobs(
      documents: List[Document],
      query: VectorSearchQuery
  ): Either[RepositoryError, List[Option[RankedJob]]] =
    documents.traverse(rankedJob(_, query)).leftMap(_ => RepositoryError.Unavailable)

  def rankedCandidates(
      documents: List[Document],
      query: VectorSearchQuery
  ): Either[RepositoryError, List[Option[RankedCandidate]]] =
    documents.traverse(rankedCandidate(_, query))

  def rankedJob(
      document: Document,
      query: VectorSearchQuery
  ): Either[NonEmptyList[MongoHiringCodecs.StoredDocumentError], Option[RankedJob]] =
    val stored = new Document(document)
    stored.remove("score")
    stored.remove("retrievalScore")
    MongoHiringCodecs.readJob(stored).toEither.map { job =>
      for {
        embedding <- job.embedding
        if embedding.meta.model == query.model
        if embedding.meta.sourceHash == SourceHash.sha256(SearchableText.job(job))
        score <- Option(document.get("score")).collect { case value: Number => value }
      } yield RankedJob(
        job,
        score.doubleValue,
        query.mode,
        embedding.meta,
        query.searchId,
        retrievalScore = Option(document.get("retrievalScore")).collect { case value: Number => value.doubleValue }
      )
    }

  def rankedCandidate(
      document: Document,
      query: VectorSearchQuery
  ): Either[RepositoryError, Option[RankedCandidate]] = Either
    .catchNonFatal {
      val id = UserId(java.util.UUID.fromString(document.getString("_id")))
      val name = document.getString("name")
      val profile = document.get("profile", classOf[Document])
      val skills = profile.get("skills", classOf[java.util.List[String]]).asScala.toSet
      val summary = Option(profile.get("experienceSummary")).collect { case value: String => value }
      val storedMeta = document.get("embeddingMeta", classOf[Document])
      val model = storedMeta.getString("model")
      val sourceHash = storedMeta.getString("sourceHash")
      val updatedAt = storedMeta.getDate("updatedAt").toInstant
      val score = document.get("score").asInstanceOf[Number].doubleValue()
      val profileValue = CandidateProfile(skills, summary, None)
      val meta = EmbeddingMeta(model, sourceHash, updatedAt)
      Option.when(model == query.model && sourceHash == SourceHash.sha256(SearchableText.candidate(profileValue))) {
        RankedCandidate(
          CandidateSearchHit(id, name, skills, summary.filter(_.nonEmpty)),
          score,
          query.mode,
          meta,
          query.searchId,
          retrievalScore = Option(document.get("retrievalScore")).collect { case value: Number => value.doubleValue }
        )
      }
    }
    .leftMap(_ => RepositoryError.Unavailable)
}
